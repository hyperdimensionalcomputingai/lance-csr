// SPDX-License-Identifier: Apache-2.0

//! A graph mapping says which table holds each node label and relationship type, and a
//! [`GraphSnapshot`] pins each of those tables to one Lance version.
//!
//! Applications that store graphs as plain Lance tables, one per label, share these types
//! instead of keeping their own copies. Their JSON is the version 1 graph descriptor:
//!
//! ```json
//! {
//!   "version": 1,
//!   "nodes": [{"label": "Person", "table": "people", "key": "id", "display": "name"}],
//!   "edges": [{"label": "KNOWS", "table": "knows", "key": "edge_id", "source": "src",
//!              "target": "dst", "sourceType": "Person", "targetType": "Person"}]
//! }
//! ```
//!
//! A relationship's `key` is optional: graph queries read only `source` and `target`, so a
//! table with no ID column of its own leaves it out.
//!
//! A snapshot adds `tables`, each a `name`, `path` and `version`. It ignores other fields, so
//! it reads a publication manifest that carries row counts or schemas as well.
//!
//! ```no_run
//! use std::collections::HashMap;
//! use lance_csr::{CypherEngine, GraphSnapshot, Result};
//!
//! # async fn example(json: &str) -> Result<()> {
//! let snapshot: GraphSnapshot = serde_json::from_str(json).expect("valid snapshot");
//! let (config, sources) = snapshot.open(&HashMap::new()).await?;
//! let engine = CypherEngine::new(config, sources, true).await?;
//! # Ok(())
//! # }
//! ```

use std::collections::{HashMap, HashSet};

use arrow_schema::{DataType, Schema, SchemaRef};
use serde::{Deserialize, Serialize};

use crate::config::{GraphConfig, NodeMapping, RelationshipMapping};
use crate::engine::{open_lance_dataset, EngineSource};
use crate::error::{GraphError, Result};

/// The only mapping format version so far.
pub const GRAPH_MAPPING_VERSION: u32 = 1;
/// The most node labels and relationship types one mapping may hold.
pub const MAX_MAPPED_TYPES: usize = 64;
/// The longest label, in bytes.
pub const MAX_LABEL_LEN: usize = 80;

/// A node label backed by one table, with `key` as its unique id column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeTableMapping {
    pub label: String,
    pub table: String,
    pub key: String,
    /// The column applications show as the node's name.
    pub display: Option<String>,
}

/// A relationship type backed by one table. `source` and `target` hold keys of the
/// `source_type` and `target_type` node labels. The optional `key` identifies
/// individual records for consumers; graph queries need only the endpoints.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EdgeTableMapping {
    pub label: String,
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub source: String,
    pub target: String,
    pub source_type: String,
    pub target_type: String,
}

/// Which table holds each node label and relationship type.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GraphMapping {
    pub version: u32,
    pub nodes: Vec<NodeTableMapping>,
    pub edges: Vec<EdgeTableMapping>,
}

/// One table at one Lance version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PinnedTable {
    pub name: String,
    /// The dataset URI. Resolve a relative path against its root before calling
    /// [`GraphSnapshot::open`].
    pub path: String,
    pub version: u64,
}

/// A set of pinned tables and the mapping that reads them as a graph.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphSnapshot {
    pub tables: Vec<PinnedTable>,
    /// `None` while the tables have no graph mapping yet.
    #[serde(default)]
    pub mapping: Option<GraphMapping>,
}

fn invalid(message: impl Into<String>) -> GraphError {
    GraphError::ConfigError {
        message: message.into(),
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

fn check_label(label: &str) -> Result<()> {
    let valid = !label.is_empty()
        && label.len() <= MAX_LABEL_LEN
        && label
            .chars()
            .enumerate()
            .all(|(i, c)| c == '_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()));
    if valid {
        Ok(())
    } else {
        Err(invalid(format!(
            "Graph labels must be ASCII identifiers of at most {MAX_LABEL_LEN} characters: '{label}'"
        )))
    }
}

impl GraphMapping {
    /// Check the rules that need no table data: a supported version, at least one node label,
    /// at most [`MAX_MAPPED_TYPES`] labels, labels that are ASCII identifiers and unique
    /// ignoring case, one graph role per table, and relationship endpoints that name a mapped
    /// node label.
    pub fn validate(&self) -> Result<()> {
        if self.version != GRAPH_MAPPING_VERSION {
            return Err(invalid(format!(
                "Unsupported graph mapping version {}; expected {GRAPH_MAPPING_VERSION}",
                self.version
            )));
        }
        if self.nodes.is_empty() {
            return Err(invalid("Map at least one node table"));
        }
        if self.nodes.len() + self.edges.len() > MAX_MAPPED_TYPES {
            return Err(invalid(format!(
                "At most {MAX_MAPPED_TYPES} node labels and relationship types are supported"
            )));
        }
        let mut labels = HashSet::new();
        let mut tables = HashSet::new();
        let roles = self
            .nodes
            .iter()
            .map(|n| (&n.label, &n.table))
            .chain(self.edges.iter().map(|e| (&e.label, &e.table)));
        for (label, table) in roles {
            check_label(label)?;
            if !labels.insert(label.to_lowercase()) {
                return Err(invalid(format!(
                    "Duplicate graph label, ignoring case: '{label}'"
                )));
            }
            if !tables.insert(table) {
                return Err(invalid(format!(
                    "Table '{table}' has more than one graph role"
                )));
            }
        }
        for edge in &self.edges {
            for end in [&edge.source_type, &edge.target_type] {
                if self.node(end).is_none() {
                    return Err(invalid(format!(
                        "Relationship '{}' connects '{end}', which is not a mapped node label",
                        edge.label
                    )));
                }
            }
        }
        Ok(())
    }

    fn node(&self, label: &str) -> Option<&NodeTableMapping> {
        self.nodes
            .iter()
            .find(|n| n.label.eq_ignore_ascii_case(label))
    }

    /// Build the [`GraphConfig`] for this mapping, given the Arrow schema of each mapped table,
    /// keyed by table name.
    ///
    /// Besides [`Self::validate`], this checks each mapped table: its columns don't collide
    /// ignoring case, the key, display and endpoint columns exist, keys are scalar, and each
    /// endpoint column has the type of the node key it refers to. Node properties are every
    /// column but the key; relationship properties are every column.
    pub fn graph_config(&self, schemas: &HashMap<String, SchemaRef>) -> Result<GraphConfig> {
        self.validate()?;
        let schema = |table: &str| -> Result<&Schema> {
            let schema = schemas
                .get(table)
                .ok_or_else(|| invalid(format!("Mapped table '{table}' has no schema")))?;
            let mut names = HashSet::new();
            if !schema
                .fields()
                .iter()
                .all(|f| names.insert(f.name().to_lowercase()))
            {
                return Err(invalid(format!(
                    "Table '{table}' has columns whose names differ only in case"
                )));
            }
            Ok(schema)
        };
        let column = |schema: &Schema, table: &str, name: &str| -> Result<DataType> {
            schema
                .field_with_name(name)
                .map(|f| f.data_type().clone())
                .map_err(|_| invalid(format!("Table '{table}' has no column '{name}'")))
        };
        let key = |schema: &Schema, table: &str, name: &str| -> Result<DataType> {
            let data_type = column(schema, table, name)?;
            if data_type.is_nested() || data_type == DataType::Null {
                return Err(invalid(format!(
                    "Key column '{table}.{name}' must have a scalar type, not {data_type}"
                )));
            }
            Ok(data_type)
        };
        let columns = |schema: &Schema, skip: Option<&str>| -> Vec<String> {
            schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .filter(|name| Some(name.as_str()) != skip)
                .collect()
        };

        let mut builder = GraphConfig::builder();
        for node in &self.nodes {
            let s = schema(&node.table)?;
            key(s, &node.table, &node.key)?;
            if let Some(display) = &node.display {
                column(s, &node.table, display)?;
            }
            builder = builder.with_node_mapping(
                NodeMapping::new(node.label.as_str(), node.key.as_str())
                    .with_properties(columns(s, Some(&node.key))),
            );
        }
        for edge in &self.edges {
            let s = schema(&edge.table)?;
            if let Some(name) = &edge.key {
                key(s, &edge.table, name)?;
            }
            for (end, end_type) in [
                (&edge.source, &edge.source_type),
                (&edge.target, &edge.target_type),
            ] {
                let node = self.node(end_type).expect("validated endpoint");
                let expected = key(schema(&node.table)?, &node.table, &node.key)?;
                let actual = key(s, &edge.table, end)?;
                if actual != expected {
                    return Err(invalid(format!(
                        "Endpoint '{}.{end}' is {actual}, but node key '{}.{}' is {expected}",
                        edge.table, node.table, node.key
                    )));
                }
            }
            builder = builder.with_relationship_mapping(
                RelationshipMapping::new(
                    edge.label.as_str(),
                    edge.source.as_str(),
                    edge.target.as_str(),
                )
                .with_endpoints(edge.source_type.as_str(), edge.target_type.as_str())
                .with_properties(columns(s, None)),
            );
        }
        builder.build()
    }
}

impl GraphSnapshot {
    /// Open every mapped table at its pinned version and build the graph configuration.
    ///
    /// Returns what [`crate::CypherEngine::new`] takes: the configuration and one source per
    /// label, keyed by the lowercased label. Tables the mapping doesn't use stay closed.
    pub async fn open(
        &self,
        storage_options: &HashMap<String, String>,
    ) -> Result<(GraphConfig, HashMap<String, EngineSource>)> {
        let mapping = self
            .mapping
            .as_ref()
            .ok_or_else(|| invalid("The snapshot has no graph mapping"))?;
        mapping.validate()?;
        let roles = mapping
            .nodes
            .iter()
            .map(|n| (&n.label, &n.table))
            .chain(mapping.edges.iter().map(|e| (&e.label, &e.table)));
        let mut schemas = HashMap::new();
        let mut sources = HashMap::new();
        for (label, table) in roles {
            let pinned = self
                .tables
                .iter()
                .find(|t| &t.name == table)
                .ok_or_else(|| invalid(format!("Mapped table '{table}' is not in the snapshot")))?;
            let dataset =
                open_lance_dataset(&pinned.path, Some(pinned.version), storage_options).await?;
            schemas.insert(table.clone(), SchemaRef::new(dataset.schema().into()));
            sources.insert(
                label.to_lowercase(),
                EngineSource::Lance {
                    uri: pinned.path.clone(),
                    storage_options: storage_options.clone(),
                    dataset: std::sync::Arc::new(dataset),
                },
            );
        }
        Ok((mapping.graph_config(&schemas)?, sources))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;

    const DESCRIPTOR: &str = r#"{
        "version": 1,
        "nodes": [{"label": "Person", "table": "people", "key": "id", "display": "name"}],
        "edges": [{"label": "KNOWS", "table": "knows", "key": "edge_id", "source": "src",
                   "target": "dst", "sourceType": "Person", "targetType": "Person"}]
    }"#;

    #[test]
    fn relationships_without_keys_round_trip_and_build() {
        let mut value: serde_json::Value = serde_json::from_str(DESCRIPTOR).unwrap();
        value["edges"][0].as_object_mut().unwrap().remove("key");
        let m: GraphMapping = serde_json::from_value(value).unwrap();
        m.validate().unwrap();
        m.graph_config(&schemas(DataType::Int64)).unwrap();
        assert!(m.edges[0].key.is_none());
        assert!(serde_json::to_value(&m).unwrap()["edges"][0]
            .get("key")
            .is_none());
    }

    fn mapping() -> GraphMapping {
        serde_json::from_str(DESCRIPTOR).unwrap()
    }

    fn schemas(endpoint: DataType) -> HashMap<String, SchemaRef> {
        let people = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let knows = Schema::new(vec![
            Field::new("edge_id", DataType::Int64, false),
            Field::new("src", endpoint.clone(), false),
            Field::new("dst", endpoint, false),
        ]);
        HashMap::from([
            ("people".to_string(), SchemaRef::new(people)),
            ("knows".to_string(), SchemaRef::new(knows)),
        ])
    }

    fn error<T>(result: Result<T>) -> String {
        result.err().expect("an error").to_string()
    }

    #[test]
    fn reads_the_descriptor_and_round_trips() {
        let m = mapping();
        m.validate().unwrap();
        assert_eq!(m.nodes[0].display.as_deref(), Some("name"));
        let back: GraphMapping = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(back, m);
        // An unknown field is a different format, not a typo to skip.
        let extra = DESCRIPTOR.replacen("\"version\"", "\"extra\": 1, \"version\"", 1);
        assert!(serde_json::from_str::<GraphMapping>(&extra).is_err());
    }

    #[test]
    fn snapshot_reads_a_publication_manifest() {
        let manifest = format!(
            r#"{{"format": "example-publication", "version": 1, "name": "demo",
                "tables": [{{"name": "people", "path": "data/people.lance", "version": 3,
                             "rows": 4, "fields": []}}],
                "mapping": {DESCRIPTOR}, "sample": null}}"#
        );
        let snapshot: GraphSnapshot = serde_json::from_str(&manifest).unwrap();
        assert_eq!(snapshot.tables[0].version, 3);
        assert_eq!(snapshot.mapping, Some(mapping()));
        let unmapped: GraphSnapshot = serde_json::from_str(r#"{"tables": []}"#).unwrap();
        assert!(unmapped.mapping.is_none());
    }

    #[test]
    fn validate_rejects_structural_errors() {
        let mut m = mapping();
        m.version = 2;
        assert!(error(m.validate()).contains("version"));

        let mut m = mapping();
        m.nodes.clear();
        assert!(error(m.validate()).contains("at least one node"));

        let mut m = mapping();
        m.edges[0].label = "person".into();
        assert!(error(m.validate()).contains("Duplicate graph label"));

        let mut m = mapping();
        m.edges[0].table = "people".into();
        assert!(error(m.validate()).contains("more than one graph role"));

        let mut m = mapping();
        m.nodes[0].label = "Pérson".into();
        assert!(error(m.validate()).contains("ASCII identifiers"));

        let mut m = mapping();
        m.edges[0].target_type = "Company".into();
        assert!(error(m.validate()).contains("not a mapped node label"));

        let mut m = mapping();
        m.edges[0].source_type = "PERSON".into();
        m.validate().unwrap();
    }

    #[test]
    fn graph_config_checks_columns_and_sets_properties() {
        let config = mapping().graph_config(&schemas(DataType::Int64)).unwrap();
        let person = config.get_node_mapping("Person").unwrap();
        assert_eq!(person.property_fields, ["name"]);
        let knows = config.get_relationship_mapping("KNOWS").unwrap();
        assert_eq!(knows.source_label.as_deref(), Some("Person"));

        assert!(error(mapping().graph_config(&schemas(DataType::Utf8))).contains("node key"));

        let mut m = mapping();
        m.nodes[0].display = Some("nickname".into());
        assert!(error(m.graph_config(&schemas(DataType::Int64))).contains("no column 'nickname'"));

        let mut s = schemas(DataType::Int64);
        s.insert(
            "people".into(),
            SchemaRef::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("ID", DataType::Int64, false),
            ])),
        );
        assert!(error(mapping().graph_config(&s)).contains("differ only in case"));
    }

    #[tokio::test]
    async fn open_reads_the_pinned_version() {
        use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
        use lance::dataset::{Dataset, WriteMode, WriteParams};

        async fn write(path: &str, batch: RecordBatch, mode: WriteMode) {
            let schema = batch.schema();
            let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
            let params = WriteParams {
                mode,
                ..Default::default()
            };
            Dataset::write(reader, path, Some(params)).await.unwrap();
        }
        let s = schemas(DataType::Int64);
        let people = |ids: Vec<i64>| {
            let names: Vec<String> = ids.iter().map(|i| format!("p{i}")).collect();
            RecordBatch::try_new(
                s["people"].clone(),
                vec![
                    std::sync::Arc::new(Int64Array::from(ids)),
                    std::sync::Arc::new(StringArray::from(names)),
                ],
            )
            .unwrap()
        };
        let knows = RecordBatch::try_new(
            s["knows"].clone(),
            vec![
                std::sync::Arc::new(Int64Array::from(vec![10])),
                std::sync::Arc::new(Int64Array::from(vec![1])),
                std::sync::Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = |name: &str| dir.path().join(name).to_string_lossy().into_owned();
        write(&path("people.lance"), people(vec![1, 2]), WriteMode::Create).await;
        write(&path("people.lance"), people(vec![3]), WriteMode::Append).await;
        write(&path("knows.lance"), knows, WriteMode::Create).await;

        let snapshot = GraphSnapshot {
            tables: vec![
                PinnedTable {
                    name: "people".into(),
                    path: path("people.lance"),
                    version: 1,
                },
                PinnedTable {
                    name: "knows".into(),
                    path: path("knows.lance"),
                    version: 1,
                },
            ],
            mapping: Some(mapping()),
        };
        let (config, sources) = snapshot.open(&HashMap::new()).await.unwrap();
        let engine = crate::CypherEngine::new(config, sources, true)
            .await
            .unwrap();
        let batch = engine
            .execute("MATCH (p:Person) RETURN count(p) AS n", HashMap::new())
            .await
            .unwrap();
        let n = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 2);

        let mut keyless = snapshot.clone();
        keyless.mapping.as_mut().unwrap().edges[0].key = None;
        keyless.open(&HashMap::new()).await.unwrap();

        let mut missing = snapshot.clone();
        missing.tables.pop();
        assert!(error(missing.open(&HashMap::new()).await).contains("not in the snapshot"));
    }
}
