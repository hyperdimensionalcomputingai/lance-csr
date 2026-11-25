// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! One immutable inventory per rich-path operator. An edge's identity is its actual
//! scanned occurrence, shared by every hop, never reconstructed from endpoint pairs.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray, StructArray,
    UInt32Array,
};
use arrow::compute::{cast, concat_batches, take};
use arrow_schema::{DataType, Field, Fields, Schema};
use datafusion::common::{exec_err, plan_err, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::{lock::Mutex, StreamExt};

/// One source scan per query, shared even by distinct named-path operators.
pub(crate) type SourceBatches = Arc<Mutex<HashMap<(bool, String), RecordBatch>>>;

use crate::csr_expand::{cooperative_yield, is_supported_id_type, EdgeRows, ROW_ADDR};
use crate::path_value::{entity_type, struct_array};

#[derive(Debug, Clone)]
pub(crate) struct NodeSource {
    pub label: String,
    pub id: String,
    pub schema: Arc<Schema>,
}

#[derive(Debug, Clone)]
pub(crate) struct EdgeSource {
    pub label: String,
    pub source_label: usize,
    pub target_label: usize,
    pub source_id: String,
    pub target_id: String,
    pub schema: Arc<Schema>,
    /// The engine's rows for this type when it has a CSR. Every path route then reads the
    /// type from them, so a relationship's identity is the same on all routes.
    pub rows: Option<Arc<EdgeRows>>,
}

/// A relationship's `{label, id}` identity, shared by every path route.
pub(crate) fn relationship_identity(labels: Vec<&str>, ids: Vec<i64>) -> StructArray {
    StructArray::from(vec![
        (
            Arc::new(Field::new("label", DataType::Utf8, false)),
            Arc::new(StringArray::from(labels)) as ArrayRef,
        ),
        (
            Arc::new(Field::new("id", DataType::Int64, false)),
            Arc::new(Int64Array::from(ids)) as ArrayRef,
        ),
    ])
}

pub(crate) fn property_fields<'a>(schemas: impl IntoIterator<Item = &'a Fields>) -> Result<Fields> {
    let mut fields = BTreeMap::<String, DataType>::new();
    for schema in schemas {
        for field in schema {
            let name = field.name().to_lowercase();
            if fields.get(&name).is_some_and(|t| t != field.data_type()) {
                return plan_err!(
                    "path entities have conflicting Arrow types for property '{name}'"
                );
            }
            fields.insert(name, field.data_type().clone());
        }
    }
    Ok(fields
        .into_iter()
        .map(|(name, t)| Field::new(name, t, true))
        .collect())
}

pub(crate) fn id_column(batch: &RecordBatch, name: &str) -> Result<Int64Array> {
    let schema = batch.schema();
    let Some((i, field)) = schema
        .fields()
        .iter()
        .enumerate()
        .find(|(_, f)| f.name().eq_ignore_ascii_case(name))
    else {
        return exec_err!("path ID column '{name}' is missing");
    };
    if !is_supported_id_type(field.data_type()) {
        return exec_err!(
            "path ID column '{name}' must be a signed integer or uint8/16/32, got {}",
            field.data_type()
        );
    }
    Ok(cast(batch.column(i), &DataType::Int64)?
        .as_primitive::<arrow::datatypes::Int64Type>()
        .clone())
}

pub(crate) fn properties(batch: &RecordBatch, fields: &Fields) -> Result<RecordBatch> {
    let columns = fields
        .iter()
        .map(|f| {
            batch
                .schema()
                .fields()
                .iter()
                .position(|old| old.name().eq_ignore_ascii_case(f.name()))
                .map(|i| batch.column(i).clone())
                .unwrap_or_else(|| new_null_array(f.data_type(), batch.num_rows()))
        })
        .collect();
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields.clone())),
        columns,
    )?)
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EdgeRef {
    pub edge: u32,
    pub node: u32,
    pub outgoing: bool,
}

#[derive(Debug)]
pub(crate) struct Inventory {
    pub nodes: StructArray,
    pub edges: StructArray,
    pub node_properties: RecordBatch,
    pub edge_properties: RecordBatch,
    pub node_labels: Vec<usize>,
    pub edge_types: Vec<usize>,
    pub endpoints: Vec<Option<(u32, u32)>>,
    pub by_key: HashMap<(usize, i64), u32, ahash::RandomState>,
    pub neighbors: Vec<Vec<EdgeRef>>,
}

impl Inventory {
    pub async fn build(
        node_sources: &[NodeSource],
        edge_sources: &[EdgeSource],
        plans: &[Arc<dyn ExecutionPlan>],
        context: Arc<TaskContext>,
        sources: &SourceBatches,
    ) -> Result<Self> {
        // Plans scan the node tables, then the relationship tables without engine rows.
        let mut batches = Vec::with_capacity(node_sources.len() + edge_sources.len());
        let mut plans = plans.iter();
        let keyed = node_sources
            .iter()
            .map(|s| (false, s.label.to_lowercase(), None))
            .chain(
                edge_sources
                    .iter()
                    .map(|s| (true, s.label.to_lowercase(), s.rows.as_ref())),
            );
        for (edge, label, rows) in keyed {
            let key = (edge, label);
            let mut sources = sources.lock().await;
            let plan = if rows.is_none() {
                Some(plans.next().ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "path source without a scan".into(),
                    )
                })?)
            } else {
                None
            };
            if !sources.contains_key(&key) {
                let batch = match (rows, plan) {
                    (Some(rows), _) => rows
                        .read_all()
                        .await
                        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?,
                    (None, Some(plan)) => {
                        let mut stream = plan.execute(0, context.clone())?;
                        let mut read = Vec::new();
                        while let Some(batch) = stream.next().await {
                            read.push(batch?);
                            cooperative_yield().await;
                        }
                        concat_batches(&plan.schema(), &read)?
                    }
                    (None, None) => unreachable!("a scan or engine rows"),
                };
                sources.insert(key.clone(), batch);
            }
            batches.push(sources[&key].clone());
        }
        let node_fields = property_fields(node_sources.iter().map(|s| s.schema.fields()))?;
        let edge_fields = property_fields(edge_sources.iter().map(|s| s.schema.fields()))?;
        let node_type = entity_type(node_fields.clone(), "node", None);
        let edge_type = entity_type(edge_fields.clone(), "relationship", Some(&node_type));
        let mut by_key = HashMap::default();
        let mut node_labels = Vec::new();
        let mut labels = Vec::new();
        let mut ids = Vec::new();
        let mut node_batches = Vec::new();
        for (label, source) in node_sources.iter().enumerate() {
            let batch = &batches[label];
            let keys = id_column(batch, &source.id)?;
            for row in 0..batch.num_rows() {
                let dense = u32::try_from(ids.len()).map_err(|_| {
                    datafusion::common::DataFusionError::Execution(
                        "path inventory exceeds u32 row references".into(),
                    )
                })?;
                if keys.is_valid(row) && by_key.insert((label, keys.value(row)), dense).is_some() {
                    return exec_err!(
                        "path values require unique node IDs within label '{}'; duplicate ID {}",
                        source.label,
                        keys.value(row)
                    );
                }
                node_labels.push(label);
                labels.push(source.label.as_str());
                ids.push(keys.is_valid(row).then(|| keys.value(row)));
                if row % 4096 == 4095 {
                    cooperative_yield().await;
                }
            }
            node_batches.push(properties(batch, &node_fields)?);
        }
        let node_properties = concat_batches(&Arc::new(Schema::new(node_fields)), &node_batches)?;
        let identity = StructArray::from(vec![
            (
                Arc::new(Field::new("label", DataType::Utf8, false)),
                Arc::new(StringArray::from(labels)) as ArrayRef,
            ),
            // Null keys never enter the lookup; their records cannot appear in a path.
            (
                Arc::new(Field::new("id", DataType::Int64, false)),
                Arc::new(Int64Array::from_iter_values(
                    ids.iter().map(|v| v.unwrap_or_default()),
                )) as ArrayRef,
            ),
        ]);
        let nodes = struct_array(
            &node_type,
            vec![
                Arc::new(StructArray::from(node_properties.clone())),
                Arc::new(identity),
            ],
        );
        let mut neighbors = vec![Vec::new(); nodes.len()];
        let mut edge_batches = Vec::new();
        let mut edge_types = Vec::new();
        let mut occurrence_ids = Vec::new();
        let mut endpoints = Vec::new();
        let mut labels = Vec::new();
        for (kind, source) in edge_sources.iter().enumerate() {
            let batch = &batches[node_sources.len() + kind];
            let sources = id_column(batch, &source.source_id)?;
            let targets = id_column(batch, &source.target_id)?;
            // Engine rows carry the identity the CSR uses; a plain scan numbers its rows.
            let row_ids = batch
                .column_by_name(ROW_ADDR)
                .map(|ids| ids.as_primitive::<arrow::datatypes::UInt64Type>().clone());
            for row in 0..batch.num_rows() {
                let edge = u32::try_from(endpoints.len()).map_err(|_| {
                    datafusion::common::DataFusionError::Execution(
                        "path inventory exceeds u32 row references".into(),
                    )
                })?;
                let ends = if sources.is_valid(row) && targets.is_valid(row) {
                    by_key
                        .get(&(source.source_label, sources.value(row)))
                        .zip(by_key.get(&(source.target_label, targets.value(row))))
                        .map(|(&a, &b)| (a, b))
                } else {
                    None
                };
                if let Some((a, b)) = ends {
                    neighbors[a as usize].push(EdgeRef {
                        edge,
                        node: b,
                        outgoing: true,
                    });
                    if a != b {
                        neighbors[b as usize].push(EdgeRef {
                            edge,
                            node: a,
                            outgoing: false,
                        });
                    }
                }
                endpoints.push(ends);
                edge_types.push(kind);
                occurrence_ids.push(
                    row_ids
                        .as_ref()
                        .map_or(row as i64, |ids| ids.value(row) as i64),
                );
                labels.push(source.label.as_str());
                if row % 4096 == 4095 {
                    cooperative_yield().await;
                }
            }
            edge_batches.push(properties(batch, &edge_fields)?);
        }
        let edge_properties = concat_batches(&Arc::new(Schema::new(edge_fields)), &edge_batches)?;
        let identity = relationship_identity(labels, occurrence_ids);
        let starts = UInt32Array::from_iter(endpoints.iter().map(|p| p.map(|(a, _)| a)));
        let ends = UInt32Array::from_iter(endpoints.iter().map(|p| p.map(|(_, b)| b)));
        let edges = struct_array(
            &edge_type,
            vec![
                Arc::new(StructArray::from(edge_properties.clone())),
                Arc::new(identity),
                take(&nodes, &starts, None)?,
                take(&nodes, &ends, None)?,
            ],
        );
        Ok(Self {
            nodes,
            edges,
            node_properties,
            edge_properties,
            node_labels,
            edge_types,
            endpoints,
            by_key,
            neighbors,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn duplicate_node_keys_do_not_choose_an_arbitrary_endpoint() {
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
        )])
        .unwrap();
        let source = NodeSource {
            label: "Person".into(),
            id: "id".into(),
            schema: batch.schema(),
        };
        let ctx = SessionContext::new();
        let plan = ctx
            .read_batch(batch)
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let error = Inventory::build(&[source], &[], &[plan], ctx.task_ctx(), &Default::default())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("duplicate ID 1"));
    }
}
