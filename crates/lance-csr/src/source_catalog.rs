// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Context-free source catalog for DataFusion logical planning.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use arrow_schema::{Schema, SchemaRef};
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::TableSource;

use crate::engine::EngineSource;
use crate::error::Result;

/// A relationship table with an extra occurrence ID column, and the name of that column.
pub type RelationshipOccurrenceTable = (Arc<dyn TableSource>, String);

/// A minimal catalog to resolve node labels and relationship types to logical table sources.
pub trait GraphSourceCatalog: Send + Sync {
    fn node_source(&self, label: &str) -> Option<Arc<dyn TableSource>>;
    fn relationship_source(&self, rel_type: &str) -> Option<Arc<dyn TableSource>>;

    /// True only when the non-null IDs in this column are known to be unique for this snapshot.
    /// An unverified source keeps the ordinary join route for overlapping unnamed path segments.
    fn has_unique_node_ids(&self, _label: &str, _column: &str) -> bool {
        false
    }

    /// An alternate source for `rel_type` with one extra `UInt64` column that identifies each
    /// stored edge occurrence (unique and stable within the pinned snapshot), plus that
    /// column's name. `None` (the default) means the catalog cannot supply one, and callers
    /// fall back to numbering parallel edges with a window.
    fn relationship_occurrence_source(
        &self,
        _rel_type: &str,
    ) -> Result<Option<RelationshipOccurrenceTable>> {
        Ok(None)
    }
}

/// A relationship's engine source, with its occurrence-id table built on first use.
enum OccurrenceSource {
    Engine {
        source: EngineSource,
        distinct: Option<HashMap<String, usize>>,
    },
    Ready(RelationshipOccurrenceTable),
}

struct NodeKey {
    column: String,
    source: EngineSource,
    unique: tokio::sync::OnceCell<bool>,
}

/// During planning, collect only the ID columns required by a candidate inventory.
/// Optimistically plan for unique IDs. Validate against the pinned source before using that plan.
pub(crate) struct InventoryProbe<C> {
    catalog: C,
    pending: Mutex<HashSet<String>>,
}

impl<C: std::borrow::Borrow<InMemoryCatalog>> InventoryProbe<C> {
    pub(crate) fn new(catalog: C) -> Self {
        Self {
            catalog,
            pending: Mutex::default(),
        }
    }

    /// Returns true when a provisional inventory must be replaced by joins.
    pub(crate) async fn verify(&self) -> Result<bool> {
        let labels =
            std::mem::take(&mut *self.pending.lock().unwrap_or_else(PoisonError::into_inner));
        let mut needs_joins = false;
        for label in labels {
            let key = &self.catalog.borrow().unique_node_keys[&label];
            let unique = key
                .unique
                .get_or_try_init(|| key.source.unique_inventory_ids(&key.column))
                .await?;
            needs_joins |= !unique;
        }
        Ok(needs_joins)
    }
}

impl<C: std::borrow::Borrow<InMemoryCatalog> + Send + Sync> GraphSourceCatalog
    for InventoryProbe<C>
{
    fn node_source(&self, label: &str) -> Option<Arc<dyn TableSource>> {
        self.catalog.borrow().node_source(label)
    }
    fn relationship_source(&self, kind: &str) -> Option<Arc<dyn TableSource>> {
        self.catalog.borrow().relationship_source(kind)
    }
    fn relationship_occurrence_source(
        &self,
        kind: &str,
    ) -> Result<Option<RelationshipOccurrenceTable>> {
        self.catalog.borrow().relationship_occurrence_source(kind)
    }
    fn has_unique_node_ids(&self, label: &str, column: &str) -> bool {
        let label = label.to_lowercase();
        let Some(key) = self.catalog.borrow().unique_node_keys.get(&label) else {
            return false;
        };
        if !key.column.eq_ignore_ascii_case(column) {
            return false;
        }
        if let Some(unique) = key.unique.get() {
            return *unique;
        }
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(label);
        true
    }
}

/// A simple in-memory catalog useful for tests and bootstrap wiring.
pub struct InMemoryCatalog {
    node_sources: HashMap<String, Arc<dyn TableSource>>,
    rel_sources: HashMap<String, Arc<dyn TableSource>>,
    occurrence_sources: HashMap<String, Mutex<OccurrenceSource>>,
    unique_node_keys: HashMap<String, NodeKey>,
}

impl InMemoryCatalog {
    pub fn new() -> Self {
        Self {
            node_sources: HashMap::new(),
            rel_sources: HashMap::new(),
            occurrence_sources: HashMap::new(),
            unique_node_keys: HashMap::new(),
        }
    }

    /// Retain the engine source behind a relationship so `relationship_occurrence_source`
    /// can build its occurrence-id table lazily. Nothing is built here.
    pub(crate) fn with_relationship_engine_source(
        mut self,
        rel_type: impl Into<String>,
        source: EngineSource,
        distinct: Option<HashMap<String, usize>>,
    ) -> Self {
        if matches!(&source, EngineSource::Lance { .. })
            && source
                .schema()
                .fields()
                .iter()
                .any(|f| f.name().eq_ignore_ascii_case("_rowaddr"))
        {
            return self;
        }
        self.occurrence_sources.insert(
            rel_type.into().to_lowercase(),
            Mutex::new(OccurrenceSource::Engine { source, distinct }),
        );
        self
    }

    /// A namespace Lance scan already exposes its snapshot-local physical row address.
    pub(crate) fn with_relationship_occurrence_table(
        mut self,
        rel_type: &str,
        table: RelationshipOccurrenceTable,
    ) -> Self {
        self.occurrence_sources.insert(
            rel_type.to_lowercase(),
            Mutex::new(OccurrenceSource::Ready(table)),
        );
        self
    }

    pub fn with_node_source(
        mut self,
        label: impl Into<String>,
        source: Arc<dyn TableSource>,
    ) -> Self {
        // Normalize key to lowercase for case-insensitive lookup
        let label = label.into().to_lowercase();
        self.unique_node_keys.remove(&label);
        self.node_sources.insert(label, source);
        self
    }

    pub(crate) fn with_node_key_source(
        mut self,
        label: &str,
        column: &str,
        source: EngineSource,
        known: Option<bool>,
    ) -> Self {
        self.unique_node_keys.insert(
            label.to_lowercase(),
            NodeKey {
                column: column.into(),
                source,
                unique: tokio::sync::OnceCell::new_with(known),
            },
        );
        self
    }

    pub(crate) fn verified_node_key(&self, label: &str) -> Option<bool> {
        self.unique_node_keys
            .get(&label.to_lowercase())?
            .unique
            .get()
            .copied()
    }

    pub(crate) fn has_unverified_node_keys(&self) -> bool {
        self.unique_node_keys
            .values()
            .any(|key| key.unique.get().is_none())
    }

    pub fn with_relationship_source(
        mut self,
        rel_type: impl Into<String>,
        source: Arc<dyn TableSource>,
    ) -> Self {
        // Normalize key to lowercase for case-insensitive lookup
        let key = rel_type.into().to_lowercase();
        self.occurrence_sources.remove(&key);
        self.rel_sources.insert(key, source);
        self
    }
}

impl Default for InMemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphSourceCatalog for InMemoryCatalog {
    /// Get node source with case-insensitive label lookup
    ///
    /// Note: Keys are stored as lowercase, so this is an O(1) operation.
    fn node_source(&self, label: &str) -> Option<Arc<dyn TableSource>> {
        self.node_sources.get(&label.to_lowercase()).cloned()
    }

    fn has_unique_node_ids(&self, label: &str, column: &str) -> bool {
        self.unique_node_keys
            .get(&label.to_lowercase())
            .is_some_and(|key| {
                key.column.eq_ignore_ascii_case(column) && key.unique.get() == Some(&true)
            })
    }

    /// Get relationship source with case-insensitive type lookup
    ///
    /// Note: Keys are stored as lowercase, so this is an O(1) operation.
    fn relationship_source(&self, rel_type: &str) -> Option<Arc<dyn TableSource>> {
        self.rel_sources.get(&rel_type.to_lowercase()).cloned()
    }

    /// Built once per relationship for this catalog (one pinned snapshot; `checkout_latest()`
    /// builds a new catalog). The lock is held while building, so concurrent queries share one build.
    fn relationship_occurrence_source(
        &self,
        rel_type: &str,
    ) -> Result<Option<RelationshipOccurrenceTable>> {
        let Some(entry) = self.occurrence_sources.get(&rel_type.to_lowercase()) else {
            return Ok(None);
        };
        let mut entry = entry.lock().unwrap_or_else(PoisonError::into_inner);
        match &*entry {
            OccurrenceSource::Ready(table) => Ok(Some(table.clone())),
            OccurrenceSource::Engine { source, distinct } => {
                let (provider, column) = source.occurrence_table_provider(rel_type)?;
                let provider = match distinct {
                    Some(counts) => Arc::new(crate::key_statistics::KeyStatisticsProvider::new(
                        provider,
                        counts.clone(),
                    )),
                    None => provider,
                };
                let table: RelationshipOccurrenceTable =
                    (Arc::new(DefaultTableSource::new(provider)), column);
                *entry = OccurrenceSource::Ready(table.clone());
                Ok(Some(table))
            }
        }
    }
}

/// A trivial logical table source with a fixed schema.
pub struct SimpleTableSource {
    schema: SchemaRef,
}

impl SimpleTableSource {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
    pub fn empty() -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
        }
    }
}

impl TableSource for SimpleTableSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
