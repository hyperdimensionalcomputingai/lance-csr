// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! A Cypher engine that registers its datasets once and serves many queries.
//!
//! [`CypherEngine`] holds a snapshot of every input: the DataFusion context, the
//! catalog, join-key statistics and, with `use_csr`, the CSR adjacencies and node
//! indexes. Queries share the snapshot read-only; [`CypherEngine::refresh`] builds a
//! complete replacement and swaps it in.
//!
//! ```no_run
//! use std::collections::HashMap;
//! use lance_csr::engine::{CypherEngine, EngineSource};
//! use lance_csr::GraphConfig;
//!
//! # async fn example() -> lance_csr::Result<()> {
//! let config = GraphConfig::builder()
//!     .node_table("Person", "id")
//!     .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id")
//!     .build()?;
//! let sources = HashMap::from([
//!     ("Person".to_string(), EngineSource::open_lance("data/person.lance", None, HashMap::new()).await?),
//!     ("KNOWS".to_string(), EngineSource::open_lance("data/knows.lance", None, HashMap::new()).await?),
//! ]);
//! let engine = CypherEngine::new(config, sources, true).await?;
//! let batch = engine
//!     .execute("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name, b.name", HashMap::new())
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::{DefaultTableSource, MemTable, TableProvider};
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use lance::datafusion::LanceTableProvider;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::Dataset;
use serde_json::Value as JsonValue;

use crate::config::GraphConfig;
use crate::csr_expand::{
    edge_changes, is_supported_id_type, Adjacency, AdjacencyCatalog, CsrQueryPlanner, NodeRows,
    PruneCsrTargetColumns, PushCsrTargetFilters,
};
use crate::error::{GraphError, Result};
use crate::key_statistics::KeyStatisticsProvider;
use crate::lance_vector_search::VectorSearch;
use crate::logical_plan::FanOut;
use crate::query::CypherQuery;
use crate::{GraphSourceCatalog, InMemoryCatalog};

/// One input dataset as registered with a [`CypherEngine`].
#[derive(Clone)]
pub enum EngineSource {
    /// On-disk Lance dataset, read lazily through `LanceTableProvider`.
    ///
    /// `dataset` is pinned to one version; only `refresh()` replaces it. `uri` and
    /// `storage_options` are kept so refresh can reopen the same location with the
    /// same credentials.
    Lance {
        uri: String,
        storage_options: HashMap<String, String>,
        dataset: Arc<Dataset>,
    },
    /// In-memory Arrow data supplied by the caller. Treated as immutable: replacing
    /// it requires a new engine.
    Arrow(RecordBatch),
}

impl EngineSource {
    /// Open a Lance dataset at `uri`, pinned to `version` (`None` means the latest).
    ///
    /// `storage_options` are kept so [`CypherEngine::refresh`] can reopen the same
    /// location with the same credentials.
    pub async fn open_lance(
        uri: impl Into<String>,
        version: Option<u64>,
        storage_options: HashMap<String, String>,
    ) -> Result<Self> {
        let uri = uri.into();
        let dataset = open_lance_dataset(&uri, version, &storage_options).await?;
        Ok(Self::Lance {
            uri,
            storage_options,
            dataset: Arc::new(dataset),
        })
    }

    /// Pinned Lance version, or `None` for Arrow input.
    pub fn lance_version(&self) -> Option<u64> {
        match self {
            Self::Lance { dataset, .. } => Some(dataset.manifest().version),
            Self::Arrow(_) => None,
        }
    }

    /// True when both refer to the same immutable data (same pinned Lance handle or
    /// same Arrow batch), so anything derived from one is valid for the other.
    fn same_data(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Lance { dataset: a, .. }, Self::Lance { dataset: b, .. }) => Arc::ptr_eq(a, b),
            (Self::Arrow(a), Self::Arrow(b)) => {
                a.num_columns() == b.num_columns()
                    && a.columns()
                        .iter()
                        .zip(b.columns())
                        .all(|(x, y)| x.to_data().ptr_eq(&y.to_data()))
            }
            _ => false,
        }
    }

    /// Row count: free for Arrow input, from Lance metadata otherwise.
    async fn num_rows(&self, name: &str) -> Result<usize> {
        Ok(match self {
            Self::Arrow(batch) => batch.num_rows(),
            Self::Lance { dataset, .. } => dataset
                .count_rows(None)
                .await
                .map_err(|e| exec_error(format!("Failed to count rows of {}: {}", name, e)))?,
        })
    }
}

/// CSR adjacency statistics for one relationship type; see [`CypherEngine::csr_stats`].
#[derive(Debug, Clone, PartialEq)]
pub struct CsrStats {
    /// Live indexed edges (null endpoints are skipped).
    pub edges: u64,
    /// Appended edges held outside the base CSR since its last full build.
    pub delta_edges: u64,
    /// Removed edges held outside the base CSR since its last full build.
    pub tombstones: u64,
    /// How the last refresh produced it: `build` (first), `rebuild` (full), `delta`
    /// (changes applied) or `reused` (no edge change).
    pub update: &'static str,
    /// Relationship dataset version it reflects (`None` for Arrow input).
    pub version: Option<u64>,
    /// Wall time of that build or delta, both directions.
    pub build_ms: f64,
}

/// Node id -> row index statistics for one node label; see [`CypherEngine::node_index_stats`].
#[derive(Debug, Clone, PartialEq)]
pub struct NodeIndexStats {
    /// Indexed node rows.
    pub rows: usize,
    /// Node dataset version it reflects (`None` for Arrow input).
    pub version: Option<u64>,
    /// Wall time of the build.
    pub build_ms: f64,
}

/// Everything a query needs, built once and shared read-only by concurrent queries.
///
/// `refresh()` builds a complete replacement and swaps it in; queries already running
/// keep the `Arc` they started with, so they finish on their original snapshot.
struct EngineState {
    sources: HashMap<String, EngineSource>,
    /// Unoptimized plan templates for this snapshot; a refresh starts an empty cache.
    plans: Mutex<VecDeque<CachedQuery>>,
    catalog: Arc<dyn GraphSourceCatalog>,
    context: SessionContext,
    /// CSR adjacencies by relationship type; empty unless the engine uses CSR.
    csr: HashMap<String, CsrEntry>,
    /// Node id -> row indexes by node label, for the fused target lookup; same lifecycle.
    node_index: HashMap<String, NodeIndexEntry>,
    /// Join-key distinct counts by dataset name, reported to DataFusion's join planner.
    key_stats: HashMap<String, KeyStatsEntry>,
}

/// Plan templates kept per snapshot.
const PLAN_CACHE_ENTRIES: usize = 128;

/// A small LRU of unoptimized plans. Values are bound after lookup, before optimization.
struct CachedQuery {
    text: String,
    parameter_types: Vec<(String, u8)>,
    query: CypherQuery,
}

// Containers change expression shape; keep them on the uncached path.
fn parameter_types(parameters: &HashMap<String, JsonValue>) -> Option<Vec<(String, u8)>> {
    let mut types = parameters
        .iter()
        .map(|(name, value)| {
            let kind = match value {
                JsonValue::Null => 0,
                JsonValue::Bool(_) => 1,
                JsonValue::String(_) => 2,
                JsonValue::Number(value) if value.is_i64() => 3,
                JsonValue::Number(value) if value.is_f64() => 4,
                _ => return None,
            };
            Some((name.clone(), kind))
        })
        .collect::<Option<Vec<_>>>()?;
    types.sort_unstable();
    Some(types)
}

/// Distinct counts for one dataset's node-id and edge-endpoint columns.
#[derive(Clone)]
struct KeyStatsEntry {
    /// Dataset the counts describe; reused while it is the same snapshot.
    source: EngineSource,
    /// Actual column name -> distinct non-null values.
    distinct: HashMap<String, usize>,
    /// For a relationship dataset: mean fan-out by direction (outgoing, incoming).
    fan_out: Option<(String, f64, f64)>,
}

/// Id -> row index for one node table, plus the source it came from.
#[derive(Clone)]
struct NodeIndexEntry {
    source: EngineSource,
    rows: Arc<NodeRows>,
    build_ms: f64,
}

/// Both-direction adjacencies for one relationship type, plus the source they came from.
#[derive(Clone)]
struct CsrEntry {
    /// Dataset the adjacencies reflect; reused while it is the same snapshot.
    source: EngineSource,
    outgoing: Arc<Adjacency>,
    incoming: Arc<Adjacency>,
    /// Wall time of the last build or delta application.
    build_ms: f64,
    /// How the last refresh produced this entry: build, rebuild, delta or reused.
    update: &'static str,
}

/// Rebuild instead of applying a delta once pending changes (appended edges +
/// tombstones, accumulated since the last full build) would exceed this fraction
/// of the base. Lookups pay for the delta on every query, and a rebuild folds it in.
///
/// This threshold also bounds refresh cost: each delta refresh copies the whole
/// accumulated delta (see the design note on `crate::csr_expand::Adjacency`).
/// Raising it makes refreshes slower as well as lookups.
// shortcut: fixed threshold, chosen from python/benchmarks/csr_delta_accumulated.py; tune per workload if deltas grow large.
const CSR_REBUILD_FRACTION: f64 = 0.25;

/// Upper bound for `target_partitions`: far above any core count, below values whose
/// per-partition setup would exhaust memory instead of returning an error.
pub const MAX_TARGET_PARTITIONS: usize = 1024;

/// Apply the edge changes between `old`'s snapshot and `source` as a delta.
///
/// `None` means rebuild: not both Lance snapshots, the diff is not meaningful
/// or failed (e.g. old files already cleaned up), or the delta would be too large.
async fn try_apply_csr_delta(
    old: &CsrEntry,
    source: &EngineSource,
    src: &str,
    dst: &str,
) -> Option<CsrEntry> {
    let (
        EngineSource::Lance {
            dataset: old_ds, ..
        },
        EngineSource::Lance {
            dataset: new_ds, ..
        },
    ) = (&old.source, source)
    else {
        return None;
    };
    let started = std::time::Instant::now();
    let Ok(Some(changes)) = edge_changes(old_ds, new_ds, src, dst).await else {
        return None;
    };
    let elapsed = || started.elapsed().as_secs_f64() * 1e3;
    if changes.is_empty() {
        return Some(CsrEntry {
            source: source.clone(),
            build_ms: elapsed(),
            update: "reused",
            ..old.clone()
        });
    }
    let base = old.outgoing.base_edges() as f64;
    let pending = old.outgoing.delta_edges() + old.outgoing.tombstones() + changes.len() as u64;
    if pending as f64 > CSR_REBUILD_FRACTION * base {
        return None;
    }
    let flip = |edges: &[(i64, i64)]| edges.iter().map(|&(a, b)| (b, a)).collect::<Vec<_>>();
    let outgoing = old.outgoing.with_changes(&changes.added, &changes.removed);
    let incoming = old
        .incoming
        .with_changes(&flip(&changes.added), &flip(&changes.removed));
    Some(CsrEntry {
        source: source.clone(),
        outgoing: Arc::new(outgoing),
        incoming: Arc::new(incoming),
        build_ms: elapsed(),
        update: "delta",
    })
}

impl EngineState {
    /// Mean fan-out of every relationship type with key statistics, both directions.
    fn fan_out(&self) -> FanOut {
        self.key_stats
            .values()
            .filter_map(|entry| entry.fan_out.as_ref())
            .flat_map(|(rel_type, outgoing, incoming)| {
                [
                    ((rel_type.clone(), true), *outgoing),
                    ((rel_type.clone(), false), *incoming),
                ]
            })
            .collect()
    }

    fn adjacency_catalog(&self) -> AdjacencyCatalog {
        let mut catalog = AdjacencyCatalog::default();
        for (rel_type, entry) in &self.csr {
            catalog.insert(rel_type, true, entry.outgoing.clone());
            catalog.insert(rel_type, false, entry.incoming.clone());
        }
        for (label, entry) in &self.node_index {
            catalog.insert_nodes(label, entry.rows.clone());
        }
        catalog
    }
}

fn exec_error(message: String) -> GraphError {
    GraphError::ExecutionError {
        message,
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

fn config_error(message: String) -> GraphError {
    GraphError::ConfigError {
        message,
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

/// Open a Lance dataset. `version = None` means the latest version.
async fn open_lance_dataset(
    uri: &str,
    version: Option<u64>,
    storage_options: &HashMap<String, String>,
) -> Result<Dataset> {
    let mut builder = DatasetBuilder::from_uri(uri).with_storage_options(storage_options.clone());
    if let Some(version) = version {
        builder = builder.with_version(version);
    }
    builder
        .load()
        .await
        .map_err(|e| exec_error(format!("Failed to open Lance dataset {}: {}", uri, e)))
}

/// Same version number and manifest commit time means the same snapshot.
///
/// The timestamp distinguishes a table deleted and recreated at the same location,
/// which restarts version numbering at 1.
fn same_lance_snapshot(a: &Dataset, b: &Dataset) -> bool {
    a.manifest().version == b.manifest().version
        && a.manifest().timestamp_nanos == b.manifest().timestamp_nanos
}

/// Check that every configured id/endpoint column exists in its dataset.
///
/// Labels and columns compare case-insensitively, matching the planner. Mappings
/// without a dataset are left for query time, as before, so unused labels stay legal.
fn validate_sources(config: &GraphConfig, schemas: &HashMap<String, SchemaRef>) -> Result<()> {
    let schema_for = |label: &str| {
        schemas
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(label))
            .map(|(_, schema)| schema)
    };
    let check = |label: &str, column: &str| -> Result<()> {
        if let Some(schema) = schema_for(label) {
            if !schema
                .fields()
                .iter()
                .any(|f| f.name().eq_ignore_ascii_case(column))
            {
                return Err(config_error(format!(
                    "Dataset for '{}' has no column '{}' required by the graph configuration",
                    label, column
                )));
            }
        }
        Ok(())
    };
    for (_, mapping) in config.iter_node_mappings() {
        check(&mapping.label, &mapping.id_field)?;
    }
    for (_, mapping) in config.iter_relationship_mappings() {
        check(&mapping.relationship_type, &mapping.source_id_field)?;
        check(&mapping.relationship_type, &mapping.target_id_field)?;
    }
    Ok(())
}

/// Validate sources and register them in a fresh catalog and session context.
async fn build_engine_state(
    config: &GraphConfig,
    sources: HashMap<String, EngineSource>,
    use_csr: bool,
    target_partitions: Option<usize>,
    previous: Option<&EngineState>,
) -> Result<EngineState> {
    let mut session_config = SessionConfig::new();
    if let Some(partitions) = target_partitions {
        session_config = session_config.with_target_partitions(partitions);
    }
    // The CSR planner only changes plans that contain CSR nodes, so install it always.
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_config(session_config)
            .with_query_planner(Arc::new(CsrQueryPlanner))
            // Move target-only filters into CSR nodes first, so pruning can then drop
            // the filter columns from the take.
            .with_optimizer_rule(Arc::new(PushCsrTargetFilters))
            .with_optimizer_rule(Arc::new(PruneCsrTargetColumns))
            .build(),
    );
    let mut schemas = HashMap::new();
    let mut providers: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();

    for (name, source) in &sources {
        let provider: Arc<dyn TableProvider> = match source {
            // No _rowid/_rowaddr columns, so Lance and Arrow inputs expose the same schema.
            EngineSource::Lance { dataset, .. } => {
                Arc::new(LanceTableProvider::new(dataset.clone(), false, false))
            }
            EngineSource::Arrow(batch) => Arc::new(
                MemTable::try_new(batch.schema(), memtable_partitions(batch)).map_err(|e| {
                    exec_error(format!("Failed to create MemTable for {}: {}", name, e))
                })?,
            ),
        };
        schemas.insert(name.clone(), provider.schema());
        // Registered unwrapped first: the CSR and key-statistics builds below scan it.
        ctx.register_table(name.to_lowercase(), provider.clone())
            .map_err(|e| exec_error(format!("Failed to register table {}: {}", name, e)))?;
        providers.insert(name.clone(), provider);
    }

    validate_sources(config, &schemas)?;
    let (csr, node_index) = if use_csr {
        (
            build_csr_entries(config, &sources, &schemas, &ctx, previous).await?,
            build_node_index(config, &sources, previous).await?,
        )
    } else {
        (HashMap::new(), HashMap::new())
    };
    let key_stats = build_key_statistics(config, &sources, &schemas, &ctx, &csr, previous).await?;

    let mut catalog = InMemoryCatalog::new();
    for (name, provider) in providers {
        // Re-register with join-key statistics, so plans see them; see `key_statistics`.
        let provider: Arc<dyn TableProvider> = match key_stats.get(&name) {
            Some(entry) => Arc::new(KeyStatisticsProvider::new(provider, entry.distinct.clone())),
            None => provider,
        };
        let table = name.to_lowercase();
        ctx.deregister_table(table.as_str())
            .and_then(|_| ctx.register_table(table.as_str(), provider.clone()))
            .map_err(|e| exec_error(format!("Failed to register table {}: {}", name, e)))?;

        // Every dataset is registered as both a node and a relationship source; the
        // GraphConfig and query pattern decide which interpretation the planner uses.
        let table_source = Arc::new(DefaultTableSource::new(provider));
        catalog = catalog
            .with_node_source(&name, table_source.clone())
            .with_relationship_source(&name, table_source);
    }
    Ok(EngineState {
        sources,
        plans: Mutex::default(),
        catalog: Arc::new(catalog),
        context: ctx,
        csr,
        node_index,
        key_stats,
    })
}

/// Build (or reuse) an id -> row index for every node label with unique integer ids.
///
/// Reused while the node dataset is the same snapshot; edge changes never touch it.
/// Labels whose ids are not integers or not unique get no entry, so their targets
/// keep the hash join.
async fn build_node_index(
    config: &GraphConfig,
    sources: &HashMap<String, EngineSource>,
    previous: Option<&EngineState>,
) -> Result<HashMap<String, NodeIndexEntry>> {
    let mut entries = HashMap::new();
    for (_, mapping) in config.iter_node_mappings() {
        let label = &mapping.label;
        let Some(source) = sources
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(label))
            .map(|(_, source)| source)
        else {
            continue;
        };
        if let Some(old) = previous.and_then(|p| p.node_index.get(label)) {
            if old.source.same_data(source) {
                entries.insert(label.clone(), old.clone());
                continue;
            }
        }
        let started = std::time::Instant::now();
        let rows = match source {
            EngineSource::Lance { dataset, .. } => {
                NodeRows::from_lance(dataset.clone(), &mapping.id_field).await?
            }
            EngineSource::Arrow(batch) => NodeRows::from_batch(batch.clone(), &mapping.id_field)?,
        };
        if let Some(rows) = rows {
            entries.insert(
                label.clone(),
                NodeIndexEntry {
                    source: source.clone(),
                    rows: Arc::new(rows),
                    build_ms: started.elapsed().as_secs_f64() * 1e3,
                },
            );
        }
    }
    Ok(entries)
}

/// Rows per in-memory batch, DataFusion's default `batch_size`.
const MEMTABLE_BATCH_ROWS: usize = 8192;

/// Split one in-memory table into zero-copy slices spread over one partition per core.
///
/// A `MemTable` holding a single large batch is one partition with one batch, and
/// `RoundRobinBatch` repartitioning moves whole batches, so every operator that does
/// not hash-repartition (such as `CsrExpandExec`) would run on one thread over all of
/// it. Slicing shares the input's buffers; nothing is copied.
fn memtable_partitions(batch: &RecordBatch) -> Vec<Vec<RecordBatch>> {
    let partitions = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let mut out = vec![Vec::new(); partitions];
    let rows = batch.num_rows();
    if rows == 0 {
        return vec![vec![batch.clone()]];
    }
    for (i, offset) in (0..rows).step_by(MEMTABLE_BATCH_ROWS).enumerate() {
        let len = MEMTABLE_BATCH_ROWS.min(rows - offset);
        out[i % partitions].push(batch.slice(offset, len));
    }
    out.retain(|p| !p.is_empty());
    out
}

/// Distinct counts for every dataset's node-id and edge-endpoint columns.
///
/// DataFusion 54 sizes hash joins from these (see `crate::key_statistics`).
/// Node ids count as unique (the row count). Edge endpoints use the CSR's distinct
/// source and target counts when the CSR is built, and otherwise one scan of the two
/// endpoint columns; only integer endpoints are counted. Reused while a dataset is
/// the same snapshot, so an unchanged `refresh()` costs nothing.
// shortcut: the fallback scan reads every edge endpoint at construction and on each changed refresh (about one CSR build's scan); switch to an approximate counter (HyperLogLog) or Lance column statistics if that shows up in startup time.
async fn build_key_statistics(
    config: &GraphConfig,
    sources: &HashMap<String, EngineSource>,
    schemas: &HashMap<String, SchemaRef>,
    ctx: &SessionContext,
    csr: &HashMap<String, CsrEntry>,
    previous: Option<&EngineState>,
) -> Result<HashMap<String, KeyStatsEntry>> {
    let mut entries = HashMap::new();
    for (name, source) in sources {
        if let Some(old) = previous.and_then(|p| p.key_stats.get(name)) {
            if old.source.same_data(source) {
                entries.insert(name.clone(), old.clone());
                continue;
            }
        }
        let column = |wanted: &str| {
            schemas[name]
                .fields()
                .iter()
                .find(|f| f.name().eq_ignore_ascii_case(wanted))
                .cloned()
        };
        let mut distinct = HashMap::new();
        let mut to_count = Vec::new();
        // Relationship type and endpoint columns, for the fan-out once counts are known.
        let mut endpoints = None;
        for (_, mapping) in config.iter_node_mappings() {
            if !mapping.label.eq_ignore_ascii_case(name) {
                continue;
            }
            if let Some(id) = column(&mapping.id_field) {
                distinct.insert(id.name().clone(), source.num_rows(name).await?);
            }
        }
        for (_, mapping) in config.iter_relationship_mappings() {
            if !mapping.relationship_type.eq_ignore_ascii_case(name) {
                continue;
            }
            let (Some(src), Some(dst)) = (
                column(&mapping.source_id_field),
                column(&mapping.target_id_field),
            ) else {
                continue;
            };
            endpoints = Some((
                mapping.relationship_type.to_lowercase(),
                src.name().clone(),
                dst.name().clone(),
            ));
            match csr.get(&mapping.relationship_type) {
                // Built from this same snapshot in this refresh, so its counts are current.
                Some(entry) => {
                    distinct.insert(src.name().clone(), entry.outgoing.num_sources());
                    distinct.insert(dst.name().clone(), entry.incoming.num_sources());
                }
                None => to_count.extend(
                    [src, dst]
                        .into_iter()
                        .filter(|f| is_supported_id_type(f.data_type()))
                        .map(|f| f.name().clone()),
                ),
            }
        }
        to_count.retain(|c| !distinct.contains_key(c));
        if !to_count.is_empty() {
            let table = name.to_lowercase();
            let counts = async {
                let df = ctx.table(table.as_str()).await?;
                let names: Vec<&str> = to_count.iter().map(String::as_str).collect();
                let mut stream = df.select_columns(&names)?.execute_stream().await?;
                let mut seen: Vec<HashSet<i64>> = vec![Default::default(); names.len()];
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    for (i, values) in seen.iter_mut().enumerate() {
                        let column =
                            arrow::compute::cast(batch.column(i), &arrow_schema::DataType::Int64)?;
                        let column = column
                            .as_any()
                            .downcast_ref::<arrow_array::Int64Array>()
                            .expect("cast to Int64");
                        values.extend(column.iter().flatten());
                    }
                }
                Ok::<_, datafusion::error::DataFusionError>(
                    seen.into_iter().map(|s| s.len()).collect::<Vec<_>>(),
                )
            }
            .await
            .map_err(|e| exec_error(format!("Failed to count join keys of {}: {}", name, e)))?;
            distinct.extend(to_count.into_iter().zip(counts));
        }
        let fan_out = match endpoints {
            Some((rel_type, src, dst)) if distinct.contains_key(&src) => {
                let edges = source.num_rows(name).await? as f64;
                let per = |c: &str| edges / distinct.get(c).copied().unwrap_or(1).max(1) as f64;
                Some((rel_type, per(&src), per(&dst)))
            }
            _ => None,
        };
        if !distinct.is_empty() {
            entries.insert(
                name.clone(),
                KeyStatsEntry {
                    source: source.clone(),
                    distinct,
                    fan_out,
                },
            );
        }
    }
    Ok(entries)
}

/// Build (or reuse) CSR adjacencies for every relationship type with integer endpoints.
///
/// An entry from `previous` is reused when its relationship dataset is the same
/// snapshot; node-table changes never invalidate it, since the CSR stores raw
/// target ids. Relationship types with non-integer endpoints get no entry and
/// queries over them use the join path.
async fn build_csr_entries(
    config: &GraphConfig,
    sources: &HashMap<String, EngineSource>,
    schemas: &HashMap<String, SchemaRef>,
    ctx: &SessionContext,
    previous: Option<&EngineState>,
) -> Result<HashMap<String, CsrEntry>> {
    let mut entries = HashMap::new();
    for (_, mapping) in config.iter_relationship_mappings() {
        let rel_type = &mapping.relationship_type;
        let Some((name, source)) = sources
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(rel_type))
        else {
            continue;
        };
        if let Some(old) = previous.and_then(|p| p.csr.get(rel_type)) {
            if old.source.same_data(source) {
                entries.insert(rel_type.clone(), old.clone());
                continue;
            }
        }
        let field = |column: &str| {
            schemas[name]
                .fields()
                .iter()
                .find(|f| f.name().eq_ignore_ascii_case(column))
                .cloned()
        };
        let (Some(src), Some(dst)) = (
            field(&mapping.source_id_field),
            field(&mapping.target_id_field),
        ) else {
            continue;
        };
        if !is_supported_id_type(src.data_type()) || !is_supported_id_type(dst.data_type()) {
            continue;
        }

        let previous_entry = previous.and_then(|p| p.csr.get(rel_type));
        if let Some(old) = previous_entry {
            if let Some(entry) = try_apply_csr_delta(old, source, src.name(), dst.name()).await {
                entries.insert(rel_type.clone(), entry);
                continue;
            }
        }

        let started = std::time::Instant::now();
        let table = name.to_lowercase();
        // Two projected scans of the endpoint columns, one per direction.
        let scan = || async {
            ctx.table(table.as_str())
                .await?
                .select_columns(&[src.name().as_str(), dst.name().as_str()])?
                .execute_stream()
                .await
                .map_err(|e| exec_error(format!("Failed to scan edges for CSR: {}", e)))
        };
        let outgoing = Adjacency::build(scan().await?, src.name(), dst.name()).await?;
        let incoming = Adjacency::build(scan().await?, dst.name(), src.name()).await?;
        entries.insert(
            rel_type.clone(),
            CsrEntry {
                source: source.clone(),
                outgoing: Arc::new(outgoing),
                incoming: Arc::new(incoming),
                build_ms: started.elapsed().as_secs_f64() * 1e3,
                update: if previous_entry.is_some() {
                    "rebuild"
                } else {
                    "build"
                },
            },
        );
    }
    Ok(entries)
}

/// Cypher query engine with a cached catalog for efficient multi-query execution.
///
/// The engine registers each dataset once and reuses the catalog and DataFusion
/// context across queries.
///
/// Lance datasets are read lazily from storage, pinned to the version they were
/// opened at: writes made later (by this or another process) are not visible until
/// [`refresh`](Self::refresh). Each dataset is pinned separately; versions across
/// tables are not a transactional multi-table snapshot. Arrow batches are held in
/// memory and never change; build a new engine to replace them.
///
/// Building the CSR (`use_csr`) is CPU-bound work inside an async function: in an
/// application with its own async runtime, construct and refresh the engine on a
/// blocking thread (for example `tokio::task::spawn_blocking` plus `block_on`).
pub struct CypherEngine {
    config: GraphConfig,
    state: RwLock<Arc<EngineState>>,
    use_csr: bool,
    target_partitions: Option<usize>,
    /// Held for a whole refresh, so a slower refresh can't swap in older data over a newer one.
    refresh_lock: futures::lock::Mutex<()>,
}

impl CypherEngine {
    /// Register `sources` (dataset name -> data) and build the snapshot.
    ///
    /// Lance datasets are not read here, except for the CSR, node-index and
    /// join-key statistics builds. With `use_csr`, relationship types with integer
    /// endpoints get CSR adjacencies and node labels with unique integer ids get a
    /// node index.
    ///
    /// # Errors
    /// [`GraphError::ConfigError`] if `sources` is empty or a configured id/endpoint
    /// column is missing; [`GraphError::ExecutionError`] if a dataset cannot be
    /// registered or scanned.
    pub async fn new(
        config: GraphConfig,
        sources: HashMap<String, EngineSource>,
        use_csr: bool,
    ) -> Result<Self> {
        Self::with_target_partitions(config, sources, use_csr, None).await
    }

    /// Like [`new`](Self::new), with a target for DataFusion query partitioning.
    ///
    /// `Some(n)` sets DataFusion's `target_partitions` for every query, kept across
    /// [`refresh`](Self::refresh); `None` keeps DataFusion's default (the number of
    /// cores). It is not a thread or memory limit. In-memory Arrow inputs are still
    /// sliced into one partition per core.
    ///
    /// # Errors
    /// As [`new`](Self::new), plus [`GraphError::ConfigError`] if `target_partitions`
    /// is outside `1..=1024`.
    pub async fn with_target_partitions(
        config: GraphConfig,
        sources: HashMap<String, EngineSource>,
        use_csr: bool,
        target_partitions: Option<usize>,
    ) -> Result<Self> {
        if sources.is_empty() {
            return Err(config_error("No input datasets provided".to_string()));
        }
        if let Some(partitions) = target_partitions {
            if !(1..=MAX_TARGET_PARTITIONS).contains(&partitions) {
                return Err(config_error(format!(
                    "target_partitions must be between 1 and {MAX_TARGET_PARTITIONS}"
                )));
            }
        }
        let state = build_engine_state(&config, sources, use_csr, target_partitions, None).await?;
        Ok(Self {
            config,
            state: RwLock::new(Arc::new(state)),
            use_csr,
            target_partitions,
            refresh_lock: Default::default(),
        })
    }

    /// The current snapshot. Cloning the `Arc` lets a query outlive a concurrent refresh.
    fn snapshot(&self) -> Arc<EngineState> {
        self.state
            .read()
            .expect("engine state lock poisoned")
            .clone()
    }

    /// Parse and configure a query, reusing this snapshot's plan template when
    /// `use_cache` is set and every parameter is a scalar.
    fn parse(
        &self,
        text: &str,
        params: HashMap<String, JsonValue>,
        state: &EngineState,
        use_cache: bool,
    ) -> Result<CypherQuery> {
        let parameters: HashMap<String, JsonValue> = params
            .into_iter()
            .map(|(name, value)| (name.to_lowercase(), value))
            .collect();
        let key = use_cache.then(|| parameter_types(&parameters)).flatten();
        if let Some(key) = &key {
            let mut plans = state.plans.lock().expect("plan cache lock poisoned");
            if let Some(index) = plans
                .iter()
                .position(|entry| entry.text == text && entry.parameter_types == *key)
            {
                let entry = plans.remove(index).expect("index from position");
                let query = entry.query.clone().with_parameters(parameters);
                plans.push_back(entry);
                return Ok(query);
            }
        }
        let query = CypherQuery::new(text)?
            .with_parameters(parameters)
            .with_config(self.config.clone())
            .with_fan_out(state.fan_out());
        let mut query = if self.use_csr {
            query.with_adjacency(state.adjacency_catalog())
        } else {
            query
        };
        if let Some(parameter_types) = key {
            // Some operators require literal values during planning. If a template
            // cannot be built, ordinary planning preserves their existing behavior.
            if query
                .prepare_cached_plan(state.catalog.clone())
                .unwrap_or(false)
            {
                let mut plans = state.plans.lock().expect("plan cache lock poisoned");
                plans
                    .retain(|entry| entry.text != text || entry.parameter_types != parameter_types);
                if plans.len() == PLAN_CACHE_ENTRIES {
                    plans.pop_front();
                }
                plans.push_back(CachedQuery {
                    text: text.to_string(),
                    parameter_types,
                    query: query.clone(),
                });
            }
        }
        Ok(query)
    }

    /// Execute a Cypher query and return the whole result as one batch.
    ///
    /// `params` gives values for `$name` parameters in the query. Use
    /// [`execute_stream`](Self::execute_stream) for large outputs.
    pub async fn execute(
        &self,
        query: &str,
        params: HashMap<String, JsonValue>,
    ) -> Result<RecordBatch> {
        let state = self.snapshot();
        self.parse(query, params, &state, true)?
            .execute_with_catalog_and_context(state.catalog.clone(), state.context.clone())
            .await
    }

    /// Execute a Cypher query and stream the result.
    ///
    /// Batches are computed as the stream is polled. Scans, filters and projections
    /// stream with bounded memory; joins, sorts and aggregations may still buffer.
    /// The stream keeps what it reads alive, so a concurrent `refresh()` does not
    /// affect it. Planning errors return here; execution errors arrive in the stream.
    /// Dropping the stream cancels the remaining work.
    pub async fn execute_stream(
        &self,
        query: &str,
        params: HashMap<String, JsonValue>,
    ) -> Result<SendableRecordBatchStream> {
        let state = self.snapshot();
        self.parse(query, params, &state, true)?
            .execute_stream_with_catalog_and_context(state.catalog.clone(), state.context.clone())
            .await
    }

    /// Return the query plan, including the physical plan with pushed-down scans.
    pub async fn explain(&self, query: &str, params: HashMap<String, JsonValue>) -> Result<String> {
        let state = self.snapshot();
        self.parse(query, params, &state, false)?
            .explain_with_catalog_and_context(state.catalog.clone(), state.context.clone())
            .await
    }

    /// Execute a Cypher query, then rerank its rows by vector similarity.
    pub async fn execute_with_vector_rerank(
        &self,
        query: &str,
        vector_search: &VectorSearch,
    ) -> Result<RecordBatch> {
        let state = self.snapshot();
        let candidates = self
            .parse(query, HashMap::new(), &state, false)?
            .execute_with_catalog_and_context(state.catalog.clone(), state.context.clone())
            .await?;
        vector_search.search(&candidates).await
    }

    /// Move every Lance dataset to its latest version.
    ///
    /// Datasets whose version and commit time are unchanged keep their open handle.
    /// The replacement is validated in full before it is swapped in; on error nothing
    /// changes and the previous snapshot stays usable. Queries already running finish
    /// on the snapshot they started with. Arrow inputs are unchanged. Concurrent calls
    /// run one at a time.
    pub async fn refresh(&self) -> Result<()> {
        let _refreshing = self.refresh_lock.lock().await;
        let current = self.snapshot();
        let mut sources = current.sources.clone();
        for source in sources.values_mut() {
            if let EngineSource::Lance {
                uri,
                storage_options,
                dataset,
            } = source
            {
                let latest = open_lance_dataset(uri, None, storage_options).await?;
                if !same_lance_snapshot(&latest, dataset) {
                    *dataset = Arc::new(latest);
                }
            }
        }
        let state = build_engine_state(
            &self.config,
            sources,
            self.use_csr,
            self.target_partitions,
            Some(&current),
        )
        .await?;
        *self.state.write().expect("engine state lock poisoned") = Arc::new(state);
        Ok(())
    }

    /// CSR adjacency statistics per relationship type (experimental, `use_csr`).
    ///
    /// An identical map after `refresh()` means the snapshot itself was unchanged.
    pub fn csr_stats(&self) -> HashMap<String, CsrStats> {
        self.snapshot()
            .csr
            .iter()
            .map(|(rel_type, entry)| {
                let stats = CsrStats {
                    edges: entry.outgoing.num_edges(),
                    delta_edges: entry.outgoing.delta_edges(),
                    tombstones: entry.outgoing.tombstones(),
                    update: entry.update,
                    version: entry.source.lance_version(),
                    build_ms: entry.build_ms,
                };
                (rel_type.clone(), stats)
            })
            .collect()
    }

    /// Node id -> row index statistics per node label (experimental, `use_csr`).
    ///
    /// A label is missing when its ids are not unique integers; its targets then use
    /// a join.
    pub fn node_index_stats(&self) -> HashMap<String, NodeIndexStats> {
        self.snapshot()
            .node_index
            .iter()
            .map(|(label, entry)| {
                let stats = NodeIndexStats {
                    rows: entry.rows.num_rows(),
                    version: entry.source.lance_version(),
                    build_ms: entry.build_ms,
                };
                (label.clone(), stats)
            })
            .collect()
    }

    /// Pinned version of each Lance dataset, keyed by dataset name.
    pub fn dataset_versions(&self) -> HashMap<String, u64> {
        self.snapshot()
            .sources
            .iter()
            .filter_map(|(name, source)| Some((name.clone(), source.lance_version()?)))
            .collect()
    }

    /// The graph configuration the engine was built with.
    pub fn config(&self) -> &GraphConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::{WriteMode, WriteParams};

    fn people_engine_sources() -> HashMap<String, EngineSource> {
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        HashMap::from([("Person".to_string(), EngineSource::Arrow(batch))])
    }

    fn person_config() -> GraphConfig {
        GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn target_partitions_survive_refresh_without_changing_other_defaults() {
        let defaults = SessionConfig::new();
        for target_partitions in [None, Some(1), Some(2)] {
            let engine = CypherEngine::with_target_partitions(
                person_config(),
                people_engine_sources(),
                false,
                target_partitions,
            )
            .await
            .unwrap();
            let before = engine.snapshot();
            engine.refresh().await.unwrap();
            for snapshot in [before, engine.snapshot()] {
                let state = snapshot.context.state();
                assert_eq!(
                    state.config().target_partitions(),
                    target_partitions.unwrap_or(defaults.target_partitions())
                );
                assert_eq!(
                    state.config().round_robin_repartition(),
                    defaults.round_robin_repartition()
                );
            }
        }
        for out_of_range in [0, MAX_TARGET_PARTITIONS + 1] {
            let result = CypherEngine::with_target_partitions(
                person_config(),
                people_engine_sources(),
                false,
                Some(out_of_range),
            )
            .await;
            assert!(matches!(result, Err(GraphError::ConfigError { .. })));
        }
    }

    #[tokio::test]
    async fn plan_cache_reuses_bounds_and_resets_on_refresh() {
        let engine = CypherEngine::new(person_config(), people_engine_sources(), false)
            .await
            .unwrap();
        let snapshot = engine.snapshot();
        let cached = || snapshot.plans.lock().unwrap().len();
        let text = "MATCH (p:Person) WHERE p.id = $value RETURN p.id";
        for value in [1, 2, 3] {
            let params = HashMap::from([("value".to_string(), JsonValue::from(value))]);
            engine.parse(text, params, &snapshot, true).unwrap();
            assert_eq!(cached(), 1);
            // The retained template keeps its first parameters: later calls looked it
            // up and rebound values rather than replacing the entry.
            assert_eq!(
                snapshot.plans.lock().unwrap()[0].query.parameters()["value"],
                JsonValue::from(1)
            );
        }
        // A different scalar type is a different template.
        for value in [JsonValue::Null, JsonValue::from(1.5)] {
            let params = HashMap::from([("value".to_string(), value)]);
            engine.parse(text, params, &snapshot, true).unwrap();
        }
        assert_eq!(cached(), 3);
        // Container parameters stay on the uncached path.
        let ids = HashMap::from([("ids".to_string(), serde_json::json!([1, 2]))]);
        engine
            .parse(
                "MATCH (p:Person) WHERE p.id IN $ids RETURN p.id",
                ids,
                &snapshot,
                true,
            )
            .unwrap();
        assert_eq!(cached(), 3);
        // Explain-style parses never cache.
        engine
            .parse(
                "MATCH (p:Person) RETURN p.id",
                HashMap::new(),
                &snapshot,
                false,
            )
            .unwrap();
        assert_eq!(cached(), 3);
        for limit in 0..130 {
            let text = format!("MATCH (p:Person) RETURN p.id AS id LIMIT {limit}");
            engine
                .parse(&text, HashMap::new(), &snapshot, true)
                .unwrap();
        }
        assert_eq!(cached(), PLAN_CACHE_ENTRIES);
        assert!(snapshot
            .plans
            .lock()
            .unwrap()
            .iter()
            .all(|e| e.text != text));

        // Cached and uncached plans return the same rows.
        let params = || HashMap::from([("value".to_string(), JsonValue::from(2))]);
        let first = engine.execute(text, params()).await.unwrap();
        let again = engine.execute(text, params()).await.unwrap();
        assert_eq!(first, again);
        assert_eq!(first.num_rows(), 1);

        // A refresh publishes a new snapshot with an empty cache; the old one is untouched.
        engine.refresh().await.unwrap();
        assert!(engine.snapshot().plans.lock().unwrap().is_empty());
        assert_eq!(cached(), PLAN_CACHE_ENTRIES);
    }

    async fn write_people(uri: &str, ids: Vec<i64>, mode: WriteMode) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))]).unwrap();
        let params = WriteParams {
            mode,
            ..Default::default()
        };
        Dataset::write(
            RecordBatchIterator::new([Ok(batch)], schema),
            uri,
            Some(params),
        )
        .await
        .unwrap();
    }

    /// A refresh that starts while another is in flight waits for it, then publishes
    /// the newest version: an older snapshot can never land after a newer one.
    #[tokio::test]
    async fn refresh_waits_for_an_in_flight_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("person.lance").display().to_string();
        write_people(&uri, vec![1], WriteMode::Create).await;
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();
        let source = EngineSource::open_lance(uri.clone(), None, HashMap::new())
            .await
            .unwrap();
        let sources = HashMap::from([("Person".to_string(), source)]);
        let engine = Arc::new(CypherEngine::new(config, sources, true).await.unwrap());

        // Stand in for refresh A, still building its replacement.
        let in_flight = engine.refresh_lock.lock().await;
        let b = tokio::spawn({
            let engine = engine.clone();
            async move { engine.refresh().await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!b.is_finished(), "refresh B ran while A was in flight");

        // A newer version lands before A finishes; B must publish it, not anything older.
        write_people(&uri, vec![2], WriteMode::Append).await;
        drop(in_flight);
        b.await.unwrap().unwrap();
        assert_eq!(engine.dataset_versions()["Person"], 2);
        assert_eq!(engine.node_index_stats()["Person"].rows, 2);
    }
}
