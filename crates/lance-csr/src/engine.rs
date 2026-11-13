// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! A Cypher engine that registers its datasets once and serves many queries.
//!
//! [`CypherEngine`] holds a snapshot of every input: the DataFusion context, the
//! catalog, join-key statistics and, with `use_csr`, the CSR adjacencies and node
//! indexes. Queries share the snapshot read-only; [`CypherEngine::checkout_latest`] builds a
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
use futures::{StreamExt, TryStreamExt};
use lance::datafusion::LanceTableProvider;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::Dataset;
use serde_json::Value as JsonValue;

use crate::config::GraphConfig;
use crate::csr_expand::{
    edge_changes, edge_changes_since, is_supported_id_type, Adjacency, AdjacencyCatalog,
    CountCsrDegrees, CsrQueryPlanner, EdgeChanges, EdgeRows, NodeRows, PruneCsrTargetColumns,
    PushCsrTargetFilters,
};
use crate::csr_store;
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
    /// `dataset` is pinned to one version; only `checkout_latest()` replaces it. `uri` and
    /// `storage_options` are kept so `checkout_latest()` can reopen the same location with the
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
    /// `storage_options` are kept so [`CypherEngine::checkout_latest`] can reopen the same
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

    /// A DataFusion table over this source. Lance is scanned lazily, with projection and
    /// filter pushdown; Arrow is sliced into one partition per core.
    pub fn table_provider(&self, name: &str) -> Result<Arc<dyn TableProvider>> {
        Ok(match self {
            // No _rowid/_rowaddr columns, so Lance and Arrow inputs expose the same schema.
            Self::Lance { dataset, .. } => {
                Arc::new(LanceTableProvider::new(dataset.clone(), false, false))
            }
            Self::Arrow(batch) => Arc::new(
                MemTable::try_new(batch.schema(), memtable_partitions(batch)).map_err(|e| {
                    exec_error(format!("Failed to create MemTable for {}: {}", name, e))
                })?,
            ),
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

    /// Rows addressable by the edge rows of an adjacency built from this source.
    fn edge_rows(&self) -> Arc<EdgeRows> {
        Arc::new(match self {
            Self::Lance { dataset, .. } => EdgeRows::from_lance(dataset.clone()),
            Self::Arrow(batch) => EdgeRows::from_batch(batch.clone()),
        })
    }

    /// `from` and `to` of every row, with row addresses for Lance; Arrow rows are
    /// numbered by position, so the batch is read as is, in order.
    async fn edge_stream(
        &self,
        from: &str,
        to: &str,
    ) -> Result<futures::stream::BoxStream<'static, Result<RecordBatch>>> {
        let scan_error =
            |e: lance::Error| exec_error(format!("Failed to scan edges for CSR: {}", e));
        Ok(match self {
            Self::Lance { dataset, .. } => {
                let mut scanner = dataset.scan();
                scanner.project(&[from, to]).map_err(scan_error)?;
                scanner.with_row_address();
                let stream = scanner.try_into_stream().await.map_err(scan_error)?;
                stream.map(move |b| b.map_err(scan_error)).boxed()
            }
            Self::Arrow(batch) => futures::stream::iter([Ok(batch.clone())]).boxed(),
        })
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
    /// Live edges outside the base CSR: appended since its last full build and not
    /// since removed. Lance's `num_unindexed_rows`, counted in edges.
    pub unindexed_edges: u64,
    /// How the last build, `checkout_latest()` or `optimize_csr()` produced it: `build` (first), `rebuild`
    /// (full), `loaded` (from a stored file, with any changes since applied as a
    /// delta), `delta` (changes applied) or `reused` (no edge change).
    pub update: &'static str,
    /// Relationship dataset version it reflects (`None` for Arrow input).
    pub version: Option<u64>,
    /// Relationship dataset version of the last full build (`None` for Arrow input).
    pub base_version: Option<u64>,
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
/// `checkout_latest()` builds a complete replacement and swaps it in; queries already running
/// keep the `Arc` they started with, so they finish on their original snapshot.
struct EngineState {
    sources: HashMap<String, EngineSource>,
    /// Unoptimized plan templates for this snapshot; `checkout_latest()` starts an empty cache.
    plans: Mutex<VecDeque<CachedQuery>>,
    /// SQL plan templates for this snapshot, under the same rules.
    sql_plans: Mutex<VecDeque<CachedSql>>,
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

/// An unoptimized SQL plan whose `$name` parameters are still placeholders.
struct CachedSql {
    text: String,
    parameter_types: Vec<(String, u8)>,
    plan: datafusion::logical_expr::LogicalPlan,
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

/// `text` configured for `state`: its graph, fan-out and, with `use_csr`, its CSR.
fn configured_query(
    config: &GraphConfig,
    use_csr: bool,
    state: &EngineState,
    text: &str,
) -> Result<CypherQuery> {
    let query = CypherQuery::new(text)?
        .with_config(config.clone())
        .with_fan_out(state.fan_out());
    Ok(if use_csr {
        query.with_adjacency(state.adjacency_catalog())
    } else {
        query
    })
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
    /// Rows of `source`, for relationship columns the adjacencies don't hold.
    edges: Arc<EdgeRows>,
    /// Version of the snapshot the base was fully built from.
    base_version: Option<u64>,
    /// Wall time of the last build or delta application.
    build_ms: f64,
    /// How the last build, `checkout_latest()` or optimize produced this entry: build, rebuild, loaded, delta or reused.
    update: &'static str,
}

/// Rebuild instead of applying a delta once pending changes (appended edges +
/// tombstones, accumulated since the last full build) would exceed this fraction
/// of the base. Lookups read the delta on every query, and a rebuild clears it.
///
/// This threshold also bounds the cost of `checkout_latest()`: each delta it applies
/// copies the whole accumulated delta (see the design note on
/// `crate::csr_expand::Adjacency`).
/// Raising it makes `checkout_latest()` slower as well as lookups.
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
            edges: source.edge_rows(),
            build_ms: elapsed(),
            update: "reused",
            ..old.clone()
        });
    }
    let (outgoing, incoming) = apply_csr_changes(&old.outgoing, &old.incoming, &changes)?;
    Some(CsrEntry {
        source: source.clone(),
        outgoing: Arc::new(outgoing),
        incoming: Arc::new(incoming),
        edges: source.edge_rows(),
        base_version: old.base_version,
        build_ms: elapsed(),
        update: "delta",
    })
}

/// `changes` applied to both directions, or `None` when the accumulated delta would
/// pass `CSR_REBUILD_FRACTION` of the base.
fn apply_csr_changes(
    outgoing: &Adjacency,
    incoming: &Adjacency,
    changes: &EdgeChanges,
) -> Option<(Adjacency, Adjacency)> {
    let too_large =
        |pending: u64| pending as f64 > CSR_REBUILD_FRACTION * outgoing.base_edges() as f64;
    if too_large(outgoing.delta_edges() + outgoing.tombstones() + changes.added.len() as u64) {
        return None;
    }
    // Removed rows are only counted by applying them; one direction is enough to decide.
    let new_outgoing = outgoing.with_changes(&changes.added, &changes.removed);
    if too_large(new_outgoing.delta_edges() + new_outgoing.tombstones()) {
        return None;
    }
    let flipped: Vec<_> = changes
        .added
        .iter()
        .map(|&(a, b, row)| (b, a, row))
        .collect();
    let new_incoming = incoming.with_changes(&flipped, &changes.removed);
    Some((new_outgoing, new_incoming))
}

/// The CSR stored for `source`, brought up to date with the changes since.
///
/// `None` when nothing usable is stored, the changes can't be computed, or they would
/// pass the rebuild threshold.
async fn load_stored_csr(source: &EngineSource, src: &str, dst: &str) -> Option<CsrEntry> {
    let EngineSource::Lance { dataset, .. } = source else {
        return None;
    };
    let started = std::time::Instant::now();
    let stored = csr_store::load(dataset, src, dst).await?;
    let changes = edge_changes_since(&stored.fragments, &stored.fields, dataset, src, dst)
        .await
        .ok()??;
    let (outgoing, incoming) = if changes.is_empty() {
        (stored.outgoing, stored.incoming)
    } else {
        apply_csr_changes(&stored.outgoing, &stored.incoming, &changes)?
    };
    Some(CsrEntry {
        source: source.clone(),
        outgoing: Arc::new(outgoing),
        incoming: Arc::new(incoming),
        edges: source.edge_rows(),
        base_version: Some(stored.version),
        build_ms: started.elapsed().as_secs_f64() * 1e3,
        update: "loaded",
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
            catalog.insert_edges(rel_type, entry.edges.clone());
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

/// Plan `sql` in `ctx` after rewriting its `GRAPH_TABLE` clauses; DDL, DML and statements
/// such as `SET` are rejected.
async fn plan_read_only_sql(
    ctx: &SessionContext,
    sql: &str,
) -> Result<datafusion::logical_expr::LogicalPlan> {
    use datafusion::execution::context::SQLOptions;

    let sql = crate::graph_table::rewrite_graph_table(sql)?;
    let plan = ctx
        .state()
        .create_logical_plan(&sql)
        .await
        .map_err(|e| plan_error(format!("SQL planning error: {e}")))?;
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
        .verify_plan(&plan)
        .map_err(|e| plan_error(e.to_string()))?;
    Ok(plan)
}

fn plan_error(message: String) -> GraphError {
    GraphError::PlanError {
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

/// Open the Lance dataset at `uri`, pinned to `version`, or at its latest version when
/// `version` is `None`. Storage options go to the object store, as in Lance itself.
pub async fn open_lance_dataset(
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
/// `rebuild_deltas` rebuilds every CSR of `previous` that carries a delta.
async fn build_engine_state(
    config: &GraphConfig,
    sources: HashMap<String, EngineSource>,
    options: &EngineOptions,
    previous: Option<&EngineState>,
    rebuild_deltas: bool,
) -> Result<EngineState> {
    let mut session_config = SessionConfig::new();
    if let Some(partitions) = options.target_partitions {
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
            .with_optimizer_rule(Arc::new(CountCsrDegrees))
            .with_optimizer_rule(Arc::new(PruneCsrTargetColumns))
            .build(),
    );
    let mut schemas = HashMap::new();
    let mut providers: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();

    for (name, source) in &sources {
        let provider = source.table_provider(name)?;
        schemas.insert(name.clone(), provider.schema());
        // Registered unwrapped first: the CSR and key-statistics builds below scan it.
        ctx.register_table(name.to_lowercase(), provider.clone())
            .map_err(|e| exec_error(format!("Failed to register table {}: {}", name, e)))?;
        providers.insert(name.clone(), provider);
    }

    validate_sources(config, &schemas)?;
    let (csr, node_index) = if options.use_csr {
        // Node indexes scan while stored CSRs decode on the blocking pool.
        futures::try_join!(
            build_csr_entries(
                config,
                &sources,
                &schemas,
                previous,
                rebuild_deltas,
                options.persist_csr,
            ),
            build_node_index(config, &sources, previous),
        )?
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
        sql_plans: Mutex::default(),
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
/// the same snapshot, so an unchanged `checkout_latest()` costs nothing.
// shortcut: the fallback scan reads every edge endpoint at construction and on each `checkout_latest()` that finds changes (about one CSR build's scan); switch to an approximate counter (HyperLogLog) or Lance column statistics if that shows up in startup time.
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
                // Built from this same snapshot in this `checkout_latest()`, so its counts are current.
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
///
/// With `persist`, a Lance relationship dataset's stored CSR is loaded instead of the
/// first build when it can be brought up to date, and every full build is stored.
/// Later rebuilds don't look for a file: the stored base is the one the engine already
/// had, whose delta just grew too large.
async fn build_csr_entries(
    config: &GraphConfig,
    sources: &HashMap<String, EngineSource>,
    schemas: &HashMap<String, SchemaRef>,
    previous: Option<&EngineState>,
    rebuild_deltas: bool,
    persist: bool,
) -> Result<HashMap<String, CsrEntry>> {
    // Types run concurrently so stored loads, which decode on the blocking pool, overlap.
    // Deltas and builds run on this task, where overlapping gains nothing and would blur
    // each one's `build_ms`, so they take turns.
    let one_at_a_time = futures::lock::Mutex::new(());
    let one_at_a_time = &one_at_a_time;
    // Collected first: a lazy `map` closure over borrows makes `checkout_latest()` lose `Send`.
    let entries: Vec<_> = config
        .iter_relationship_mappings()
        .map(|(_, mapping)| async move {
            let rel_type = &mapping.relationship_type;
            let Some((name, source)) = sources
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(rel_type))
            else {
                return Ok(None);
            };
            if let Some(old) = previous.and_then(|p| p.csr.get(rel_type)) {
                let delta = old.outgoing.delta_edges() + old.outgoing.tombstones() > 0;
                if old.source.same_data(source) && !(rebuild_deltas && delta) {
                    return Ok(Some((rel_type.clone(), old.clone())));
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
                return Ok(None);
            };
            if !is_supported_id_type(src.data_type()) || !is_supported_id_type(dst.data_type()) {
                return Ok(None);
            }

            let previous_entry = previous.and_then(|p| p.csr.get(rel_type));
            if persist && previous_entry.is_none() {
                if let Some(entry) = load_stored_csr(source, src.name(), dst.name()).await {
                    return Ok(Some((rel_type.clone(), entry)));
                }
            }

            let _turn = one_at_a_time.lock().await;
            if let Some(old) = previous_entry.filter(|_| !rebuild_deltas) {
                if let Some(entry) = try_apply_csr_delta(old, source, src.name(), dst.name()).await
                {
                    return Ok(Some((rel_type.clone(), entry)));
                }
            }

            // The one full build: the first, a `checkout_latest()` whose delta would pass the threshold,
            // and optimize_csr() (`rebuild_deltas`) all end here.
            let started = std::time::Instant::now();
            // Two projected scans of the endpoint columns, one per direction.
            let scan = || source.edge_stream(src.name(), dst.name());
            let outgoing = Adjacency::build(scan().await?, src.name(), dst.name()).await?;
            let incoming = Adjacency::build(scan().await?, dst.name(), src.name()).await?;
            let build_ms = started.elapsed().as_secs_f64() * 1e3;
            if let (true, EngineSource::Lance { dataset, .. }) = (persist, source) {
                // A failed write (read-only dataset or credentials) only costs the next start a build.
                if let Err(e) =
                    csr_store::save(dataset, src.name(), dst.name(), &outgoing, &incoming).await
                {
                    log::warn!("Could not store the CSR of {rel_type}: {e}");
                }
            }
            Ok::<_, GraphError>(Some((
                rel_type.clone(),
                CsrEntry {
                    source: source.clone(),
                    outgoing: Arc::new(outgoing),
                    incoming: Arc::new(incoming),
                    edges: source.edge_rows(),
                    base_version: source.lance_version(),
                    build_ms,
                    update: if previous_entry.is_some() {
                        "rebuild"
                    } else {
                        "build"
                    },
                },
            )))
        })
        .collect();
    let parallelism = std::thread::available_parallelism().map_or(1, |n| n.get());
    let entries: Vec<_> = futures::stream::iter(entries)
        .buffer_unordered(parallelism)
        .try_collect()
        .await?;
    Ok(entries.into_iter().flatten().collect())
}

/// Cypher query engine with a cached catalog for efficient multi-query execution.
///
/// The engine registers each dataset once and reuses the catalog and DataFusion
/// context across queries.
///
/// Lance datasets are read lazily from storage, pinned to the version they were
/// opened at: writes made later (by this or another process) are not visible until
/// [`checkout_latest`](Self::checkout_latest). Each dataset is pinned separately; versions across
/// tables are not a transactional multi-table snapshot. Arrow batches are held in
/// memory and never change; build a new engine to replace them.
///
/// Building the CSR (`use_csr`) is CPU-bound work inside an async function: in an
/// application with its own async runtime, construct the engine and call `checkout_latest()` on a
/// blocking thread (for example `tokio::task::spawn_blocking` plus `block_on`).
/// Options for [`CypherEngine::with_options`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EngineOptions {
    /// Build CSR adjacencies and node indexes; see [`CypherEngine::new`].
    pub use_csr: bool,
    /// DataFusion `target_partitions`; see [`CypherEngine::with_target_partitions`].
    pub target_partitions: Option<usize>,
    /// Store each fully built CSR inside its Lance relationship dataset, at
    /// `_lance_csr/<version>.arrow`, and load it instead of building when an engine
    /// starts. Changes since the stored version apply as a delta, under
    /// the same 25% threshold as `checkout_latest()`. Ignored without `use_csr` and for Arrow
    /// input. A failed write is logged and doesn't fail the engine.
    pub persist_csr: bool,
}

pub struct CypherEngine {
    config: GraphConfig,
    state: RwLock<Arc<EngineState>>,
    options: EngineOptions,
    /// Held for a whole update of the snapshot, so a slower `checkout_latest()` can't swap in older
    /// data over a newer one.
    update_lock: futures::lock::Mutex<()>,
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
    /// [`checkout_latest`](Self::checkout_latest); `None` keeps DataFusion's default (the number of
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
        let options = EngineOptions {
            use_csr,
            target_partitions,
            ..Default::default()
        };
        Self::with_options(config, sources, options).await
    }

    /// Like [`new`](Self::new), with every [`EngineOptions`] setting.
    ///
    /// # Errors
    /// As [`with_target_partitions`](Self::with_target_partitions).
    pub async fn with_options(
        config: GraphConfig,
        sources: HashMap<String, EngineSource>,
        options: EngineOptions,
    ) -> Result<Self> {
        if sources.is_empty() {
            return Err(config_error("No input datasets provided".to_string()));
        }
        if let Some(partitions) = options.target_partitions {
            if !(1..=MAX_TARGET_PARTITIONS).contains(&partitions) {
                return Err(config_error(format!(
                    "target_partitions must be between 1 and {MAX_TARGET_PARTITIONS}"
                )));
            }
        }
        let state = build_engine_state(&config, sources, &options, None, false).await?;
        Ok(Self {
            config,
            state: RwLock::new(Arc::new(state)),
            options,
            update_lock: Default::default(),
        })
    }

    /// The current snapshot. Cloning the `Arc` lets a query outlive a concurrent `checkout_latest()`.
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
        let mut query = configured_query(&self.config, self.options.use_csr, state, text)?
            .with_parameters(parameters);
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
    /// The stream keeps what it reads alive, so a concurrent `checkout_latest()` does not
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

    /// Run `sql` on this snapshot; see [`SqlEngine::execute`].
    async fn sql(&self, sql: &str, params: HashMap<String, JsonValue>) -> Result<RecordBatch> {
        crate::query::collect_batch(self.sql_dataframe(sql, params, true).await?).await
    }

    /// See [`SqlEngine::explain`].
    async fn explain_sql(&self, sql: &str, params: HashMap<String, JsonValue>) -> Result<String> {
        let df = self.sql_dataframe(sql, params, false).await?;
        let logical = df
            .clone()
            .into_optimized_plan()
            .map_err(|e| plan_error(format!("Failed to optimize SQL plan: {e}")))?;
        let physical = df
            .create_physical_plan()
            .await
            .map_err(|e| plan_error(format!("Failed to create physical plan: {e}")))?;
        Ok(format!(
            "== Logical Plan ==\n{}\n\n== Physical Plan ==\n{}",
            logical.display_indent(),
            datafusion::physical_plan::displayable(physical.as_ref()).indent(true),
        ))
    }

    /// Plan `sql`, or reuse this snapshot's template for it, then bind `params`.
    ///
    /// A template is the unoptimized plan with every `$name` still a placeholder: SQL's own,
    /// and those of `GRAPH_TABLE` patterns, which are planned as Cypher templates. It is
    /// keyed like the Cypher cache, and skipped when any pattern can't be a template.
    async fn sql_dataframe(
        &self,
        sql: &str,
        params: HashMap<String, JsonValue>,
        use_cache: bool,
    ) -> Result<datafusion::dataframe::DataFrame> {
        use std::sync::atomic::{AtomicBool, Ordering};

        let state = self.snapshot();
        let params: HashMap<String, JsonValue> = params
            .into_iter()
            .map(|(name, value)| (name.to_lowercase(), value))
            .collect();
        let key = use_cache.then(|| parameter_types(&params)).flatten();
        // A copy, so the function (which holds the snapshot) isn't registered in it.
        let ctx = SessionContext::new_with_state(state.context.state());
        let cached = key.as_ref().and_then(|key| {
            let mut plans = state.sql_plans.lock().expect("plan cache lock poisoned");
            let index = plans
                .iter()
                .position(|entry| entry.text == sql && entry.parameter_types == *key)?;
            let entry = plans.remove(index).expect("index from position");
            let plan = entry.plan.clone();
            plans.push_back(entry);
            Some(plan)
        });
        let plan = match cached {
            Some(plan) => plan,
            None => {
                let templated = Arc::new(AtomicBool::new(key.is_some()));
                self.register_graph_table(&ctx, &state, &params, templated.clone());
                let plan = plan_read_only_sql(&ctx, sql).await?;
                if let Some(parameter_types) = key.filter(|_| templated.load(Ordering::Relaxed)) {
                    let mut plans = state.sql_plans.lock().expect("plan cache lock poisoned");
                    if plans.len() == PLAN_CACHE_ENTRIES {
                        plans.pop_front();
                    }
                    plans.push_back(CachedSql {
                        text: sql.to_string(),
                        parameter_types,
                        plan: plan.clone(),
                    });
                }
                plan
            }
        };
        let plan = match crate::query::scalar_values(&params) {
            Some(values) => plan
                .with_param_values(values)
                .map_err(|e| plan_error(format!("Failed to bind query parameters: {e}")))?,
            _ => plan,
        };
        Ok(crate::query::planned_dataframe(ctx, plan))
    }

    /// Register `graph_table` in `ctx`. While `templated` is set, patterns are planned as
    /// templates; one that can't be clears it and is planned with `params` as values.
    fn register_graph_table(
        &self,
        ctx: &SessionContext,
        state: &Arc<EngineState>,
        params: &HashMap<String, JsonValue>,
        templated: Arc<std::sync::atomic::AtomicBool>,
    ) {
        use crate::graph_table::{GraphTableFunction, GRAPH_TABLE_FUNCTION};
        use std::sync::atomic::Ordering;

        let (config, use_csr) = (self.config.clone(), self.options.use_csr);
        let (state, params) = (state.clone(), params.clone());
        ctx.register_udtf(
            GRAPH_TABLE_FUNCTION,
            Arc::new(GraphTableFunction::new(move |text| {
                if config.iter_node_mappings().len() == 0
                    && config.iter_relationship_mappings().len() == 0
                {
                    return Err(config_error(
                        "GRAPH_TABLE needs a graph: create the SqlEngine with a GraphConfig"
                            .to_string(),
                    ));
                }
                let mut query = configured_query(&config, use_csr, &state, text)?
                    .with_parameters(params.clone());
                if templated.load(Ordering::Relaxed)
                    && query
                        .prepare_cached_plan(state.catalog.clone())
                        .unwrap_or(false)
                {
                    if let Some(plan) = query.template_plan() {
                        return Ok(plan);
                    }
                }
                templated.store(false, Ordering::Relaxed);
                query.datafusion_plan(state.catalog.clone())
            })),
        );
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

    /// Move every Lance dataset to its latest version: the engine's counterpart of
    /// Lance's `Dataset::checkout_latest()`.
    ///
    /// Datasets whose version and commit time are unchanged keep their open handle.
    /// The replacement is validated in full before it is swapped in; on error nothing
    /// changes and the previous snapshot stays usable. Queries already running finish
    /// on the snapshot they started with. Arrow inputs are unchanged. Concurrent calls
    /// run one at a time.
    pub async fn checkout_latest(&self) -> Result<()> {
        let _updating = self.update_lock.lock().await;
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
        let state =
            build_engine_state(&self.config, sources, &self.options, Some(&current), false).await?;
        *self.state.write().expect("engine state lock poisoned") = Arc::new(state);
        Ok(())
    }

    /// Rebuild each CSR that carries a delta, at the snapshot the engine is pinned to: the
    /// CSR's counterpart of Lance's `optimize_indices()`.
    ///
    /// `checkout_latest()` applies changes as a delta over the base CSR and rebuilds on its own
    /// only once they pass 25% of the base. This rebuilds every CSR that carries a delta
    /// now, so lookups stop carrying it; one without a delta, and everything else, is
    /// reused. Results never change. The replacement is validated in full before it is
    /// swapped in, as for `checkout_latest()`, and the two run one at a time.
    pub async fn optimize_csr(&self) -> Result<()> {
        let _updating = self.update_lock.lock().await;
        let current = self.snapshot();
        let state = build_engine_state(
            &self.config,
            current.sources.clone(),
            &self.options,
            Some(&current),
            true,
        )
        .await?;
        *self.state.write().expect("engine state lock poisoned") = Arc::new(state);
        Ok(())
    }

    /// Delete the CSR files stored for this engine's Lance relationship datasets
    /// (see [`EngineOptions::persist_csr`]), returning how many files were deleted.
    ///
    /// The engine keeps the CSR it has in memory, so queries are unaffected. An
    /// engine with `persist_csr` stores a new file at its next full build; create
    /// engines without it to stay in memory only. Runs one at a time with `checkout_latest()`
    /// and `optimize_csr()`.
    pub async fn delete_stored_csr(&self) -> Result<usize> {
        let _updating = self.update_lock.lock().await;
        let current = self.snapshot();
        let mut deleted = 0;
        for (_, mapping) in self.config.iter_relationship_mappings() {
            let source = current
                .sources
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(&mapping.relationship_type));
            if let Some((_, EngineSource::Lance { dataset, .. })) = source {
                deleted += csr_store::delete_all(dataset).await?;
            }
        }
        Ok(deleted)
    }

    /// CSR adjacency statistics per relationship type (experimental, `use_csr`).
    ///
    /// An identical map after `checkout_latest()` means the snapshot itself was unchanged.
    pub fn csr_stats(&self) -> HashMap<String, CsrStats> {
        self.snapshot()
            .csr
            .iter()
            .map(|(rel_type, entry)| {
                let stats = CsrStats {
                    edges: entry.outgoing.num_edges(),
                    delta_edges: entry.outgoing.delta_edges(),
                    tombstones: entry.outgoing.tombstones(),
                    unindexed_edges: entry.outgoing.unindexed_edges(),
                    update: entry.update,
                    version: entry.source.lance_version(),
                    base_version: entry.base_version,
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

/// SQL over a set of datasets, with graph patterns when it has a graph.
///
/// Datasets are tables under their lower-cased names; Lance datasets are scanned in place
/// at their pinned version, with column and filter pushdown. With a [`GraphConfig`], a
/// `GRAPH_TABLE (MATCH ... COLUMNS (...))` clause (see [`crate::graph_table`]) embeds a
/// Cypher pattern, whose hops use the CSR when the engine has one.
///
/// It runs on a [`CypherEngine`]'s snapshot. [`from_engine`](Self::from_engine) shares an
/// existing engine's, so the datasets, CSR and statistics are built once for both, and
/// [`checkout_latest`](Self::checkout_latest) on either moves both.
#[derive(Clone)]
pub struct SqlEngine {
    engine: Arc<CypherEngine>,
}

impl SqlEngine {
    /// An engine over `sources` with no graph: plain SQL only.
    ///
    /// # Errors
    /// As [`CypherEngine::new`].
    pub async fn new(sources: HashMap<String, EngineSource>) -> Result<Self> {
        Self::with_graph(GraphConfig::default(), sources, EngineOptions::default()).await
    }

    /// An engine over `sources` with `config` as its graph, built as
    /// [`CypherEngine::with_options`] builds one.
    ///
    /// # Errors
    /// As [`CypherEngine::with_options`].
    pub async fn with_graph(
        config: GraphConfig,
        sources: HashMap<String, EngineSource>,
        options: EngineOptions,
    ) -> Result<Self> {
        let engine = CypherEngine::with_options(config, sources, options).await?;
        Ok(Self::from_engine(Arc::new(engine)))
    }

    /// SQL over `engine`'s snapshot, sharing its datasets, CSR and statistics.
    pub fn from_engine(engine: Arc<CypherEngine>) -> Self {
        Self { engine }
    }

    /// Run a SQL query and return the whole result as one batch.
    ///
    /// `params` gives values for `$name` parameters, in the SQL and in `GRAPH_TABLE`
    /// patterns alike. Plans are cached per snapshot like Cypher templates: keyed by the
    /// query text and the parameters' names and scalar types, and bound on every call.
    /// Container parameters, and patterns Cypher can't plan as templates, are planned
    /// on every call.
    ///
    /// # Errors
    /// [`GraphError::PlanError`] for invalid SQL, a missing parameter, or a query that
    /// would write (DDL, DML, `SET`); [`GraphError::ConfigError`] for `GRAPH_TABLE`
    /// without a graph; [`GraphError::ExecutionError`] if execution fails.
    pub async fn execute(
        &self,
        sql: &str,
        params: HashMap<String, JsonValue>,
    ) -> Result<RecordBatch> {
        self.engine.sql(sql, params).await
    }

    /// The optimized logical and physical plans of a query, always planned afresh.
    pub async fn explain(&self, sql: &str, params: HashMap<String, JsonValue>) -> Result<String> {
        self.engine.explain_sql(sql, params).await
    }

    /// Move to the latest version of every Lance dataset; see [`CypherEngine::checkout_latest`].
    pub async fn checkout_latest(&self) -> Result<()> {
        self.engine.checkout_latest().await
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
    async fn target_partitions_survive_checkout_latest_without_changing_other_defaults() {
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
            engine.checkout_latest().await.unwrap();
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
    async fn plan_cache_reuses_bounds_and_resets_on_checkout_latest() {
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

        // `checkout_latest()` publishes a new snapshot with an empty cache; the old one is untouched.
        engine.checkout_latest().await.unwrap();
        assert!(engine.snapshot().plans.lock().unwrap().is_empty());
        assert_eq!(cached(), PLAN_CACHE_ENTRIES);
    }

    #[tokio::test]
    async fn execution_deadline_stops_nested_loop_joins() {
        let engine = CypherEngine::new(person_config(), people_engine_sources(), false)
            .await
            .unwrap();
        // A non-equality join plans as a nested-loop join, whose filter checks the deadline.
        let query = "MATCH (a:Person), (b:Person) WHERE a.id < b.id RETURN count(*) AS n";
        let expired = crate::query::with_execution_deadline(
            Some(tokio::time::Instant::now()),
            engine.execute(query, HashMap::new()),
        )
        .await;
        let message = expired.unwrap_err().to_string();
        assert!(message.contains("Query timeout exceeded"), "{message}");
        // No deadline, or a distant one, leaves the same query untouched.
        let later = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        for deadline in [None, Some(later)] {
            let batch = crate::query::with_execution_deadline(
                deadline,
                engine.execute(query, HashMap::new()),
            )
            .await
            .unwrap();
            assert_eq!(batch.num_rows(), 1);
        }
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

    /// A `checkout_latest()` that starts while another is in flight waits for it, then publishes
    /// the newest version: an older snapshot can never land after a newer one.
    #[tokio::test]
    async fn checkout_latest_waits_for_an_in_flight_checkout() {
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

        // Stand in for `checkout_latest()` A, still building its replacement.
        let in_flight = engine.update_lock.lock().await;
        let b = tokio::spawn({
            let engine = engine.clone();
            async move { engine.checkout_latest().await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!b.is_finished(), "checkout B ran while A was in flight");

        // A newer version lands before A finishes; B must publish it, not anything older.
        write_people(&uri, vec![2], WriteMode::Append).await;
        drop(in_flight);
        b.await.unwrap().unwrap();
        assert_eq!(engine.dataset_versions()["Person"], 2);
        assert_eq!(engine.node_index_stats()["Person"].rows, 2);
    }

    async fn write_knows(uri: &str, edges: &[(i64, i64)], mode: WriteMode) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int64, false),
            Field::new("dst", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(edges.iter().map(|e| e.0))),
                Arc::new(Int64Array::from_iter_values(edges.iter().map(|e| e.1))),
            ],
        )
        .unwrap();
        let params = WriteParams {
            mode,
            max_rows_per_file: 40,
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

    /// Every `(a, b)` reachable in two hops, sorted, from an engine over `uri`.
    async fn two_hops(uri: &str, persist_csr: bool) -> (Vec<(i64, i64)>, CsrStats) {
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table("KNOWS", "Person", "Person", "src", "dst")
            .build()
            .unwrap();
        let people = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from_iter_values(0..60)) as arrow_array::ArrayRef,
        )])
        .unwrap();
        let knows = EngineSource::open_lance(uri, None, HashMap::new())
            .await
            .unwrap();
        let sources = HashMap::from([
            ("Person".to_string(), EngineSource::Arrow(people)),
            ("KNOWS".to_string(), knows),
        ]);
        let options = EngineOptions {
            use_csr: true,
            persist_csr,
            ..Default::default()
        };
        let engine = CypherEngine::with_options(config, sources, options)
            .await
            .unwrap();
        let batch = engine
            .execute(
                "MATCH (a:Person)-[:KNOWS]->(:Person)<-[:KNOWS]-(b:Person) \
                 RETURN a.id AS a, b.id AS b",
                HashMap::new(),
            )
            .await
            .unwrap();
        let column = |i: usize| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone()
        };
        let (a, b) = (column(0), column(1));
        let mut rows: Vec<_> = a
            .values()
            .iter()
            .copied()
            .zip(b.values().iter().copied())
            .collect();
        rows.sort_unstable();
        (rows, engine.csr_stats()["KNOWS"].clone())
    }

    fn stored_files(uri: &str) -> Vec<String> {
        let mut names: Vec<String> =
            std::fs::read_dir(std::path::Path::new(uri).join("_lance_csr"))
                .map(|entries| {
                    entries
                        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                        .collect()
                })
                .unwrap_or_default();
        names.sort();
        names
    }

    /// A stored CSR is loaded in place of a build, brought up to date with later
    /// appends and deletes, ignored when corrupt, and replaced by `optimize_csr()`;
    /// every answer matches an engine that builds from scratch.
    #[tokio::test]
    async fn persisted_csr_loads_and_catches_up() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("knows.lance").display().to_string();
        // Duplicate edges and a self-loop, over three fragments.
        let mut edges: Vec<(i64, i64)> = (0..100).map(|i| (i % 50, (i * 7) % 60)).collect();
        edges.extend([(3, 3), (3, 21), (3, 21)]);
        write_knows(&uri, &edges, WriteMode::Create).await;

        let (expected, stats) = two_hops(&uri, false).await;
        assert_eq!(stats.update, "build");
        assert!(
            stored_files(&uri).is_empty(),
            "nothing is stored without persist_csr"
        );

        let (rows, stats) = two_hops(&uri, true).await;
        assert_eq!((rows == expected, stats.update), (true, "build"));
        assert_eq!(stored_files(&uri), ["1.arrow"]);

        let (rows, stats) = two_hops(&uri, true).await;
        assert_eq!((rows == expected, stats.update), (true, "loaded"));
        assert_eq!((stats.edges, stats.base_version), (103, Some(1)));

        // An append and a delete since the stored version become a delta.
        write_knows(&uri, &[(1, 2), (2, 1), (40, 3)], WriteMode::Append).await;
        let mut ds = Dataset::open(&uri).await.unwrap();
        ds.delete("src = 3 AND dst = 21").await.unwrap();
        let (expected, _) = two_hops(&uri, false).await;
        let (rows, stats) = two_hops(&uri, true).await;
        assert_eq!((rows == expected, stats.update), (true, "loaded"));
        assert_eq!(
            (
                stats.delta_edges,
                stats.tombstones,
                stats.version,
                stats.base_version
            ),
            (3, 3, Some(3), Some(1))
        );
        assert_eq!(
            stored_files(&uri),
            ["1.arrow"],
            "a delta doesn't store a new base"
        );

        // A corrupt file is ignored: the engine builds and stores the current version.
        std::fs::write(
            dir.path().join("knows.lance/_lance_csr/1.arrow"),
            b"not arrow",
        )
        .unwrap();
        let (rows, stats) = two_hops(&uri, true).await;
        assert_eq!((rows == expected, stats.update), (true, "build"));
        assert_eq!(stored_files(&uri), ["3.arrow"], "older files are pruned");

        // optimize_csr() stores the rebuilt base of a CSR loaded with a delta.
        write_knows(&uri, &[(5, 6)], WriteMode::Append).await;
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table("KNOWS", "Person", "Person", "src", "dst")
            .build()
            .unwrap();
        let knows = EngineSource::open_lance(&uri, None, HashMap::new())
            .await
            .unwrap();
        let options = EngineOptions {
            use_csr: true,
            persist_csr: true,
            ..Default::default()
        };
        let engine = CypherEngine::with_options(
            config,
            HashMap::from([("KNOWS".to_string(), knows)]),
            options,
        )
        .await
        .unwrap();
        assert_eq!(engine.csr_stats()["KNOWS"].delta_edges, 1);
        engine.optimize_csr().await.unwrap();
        let stats = &engine.csr_stats()["KNOWS"];
        assert_eq!((stats.update, stats.base_version), ("rebuild", Some(4)));
        assert_eq!(stored_files(&uri), ["4.arrow"]);
        let (rows, stats) = two_hops(&uri, true).await;
        assert_eq!(stats.update, "loaded");
        assert_eq!(rows, two_hops(&uri, false).await.0);

        // Deleting the stored files leaves the engine's CSR in place.
        assert_eq!(engine.delete_stored_csr().await.unwrap(), 1);
        assert!(stored_files(&uri).is_empty());
        assert!(!dir.path().join("knows.lance/_lance_csr").exists());
        assert_eq!(engine.csr_stats()["KNOWS"].edges, 104);
        assert_eq!(engine.delete_stored_csr().await.unwrap(), 0);
        assert_eq!(two_hops(&uri, true).await.1.update, "build");
    }

    /// An engine over Lance `Person(id)` and `KNOWS(src, dst)` datasets in `dir`.
    async fn lance_graph_engine(dir: &std::path::Path, use_csr: bool) -> CypherEngine {
        let (people, knows) = (
            dir.join("person").display().to_string(),
            dir.join("knows").display().to_string(),
        );
        write_people(&people, (0..10).collect(), WriteMode::Create).await;
        let edges: Vec<(i64, i64)> = (0..10)
            .flat_map(|i| [(i, (i + 1) % 10), (i, (i + 3) % 10)])
            .collect();
        write_knows(&knows, &edges, WriteMode::Create).await;
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table("KNOWS", "Person", "Person", "src", "dst")
            .build()
            .unwrap();
        let sources = HashMap::from([
            (
                "Person".to_string(),
                EngineSource::open_lance(&people, None, HashMap::new())
                    .await
                    .unwrap(),
            ),
            (
                "KNOWS".to_string(),
                EngineSource::open_lance(&knows, None, HashMap::new())
                    .await
                    .unwrap(),
            ),
        ]);
        CypherEngine::new(config, sources, use_csr).await.unwrap()
    }

    fn int_column(batch: &RecordBatch, name: &str) -> Vec<i64> {
        let column = batch.column_by_name(name).unwrap();
        let column = arrow::compute::cast(column, &DataType::Int64).unwrap();
        column
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn params(values: &[(&str, JsonValue)]) -> HashMap<String, JsonValue> {
        values
            .iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect()
    }

    /// GRAPH_TABLE hops run on the CSR inside SQL, and match both Cypher and the join path.
    #[tokio::test]
    async fn sql_graph_table_uses_csr_and_matches_cypher() {
        let dir = tempfile::tempdir().unwrap();
        let csr = Arc::new(lance_graph_engine(dir.path(), true).await);
        let joins = Arc::new(lance_graph_engine(&dir.path().join("joins"), false).await);
        let sql = "SELECT g.b, count(*) AS n FROM GRAPH_TABLE (
                       MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a.id < 5
                       COLUMNS (b.id AS b)
                   ) g JOIN person p ON p.id = g.b
                   GROUP BY g.b ORDER BY g.b";
        let cypher = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a.id < 5
                      RETURN b.id AS b, count(*) AS n ORDER BY b";
        let expected = csr.execute(cypher, HashMap::new()).await.unwrap();
        for engine in [&csr, &joins] {
            let result = SqlEngine::from_engine(engine.clone())
                .execute(sql, HashMap::new())
                .await
                .unwrap();
            assert_eq!(int_column(&result, "b"), int_column(&expected, "b"));
            assert_eq!(int_column(&result, "n"), int_column(&expected, "n"));
        }
        let explain = |engine: &Arc<CypherEngine>| {
            let engine = SqlEngine::from_engine(engine.clone());
            async move { engine.explain(sql, HashMap::new()).await.unwrap() }
        };
        assert!(explain(&csr).await.contains("CsrExpandExec"));
        assert!(!explain(&joins).await.contains("CsrExpand"));

        // The direct function call is the same plan.
        let direct =
            "SELECT * FROM graph_table('MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.id AS b')";
        let sql_engine = SqlEngine::from_engine(csr);
        assert_eq!(
            sql_engine
                .execute(direct, HashMap::new())
                .await
                .unwrap()
                .num_rows(),
            20
        );
    }

    /// Plain SQL scans Lance datasets with pushdown, only reads, and needs no graph.
    #[tokio::test]
    async fn sql_scans_lance_lazily_and_rejects_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("knows").display().to_string();
        write_knows(&path, &[(3, 4), (3, 6), (1, 2)], WriteMode::Create).await;
        let knows = EngineSource::open_lance(&path, None, HashMap::new())
            .await
            .unwrap();
        let engine = SqlEngine::new(HashMap::from([("KNOWS".to_string(), knows)]))
            .await
            .unwrap();
        let sql = "SELECT dst FROM knows WHERE src = 3 ORDER BY dst";
        let result = engine.execute(sql, HashMap::new()).await.unwrap();
        assert_eq!(int_column(&result, "dst"), vec![4, 6]);
        let plan = engine.explain(sql, HashMap::new()).await.unwrap();
        assert!(plan.contains("full_filter=src = Int64(3)"), "{plan}");
        for query in [
            "CREATE TABLE t AS SELECT 1",
            "COPY knows TO '/tmp/knows.parquet'",
            "INSERT INTO knows VALUES (9, 9)",
            "SET datafusion.execution.batch_size = 1",
            "SELECT * FROM GRAPH_TABLE (MATCH (a:Person) COLUMNS (a.id))",
        ] {
            assert!(
                engine.execute(query, HashMap::new()).await.is_err(),
                "{query}"
            );
        }
    }

    /// One template per query text and parameter types, bound on every call, in SQL
    /// and in GRAPH_TABLE alike; container values and checkout_latest skip or reset it.
    #[tokio::test]
    async fn sql_parameters_bind_into_cached_templates() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(lance_graph_engine(dir.path(), true).await);
        let sql_engine = SqlEngine::from_engine(engine.clone());
        let cached = || engine.snapshot().sql_plans.lock().unwrap().len();
        let sql = "SELECT g.b FROM GRAPH_TABLE (
                       MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id = $start
                       COLUMNS (b.id AS b)
                   ) g WHERE g.b <> $skip ORDER BY g.b";
        // Person i knows i + 1 and i + 3 (mod 10).
        for (start, skip, expected) in [(1, 0, vec![2, 4]), (2, 3, vec![5]), (8, 9, vec![1])] {
            let values = params(&[("start", start.into()), ("SKIP", skip.into())]);
            let result = sql_engine.execute(sql, values).await.unwrap();
            assert_eq!(int_column(&result, "b"), expected);
            assert_eq!(cached(), 1);
        }
        let plan = sql_engine
            .explain(sql, params(&[("start", 1.into()), ("skip", 0.into())]))
            .await
            .unwrap();
        assert!(
            plan.contains("CsrExpandExec") && plan.contains("id = Int64(1)"),
            "{plan}"
        );

        // Other parameter types are another template; a container is planned each time.
        let values = params(&[("start", 1.into()), ("skip", JsonValue::Null)]);
        assert!(sql_engine.execute(sql, values).await.unwrap().num_rows() == 0);
        assert_eq!(cached(), 2);
        let listed =
            "SELECT * FROM GRAPH_TABLE (MATCH (a:Person) WHERE a.id IN $ids COLUMNS (a.id AS id))
                      ORDER BY id";
        let values = params(&[("ids", serde_json::json!([3, 1]))]);
        let result = sql_engine.execute(listed, values).await.unwrap();
        assert_eq!(int_column(&result, "id"), vec![1, 3]);
        assert_eq!(cached(), 2);

        assert!(sql_engine
            .execute(sql, params(&[("start", 1.into())]))
            .await
            .is_err());
        sql_engine.checkout_latest().await.unwrap();
        assert_eq!(cached(), 0);
    }
}
