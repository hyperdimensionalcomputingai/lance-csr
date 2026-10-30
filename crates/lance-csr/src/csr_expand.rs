// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Experimental CSR-backed single-hop expansion.
//!
//! The join path expands `(a)-[:R]->(b)` by scanning the whole relationship table
//! and hash-joining it to `a`. When an [`Adjacency`] for `R` is supplied, the planner
//! instead emits a [`CsrExpandNode`]: for each input row it looks up `a`'s neighbours
//! in the CSR and appends the relationship's two endpoint columns, exactly as the
//! relationship scan would have produced them.
//!
//! When a [`NodeRows`] index exists for the target label, the node also replaces the
//! target join: each neighbour id is mapped to its row in the target node table and
//! those rows are fetched by position (Lance `take_rows`, or Arrow `take` for
//! in-memory tables). Neighbours with no target row are dropped, as the inner join
//! would drop them. Otherwise the target join is planned as before.
//!
//! The adjacency is built from the relationship table alone:
//! - source endpoint values are mapped to dense CSR vertex positions in a hash map;
//! - neighbours store the raw target endpoint values (`i64` bit-cast to `u64`), so
//!   no node-table scan or second map is needed, and node-table writes never
//!   invalidate it. Rebuild only when the relationship table changes.
//!
//! The node index depends only on its node table, so the two are rebuilt
//! independently: edge commits rebuild the CSR, node commits rebuild the index.
//!
//! Only integer endpoint columns are supported; other shapes keep the join path.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    new_empty_array, Array, ArrayRef, AsArray, Int64Array, RecordBatch, UInt32Array, UInt64Array,
};
use arrow::compute::{cast, filter as filter_array, take};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::Column;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::logical_expr::{Extension, LogicalPlanBuilder};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use futures::StreamExt;
use lance::dataset::{Dataset, ProjectionRequest};
use lance_table::format::Fragment;

/// Yield after bounded CPU work without requiring a particular async runtime.
pub(crate) async fn cooperative_yield() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await
}

/// Lance's row-address column (`lance_core::ROW_ADDR`, not re-exported by `lance`).
const ROW_ADDR: &str = "_rowaddr";

use crate::case_insensitive::qualify_column;

/// Integer-keyed maps on the per-edge lookup path. With std's default hasher
/// (SipHash), hashing dominated CSR expansion time (profiled on LDBC Q30); ahash is
/// what Arrow and DataFusion use for their own hash tables.
type FastMap<K, V> = HashMap<K, V, ahash::RandomState>;
use crate::csr_index::{CsrIndex, CsrIndexBuilder};
use crate::error::{GraphError, Result};

/// Neighbour lookup for one relationship type in one direction.
///
/// An immutable CSR base, shared between snapshots via `Arc`, plus one delta applied
/// by [`Adjacency::with_changes`]: appended edges per source key, and tombstones as
/// sorted offsets into each source's base-plus-appended neighbours. Offsets let
/// readers skip deleted occurrences without copying per-source deletion state.
/// Identical edges remain distinct occurrences. Each neighbour carries the row its
/// edge came from (a Lance row address, or a row index for in-memory input), so the
/// base holds 16 bytes per edge: relationship columns can be fetched by row, and
/// deleted edges are found by row without reading the old dataset version.
///
/// # Design note for future changes (read before "optimizing" the delta)
///
/// `with_changes` copies the whole delta (not the base) on every refresh, so refresh
/// cost grows with the delta accumulated since the last full build, not just with
/// the new commit. This is deliberate. Snapshots must stay immutable because
/// in-flight queries keep using the previous one, so the delta cannot be mutated in
/// place. The copy is bounded: the engine rebuilds once the delta exceeds
/// `CSR_REBUILD_FRACTION` (25%) of the base.
/// Deletions additionally scan each affected source's neighbours once to resolve
/// occurrence offsets; queries can then resume without allocating deletion maps.
///
/// Measured at 10M edges (`python/benchmarks/csr_delta_accumulated.py`, 20 × 1%
/// appends): refresh grows from about 11 ms to about 80 ms as the delta reaches 20%,
/// against 1.2–1.6 s for a full rebuild. Two alternatives were tried or assessed
/// and rejected:
/// - shared, immutable delta layers merged like a binary counter: median refresh
///   74 → 32 ms, but merge cascades spiked to 298 ms, lookups got 5–25% slower, and
///   the code was about 60 lines larger;
/// - a persistent hash map (`imbl`): no spikes and one lookup, but about five new
///   crates and `unsafe` code to trust.
///
/// Revisit if refreshes become frequent on graphs far larger than 10M edges, or if
/// the rebuild threshold is raised.
pub struct Adjacency {
    base: Arc<CsrBase>,
    /// Appended `(neighbour, edge row)` pairs per source key.
    added: FastMap<i64, Vec<(u64, u64)>>,
    removed: FastMap<i64, Vec<usize>>,
    added_count: u64,
    removed_count: u64,
    /// Tombstones that remove appended edges rather than base ones.
    removed_added_count: u64,
}

struct CsrBase {
    /// Source endpoint value -> dense CSR vertex position.
    dense: FastMap<i64, u64>,
    csr: CsrIndex,
}

impl Adjacency {
    /// Build from a stream of relationship batches, reading edges `from -> to`.
    ///
    /// Column names match case-insensitively. Edges with a null endpoint are
    /// skipped: they can never satisfy the equality join they replace. Each edge's
    /// row is read from a `_rowaddr` column when the stream has one (a Lance scan);
    /// otherwise rows are numbered by their position in the stream.
    pub async fn build<E: fmt::Display>(
        mut stream: impl futures::Stream<Item = std::result::Result<RecordBatch, E>> + Unpin,
        from: &str,
        to: &str,
    ) -> Result<Self> {
        let mut dense: FastMap<i64, u64> = FastMap::default();
        let mut builder = CsrIndexBuilder::new();
        let mut position = 0u64;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| exec_error(format!("Failed to read edges: {}", e)))?;
            let from_col = int64_column(&batch, from)?;
            let to_col = int64_column(&batch, to)?;
            let addresses = batch
                .column_by_name(ROW_ADDR)
                .map(|c| c.as_primitive::<arrow::datatypes::UInt64Type>().clone());
            for i in 0..batch.num_rows() {
                if from_col.is_null(i) || to_col.is_null(i) {
                    continue;
                }
                let row = addresses
                    .as_ref()
                    .map_or(position + i as u64, |a| a.value(i));
                let next = dense.len() as u64;
                let src = *dense.entry(from_col.value(i)).or_insert(next);
                builder = builder.add_edge(src, to_col.value(i) as u64, row);
            }
            position += batch.num_rows() as u64;
        }
        // Explicit vertex count: neighbours are raw ids, not positions, so the
        // builder must not size its offsets from them.
        let csr = builder.with_num_vertices(dense.len() as u64).build();
        Ok(Self {
            base: Arc::new(CsrBase { dense, csr }),
            added: FastMap::default(),
            removed: FastMap::default(),
            added_count: 0,
            removed_count: 0,
            removed_added_count: 0,
        })
    }

    /// A new adjacency sharing this base, with `removed` edge rows tombstoned and
    /// `added` edges (`(from, to, row)` in this adjacency's direction) appended, on
    /// top of the existing delta.
    ///
    /// Removals apply to the edges present before this call, so a rewritten fragment
    /// can drop its old rows and add new ones under the same fragment id. `self` is
    /// unchanged, so snapshots still using it keep their answers. The base is never copied.
    // shortcut: copies the accumulated delta on refresh; bounded by the engine's 25% rebuild threshold. Upgrade to a persistent map if refreshes get frequent on much larger graphs.
    pub fn with_changes(&self, added: &[(i64, i64, u64)], removed: &RemovedRows) -> Self {
        let mut next = Self {
            base: self.base.clone(),
            added: self.added.clone(),
            removed: self.removed.clone(),
            added_count: self.added_count + added.len() as u64,
            removed_count: self.removed_count,
            removed_added_count: self.removed_added_count,
        };
        if !removed.is_empty() {
            // A row doesn't say which source holds it, so every live slot is checked.
            let base = self.base.dense.iter().map(|(&key, &v)| (key, v));
            let added_only = self
                .added
                .keys()
                .filter(|k| !self.base.dense.contains_key(*k));
            for (key, vertex) in base
                .map(|(k, v)| (k, Some(v)))
                .chain(added_only.map(|&k| (k, None)))
            {
                let base_rows = vertex.map_or(&[][..], |v| self.base.csr.edge_rows(v));
                let added_rows = self.added.get(&key).map_or(&[][..], Vec::as_slice);
                let prior = self.removed.get(&key).map_or(&[][..], Vec::as_slice);
                let mut prior = prior.iter().copied().peekable();
                let mut positions = Vec::new();
                let rows = base_rows
                    .iter()
                    .copied()
                    .chain(added_rows.iter().map(|&(_, r)| r));
                for (position, row) in rows.enumerate() {
                    if prior.peek() == Some(&position) {
                        prior.next();
                    } else if removed.contains(row) {
                        positions.push(position);
                    }
                }
                if !positions.is_empty() {
                    next.removed_count += positions.len() as u64;
                    next.removed_added_count +=
                        positions.iter().filter(|&&p| p >= base_rows.len()).count() as u64;
                    let tombstones = next.removed.entry(key).or_default();
                    tombstones.extend(positions);
                    tombstones.sort_unstable();
                }
            }
        }
        for &(from, to, row) in added {
            next.added.entry(from).or_default().push((to as u64, row));
        }
        next
    }

    /// Call `f` for each target endpoint value reachable from `key`, duplicates included.
    pub fn for_each_neighbor(&self, key: i64, f: impl FnMut(u64)) {
        self.neighbors_from(key, &mut 0).for_each(f);
    }

    /// Neighbours and their edge rows in the base.
    #[inline(always)]
    fn base_slots(&self, key: i64) -> (&[u64], &[u64]) {
        self.base.dense.get(&key).map_or((&[], &[]), |&v| {
            (self.base.csr.neighbors(v), self.base.csr.edge_rows(v))
        })
    }

    /// Resume at an occurrence offset, borrowing all neighbour and deletion storage.
    #[inline(always)]
    fn neighbors_from<'a>(
        &'a self,
        key: i64,
        offset: &'a mut usize,
    ) -> impl Iterator<Item = u64> + 'a {
        self.candidates_from::<false>(key, offset)
            .flatten()
            .map(|(n, _)| n)
    }

    /// `(neighbour, edge row)` per occurrence, `None` for a tombstone: tombstones stay
    /// visible to cancellation without repeating lookups for every edge. Without
    /// `ROWS` the edge row is 0 and never loaded, so plain hops pay nothing for it.
    #[inline(always)]
    fn candidates_from<'a, const ROWS: bool>(
        &'a self,
        key: i64,
        offset: &'a mut usize,
    ) -> impl Iterator<Item = Option<(u64, u64)>> + 'a {
        let (base, base_rows) = self.base_slots(key);
        let added = self.added.get(&key).map_or(&[][..], Vec::as_slice);
        let removed = self.removed.get(&key).map_or(&[][..], Vec::as_slice);
        let mut removed = &removed[removed.partition_point(|&position| position < *offset)..];
        let added_offset = offset.saturating_sub(base.len());
        let base_start = (*offset).min(base.len());
        let base_rows = &base_rows[base_start..];
        base[base_start..]
            .iter()
            .enumerate()
            .map(move |(i, &n)| (n, if ROWS { base_rows[i] } else { 0 }))
            .chain(
                added
                    .get(added_offset..)
                    .unwrap_or_default()
                    .iter()
                    .map(|&(n, row)| (n, if ROWS { row } else { 0 })),
            )
            .map(move |slot| {
                let position = *offset;
                *offset += 1;
                if removed.first() == Some(&position) {
                    removed = &removed[1..];
                    None
                } else {
                    Some(slot)
                }
            })
    }

    /// Live edges: base + appended - removed.
    pub fn num_edges(&self) -> u64 {
        self.base.csr.num_edges() + self.added_count - self.removed_count
    }

    pub fn base_edges(&self) -> u64 {
        self.base.csr.num_edges()
    }

    /// Distinct source endpoints in the base (deltas excluded): an estimate for planning.
    pub fn num_sources(&self) -> usize {
        self.base.dense.len()
    }

    /// Appended edges held outside the base.
    pub fn delta_edges(&self) -> u64 {
        self.added_count
    }

    /// Removed edges recorded as tombstones.
    pub fn tombstones(&self) -> u64 {
        self.removed_count
    }

    /// Live edges outside the base: appended and not since removed.
    pub fn unindexed_edges(&self) -> u64 {
        self.added_count - self.removed_added_count
    }
}

// Summary only: DataFusion formats extension nodes with `{:?}` during physical
// planning, so a derived Debug would print every edge on every query.
impl fmt::Debug for Adjacency {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Adjacency")
            .field("sources", &self.base.dense.len())
            .field("edges", &self.num_edges())
            .field("delta_edges", &self.added_count)
            .field("tombstones", &self.removed_count)
            .finish()
    }
}

fn int64_column(batch: &RecordBatch, name: &str) -> Result<Int64Array> {
    let (idx, _) = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .find(|(_, f)| f.name().eq_ignore_ascii_case(name))
        .ok_or_else(|| exec_error(format!("Edge column '{}' not found", name)))?;
    let array = cast(batch.column(idx), &DataType::Int64)
        .map_err(|e| exec_error(format!("Edge column '{}' is not an integer: {}", name, e)))?;
    Ok(array.as_primitive::<Int64Type>().clone())
}

/// Integer types whose values round-trip through `i64` without loss.
pub fn is_supported_id_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
    )
}

/// Edge rows gone since an adjacency was built, by row address: fragment id in the
/// high 32 bits, offset in the low 32 (row index for in-memory input, fragment 0).
#[derive(Debug, Default, PartialEq)]
pub struct RemovedRows {
    /// Fragment id -> newly deleted offsets, or `None` when the whole fragment is gone.
    fragments: FastMap<u32, Option<HashSet<u32>>>,
}

impl RemovedRows {
    /// Every row of fragment `id`.
    pub fn fragment(&mut self, id: u32) {
        self.fragments.insert(id, None);
    }

    /// Rows `offsets` of fragment `id`.
    pub fn rows(&mut self, id: u32, offsets: impl IntoIterator<Item = u32>) {
        if let Some(rows) = self
            .fragments
            .entry(id)
            .or_insert_with(|| Some(HashSet::new()))
        {
            rows.extend(offsets);
        }
    }

    #[inline]
    fn contains(&self, row: u64) -> bool {
        match self.fragments.get(&((row >> 32) as u32)) {
            None => false,
            Some(None) => true,
            Some(Some(offsets)) => offsets.contains(&(row as u32)),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.fragments.is_empty()
    }
}

/// Edges added and rows removed between two versions of a relationship table.
/// `added` holds `(source, target, row address)` in table orientation, skipping
/// null endpoints.
#[derive(Debug, Default, PartialEq)]
pub struct EdgeChanges {
    pub added: Vec<(i64, i64, u64)>,
    pub removed: RemovedRows,
}

impl EdgeChanges {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }
}

/// Diff two versions of a relationship table by fragment, reading only what changed.
///
/// Per fragment id: only in `new` -> its rows are added; only in `old` -> its rows
/// are removed; endpoint data files or overlays differ (rewrite, in-place column
/// update) -> its old rows are removed and its new rows added; only the deletion
/// file differs -> the rows in `new`'s deletion vector are removed. Fragments whose
/// endpoint data and deletions are unchanged are skipped, so non-endpoint schema
/// changes cost nothing.
///
/// Removals are row addresses, matched against the rows an adjacency stores, so
/// nothing is read from `old` beyond its manifest: the diff still works after
/// `old`'s files are cleaned up.
///
/// Returns `None` when a diff is not meaningful (an endpoint column was dropped,
/// re-created or retyped); the caller should rebuild, as it should on errors.
pub async fn edge_changes(
    old: &Dataset,
    new: &Dataset,
    src: &str,
    dst: &str,
) -> Result<Option<EdgeChanges>> {
    let endpoint_ids = |ds: &Dataset| -> Option<[(i32, DataType); 2]> {
        let field = |name: &str| ds.schema().field(name).map(|f| (f.id, f.data_type()));
        Some([field(src)?, field(dst)?])
    };
    let old_ids = endpoint_ids(old);
    if old_ids.is_none() || old_ids != endpoint_ids(new) {
        return Ok(None);
    }
    let fields: Vec<i32> = old_ids.unwrap().iter().map(|(id, _)| *id).collect();
    let touches = |f: &[i32]| f.iter().any(|id| fields.contains(id));
    // The parts of a fragment that determine its endpoint values.
    let endpoint_data = |frag: &Fragment| {
        let files: Vec<_> = frag.files.iter().filter(|d| touches(&d.fields)).collect();
        let overlays: Vec<_> = frag
            .overlays
            .iter()
            .filter(|o| touches(&o.data_file.fields))
            .collect();
        format!("{:?}{:?}", files, overlays)
    };

    let old_frags: HashMap<u64, Fragment> = old
        .get_fragments()
        .into_iter()
        .map(|f| (f.id() as u64, f.metadata().clone()))
        .collect();
    let new_frags: HashMap<u64, Fragment> = new
        .get_fragments()
        .into_iter()
        .map(|f| (f.id() as u64, f.metadata().clone()))
        .collect();

    let mut changes = EdgeChanges::default();
    let mut added_whole = Vec::new();
    for (&id, old_frag) in &old_frags {
        let Some(new_frag) = new_frags.get(&id) else {
            changes.removed.fragment(id as u32);
            continue;
        };
        if endpoint_data(old_frag) != endpoint_data(new_frag) {
            changes.removed.fragment(id as u32);
            added_whole.push(new_frag.clone());
        } else if old_frag.deletion_file != new_frag.deletion_file {
            // Rows deleted before `old` are not in the adjacency, so the whole vector will do.
            let deleted = match new.get_fragment(id as usize) {
                Some(frag) => frag.get_deletion_vector().await.map_err(lance_error)?,
                None => None,
            };
            if let Some(dv) = deleted {
                changes.removed.rows(id as u32, dv.to_sorted_iter());
            }
        }
    }
    for (id, new_frag) in &new_frags {
        if !old_frags.contains_key(id) {
            added_whole.push(new_frag.clone());
        }
    }
    if !added_whole.is_empty() {
        changes.added = scan_edges(new, added_whole, src, dst).await?;
    }
    Ok(Some(changes))
}

/// Live `(src, dst, row address)` rows of `fragments` in `ds`. Null endpoints are skipped.
async fn scan_edges(
    ds: &Dataset,
    fragments: Vec<Fragment>,
    src: &str,
    dst: &str,
) -> Result<Vec<(i64, i64, u64)>> {
    let mut scanner = ds.scan();
    scanner.with_fragments(fragments);
    scanner.project(&[src, dst]).map_err(lance_error)?;
    scanner.with_row_address();
    let mut stream = scanner.try_into_stream().await.map_err(lance_error)?;
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(lance_error)?;
        let (s, d) = (int64_column(&batch, src)?, int64_column(&batch, dst)?);
        let addrs = batch
            .column_by_name(ROW_ADDR)
            .ok_or_else(|| exec_error("Edge scan returned no row addresses".into()))?
            .as_primitive::<arrow::datatypes::UInt64Type>();
        for i in 0..batch.num_rows() {
            if s.is_valid(i) && d.is_valid(i) {
                out.push((s.value(i), d.value(i), addrs.value(i)));
            }
        }
    }
    Ok(out)
}

fn lance_error(e: lance::Error) -> GraphError {
    exec_error(format!("Lance: {}", e))
}

/// Rows of one node table, addressable by node id, for the fused target lookup.
pub struct NodeRows {
    /// Node id -> Lance row id, or row index into the in-memory batch.
    positions: FastMap<i64, u64>,
    data: TableData,
    schema: SchemaRef,
    /// Index of the id column in `schema`; its values are reconstructed, never fetched.
    id_index: usize,
    #[cfg(test)]
    fetched_cells: std::sync::atomic::AtomicUsize,
}

/// A table whose rows are fetched by position.
enum TableData {
    /// Pinned dataset; positions are Lance row ids (node tables) or row addresses
    /// (relationship tables), which stay valid for this version.
    Lance(Arc<Dataset>),
    Memory(RecordBatch),
}

impl TableData {
    /// Fetch `columns` (ascending schema indices) of the rows at `positions`, in
    /// row order with duplicates kept. Lance positions are row addresses when
    /// `addresses` is set, row ids otherwise.
    async fn take(
        &self,
        schema: &Schema,
        positions: &[u64],
        columns: &[usize],
        addresses: bool,
    ) -> Result<Vec<ArrayRef>> {
        if columns.is_empty() {
            return Ok(vec![]);
        }
        if positions.is_empty() {
            return Ok(columns
                .iter()
                .map(|&c| new_empty_array(schema.field(c).data_type()))
                .collect());
        }
        let take_error = |e: lance::Error| exec_error(format!("Failed to take rows: {}", e));
        match self {
            Self::Lance(dataset) => {
                let names = columns.iter().map(|&c| schema.field(c).name());
                // Lance returns projected columns in schema order, which `columns` follows.
                let projection = ProjectionRequest::from_columns(names, dataset.schema());
                let batch = if addresses {
                    let plan = projection
                        .into_projection_plan(dataset.clone())
                        .map_err(take_error)?;
                    lance::dataset::TakeBuilder::try_new_from_addresses(
                        dataset.clone(),
                        positions.to_vec(),
                        Arc::new(plan),
                    )
                    .map_err(take_error)?
                    .execute()
                    .await
                } else {
                    dataset.take_rows(positions, projection).await
                }
                .map_err(take_error)?;
                Ok(batch.columns().to_vec())
            }
            Self::Memory(batch) => {
                let indices = UInt64Array::from(positions.to_vec());
                columns
                    .iter()
                    .map(|&c| take(batch.column(c).as_ref(), &indices, None))
                    .collect::<std::result::Result<_, _>>()
                    .map_err(|e| exec_error(format!("Failed to take rows: {}", e)))
            }
        }
    }
}

impl NodeRows {
    /// Index a Lance node table by scanning only its id column with row ids.
    ///
    /// Returns `None` when ids are not integers or are not unique: a duplicate id
    /// matches several rows in the join, which a single position cannot reproduce.
    pub async fn from_lance(dataset: Arc<Dataset>, id_column: &str) -> Result<Option<Self>> {
        let schema: SchemaRef = Arc::new(Schema::from(dataset.schema()));
        let Some((id_index, id_name)) = supported_id_column(&schema, id_column) else {
            return Ok(None);
        };
        let lance_err = |e: lance::Error| exec_error(format!("Failed to scan node ids: {}", e));
        let mut scanner = dataset.scan();
        scanner.project(&[id_name.as_str()]).map_err(lance_err)?;
        scanner.with_row_id();
        let mut stream = scanner.try_into_stream().await.map_err(lance_err)?;
        let mut positions = FastMap::default();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(lance_err)?;
            let ids = int64_column(&batch, &id_name)?;
            let row_ids = batch
                .column_by_name(lance::dataset::ROW_ID)
                .ok_or_else(|| exec_error("Node scan returned no row ids".to_string()))?
                .as_primitive::<arrow::datatypes::UInt64Type>()
                .clone();
            if !insert_unique(&mut positions, &ids, |i| row_ids.value(i)) {
                return Ok(None);
            }
        }
        Ok(Some(Self {
            positions,
            data: TableData::Lance(dataset),
            schema,
            id_index,
            #[cfg(test)]
            fetched_cells: std::sync::atomic::AtomicUsize::new(0),
        }))
    }

    /// Index an in-memory node table. Same `None` rules as [`Self::from_lance`].
    pub fn from_batch(batch: RecordBatch, id_column: &str) -> Result<Option<Self>> {
        let Some((id_index, id_name)) = supported_id_column(&batch.schema(), id_column) else {
            return Ok(None);
        };
        let ids = int64_column(&batch, &id_name)?;
        let mut positions = FastMap::default();
        if !insert_unique(&mut positions, &ids, |i| i as u64) {
            return Ok(None);
        }
        Ok(Some(Self {
            positions,
            schema: batch.schema(),
            data: TableData::Memory(batch),
            id_index,
            #[cfg(test)]
            fetched_cells: std::sync::atomic::AtomicUsize::new(0),
        }))
    }

    pub fn num_rows(&self) -> usize {
        self.positions.len()
    }

    pub(crate) fn position(&self, id: i64) -> Option<u64> {
        self.positions.get(&id).copied()
    }

    /// Fetch `columns` (ascending schema indices) of the rows at `positions`, in
    /// row order with duplicates kept. Returns one array per requested column.
    async fn take(&self, positions: &[u64], columns: &[usize]) -> Result<Vec<ArrayRef>> {
        #[cfg(test)]
        if !columns.is_empty() {
            self.fetched_cells.fetch_add(
                positions.len() * columns.len(),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        self.data
            .take(&self.schema, positions, columns, false)
            .await
    }

    /// `columns` (ascending schema indices) of every row, from one sequential read.
    /// `None` for an in-memory table, which already gathers without I/O.
    async fn scan_columns(&self, columns: &[usize]) -> Result<Option<TargetColumns>> {
        let TableData::Lance(dataset) = &self.data else {
            return Ok(None);
        };
        let scan_error = |e: lance::Error| exec_error(format!("Failed to scan nodes: {}", e));
        let names: Vec<&str> = columns
            .iter()
            .map(|&c| self.schema.field(c).name().as_str())
            .collect();
        let mut scanner = dataset.scan();
        scanner.project(&names).map_err(scan_error)?;
        scanner.with_row_id();
        let mut stream = scanner.try_into_stream().await.map_err(scan_error)?;
        let mut index = FastMap::default();
        let mut parts: Vec<Vec<ArrayRef>> = vec![Vec::new(); columns.len()];
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(scan_error)?;
            let row_ids = batch
                .column_by_name(lance::dataset::ROW_ID)
                .ok_or_else(|| exec_error("Node scan returned no row ids".into()))?
                .as_primitive::<arrow::datatypes::UInt64Type>();
            // Row ids are not offsets (fragments, deletions, stable ids), so map them.
            for &row_id in row_ids.values() {
                let next = index.len() as u64;
                index.insert(row_id, next);
            }
            for (part, name) in parts.iter_mut().zip(&names) {
                part.push(
                    batch
                        .column_by_name(name)
                        .cloned()
                        .ok_or_else(|| exec_error("Node scan lost a column".into()))?,
                );
            }
            cooperative_yield().await;
        }
        let concat_error = |e: arrow::error::ArrowError| exec_error(e.to_string());
        let arrays = parts
            .iter()
            .zip(columns)
            .map(|(part, &c)| {
                let part: Vec<&dyn Array> = part.iter().map(|a| a.as_ref()).collect();
                if part.is_empty() {
                    Ok((c, new_empty_array(self.schema.field(c).data_type())))
                } else {
                    arrow::compute::concat(&part)
                        .map(|a| (c, a))
                        .map_err(concat_error)
                }
            })
            .collect::<Result<_>>()?;
        Ok(Some(TargetColumns {
            index,
            columns: arrays,
        }))
    }
}

/// A target table's columns read once, sequentially, for a hop that fetches a large
/// share of it; later fetches gather from memory instead of reading pages again.
struct TargetColumns {
    /// Lance row id -> position in `columns`' arrays.
    index: FastMap<u64, u64>,
    /// Schema index -> the column's values for every row.
    columns: Vec<(usize, ArrayRef)>,
}

impl TargetColumns {
    /// Same contract as [`NodeRows::take`], for columns this read covers.
    fn take(&self, positions: &[u64], columns: &[usize]) -> Result<Vec<ArrayRef>> {
        let indices = positions
            .iter()
            .map(|p| self.index.get(p).copied())
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| exec_error("Node row missing from its own snapshot".into()))?;
        let indices = UInt64Array::from(indices);
        columns
            .iter()
            .map(|c| {
                let (_, array) = self
                    .columns
                    .iter()
                    .find(|(column, _)| column == c)
                    .ok_or_else(|| exec_error("Target column was not read".into()))?;
                take(array.as_ref(), &indices, None)
                    .map_err(|e| exec_error(format!("Failed to take rows: {}", e)))
            })
            .collect()
    }
}

/// Shared by an operator's partitions: target rows fetched so far, and the target
/// columns once that count made one sequential read worthwhile. `None` once set
/// means the table is in memory and gathers directly.
#[derive(Default)]
struct TargetScanState {
    fetched: std::sync::atomic::AtomicU64,
    columns: std::sync::OnceLock<Option<Arc<TargetColumns>>>,
    building: futures::lock::Mutex<()>,
}

impl fmt::Debug for TargetScanState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TargetScanState")
            .field("scanned", &self.columns.get().is_some_and(Option::is_some))
            .finish()
    }
}

/// Switch from fetching target columns per chunk to one read of the needed columns
/// once the fetched target rows pass 1/`TARGET_SCAN_FRACTION` of the node table and
/// one batch (a scan's setup outweighs one small take), as
/// [`EDGE_MASK_FRACTION`] does for relationship filters. The read columns stay in
/// memory for the rest of the query.
// shortcut: fixed share and no memory budget (the engine sets no DataFusion memory pool); add a reservation and fall back to per-chunk fetches if one is configured.
const TARGET_SCAN_FRACTION: u64 = 8;

/// Fetch a target's columns from the read-once columns when present, else by row id.
async fn take_target(
    target: &TargetLookup,
    scanned: Option<&TargetColumns>,
    positions: &[u64],
    columns: &[usize],
) -> datafusion::common::Result<Vec<ArrayRef>> {
    match scanned {
        Some(scanned) if !columns.is_empty() => scanned.take(positions, columns),
        _ => target.rows.take(positions, columns).await,
    }
    .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))
}

// Summary only, for the same reason as `Adjacency`.
impl fmt::Debug for NodeRows {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("NodeRows")
            .field("rows", &self.positions.len())
            .finish()
    }
}

/// Index and actual name of the id column, if it exists (case-insensitively) with a supported type.
fn supported_id_column(schema: &Schema, id_column: &str) -> Option<(usize, String)> {
    schema
        .fields()
        .iter()
        .enumerate()
        .find(|(_, f)| f.name().eq_ignore_ascii_case(id_column))
        .filter(|(_, f)| is_supported_id_type(f.data_type()))
        .map(|(i, f)| (i, f.name().clone()))
}

/// Insert non-null ids; false if any id repeats.
fn insert_unique(
    positions: &mut FastMap<i64, u64>,
    ids: &Int64Array,
    position_of: impl Fn(usize) -> u64,
) -> bool {
    for i in 0..ids.len() {
        if ids.is_valid(i) && positions.insert(ids.value(i), position_of(i)).is_some() {
            return false;
        }
    }
    true
}

/// Rows of one relationship table, fetched by the edge rows an [`Adjacency`] stores.
/// Must be the same snapshot the adjacency describes.
pub struct EdgeRows {
    data: TableData,
    schema: SchemaRef,
}

impl EdgeRows {
    /// Rows of a pinned Lance dataset, by row address.
    pub fn from_lance(dataset: Arc<Dataset>) -> Self {
        Self {
            schema: Arc::new(Schema::from(dataset.schema())),
            data: TableData::Lance(dataset),
        }
    }

    /// Rows of an in-memory table, by row index.
    pub fn from_batch(batch: RecordBatch) -> Self {
        Self {
            schema: batch.schema(),
            data: TableData::Memory(batch),
        }
    }
}

impl EdgeRows {
    async fn take(&self, rows: &[u64], columns: &[usize]) -> Result<Vec<ArrayRef>> {
        self.data.take(&self.schema, rows, columns, true).await
    }

    /// The rows passing `filter`, from one sequential read of its columns.
    async fn mask(&self, filter: &CompiledFilter) -> Result<EdgeMask> {
        let mut mask = EdgeMask::default();
        let df_error = |e: datafusion::common::DataFusionError| exec_error(e.to_string());
        match &self.data {
            TableData::Memory(batch) => {
                let columns = filter.columns.iter().map(|&c| batch.column(c).clone());
                let keep = filter.keep(columns.collect()).map_err(df_error)?;
                mask.extend(
                    keep.iter()
                        .enumerate()
                        .filter(|(_, &k)| k)
                        .map(|(i, _)| i as u64),
                );
            }
            TableData::Lance(dataset) => {
                let scan_error =
                    |e: lance::Error| exec_error(format!("Failed to scan edges: {}", e));
                let names: Vec<&str> = filter
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect();
                let mut scanner = dataset.scan();
                scanner.project(&names).map_err(scan_error)?;
                scanner.with_row_address();
                let mut stream = scanner.try_into_stream().await.map_err(scan_error)?;
                while let Some(batch) = stream.next().await {
                    let batch = batch.map_err(scan_error)?;
                    let columns = names
                        .iter()
                        .map(|n| batch.column_by_name(n).cloned())
                        .collect::<Option<Vec<_>>>()
                        .ok_or_else(|| exec_error("Edge scan lost a filter column".into()))?;
                    let addresses = batch
                        .column_by_name(ROW_ADDR)
                        .ok_or_else(|| exec_error("Edge scan returned no row addresses".into()))?
                        .as_primitive::<arrow::datatypes::UInt64Type>()
                        .clone();
                    let keep = filter.keep(columns).map_err(df_error)?;
                    mask.extend(
                        keep.iter()
                            .zip(addresses.values())
                            .filter(|(&k, _)| k)
                            .map(|(_, &a)| a),
                    );
                    cooperative_yield().await;
                }
            }
        }
        Ok(mask)
    }
}

/// The edge rows that pass a relationship filter: one bitset per fragment id, indexed
/// by the row's offset in it (row index for in-memory input, fragment 0). Indexed by
/// fragment id rather than hashed: it is checked once per expanded edge.
#[derive(Default)]
struct EdgeMask {
    fragments: Vec<Vec<u64>>,
}

impl EdgeMask {
    fn extend(&mut self, rows: impl Iterator<Item = u64>) {
        for row in rows {
            let (fragment, offset) = ((row >> 32) as usize, row as u32 as usize);
            if self.fragments.len() <= fragment {
                self.fragments.resize_with(fragment + 1, Vec::new);
            }
            let bits = &mut self.fragments[fragment];
            if bits.len() <= offset / 64 {
                bits.resize(offset / 64 + 1, 0);
            }
            bits[offset / 64] |= 1 << (offset % 64);
        }
    }

    #[inline]
    fn contains(&self, row: u64) -> bool {
        let offset = row as u32 as usize;
        self.fragments
            .get((row >> 32) as usize)
            .and_then(|bits| bits.get(offset / 64))
            .is_some_and(|word| word >> (offset % 64) & 1 == 1)
    }
}

/// Shared by an operator's partitions: edges expanded before the relationship filter
/// ran, and the filter's mask once that count made a full read worthwhile.
#[derive(Default)]
struct EdgeFilterState {
    expanded: std::sync::atomic::AtomicU64,
    mask: std::sync::OnceLock<Arc<EdgeMask>>,
    building: futures::lock::Mutex<()>,
}

impl fmt::Debug for EdgeFilterState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("EdgeFilterState")
            .field("masked", &self.mask.get().is_some())
            .finish()
    }
}

/// Switch from fetching filter columns per expanded edge to one read of the whole
/// column once the expanded edges pass 1/`EDGE_MASK_FRACTION` of the relationship's
/// edges. A batch of scattered rows from a large Lance table touches most of the
/// column's pages, so each batch fetched before the switch costs about one full read.
// shortcut: fixed share, measured on IC5 (half of a 1.6M-edge table) and IC11; tune if hops in between regress.
const EDGE_MASK_FRACTION: u64 = 256;

// Summary only, for the same reason as `Adjacency`.
impl fmt::Debug for EdgeRows {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("EdgeRows")
            .field("columns", &self.schema.fields().len())
            .finish()
    }
}

/// A named relationship's columns other than its endpoints, fetched from `rows`.
#[derive(Clone)]
pub struct EdgeLookup {
    variable: String,
    rows: Arc<EdgeRows>,
    /// Endpoint column names, output by the expansion itself.
    endpoints: [String; 2],
    /// Output fields, named as the relationship scan would name them (`{variable}__{column}`).
    fields: Vec<Field>,
    /// For each output field, its column index in `rows` (ascending).
    columns: Vec<usize>,
}

impl EdgeLookup {
    /// Every column of `rows` except `endpoints`, or `None` if `rows` lacks one of
    /// `names` (the relationship columns the planner expects).
    pub fn new(
        variable: &str,
        rows: Arc<EdgeRows>,
        names: &[&str],
        endpoints: [&str; 2],
    ) -> Option<Self> {
        let mut columns: Vec<usize> = names
            .iter()
            .filter(|n| !endpoints.iter().any(|e| e.eq_ignore_ascii_case(n)))
            .map(|n| rows.schema.index_of(n).ok())
            .collect::<Option<_>>()?;
        columns.sort_unstable();
        let fields = columns
            .iter()
            .map(|&c| {
                let f = rows.schema.field(c);
                Field::new(
                    qualify_column(variable, f.name()),
                    f.data_type().clone(),
                    f.is_nullable(),
                )
            })
            .collect();
        Some(Self {
            variable: variable.to_string(),
            rows,
            endpoints: endpoints.map(str::to_string),
            fields,
            columns,
        })
    }

    /// Qualified output name (`k__since`) -> the table's own column name, for every
    /// non-endpoint column, including ones pruned from the output.
    fn raw_columns(&self) -> HashMap<String, String> {
        self.rows
            .schema
            .fields()
            .iter()
            .filter(|f| {
                !self
                    .endpoints
                    .iter()
                    .any(|e| e.eq_ignore_ascii_case(f.name()))
            })
            .map(|f| (qualify_column(&self.variable, f.name()), f.name().clone()))
            .collect()
    }

    fn compile_filter(
        &self,
        filter: &Expr,
        state: &SessionState,
    ) -> datafusion::common::Result<CompiledFilter> {
        CompiledFilter::new(filter, &self.raw_columns(), &self.rows.schema, state)
    }

    fn describe_columns(&self) -> String {
        let names: Vec<_> = self.fields.iter().map(|f| f.name().as_str()).collect();
        format!("[{}]", names.join(", "))
    }
}

impl fmt::Debug for EdgeLookup {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("EdgeLookup")
            .field("variable", &self.variable)
            .field("columns", &self.describe_columns())
            .finish()
    }
}

/// A fused target lookup: the target variable's columns, fetched from `rows`.
#[derive(Clone)]
pub struct TargetLookup {
    pub variable: String,
    pub rows: Arc<NodeRows>,
    /// Output fields, named as the target scan would name them (`{variable}__{column}`).
    fields: Vec<Field>,
    /// For each output field, its column index in `rows` (ascending).
    columns: Vec<usize>,
}

/// A moved target or relationship filter compiled for one query: evaluated on just
/// the columns it reads, for the candidate rows of each batch.
#[derive(Debug, Clone)]
struct CompiledFilter {
    predicate: Arc<dyn PhysicalExpr>,
    /// Table column indices the predicate reads, ascending (`schema`'s order).
    columns: Vec<usize>,
    schema: SchemaRef,
}

impl CompiledFilter {
    /// Compile `filter`, an expression over qualified columns such as `b__name`,
    /// against `table`'s own columns; `raw` maps each qualified name to its column.
    fn new(
        filter: &Expr,
        raw: &HashMap<String, String>,
        table: &Schema,
        state: &SessionState,
    ) -> datafusion::common::Result<Self> {
        let filter = filter
            .clone()
            .transform(|e| match &e {
                Expr::Column(c) => match raw.get(&c.name) {
                    Some(name) => Ok(Transformed::yes(Expr::Column(Column::from_name(name)))),
                    None => Ok(Transformed::no(e)),
                },
                _ => Ok(Transformed::no(e)),
            })?
            .data;
        let mut columns: Vec<usize> = filter
            .column_refs()
            .iter()
            .map(|c| table.index_of(&c.name))
            .collect::<std::result::Result<_, _>>()?;
        columns.sort_unstable();
        columns.dedup();
        let schema = Arc::new(table.project(&columns)?);
        let predicate =
            state.create_physical_expr(filter, &DFSchema::try_from(schema.as_ref().clone())?)?;
        Ok(Self {
            predicate,
            columns,
            schema,
        })
    }

    /// Which rows of `columns` (one array per filter column) pass; null counts as false.
    fn keep(&self, columns: Vec<ArrayRef>) -> datafusion::common::Result<Vec<bool>> {
        let candidates = RecordBatch::try_new(self.schema.clone(), columns)?;
        let mask = self
            .predicate
            .evaluate(&candidates)?
            .into_array(candidates.num_rows())?;
        let mask = mask.as_boolean();
        Ok((0..mask.len())
            .map(|i| mask.is_valid(i) && mask.value(i))
            .collect())
    }
}

// Summary only, like `Adjacency`: DataFusion formats extension nodes with `{:?}`.
impl fmt::Debug for TargetLookup {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TargetLookup")
            .field("variable", &self.variable)
            .field("columns", &self.describe_columns())
            .finish()
    }
}

impl TargetLookup {
    /// Qualified output name (`b__name`) -> the node table's own column name, for
    /// every column of the table, including ones pruned from the output.
    fn raw_columns(&self) -> HashMap<String, String> {
        self.rows
            .schema
            .fields()
            .iter()
            .map(|f| (qualify_column(&self.variable, f.name()), f.name().clone()))
            .collect()
    }

    /// Compile `filter` (an expression over qualified target columns such as
    /// `b__name`) against the target table's own columns, for per-batch evaluation.
    fn compile_filter(
        &self,
        filter: &Expr,
        state: &SessionState,
    ) -> datafusion::common::Result<CompiledFilter> {
        CompiledFilter::new(filter, &self.raw_columns(), &self.rows.schema, state)
    }

    /// `[b__id*, b__name]`: output columns, `*` marking the id rebuilt from neighbour values.
    fn describe_columns(&self) -> String {
        let names: Vec<String> = self
            .columns
            .iter()
            .zip(&self.fields)
            .map(|(&c, f)| {
                let mark = if c == self.rows.id_index { "*" } else { "" };
                format!("{}{}", f.name(), mark)
            })
            .collect();
        format!("[{}]", names.join(", "))
    }

    /// All target columns; [`PruneCsrTargetColumns`] later drops unused ones.
    pub fn new(variable: String, rows: Arc<NodeRows>, fields: Vec<Field>) -> Self {
        let columns = (0..fields.len()).collect();
        Self {
            variable,
            rows,
            fields,
            columns,
        }
    }
}

/// Adjacencies and node indexes available to the planner.
///
/// Adjacencies are keyed by lowercase relationship type and direction; node indexes
/// by lowercase node label.
#[derive(Debug, Clone, Default)]
pub struct AdjacencyCatalog {
    entries: HashMap<(String, bool), Arc<Adjacency>>,
    nodes: HashMap<String, Arc<NodeRows>>,
    edges: HashMap<String, Arc<EdgeRows>>,
}

impl AdjacencyCatalog {
    /// Rows of `rel_type`'s table, matching the edge rows of its adjacencies.
    pub fn insert_edges(&mut self, rel_type: &str, rows: Arc<EdgeRows>) {
        self.edges.insert(rel_type.to_lowercase(), rows);
    }

    pub fn edges(&self, rel_type: &str) -> Option<&Arc<EdgeRows>> {
        self.edges.get(&rel_type.to_lowercase())
    }

    pub fn insert(&mut self, rel_type: &str, outgoing: bool, adjacency: Arc<Adjacency>) {
        self.entries
            .insert((rel_type.to_lowercase(), outgoing), adjacency);
    }

    pub fn get(&self, rel_type: &str, outgoing: bool) -> Option<&Arc<Adjacency>> {
        self.entries.get(&(rel_type.to_lowercase(), outgoing))
    }

    pub fn insert_nodes(&mut self, label: &str, rows: Arc<NodeRows>) {
        self.nodes.insert(label.to_lowercase(), rows);
    }

    pub fn nodes(&self, label: &str) -> Option<&Arc<NodeRows>> {
        self.nodes.get(&label.to_lowercase())
    }
}

/// The neighbours one expansion follows: one adjacency for a directed pattern, or
/// the outgoing and incoming adjacencies of the same relationship type for an
/// undirected one. Undirected expansion visits outgoing neighbours, then incoming
/// ones, skipping self-loops the second time so each edge matches once from each
/// end and a self-loop once: the same rows as the join path's reversed-edge scan.
#[derive(Debug, Clone)]
pub struct Neighbors {
    forward: Arc<Adjacency>,
    reverse: Option<Arc<Adjacency>>,
}

/// Occurrence offsets are sufficient to pause even a high-degree source row.
#[derive(Clone, Copy, Default)]
pub(crate) struct NeighborPosition {
    forward: usize,
    reverse: usize,
}

impl Neighbors {
    pub fn directed(adjacency: Arc<Adjacency>) -> Self {
        Self {
            forward: adjacency,
            reverse: None,
        }
    }

    pub fn undirected(outgoing: Arc<Adjacency>, incoming: Arc<Adjacency>) -> Self {
        Self {
            forward: outgoing,
            reverse: Some(incoming),
        }
    }

    pub fn for_each_neighbor(&self, key: i64, f: impl FnMut(u64)) {
        self.neighbors_from(key, &mut NeighborPosition::default())
            .for_each(f);
    }

    #[inline(always)]
    pub(crate) fn neighbors_from<'a>(
        &'a self,
        key: i64,
        position: &'a mut NeighborPosition,
    ) -> impl Iterator<Item = u64> + 'a {
        self.candidates_from::<false>(key, position)
            .flatten()
            .map(|(n, _)| n)
    }

    /// `(neighbour, edge row)` candidates, with edge rows only when `ROWS`. Borrow each
    /// adjacency once while exposing tombstones and suppressed self-loops as work.
    #[inline(always)]
    pub(crate) fn candidates_from<'a, const ROWS: bool>(
        &'a self,
        key: i64,
        position: &'a mut NeighborPosition,
    ) -> impl Iterator<Item = Option<(u64, u64)>> + 'a {
        let forward = self
            .forward
            .candidates_from::<ROWS>(key, &mut position.forward);
        let reverse = self
            .reverse
            .as_ref()
            .map(|r| r.candidates_from::<ROWS>(key, &mut position.reverse));
        forward.chain(
            reverse
                .into_iter()
                .flatten()
                .map(move |candidate| candidate.filter(|&(n, _)| n as i64 != key)),
        )
    }

    /// Edges visited over all keys (self-loops count once per direction in this estimate).
    pub fn num_edges(&self) -> u64 {
        self.forward.num_edges() + self.reverse.as_ref().map_or(0, |r| r.num_edges())
    }

    fn is_undirected(&self) -> bool {
        self.reverse.is_some()
    }

    /// Mean neighbours per source key over the base CSRs: a planning estimate.
    fn mean_degree(&self) -> f64 {
        let degree = |a: &Adjacency| a.base_edges() as f64 / a.num_sources().max(1) as f64;
        degree(&self.forward) + self.reverse.as_deref().map_or(0.0, degree)
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.forward, &other.forward)
            && match (&self.reverse, &other.reverse) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            }
    }
}

/// Logical node: append `(key_output, neighbor_output)` for every neighbour of `key_column`.
#[derive(Debug, Clone)]
pub struct CsrExpandNode {
    input: LogicalPlan,
    /// Input column holding the source node id.
    key_column: String,
    /// Output names/types of the relationship endpoint columns, as the relationship scan names them.
    key_output: Field,
    neighbor_output: Field,
    adjacency: Neighbors,
    /// When set, the target node's columns are fetched here instead of by a join.
    target: Option<TargetLookup>,
    /// When set, a named relationship's other columns are fetched here by edge row.
    edge: Option<EdgeLookup>,
    /// WHERE conjuncts over target columns only, moved here by [`PushCsrTargetFilters`]:
    /// neighbours that fail it are dropped before any take.
    target_filter: Option<Expr>,
    /// WHERE conjuncts over the named relationship's own columns only, moved here by
    /// [`PushCsrTargetFilters`]: edges that fail it are dropped before any other take.
    edge_filter: Option<Expr>,
    /// Closing edge: input column holding the already-bound target id. Only neighbours
    /// equal to it are kept, replacing a filter after a full expansion.
    closing_column: Option<String>,
    traversal: Option<Arc<crate::csr_traversal::Traversal>>,
    schema: DFSchemaRef,
}

impl CsrExpandNode {
    pub fn try_new(
        input: LogicalPlan,
        key_column: String,
        key_output: Field,
        neighbor_output: Field,
        adjacency: Neighbors,
        target: Option<TargetLookup>,
        edge: Option<EdgeLookup>,
    ) -> Result<Self> {
        let mut added_fields = vec![key_output.clone(), neighbor_output.clone()];
        added_fields.extend(target.iter().flat_map(|t| t.fields.iter().cloned()));
        added_fields.extend(edge.iter().flat_map(|e| e.fields.iter().cloned()));
        let added = DFSchema::try_from(Schema::new(added_fields))
            .map_err(|e| plan_error(format!("CsrExpand schema: {}", e)))?;
        let schema = input
            .schema()
            .join(&added)
            .map_err(|e| plan_error(format!("CsrExpand schema: {}", e)))?;
        Ok(Self {
            input,
            key_column,
            key_output,
            neighbor_output,
            adjacency,
            target,
            edge,
            target_filter: None,
            edge_filter: None,
            closing_column: None,
            traversal: None,
            schema: Arc::new(schema),
        })
    }

    /// Keep only neighbours equal to the bound target id in `column` (a closing edge).
    pub fn with_closing_column(mut self, column: String) -> Self {
        self.closing_column = Some(column);
        self
    }

    pub(crate) fn with_traversal(mut self, traversal: crate::csr_traversal::Traversal) -> Self {
        self.traversal = Some(Arc::new(traversal));
        self
    }

    /// The same node over `input` with `target` and `edge`, keeping the moved filter
    /// and closing column.
    fn rebuilt(
        &self,
        input: LogicalPlan,
        target: Option<TargetLookup>,
        edge: Option<EdgeLookup>,
    ) -> Result<Self> {
        let mut node = Self::try_new(
            input,
            self.key_column.clone(),
            self.key_output.clone(),
            self.neighbor_output.clone(),
            self.adjacency.clone(),
            target,
            edge,
        )?;
        node.target_filter = self.target_filter.clone();
        node.edge_filter = self.edge_filter.clone();
        node.closing_column = self.closing_column.clone();
        node.traversal = self.traversal.clone();
        Ok(node)
    }

    /// This node with only the target and edge columns named in `used`, or `None`
    /// if nothing changes.
    fn with_target_columns(&self, used: &HashSet<String>) -> Option<Self> {
        let keep = |columns: &[usize], fields: &[Field]| -> (Vec<usize>, Vec<Field>) {
            columns
                .iter()
                .zip(fields)
                .filter(|(_, f)| used.contains(f.name()))
                .map(|(&c, f)| (c, f.clone()))
                .unzip()
        };
        let target = self.target.as_ref().map(|t| {
            let (columns, fields) = keep(&t.columns, &t.fields);
            TargetLookup {
                columns,
                fields,
                ..t.clone()
            }
        });
        let edge = self.edge.as_ref().map(|e| {
            let (columns, fields) = keep(&e.columns, &e.fields);
            EdgeLookup {
                columns,
                fields,
                ..e.clone()
            }
        });
        let width = |t: Option<&TargetLookup>, e: Option<&EdgeLookup>| {
            t.map_or(0, |t| t.fields.len()) + e.map_or(0, |e| e.fields.len())
        };
        if width(target.as_ref(), edge.as_ref()) == width(self.target.as_ref(), self.edge.as_ref())
        {
            return None;
        }
        self.rebuilt(self.input.clone(), target, edge).ok()
    }

    /// Names of the columns this node adds (endpoints, then any target and edge columns).
    fn added_columns(&self) -> impl Iterator<Item = &String> {
        [self.key_output.name(), self.neighbor_output.name()]
            .into_iter()
            .chain(
                self.target
                    .iter()
                    .flat_map(|t| t.fields.iter().map(|f| f.name())),
            )
            .chain(
                self.edge
                    .iter()
                    .flat_map(|e| e.fields.iter().map(|f| f.name())),
            )
    }
}

// Logical nodes must be comparable; the adjacency compares by identity.
impl PartialEq for CsrExpandNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.key_column == other.key_column
            && self.key_output == other.key_output
            && self.neighbor_output == other.neighbor_output
            && self.adjacency.ptr_eq(&other.adjacency)
            && match (&self.target, &other.target) {
                (Some(a), Some(b)) => {
                    a.variable == b.variable
                        && a.columns == b.columns
                        && Arc::ptr_eq(&a.rows, &b.rows)
                }
                (None, None) => true,
                _ => false,
            }
            && match (&self.edge, &other.edge) {
                (Some(a), Some(b)) => {
                    a.variable == b.variable
                        && a.columns == b.columns
                        && Arc::ptr_eq(&a.rows, &b.rows)
                }
                (None, None) => true,
                _ => false,
            }
            && self.target_filter == other.target_filter
            && self.edge_filter == other.edge_filter
            && self.closing_column == other.closing_column
            && match (&self.traversal, &other.traversal) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            }
    }
}
impl Eq for CsrExpandNode {}
impl Hash for CsrExpandNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.key_column.hash(state);
        self.key_output.name().hash(state);
        self.neighbor_output.name().hash(state);
        self.target.as_ref().map(|t| &t.variable).hash(state);
        self.edge
            .as_ref()
            .map(|e| (&e.variable, &e.columns))
            .hash(state);
        self.target_filter.hash(state);
        self.edge_filter.hash(state);
        self.closing_column.hash(state);
    }
}
impl PartialOrd for CsrExpandNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (
            &self.key_column,
            self.key_output.name(),
            self.neighbor_output.name(),
        )
            .partial_cmp(&(
                &other.key_column,
                other.key_output.name(),
                other.neighbor_output.name(),
            ))
    }
}

impl UserDefinedLogicalNodeCore for CsrExpandNode {
    fn name(&self) -> &str {
        "CsrExpand"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    /// Filters on input columns may move below the expansion; filters on the
    /// columns this node creates must stay above it.
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        self.added_columns().cloned().collect()
    }

    /// Projection pushdown: the input only needs the requested input columns plus
    /// the key; the two appended columns are computed here.
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        let input_width = self.input.schema().fields().len();
        let key = self
            .input
            .schema()
            .index_of_column_by_name(None, &self.key_column)?;
        let closing = self
            .closing_column
            .as_deref()
            .map(|c| self.input.schema().index_of_column_by_name(None, c));
        let closing = match closing {
            Some(None) => return None,
            Some(Some(i)) => Some(i),
            None => None,
        };
        let mut needed: Vec<usize> = output_columns
            .iter()
            .copied()
            .filter(|&i| i < input_width)
            .chain(std::iter::once(key))
            .chain(closing)
            .collect();
        needed.sort_unstable();
        needed.dedup();
        Some(vec![needed])
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CsrExpand: key={} -> [{}, {}], edges={}",
            self.key_column,
            self.key_output.name(),
            self.neighbor_output.name(),
            self.adjacency.num_edges()
        )?;
        if self.adjacency.is_undirected() {
            write!(f, ", undirected")?;
        }
        if let Some(target) = &self.target {
            write!(f, ", take={}{}", target.variable, target.describe_columns())?;
        }
        if let Some(edge) = &self.edge {
            write!(f, ", edge={}{}", edge.variable, edge.describe_columns())?;
        }
        if let Some(filter) = &self.target_filter {
            write!(f, ", filter={}", filter)?;
        }
        if let Some(filter) = &self.edge_filter {
            write!(f, ", edge_filter={}", filter)?;
        }
        if let Some(column) = &self.closing_column {
            write!(f, ", match={}", column)?;
        }
        if let Some(walk) = &self.traversal {
            write!(f, ", iterative hops={}..{}", walk.min_hops, walk.max_hops)?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        let input = inputs.into_iter().next().expect("CsrExpand has one input");
        self.rebuilt(input, self.target.clone(), self.edge.clone())
            .map_err(|e| datafusion::common::DataFusionError::Plan(e.to_string()))
    }
}

/// Physical operator for [`CsrExpandNode`]; expands input rows in bounded chunks.
#[derive(Debug)]
pub struct CsrExpandExec {
    input: Arc<dyn ExecutionPlan>,
    key_column: String,
    adjacency: Neighbors,
    target: Option<TargetLookup>,
    edge: Option<EdgeLookup>,
    /// The moved target filter, if any.
    target_filter: Option<CompiledFilter>,
    /// The moved relationship filter, if any, and its shared progress.
    edge_filter: Option<CompiledFilter>,
    edge_state: Arc<EdgeFilterState>,
    /// Target rows fetched so far, and the target columns once read whole.
    target_state: Arc<TargetScanState>,
    closing_column: Option<String>,
    traversal: Option<Arc<crate::csr_traversal::Traversal>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl CsrExpandExec {
    #[allow(clippy::too_many_arguments)]
    fn new(
        input: Arc<dyn ExecutionPlan>,
        key_column: String,
        adjacency: Neighbors,
        target: Option<TargetLookup>,
        edge: Option<EdgeLookup>,
        target_filter: Option<CompiledFilter>,
        edge_filter: Option<CompiledFilter>,
        closing_column: Option<String>,
        traversal: Option<Arc<crate::csr_traversal::Traversal>>,
        schema: SchemaRef,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            key_column,
            adjacency,
            target,
            edge,
            target_filter,
            edge_filter,
            edge_state: Arc::default(),
            target_state: Arc::default(),
            closing_column,
            traversal,
            schema,
            properties,
        }
    }
}

/// One input batch and its current row/occurrence, independent of total fan-out.
struct BatchExpansion {
    batch: RecordBatch,
    keys: Int64Array,
    bound: Option<Int64Array>,
    row: usize,
    position: NeighborPosition,
    walk: Option<crate::csr_traversal::TraversalCursor>,
    /// Record each output row's edge row, for a relationship-column fetch.
    edge_rows: bool,
    /// Passing edge rows of the relationship filter, once built: others are skipped.
    edge_mask: Option<Arc<EdgeMask>>,
}

impl BatchExpansion {
    fn try_new(
        batch: RecordBatch,
        key_index: usize,
        closing_index: Option<usize>,
    ) -> datafusion::common::Result<Self> {
        let ints = |i| {
            cast(batch.column(i), &DataType::Int64).map(|a| a.as_primitive::<Int64Type>().clone())
        };
        let keys = ints(key_index)?;
        let bound = closing_index.map(ints).transpose()?;
        Ok(Self {
            batch,
            keys,
            bound,
            row: 0,
            position: NeighborPosition::default(),
            walk: None,
            edge_rows: false,
            edge_mask: None,
        })
    }

    /// Collect only the next chunk; target properties are fetched after this returns.
    async fn next_chunk(
        &mut self,
        adjacency: &Neighbors,
        target: Option<&TargetLookup>,
        batch_size: usize,
    ) -> ExpandedRows {
        let mut chunk = ExpandedRows::default();
        loop {
            let paused = if self.edge_rows {
                self.fill_chunk::<true>(adjacency, target, batch_size, &mut chunk)
            } else {
                self.fill_chunk::<false>(adjacency, target, batch_size, &mut chunk)
            };
            if paused {
                cooperative_yield().await;
            }
            if self.row == self.keys.len() || chunk.rows.len() == batch_size.max(1) {
                return chunk;
            }
        }
    }

    /// Keep coroutine state outside the scan while counting deleted occurrences as work.
    fn fill_chunk<const ROWS: bool>(
        &mut self,
        adjacency: &Neighbors,
        target: Option<&TargetLookup>,
        batch_size: usize,
        chunk: &mut ExpandedRows,
    ) -> bool {
        let mut position = self.position;
        let mut row = self.row;
        let mut work = 0_usize;
        'rows: while row < self.keys.len() {
            let i = row;
            let previous_work = work;
            if self.keys.is_valid(i) && self.bound.as_ref().is_none_or(|b| b.is_valid(i)) {
                let key = self.keys.value(i);
                let required = self.bound.as_ref().map(|b| b.value(i));
                for candidate in adjacency
                    .candidates_from::<ROWS>(key, &mut position)
                    .take(1024 - work)
                {
                    work += 1;
                    let Some((neighbor, edge_row)) = candidate else {
                        continue;
                    };
                    if ROWS
                        && self
                            .edge_mask
                            .as_ref()
                            .is_some_and(|m| !m.contains(edge_row))
                    {
                        continue;
                    }
                    let neighbor = neighbor as i64;
                    if required.is_some_and(|r| r != neighbor) {
                        continue;
                    }
                    if let Some(target) = target {
                        let Some(target_row) = target.rows.position(neighbor) else {
                            continue;
                        };
                        chunk.target_positions.push(target_row);
                    }
                    chunk.rows.push(i as u32);
                    chunk.key_values.push(key);
                    chunk.neighbor_values.push(neighbor);
                    if ROWS {
                        chunk.edge_rows.push(edge_row);
                    }
                    if chunk.rows.len() == batch_size.max(1) {
                        break 'rows;
                    }
                }
                if work == 1024 {
                    break;
                }
            }
            if work == previous_work {
                work += 1;
            }
            row += 1;
            position = NeighborPosition::default();
            if work == 1024 {
                break;
            }
        }
        self.row = row;
        self.position = position;
        work == 1024
    }

    async fn next_path_chunk(
        &mut self,
        graph: &crate::csr_traversal::Traversal,
        target: Option<&TargetLookup>,
        batch_size: usize,
    ) -> ExpandedRows {
        let mut chunk = ExpandedRows::default();
        let mut source_work = 0_usize;
        while self.row < self.keys.len() {
            source_work += 1;
            if source_work.is_multiple_of(1024) {
                cooperative_yield().await;
            }
            let i = self.row;
            if self.keys.is_valid(i) && self.bound.as_ref().is_none_or(|b| b.is_valid(i)) {
                let key = self.keys.value(i);
                let walk = self
                    .walk
                    .get_or_insert_with(|| crate::csr_traversal::TraversalCursor::new(graph, key));
                let mut candidates = 0usize;
                while let Some(neighbor) = walk.next(graph).await {
                    candidates += 1;
                    if candidates.is_multiple_of(1024) {
                        cooperative_yield().await;
                    }
                    if self.bound.as_ref().is_some_and(|b| b.value(i) != neighbor) {
                        continue;
                    }
                    if let Some(target) = target {
                        let Some(position) = target.rows.position(neighbor) else {
                            continue;
                        };
                        chunk.target_positions.push(position);
                    }
                    chunk.rows.push(i as u32);
                    chunk.key_values.push(key);
                    chunk.neighbor_values.push(neighbor);
                    if chunk.rows.len() == batch_size.max(1) {
                        return chunk;
                    }
                }
            }
            self.row += 1;
            self.walk = None;
        }
        chunk
    }
}

/// Expanded rows of one input batch: input row index, both endpoints, the edge row
/// (single hops only), and the target row position when a lookup is fused in.
#[derive(Default)]
struct ExpandedRows {
    rows: Vec<u32>,
    key_values: Vec<i64>,
    neighbor_values: Vec<i64>,
    edge_rows: Vec<u64>,
    target_positions: Vec<u64>,
}

impl ExpandedRows {
    /// Keep the rows whose entry in `keep` is true.
    fn retain(&mut self, keep: &[bool]) {
        retain_by_mask(&mut self.rows, keep);
        retain_by_mask(&mut self.key_values, keep);
        retain_by_mask(&mut self.neighbor_values, keep);
        retain_by_mask(&mut self.edge_rows, keep);
        retain_by_mask(&mut self.target_positions, keep);
    }
}

/// Apply the moved target filter to `expanded`, then build the output batch.
#[allow(clippy::too_many_arguments)]
async fn materialize(
    batch: &RecordBatch,
    expanded: ExpandedRows,
    target: Option<&TargetLookup>,
    scanned: Option<&TargetColumns>,
    edge: Option<&EdgeLookup>,
    target_filter: Option<&CompiledFilter>,
    edge_filter: Option<&CompiledFilter>,
    schema: &SchemaRef,
) -> datafusion::common::Result<RecordBatch> {
    let mut expanded = expanded;
    if let (Some(edge), Some(filter)) = (edge, edge_filter) {
        // Fetch only the relationship filter's columns and drop failing edges first.
        let columns = edge
            .rows
            .take(&expanded.edge_rows, &filter.columns)
            .await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        expanded.retain(&filter.keep(columns)?);
    }
    let ExpandedRows {
        mut rows,
        mut key_values,
        mut neighbor_values,
        mut edge_rows,
        mut target_positions,
    } = expanded;
    let mut reused = Vec::new();
    if let (Some(target), Some(filter)) = (target, target_filter) {
        // Take only the filter's columns, evaluate, and keep passing rows before
        // taking anything else. Null counts as false, as in a WHERE clause.
        let fetch: Vec<_> = filter
            .columns
            .iter()
            .copied()
            .filter(|&c| c != target.rows.id_index)
            .collect();
        let mut taken = take_target(target, scanned, &target_positions, &fetch)
            .await?
            .into_iter();
        let arrays = filter
            .columns
            .iter()
            .enumerate()
            .map(|(i, &c)| {
                if c == target.rows.id_index {
                    cast(
                        &Int64Array::from(neighbor_values.clone()),
                        filter.schema.field(i).data_type(),
                    )
                } else {
                    Ok(taken
                        .next()
                        .expect("one fetched array per non-id filter column"))
                }
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let candidates = RecordBatch::try_new(filter.schema.clone(), arrays)?;
        let mask = filter
            .predicate
            .evaluate(&candidates)?
            .into_array(candidates.num_rows())?;
        let mask = mask.as_boolean();
        let keep: Vec<bool> = (0..mask.len())
            .map(|i| mask.is_valid(i) && mask.value(i))
            .collect();
        for (i, &column) in filter.columns.iter().enumerate() {
            if column != target.rows.id_index && target.columns.contains(&column) {
                reused.push((column, filter_array(candidates.column(i), mask)?));
            }
        }
        retain_by_mask(&mut rows, &keep);
        retain_by_mask(&mut key_values, &keep);
        retain_by_mask(&mut neighbor_values, &keep);
        retain_by_mask(&mut edge_rows, &keep);
        retain_by_mask(&mut target_positions, &keep);
    }
    let rows = UInt32Array::from(rows);
    let mut columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &rows, None))
        .collect::<std::result::Result<_, _>>()?;
    let width = batch.num_columns();
    let neighbor_values = Int64Array::from(neighbor_values);
    columns.push(cast(
        &Int64Array::from(key_values),
        schema.field(width).data_type(),
    )?);
    columns.push(cast(&neighbor_values, schema.field(width + 1).data_type())?);
    if let Some(target) = target {
        let id_index = target.rows.id_index;
        let fetch: Vec<usize> = target
            .columns
            .iter()
            .copied()
            .filter(|&c| c != id_index && !reused.iter().any(|(column, _)| *column == c))
            .collect();
        let mut fetched = take_target(target, scanned, &target_positions, &fetch)
            .await?
            .into_iter();
        for (&c, field) in target.columns.iter().zip(&target.fields) {
            // The target id equals the neighbour value, so it is never fetched.
            columns.push(if c == id_index {
                cast(&neighbor_values, field.data_type())?
            } else if let Some((_, array)) = reused.iter().find(|(column, _)| *column == c) {
                array.clone()
            } else {
                fetched.next().expect("one fetched array per non-id column")
            });
        }
    }
    if let Some(edge) = edge {
        let fetched = edge
            .rows
            .take(&edge_rows, &edge.columns)
            .await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        columns.extend(fetched);
    }
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// Keep the elements of `values` whose position in `keep` is true.
fn retain_by_mask<T>(values: &mut Vec<T>, keep: &[bool]) {
    let mut keep = keep.iter();
    values.retain(|_| *keep.next().expect("one mask entry per value"));
}

impl DisplayAs for CsrExpandExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CsrExpandExec: key={}, edges={}",
            self.key_column,
            self.adjacency.num_edges()
        )?;
        if self.adjacency.is_undirected() {
            write!(f, ", undirected")?;
        }
        if let Some(target) = &self.target {
            write!(f, ", take={}{}", target.variable, target.describe_columns())?;
        }
        if let Some(edge) = &self.edge {
            write!(f, ", edge={}{}", edge.variable, edge.describe_columns())?;
        }
        if let Some(filter) = &self.target_filter {
            write!(f, ", filter={}", filter.predicate)?;
        }
        if let Some(filter) = &self.edge_filter {
            write!(f, ", edge_filter={}", filter.predicate)?;
        }
        if let Some(column) = &self.closing_column {
            write!(f, ", match={}", column)?;
        }
        if let Some(walk) = &self.traversal {
            write!(f, ", iterative hops={}..{}", walk.min_hops, walk.max_hops)?;
        }
        Ok(())
    }
}

impl ExecutionPlan for CsrExpandExec {
    fn name(&self) -> &str {
        "CsrExpandExec"
    }

    /// Estimated output rows: input rows times the mean degree. Column statistics
    /// are unknown. Feeds DataFusion's join sizing above the expansion.
    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::common::Result<Arc<datafusion::common::Statistics>> {
        use datafusion::common::stats::Precision;
        let input = self.input.partition_statistics(partition)?;
        let mut stats = datafusion::common::Statistics::new_unknown(&self.schema);
        if self.traversal.is_some() {
            return Ok(Arc::new(stats));
        }
        let degree = self.adjacency.mean_degree();
        stats.num_rows = match input.num_rows {
            Precision::Exact(n) | Precision::Inexact(n) => {
                Precision::Inexact((n as f64 * degree).ceil() as usize)
            }
            Precision::Absent => Precision::Absent,
        };
        Ok(Arc::new(stats))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let input = children
            .into_iter()
            .next()
            .expect("CsrExpandExec has one child");
        Ok(Arc::new(Self::new(
            input,
            self.key_column.clone(),
            self.adjacency.clone(),
            self.target.clone(),
            self.edge.clone(),
            self.target_filter.clone(),
            self.edge_filter.clone(),
            self.closing_column.clone(),
            self.traversal.clone(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context.clone())?;
        // Resolve by name at execution time, so later column pruning cannot shift it.
        let key_index = input.schema().index_of(&self.key_column)?;
        let closing_index = self
            .closing_column
            .as_deref()
            .map(|c| input.schema().index_of(c))
            .transpose()?;
        let adjacency = self.adjacency.clone();
        let target = self.target.clone().map(Arc::new);
        let edge = self.edge.clone().map(Arc::new);
        let target_filter = self.target_filter.clone().map(Arc::new);
        let edge_filter = self.edge_filter.clone().map(Arc::new);
        let edge_state = self.edge_state.clone();
        let mask_after = (self.adjacency.num_edges() / EDGE_MASK_FRACTION).max(1);
        let target_state = self.target_state.clone();
        // Every non-id column the target fetches: its output and its filter's columns.
        let scan_columns: Arc<Vec<usize>> = Arc::new(
            self.target
                .as_ref()
                .map(|t| {
                    let filter = self.target_filter.iter().flat_map(|f| &f.columns);
                    let mut columns: Vec<usize> = t
                        .columns
                        .iter()
                        .chain(filter)
                        .copied()
                        .filter(|&c| c != t.rows.id_index)
                        .collect();
                    columns.sort_unstable();
                    columns.dedup();
                    columns
                })
                .unwrap_or_default(),
        );
        let batch_size = context.session_config().batch_size();
        // A hop that fetches at most one batch makes at most one take: never worth a scan.
        let scan_after = self.target.as_ref().map_or(1, |t| {
            (t.rows.num_rows() as u64 / TARGET_SCAN_FRACTION).max(batch_size as u64 + 1)
        });
        let schema = self.schema.clone();
        let out_schema = schema.clone();
        let traversal = self.traversal.clone();
        // Keep one input and one materialization in flight per partition. A hub's
        // remaining neighbours stay in the immutable adjacency until the next poll.
        let stream = futures::stream::try_unfold((input, None), move |(mut input, mut pending)| {
            let (adjacency, target, edge, target_filter, schema) = (
                adjacency.clone(),
                target.clone(),
                edge.clone(),
                target_filter.clone(),
                schema.clone(),
            );
            let (edge_filter, edge_state) = (edge_filter.clone(), edge_state.clone());
            let (target_state, scan_columns) = (target_state.clone(), scan_columns.clone());
            let traversal = traversal.clone();
            async move {
                loop {
                    if pending.is_none() {
                        let Some(batch) = input.next().await else {
                            return Ok(None);
                        };
                        let mut expansion =
                            BatchExpansion::try_new(batch?, key_index, closing_index)?;
                        expansion.edge_rows = edge.is_some();
                        pending = Some(expansion);
                    }
                    let expansion = pending.as_mut().expect("input batch just set");
                    // Once built, the mask filters edges during expansion instead.
                    let mask = edge_filter.as_ref().and(edge_state.mask.get().cloned());
                    let mut filter = if mask.is_some() {
                        None
                    } else {
                        edge_filter.as_deref()
                    };
                    expansion.edge_mask = mask;
                    let chunk = match &traversal {
                        Some(walk) => {
                            expansion
                                .next_path_chunk(walk, target.as_deref(), batch_size)
                                .await
                        }
                        None => {
                            expansion
                                .next_chunk(&adjacency, target.as_deref(), batch_size)
                                .await
                        }
                    };
                    if chunk.rows.is_empty() {
                        pending = None;
                        continue;
                    }
                    let mut chunk = chunk;
                    if let (Some(edge), Some(compiled)) = (&edge, filter) {
                        // Decide before fetching: a chunk that crosses the threshold is
                        // filtered by the new mask, without its own scattered fetch.
                        use std::sync::atomic::Ordering::Relaxed;
                        let expanded = chunk.rows.len() as u64;
                        if edge_state.expanded.fetch_add(expanded, Relaxed) + expanded >= mask_after
                        {
                            let _building = edge_state.building.lock().await;
                            if edge_state.mask.get().is_none() {
                                let mask = edge.rows.mask(compiled).await.map_err(|e| {
                                    datafusion::common::DataFusionError::External(Box::new(e))
                                })?;
                                let _ = edge_state.mask.set(Arc::new(mask));
                            }
                        }
                        if let Some(mask) = edge_state.mask.get() {
                            let keep: Vec<bool> =
                                chunk.edge_rows.iter().map(|&r| mask.contains(r)).collect();
                            chunk.retain(&keep);
                            filter = None;
                        }
                    }
                    if let Some(target) = target.as_deref().filter(|_| !scan_columns.is_empty()) {
                        // Same decision as the edge mask: this chunk already uses the read.
                        use std::sync::atomic::Ordering::Relaxed;
                        let fetched = chunk.target_positions.len() as u64;
                        if target_state.columns.get().is_none()
                            && target_state.fetched.fetch_add(fetched, Relaxed) + fetched
                                >= scan_after
                        {
                            let _building = target_state.building.lock().await;
                            if target_state.columns.get().is_none() {
                                let columns =
                                    target.rows.scan_columns(&scan_columns).await.map_err(|e| {
                                        datafusion::common::DataFusionError::External(Box::new(e))
                                    })?;
                                let _ = target_state.columns.set(columns.map(Arc::new));
                            }
                        }
                    }
                    let scanned = target_state.columns.get().cloned().flatten();
                    let batch = materialize(
                        &expansion.batch,
                        chunk,
                        target.as_deref(),
                        scanned.as_deref(),
                        edge.as_deref(),
                        target_filter.as_deref(),
                        filter,
                        &schema,
                    )
                    .await?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    return Ok(Some((batch, (input, pending))));
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

/// Lowers [`CsrExpandNode`] to [`CsrExpandExec`].
#[derive(Debug, Default)]
pub struct CsrExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for CsrExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<crate::apply::ApplyNode>() {
            return Ok(Some(crate::apply::plan_apply(
                node,
                physical_inputs,
                session_state,
            )));
        }
        if let Some(node) = node
            .as_any()
            .downcast_ref::<crate::path_match::PathMatchNode>()
        {
            return crate::path_match::plan_path_match(node, physical_inputs, session_state)
                .map(Some);
        }
        if let Some(node) = node
            .as_any()
            .downcast_ref::<crate::shortest_path::ShortestPathNode>()
        {
            return Ok(Some(crate::shortest_path::plan_shortest_path(
                node,
                physical_inputs,
            )));
        }
        let Some(node) = node.as_any().downcast_ref::<CsrExpandNode>() else {
            return Ok(None);
        };
        let target_filter = match (&node.target, &node.target_filter) {
            (Some(target), Some(filter)) => Some(target.compile_filter(filter, session_state)?),
            _ => None,
        };
        let edge_filter = match (&node.edge, &node.edge_filter) {
            (Some(edge), Some(filter)) => Some(edge.compile_filter(filter, session_state)?),
            _ => None,
        };
        Ok(Some(Arc::new(CsrExpandExec::new(
            physical_inputs[0].clone(),
            node.key_column.clone(),
            node.adjacency.clone(),
            node.target.clone(),
            node.edge.clone(),
            target_filter,
            edge_filter,
            node.closing_column.clone(),
            node.traversal.clone(),
            Arc::new(node.schema.as_arrow().clone()),
        ))))
    }
}

/// Query planner that adds [`CsrExtensionPlanner`]; install it on any
/// `SessionContext` that executes plans built with an [`AdjacencyCatalog`] or CALL subqueries.
#[derive(Debug, Default)]
pub struct CsrQueryPlanner;

#[async_trait]
impl QueryPlanner for CsrQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(CsrExtensionPlanner)])
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Optimizer rule: move WHERE conjuncts that read only a fused target's columns, or
/// only a named relationship's own columns, into the CSR node, which evaluates them
/// before taking that table's other columns.
///
/// Without this, `... WHERE tc.name = 'Athlete'` takes every projected target column
/// for every expanded row, then filters. The CSR node
/// instead takes just the filter's columns for each batch's candidates, evaluates the
/// predicate, and takes the remaining columns only for rows that pass; columns used
/// only by the filter are then pruned from the output. The cost follows the expanded
/// rows, never the target table's size. (Evaluating the filter once per query over the
/// whole target table was tried first: it cost more than it saved whenever the
/// expansion was already small, which re-rooting usually ensures.) Volatile conjuncts
/// stay put.
#[derive(Debug, Default)]
pub struct PushCsrTargetFilters;

impl OptimizerRule for PushCsrTargetFilters {
    fn name(&self) -> &str {
        "push_csr_target_filters"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::common::Result<Transformed<LogicalPlan>> {
        plan.transform_up(|node| {
            let LogicalPlan::Filter(filter) = &node else {
                return Ok(Transformed::no(node));
            };
            // Projection pushdown can leave column-only projections between the filter
            // and the CSR node; look through them (they are rebuilt unchanged below).
            let mut projections = Vec::new();
            let mut below = filter.input.as_ref();
            while let LogicalPlan::Projection(p) = below {
                if !p.expr.iter().all(|e| matches!(e, Expr::Column(_))) {
                    return Ok(Transformed::no(node));
                }
                projections.push(p.expr.clone());
                below = p.input.as_ref();
            }
            let LogicalPlan::Extension(ext) = below else {
                return Ok(Transformed::no(node));
            };
            let Some(csr) = ext.node.as_any().downcast_ref::<CsrExpandNode>() else {
                return Ok(Transformed::no(node));
            };
            let target_columns = csr.target.as_ref().map(TargetLookup::raw_columns);
            let edge_columns = csr.edge.as_ref().map(EdgeLookup::raw_columns);
            // Conjuncts over only one table's columns move; anything else stays here.
            let over = |e: &Expr, columns: &Option<HashMap<String, String>>| {
                let refs = e.column_refs();
                columns.as_ref().is_some_and(|columns| {
                    !refs.is_empty()
                        && !e.is_volatile()
                        && refs.iter().all(|c| columns.contains_key(&c.name))
                })
            };
            let (mut to_target, mut to_edge, mut kept) = (vec![], vec![], vec![]);
            for e in split_conjunction(&filter.predicate) {
                if over(e, &target_columns) {
                    to_target.push(e.clone());
                } else if over(e, &edge_columns) {
                    to_edge.push(e.clone());
                } else {
                    kept.push(e);
                }
            }
            if to_target.is_empty() && to_edge.is_empty() {
                return Ok(Transformed::no(node));
            }
            let mut pushed = csr.clone();
            pushed.target_filter = conjunction(csr.target_filter.iter().cloned().chain(to_target));
            pushed.edge_filter = conjunction(csr.edge_filter.iter().cloned().chain(to_edge));
            let mut rebuilt = LogicalPlan::Extension(Extension {
                node: Arc::new(pushed),
            });
            for exprs in projections.into_iter().rev() {
                rebuilt = LogicalPlanBuilder::from(rebuilt).project(exprs)?.build()?;
            }
            Ok(Transformed::yes(
                match conjunction(kept.into_iter().cloned()) {
                    Some(rest) => LogicalPlanBuilder::from(rebuilt).filter(rest)?.build()?,
                    None => rebuilt,
                },
            ))
        })
    }
}

/// Optimizer rule: drop fused target columns that nothing in the plan uses.
///
/// DataFusion's projection pushdown prunes an extension node's inputs but not its
/// own outputs, so without this every target column would be fetched per row.
/// "Used" is conservative: any column referenced by an expression, the plan's
/// output columns, every CSR node's key, and all inputs of nodes whose column use is
/// implicit (e.g. `DISTINCT`, `UNION`).
#[derive(Debug, Default)]
pub struct PruneCsrTargetColumns;

impl OptimizerRule for PruneCsrTargetColumns {
    fn name(&self) -> &str {
        "prune_csr_target_columns"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::common::Result<Transformed<LogicalPlan>> {
        let used = used_columns(&plan)?;
        plan.transform_up(|node| {
            if let LogicalPlan::Extension(ext) = &node {
                if let Some(csr) = ext.node.as_any().downcast_ref::<CsrExpandNode>() {
                    if let Some(pruned) = csr.with_target_columns(&used) {
                        return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                            node: Arc::new(pruned),
                        })));
                    }
                }
            }
            Ok(Transformed::no(node))
        })
    }
}

fn used_columns(plan: &LogicalPlan) -> datafusion::common::Result<HashSet<String>> {
    let mut used: HashSet<String> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    plan.apply_with_subqueries(|node| {
        for expr in node.expressions() {
            used.extend(expr.column_refs().into_iter().map(|c| c.name.clone()));
        }
        let explicit = match node {
            LogicalPlan::Extension(ext) => {
                match ext.node.as_any().downcast_ref::<CsrExpandNode>() {
                    Some(csr) => {
                        // Read by the node itself, not through an expression.
                        used.insert(csr.key_column.clone());
                        used.extend(csr.closing_column.clone());
                        true
                    }
                    None => false,
                }
            }
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Values(_) => true,
            _ => false,
        };
        if !explicit {
            for input in node.inputs() {
                used.extend(input.schema().fields().iter().map(|f| f.name().clone()));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(used)
}

fn plan_error(message: String) -> GraphError {
    GraphError::PlanError {
        message,
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

fn exec_error(message: String) -> GraphError {
    GraphError::ExecutionError {
        message,
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    fn edge_stream(src: Vec<Option<i64>>, dst: Vec<Option<i64>>) -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int64, true),
            Field::new("dst", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(src)),
                Arc::new(Int64Array::from(dst)),
            ],
        )
        .unwrap();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(vec![Ok(batch)]),
        ))
    }

    #[tokio::test]
    async fn build_keeps_duplicates_skips_nulls_and_handles_large_ids() {
        let big = 1_i64 << 40;
        let stream = edge_stream(
            vec![Some(big), Some(big), Some(7), None, Some(7)],
            vec![Some(1), Some(1), Some(-3), Some(5), None],
        );
        let adj = Adjacency::build(stream, "SRC", "dst").await.unwrap();
        assert_eq!(adj.num_edges(), 3);
        assert_eq!(neighbors(&adj, big), vec![1, 1]);
        assert_eq!(neighbors(&adj, 7), vec![-3]);
        assert!(neighbors(&adj, 42).is_empty());
    }

    #[tokio::test]
    async fn undirected_neighbors_visit_both_directions_and_self_loops_once() {
        let edges = || {
            edge_stream(
                vec![Some(1), Some(1), Some(3)],
                vec![Some(2), Some(3), Some(3)],
            )
        };
        let out = Adjacency::build(edges(), "src", "dst").await.unwrap();
        let inc = Adjacency::build(edges(), "dst", "src").await.unwrap();
        let both = Neighbors::undirected(Arc::new(out), Arc::new(inc));
        let collect = |key| {
            let mut v = Vec::new();
            both.for_each_neighbor(key, |n| v.push(n as i64));
            v
        };
        assert_eq!(collect(1), vec![2, 3]);
        assert_eq!(collect(2), vec![1]);
        assert_eq!(collect(3), vec![3, 1]);
    }

    /// Rows of an in-memory edge table, by index.
    fn rows(indices: impl IntoIterator<Item = u64>) -> RemovedRows {
        let mut removed = RemovedRows::default();
        removed.rows(0, indices.into_iter().map(|i| i as u32));
        removed
    }

    fn neighbors(adj: &Adjacency, key: i64) -> Vec<i64> {
        let mut out = Vec::new();
        adj.for_each_neighbor(key, |n| out.push(n as i64));
        out.sort_unstable();
        out
    }

    #[tokio::test]
    async fn changes_layer_over_shared_base_with_multiset_tombstones() {
        let base = Adjacency::build(
            edge_stream(
                vec![Some(1), Some(1), Some(1)],
                vec![Some(2), Some(2), Some(3)],
            ),
            "src",
            "dst",
        )
        .await
        .unwrap();
        // Remove one of the two 1->2 edges, add 1->4 and a new source 9->1.
        let next = base.with_changes(&[(1, 4, 3), (9, 1, 4)], &rows([0]));
        assert_eq!(neighbors(&next, 1), vec![2, 3, 4]);
        assert_eq!(neighbors(&next, 9), vec![1]);
        assert_eq!(
            (next.num_edges(), next.delta_edges(), next.tombstones()),
            (4, 2, 1)
        );
        assert_eq!(next.unindexed_edges(), 2);
        // Removing an appended edge cancels it; the original is untouched.
        let later = next.with_changes(&[], &rows([4]));
        assert!(neighbors(&later, 9).is_empty());
        assert_eq!((later.tombstones(), later.unindexed_edges()), (2, 1));
        assert_eq!(neighbors(&base, 1), vec![2, 2, 3]);
        assert!(Arc::ptr_eq(&base.base, &later.base));
    }

    #[tokio::test]
    async fn resumed_neighbors_cross_the_base_and_delta_boundary() {
        let base = Adjacency::build(
            edge_stream(vec![Some(1), Some(1)], vec![Some(10), Some(11)]),
            "src",
            "dst",
        )
        .await
        .unwrap();
        let next = base
            .with_changes(&[(1, 12, 2), (1, 12, 3), (1, 13, 4)], &rows([]))
            .with_changes(&[], &rows([1, 2]));
        // Row 1 is a base edge, row 2 an appended one.
        assert_eq!((next.tombstones(), next.unindexed_edges()), (2, 2));
        for (mut offset, expected) in [
            (0, vec![10, 12, 13]),
            (1, vec![12, 13]),
            (2, vec![12, 13]),
            (3, vec![12, 13]),
            (4, vec![13]),
            (5, vec![]),
            (6, vec![]),
        ] {
            assert_eq!(
                next.neighbors_from(1, &mut offset).collect::<Vec<_>>(),
                expected
            );
        }
        assert_eq!(neighbors(&base, 1), vec![10, 11]);
    }

    #[tokio::test]
    async fn debug_output_does_not_grow_with_edges() {
        let n = 10_000;
        let adj = Adjacency::build(
            edge_stream((0..n).map(Some).collect(), (0..n).map(Some).collect()),
            "src",
            "dst",
        )
        .await
        .unwrap();
        assert!(format!("{:?}", adj).len() < 100);
    }

    #[test]
    fn node_rows_reject_duplicate_ids_and_skip_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new("ID", DataType::Int32, true)]));
        let batch = |ids: Vec<Option<i32>>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow::array::Int32Array::from(ids))],
            )
            .unwrap()
        };
        let rows = NodeRows::from_batch(batch(vec![Some(5), None, Some(7)]), "id")
            .unwrap()
            .unwrap();
        assert_eq!(
            (rows.num_rows(), rows.position(7), rows.position(9)),
            (2, Some(2), None)
        );
        assert!(NodeRows::from_batch(batch(vec![Some(5), Some(5)]), "id")
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn many_small_changes_match_a_reference_multiset() {
        let mut adj = Adjacency::build(edge_stream(vec![Some(0)], vec![Some(0)]), "src", "dst")
            .await
            .unwrap();
        // (neighbour, row) pairs; row 0 is the base edge.
        let mut live = vec![(0_i64, 0_u64)];
        for step in 1..=200_i64 {
            let removed = match step % 3 {
                0 => rows(live.pop().map(|(_, row)| row)),
                _ => rows([]),
            };
            let row = 2 * step as u64;
            adj = adj.with_changes(&[(0, step, row - 1), (0, step, row)], &removed);
            live.extend([(step, row - 1), (step, row)]);
        }
        let mut expected: Vec<i64> = live.into_iter().map(|(n, _)| n).collect();
        expected.sort_unstable();
        assert_eq!(neighbors(&adj, 0), expected);
        assert_eq!(adj.num_edges() as usize, expected.len());
    }

    #[tokio::test]
    async fn expansion_repeats_rows_per_neighbor_and_resumes_between_chunks() {
        let adj = Adjacency::build(
            edge_stream(vec![Some(1), Some(1)], vec![Some(2), Some(3)]),
            "src",
            "dst",
        )
        .await
        .unwrap();
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "a__id",
            DataType::Int32,
            true,
        )]));
        let input = RecordBatch::try_new(
            input_schema,
            vec![Arc::new(arrow::array::Int32Array::from(vec![
                Some(1),
                None,
                Some(9),
            ]))],
        )
        .unwrap();
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("a__id", DataType::Int32, true),
            Field::new("r__src", DataType::Int32, true),
            Field::new("r__dst", DataType::Int32, true),
        ]));
        let adj = Neighbors::directed(Arc::new(adj));
        let mut expansion = BatchExpansion::try_new(input.clone(), 0, None).unwrap();
        let chunk = expansion.next_chunk(&adj, None, 8192).await;
        let out = materialize(&input, chunk, None, None, None, None, None, &out_schema)
            .await
            .unwrap();
        assert_eq!(out.num_rows(), 2);
        let dst = out.column(2).as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(dst.values().to_vec(), vec![2, 3]);
        assert!(expansion.next_chunk(&adj, None, 8192).await.rows.is_empty());
        // Output is split at `batch_size` rows.
        let mut expansion = BatchExpansion::try_new(input, 0, None).unwrap();
        assert_eq!(
            expansion.next_chunk(&adj, None, 1).await.neighbor_values,
            vec![2]
        );
        assert_eq!(expansion.row, 0);
        assert_eq!(expansion.position.forward, 1);
        assert_eq!(
            expansion.next_chunk(&adj, None, 1).await.neighbor_values,
            vec![3]
        );
        assert!(expansion.next_chunk(&adj, None, 1).await.rows.is_empty());
    }

    #[tokio::test]
    async fn cancellation_without_edges_in_variable_paths_yields_between_source_rows() {
        let config = crate::config::GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table("KNOWS", "Person", "Person", "src", "dst")
            .build()
            .unwrap();
        let nodes =
            RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
                .unwrap();
        let mut catalog = AdjacencyCatalog::default();
        catalog.insert_nodes(
            "Person",
            Arc::new(NodeRows::from_batch(nodes, "id").unwrap().unwrap()),
        );
        catalog.insert(
            "KNOWS",
            true,
            Arc::new(
                Adjacency::build(edge_stream(vec![], vec![]), "src", "dst")
                    .await
                    .unwrap(),
            ),
        );
        let walk = crate::csr_traversal::Traversal::new(
            &config,
            &catalog,
            &["KNOWS".into()],
            &crate::ast::RelationshipDirection::Outgoing,
            "Person",
            "Person",
            1,
            1,
        )
        .unwrap();
        let input = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from(vec![1; 100_000])) as ArrayRef,
        )])
        .unwrap();
        let mut expansion = BatchExpansion::try_new(input, 0, None).unwrap();
        let mut next = Box::pin(expansion.next_path_chunk(&walk, None, 1));
        assert!(futures::poll!(next.as_mut()).is_pending());
        drop(next);
        assert!(expansion.row > 0 && expansion.row <= 1024);
        assert!(expansion
            .next_path_chunk(&walk, None, 1)
            .await
            .rows
            .is_empty());
    }

    #[tokio::test]
    async fn cancellation_without_edges_yields_while_skipping_source_rows() {
        let adjacency = Neighbors::directed(Arc::new(
            Adjacency::build(edge_stream(vec![Some(1)], vec![Some(2)]), "src", "dst")
                .await
                .unwrap(),
        ));
        for missing in [Some(99), None] {
            let input = RecordBatch::try_from_iter([(
                "id",
                Arc::new(Int64Array::from(vec![missing; 100_000])) as ArrayRef,
            )])
            .unwrap();
            let mut expansion = BatchExpansion::try_new(input, 0, None).unwrap();
            let mut next = Box::pin(expansion.next_chunk(&adjacency, None, 1));
            assert!(futures::poll!(next.as_mut()).is_pending());
            drop(next);
            assert!(expansion.row > 0 && expansion.row <= 1024);
            assert!(expansion
                .next_chunk(&adjacency, None, 1)
                .await
                .rows
                .is_empty());
        }
    }

    #[tokio::test]
    async fn expansion_yields_before_scanning_all_rejected_neighbors() {
        let adj = Neighbors::directed(Arc::new(
            Adjacency::build(
                edge_stream(vec![Some(1); 100_000], (2..100_002).map(Some).collect()),
                "src",
                "dst",
            )
            .await
            .unwrap(),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("bound", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![-1])),
            ],
        )
        .unwrap();
        let mut expansion = BatchExpansion::try_new(batch, 0, Some(1)).unwrap();
        let mut next = Box::pin(expansion.next_chunk(&adj, None, 1));
        assert!(futures::poll!(next.as_mut()).is_pending());
        drop(next);
        assert!(expansion.position.forward <= 1024);
        assert!(expansion.position.forward > 0);
    }

    #[tokio::test]
    async fn expansion_yields_and_resumes_through_a_long_tombstone_run() {
        let n = 5_000;
        let base = Adjacency::build(
            edge_stream(vec![Some(1); n], (2..n as i64 + 2).map(Some).collect()),
            "src",
            "dst",
        )
        .await
        .unwrap();
        // Every edge but the last.
        let adjacency =
            Neighbors::directed(Arc::new(base.with_changes(&[], &rows(0..n as u64 - 1))));
        let input =
            RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
                .unwrap();
        let mut expansion = BatchExpansion::try_new(input, 0, None).unwrap();
        let mut next = Box::pin(expansion.next_chunk(&adjacency, None, 1));
        assert!(futures::poll!(next.as_mut()).is_pending());
        drop(next);
        assert_eq!(expansion.position.forward, 1024);
        assert_eq!(
            expansion
                .next_chunk(&adjacency, None, 1)
                .await
                .neighbor_values,
            vec![n as i64 + 1]
        );
        assert!(expansion
            .next_chunk(&adjacency, None, 1)
            .await
            .rows
            .is_empty());
    }

    #[tokio::test]
    async fn high_degree_expansion_visits_only_the_first_chunk() {
        let n = 100_000;
        let base = Adjacency::build(
            edge_stream(vec![Some(0); n], (1..=n as i64).map(Some).collect()),
            "src",
            "dst",
        )
        .await
        .unwrap();
        // Odd neighbours: rows 0, 2, 4, ...
        let removed = rows((0..n as u64).step_by(2));
        let adj = Neighbors::directed(Arc::new(base.with_changes(&[], &removed)));
        let input =
            RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![0])) as ArrayRef)])
                .unwrap();
        let mut expansion = BatchExpansion::try_new(input, 0, None).unwrap();
        let chunk = expansion.next_chunk(&adj, None, 32).await;
        assert_eq!(
            chunk.neighbor_values,
            (2..=64).step_by(2).collect::<Vec<_>>()
        );
        assert_eq!(chunk.rows.len(), 32);
        // The rest of the hub, including its tombstones, has not been visited.
        assert_eq!(expansion.row, 0);
        assert_eq!(expansion.position.forward, 64);
        assert_eq!(
            expansion.next_chunk(&adj, None, 32).await.neighbor_values[0],
            66
        );
        assert_eq!(neighbors(&base, 0).len(), n);
    }

    #[tokio::test]
    async fn chunk_boundaries_preserve_closing_edges_and_missing_target_rows() {
        let adjacency = Adjacency::build(
            edge_stream(vec![Some(1); 4], vec![Some(2), Some(2), Some(3), Some(999)]),
            "src",
            "dst",
        )
        .await
        .unwrap();
        let adjacency = Neighbors::directed(Arc::new(adjacency));
        let input = RecordBatch::try_from_iter([
            (
                "key",
                Arc::new(Int64Array::from(vec![Some(1), Some(1), None, Some(1)])) as ArrayRef,
            ),
            (
                "bound",
                Arc::new(Int64Array::from(vec![Some(2), None, Some(2), Some(3)])) as ArrayRef,
            ),
        ])
        .unwrap();
        let nodes =
            RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![2])) as ArrayRef)])
                .unwrap();
        let target = TargetLookup::new(
            "b".to_string(),
            Arc::new(NodeRows::from_batch(nodes, "id").unwrap().unwrap()),
            vec![Field::new("b__id", DataType::Int64, false)],
        );
        let mut expansion = BatchExpansion::try_new(input, 0, Some(1)).unwrap();
        for _ in 0..2 {
            let chunk = expansion.next_chunk(&adjacency, Some(&target), 1).await;
            assert_eq!(chunk.rows, vec![0]);
            assert_eq!(chunk.neighbor_values, vec![2]);
            assert_eq!(chunk.target_positions, vec![0]);
        }
        assert!(expansion
            .next_chunk(&adjacency, Some(&target), 1)
            .await
            .rows
            .is_empty());
    }

    #[tokio::test]
    async fn resumed_neighbors_keep_duplicate_deletions_and_self_loops_exact() {
        let edges = || edge_stream(vec![Some(1); 4], vec![Some(1), Some(2), Some(2), Some(3)]);
        let base = Adjacency::build(edges(), "src", "dst").await.unwrap();
        // Rows: 0 = 1->1, 1 and 2 = 1->2, 3 = 1->3.
        let next = base.with_changes(&[(1, 2, 4), (1, 4, 5)], &rows([1, 3]));
        let later = next.with_changes(&[(1, 3, 6)], &rows([2, 4]));
        assert_eq!(neighbors(&later, 1), vec![1, 3, 4]);
        assert_eq!(neighbors(&next, 1), vec![1, 2, 2, 4]);
        assert_eq!(neighbors(&base, 1), vec![1, 2, 2, 3]);

        let reverse = Adjacency::build(edges(), "dst", "src").await.unwrap();
        let both = Neighbors::undirected(Arc::new(base), Arc::new(reverse));
        let mut position = NeighborPosition::default();
        let mut got = Vec::new();
        while let Some(n) = both.neighbors_from(1, &mut position).next() {
            got.push(n);
        }
        assert_eq!(got, vec![1, 2, 2, 3]);
        let mut position = NeighborPosition::default();
        assert_eq!(both.neighbors_from(2, &mut position).next(), Some(1));
        assert_eq!(both.neighbors_from(2, &mut position).next(), Some(1));
        assert_eq!(both.neighbors_from(2, &mut position).next(), None);
    }

    #[tokio::test]
    async fn materialization_reuses_filter_columns_for_memory_and_lance() {
        use arrow::array::{RecordBatchIterator, StringArray};
        use datafusion::logical_expr::{col, lit};
        use std::sync::atomic::Ordering;

        let nodes = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])) as ArrayRef,
            ),
            (
                "age",
                Arc::new(Int64Array::from(vec![Some(2), None, Some(5), Some(1)])) as ArrayRef,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec!["ten", "twenty", "thirty", "forty"])) as ArrayRef,
            ),
            (
                "unused",
                Arc::new(StringArray::from(vec!["unused payload"; 4])) as ArrayRef,
            ),
        ])
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nodes.lance");
        let reader = RecordBatchIterator::new(vec![Ok(nodes.clone())], nodes.schema());
        let dataset = Dataset::write(
            reader,
            path.to_str().unwrap(),
            None::<lance::dataset::WriteParams>,
        )
        .await
        .unwrap();
        let sources = [
            NodeRows::from_batch(nodes.clone(), "id").unwrap().unwrap(),
            NodeRows::from_lance(Arc::new(dataset), "id")
                .await
                .unwrap()
                .unwrap(),
        ];
        let input = RecordBatch::try_from_iter([(
            "a__id",
            Arc::new(Int64Array::from(vec![999])) as ArrayRef,
        )])
        .unwrap();
        let state = datafusion::execution::context::SessionContext::new().state();
        for source in sources {
            let source = Arc::new(source);
            for selected in [vec![0, 1, 2], vec![0, 1], vec![2], vec![]] {
                let fields: Vec<_> = selected
                    .iter()
                    .map(|&i| {
                        let f = nodes.schema().field(i).clone();
                        Field::new(
                            format!("b__{}", f.name()),
                            f.data_type().clone(),
                            f.is_nullable(),
                        )
                    })
                    .collect();
                let target = TargetLookup {
                    variable: "b".into(),
                    rows: source.clone(),
                    columns: selected.clone(),
                    fields: fields.clone(),
                };
                let schema = Arc::new(Schema::new(
                    [
                        Field::new("a__id", DataType::Int64, false),
                        Field::new("edge__src", DataType::Int64, false),
                        Field::new("edge__dst", DataType::Int64, false),
                    ]
                    .into_iter()
                    .chain(fields)
                    .collect::<Vec<_>>(),
                ));
                for threshold in [2, 99] {
                    source.fetched_cells.store(0, Ordering::Relaxed);
                    let predicate = col("b__age")
                        .gt_eq(lit(threshold))
                        .and(col("b__id").gt_eq(lit(10)));
                    let filter = target.compile_filter(&predicate, &state).unwrap();
                    let neighbors = vec![30, 10, 20, 30, 40, 10];
                    let expanded = ExpandedRows {
                        rows: vec![0; 6],
                        key_values: vec![999; 6],
                        target_positions: neighbors
                            .iter()
                            .map(|&n| source.position(n).unwrap())
                            .collect(),
                        neighbor_values: neighbors,
                        edge_rows: vec![],
                    };
                    let out = materialize(
                        &input,
                        expanded,
                        Some(&target),
                        None,
                        None,
                        Some(&filter),
                        None,
                        &schema,
                    )
                    .await
                    .unwrap();
                    let expected_rows: Vec<u32> = if threshold == 2 {
                        vec![2, 0, 2, 0]
                    } else {
                        vec![]
                    };
                    assert_eq!(out.num_rows(), expected_rows.len());
                    for (i, &column) in selected.iter().enumerate() {
                        let expected = take(
                            nodes.column(column),
                            &UInt32Array::from(expected_rows.clone()),
                            None,
                        )
                        .unwrap();
                        assert_eq!(out.column(3 + i).to_data(), expected.to_data());
                    }
                    // Only age is read for six candidates; only name needs a later read.
                    let projection_cells = if selected.contains(&2) {
                        expected_rows.len()
                    } else {
                        0
                    };
                    assert_eq!(
                        source.fetched_cells.load(Ordering::Relaxed),
                        6 + projection_cells
                    );
                }
            }
        }
    }

    #[test]
    fn edge_mask_marks_rows_per_fragment() {
        let mut mask = EdgeMask::default();
        mask.extend([5, 64, (3 << 32) | 130].into_iter());
        assert!(mask.contains(5) && mask.contains(64) && mask.contains((3 << 32) | 130));
        assert!(!mask.contains(6) && !mask.contains(130) && !mask.contains((3 << 32) | 5));
        assert!(!mask.contains((9 << 32) | 5) && !mask.contains(1 << 20));
    }

    #[tokio::test]
    async fn relationship_filter_runs_per_chunk_or_as_a_mask() {
        use arrow::array::RecordBatchIterator;
        use datafusion::logical_expr::{col, lit};

        // Rows: 1->2 (since 10), 1->3 (20), 1->2 (30), 2->3 (40).
        let edges = RecordBatch::try_from_iter([
            (
                "src",
                Arc::new(Int64Array::from(vec![1, 1, 1, 2])) as ArrayRef,
            ),
            (
                "dst",
                Arc::new(Int64Array::from(vec![2, 3, 2, 3])) as ArrayRef,
            ),
            (
                "since",
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])) as ArrayRef,
            ),
        ])
        .unwrap();
        let stream = || {
            Box::pin(RecordBatchStreamAdapter::new(
                edges.schema(),
                futures::stream::iter(vec![Ok(edges.clone())]),
            ))
        };
        let adjacency = Neighbors::directed(Arc::new(
            Adjacency::build(stream(), "src", "dst").await.unwrap(),
        ));
        let rows = Arc::new(EdgeRows::from_batch(edges.clone()));
        let edge =
            EdgeLookup::new("r", rows.clone(), &["src", "dst", "since"], ["src", "dst"]).unwrap();
        let state = datafusion::execution::context::SessionContext::new().state();
        let filter = edge
            .compile_filter(&col("r__since").gt(lit(15_i64)), &state)
            .unwrap();
        let input = RecordBatch::try_from_iter([(
            "a__id",
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        )])
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a__id", DataType::Int64, true),
            Field::new("r__src", DataType::Int64, true),
            Field::new("r__dst", DataType::Int64, true),
            Field::new("r__since", DataType::Int64, true),
        ]));
        let since = |batch: &RecordBatch| {
            let mut v = batch
                .column(3)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec();
            v.sort_unstable();
            v
        };

        // Per chunk: the filter column is fetched by edge row, then the rest for survivors.
        let mut expansion = BatchExpansion::try_new(input.clone(), 0, None).unwrap();
        expansion.edge_rows = true;
        let chunk = expansion.next_chunk(&adjacency, None, 8192).await;
        assert_eq!(chunk.edge_rows.len(), 3);
        let out = materialize(
            &input,
            chunk,
            None,
            None,
            Some(&edge),
            None,
            Some(&filter),
            &schema,
        )
        .await
        .unwrap();
        assert_eq!(since(&out), vec![20, 30]);

        // As a mask: failing edges are skipped during expansion, before any take.
        let mask = Arc::new(rows.mask(&filter).await.unwrap());
        assert!(!mask.contains(0) && mask.contains(1) && mask.contains(2) && mask.contains(3));
        let mut expansion = BatchExpansion::try_new(input.clone(), 0, None).unwrap();
        expansion.edge_rows = true;
        expansion.edge_mask = Some(mask);
        let chunk = expansion.next_chunk(&adjacency, None, 8192).await;
        assert_eq!(chunk.edge_rows, vec![1, 2]);
        let out = materialize(&input, chunk, None, None, Some(&edge), None, None, &schema)
            .await
            .unwrap();
        assert_eq!(since(&out), vec![20, 30]);

        // From Lance, across fragments: the mask holds exactly the passing row addresses.
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("edges.lance");
        let reader = RecordBatchIterator::new(vec![Ok(edges.clone())], edges.schema());
        let params = lance::dataset::WriteParams {
            max_rows_per_file: 2,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, path.to_str().unwrap(), Some(params))
            .await
            .unwrap();
        assert_eq!(dataset.get_fragments().len(), 2);
        let lance_rows = EdgeRows::from_lance(Arc::new(dataset));
        let mask = lance_rows.mask(&filter).await.unwrap();
        let (first, second) = (0_u64, 1_u64 << 32);
        assert!(!mask.contains(first) && mask.contains(first + 1));
        assert!(mask.contains(second) && mask.contains(second + 1));
    }

    #[tokio::test]
    async fn target_columns_read_once_match_fetches_by_row_id() {
        use arrow::array::{RecordBatchIterator, StringArray};

        let nodes = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from((0..7).collect::<Vec<i64>>())) as ArrayRef,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("c"),
                    Some("d"),
                    Some("e"),
                    Some("f"),
                    Some("g"),
                ])) as ArrayRef,
            ),
            (
                "age",
                Arc::new(Int64Array::from(vec![70, 71, 72, 73, 74, 75, 76])) as ArrayRef,
            ),
        ])
        .unwrap();
        assert!(NodeRows::from_batch(nodes.clone(), "id")
            .unwrap()
            .unwrap()
            .scan_columns(&[1])
            .await
            .unwrap()
            .is_none());
        // Three fragments and a deletion, so row ids are neither dense nor offsets.
        for stable in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("nodes.lance");
            let reader = RecordBatchIterator::new(vec![Ok(nodes.clone())], nodes.schema());
            let params = lance::dataset::WriteParams {
                max_rows_per_file: 3,
                enable_stable_row_ids: stable,
                ..Default::default()
            };
            let mut dataset = Dataset::write(reader, path.to_str().unwrap(), Some(params))
                .await
                .unwrap();
            dataset.delete("id = 1").await.unwrap();
            let rows = NodeRows::from_lance(Arc::new(dataset), "id")
                .await
                .unwrap()
                .unwrap();
            let scanned = rows.scan_columns(&[1, 2]).await.unwrap().unwrap();
            let positions: Vec<u64> = [6, 0, 4, 6, 3]
                .iter()
                .map(|&id| rows.position(id).unwrap())
                .collect();
            for columns in [vec![1, 2], vec![2], vec![]] {
                let expected = rows.take(&positions, &columns).await.unwrap();
                let got = scanned.take(&positions, &columns).unwrap();
                assert_eq!(got.len(), expected.len());
                for (got, expected) in got.iter().zip(&expected) {
                    assert_eq!(got.to_data(), expected.to_data(), "stable ids: {stable}");
                }
            }
            assert!(scanned.take(&[u64::MAX], &[2]).is_err());
        }
    }
}
