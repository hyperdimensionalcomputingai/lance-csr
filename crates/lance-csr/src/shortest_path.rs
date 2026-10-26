// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! `shortestPath` / `allShortestPaths` as a breadth-first search over the adjacency.
//!
//! [`ShortestPathNode`] takes rows that already bind both end nodes and appends, per
//! matching path, the path's length and its node ids (`<path>__length`, `<path>__nodes`),
//! repeating the input row once per path. Rows with no path within the length range are
//! dropped, as MATCH requires; OPTIONAL MATCH keeps them with nulls through its left join.
//!
//! The adjacency is the engine's CSR when it has one (`use_csr=True`). Otherwise the node has
//! a second input, a scan of the relationship's two endpoint columns, and builds an adjacency
//! from it once per query, shared by all partitions.
//!
//! One search runs per distinct source id in a batch, level by level, until every target in
//! that batch is reached at its shortest depth (or the depth limit). Predecessors are kept
//! per node, so all shortest paths can be enumerated back from each target.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::Int64Builder;
use arrow::array::{Array, ArrayRef, AsArray, Int64Array, ListBuilder, RecordBatch, UInt32Array};
use arrow::compute::{cast, take};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::lock::Mutex;
use futures::StreamExt;

use crate::csr_expand::{Adjacency, Neighbors};
use crate::error::{GraphError, Result};

type FastMap<K, V> = HashMap<K, V, ahash::RandomState>;

/// Which way the search follows edges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum SearchDirection {
    Outgoing,
    Incoming,
    Undirected,
}

/// The search parameters shared by the logical and physical nodes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct ShortestPathSpec {
    /// Input columns holding the source and target node ids.
    pub source_column: String,
    pub target_column: String,
    pub direction: SearchDirection,
    pub min_hops: u32,
    pub max_hops: u32,
    /// Every shortest path, not just one.
    pub all: bool,
    /// Output column names.
    pub length_column: String,
    pub nodes_column: String,
    /// Endpoint columns of the edge scan (the second input), when there is no CSR.
    pub edge_source: String,
    pub edge_target: String,
}

/// Logical node; see the module docs.
#[derive(Debug, Clone)]
pub struct ShortestPathNode {
    input: LogicalPlan,
    /// Scan of the relationship's endpoints; `None` when `adjacency` is given.
    edges: Option<LogicalPlan>,
    adjacency: Option<Neighbors>,
    spec: ShortestPathSpec,
    schema: DFSchemaRef,
}

impl ShortestPathNode {
    pub fn try_new(
        input: LogicalPlan,
        edges: Option<LogicalPlan>,
        adjacency: Option<Neighbors>,
        spec: ShortestPathSpec,
    ) -> Result<Self> {
        if edges.is_none() == adjacency.is_none() {
            return Err(plan_error(
                "shortestPath needs exactly one of a CSR or an edge scan",
            ));
        }
        let added = DFSchema::try_from(Schema::new(vec![
            Field::new(&spec.length_column, DataType::Int64, true),
            Field::new(
                &spec.nodes_column,
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ]))
        .map_err(|e| plan_error(&format!("shortestPath schema: {e}")))?;
        let schema = input
            .schema()
            .join(&added)
            .map_err(|e| plan_error(&format!("shortestPath schema: {e}")))?;
        Ok(Self {
            input,
            edges,
            adjacency,
            spec,
            schema: Arc::new(schema),
        })
    }
}

impl PartialEq for ShortestPathNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.edges == other.edges
            && self.spec == other.spec
            && match (&self.adjacency, &other.adjacency) {
                (Some(a), Some(b)) => a.ptr_eq(b),
                (None, None) => true,
                _ => false,
            }
    }
}
impl Eq for ShortestPathNode {}
impl Hash for ShortestPathNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.spec.hash(state);
    }
}
impl PartialOrd for ShortestPathNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.spec.partial_cmp(&other.spec)
    }
}

impl UserDefinedLogicalNodeCore for ShortestPathNode {
    fn name(&self) -> &str {
        "ShortestPath"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        std::iter::once(&self.input)
            .chain(self.edges.as_ref())
            .collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = &self.spec;
        write!(
            f,
            "ShortestPath: {} -> {}, {:?}, hops {}..{}, {}, {}",
            s.source_column,
            s.target_column,
            s.direction,
            s.min_hops,
            s.max_hops,
            if s.all { "all" } else { "one" },
            if self.adjacency.is_some() {
                "csr"
            } else {
                "edge scan"
            }
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        let edges = (inputs.len() > 1).then(|| inputs.pop().expect("two inputs"));
        let input = inputs.pop().expect("an input");
        Self::try_new(input, edges, self.adjacency.clone(), self.spec.clone())
            .map_err(|e| datafusion::common::DataFusionError::Plan(e.to_string()))
    }
}

/// Physical operator for [`ShortestPathNode`].
#[derive(Debug)]
pub struct ShortestPathExec {
    input: Arc<dyn ExecutionPlan>,
    /// Single-partition edge scan, when there is no CSR.
    edges: Option<Arc<dyn ExecutionPlan>>,
    /// The CSR, or the adjacency built from `edges` by the first batch that needs it.
    adjacency: Arc<Mutex<Option<Neighbors>>>,
    spec: ShortestPathSpec,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl ShortestPathExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        edges: Option<Arc<dyn ExecutionPlan>>,
        adjacency: Option<Neighbors>,
        spec: ShortestPathSpec,
        schema: SchemaRef,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            edges,
            adjacency: Arc::new(Mutex::new(adjacency)),
            spec,
            schema,
            properties,
        }
    }
}

impl DisplayAs for ShortestPathExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let s = &self.spec;
        write!(
            f,
            "ShortestPathExec: {} -> {}, {:?}, hops {}..{}, {}",
            s.source_column,
            s.target_column,
            s.direction,
            s.min_hops,
            s.max_hops,
            if s.all { "all" } else { "one" }
        )
    }
}

impl ExecutionPlan for ShortestPathExec {
    fn name(&self) -> &str {
        "ShortestPathExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        std::iter::once(&self.input)
            .chain(self.edges.as_ref())
            .collect()
    }

    /// The edge scan must be one partition: the build reads it as a single stream. Without
    /// this, DataFusion's distribution rule drops a coalesce added at planning time and the
    /// build sees only partition 0's edges.
    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        use datafusion::physical_plan::Distribution;
        std::iter::once(Distribution::UnspecifiedDistribution)
            .chain(self.edges.as_ref().map(|_| Distribution::SinglePartition))
            .collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let edges = (children.len() > 1).then(|| children.pop().expect("two children"));
        let input = children.pop().expect("a child");
        let mut exec = Self::new(input, edges, None, self.spec.clone(), self.schema.clone());
        exec.adjacency = self.adjacency.clone();
        Ok(Arc::new(exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context.clone())?;
        let source_index = input.schema().index_of(&self.spec.source_column)?;
        let target_index = input.schema().index_of(&self.spec.target_column)?;
        let (cell, edges, spec, schema) = (
            self.adjacency.clone(),
            self.edges.clone(),
            self.spec.clone(),
            self.schema.clone(),
        );
        let out_schema = schema.clone();
        let stream = input.then(move |batch| {
            let (cell, edges, spec, schema, context) = (
                cell.clone(),
                edges.clone(),
                spec.clone(),
                schema.clone(),
                context.clone(),
            );
            async move {
                let adjacency = {
                    let mut slot = cell.lock().await;
                    if slot.is_none() {
                        *slot = Some(build_adjacency(edges, &spec, context).await?);
                    }
                    slot.clone().expect("just set")
                };
                search_batch(
                    &batch?,
                    source_index,
                    target_index,
                    &adjacency,
                    &spec,
                    &schema,
                )
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

/// The adjacency for a query without a CSR: built once from the edge scan, in the
/// direction(s) the search follows.
async fn build_adjacency(
    edges: Option<Arc<dyn ExecutionPlan>>,
    spec: &ShortestPathSpec,
    context: Arc<TaskContext>,
) -> datafusion::common::Result<Neighbors> {
    let edges = edges.expect("an edge scan when there is no CSR");
    let read = |from: String, to: String| {
        let (edges, context) = (edges.clone(), context.clone());
        async move {
            let stream = edges.execute(0, context)?;
            Adjacency::build(stream, &from, &to)
                .await
                .map(Arc::new)
                .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))
        }
    };
    let (src, dst) = (spec.edge_source.clone(), spec.edge_target.clone());
    Ok(match spec.direction {
        SearchDirection::Outgoing => Neighbors::directed(read(src, dst).await?),
        SearchDirection::Incoming => Neighbors::directed(read(dst, src).await?),
        SearchDirection::Undirected => {
            Neighbors::undirected(read(src.clone(), dst.clone()).await?, read(dst, src).await?)
        }
    })
}

/// Breadth-first search from `source` until every node in `targets` is reached (or the
/// frontier empties, or `max_hops`). Returns each reached node's depth and, for every node
/// at depth d > 0, its predecessors at depth d - 1.
fn search(
    adjacency: &Neighbors,
    source: i64,
    targets: &HashSet<i64>,
    max_hops: u32,
) -> (FastMap<i64, u32>, FastMap<i64, Vec<i64>>) {
    let mut depth: FastMap<i64, u32> = FastMap::default();
    let mut parents: FastMap<i64, Vec<i64>> = FastMap::default();
    depth.insert(source, 0);
    let mut remaining: HashSet<i64> = targets.iter().copied().filter(|t| *t != source).collect();
    let mut frontier = vec![source];
    let mut level = 0;
    while !frontier.is_empty() && !remaining.is_empty() && level < max_hops {
        level += 1;
        let mut next = Vec::new();
        for &node in &frontier {
            adjacency.for_each_neighbor(node, |n| {
                let n = n as i64;
                match depth.get(&n) {
                    // Another shortest way into a node first reached at this level.
                    Some(&d) if d == level => parents.entry(n).or_default().push(node),
                    Some(_) => {}
                    None => {
                        depth.insert(n, level);
                        parents.insert(n, vec![node]);
                        next.push(n);
                    }
                }
            });
        }
        // Finish the level before stopping, so every shortest path is recorded.
        for n in &next {
            remaining.remove(n);
        }
        frontier = next;
    }
    (depth, parents)
}

/// Shortest paths from the search source to `target`, as node ids from source to target:
/// all of them, or the first found.
fn paths_to(target: i64, parents: &FastMap<i64, Vec<i64>>, all: bool) -> Vec<Vec<i64>> {
    let mut out = Vec::new();
    let mut stack = vec![vec![target]];
    while let Some(path) = stack.pop() {
        let head = *path.last().expect("non-empty");
        match parents.get(&head) {
            None => {
                let mut path = path;
                path.reverse();
                out.push(path);
                if !all {
                    break;
                }
            }
            Some(ps) => {
                let take = if all { ps.len() } else { 1 };
                for &p in ps.iter().take(take) {
                    let mut next = path.clone();
                    next.push(p);
                    stack.push(next);
                }
            }
        }
    }
    out
}

fn search_batch(
    batch: &RecordBatch,
    source_index: usize,
    target_index: usize,
    adjacency: &Neighbors,
    spec: &ShortestPathSpec,
    schema: &SchemaRef,
) -> datafusion::common::Result<RecordBatch> {
    let sources = cast(batch.column(source_index), &DataType::Int64)?;
    let sources = sources.as_primitive::<Int64Type>();
    let targets = cast(batch.column(target_index), &DataType::Int64)?;
    let targets = targets.as_primitive::<Int64Type>();
    // Targets per source, so each source is searched once for the whole batch.
    let mut by_source: FastMap<i64, HashSet<i64>> = FastMap::default();
    for i in 0..batch.num_rows() {
        if sources.is_valid(i) && targets.is_valid(i) {
            by_source
                .entry(sources.value(i))
                .or_default()
                .insert(targets.value(i));
        }
    }
    let searches: FastMap<i64, _> = by_source
        .iter()
        .map(|(&s, ts)| (s, search(adjacency, s, ts, spec.max_hops)))
        .collect();
    let mut rows = Vec::new();
    let mut lengths = Vec::new();
    let mut nodes = ListBuilder::new(Int64Builder::new());
    for i in 0..batch.num_rows() {
        if !(sources.is_valid(i) && targets.is_valid(i)) {
            continue;
        }
        let (s, t) = (sources.value(i), targets.value(i));
        let (depth, parents) = &searches[&s];
        let Some(&d) = depth.get(&t) else {
            continue;
        };
        if d < spec.min_hops || d > spec.max_hops {
            continue;
        }
        for path in paths_to(t, parents, spec.all) {
            rows.push(i as u32);
            lengths.push(d as i64);
            nodes.values().append_slice(&path);
            nodes.append(true);
        }
    }
    let rows = UInt32Array::from(rows);
    let mut columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &rows, None))
        .collect::<std::result::Result<_, _>>()?;
    columns.push(Arc::new(Int64Array::from(lengths)));
    columns.push(Arc::new(nodes.finish()));
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// Plan [`ShortestPathNode`]s; installed by [`crate::csr_expand::CsrQueryPlanner`].
pub(crate) fn plan_shortest_path(
    node: &ShortestPathNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Arc<dyn ExecutionPlan> {
    let edges = physical_inputs.get(1).map(|scan| {
        // One stream feeds the build: merge the scan's partitions.
        Arc::new(CoalescePartitionsExec::new(scan.clone())) as Arc<dyn ExecutionPlan>
    });
    Arc::new(ShortestPathExec::new(
        physical_inputs[0].clone(),
        edges,
        node.adjacency.clone(),
        node.spec.clone(),
        Arc::new(node.schema.as_arrow().clone()),
    ))
}

fn plan_error(message: &str) -> GraphError {
    GraphError::PlanError {
        message: message.to_string(),
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn neighbors(edges: &[(i64, i64)]) -> Neighbors {
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int64, false),
            Field::new("dst", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    edges.iter().map(|e| e.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    edges.iter().map(|e| e.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let stream = || {
            let batch = batch.clone();
            Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::once(async move { Ok(batch) }),
            )) as SendableRecordBatchStream
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let out = rt
            .block_on(Adjacency::build(stream(), "src", "dst"))
            .unwrap();
        let inc = rt
            .block_on(Adjacency::build(stream(), "dst", "src"))
            .unwrap();
        Neighbors::undirected(Arc::new(out), Arc::new(inc))
    }

    #[test]
    fn finds_every_shortest_path_and_only_shortest_ones() {
        // Two 2-hop routes 1-2-4 and 1-3-4, and a longer 1-5-6-4.
        let adj = neighbors(&[(1, 2), (2, 4), (1, 3), (3, 4), (1, 5), (5, 6), (6, 4)]);
        let (depth, parents) = search(&adj, 1, &HashSet::from([4]), 10);
        assert_eq!(depth[&4], 2);
        let mut all = paths_to(4, &parents, true);
        all.sort();
        assert_eq!(all, vec![vec![1, 2, 4], vec![1, 3, 4]]);
        assert_eq!(paths_to(4, &parents, false).len(), 1);
        // Beyond the hop limit, nothing is reached.
        let (depth, _) = search(&adj, 1, &HashSet::from([4]), 1);
        assert!(!depth.contains_key(&4));
    }
}
