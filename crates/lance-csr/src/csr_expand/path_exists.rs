// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! A pattern predicate answered per row by walking the CSR from the row's start node.
//!
//! The relational marking of `WHERE (a)-[:R]->()-[:S]->(b)` plans everything before the
//! predicate a second time as a seed, matches the pattern from it and joins the marks
//! back, so each predicate doubles the plan above it. This node instead walks the
//! adjacencies from each input row's `start` node, stops at the first path found, and
//! appends the same mark: `true`, or null when there is no path.

use super::*;

/// One hop of the walk: its adjacency, and the node table every node it reaches must be
/// in, as the relational plan joins each node of the pattern to its table.
#[derive(Debug, Clone)]
pub struct Hop {
    pub neighbors: Neighbors,
    pub nodes: Arc<NodeRows>,
    /// The same edges from their other end, when some source has a long list: a last hop
    /// to a bound end then reads the shorter of its two ends' lists.
    back: Option<Neighbors>,
}

impl Hop {
    pub fn new(neighbors: Neighbors, nodes: Arc<NodeRows>) -> Self {
        let back = (neighbors.max_degree_bound() > SCAN_CLOSING_UP_TO)
            .then(|| neighbors.flipped())
            .flatten();
        Self {
            neighbors,
            nodes,
            back,
        }
    }
}

#[derive(Clone)]
pub struct PathExistsNode {
    input: LogicalPlan,
    start: String,
    /// The bound node the last hop must reach; any node when `None`.
    end: Option<String>,
    hops: Vec<Hop>,
    mark: String,
    schema: DFSchemaRef,
}

impl PathExistsNode {
    pub fn try_new(
        input: LogicalPlan,
        start: String,
        end: Option<String>,
        hops: Vec<Hop>,
        mark: String,
    ) -> Result<Self> {
        let added = DFSchema::try_from(Schema::new(vec![Field::new(
            &mark,
            DataType::Boolean,
            true,
        )]))
        .map_err(|e| plan_error(format!("PathExists schema: {}", e)))?;
        let schema = input
            .schema()
            .join(&added)
            .map_err(|e| plan_error(format!("PathExists schema: {}", e)))?;
        Ok(Self {
            input,
            start,
            end,
            hops,
            mark,
            schema: Arc::new(schema),
        })
    }

    /// The start and end columns, which the node reads without an expression.
    pub(super) fn endpoints(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.start.as_str()).chain(self.end.as_deref())
    }

    fn describe(&self) -> String {
        let end = self.end.as_ref().map(|e| format!(", match={e}"));
        format!(
            "start={}, hops={}{}, mark={}",
            self.start,
            self.hops.len(),
            end.unwrap_or_default(),
            self.mark
        )
    }
}

/// Whether a path over `hops` leads from `node` to `end` (or anywhere). `explored` holds
/// the (hops left, node) pairs this row has already walked: any path from one ends the
/// search, so walking it again can only fail again.
fn reaches(
    hops: &[Hop],
    node: i64,
    end: Option<i64>,
    explored: &mut HashSet<(usize, i64), ahash::RandomState>,
) -> bool {
    let Some((hop, rest)) = hops.split_first() else {
        return end.is_none_or(|end| end == node);
    };
    if let (true, Some(end)) = (rest.is_empty(), end) {
        // The last hop to a bound end closes the path: read the shorter of its two ends'
        // lists, as a closing edge does.
        if hop.nodes.position(end).is_none() {
            return false;
        }
        let (list, from, to) = match &hop.back {
            Some(back) if back.degree_bound(end) < hop.neighbors.degree_bound(node) => {
                (back, end, node)
            }
            _ => (&hop.neighbors, node, end),
        };
        let mut position = NeighborPosition::default();
        return list
            .neighbors_from(from, &mut position)
            .any(|next| next as i64 == to);
    }
    // A one-hop predicate never revisits a node, so it keeps no memo.
    if !rest.is_empty() && !explored.insert((rest.len(), node)) {
        return false;
    }
    let mut position = NeighborPosition::default();
    let found = hop
        .neighbors
        .neighbors_from(node, &mut position)
        .any(|next| {
            hop.nodes.position(next as i64).is_some() && reaches(rest, next as i64, end, explored)
        });
    found
}

// Logical nodes must be comparable; adjacencies compare by identity.
impl PartialEq for PathExistsNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.start == other.start
            && self.end == other.end
            && self.mark == other.mark
            && self.hops.len() == other.hops.len()
            && self
                .hops
                .iter()
                .zip(&other.hops)
                .all(|(a, b)| a.neighbors.ptr_eq(&b.neighbors) && Arc::ptr_eq(&a.nodes, &b.nodes))
    }
}
impl Eq for PathExistsNode {}
impl Hash for PathExistsNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.start.hash(state);
        self.end.hash(state);
        self.mark.hash(state);
    }
}
impl PartialOrd for PathExistsNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (&self.start, &self.mark).partial_cmp(&(&other.start, &other.mark))
    }
}

// DataFusion formats every extension node while planning it; printing the input plan
// here would make that quadratic in the plan's depth.
impl std::fmt::Debug for PathExistsNode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for PathExistsNode {
    fn name(&self) -> &str {
        "PathExists"
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

    /// Filters on the mark must stay above the node.
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        HashSet::from([self.mark.clone()])
    }

    /// The input only needs the requested input columns plus the endpoints.
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        let schema = self.input.schema();
        let input_width = schema.fields().len();
        let mut needed: Vec<usize> = output_columns
            .iter()
            .copied()
            .filter(|&i| i < input_width)
            .collect();
        for column in std::iter::once(&self.start).chain(&self.end) {
            needed.push(schema.index_of_column_by_name(None, column)?);
        }
        needed.sort_unstable();
        needed.dedup();
        Some(vec![needed])
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PathExists: {}", self.describe())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        let input = inputs.into_iter().next().ok_or_else(|| {
            datafusion::common::internal_datafusion_err!("PathExists has one input")
        })?;
        Self::try_new(
            input,
            self.start.clone(),
            self.end.clone(),
            self.hops.clone(),
            self.mark.clone(),
        )
        .map_err(|e| datafusion::common::DataFusionError::Plan(e.to_string()))
    }
}

/// Executes [`PathExistsNode`] one input batch at a time.
#[derive(Debug)]
pub struct PathExistsExec {
    /// Per-partition metrics for `EXPLAIN ANALYZE`.
    metrics: datafusion::physical_plan::metrics::ExecutionPlanMetricsSet,
    input: Arc<dyn ExecutionPlan>,
    start: String,
    end: Option<String>,
    hops: Arc<Vec<Hop>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl PathExistsExec {
    fn new(node: &PathExistsNode, input: Arc<dyn ExecutionPlan>) -> Self {
        let schema: SchemaRef = Arc::new(node.schema.as_arrow().clone());
        let properties = Self::properties_for(&schema, &input);
        Self {
            metrics: Default::default(),
            input,
            start: node.start.clone(),
            end: node.end.clone(),
            hops: Arc::new(node.hops.clone()),
            schema,
            properties,
        }
    }

    fn properties_for(schema: &SchemaRef, input: &Arc<dyn ExecutionPlan>) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }

    /// The batch with each row's mark appended. A null endpoint matches no path.
    fn mark(
        batch: RecordBatch,
        start: &str,
        end: Option<&str>,
        hops: &[Hop],
        schema: &SchemaRef,
    ) -> datafusion::common::Result<RecordBatch> {
        let external = |e: GraphError| datafusion::common::DataFusionError::External(Box::new(e));
        let starts = int64_column(&batch, start).map_err(external)?;
        let ends = end
            .map(|end| int64_column(&batch, end))
            .transpose()
            .map_err(external)?;
        let mut explored = HashSet::default();
        let marks: arrow::array::BooleanArray = (0..batch.num_rows())
            .map(|i| {
                let end = match &ends {
                    Some(ends) if ends.is_null(i) => return None,
                    Some(ends) => Some(ends.value(i)),
                    None => None,
                };
                explored.clear();
                (starts.is_valid(i) && reaches(hops, starts.value(i), end, &mut explored))
                    .then_some(true)
            })
            .collect();
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(marks));
        Ok(RecordBatch::try_new(schema.clone(), columns)?)
    }
}

impl DisplayAs for PathExistsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let end = self.end.as_ref().map(|e| format!(", match={e}"));
        write!(
            f,
            "PathExistsExec: start={}, hops={}{}",
            self.start,
            self.hops.len(),
            end.unwrap_or_default()
        )
    }
}

impl ExecutionPlan for PathExistsExec {
    fn name(&self) -> &str {
        "PathExistsExec"
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
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
        let input = children.into_iter().next().ok_or_else(|| {
            datafusion::common::internal_datafusion_err!("PathExistsExec has one child")
        })?;
        Ok(Arc::new(Self {
            metrics: Default::default(),
            properties: Self::properties_for(&self.schema, &input),
            input,
            start: self.start.clone(),
            end: self.end.clone(),
            hops: self.hops.clone(),
            schema: self.schema.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let (input, input_time) =
            crate::exec_metrics::time_input(self.input.execute(partition, context)?);
        let (start, end, hops, schema) = (
            self.start.clone(),
            self.end.clone(),
            self.hops.clone(),
            self.schema.clone(),
        );
        // shortcut: one batch is marked without yielding; bound the work per poll if
        // predicates over high-degree hops show up in cancellation latency.
        let stream =
            input.map(move |batch| Self::mark(batch?, &start, end.as_deref(), &hops, &schema));
        let output: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream));
        Ok(crate::exec_metrics::measure(
            output,
            &self.metrics,
            partition,
            input_time,
        ))
    }
}

pub(super) fn plan_path_exists(
    node: &PathExistsNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Arc<dyn ExecutionPlan> {
    Arc::new(PathExistsExec::new(node, physical_inputs[0].clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csr_expand::tests::edge_stream;

    async fn adjacency(edges: &[(i64, i64)], from: &str, to: &str) -> Arc<Adjacency> {
        let (src, dst) = edges.iter().map(|&(a, b)| (Some(a), Some(b))).unzip();
        Arc::new(
            Adjacency::build(edge_stream(src, dst), from, to)
                .await
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn walks_agree_with_or_without_the_shorter_end_and_skip_explored_nodes() {
        // Two R paths reach 4 (via 2 and 3), then 4 -R-> 5, whose P list holds 10..=300.
        let r = [(1, 2), (1, 3), (2, 4), (3, 4), (4, 5)];
        let p: Vec<_> = (10..=300).map(|x| (5, x)).collect();
        let ids = Arc::new(Int64Array::from_iter_values(1..=400)) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("id", ids)]).unwrap();
        let nodes = Arc::new(NodeRows::from_batch(batch, "id").unwrap().unwrap());
        let hop = |edges: Arc<Adjacency>, back: Arc<Adjacency>| {
            Hop::new(
                Neighbors::directed(edges).with_back(Some(back)),
                nodes.clone(),
            )
        };
        let (r_out, r_in) = (
            adjacency(&r, "src", "dst").await,
            adjacency(&r, "dst", "src").await,
        );
        let (p_out, p_in) = (
            adjacency(&p, "src", "dst").await,
            adjacency(&p, "dst", "src").await,
        );
        let hops = vec![
            hop(r_out.clone(), r_in.clone()),
            hop(r_out.clone(), r_in.clone()),
            hop(r_out, r_in),
            hop(p_out, p_in),
        ];
        // Only P has a list long enough to be worth reading from the other end.
        assert!(hops[3].back.is_some() && hops[0].back.is_none());
        let scanned: Vec<Hop> = hops
            .iter()
            .map(|h| Hop {
                back: None,
                ..h.clone()
            })
            .collect();

        let mut explored = HashSet::default();
        for (end, expected) in [
            (Some(150), true),
            (Some(5), false),
            (Some(999), false),
            (None, true),
        ] {
            for walk in [&hops, &scanned] {
                explored.clear();
                assert_eq!(reaches(walk, 1, end, &mut explored), expected, "{end:?}");
            }
        }
        // 4 is reached twice, both times with one hop after its own, and explored once.
        explored.clear();
        assert!(!reaches(&hops, 1, Some(5), &mut explored));
        assert!(explored.contains(&(1, 4)));
        assert_eq!(explored.len(), 4); // 1, 2, 3 and 4: the second visit to 4 adds nothing
    }
}
