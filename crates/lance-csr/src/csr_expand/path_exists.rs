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

/// Whether a path over `hops` leads from `node` to `end` (or anywhere).
// shortcut: no memo of visited nodes, so a long chain of high-degree hops with a bound end
// costs the product of their degrees per row; add one if longer predicates appear.
fn reaches(hops: &[Hop], node: i64, end: Option<i64>) -> bool {
    let Some((hop, rest)) = hops.split_first() else {
        return end.is_none_or(|end| end == node);
    };
    let mut position = NeighborPosition::default();
    let found = hop
        .neighbors
        .neighbors_from(node, &mut position)
        .any(|next| hop.nodes.position(next as i64).is_some() && reaches(rest, next as i64, end));
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
        let input = inputs.into_iter().next().expect("PathExists has one input");
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
        let marks: arrow::array::BooleanArray = (0..batch.num_rows())
            .map(|i| {
                let end = match &ends {
                    Some(ends) if ends.is_null(i) => return None,
                    Some(ends) => Some(ends.value(i)),
                    None => None,
                };
                (starts.is_valid(i) && reaches(hops, starts.value(i), end)).then_some(true)
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
            .expect("PathExistsExec has one child");
        Ok(Arc::new(Self {
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
        let input = self.input.execute(partition, context)?;
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
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

pub(super) fn plan_path_exists(
    node: &PathExistsNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Arc<dyn ExecutionPlan> {
    Arc::new(PathExistsExec::new(node, physical_inputs[0].clone()))
}
