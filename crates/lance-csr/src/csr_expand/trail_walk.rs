// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! A pattern of several relationship segments, walked on the CSR from each input row.
//!
//! Cypher never walks a relationship twice in one match. Separate expand operators share
//! only node ids, so in `(a)-[:R*1..2]-(f)-[:R]-(g)` the hop after `f` cannot tell which
//! edges the variable-length segment walked. This node walks every segment of the pattern
//! in one depth-first search seeded at the row's start node. The stack holds the edges
//! walked so far, each identified by its relationship type and stored edge row, so an
//! edge is used once across all segments, parallel edges stay distinct, and the work
//! follows the paths from that start node: no edge inventory is built. It appends the id
//! of each segment's end node that is not already known.

use super::*;
use crate::ast::RelationshipDirection;

/// A segment end that must equal a value known before the walk reaches it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Tie {
    /// The bound input column with this index in [`TrailWalkNode`]'s `bound` list.
    Input(usize),
    /// An earlier boundary of the pattern: 0 is the start node, `k` the end of segment
    /// `k - 1`.
    Boundary(usize),
}

/// One segment of the pattern, as the planner describes it.
pub(crate) struct WalkSegmentSpec<'a> {
    pub types: &'a [String],
    pub direction: &'a RelationshipDirection,
    pub end_label: &'a str,
    pub min: u32,
    pub max: u32,
    pub tie: Option<Tie>,
}

impl WalkSegmentSpec<'_> {
    /// Which alternatives occur on a schema path from start to end within this range.
    /// Track the set of types reaching each label at each depth, without enumerating paths.
    fn participating_types(
        &self,
        config: &crate::config::GraphConfig,
        start: &str,
    ) -> Option<HashSet<usize>> {
        let mut steps = Vec::new();
        for (i, name) in self.types.iter().enumerate() {
            let mapping = config.get_relationship_mapping(name)?;
            let src = mapping.source_label.as_deref()?.to_ascii_lowercase();
            let dst = mapping.target_label.as_deref()?.to_ascii_lowercase();
            if *self.direction != RelationshipDirection::Incoming {
                steps.push((src.clone(), dst.clone(), i));
            }
            if *self.direction == RelationshipDirection::Incoming
                || (*self.direction == RelationshipDirection::Undirected && src != dst)
            {
                steps.push((dst, src, i));
            }
        }
        let start = start.to_ascii_lowercase();
        let end = self.end_label.to_ascii_lowercase();
        let mut frontier = HashMap::from([(start.as_str(), HashSet::new())]);
        let mut used = HashSet::new();
        for depth in 1..=self.max {
            let mut next = HashMap::<&str, HashSet<usize>>::new();
            for (from, to, i) in &steps {
                if let Some(history) = frontier.get(from.as_str()) {
                    let reached = next.entry(to).or_default();
                    reached.extend(history.iter().copied());
                    reached.insert(*i);
                }
            }
            if depth >= self.min {
                if let Some(types) = next.get(end.as_str()) {
                    used.extend(types);
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        Some(used)
    }
}

#[derive(Debug)]
struct WalkStep {
    from: usize,
    to: usize,
    /// Index of the relationship type across the whole pattern: with the edge row, it
    /// identifies the edge, whichever segment walks it.
    rel: usize,
    neighbors: Neighbors,
}

#[derive(Debug)]
struct WalkSegment {
    steps: Vec<WalkStep>,
    end: usize,
    min: u32,
    max: u32,
    tie: Option<Tie>,
}

#[derive(Debug)]
pub(crate) struct TrailWalk {
    segments: Vec<WalkSegment>,
    /// Node index per label: every node a hop reaches must have a row, as the joins
    /// drop dangling edges at every hop.
    nodes: Vec<Arc<NodeRows>>,
    labels: Vec<String>,
    start: usize,
}

impl TrailWalk {
    /// `None` when a type has no CSR for its direction or a label has no node index.
    pub(crate) fn new(
        config: &crate::config::GraphConfig,
        catalog: &AdjacencyCatalog,
        start_label: &str,
        specs: &[WalkSegmentSpec],
    ) -> Option<Self> {
        let mut labels: Vec<String> = Vec::new();
        let mut nodes = Vec::new();
        let mut label = |name: &str| -> Option<usize> {
            if let Some(index) = labels.iter().position(|l| l.eq_ignore_ascii_case(name)) {
                return Some(index);
            }
            nodes.push(catalog.nodes(name)?.clone());
            labels.push(name.to_owned());
            Some(labels.len() - 1)
        };
        let mut types: Vec<String> = Vec::new();
        let start = label(start_label)?;
        let mut segments = Vec::with_capacity(specs.len());
        let mut segment_start = start_label;
        for spec in specs {
            let end = label(spec.end_label)?;
            let participating = if spec.types.len() > 1 {
                Some(spec.participating_types(config, segment_start)?)
            } else {
                None
            };
            segment_start = spec.end_label;
            let mut steps = Vec::new();
            for (i, name) in spec.types.iter().enumerate() {
                if participating
                    .as_ref()
                    .is_some_and(|types| !types.contains(&i))
                {
                    continue;
                }
                let mapping = config.get_relationship_mapping(name)?;
                let kind = &mapping.relationship_type;
                let rel = match types.iter().position(|t| t.eq_ignore_ascii_case(kind)) {
                    Some(rel) => rel,
                    None => {
                        types.push(kind.clone());
                        types.len() - 1
                    }
                };
                let (from, to) = (
                    label(mapping.source_label.as_deref()?)?,
                    label(mapping.target_label.as_deref()?)?,
                );
                let get = |outgoing| catalog.get(name, outgoing).cloned();
                let step = |from, to, neighbors| WalkStep {
                    from,
                    to,
                    rel,
                    neighbors,
                };
                match spec.direction {
                    RelationshipDirection::Outgoing => {
                        steps.push(step(from, to, Neighbors::directed(get(true)?)))
                    }
                    RelationshipDirection::Incoming => {
                        steps.push(step(to, from, Neighbors::directed(get(false)?)))
                    }
                    RelationshipDirection::Undirected if from == to => steps.push(step(
                        from,
                        to,
                        Neighbors::undirected(get(true)?, get(false)?),
                    )),
                    RelationshipDirection::Undirected => {
                        steps.push(step(from, to, Neighbors::directed(get(true)?)));
                        steps.push(step(to, from, Neighbors::directed(get(false)?)));
                    }
                }
            }
            segments.push(WalkSegment {
                steps,
                end,
                min: spec.min,
                max: spec.max,
                tie: spec.tie,
            });
        }
        Some(Self {
            segments,
            nodes,
            labels,
            start,
        })
    }

    /// One participating adjacency for the single-segment expansion operator.
    pub(crate) fn first_neighbors(&self) -> Option<&Neighbors> {
        Some(&self.segments.first()?.steps.first()?.neighbors)
    }

    /// Node index of the label at the end of `segment`.
    pub(crate) fn end_rows(&self, segment: usize) -> &Arc<NodeRows> {
        &self.nodes[self.segments[segment].end]
    }

    /// The segments whose end id the node outputs: those not tied to a known value.
    fn outputs(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.segments.len()).filter(|&s| self.segments[s].tie.is_none())
    }

    fn describe(&self) -> String {
        let segments: Vec<String> = self
            .segments
            .iter()
            .map(|s| format!("{}..{}->{}", s.min, s.max, self.labels[s.end]))
            .collect();
        format!(
            "from {}, segments=[{}]",
            self.labels[self.start],
            segments.join(", ")
        )
    }
}

struct Frame {
    segment: usize,
    /// Hops walked in `segment` so far.
    depth: u32,
    label: usize,
    key: i64,
    step: usize,
    position: NeighborPosition,
    emit: bool,
    /// The edge this frame was reached by, as (type index, edge row).
    edge: Option<(usize, u64)>,
}

impl Frame {
    fn new(segment: usize, depth: u32, label: usize, key: i64, edge: Option<(usize, u64)>) -> Self {
        Self {
            segment,
            depth,
            label,
            key,
            step: 0,
            position: NeighborPosition::default(),
            emit: true,
            edge,
        }
    }
}

/// A resumable walk from one start node. One frame per hop: a segment's end starts the
/// next segment with a frame of depth 0 at the same node, so the stack also holds every
/// boundary reached so far. A one-segment walk is the variable-length expand's walker.
pub(crate) struct TrailCursor {
    stack: Vec<Frame>,
    /// Start id and completed segment ends on the current branch.
    boundaries: Vec<i64>,
}

impl TrailCursor {
    pub(crate) fn new(walk: &TrailWalk, key: i64) -> Self {
        Self {
            stack: vec![Frame::new(0, 0, walk.start, key, None)],
            boundaries: vec![key],
        }
    }

    /// Frames on the stack: one per hop of the current branch, plus one per boundary.
    #[cfg(test)]
    pub(crate) fn depth(&self) -> usize {
        self.stack.len()
    }

    fn bound_end(&self, tie: Option<Tie>, bound: &[i64]) -> Option<i64> {
        match tie? {
            Tie::Input(i) => Some(bound[i]),
            Tie::Boundary(k) => Some(self.boundaries[k]),
        }
    }

    /// Emit final endpoints with the same preceding boundaries as a group. Read a
    /// contiguous run of final-hop candidates without rebuilding its CSR iterator or
    /// pushing a stack frame for every row. At most `limit` ends per group.
    pub(crate) async fn next_group(
        &mut self,
        walk: &TrailWalk,
        bound: &[i64],
        limit: usize,
        ends: &mut Vec<i64>,
    ) -> bool {
        ends.clear();
        let mut work = 0_usize;
        loop {
            if work >= 1024 {
                cooperative_yield().await;
                work = 0;
            }
            work += 1;
            let Some(top) = self.stack.len().checked_sub(1) else {
                return false;
            };
            let (s, depth, label, key, emit) = {
                let frame = &mut self.stack[top];
                let emit = std::mem::replace(&mut frame.emit, false);
                (frame.segment, frame.depth, frame.label, frame.key, emit)
            };
            let segment = &walk.segments[s];
            if emit
                && depth >= segment.min
                && label == segment.end
                && self.bound_end(segment.tie, bound).is_none_or(|v| v == key)
            {
                if s + 1 == walk.segments.len() {
                    ends.push(key);
                    return true;
                }
                self.boundaries.push(key);
                self.stack.push(Frame::new(s + 1, 0, label, key, None));
                continue;
            }
            if depth == segment.max || self.stack[top].step == segment.steps.len() {
                if depth == 0 {
                    self.boundaries.pop();
                }
                self.stack.pop();
                continue;
            }
            let final_hop = s + 1 == walk.segments.len() && depth + 1 == segment.max;
            let tied = self.bound_end(segment.tie, bound);
            let (frame, ancestors) = self.stack.split_last_mut().expect("nonempty stack");
            let step = &segment.steps[frame.step];
            if final_hop {
                let chunk = limit.min(256);
                let mut visited = 0;
                if step.from == label && step.to == segment.end {
                    for candidate in step
                        .neighbors
                        .candidates_from::<true>(key, &mut frame.position)
                        .take(chunk)
                    {
                        visited += 1;
                        let Some((next, row)) = candidate else {
                            continue;
                        };
                        let next = next as i64;
                        let edge = Some((step.rel, row));
                        if frame.edge != edge
                            && !ancestors.iter().any(|f| f.edge == edge)
                            && tied.is_none_or(|v| v == next)
                            && walk.nodes[step.to].position(next).is_some()
                        {
                            ends.push(next);
                        }
                    }
                }
                if visited < chunk {
                    frame.step += 1;
                    frame.position = NeighborPosition::default();
                }
                work += visited;
                if !ends.is_empty() {
                    return true;
                }
                continue;
            }
            let next = if step.from == label {
                step.neighbors
                    .candidates_from::<true>(key, &mut frame.position)
                    .next()
            } else {
                None
            };
            let (neighbor, edge) = match next {
                Some(Some((neighbor, row))) => (neighbor as i64, (step.rel, row)),
                // A tombstone or a suppressed self-loop: counted as work, nothing to visit.
                Some(None) => continue,
                None => {
                    frame.step += 1;
                    frame.position = NeighborPosition::default();
                    continue;
                }
            };
            if walk.nodes[step.to].position(neighbor).is_none()
                || self.stack.iter().any(|f| f.edge == Some(edge))
            {
                continue;
            }
            self.stack
                .push(Frame::new(s, depth + 1, step.to, neighbor, Some(edge)));
        }
    }
}

/// `input` plus the end node id of each segment of a pattern whose end is not tied to a
/// known value, one row per match.
#[derive(Clone)]
pub(crate) struct TrailWalkNode {
    input: LogicalPlan,
    /// Input column holding the start node id.
    start: String,
    /// Input columns that segment ends tied by [`Tie::Input`] must equal.
    bound: Vec<String>,
    walk: Arc<TrailWalk>,
    schema: DFSchemaRef,
}

impl TrailWalkNode {
    /// `outputs` names one column per untied segment end, in segment order.
    pub(crate) fn try_new(
        input: LogicalPlan,
        start: String,
        bound: Vec<String>,
        walk: Arc<TrailWalk>,
        outputs: Vec<Field>,
    ) -> Result<Self> {
        if outputs.len() != walk.outputs().count() {
            return Err(plan_error(
                "TrailWalk needs one output column per untied segment end".to_string(),
            ));
        }
        let added = DFSchema::try_from(Schema::new(outputs))
            .map_err(|e| plan_error(format!("TrailWalk schema: {}", e)))?;
        let schema = input
            .schema()
            .join(&added)
            .map_err(|e| plan_error(format!("TrailWalk schema: {}", e)))?;
        Ok(Self {
            input,
            start,
            bound,
            walk,
            schema: Arc::new(schema),
        })
    }

    fn added_columns(&self) -> impl Iterator<Item = &String> {
        let width = self.input.schema().fields().len();
        self.schema.fields()[width..].iter().map(|f| f.name())
    }

    fn describe(&self) -> String {
        let ends: Vec<&String> = self.added_columns().collect();
        let mut text = format!("start={}, {}", self.start, self.walk.describe());
        if !ends.is_empty() {
            text.push_str(&format!(
                ", ends=[{}]",
                ends.iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !self.bound.is_empty() {
            text.push_str(&format!(", match=[{}]", self.bound.join(", ")));
        }
        text
    }
}

// Logical nodes must be comparable; the walk compares by identity.
impl PartialEq for TrailWalkNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.start == other.start
            && self.bound == other.bound
            && Arc::ptr_eq(&self.walk, &other.walk)
            && self.schema == other.schema
    }
}
impl Eq for TrailWalkNode {}
impl Hash for TrailWalkNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.start.hash(state);
        self.bound.hash(state);
        Arc::as_ptr(&self.walk).hash(state);
    }
}
impl PartialOrd for TrailWalkNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (&self.start, &self.bound)
            .partial_cmp(&(&other.start, &other.bound))
            .filter(|order| *order != Ordering::Equal || self == other)
    }
}

// DataFusion formats every extension node while planning it; printing the input plan
// here would make that quadratic in the plan's depth.
impl std::fmt::Debug for TrailWalkNode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for TrailWalkNode {
    fn name(&self) -> &str {
        "TrailWalk"
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

    /// Filters on the end ids this node creates must stay above it.
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        self.added_columns().cloned().collect()
    }

    /// The input only needs the requested input columns, the start and the bound ends.
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        let schema = self.input.schema();
        let width = schema.fields().len();
        let mut needed: Vec<usize> = output_columns
            .iter()
            .copied()
            .filter(|&i| i < width)
            .collect();
        for column in std::iter::once(&self.start).chain(&self.bound) {
            needed.push(schema.index_of_column_by_name(None, column)?);
        }
        needed.sort_unstable();
        needed.dedup();
        Some(vec![needed])
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TrailWalk: {}", self.describe())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        let input = inputs.into_iter().next().ok_or_else(|| {
            datafusion::common::internal_datafusion_err!("TrailWalk has one input")
        })?;
        let width = self.input.schema().fields().len();
        let outputs = self.schema.fields()[width..]
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        Self::try_new(
            input,
            self.start.clone(),
            self.bound.clone(),
            self.walk.clone(),
            outputs,
        )
        .map_err(|e| datafusion::common::DataFusionError::Plan(e.to_string()))
    }
}

/// One input batch being walked, and where its walk paused.
struct WalkBatch {
    batch: RecordBatch,
    starts: Int64Array,
    bound: Vec<Int64Array>,
    row: usize,
    cursor: Option<TrailCursor>,
}

impl WalkBatch {
    fn try_new(
        batch: RecordBatch,
        start: usize,
        bound: &[usize],
    ) -> datafusion::common::Result<Self> {
        let ints = |i: usize| {
            cast(batch.column(i), &DataType::Int64).map(|a| a.as_primitive::<Int64Type>().clone())
        };
        let starts = ints(start)?;
        let bound = bound
            .iter()
            .map(|&i| ints(i))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Self {
            batch,
            starts,
            bound,
            row: 0,
            cursor: None,
        })
    }

    /// Up to `batch_size` matches: their input rows, and per output column the end ids.
    async fn next_chunk(
        &mut self,
        walk: &TrailWalk,
        batch_size: usize,
    ) -> (Vec<u32>, Vec<Vec<i64>>) {
        let outputs: Vec<usize> = walk.outputs().collect();
        let mut rows = Vec::new();
        let mut columns = vec![Vec::new(); outputs.len()];
        let mut ends = Vec::new();
        let mut bound = vec![0_i64; self.bound.len()];
        let mut visited = 0_usize;
        while self.row < self.starts.len() {
            let i = self.row;
            // A null start or bound end matches nothing.
            if self.starts.is_valid(i) && self.bound.iter().all(|b| b.is_valid(i)) {
                for (slot, values) in bound.iter_mut().zip(&self.bound) {
                    *slot = values.value(i);
                }
                let key = self.starts.value(i);
                let cursor = self
                    .cursor
                    .get_or_insert_with(|| TrailCursor::new(walk, key));
                while cursor
                    .next_group(walk, &bound, batch_size.max(1) - rows.len(), &mut ends)
                    .await
                {
                    rows.resize(rows.len() + ends.len(), i as u32);
                    for (column, &s) in columns.iter_mut().zip(&outputs) {
                        if s + 1 == walk.segments.len() {
                            column.extend_from_slice(&ends);
                        } else {
                            column.resize(rows.len(), cursor.boundaries[s + 1]);
                        }
                    }
                    if rows.len() == batch_size.max(1) {
                        return (rows, columns);
                    }
                }
            }
            self.row += 1;
            self.cursor = None;
            visited += 1;
            if visited.is_multiple_of(1024) {
                cooperative_yield().await;
            }
        }
        (rows, columns)
    }

    fn materialize(
        &self,
        rows: Vec<u32>,
        ends: Vec<Vec<i64>>,
        schema: &SchemaRef,
    ) -> datafusion::common::Result<RecordBatch> {
        let indices = UInt32Array::from(rows);
        let mut columns: Vec<ArrayRef> = self
            .batch
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), &indices, None))
            .collect::<std::result::Result<_, _>>()?;
        let width = columns.len();
        for (i, values) in ends.into_iter().enumerate() {
            columns.push(cast(
                &Int64Array::from(values),
                schema.field(width + i).data_type(),
            )?);
        }
        Ok(RecordBatch::try_new(schema.clone(), columns)?)
    }
}

/// Executes [`TrailWalkNode`], one bounded chunk of matches at a time.
#[derive(Debug)]
pub struct TrailWalkExec {
    /// Per-partition metrics for `EXPLAIN ANALYZE`.
    metrics: datafusion::physical_plan::metrics::ExecutionPlanMetricsSet,
    input: Arc<dyn ExecutionPlan>,
    start: String,
    bound: Vec<String>,
    walk: Arc<TrailWalk>,
    description: String,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl TrailWalkExec {
    fn new(node: &TrailWalkNode, input: Arc<dyn ExecutionPlan>) -> Self {
        let schema: SchemaRef = Arc::new(node.schema.as_arrow().clone());
        Self {
            metrics: Default::default(),
            properties: Self::properties_for(&schema, &input),
            input,
            start: node.start.clone(),
            bound: node.bound.clone(),
            walk: node.walk.clone(),
            description: node.describe(),
            schema,
        }
    }

    fn properties_for(schema: &SchemaRef, input: &Arc<dyn ExecutionPlan>) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl DisplayAs for TrailWalkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TrailWalkExec: {}", self.description)
    }
}

impl ExecutionPlan for TrailWalkExec {
    fn name(&self) -> &str {
        "TrailWalkExec"
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
            datafusion::common::internal_datafusion_err!("TrailWalkExec has one child")
        })?;
        Ok(Arc::new(Self {
            metrics: Default::default(),
            properties: Self::properties_for(&self.schema, &input),
            input,
            start: self.start.clone(),
            bound: self.bound.clone(),
            walk: self.walk.clone(),
            description: self.description.clone(),
            schema: self.schema.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let (input, input_time) =
            crate::exec_metrics::time_input(self.input.execute(partition, context.clone())?);
        // Resolve by name at execution time, so later column pruning cannot shift them.
        let start = input.schema().index_of(&self.start)?;
        let bound = self
            .bound
            .iter()
            .map(|c| input.schema().index_of(c))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let batch_size = context.session_config().batch_size();
        let (walk, schema) = (self.walk.clone(), self.schema.clone());
        let stream = futures::stream::try_unfold(
            (input, None::<WalkBatch>),
            move |(mut input, mut pending)| {
                let (walk, schema, bound) = (walk.clone(), schema.clone(), bound.clone());
                async move {
                    loop {
                        if pending.is_none() {
                            let Some(batch) = input.next().await else {
                                return Ok(None);
                            };
                            pending = Some(WalkBatch::try_new(batch?, start, &bound)?);
                        }
                        let walking = pending.as_mut().expect("input batch just set");
                        let (rows, ends) = walking.next_chunk(&walk, batch_size).await;
                        if rows.is_empty() {
                            pending = None;
                            continue;
                        }
                        let batch = walking.materialize(rows, ends, &schema)?;
                        return Ok(Some((batch, (input, pending))));
                    }
                }
            },
        );
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

pub(super) fn plan_trail_walk(
    node: &TrailWalkNode,
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Arc<dyn ExecutionPlan> {
    Arc::new(TrailWalkExec::new(node, physical_inputs[0].clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    /// Nodes 1, 2, 3 of label N; R edges 1->2 twice (parallel) and 2->3.
    async fn graph() -> (crate::config::GraphConfig, AdjacencyCatalog) {
        let config = crate::config::GraphConfig::builder()
            .node_table("N", "id")
            .rel_table("R", "N", "N", "src", "dst")
            .build()
            .unwrap();
        let mut catalog = AdjacencyCatalog::default();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        catalog.insert_nodes(
            "N",
            Arc::new(NodeRows::from_batch(batch, "id").unwrap().unwrap()),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int64, false),
            Field::new("dst", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(Int64Array::from(vec![2, 2, 3])),
            ],
        )
        .unwrap();
        for (outgoing, from, to) in [(true, "src", "dst"), (false, "dst", "src")] {
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::iter([Ok(batch.clone())]),
            ));
            catalog.insert(
                "R",
                outgoing,
                Arc::new(Adjacency::build(stream, from, to).await.unwrap()),
            );
        }
        (config, catalog)
    }

    async fn matches(walk: &TrailWalk, start: i64, bound: &[i64]) -> Vec<Vec<i64>> {
        let mut cursor = TrailCursor::new(walk, start);
        let mut ends = Vec::new();
        let mut out = Vec::new();
        while cursor.next_group(walk, bound, 2, &mut ends).await {
            for end in &ends {
                let mut row = cursor.boundaries[1..].to_vec();
                row.push(*end);
                out.push(row);
            }
        }
        out.sort();
        out
    }

    fn segment<'a>(
        types: &'a [String],
        direction: &'a RelationshipDirection,
        [min, max]: [u32; 2],
        tie: Option<Tie>,
    ) -> WalkSegmentSpec<'a> {
        WalkSegmentSpec {
            types,
            direction,
            end_label: "N",
            min,
            max,
            tie,
        }
    }

    #[tokio::test]
    async fn segments_share_one_set_of_walked_edges() {
        let (config, catalog) = graph().await;
        let r = ["R".to_string()];
        let undirected = RelationshipDirection::Undirected;
        // (1)-[:R*1..2]-(f)-[:R]-(g): the hop after f never reuses an edge of the trail.
        let walk = TrailWalk::new(
            &config,
            &catalog,
            "N",
            &[
                segment(&r, &undirected, [1, 2], None),
                segment(&r, &undirected, [1, 1], None),
            ],
        )
        .unwrap();
        assert_eq!(
            matches(&walk, 1, &[]).await,
            vec![vec![2, 1], vec![2, 1], vec![2, 3], vec![2, 3]]
        );
    }

    #[tokio::test]
    async fn mixed_directions_and_zero_length_segments() {
        let (config, catalog) = graph().await;
        let r = ["R".to_string()];
        let (out, inc) = (
            RelationshipDirection::Outgoing,
            RelationshipDirection::Incoming,
        );
        // (1)-[:R*0..1]->(f)<-[:R]-(g): from 2 the incoming hop takes the other parallel edge.
        let walk = TrailWalk::new(
            &config,
            &catalog,
            "N",
            &[
                segment(&r, &out, [0, 1], None),
                segment(&r, &inc, [1, 1], None),
            ],
        )
        .unwrap();
        assert_eq!(matches(&walk, 1, &[]).await, vec![vec![2, 1], vec![2, 1]]);
    }

    #[tokio::test]
    async fn ties_to_bound_inputs_and_earlier_boundaries() {
        let (config, catalog) = graph().await;
        let r = ["R".to_string()];
        let undirected = RelationshipDirection::Undirected;
        // (a)-[:R*1..2]-(f)-[:R]-(a): back to the start over the other parallel edge.
        let walk = TrailWalk::new(
            &config,
            &catalog,
            "N",
            &[
                segment(&r, &undirected, [1, 2], None),
                segment(&r, &undirected, [1, 1], Some(Tie::Boundary(0))),
            ],
        )
        .unwrap();
        assert_eq!(matches(&walk, 1, &[]).await, vec![vec![2, 1], vec![2, 1]]);
        // The same pattern with the end bound by an input column.
        let walk = TrailWalk::new(
            &config,
            &catalog,
            "N",
            &[
                segment(&r, &undirected, [1, 2], None),
                segment(&r, &undirected, [1, 1], Some(Tie::Input(0))),
            ],
        )
        .unwrap();
        assert_eq!(matches(&walk, 1, &[3]).await, vec![vec![2, 3], vec![2, 3]]);
        assert!(matches(&walk, 1, &[2]).await.is_empty());
    }

    #[tokio::test]
    async fn missing_index_declines_the_walk() {
        let (config, catalog) = graph().await;
        let r = ["R".to_string()];
        let out = RelationshipDirection::Outgoing;
        let specs = [WalkSegmentSpec {
            types: &r,
            direction: &out,
            end_label: "Missing",
            min: 1,
            max: 1,
            tie: None,
        }];
        assert!(TrailWalk::new(&config, &catalog, "N", &specs).is_none());
    }
}
