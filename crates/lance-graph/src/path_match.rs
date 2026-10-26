// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! Rich paths use an immutable edge inventory, resumable trail enumeration, and the
//! existing bidirectional BFS on a bipartite node/edge graph for shortest paths.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef, Result};
use datafusion::execution::session_state::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{col, Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::{lock::Mutex, StreamExt};

use crate::ast::{PropertyValue, RelationshipDirection};
use crate::csr_expand::{cooperative_yield, Adjacency, Neighbors};
use crate::path_inventory::{id_column, property_fields, EdgeSource, Inventory, NodeSource};
use crate::path_value::{entity_type, path_type, paths_array, PathRows};

type Parents = HashMap<i64, Vec<i64>, ahash::RandomState>;

#[derive(Debug, Clone)]
pub(crate) struct Binding {
    pub variable: String,
    pub label: usize,
    pub id_column: String,
    pub properties: HashMap<String, PropertyValue>,
}

#[derive(Debug, Clone)]
pub(crate) struct Step {
    pub types: Vec<usize>,
    pub direction: RelationshipDirection,
    pub min: u32,
    pub max: u32,
    pub relationship: Option<String>,
    pub properties: HashMap<String, PropertyValue>,
}

#[derive(Debug)]
pub(crate) struct PathSpec {
    pub variable: String,
    pub sources: crate::path_inventory::SourceBatches,
    pub nodes: Vec<NodeSource>,
    pub edges: Vec<EdgeSource>,
    pub bindings: Vec<Binding>,
    pub steps: Vec<Step>,
    /// Some(all): exactly one shortest-path segment, with both endpoints bound.
    pub shortest: Option<bool>,
}

#[derive(Clone)]
pub(crate) struct PathMatchNode {
    inputs: Vec<LogicalPlan>,
    spec: Arc<PathSpec>,
    schema: DFSchemaRef,
    outputs: Vec<Output>,
}

#[derive(Debug, Clone)]
enum Output {
    RelationshipValue { step: usize },
    Node { boundary: usize, property: usize },
    Relationship { step: usize, property: usize },
}

// DataFusion formats extension nodes during planning; never print cached graph batches.
impl fmt::Debug for PathMatchNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PathMatchNode")
            .field("variable", &self.spec.variable)
            .field("sources", &(self.inputs.len() - 1))
            .finish()
    }
}

impl PathMatchNode {
    pub fn try_new(inputs: Vec<LogicalPlan>, spec: PathSpec) -> Result<Self> {
        let node_fields = property_fields(spec.nodes.iter().map(|s| s.schema.clone()))?;
        let edge_fields = property_fields(spec.edges.iter().map(|s| s.schema.clone()))?;
        let node_type = entity_type(node_fields.clone(), "node", None);
        let edge_type = entity_type(edge_fields.clone(), "relationship", Some(&node_type));
        let mut fields = Vec::new();
        let mut outputs = Vec::new();
        let input = inputs[0].schema();
        for (boundary, binding) in spec.bindings.iter().enumerate().skip(1) {
            if input.has_column_with_unqualified_name(&binding.id_column) {
                continue;
            }
            for field in spec.nodes[binding.label].schema.fields() {
                let name = crate::case_insensitive::qualify_column(&binding.variable, field.name());
                if fields.iter().any(|f: &Field| f.name() == &name) {
                    continue;
                }
                let property = node_fields
                    .iter()
                    .position(|f| f.name().eq_ignore_ascii_case(field.name()))
                    .expect("unified property");
                fields.push(Field::new(name, field.data_type().clone(), true));
                outputs.push(Output::Node { boundary, property });
            }
        }
        for (step, s) in spec.steps.iter().enumerate() {
            if let Some(variable) = &s.relationship {
                if s.min != 1 || s.max != 1 {
                    return datafusion::common::plan_err!("a relationship variable in a named variable-length path is not supported; use relationships(path)");
                }
                fields.push(Field::new(variable.to_lowercase(), edge_type.clone(), true));
                outputs.push(Output::RelationshipValue { step });
                for field in &edge_fields {
                    fields.push(Field::new(
                        crate::case_insensitive::qualify_column(variable, field.name()),
                        field.data_type().clone(),
                        true,
                    ));
                    outputs.push(Output::Relationship {
                        step,
                        property: edge_fields
                            .iter()
                            .position(|f| f.name() == field.name())
                            .unwrap(),
                    });
                }
            }
        }
        fields.push(Field::new(
            spec.variable.to_lowercase(),
            path_type(node_type, edge_type),
            true,
        ));
        let schema = Arc::new(input.join(&DFSchema::try_from(Schema::new(fields))?)?);
        Ok(Self {
            inputs,
            spec: Arc::new(spec),
            schema,
            outputs,
        })
    }
}
impl PartialEq for PathMatchNode {
    fn eq(&self, other: &Self) -> bool {
        self.inputs == other.inputs && Arc::ptr_eq(&self.spec, &other.spec)
    }
}
impl Eq for PathMatchNode {}
impl Hash for PathMatchNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inputs.hash(state);
        Arc::as_ptr(&self.spec).hash(state);
    }
}
impl PartialOrd for PathMatchNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Arc::as_ptr(&self.spec).partial_cmp(&Arc::as_ptr(&other.spec))
    }
}
impl UserDefinedLogicalNodeCore for PathMatchNode {
    fn name(&self) -> &str {
        "PathMatch"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        self.schema
            .fields()
            .iter()
            .skip(self.inputs[0].schema().fields().len())
            .map(|f| f.name().clone())
            .collect()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PathMatch: {}, {}",
            self.spec.variable,
            if self.spec.shortest.is_some() {
                "shortest edge inventory"
            } else {
                "trail edge inventory"
            }
        )
    }
    fn with_exprs_and_inputs(&self, _: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            inputs,
            ..self.clone()
        })
    }
}

#[derive(Debug)]
struct Ready {
    inventory: Inventory,
    nodes_allowed: Vec<BooleanArray>,
    edges_allowed: Vec<BooleanArray>,
    shortest: Option<(Neighbors, Neighbors)>,
}

struct PathMatchExec {
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    spec: Arc<PathSpec>,
    outputs: Vec<Output>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    predicates: Vec<Option<Arc<dyn PhysicalExpr>>>,
    ready: Arc<Mutex<Option<Arc<Ready>>>>,
}

fn predicate(properties: &HashMap<String, PropertyValue>) -> Option<Expr> {
    properties
        .iter()
        .map(|(key, value)| {
            col(key.to_lowercase()).eq(crate::datafusion_planner::expression::to_df_value_expr(
                &crate::ast::ValueExpression::Literal(value.clone()),
            ))
        })
        .reduce(Expr::and)
}

pub(crate) fn plan_path_match(
    node: &PathMatchNode,
    inputs: &[Arc<dyn ExecutionPlan>],
    state: &SessionState,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut plans = vec![inputs[0].clone()];
    plans.extend(
        inputs[1..]
            .iter()
            .map(|p| Arc::new(CoalescePartitionsExec::new(p.clone())) as Arc<dyn ExecutionPlan>),
    );
    let nodes = DFSchema::try_from(Schema::new(property_fields(
        node.spec.nodes.iter().map(|s| s.schema.clone()),
    )?))?;
    let edges = DFSchema::try_from(Schema::new(property_fields(
        node.spec.edges.iter().map(|s| s.schema.clone()),
    )?))?;
    let predicates = node
        .spec
        .bindings
        .iter()
        .map(|b| (&b.properties, &nodes))
        .chain(node.spec.steps.iter().map(|s| (&s.properties, &edges)))
        .map(|(properties, schema)| {
            predicate(properties)
                .map(|e| state.create_physical_expr(e, schema))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = Arc::new(node.schema.as_arrow().clone());
    let properties = Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema.clone()),
        inputs[0].output_partitioning().clone(),
        EmissionType::Incremental,
        Boundedness::Bounded,
    ));
    Ok(Arc::new(PathMatchExec {
        inputs: plans,
        spec: node.spec.clone(),
        outputs: node.outputs.clone(),
        schema,
        properties,
        predicates,
        ready: Arc::new(Mutex::new(None)),
    }))
}

impl fmt::Debug for PathMatchExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PathMatchExec")
            .field("variable", &self.spec.variable)
            .field("sources", &(self.inputs.len() - 1))
            .finish()
    }
}

impl DisplayAs for PathMatchExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PathMatchExec: {} edge inventory", self.spec.variable)
    }
}
impl ExecutionPlan for PathMatchExec {
    fn name(&self) -> &str {
        "PathMatchExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }
    // Inventory builds consume one stream per source; keep the coalescers through optimization.
    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        use datafusion::physical_plan::Distribution;
        std::iter::once(Distribution::UnspecifiedDistribution)
            .chain((1..self.inputs.len()).map(|_| Distribution::SinglePartition))
            .collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(self.schema.clone()),
            inputs[0].output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(Self {
            inputs,
            spec: self.spec.clone(),
            outputs: self.outputs.clone(),
            schema: self.schema.clone(),
            properties,
            predicates: self.predicates.clone(),
            ready: self.ready.clone(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.inputs[0].execute(partition, context.clone())?;
        let (cell, plans, spec, outputs, schema, predicates) = (
            self.ready.clone(),
            self.inputs[1..].to_vec(),
            self.spec.clone(),
            self.outputs.clone(),
            self.schema.clone(),
            self.predicates.clone(),
        );
        let out_schema = schema.clone();
        let batch_size = context.session_config().batch_size();
        let stream = futures::stream::try_unfold(
            (input, None::<Pending>),
            move |(mut input, mut pending)| {
                let (cell, plans, spec, outputs, schema, predicates, context) = (
                    cell.clone(),
                    plans.clone(),
                    spec.clone(),
                    outputs.clone(),
                    schema.clone(),
                    predicates.clone(),
                    context.clone(),
                );
                async move {
                    loop {
                        if pending.is_none() {
                            let Some(batch) = input.next().await else {
                                return Ok(None);
                            };
                            let batch = batch?;
                            let graph = {
                                let mut ready = cell.lock().await;
                                if ready.is_none() {
                                    *ready = Some(Arc::new(
                                        Ready::build(&spec, &plans, &predicates, context.clone())
                                            .await?,
                                    ));
                                }
                                ready.as_ref().expect("built").clone()
                            };
                            pending = Some(Pending::new(batch, graph, &spec)?);
                        }
                        let p = pending.as_mut().unwrap();
                        let (rows, paths) = p.next(&spec, batch_size).await?;
                        if rows.is_empty() {
                            pending = None;
                            continue;
                        }
                        let indices = UInt32Array::from(rows);
                        let mut columns: Vec<ArrayRef> = p
                            .batch
                            .columns()
                            .iter()
                            .map(|c| take(c, &indices, None))
                            .collect::<std::result::Result<_, _>>()?;
                        for output in &outputs {
                            let (array, positions) = match output {
                                Output::RelationshipValue { step } => {
                                    let positions = UInt32Array::from_iter_values(
                                        paths.iter().map(|p| p.edges[p.boundaries[*step]]),
                                    );
                                    columns.push(take(&p.graph.inventory.edges, &positions, None)?);
                                    continue;
                                }
                                Output::Node { boundary, property } => (
                                    p.graph.inventory.node_properties.column(*property),
                                    UInt32Array::from_iter_values(
                                        paths.iter().map(|p| p.nodes[p.boundaries[*boundary]]),
                                    ),
                                ),
                                Output::Relationship { step, property } => (
                                    p.graph.inventory.edge_properties.column(*property),
                                    UInt32Array::from_iter_values(
                                        paths.iter().map(|p| p.edges[p.boundaries[*step]]),
                                    ),
                                ),
                            };
                            columns.push(take(array, &positions, None)?);
                        }
                        columns.push(paths_array(
                            &paths,
                            &p.graph.inventory.nodes,
                            &p.graph.inventory.edges,
                        )?);
                        let batch = RecordBatch::try_new(schema, columns)?;
                        return Ok(Some((batch, (input, pending))));
                    }
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

impl Ready {
    async fn build(
        spec: &PathSpec,
        plans: &[Arc<dyn ExecutionPlan>],
        predicates: &[Option<Arc<dyn PhysicalExpr>>],
        context: Arc<TaskContext>,
    ) -> Result<Self> {
        let inventory =
            Inventory::build(&spec.nodes, &spec.edges, plans, context, &spec.sources).await?;
        let mask =
            |p: &Option<Arc<dyn PhysicalExpr>>, batch: &RecordBatch| -> Result<BooleanArray> {
                match p {
                    Some(p) => Ok(p
                        .evaluate(batch)?
                        .into_array(batch.num_rows())?
                        .as_boolean()
                        .clone()),
                    None => Ok(BooleanArray::from(vec![true; batch.num_rows()])),
                }
            };
        let nodes_allowed = predicates[..spec.bindings.len()]
            .iter()
            .map(|p| mask(p, &inventory.node_properties))
            .collect::<Result<Vec<_>>>()?;
        let edges_allowed = predicates[spec.bindings.len()..]
            .iter()
            .map(|p| mask(p, &inventory.edge_properties))
            .collect::<Result<Vec<_>>>()?;
        let shortest = if spec.shortest.is_some() {
            let step = &spec.steps[0];
            let mut from = Vec::new();
            let mut to = Vec::new();
            for (edge, ends) in inventory.endpoints.iter().enumerate() {
                if let Some((a, b)) = ends {
                    let vertex = inventory.nodes.len() as i64 + edge as i64;
                    let (a, b) = (*a as i64, *b as i64);
                    match step.direction {
                        RelationshipDirection::Outgoing => {
                            from.extend([a, vertex]);
                            to.extend([vertex, b]);
                        }
                        RelationshipDirection::Incoming => {
                            from.extend([b, vertex]);
                            to.extend([vertex, a]);
                        }
                        RelationshipDirection::Undirected => {
                            from.extend([a, vertex, b, vertex]);
                            to.extend([vertex, b, vertex, a]);
                        }
                    }
                }
                if edge % 1024 == 1023 {
                    cooperative_yield().await;
                }
            }
            let batch = RecordBatch::try_from_iter([
                ("src", Arc::new(Int64Array::from(from)) as ArrayRef),
                ("dst", Arc::new(Int64Array::from(to)) as ArrayRef),
            ])?;
            let stream = || {
                let chunks: Vec<_> = (0..batch.num_rows())
                    .step_by(1024)
                    .map(|start| batch.slice(start, (batch.num_rows() - start).min(1024)))
                    .collect();
                Box::pin(RecordBatchStreamAdapter::new(
                    batch.schema(),
                    futures::stream::iter(chunks).then(|batch| async move {
                        cooperative_yield().await;
                        Ok(batch)
                    }),
                )) as SendableRecordBatchStream
            };
            Some((
                Neighbors::directed(Arc::new(
                    Adjacency::build(stream(), "src", "dst")
                        .await
                        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?,
                )),
                Neighbors::directed(Arc::new(
                    Adjacency::build(stream(), "dst", "src")
                        .await
                        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?,
                )),
            ))
        } else {
            None
        };
        Ok(Self {
            inventory,
            nodes_allowed,
            edges_allowed,
            shortest,
        })
    }
}

struct Pending {
    batch: RecordBatch,
    graph: Arc<Ready>,
    keys: Vec<Option<Int64Array>>,
    row: usize,
    walk: Option<Walk>,
    shortest: Option<ShortestPaths>,
}
impl Pending {
    fn new(batch: RecordBatch, graph: Arc<Ready>, spec: &PathSpec) -> Result<Self> {
        let keys = spec
            .bindings
            .iter()
            .map(|b| {
                batch
                    .schema()
                    .index_of(&b.id_column)
                    .ok()
                    .map(|_| id_column(&batch, &b.id_column))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            batch,
            graph,
            keys,
            row: 0,
            walk: None,
            shortest: None,
        })
    }
    fn bound(&self, spec: &PathSpec) -> Option<Vec<Option<u32>>> {
        self.keys
            .iter()
            .zip(&spec.bindings)
            .map(|(keys, binding)| match keys {
                None => Some(None),
                Some(keys) if keys.is_null(self.row) => None,
                Some(keys) => self
                    .graph
                    .inventory
                    .by_key
                    .get(&(binding.label, keys.value(self.row)))
                    .copied()
                    .map(Some),
            })
            .collect()
    }
    async fn next(&mut self, spec: &PathSpec, size: usize) -> Result<(Vec<u32>, Vec<PathRows>)> {
        let mut rows = Vec::new();
        let mut paths = Vec::new();
        while self.row < self.batch.num_rows() && rows.len() < size {
            if self.walk.is_none() && self.shortest.is_none() {
                let Some(bound) = self.bound(spec) else {
                    self.row += 1;
                    continue;
                };
                let source = bound[0].expect("source bound by planner");
                if !allowed(&self.graph.nodes_allowed[0], source) {
                    self.row += 1;
                    continue;
                }
                if let Some(all) = spec.shortest {
                    let target = bound[1].expect("shortest target bound");
                    let (forward, reverse) =
                        self.graph.shortest.as_ref().expect("shortest adjacency");
                    let step = &spec.steps[0];
                    let max = step.max.saturating_mul(2);
                    let (depths, parents) = crate::shortest_path::search_bidirectional(
                        forward,
                        reverse,
                        source as i64,
                        target as i64,
                        max,
                    )
                    .await;
                    if depths
                        .get(&(target as i64))
                        .is_none_or(|&depth| depth / 2 < step.min)
                    {
                        self.row += 1;
                        continue;
                    }
                    self.shortest = Some(ShortestPaths::new(
                        target as i64,
                        parents,
                        all,
                        self.graph.inventory.nodes.len(),
                    ));
                } else {
                    self.walk = Some(Walk::new(source, bound));
                }
            }
            let path = if let Some(search) = &mut self.shortest {
                search.next().await
            } else {
                self.walk.as_mut().unwrap().next(spec, &self.graph).await
            };
            if let Some(path) = path {
                rows.push(self.row as u32);
                paths.push(path);
            } else {
                self.walk = None;
                self.shortest = None;
                self.row += 1;
            }
            cooperative_yield().await;
        }
        Ok((rows, paths))
    }
}

fn allowed(mask: &BooleanArray, row: u32) -> bool {
    mask.is_valid(row as usize) && mask.value(row as usize)
}

struct Frame {
    stage: usize,
    hops: u32,
    node: u32,
    next: usize,
    advanced: bool,
    entered: bool,
}
struct Walk {
    frames: Vec<Frame>,
    path: PathRows,
    bound: Vec<Option<u32>>,
}
impl Walk {
    fn new(source: u32, bound: Vec<Option<u32>>) -> Self {
        Self {
            frames: vec![Frame {
                stage: 0,
                hops: 0,
                node: source,
                next: 0,
                advanced: false,
                entered: false,
            }],
            path: PathRows {
                nodes: vec![source],
                edges: vec![],
                boundaries: vec![0],
            },
            bound,
        }
    }
    fn pop(&mut self) {
        let frame = self.frames.pop().unwrap();
        if frame.entered {
            self.path.nodes.pop();
            self.path.edges.pop();
        } else if frame.stage > 0 {
            self.path.boundaries.pop();
        }
    }
    async fn next(&mut self, spec: &PathSpec, graph: &Ready) -> Option<PathRows> {
        let mut work = 0_usize;
        while let Some(frame) = self.frames.last_mut() {
            work += 1;
            if work.is_multiple_of(1024) {
                cooperative_yield().await;
            }
            if frame.stage == spec.steps.len() {
                if !frame.advanced {
                    frame.advanced = true;
                    return Some(self.path.clone());
                }
                self.pop();
                continue;
            }
            let step = &spec.steps[frame.stage];
            let binding = &spec.bindings[frame.stage + 1];
            if !frame.advanced {
                frame.advanced = true;
                let same = spec.bindings[..frame.stage + 1]
                    .iter()
                    .position(|b| b.variable == binding.variable)
                    .map(|i| self.path.nodes[self.path.boundaries[i]]);
                if frame.hops >= step.min
                    && graph.inventory.node_labels[frame.node as usize] == binding.label
                    && self.bound[frame.stage + 1].is_none_or(|n| n == frame.node)
                    && same.is_none_or(|n| n == frame.node)
                    && allowed(&graph.nodes_allowed[frame.stage + 1], frame.node)
                {
                    let next = Frame {
                        stage: frame.stage + 1,
                        hops: 0,
                        node: frame.node,
                        next: 0,
                        advanced: false,
                        entered: false,
                    };
                    self.path.boundaries.push(self.path.nodes.len() - 1);
                    self.frames.push(next);
                    continue;
                }
            }
            if frame.hops == step.max {
                self.pop();
                continue;
            }
            let Some(edge) = graph.inventory.neighbors[frame.node as usize].get(frame.next) else {
                self.pop();
                continue;
            };
            frame.next += 1;
            let direction = match step.direction {
                RelationshipDirection::Outgoing => edge.outgoing,
                RelationshipDirection::Incoming => !edge.outgoing || edge.node == frame.node,
                RelationshipDirection::Undirected => true,
            };
            if !direction
                || !step
                    .types
                    .contains(&graph.inventory.edge_types[edge.edge as usize])
                || self.path.edges.contains(&edge.edge)
                || !allowed(&graph.edges_allowed[frame.stage], edge.edge)
            {
                continue;
            }
            let next = Frame {
                stage: frame.stage,
                hops: frame.hops + 1,
                node: edge.node,
                next: 0,
                advanced: false,
                entered: true,
            };
            self.path.nodes.push(edge.node);
            self.path.edges.push(edge.edge);
            self.frames.push(next);
        }
        None
    }
}

/// Depth-first predecessor enumeration keeps only one in-progress shortest path.
struct ShortestPaths {
    parents: Parents,
    stack: Vec<(i64, usize)>,
    all: bool,
    done: bool,
    nodes: usize,
}
impl ShortestPaths {
    fn new(target: i64, parents: Parents, all: bool, nodes: usize) -> Self {
        Self {
            parents,
            stack: vec![(target, 0)],
            all,
            done: false,
            nodes,
        }
    }
    async fn next(&mut self) -> Option<PathRows> {
        if self.done {
            return None;
        }
        let mut work = 0_usize;
        while let Some((node, next)) = self.stack.last_mut() {
            work += 1;
            if work.is_multiple_of(1024) {
                cooperative_yield().await;
            }
            match self.parents.get(node) {
                None if *next == 0 => {
                    *next = 1;
                    let nodes: Vec<_> = self
                        .stack
                        .iter()
                        .rev()
                        .step_by(2)
                        .map(|(n, _)| *n as u32)
                        .collect();
                    let edges = self
                        .stack
                        .iter()
                        .rev()
                        .skip(1)
                        .step_by(2)
                        .map(|(n, _)| (*n as usize - self.nodes) as u32)
                        .collect();
                    let end = nodes.len() - 1;
                    self.done = !self.all;
                    return Some(PathRows {
                        nodes,
                        edges,
                        boundaries: vec![0, end],
                    });
                }
                Some(parents) if *next < parents.len() => {
                    let parent = parents[*next];
                    *next += 1;
                    self.stack.push((parent, 0));
                }
                _ => {
                    self.stack.pop();
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    async fn graph(
        edges: &[(i64, i64)],
        max: u32,
        shortest: Option<bool>,
    ) -> (PathSpec, Arc<Ready>) {
        let n = edges
            .iter()
            .flat_map(|&(a, b)| [a, b])
            .max()
            .unwrap_or(3)
            .max(3);
        let nodes = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from_iter_values(1..=n)) as ArrayRef,
        )])
        .unwrap();
        let edge_batch = RecordBatch::try_from_iter([
            (
                "src",
                Arc::new(Int64Array::from_iter_values(edges.iter().map(|e| e.0))) as ArrayRef,
            ),
            (
                "dst",
                Arc::new(Int64Array::from_iter_values(edges.iter().map(|e| e.1))) as ArrayRef,
            ),
            (
                "weight",
                Arc::new(Int64Array::from_iter_values(0..edges.len() as i64)) as ArrayRef,
            ),
        ])
        .unwrap();
        let binding = |variable: &str| Binding {
            variable: variable.into(),
            label: 0,
            id_column: format!("{variable}__id"),
            properties: HashMap::new(),
        };
        let spec = PathSpec {
            variable: "p".into(),
            sources: Default::default(),
            nodes: vec![NodeSource {
                label: "Person".into(),
                id: "id".into(),
                schema: nodes.schema(),
            }],
            edges: vec![EdgeSource {
                label: "R".into(),
                source_label: 0,
                target_label: 0,
                source_id: "src".into(),
                target_id: "dst".into(),
                schema: edge_batch.schema(),
            }],
            bindings: vec![binding("a"), binding("b")],
            steps: vec![Step {
                types: vec![0],
                direction: RelationshipDirection::Outgoing,
                min: 0,
                max,
                relationship: None,
                properties: HashMap::new(),
            }],
            shortest,
        };
        let ctx = SessionContext::new();
        let mut plans = Vec::new();
        for batch in [nodes, edge_batch] {
            plans.push(
                ctx.read_batch(batch)
                    .unwrap()
                    .create_physical_plan()
                    .await
                    .unwrap(),
            );
        }
        let ready = Ready::build(&spec, &plans, &[None, None, None], ctx.task_ctx())
            .await
            .unwrap();
        (spec, Arc::new(ready))
    }

    #[tokio::test]
    async fn named_walk_keeps_edge_occurrences_and_never_reuses_one() {
        let (spec, graph) = graph(&[(1, 2), (1, 2), (2, 3), (3, 3)], 20, None).await;
        let mut walk = Walk::new(0, vec![Some(0), None]);
        let mut paths = Vec::new();
        while let Some(path) = walk.next(&spec, &graph).await {
            paths.push(path);
        }
        assert_eq!(paths.len(), 7); // identity, two one-hop, two two-hop, two with self-loop
        assert!(paths
            .iter()
            .all(|p| p.edges.iter().collect::<HashSet<_>>().len() == p.edges.len()));
        let two: Vec<_> = paths
            .iter()
            .filter(|p| p.edges.len() == 2)
            .map(|p| p.edges.clone())
            .collect();
        assert_eq!(two, vec![vec![0, 2], vec![1, 2]]);
        let array = paths_array(&paths, &graph.inventory.nodes, &graph.inventory.edges).unwrap();
        let rels = array.as_struct().column(3).as_list::<i32>();
        assert_eq!(rels.value(0).len(), 0);
        let first = rels.value(1);
        assert_eq!(
            first
                .as_struct()
                .column(1)
                .as_struct()
                .column(1)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(0),
            0
        );
    }

    #[tokio::test]
    async fn shortest_values_preserve_parallel_edges_and_bounded_output() {
        let (spec, graph) = graph(
            &[(1, 2), (1, 2), (2, 3), (2, 3), (3, 3)],
            u32::MAX,
            Some(true),
        )
        .await;
        let batch = RecordBatch::try_from_iter([
            ("a__id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ("b__id", Arc::new(Int64Array::from(vec![3])) as ArrayRef),
        ])
        .unwrap();
        let mut pending = Pending::new(batch, graph, &spec).unwrap();
        let mut edges = Vec::new();
        loop {
            let (rows, paths) = pending.next(&spec, 1).await.unwrap();
            assert!(rows.len() <= 1);
            if rows.is_empty() {
                break;
            }
            assert_eq!(paths[0].nodes, vec![0, 1, 2]);
            edges.push(paths[0].edges.clone());
        }
        edges.sort();
        assert_eq!(edges, vec![vec![0, 2], vec![0, 3], vec![1, 2], vec![1, 3]]);
    }

    #[tokio::test]
    async fn rejected_hub_yields_before_scanning_all_edges() {
        let (spec, graph) = graph(&(2..100_002).map(|n| (1, n)).collect::<Vec<_>>(), 1, None).await;
        let mut walk = Walk::new(0, vec![Some(0), Some(0)]);
        assert!(walk.next(&spec, &graph).await.is_some()); // zero hop
        let mut next = Box::pin(walk.next(&spec, &graph));
        assert!(futures::poll!(next.as_mut()).is_pending());
        drop(next);
        assert!(walk.frames[0].next > 0 && walk.frames[0].next <= 1024);
    }
}
