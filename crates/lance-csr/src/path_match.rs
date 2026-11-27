// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! Rich paths enumerate trails resumably, either by walking the engine's CSR, checking
//! relationship filters only on the edges reached (the approach Kùzu takes for filtered
//! recursive joins), or over an immutable edge inventory. Shortest paths use the existing
//! bidirectional BFS on a bipartite node/edge graph of the inventory.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch, StructArray, UInt32Array,
};
use arrow::compute::take;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, DFSchemaRef, Result};
use datafusion::execution::session_state::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{col, Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::{lock::Mutex, StreamExt};

use crate::ast::{PropertyValue, RelationshipDirection};
use crate::csr_expand::{
    cooperative_yield, mask_after, Adjacency, CompiledFilter, EdgeFilterState, EdgeRows, Neighbors,
};
use crate::path_inventory::{
    id_column, properties, property_fields, relationship_identity, EdgeSource, Inventory,
    NodeSource, SourceBatches,
};
use crate::path_value::{entity_type, path_type, paths_array, PathRows};

type Parents = HashMap<i64, Vec<i64>, ahash::RandomState>;

#[derive(Debug, Clone)]
pub(crate) struct Binding {
    pub variable: String,
    pub label: usize,
    pub id_column: String,
}

#[derive(Debug, Clone)]
pub(crate) struct Step {
    pub types: Vec<usize>,
    pub direction: RelationshipDirection,
    pub min: u32,
    pub max: u32,
    pub relationship: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct PathSpec {
    pub variable: Option<String>,
    /// Expose typed boundary-node values alongside their property columns.
    pub node_values: bool,
    pub nodes: Vec<NodeSource>,
    pub edges: Vec<EdgeSource>,
    pub bindings: Vec<Binding>,
    pub steps: Vec<Step>,
    /// Some(all): exactly one shortest-path segment, with both endpoints bound.
    pub shortest: Option<bool>,
    /// Walk the engine's CSR instead of building an edge inventory.
    pub csr: Option<CsrRoute>,
    /// The walk runs from the written pattern's end; path values are reversed back.
    pub reversed: bool,
}

/// The engine's CSR for every relationship a trail walk follows. Edges are read
/// only where the walk goes; an edge's identity is its row in the CSR.
#[derive(Debug, Clone)]
pub(crate) struct CsrRoute {
    /// Per step, every (type, direction) adjacency the step follows. Each type's rows are
    /// its `EdgeSource::rows`.
    pub steps: Vec<Vec<CsrHop>>,
}

impl PathSpec {
    /// The engine rows of relationship type `kind`, which the CSR route requires.
    fn rows(&self, kind: usize) -> &EdgeRows {
        self.edges[kind]
            .rows
            .as_deref()
            .expect("csr route has engine rows")
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CsrHop {
    pub kind: usize,
    pub neighbors: Neighbors,
    /// Labels of the node a hop leaves and the node it reaches.
    pub from: usize,
    pub to: usize,
    /// The adjacency is keyed by target: the edge runs from `to` to `from`.
    pub reversed: bool,
    /// The incoming half of an undirected step: the outgoing half already matched self-loops.
    pub skip_self_loops: bool,
}

#[derive(Clone)]
pub(crate) struct PathMatchNode {
    inputs: Vec<LogicalPlan>,
    spec: Arc<PathSpec>,
    sources: SourceBatches,
    // Node bindings first, then relationship steps, in traversal order.
    filters: Vec<Vec<(String, Expr)>>,
    /// A condition every relationship of the path meets, over its property columns.
    relationship_condition: Option<Expr>,
    schema: DFSchemaRef,
    outputs: Vec<Output>,
}

#[derive(Debug, Clone)]
enum Output {
    NodeValue { boundary: usize },
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
    pub fn try_new<'a>(
        inputs: Vec<LogicalPlan>,
        spec: PathSpec,
        sources: SourceBatches,
        properties: impl IntoIterator<Item = &'a HashMap<String, PropertyValue>>,
        relationship_condition: Option<Expr>,
    ) -> Result<Self> {
        let filters = properties
            .into_iter()
            .map(|properties| {
                properties
                    .iter()
                    .map(|(key, value)| {
                        let value = crate::datafusion_planner::expression::to_df_value_expr(
                            &crate::ast::ValueExpression::Literal(value.clone()),
                        );
                        (key.to_lowercase(), value)
                    })
                    .collect()
            })
            .collect();
        let node_fields = property_fields(spec.nodes.iter().map(|s| s.schema.fields()))?;
        let edge_fields = property_fields(spec.edges.iter().map(|s| s.schema.fields()))?;
        let node_type = entity_type(node_fields.clone(), "node", None);
        let edge_type = entity_type(edge_fields.clone(), "relationship", Some(&node_type));
        let mut fields = Vec::new();
        let mut outputs = Vec::new();
        let input = inputs[0].schema();
        for (boundary, binding) in spec
            .bindings
            .iter()
            .enumerate()
            .filter(|_| spec.node_values)
        {
            let name = binding.variable.to_lowercase();
            if !input.has_column_with_unqualified_name(&name)
                && !fields.iter().any(|f: &Field| f.name() == &name)
            {
                fields.push(Field::new(name, node_type.clone(), true));
                outputs.push(Output::NodeValue { boundary });
            }
        }
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
                fields.push(field.as_ref().clone().with_name(name).with_nullable(true));
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
                    fields.push(
                        field
                            .as_ref()
                            .clone()
                            .with_name(crate::case_insensitive::qualify_column(
                                variable,
                                field.name(),
                            ))
                            .with_nullable(true),
                    );
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
        if let Some(variable) = &spec.variable {
            fields.push(Field::new(
                variable.to_lowercase(),
                path_type(node_type, edge_type),
                true,
            ));
        }
        let schema = Arc::new(input.join(&DFSchema::try_from(Schema::new(fields))?)?);
        Ok(Self {
            inputs,
            spec: Arc::new(spec),
            sources,
            filters,
            relationship_condition,
            schema,
            outputs,
        })
    }

    /// This node without its path value, when no column in `used` reads the path.
    pub(crate) fn without_unused_path(&self, used: &HashSet<String>) -> Result<Option<Self>> {
        let Some(variable) = &self.spec.variable else {
            return Ok(None);
        };
        if used.contains(&variable.to_lowercase()) {
            return Ok(None);
        }
        // The path value is the last column `try_new` adds.
        let kept = self.schema.fields().len() - 1;
        let schema = DFSchema::new_with_metadata(
            self.schema
                .iter()
                .take(kept)
                .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
                .collect(),
            self.schema.metadata().clone(),
        )?
        .with_functional_dependencies(self.schema.functional_dependencies().clone())?;
        let mut spec = PathSpec::clone(&self.spec);
        spec.variable = None;
        Ok(Some(Self {
            spec: Arc::new(spec),
            schema: Arc::new(schema),
            ..self.clone()
        }))
    }
}
impl PartialEq for PathMatchNode {
    fn eq(&self, other: &Self) -> bool {
        self.inputs == other.inputs
            && Arc::ptr_eq(&self.spec, &other.spec)
            && self.filters == other.filters
            && self.relationship_condition == other.relationship_condition
    }
}
impl Eq for PathMatchNode {}
impl Hash for PathMatchNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inputs.hash(state);
        Arc::as_ptr(&self.spec).hash(state);
        self.filters.hash(state);
        self.relationship_condition.hash(state);
    }
}
impl PartialOrd for PathMatchNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Arc::as_ptr(&self.spec)
            .partial_cmp(&Arc::as_ptr(&other.spec))
            // Same spec but different bound filters are not equal, so don't report Equal (PartialOrd must agree with PartialEq).
            .filter(|order| *order != Ordering::Equal || self == other)
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
        // The property columns belong to the inventory, so only expose the values and
        // the condition's parameters, which DataFusion binds per execution.
        self.filters
            .iter()
            .flatten()
            .map(|(_, value)| value.clone())
            .chain(placeholders(self.relationship_condition.as_ref()))
            .collect()
    }
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        // DataFusion copies a pushed predicate into every input, so only columns that
        // every input carries may be pushed; others (start-node columns are only in the
        // first input) stay above this node. The logical planner filters the start scan
        // with the WHERE conjuncts on the start node instead (`conjuncts_on`).
        self.schema
            .fields()
            .iter()
            .map(|f| f.name())
            .filter(|name| {
                !self
                    .inputs
                    .iter()
                    .all(|input| input.schema().has_column_with_unqualified_name(name))
            })
            .cloned()
            .collect()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PathMatch: {}, {}",
            self.spec.variable.as_deref().unwrap_or("endpoints"),
            if self.spec.shortest.is_some() {
                "shortest edge inventory"
            } else if self.spec.csr.is_some() {
                "trail csr walk"
            } else {
                "trail edge inventory"
            }
        )
    }
    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        let mut node = self.clone();
        let values = node.filters.iter().map(Vec::len).sum::<usize>();
        if exprs.len() != values + placeholders(node.relationship_condition.as_ref()).len() {
            return datafusion::common::internal_err!(
                "PathMatch expected one expression per property and parameter"
            );
        }
        let mut exprs = exprs.into_iter();
        for ((_, value), expr) in node.filters.iter_mut().flatten().zip(exprs.by_ref()) {
            *value = expr;
        }
        if let Some(condition) = node.relationship_condition.take() {
            // Placeholders are leaves, so post-order meets them in `placeholders`' order.
            node.relationship_condition = Some(
                condition
                    .transform_up(|e| match e {
                        Expr::Placeholder(_) => {
                            Ok(Transformed::yes(exprs.next().expect("counted above")))
                        }
                        e => Ok(Transformed::no(e)),
                    })
                    .data()?,
            );
        }
        node.inputs = inputs;
        Ok(node)
    }
}

/// The parameters in `condition`, in tree order.
fn placeholders(condition: Option<&Expr>) -> Vec<Expr> {
    let mut found = Vec::new();
    if let Some(condition) = condition {
        condition
            .apply(|e| {
                if matches!(e, Expr::Placeholder(_)) {
                    found.push(e.clone());
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("visiting does not fail");
    }
    found
}

/// `condition` with its columns named as in `fields`; a property they lack is null.
fn on_fields(condition: &Expr, fields: &arrow_schema::Fields) -> Result<Expr> {
    condition
        .clone()
        .transform(|e| match &e {
            Expr::Column(c) => Ok(Transformed::yes(
                match fields
                    .iter()
                    .find(|f| f.name().eq_ignore_ascii_case(&c.name))
                {
                    Some(f) => Expr::Column(datafusion::common::Column::from_name(f.name())),
                    None => datafusion::logical_expr::lit(datafusion::common::ScalarValue::Null),
                },
            )),
            _ => Ok(Transformed::no(e)),
        })
        .data()
}

pub(crate) fn has_path_match(plan: &LogicalPlan) -> Result<bool> {
    Ok(plan.apply_with_subqueries(|plan| {
        Ok(match plan {
            LogicalPlan::Extension(ext) if ext.node.as_any().is::<PathMatchNode>() => {
                TreeNodeRecursion::Stop
            }
            _ => TreeNodeRecursion::Continue,
        })
    })? == TreeNodeRecursion::Stop)
}

/// Give each execution its own raw source batches while preserving sharing among
/// PathMatch nodes from the same graph planning context.
pub(crate) fn refresh_path_sources(plan: LogicalPlan) -> Result<LogicalPlan> {
    let mut sources = HashMap::<_, SourceBatches>::new();
    plan.transform_down_with_subqueries(|plan| {
        if let LogicalPlan::Extension(ext) = &plan {
            if let Some(path) = ext.node.as_any().downcast_ref::<PathMatchNode>() {
                let mut path = path.clone();
                path.sources = sources
                    .entry(Arc::as_ptr(&path.sources))
                    .or_default()
                    .clone();
                return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(path),
                })));
            }
        }
        Ok(Transformed::no(plan))
    })
    .data()
}

#[derive(Debug)]
struct Ready {
    inventory: Inventory,
    nodes_allowed: Vec<BooleanArray>,
    edges_allowed: Vec<BooleanArray>,
    /// Each node's key, by inventory position.
    node_keys: Int64Array,
    shortest: Option<(Neighbors, Neighbors)>,
    /// CSR route only: per step and hop, its filter and what that filter has checked.
    hops: Vec<Vec<(HopFilter, Arc<EdgeFilterState>)>>,
    /// CSR route only: the inventory has no edges, so relationship values are built here.
    edge_type: arrow_schema::DataType,
    edge_fields: arrow_schema::Fields,
}

/// A step's relationship filter, compiled for one hop's table.
#[derive(Debug, Clone)]
enum HopFilter {
    All,
    /// The table lacks a filtered property, so no edge has a value equal to it.
    None,
    Where(CompiledFilter),
}

struct PathMatchExec {
    /// Per-partition metrics for `EXPLAIN ANALYZE`.
    metrics: datafusion::physical_plan::metrics::ExecutionPlanMetricsSet,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    spec: Arc<PathSpec>,
    sources: SourceBatches,
    outputs: Vec<Output>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    predicates: Vec<Option<Arc<dyn PhysicalExpr>>>,
    /// CSR route only: per step and hop, its relationship filter.
    hop_filters: Vec<Vec<HopFilter>>,
    ready: Arc<Mutex<Option<Arc<Ready>>>>,
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
        node.spec.nodes.iter().map(|s| s.schema.fields()),
    )?))?;
    let edges = DFSchema::try_from(Schema::new(property_fields(
        node.spec.edges.iter().map(|s| s.schema.fields()),
    )?))?;
    let predicates = node
        .filters
        .iter()
        .enumerate()
        .map(|(index, properties)| {
            let (schema, condition) = if index < node.spec.bindings.len() {
                (&nodes, None)
            } else {
                let condition = node.relationship_condition.as_ref();
                (
                    &edges,
                    condition
                        .map(|c| on_fields(c, edges.fields()))
                        .transpose()?,
                )
            };
            properties
                .iter()
                .map(|(key, value)| col(key.clone()).eq(value.clone()))
                .chain(condition)
                .reduce(Expr::and)
                .map(|expr| state.create_physical_expr(expr, schema))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    let hop_filters = node
        .spec
        .csr
        .iter()
        .flat_map(|csr| csr.steps.iter().enumerate())
        .map(|(step, hops)| {
            let filters = &node.filters[node.spec.bindings.len() + step];
            hops.iter()
                .map(|hop| {
                    if filters.is_empty() && node.relationship_condition.is_none() {
                        return Ok(HopFilter::All);
                    }
                    let table = node.spec.rows(hop.kind).schema();
                    let mut terms = Vec::new();
                    for (key, value) in filters {
                        let Some(field) = table
                            .fields()
                            .iter()
                            .find(|f| f.name().eq_ignore_ascii_case(key))
                        else {
                            return Ok(HopFilter::None);
                        };
                        terms.push(
                            Expr::Column(datafusion::common::Column::from_name(field.name()))
                                .eq(value.clone()),
                        );
                    }
                    if let Some(condition) = &node.relationship_condition {
                        terms.push(on_fields(condition, table.fields())?);
                    }
                    let filter = terms.into_iter().reduce(Expr::and).expect("filtered");
                    if filter.column_refs().is_empty() {
                        // The table lacks every property the condition reads, so the
                        // condition is the same for all its edges.
                        let one = RecordBatch::try_new_with_options(
                            Arc::new(Schema::empty()),
                            vec![],
                            &arrow::array::RecordBatchOptions::new().with_row_count(Some(1)),
                        )?;
                        let value = state
                            .create_physical_expr(filter, &DFSchema::empty())?
                            .evaluate(&one)?
                            .into_array(1)?;
                        let pass = value
                            .as_boolean_opt()
                            .is_some_and(|v| v.is_valid(0) && v.value(0));
                        return Ok(if pass {
                            HopFilter::All
                        } else {
                            HopFilter::None
                        });
                    }
                    Ok(HopFilter::Where(CompiledFilter::new(
                        &filter,
                        &HashMap::new(),
                        table,
                        state,
                    )?))
                })
                .collect::<Result<Vec<_>>>()
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
        metrics: Default::default(),
        inputs: plans,
        spec: node.spec.clone(),
        sources: node.sources.clone(),
        outputs: node.outputs.clone(),
        schema,
        properties,
        predicates,
        hop_filters,
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
        write!(
            f,
            "PathMatchExec: {} {}",
            self.spec.variable.as_deref().unwrap_or("endpoints"),
            if self.spec.csr.is_some() {
                "csr walk"
            } else {
                "edge inventory"
            }
        )
    }
}
impl ExecutionPlan for PathMatchExec {
    fn name(&self) -> &str {
        "PathMatchExec"
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
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
            metrics: Default::default(),
            inputs,
            spec: self.spec.clone(),
            sources: self.sources.clone(),
            outputs: self.outputs.clone(),
            schema: self.schema.clone(),
            properties,
            predicates: self.predicates.clone(),
            hop_filters: self.hop_filters.clone(),
            ready: self.ready.clone(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (input, input_time) =
            crate::exec_metrics::time_input(self.inputs[0].execute(partition, context.clone())?);
        let (cell, plans, spec, outputs, schema, predicates, hop_filters) = (
            self.ready.clone(),
            self.inputs[1..].to_vec(),
            self.spec.clone(),
            self.outputs.clone(),
            self.schema.clone(),
            self.predicates.clone(),
            self.hop_filters.clone(),
        );
        let sources = self.sources.clone();
        let reads_edges = spec.variable.is_some()
            || outputs.iter().any(|o| {
                matches!(
                    o,
                    Output::RelationshipValue { .. } | Output::Relationship { .. }
                )
            });
        let out_schema = schema.clone();
        let batch_size = context.session_config().batch_size();
        let stream = futures::stream::try_unfold(
            (input, None::<Pending>),
            move |(mut input, mut pending)| {
                let (cell, plans, spec, outputs, schema, predicates, hop_filters, context) = (
                    cell.clone(),
                    plans.clone(),
                    spec.clone(),
                    outputs.clone(),
                    schema.clone(),
                    predicates.clone(),
                    hop_filters.clone(),
                    context.clone(),
                );
                let sources = sources.clone();
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
                                        Ready::build(
                                            &spec,
                                            &plans,
                                            &predicates,
                                            &hop_filters,
                                            context.clone(),
                                            &sources,
                                        )
                                        .await?,
                                    ));
                                }
                                ready.as_ref().expect("built").clone()
                            };
                            pending = Some(Pending::new(batch, graph, &spec)?);
                        }
                        let p = pending.as_mut().unwrap();
                        let (rows, mut paths) = p.next(&spec, batch_size).await?;
                        if rows.is_empty() {
                            pending = None;
                            continue;
                        }
                        let edges = match &spec.csr {
                            Some(_) if reads_edges => p.csr_edges(&spec, &mut paths).await?,
                            // Nothing reads relationship records: don't fetch them.
                            Some(_) => StructArray::new_empty_fields(0, None),
                            None => p.graph.inventory.edges.clone(),
                        };
                        let indices = UInt32Array::from(rows);
                        let mut columns: Vec<ArrayRef> = p
                            .batch
                            .columns()
                            .iter()
                            .map(|c| take(c, &indices, None))
                            .collect::<std::result::Result<_, _>>()?;
                        for output in &outputs {
                            let (array, positions) = match output {
                                Output::NodeValue { boundary } => {
                                    let positions = UInt32Array::from_iter_values(
                                        paths.iter().map(|p| p.nodes[p.boundaries[*boundary]]),
                                    );
                                    columns.push(take(&p.graph.inventory.nodes, &positions, None)?);
                                    continue;
                                }
                                Output::RelationshipValue { step } => {
                                    let positions = UInt32Array::from_iter_values(
                                        paths.iter().map(|p| p.edges[p.boundaries[*step]]),
                                    );
                                    columns.push(take(&edges, &positions, None)?);
                                    continue;
                                }
                                Output::Node { boundary, property } => (
                                    p.graph.inventory.node_properties.column(*property),
                                    UInt32Array::from_iter_values(
                                        paths.iter().map(|p| p.nodes[p.boundaries[*boundary]]),
                                    ),
                                ),
                                Output::Relationship { step, property } => (
                                    edges.column(0).as_struct().column(*property),
                                    UInt32Array::from_iter_values(
                                        paths.iter().map(|p| p.edges[p.boundaries[*step]]),
                                    ),
                                ),
                            };
                            columns.push(take(array, &positions, None)?);
                        }
                        if spec.variable.is_some() {
                            if spec.reversed {
                                for path in &mut paths {
                                    path.nodes.reverse();
                                    path.edges.reverse();
                                }
                            }
                            columns.push(paths_array(&paths, &p.graph.inventory.nodes, &edges)?);
                        }
                        let batch = RecordBatch::try_new(schema, columns)?;
                        return Ok(Some((batch, (input, pending))));
                    }
                }
            },
        );
        let output: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(out_schema, stream));
        Ok(crate::exec_metrics::measure(
            output,
            &self.metrics,
            partition,
            input_time,
        ))
    }
}

impl Ready {
    async fn build(
        spec: &PathSpec,
        plans: &[Arc<dyn ExecutionPlan>],
        predicates: &[Option<Arc<dyn PhysicalExpr>>],
        hop_filters: &[Vec<HopFilter>],
        context: Arc<TaskContext>,
        sources: &SourceBatches,
    ) -> Result<Self> {
        // The CSR route reads no relationship table up front: its plans scan only nodes.
        let edges = if spec.csr.is_some() {
            &[][..]
        } else {
            &spec.edges
        };
        let inventory = Inventory::build(&spec.nodes, edges, plans, context, sources).await?;
        let edge_fields = property_fields(spec.edges.iter().map(|s| s.schema.fields()))?;
        let node_type = inventory.nodes.data_type().clone();
        let edge_type = entity_type(edge_fields.clone(), "relationship", Some(&node_type));
        // Hops of one step over the same table share a state: the two halves of an
        // undirected step then count their checks together and read one mask.
        let hops = match &spec.csr {
            Some(csr) => csr
                .steps
                .iter()
                .zip(hop_filters)
                .map(|(step, filters)| {
                    let mut states: HashMap<usize, Arc<EdgeFilterState>> = HashMap::new();
                    step.iter()
                        .zip(filters)
                        .map(|(hop, f)| (f.clone(), states.entry(hop.kind).or_default().clone()))
                        .collect()
                })
                .collect(),
            None => Vec::new(),
        };
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
            .filter(|_| spec.csr.is_none())
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
        let node_keys = inventory
            .nodes
            .column(1)
            .as_struct()
            .column(1)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .clone();
        Ok(Self {
            inventory,
            nodes_allowed,
            edges_allowed,
            shortest,
            hops,
            edge_type,
            edge_fields,
            node_keys,
        })
    }
}

/// An edge a CSR walk can enter: its type, CSR row and inventory endpoints.
#[derive(Clone, Copy)]
struct CsrEdge {
    kind: usize,
    row: u64,
    source: u32,
    target: u32,
}

/// Numbers the CSR edges a batch's walks enter, so a trail can check reuse and paths
/// can refer to edges as the inventory route does. Also keeps each (step, node)'s
/// candidates: trails reach the same node many times, and the list never changes.
#[derive(Default)]
struct CsrEdges {
    ids: HashMap<(usize, u64), u32, ahash::RandomState>,
    edges: Vec<CsrEdge>,
    candidates: HashMap<(usize, u32), Candidates, ahash::RandomState>,
    /// Relationship records built for returned paths, kept for the batch's later output.
    records: Vec<ArrayRef>,
    /// Per edge id, its record's (part, offset) in `records`.
    record: HashMap<u32, (usize, usize), ahash::RandomState>,
}

/// (next node, edge id) pairs a frame can take.
type Candidates = Arc<[(u32, u32)]>;

impl CsrEdges {
    fn id(&mut self, edge: CsrEdge) -> u32 {
        *self.ids.entry((edge.kind, edge.row)).or_insert_with(|| {
            self.edges.push(edge);
            (self.edges.len() - 1) as u32
        })
    }
}

/// The edges a step can take from `node`, through the engine's CSR, that pass the step's
/// filter and reach a node of the inventory, read once per batch. Filters read only these
/// edges' columns.
async fn csr_candidates(
    spec: &PathSpec,
    graph: &Ready,
    edges: &mut CsrEdges,
    stage: usize,
    node: u32,
) -> Result<Candidates> {
    if let Some(candidates) = edges.candidates.get(&(stage, node)) {
        return Ok(candidates.clone());
    }
    let csr = spec.csr.as_ref().expect("csr route");
    let key = graph.node_keys.value(node as usize);
    let label = graph.inventory.node_labels[node as usize];
    let mut candidates = Vec::new();
    for (hop, (filter, state)) in csr.steps[stage].iter().zip(&graph.hops[stage]) {
        if hop.from != label || matches!(filter, HopFilter::None) {
            continue;
        }
        let first = candidates.len();
        let mut position = Default::default();
        for (neighbor, row) in hop
            .neighbors
            .candidates_from::<true>(key, &mut position)
            .flatten()
        {
            let neighbor = neighbor as i64;
            if hop.skip_self_loops && neighbor == key {
                continue;
            }
            let Some(&next) = graph.inventory.by_key.get(&(hop.to, neighbor)) else {
                continue;
            };
            let (source, target) = if hop.reversed {
                (next, node)
            } else {
                (node, next)
            };
            candidates.push((
                next,
                CsrEdge {
                    kind: hop.kind,
                    row,
                    source,
                    target,
                },
            ));
        }
        if let HopFilter::Where(filter) = filter {
            let rows = spec.rows(hop.kind);
            let edge_rows: Vec<u64> = candidates[first..].iter().map(|(_, e)| e.row).collect();
            let keep = state
                .keep(rows, &edge_rows, filter, mask_after(rows, &hop.neighbors))
                .await
                .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
            let mut keep = keep.into_iter();
            let mut index = 0;
            candidates.retain(|_| {
                index += 1;
                index <= first || keep.next().unwrap_or(false)
            });
        }
    }
    let candidates: Candidates = candidates
        .into_iter()
        .map(|(next, edge)| (next, edges.id(edge)))
        .collect();
    edges.candidates.insert((stage, node), candidates.clone());
    Ok(candidates)
}

struct Pending {
    batch: RecordBatch,
    graph: Arc<Ready>,
    keys: Vec<Option<Int64Array>>,
    row: usize,
    walk: Option<Walk>,
    shortest: Option<ShortestPaths>,
    /// CSR route only: the edges this batch's walks entered.
    edges: CsrEdges,
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
            edges: CsrEdges::default(),
        })
    }
    /// Relationship values for the edges of `paths`, read from their tables now. Renumbers
    /// each path's edges to their positions in the returned array.
    async fn csr_edges(&mut self, spec: &PathSpec, paths: &mut [PathRows]) -> Result<StructArray> {
        let used: Vec<u32> = paths
            .iter_mut()
            .flat_map(|p| p.edges.iter_mut())
            .enumerate()
            .map(|(position, edge)| std::mem::replace(edge, position as u32))
            .collect();
        // Paths share edges: read and build each edge once per input batch, by type, then
        // pick every path's records from those parts.
        let mut distinct: Vec<u32> = used
            .iter()
            .copied()
            .filter(|id| !self.edges.record.contains_key(id))
            .collect();
        distinct.sort_unstable_by_key(|&id| (self.edges.edges[id as usize].kind, id));
        distinct.dedup();
        let error = |e: crate::error::GraphError| {
            datafusion::common::DataFusionError::External(Box::new(e))
        };
        for kind in 0..spec.edges.len() {
            let ids: Vec<u32> = distinct
                .iter()
                .copied()
                .filter(|&id| self.edges.edges[id as usize].kind == kind)
                .collect();
            let edges: Vec<CsrEdge> = ids
                .iter()
                .map(|&id| self.edges.edges[id as usize])
                .collect();
            if edges.is_empty() {
                continue;
            }
            let rows = spec.rows(kind);
            let edge_rows: Vec<u64> = edges.iter().map(|e| e.row).collect();
            let columns: Vec<usize> = (0..rows.schema().fields().len()).collect();
            let batch = RecordBatch::try_new(
                rows.schema().clone(),
                rows.take(&edge_rows, &columns).await.map_err(error)?,
            )?;
            let ends =
                |end: fn(&CsrEdge) -> u32| UInt32Array::from_iter_values(edges.iter().map(end));
            let identity = relationship_identity(
                vec![spec.edges[kind].label.as_str(); edges.len()],
                edge_rows.iter().map(|&r| r as i64).collect(),
            );
            let properties = properties(&batch, &self.graph.edge_fields)?;
            let part = self.edges.records.len();
            self.edges.record.extend(
                ids.iter()
                    .enumerate()
                    .map(|(offset, &id)| (id, (part, offset))),
            );
            self.edges
                .records
                .push(Arc::new(crate::path_value::struct_array(
                    &self.graph.edge_type,
                    vec![
                        Arc::new(StructArray::from(properties)),
                        Arc::new(identity),
                        take(&self.graph.inventory.nodes, &ends(|e| e.source), None)?,
                        take(&self.graph.inventory.nodes, &ends(|e| e.target), None)?,
                    ],
                )));
        }
        if used.is_empty() {
            return Ok(arrow::array::new_empty_array(&self.graph.edge_type)
                .as_struct()
                .clone());
        }
        let parts: Vec<&dyn Array> = self.edges.records.iter().map(|a| a.as_ref()).collect();
        let positions: Vec<(usize, usize)> = used.iter().map(|id| self.edges.record[id]).collect();
        Ok(arrow::compute::interleave(&parts, &positions)?
            .as_struct()
            .clone())
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
                self.walk
                    .as_mut()
                    .unwrap()
                    .next(spec, &self.graph, &mut self.edges)
                    .await?
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
    /// CSR route only: the edges this frame can take, read when it first needs one.
    candidates: Option<Candidates>,
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
                candidates: None,
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
    async fn next(
        &mut self,
        spec: &PathSpec,
        graph: &Ready,
        edges: &mut CsrEdges,
    ) -> Result<Option<PathRows>> {
        let mut work = 0_usize;
        while let Some(frame) = self.frames.last_mut() {
            work += 1;
            if work.is_multiple_of(1024) {
                cooperative_yield().await;
            }
            if frame.stage == spec.steps.len() {
                if !frame.advanced {
                    frame.advanced = true;
                    return Ok(Some(self.path.clone()));
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
                        candidates: None,
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
            let (edge, node) = if spec.csr.is_some() {
                if frame.candidates.is_none() {
                    frame.candidates =
                        Some(csr_candidates(spec, graph, edges, frame.stage, frame.node).await?);
                }
                let candidates = frame.candidates.as_ref().expect("fetched");
                let Some(&(node, edge)) = candidates.get(frame.next) else {
                    self.pop();
                    continue;
                };
                frame.next += 1;
                if self.path.edges.contains(&edge) {
                    continue;
                }
                (edge, node)
            } else {
                let Some(edge) = graph.inventory.neighbors[frame.node as usize].get(frame.next)
                else {
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
                (edge.edge, edge.node)
            };
            let next = Frame {
                stage: frame.stage,
                hops: frame.hops + 1,
                node,
                next: 0,
                advanced: false,
                entered: true,
                candidates: None,
            };
            self.path.nodes.push(node);
            self.path.edges.push(edge);
            self.frames.push(next);
        }
        Ok(None)
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
        };
        let spec = PathSpec {
            variable: Some("p".into()),
            node_values: false,
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
                rows: None,
            }],
            bindings: vec![binding("a"), binding("b")],
            steps: vec![Step {
                types: vec![0],
                direction: RelationshipDirection::Outgoing,
                min: 0,
                max,
                relationship: None,
            }],
            shortest,
            csr: None,
            reversed: false,
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
        let ready = Ready::build(
            &spec,
            &plans,
            &[None, None, None],
            &[],
            ctx.task_ctx(),
            &Default::default(),
        )
        .await
        .unwrap();
        (spec, Arc::new(ready))
    }

    #[tokio::test]
    async fn named_walk_keeps_edge_occurrences_and_never_reuses_one() {
        let (spec, graph) = graph(&[(1, 2), (1, 2), (2, 3), (3, 3)], 20, None).await;
        let mut walk = Walk::new(0, vec![Some(0), None]);
        let mut paths = Vec::new();
        while let Some(path) = walk
            .next(&spec, &graph, &mut Default::default())
            .await
            .unwrap()
        {
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
        assert!(walk
            .next(&spec, &graph, &mut Default::default())
            .await
            .unwrap()
            .is_some()); // zero hop
        let mut edges = CsrEdges::default();
        let mut next = Box::pin(walk.next(&spec, &graph, &mut edges));
        assert!(futures::poll!(next.as_mut()).is_pending());
        drop(next);
        assert!(walk.frames[0].next > 0 && walk.frames[0].next <= 1024);
    }
}
