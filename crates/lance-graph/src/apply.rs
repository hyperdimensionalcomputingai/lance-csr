// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! Read-only CALL: one independently bound child stream per outer row.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::{DFSchemaRef, Result, ScalarValue};
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    execute_stream, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties,
};
use futures::StreamExt;

/// The child is deliberately unoptimized until its imports have concrete values.
/// It is not an ordinary input: planning it before binding would give global top-k
/// and aggregate semantics instead of evaluating them once per outer row.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ApplyNode {
    input: LogicalPlan,
    inner: LogicalPlan,
    /// (outer column name, private placeholder name).
    imports: Vec<(String, String)>,
    schema: DFSchemaRef,
}

impl ApplyNode {
    pub(crate) fn try_new(
        input: LogicalPlan,
        inner: LogicalPlan,
        imports: Vec<(String, String)>,
    ) -> Result<Self> {
        let schema = Arc::new(input.schema().join(inner.schema())?);
        Ok(Self {
            input,
            inner,
            imports,
            schema,
        })
    }
}

impl PartialOrd for ApplyNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input
            .partial_cmp(&other.input)
            .and_then(|order| match order {
                Ordering::Equal => self.inner.partial_cmp(&other.inner),
                order => Some(order),
            })
            .filter(|order| *order != Ordering::Equal || self == other)
    }
}

impl UserDefinedLogicalNodeCore for ApplyNode {
    fn name(&self) -> &str {
        "Apply"
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
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Apply: imports={:?}, child={}",
            self.imports
                .iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            self.inner.display_indent()
        )
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Self::try_new(inputs.remove(0), self.inner.clone(), self.imports.clone())
    }
}

pub(crate) struct ApplyExec {
    input: Arc<dyn ExecutionPlan>,
    inner: LogicalPlan,
    imports: Vec<(String, String)>,
    state: Arc<SessionState>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for ApplyExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ApplyExec")
            .field("imports", &self.imports)
            .finish_non_exhaustive()
    }
}

impl ApplyExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        inner: LogicalPlan,
        imports: Vec<(String, String)>,
        state: Arc<SessionState>,
        schema: SchemaRef,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Incremental,
            input.boundedness(),
        ));
        Self {
            input,
            inner,
            imports,
            state,
            schema,
            properties,
        }
    }
}

impl DisplayAs for ApplyExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ApplyExec: per-row CALL, {} imported columns",
            self.imports.len()
        )
    }
}

impl ExecutionPlan for ApplyExec {
    fn name(&self) -> &str {
        "ApplyExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children.remove(0),
            self.inner.clone(),
            self.imports.clone(),
            self.state.clone(),
            self.schema.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context.clone())?;
        let indices = self
            .imports
            .iter()
            .map(|(column, name)| Ok((input.schema().index_of(column)?, name.clone())))
            .collect::<Result<Vec<_>>>()?;
        let state = ApplyStreamState {
            input,
            batch: None,
            next_row: 0,
            iterations_since_yield: 0,
            current: vec![],
            child: None,
            inner: self.inner.clone(),
            imports: indices,
            session: self.state.clone(),
            context,
            schema: self.schema.clone(),
        };
        let stream = futures::stream::try_unfold(state, |mut state| async move {
            loop {
                // Bound all ready work, including empty outer and child batches.
                if state.iterations_since_yield == 64 {
                    crate::csr_expand::cooperative_yield().await;
                    state.iterations_since_yield = 0;
                }
                state.iterations_since_yield += 1;
                if let Some(child) = &mut state.child {
                    if let Some(batch) = child.next().await {
                        let batch = batch?;
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        let mut columns = state
                            .current
                            .iter()
                            .map(|value| value.to_array_of_size(batch.num_rows()))
                            .collect::<Result<Vec<_>>>()?;
                        columns.extend_from_slice(batch.columns());
                        let output = RecordBatch::try_new(state.schema.clone(), columns)?;
                        return Ok(Some((output, state)));
                    }
                    state.child = None;
                }
                if state
                    .batch
                    .as_ref()
                    .is_none_or(|batch| state.next_row >= batch.num_rows())
                {
                    let Some(batch) = state.input.next().await else {
                        return Ok(None);
                    };
                    state.batch = Some(batch?);
                    state.next_row = 0;
                    if state
                        .batch
                        .as_ref()
                        .is_some_and(|batch| batch.num_rows() == 0)
                    {
                        continue;
                    }
                }
                let batch = state.batch.as_ref().expect("a nonempty outer batch");
                state.current = batch
                    .columns()
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, state.next_row))
                    .collect::<Result<Vec<_>>>()?;
                state.next_row += 1;
                let parameters: HashMap<_, _> = state
                    .imports
                    .iter()
                    .map(|(index, name)| (name.clone(), state.current[*index].clone()))
                    .collect();
                let bound = state.inner.clone().with_param_values(parameters)?;
                let physical = state.session.create_physical_plan(&bound).await?;
                state.child = Some(execute_stream(physical, state.context.clone())?);
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

struct ApplyStreamState {
    input: SendableRecordBatchStream,
    batch: Option<RecordBatch>,
    next_row: usize,
    iterations_since_yield: usize,
    current: Vec<ScalarValue>,
    child: Option<SendableRecordBatchStream>,
    inner: LogicalPlan,
    imports: Vec<(usize, String)>,
    session: Arc<SessionState>,
    context: Arc<TaskContext>,
    schema: SchemaRef,
}

pub(crate) fn plan_apply(
    node: &ApplyNode,
    inputs: &[Arc<dyn ExecutionPlan>],
    state: &SessionState,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(ApplyExec::new(
        inputs[0].clone(),
        node.inner.clone(),
        node.imports.clone(),
        Arc::new(state.clone()),
        Arc::new(node.schema.as_arrow().clone()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CypherQuery, GraphConfig};
    use arrow_array::{Int64Array, StringArray};

    fn graph() -> (GraphConfig, HashMap<String, RecordBatch>) {
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table("KNOWS", "Person", "Person", "src", "dst")
            .build()
            .unwrap();
        let people = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])) as arrow_array::ArrayRef,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec!["A", "B", "C", "D"])),
            ),
            ("age", Arc::new(Int64Array::from(vec![20, 30, 40, 50]))),
        ])
        .unwrap();
        let edges = RecordBatch::try_from_iter(vec![
            (
                "src",
                Arc::new(Int64Array::from(vec![10, 10, 10, 20])) as arrow_array::ArrayRef,
            ),
            ("dst", Arc::new(Int64Array::from(vec![20, 20, 30, 40]))),
        ])
        .unwrap();
        (
            config,
            HashMap::from([("Person".into(), people), ("KNOWS".into(), edges)]),
        )
    }

    async fn run(text: &str) -> crate::error::Result<RecordBatch> {
        let (config, data) = graph();
        CypherQuery::new(text)?
            .with_config(config)
            .execute(data, None)
            .await
    }

    fn ints(batch: &RecordBatch, name: &str) -> Vec<i64> {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[tokio::test]
    async fn call_empty_memory_batches_yield_before_end_of_stream() {
        use datafusion::datasource::{MemTable, TableProvider};
        use datafusion::execution::context::SessionContext;
        use datafusion::logical_expr::{lit, LogicalPlanBuilder};
        use futures::{Future, TryStreamExt};
        use std::task::{Context, Poll};

        let batch = RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(Int64Array::from(vec![7])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        let batches = vec![RecordBatch::new_empty(batch.schema()); 65];
        let table = MemTable::try_new(batch.schema(), vec![batches]).unwrap();
        let state = SessionContext::new().state();
        let input = table.scan(&state, None, &[], None).await.unwrap();
        let inner = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1_i64).alias("n")])
            .unwrap()
            .build()
            .unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            batch.schema().field(0).clone(),
            inner.schema().as_arrow().field(0).clone(),
        ]));
        let plan = ApplyExec::new(input, inner, vec![], Arc::new(state.clone()), schema);
        let mut stream = plan.execute(0, state.task_ctx()).unwrap();
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        // Isolate Apply's own bound from Tokio's task-wide scheduling budget.
        let mut next = Box::pin(tokio::task::unconstrained(stream.next()));
        assert!(matches!(next.as_mut().poll(&mut cx), Poll::Pending));
        drop(next);
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn call_per_row_top_k_and_entity_export() {
        let result = run("MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) RETURN f AS friend ORDER BY friend.age DESC LIMIT 1 } RETURN p.id AS id, friend.id AS friend ORDER BY id").await.unwrap();
        assert_eq!(ints(&result, "id"), [10, 20]);
        assert_eq!(ints(&result, "friend"), [30, 40]);
    }

    #[tokio::test]
    async fn call_aggregate_keeps_empty_child_and_outer_multiplicity() {
        let result = run("MATCH (p:Person) UNWIND [1, 1] AS duplicate CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) RETURN count(f) AS n } RETURN p.id AS id, n ORDER BY id").await.unwrap();
        assert_eq!(ints(&result, "id"), [10, 10, 20, 20, 30, 30, 40, 40]);
        assert_eq!(ints(&result, "n"), [3, 3, 1, 1, 0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn call_nested_and_sibling_imports_have_separate_bindings() {
        let (config, data) = graph();
        let result = CypherQuery::new("UNWIND [1, 2] AS x CALL { WITH x CALL { WITH x RETURN x + $__call_1_0 AS y } RETURN y + 1 AS z } CALL { WITH z RETURN z * 2 AS answer } RETURN x, answer ORDER BY x")
            .unwrap().with_parameter("__call_1_0", 10).with_config(config).execute(data, None).await.unwrap();
        assert_eq!(ints(&result, "x"), [1, 2]);
        assert_eq!(ints(&result, "answer"), [24, 26]);
    }

    #[tokio::test]
    async fn call_empty_outer_never_evaluates_child() {
        let result =
            run("MATCH (p:Person) WHERE p.id < 0 CALL { RETURN 1 / 0 AS invalid } RETURN invalid")
                .await
                .unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[tokio::test]
    async fn call_inner_names_are_lexical_and_exports_explicit() {
        let result = run("UNWIND [1, 2] AS x CALL { UNWIND [10, 20] AS x RETURN x AS y } RETURN x, y ORDER BY x, y").await.unwrap();
        assert_eq!(ints(&result, "x"), [1, 1, 2, 2]);
        assert_eq!(ints(&result, "y"), [10, 20, 10, 20]);
        for (query, message) in [
            (
                "UNWIND [1] AS x CALL { RETURN x AS y } RETURN y",
                "Undefined variable",
            ),
            (
                "UNWIND [1] AS x CALL { RETURN 2 AS x } RETURN x",
                "collides",
            ),
            ("CALL { RETURN 1 + 2 } RETURN 1", "need an alias"),
            (
                "CALL { WITH missing RETURN missing AS x } RETURN x",
                "undefined variable",
            ),
            (
                "UNWIND [1] AS x CALL { WITH x + 1 AS y RETURN y } RETURN y",
                "importing WITH",
            ),
            (
                "CALL { UNWIND [1] AS local RETURN 2 AS y } RETURN local",
                "Undefined variable",
            ),
        ] {
            let error = run(query).await.unwrap_err().to_string();
            assert!(error.contains(message), "{query}: {error}");
        }
    }

    #[tokio::test]
    async fn call_union_branches_bind_independently_and_keep_outer_duplicates() {
        let result = run("UNWIND [1, 1, 2] AS x CALL { WITH x RETURN x AS n UNION WITH x RETURN x AS n } RETURN x, n ORDER BY x").await.unwrap();
        assert_eq!(ints(&result, "x"), [1, 1, 2]);
        assert_eq!(ints(&result, "n"), [1, 1, 2]);
        let result = run("WITH 2 AS x, 3 AS y CALL { WITH x RETURN x AS n UNION ALL WITH y RETURN y AS n } RETURN n ORDER BY n").await.unwrap();
        assert_eq!(ints(&result, "n"), [2, 3]);
        let error = run("WITH 2 AS x CALL { WITH x RETURN x AS n UNION RETURN x AS n } RETURN n")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("Undefined variable"), "{error}");
    }

    #[tokio::test]
    async fn call_union_node_exports_and_empty_aggregate_branches() {
        let result = run("MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) RETURN f AS friend UNION WITH p MATCH (p)-[:KNOWS]->(f:Person) RETURN f AS friend } RETURN p.id AS id, friend.id AS friend ORDER BY id, friend").await.unwrap();
        assert_eq!(ints(&result, "id"), [10, 10, 20]);
        assert_eq!(ints(&result, "friend"), [20, 30, 40]);
        let result = run("MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) RETURN count(f) AS n UNION ALL WITH p MATCH (p)-[:KNOWS]->(f:Person) WHERE f.id < 0 RETURN count(f) AS n } RETURN p.id AS id, n ORDER BY id, n").await.unwrap();
        assert_eq!(ints(&result, "n"), [0, 3, 0, 1, 0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn call_union_uses_coerced_schema_for_numbers_and_nulls() {
        let batch = run("CALL { RETURN 1 AS n UNION ALL RETURN 2.5 AS n } RETURN n ORDER BY n")
            .await
            .unwrap();
        assert_eq!(
            batch.schema().field(0).data_type(),
            &arrow_schema::DataType::Float64
        );
        for text in [
            "RETURN null AS n UNION ALL RETURN 1 AS n",
            "RETURN 1 AS n UNION ALL RETURN null AS n",
            "CALL { RETURN null AS n UNION ALL RETURN 1 AS n } RETURN n",
        ] {
            let batch = run(text).await.unwrap();
            assert_eq!(
                batch.schema().field(0).data_type(),
                &arrow_schema::DataType::Int64
            );
            assert!(batch.schema().field(0).is_nullable());
            assert_eq!(batch.column(0).null_count(), 1);
        }
    }

    #[test]
    fn call_bypasses_snapshot_templates_in_every_reading_stage() {
        let (config, _) = graph();
        let catalog = Arc::new(lance_graph_catalog::InMemoryCatalog::new());
        for text in [
            "CALL { RETURN 1 AS n } RETURN n",
            "WITH 1 AS x CALL { WITH x RETURN x AS n } RETURN n",
            "WITH 1 AS x WITH x AS y CALL { WITH y RETURN y AS n } RETURN n",
            "RETURN 1 AS n UNION CALL { RETURN 2 AS n } RETURN n",
            "RETURN 1 AS n UNION RETURN 2 AS n LIMIT $take",
            "MATCH path = (p:Person)-[:KNOWS]->(f:Person) RETURN path",
            "MATCH path = shortestPath((p:Person)-[:KNOWS*..3]->(f:Person)) RETURN relationships(path)",
        ] {
            let mut query = CypherQuery::new(text).unwrap().with_config(config.clone());
            assert!(!query.prepare_cached_plan(catalog.clone()).unwrap());
        }
    }

    #[test]
    fn call_parser_requires_returning_read_only_body() {
        for query in [
            "CALL db.labels() RETURN 1",
            "CALL { CREATE (n:Person) RETURN n } RETURN n",
            "CALL { MATCH (n:Person) } RETURN n",
            "CALL { RETURN 1 AS x } YIELD x RETURN x",
        ] {
            assert!(CypherQuery::new(query).is_err(), "{query}");
        }
    }
}
