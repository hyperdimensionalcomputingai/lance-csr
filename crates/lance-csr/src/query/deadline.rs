// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! Cooperative query deadlines: checkpoints in join work that stop a query once its deadline passes.

use std::future::Future;
use std::sync::Arc;
use tokio::time::Instant;

use arrow_schema::{DataType, Field, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{create_udf, ColumnarValue, Operator, Volatility};
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::joins::{
    CrossJoinExec, NestedLoopJoinExec, NestedLoopJoinExecBuilder,
};
use datafusion::physical_plan::ExecutionPlan;

tokio::task_local! {
    static EXECUTION_DEADLINE: Option<Instant>;
}

/// Run `future` with a cooperative query deadline.
///
/// Queries planned inside `future` (for example [`CypherEngine::execute`] or
/// [`CypherEngine::execute_stream`]) get deadline checks in their joins: nested-loop
/// joins (joins with non-equality conditions) check `deadline` while evaluating
/// candidates and fail with a `"Query timeout exceeded"` execution error, even when no
/// rows match; cross joins yield to the runtime between batches. `None` sets no
/// deadline. The deadline is never stored in the engine; it applies only to work inside
/// `future`.
///
/// Yielding only lets a timer stop the work, and other operators are not checked, so
/// pair this with [`tokio::time::timeout_at`] on the same deadline:
///
/// ```no_run
/// # use std::collections::HashMap;
/// # async fn example(engine: &lance_csr::CypherEngine) -> lance_csr::Result<()> {
/// use std::time::Duration;
/// use lance_csr::query::with_execution_deadline;
/// use tokio::time::{timeout_at, Instant};
///
/// let deadline = Instant::now() + Duration::from_secs(2);
/// let text = "MATCH (a), (b) WHERE a.id < b.id RETURN count(*) AS n";
/// let query = engine.execute(text, HashMap::new());
/// match timeout_at(deadline, with_execution_deadline(Some(deadline), query)).await {
///     Ok(result) => println!("{:?}", result?),
///     Err(_) => println!("timed out"),
/// }
/// # Ok(())
/// # }
/// ```
///
/// [`CypherEngine::execute`]: crate::CypherEngine::execute
/// [`CypherEngine::execute_stream`]: crate::CypherEngine::execute_stream
pub async fn with_execution_deadline<F: Future>(deadline: Option<Instant>, future: F) -> F::Output {
    EXECUTION_DEADLINE.scope(deadline, future).await
}

pub(super) fn context(ctx: SessionContext) -> SessionContext {
    let Some(deadline) = EXECUTION_DEADLINE
        .try_with(|deadline| *deadline)
        .ok()
        .flatten()
    else {
        return ctx;
    };
    SessionContext::new_with_state(
        SessionStateBuilder::new_from_existing(ctx.state())
            .with_physical_optimizer_rule(Arc::new(DeadlineJoins { deadline }))
            .build(),
    )
}

#[derive(Debug)]
struct DeadlineJoins {
    deadline: Instant,
}

impl PhysicalOptimizerRule for DeadlineJoins {
    fn name(&self) -> &str {
        "query_deadline_joins"
    }

    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let deadline = self.deadline;
        let function = create_udf(
            "__query_deadline",
            vec![],
            DataType::Boolean,
            Volatility::Volatile,
            Arc::new(move |_| {
                if Instant::now() >= deadline {
                    return Err(DataFusionError::Execution("Query timeout exceeded".into()));
                }
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
            }),
        );
        let guard: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
            "__query_deadline",
            Arc::new(function),
            vec![],
            Arc::new(Field::new("__query_deadline", DataType::Boolean, false)),
            Arc::new(config.clone()),
        ));
        plan.transform_up(|plan| {
            if let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>() {
                // Output-only yielding cannot interrupt long runs of rejected join candidates.
                let filter = match join.filter() {
                    Some(filter) => JoinFilter::new(
                        Arc::new(BinaryExpr::new(
                            guard.clone(),
                            Operator::And,
                            filter.expression().clone(),
                        )),
                        filter.column_indices().to_vec(),
                        filter.schema().clone(),
                    ),
                    None => JoinFilter::new(guard.clone(), vec![], Arc::new(Schema::empty())),
                };
                let joined = NestedLoopJoinExecBuilder::new(
                    join.left().clone(),
                    join.right().clone(),
                    *join.join_type(),
                )
                .with_filter(Some(filter))
                .with_projection_ref(join.projection().clone())
                .build()?;
                Ok(Transformed::yes(
                    Arc::new(CooperativeExec::new(Arc::new(joined))) as Arc<dyn ExecutionPlan>,
                ))
            } else if plan.is::<CrossJoinExec>() {
                Ok(Transformed::yes(
                    Arc::new(CooperativeExec::new(plan)) as Arc<dyn ExecutionPlan>
                ))
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .map(|plan| plan.data)
    }
}
