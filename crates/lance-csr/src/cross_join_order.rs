//! Build a cross join on the input with fewer rows.
//!
//! DataFusion's join selection orders a cross join by `total_byte_size` when both inputs report
//! one, and Lance scans do since lance 14. But `CrossJoinExec` emits one output batch per build
//! row, so a narrow table with many rows on the build side, against a wide single row, turns one
//! batch into thousands of one-row batches for every operator above the join. IC3 slowed 14% in
//! lazy mode that way. This rule runs right after join selection and swaps such joins back.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::Result;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::CrossJoinExec;
use datafusion::physical_plan::ExecutionPlan;

/// Swap a cross join's inputs when its build (left) side has more rows than its probe side.
#[derive(Debug)]
struct CrossJoinBuildRows;

impl PhysicalOptimizerRule for CrossJoinBuildRows {
    fn name(&self) -> &str {
        "cross_join_build_rows"
    }

    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let Some(join) = plan.downcast_ref::<CrossJoinExec>() else {
                return Ok(Transformed::no(plan));
            };
            let rows = |side: &Arc<dyn ExecutionPlan>| {
                Ok::<_, datafusion::error::DataFusionError>(
                    side.partition_statistics(None)?
                        .num_rows
                        .get_value()
                        .copied(),
                )
            };
            match (rows(join.left())?, rows(join.right())?) {
                (Some(left), Some(right)) if right < left => {
                    Ok(Transformed::yes(join.swap_inputs()?))
                }
                _ => Ok(Transformed::no(plan)),
            }
        })
        .data()
    }
}

/// `state` with [`CrossJoinBuildRows`] right after DataFusion's join selection, so distribution
/// enforcement still sees the final join order.
pub(crate) fn install(state: SessionState) -> SessionState {
    let mut rules = state.physical_optimizers().to_vec();
    let at = rules
        .iter()
        .position(|rule| rule.name() == "join_selection")
        .map_or(rules.len(), |i| i + 1);
    rules.insert(at, Arc::new(CrossJoinBuildRows));
    SessionStateBuilder::from(state)
        .with_physical_optimizer_rules(rules)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::{collect, displayable};

    /// A narrow 100-row table against a wide one-row table: DataFusion builds on the narrow one
    /// (fewer bytes), the rule moves the build to the single row, and the result is unchanged.
    #[tokio::test]
    async fn builds_on_the_side_with_fewer_rows() {
        let state = |rule: bool| {
            let state = SessionContext::new().state();
            SessionContext::new_with_state(if rule { install(state) } else { state })
        };
        let many = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let many = RecordBatch::try_new(
            many.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..100))],
        )
        .unwrap();
        let one = Arc::new(Schema::new(vec![Field::new("note", DataType::Utf8, false)]));
        let one = RecordBatch::try_new(
            one.clone(),
            vec![Arc::new(StringArray::from(vec!["x".repeat(100_000)]))],
        )
        .unwrap();
        let mut outputs = vec![];
        for rule in [false, true] {
            let ctx = state(rule);
            for (name, batch) in [("many", &many), ("one", &one)] {
                let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
                ctx.register_table(name, Arc::new(table)).unwrap();
            }
            let df = ctx
                .sql("SELECT id, length(note) AS n FROM one CROSS JOIN many")
                .await
                .unwrap();
            let plan = df.create_physical_plan().await.unwrap();
            let shown = displayable(plan.as_ref()).indent(true).to_string();
            let batches = collect(plan, ctx.task_ctx()).await.unwrap();
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 100, "{shown}");
            outputs.push((batches.len(), shown));
        }
        // Without the rule the build side is the 100 rows: one output batch per row.
        assert!(outputs[0].0 >= 100, "{}", outputs[0].1);
        assert!(outputs[1].0 < 100, "{}", outputs[1].1);
    }
}
