// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! List comprehensions: `[x IN list WHERE keep | map]`, per row.

use std::collections::HashSet;

use arrow_schema::DataType;
use datafusion::common::Column;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions_aggregate::array_agg::array_agg;
use datafusion::functions_aggregate::count::count;
use datafusion::functions_nested::expr_fn::{array_element, array_length, gen_series, make_array};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{
    col, lit, Cast, Expr, ExprFunctionExt, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder,
};

use super::super::analysis::PlanningContext;
use crate::ast::{BooleanExpression, ValueExpression};
use crate::case_insensitive::qualify_column;
use crate::datafusion_planner::DataFusionPlanner;
use crate::error::Result;
use crate::logical_plan::LogicalOperator;

fn int64(expr: Expr) -> Expr {
    Expr::Cast(Cast::new(Box::new(expr), DataType::Int64))
}

impl DataFusionPlanner {
    /// Number the input rows, unnest each row's list as (position, element) rows (one
    /// padding row for an empty or null list, so every input row survives), build
    /// `element` over them, then collect the kept values back per input row by position.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_list_comprehension(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
        list: &ValueExpression,
        (variable, label): (&str, Option<&str>),
        element: &LogicalOperator,
        keep: Option<&BooleanExpression>,
        map: &ValueExpression,
        output: &str,
        count_only: bool,
    ) -> Result<LogicalPlan> {
        let error = |what: &str| {
            let what = what.to_string();
            move |e| self.plan_error(&what, e)
        };
        let row = format!("{output}__row");
        let pos = format!("{output}__pos");
        let numbered = LogicalPlanBuilder::from(self.build_operator(ctx, input)?)
            .window(vec![row_number().alias(&row)])
            .map_err(error("Failed to number comprehension rows"))?
            .build()
            .map_err(error("Failed to number comprehension rows"))?;
        let list = self.value_expr(list, numbered.schema())?;
        let list = if list
            .get_type(numbered.schema())
            .map_err(error("Failed to type comprehension list"))?
            == DataType::Null
        {
            datafusion::logical_expr::cast(list, DataType::new_list(DataType::Null, true))
        } else {
            list
        };
        let length = int64(coalesce(vec![array_length(list.clone()), lit(0_u64)]));
        let last = datafusion::functions::core::expr_fn::greatest(vec![length.clone(), lit(1_i64)]);
        let positions = gen_series(lit(1_i64), last, lit(1_i64));
        let exploded = LogicalPlanBuilder::from(numbered.clone())
            .project(vec![
                datafusion::logical_expr::wildcard(),
                datafusion::logical_expr::select_expr::SelectExpr::Expression(
                    positions.alias(&pos),
                ),
            ])
            .and_then(|b| b.unnest_column(pos.as_str()))
            .map_err(error("Failed to unnest comprehension list"))?;
        let item = array_element(list, col(&pos));
        let element_rows = match label {
            None => exploded
                .project(vec![
                    datafusion::logical_expr::wildcard(),
                    datafusion::logical_expr::select_expr::SelectExpr::Expression(
                        item.alias(variable.to_lowercase()),
                    ),
                ])
                .and_then(|b| b.build())
                .map_err(error("Failed to project comprehension elements"))?,
            Some(label) => {
                // Node ids: each element joins to its node, keeping padding rows.
                let id_column = format!("{output}__id");
                let mapping = self.config.get_node_mapping(label).ok_or_else(|| {
                    crate::error::GraphError::PlanError {
                        message: format!("No node mapping for '{label}'"),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    }
                })?;
                let nodes = self.build_scan(ctx, variable, label, &Default::default())?;
                let joined = exploded
                    .project(vec![
                        datafusion::logical_expr::wildcard(),
                        datafusion::logical_expr::select_expr::SelectExpr::Expression(
                            item.alias(&id_column),
                        ),
                    ])
                    .and_then(|b| {
                        b.join(
                            nodes,
                            JoinType::Left,
                            (
                                vec![id_column.clone()],
                                vec![qualify_column(variable, &mapping.id_field)],
                            ),
                            None,
                        )
                    })
                    .and_then(|b| b.build())
                    .map_err(error("Failed to join comprehension elements to nodes"))?;
                let kept: Vec<Expr> = joined
                    .schema()
                    .columns()
                    .into_iter()
                    .filter(|c| c.name != id_column)
                    .map(Expr::Column)
                    .collect();
                LogicalPlanBuilder::from(joined)
                    .project(kept)
                    .and_then(|b| b.build())
                    .map_err(error("Failed to project comprehension elements"))?
            }
        };

        ctx.comprehension_elements.push(element_rows);
        let elements = self.build_operator(ctx, element);
        ctx.comprehension_elements.pop();
        let elements = elements?;

        // Padding rows (position past the list's end) are never collected.
        let mut kept = col(&pos).lt_eq(length);
        if let Some(keep) = keep {
            kept = kept.and(self.boolean_expr(keep, elements.schema())?);
        }
        let aggregate = if count_only {
            // Count rows, not the mapped value: null elements still occupy a slot.
            count(lit(1_i64)).filter(kept).build()
        } else {
            let value = self.value_expr(map, elements.schema())?;
            array_agg(value)
                .filter(kept)
                .order_by(vec![col(&pos).sort(true, false)])
                .build()
        };
        let collected = aggregate
            .map_err(error("Failed to aggregate comprehension values"))?
            .alias(output);
        // One group per input row: its number plus every input column, all equal within it.
        let names: HashSet<String> = numbered
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let group: Vec<Expr> = elements
            .schema()
            .columns()
            .into_iter()
            .filter(|c| names.contains(&c.name))
            .map(Expr::Column)
            .collect();
        let mut out: Vec<Expr> = numbered
            .schema()
            .fields()
            .iter()
            .filter(|f| *f.name() != row)
            .map(|f| Expr::Column(Column::new_unqualified(f.name())))
            .collect();
        let value = if count_only {
            col(output)
        } else {
            coalesce(vec![col(output), make_array(vec![])])
        };
        out.push(value.alias(output));
        LogicalPlanBuilder::from(elements)
            .aggregate(group, vec![collected])
            .and_then(|b| b.project(out))
            .and_then(|b| b.build())
            .map_err(error("Failed to build list comprehension"))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{types::Int64Type, ArrayRef, Int64Array, ListArray, RecordBatch};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn data() -> HashMap<String, RecordBatch> {
        HashMap::from([(
            "Person".into(),
            RecordBatch::try_from_iter([
                (
                    "id",
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                ),
                (
                    "values",
                    Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                        Some(vec![Some(1), None, Some(1), Some(2)]),
                        Some(vec![]),
                        None,
                        Some(vec![None]),
                    ])) as ArrayRef,
                ),
            ])
            .unwrap(),
        )])
    }

    #[tokio::test]
    async fn comprehension_counts_keep_nulls_duplicates_and_empty_rows() {
        let config = crate::GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();
        for (predicate, expected) in [
            ("", vec![4, 0, 0, 1]),
            ("WHERE x > 1", vec![1, 0, 0, 0]),
            ("WHERE x IS NULL", vec![1, 0, 0, 1]),
            ("WHERE x IS NULL OR x > 0", vec![4, 0, 0, 1]),
        ] {
            // Duplicated input rows must remain distinct; an identity map is the
            // unoptimized reference and also retains null elements in ARRAY_AGG.
            let make_query = |map| {
                crate::CypherQuery::new(&format!(
                    "MATCH (p:Person) UNWIND [1, 1] AS duplicate RETURN p.id AS id, \
                 size([x IN p.values {predicate} {map}]) AS n ORDER BY id"
                ))
                .unwrap()
                .with_config(config.clone())
            };
            let query = make_query("");
            let actual = query.execute(data(), None).await.unwrap();
            let reference = make_query("| x").execute(data(), None).await.unwrap();
            assert_eq!(actual.columns(), reference.columns(), "{predicate}");
            assert_eq!(
                actual.schema().field(1).data_type(),
                reference.schema().field(1).data_type()
            );
            assert_eq!(actual.schema().field(0).name(), "id");
            assert_eq!(actual.schema().field(1).name(), "n");
            assert!(!actual.schema().field(1).is_nullable());
            let counts = actual
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(
                counts.values().to_vec(),
                expected
                    .into_iter()
                    .flat_map(|n| [n, n])
                    .collect::<Vec<_>>()
            );
            let plan = query.explain(data()).await.unwrap().to_lowercase();
            assert!(plan.contains("count("), "{plan}");
            assert!(!plan.contains("array_agg("), "{plan}");
            let mapped = make_query("| x")
                .explain(data())
                .await
                .unwrap()
                .to_lowercase();
            assert!(mapped.contains("array_agg("), "{mapped}");
        }
    }
}
