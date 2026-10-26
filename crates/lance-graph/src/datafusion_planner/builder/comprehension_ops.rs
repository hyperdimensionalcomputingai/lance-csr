// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! List comprehensions: `[x IN list WHERE keep | map]`, per row.

use std::collections::HashSet;

use arrow_schema::DataType;
use datafusion::common::Column;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions_aggregate::array_agg::array_agg;
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
        let value = self.value_expr(map, elements.schema())?;
        let collected = array_agg(value)
            .filter(kept)
            .order_by(vec![col(&pos).sort(true, false)])
            .build()
            .map_err(error("Failed to collect comprehension values"))?
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
        out.push(coalesce(vec![col(output), make_array(vec![])]).alias(output));
        LogicalPlanBuilder::from(elements)
            .aggregate(group, vec![collected])
            .and_then(|b| b.project(out))
            .and_then(|b| b.build())
            .map_err(error("Failed to build list comprehension"))
    }
}
