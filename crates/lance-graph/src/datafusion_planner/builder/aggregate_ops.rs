// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Aggregation operations: Projection with aggregates and grouping

use crate::ast::ValueExpression;
use crate::datafusion_planner::DataFusionPlanner;
use crate::error::Result;
use crate::logical_plan::*;
use datafusion::logical_expr::{col, LogicalPlan, LogicalPlanBuilder};

/// Replace each aggregate inside `expr` with a reference to a new column `__agg_<n>`,
/// pushing the aggregate to `aggregates`, so `head(collect(x))` or `count(x) + 1` aggregate
/// first and apply the outer expression after. Cypher's `collect` gives `[]`, not null,
/// when nothing was collected.
fn split_aggregates(expr: &mut ValueExpression, aggregates: &mut Vec<(String, ValueExpression)>) {
    crate::ast::transform_value(expr, &mut |expr| {
        if let ValueExpression::AggregateFunction { name, .. } = expr {
            let column = format!("__agg_{}", aggregates.len());
            let is_collect = name.eq_ignore_ascii_case("collect");
            let aggregate = std::mem::replace(expr, ValueExpression::Variable(column.clone()));
            aggregates.push((column.clone(), aggregate));
            if is_collect {
                *expr = ValueExpression::ScalarFunction {
                    name: "coalesce".to_string(),
                    args: vec![
                        ValueExpression::Variable(column),
                        ValueExpression::ScalarFunction {
                            name: "__list".to_string(),
                            args: vec![],
                        },
                    ],
                };
            }
        }
    });
}

impl DataFusionPlanner {
    pub(crate) fn build_project_with_aggregates(
        &self,
        input_plan: LogicalPlan,
        projections: &[ProjectionItem],
        ordering: &[datafusion::logical_expr::SortExpr],
    ) -> Result<LogicalPlan> {
        // Separate group expressions (non-aggregates) from aggregate expressions
        let mut group_exprs = Vec::new();
        let mut agg_exprs = Vec::new();
        // Each aggregating item, with its aggregates replaced by `__agg_<n>` columns.
        let mut outer = Vec::new();
        let mut aggregates = Vec::new();

        let schema = input_plan.schema().clone();
        let bound: Vec<ProjectionItem> = projections
            .iter()
            .map(|p| ProjectionItem {
                expression: super::super::expression::bind_value(&p.expression, &schema),
                alias: p.alias.clone(),
            })
            .collect();
        let projections = bound.as_slice();
        for p in projections {
            // Grouping by a node groups by all its columns (`WITH b, count(*) AS n`).
            if let Some(columns) = Self::entity_columns(&schema, &p.expression, None) {
                group_exprs.extend(columns.into_iter().map(|c| match c {
                    datafusion::logical_expr::Expr::Alias(a) => *a.expr,
                    other => other,
                }));
                continue;
            }
            if super::super::expression::contains_aggregate(&p.expression) {
                // Aliases are lowercase, like plain projections: the final projection refers
                // to columns with `col`, which normalizes identifiers.
                let alias = if let Some(alias) = &p.alias {
                    alias.to_lowercase()
                } else {
                    super::super::expression::to_cypher_column_name(&p.expression)
                };
                let mut rewritten = p.expression.clone();
                let first = aggregates.len();
                split_aggregates(&mut rewritten, &mut aggregates);
                for (column, aggregate) in &aggregates[first..] {
                    let mut aggregate = self.value_expr(aggregate, &schema)?;
                    // collect() gathers in the order of a preceding `WITH ... ORDER BY`.
                    if let datafusion::logical_expr::Expr::AggregateFunction(f) = &mut aggregate {
                        if f.func.name() == "array_agg" && !f.params.distinct {
                            f.params.order_by = ordering.to_vec();
                        }
                    }
                    agg_exprs.push(aggregate.alias(column));
                }
                outer.push(super::super::expression::to_df_value_expr(&rewritten).alias(alias));
            } else {
                // Group expressions: use raw expression for grouping, no alias
                group_exprs.push(self.value_expr(&p.expression, &schema)?);
            }
        }

        // After aggregation, add a projection to apply aliases to group columns
        let mut final_projection = Vec::new();
        let mut agg_idx = 0;
        for p in projections {
            if let Some(columns) = Self::entity_columns(&schema, &p.expression, p.alias.as_deref())
            {
                // After grouping, the node's columns are plain group columns of the same names.
                final_projection.extend(columns.into_iter().map(|c| match c {
                    datafusion::logical_expr::Expr::Alias(a) => {
                        let datafusion::logical_expr::Expr::Column(column) = *a.expr else {
                            unreachable!("entity_columns returns aliased columns")
                        };
                        col(datafusion::common::Column::new_unqualified(column.name)).alias(a.name)
                    }
                    other => other,
                }));
            } else if !super::super::expression::contains_aggregate(&p.expression) {
                // Re-create the expression and apply alias
                let expr = super::super::expression::to_df_value_expr(&p.expression);
                let aliased = if let Some(alias) = &p.alias {
                    expr.alias(alias.to_lowercase())
                } else {
                    let cypher_name =
                        super::super::expression::to_cypher_column_name(&p.expression);
                    expr.alias(cypher_name)
                };
                final_projection.push(aliased);
            } else {
                final_projection.push(outer[agg_idx].clone());
                agg_idx += 1;
            }
        }

        let aggregate = LogicalPlanBuilder::from(input_plan)
            .aggregate(group_exprs, agg_exprs)
            .and_then(|b| b.build())
            .map_err(|e| self.plan_error("Failed to build aggregate", e))?;
        let final_projection = final_projection
            .into_iter()
            .map(|expr| self.resolve_expr(expr, aggregate.schema()))
            .collect::<Result<Vec<_>>>()?;
        LogicalPlanBuilder::from(aggregate)
            .project(final_projection)
            .map_err(|e| self.plan_error("Failed to project after aggregate", e))?
            .build()
            .map_err(|e| self.plan_error("Failed to build plan", e))
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::ValueExpression;
    use crate::datafusion_planner::{
        test_fixtures::{make_catalog, person_config, person_scan},
        DataFusionPlanner, GraphPhysicalPlanner,
    };
    use crate::logical_plan::{LogicalOperator, ProjectionItem};

    #[test]
    fn test_project_with_aggregate_alias() {
        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());

        let scan = person_scan("n");
        let project = LogicalOperator::Project {
            input: Box::new(scan),
            projections: vec![ProjectionItem {
                expression: ValueExpression::AggregateFunction {
                    name: "count".to_string(),
                    args: vec![ValueExpression::Variable("*".to_string())],
                    distinct: false,
                },
                alias: Some("total".to_string()),
            }],
        };

        let df_plan = planner.plan(&project).unwrap();
        let s = format!("{:?}", df_plan);
        assert!(
            s.contains("Aggregate") || s.contains("count"),
            "plan missing Aggregate or count: {}",
            s
        );
    }

    #[test]
    fn test_project_with_aggregate_without_alias() {
        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());

        let scan = person_scan("n");
        let project = LogicalOperator::Project {
            input: Box::new(scan),
            projections: vec![ProjectionItem {
                expression: ValueExpression::AggregateFunction {
                    name: "count".to_string(),
                    args: vec![ValueExpression::Variable("*".to_string())],
                    distinct: false,
                },
                alias: None,
            }],
        };

        let df_plan = planner.plan(&project).unwrap();
        let s = format!("{:?}", df_plan);
        assert!(
            s.contains("Aggregate") || s.contains("count"),
            "plan missing Aggregate or count: {}",
            s
        );
    }
}
