// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Aggregation operations: Projection with aggregates and grouping

use crate::datafusion_planner::DataFusionPlanner;
use crate::error::Result;
use crate::logical_plan::*;
use datafusion::logical_expr::{col, LogicalPlan, LogicalPlanBuilder};

impl DataFusionPlanner {
    pub(crate) fn build_project_with_aggregates(
        &self,
        input_plan: LogicalPlan,
        projections: &[ProjectionItem],
    ) -> Result<LogicalPlan> {
        // Separate group expressions (non-aggregates) from aggregate expressions
        let mut group_exprs = Vec::new();
        let mut agg_exprs = Vec::new();
        // Store computed aliases for aggregates to reuse in final projection
        let mut agg_aliases = Vec::new();

        let schema = input_plan.schema().clone();
        for p in projections {
            // Grouping by a node groups by all its columns (`WITH b, count(*) AS n`).
            if let Some(columns) = Self::entity_columns(&schema, &p.expression, None) {
                group_exprs.extend(columns.into_iter().map(|c| match c {
                    datafusion::logical_expr::Expr::Alias(a) => *a.expr,
                    other => other,
                }));
                continue;
            }
            let expr = super::super::expression::to_df_value_expr(&p.expression);

            if super::super::expression::contains_aggregate(&p.expression) {
                // Aggregate expressions get aliased. Lowercase, like plain projections: the
                // final projection refers to it with `col`, which normalizes identifiers, so a
                // camelCase alias (`count(*) AS postCount`) failed to plan.
                let alias = if let Some(alias) = &p.alias {
                    alias.to_lowercase()
                } else {
                    super::super::expression::to_cypher_column_name(&p.expression)
                };
                agg_exprs.push(expr.alias(&alias));
                agg_aliases.push(alias);
            } else {
                // Group expressions: use raw expression for grouping, no alias
                group_exprs.push(expr);
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
                // For aggregates, reference the column using the same alias we computed earlier
                final_projection.push(col(&agg_aliases[agg_idx]));
                agg_idx += 1;
            }
        }

        LogicalPlanBuilder::from(input_plan)
            .aggregate(group_exprs, agg_exprs)
            .map_err(|e| self.plan_error("Failed to build aggregate", e))?
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
