// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Aggregation operations: Projection with aggregates and grouping

use std::sync::Arc;

use crate::ast::ValueExpression;
use crate::csr_expand::{NodeLookupNode, NodeRows};
use crate::datafusion_planner::analysis::PlanningContext;
use crate::datafusion_planner::DataFusionPlanner;
use crate::error::Result;
use crate::logical_plan::*;
use datafusion::common::Column;
use datafusion::logical_expr::{col, Expr, Extension, LogicalPlan, LogicalPlanBuilder};

/// A node grouped by its id alone, whose other columns are looked up after the aggregate.
struct LateNode {
    variable: String,
    rows: Arc<NodeRows>,
    /// The id column, as in the input (`b__id`).
    id: String,
    /// The node's other columns (`b__name`), with their names in the node table.
    columns: Vec<(String, String)>,
}

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

/// `count(v)` lowers to `count(v__id)`, which nodes resolve to their key earlier. For a
/// value without an id column (a path, or an UNWIND or WITH value), count the value itself.
/// For a relationship, count the rows where any of its `v__*` columns is set: a matched
/// relationship has non-null endpoints, an unmatched OPTIONAL one has none. `DISTINCT`
/// compares all its columns, so parallel edges with identical properties count once.
fn count_values_without_ids(
    args: &mut [datafusion::logical_expr::Expr],
    distinct: bool,
    schema: &datafusion::common::DFSchema,
) {
    use datafusion::logical_expr::{lit, when, Expr};
    for arg in args {
        let Expr::Column(column) = arg else {
            continue;
        };
        let Some(variable) = column.name.strip_suffix("__id") else {
            continue;
        };
        if schema.has_column_with_unqualified_name(&column.name) {
            continue;
        }
        if schema.has_column_with_unqualified_name(variable) {
            *arg = col(datafusion::common::Column::new_unqualified(variable));
            continue;
        }
        let prefix = format!("{variable}__");
        let columns: Vec<_> = schema
            .iter()
            .filter(|(_, f)| f.name().starts_with(&prefix))
            .map(|(q, f)| {
                (
                    f.name()[prefix.len()..].to_string(),
                    Expr::Column((q, f).into()),
                )
            })
            .collect();
        let Some(bound) = columns
            .iter()
            .map(|(_, c)| c.clone().is_not_null())
            .reduce(Expr::or)
        else {
            continue;
        };
        let value = if distinct {
            datafusion::functions::core::expr_fn::named_struct(
                columns
                    .into_iter()
                    .flat_map(|(name, c)| [lit(name), c])
                    .collect(),
            )
        } else {
            lit(1_i64)
        };
        // Building a CASE can only fail on an empty WHEN list, which `bound` rules out.
        *arg = when(bound, value).end().expect("CASE with one WHEN");
    }
}

impl DataFusionPlanner {
    /// For `WITH b, ...` grouped by a node: when `b`'s label has an in-memory node index
    /// (unique integer ids), group by the id alone and look the other columns up
    /// afterwards, once per node. Rows before the grouping can outnumber nodes many times
    /// over (an expansion), and carrying each node's columns through the aggregate costs
    /// more than the lookup. With unique ids, grouping by the id and by all the columns are
    /// the same. A Lance node table keeps the plain grouping: the input rows already hold
    /// the node's columns, read by an earlier step, so grouping by them reads nothing more,
    /// while a lookup afterwards would read them from disk a second time.
    fn late_node(
        &self,
        ctx: &PlanningContext,
        expr: &ValueExpression,
        columns: &[Expr],
    ) -> Option<LateNode> {
        let ValueExpression::Variable(var) = expr else {
            return None;
        };
        let label = ctx.analysis.var_to_label.get(var)?;
        let rows = self.adjacency.as_ref()?.nodes(label)?.clone();
        if !rows.in_memory() {
            return None;
        }
        let id = self.id_column(ctx, var)?;
        let prefix = format!("{}__", var.to_lowercase());
        let table = rows.schema().clone();
        let mut found_id = false;
        let mut others = Vec::new();
        for column in columns {
            let Expr::Alias(alias) = column else {
                return None;
            };
            if alias.name == id {
                found_id = true;
                continue;
            }
            let suffix = alias.name.strip_prefix(&prefix)?;
            // Only the table's own columns can be fetched again.
            let field = table
                .fields()
                .iter()
                .find(|f| f.name().eq_ignore_ascii_case(suffix))?;
            others.push((alias.name.clone(), field.name().clone()));
        }
        (found_id && !others.is_empty()).then(|| LateNode {
            variable: var.to_lowercase(),
            rows,
            id,
            columns: others,
        })
    }

    pub(crate) fn build_project_with_aggregates(
        &self,
        ctx: &PlanningContext,
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
        let mut late = Vec::new();
        for p in projections {
            // Grouping by a node groups by all its columns (`WITH b, count(*) AS n`).
            if let Some(columns) = Self::entity_columns(&schema, &p.expression, None) {
                if let Some(node) = self.late_node(ctx, &p.expression, &columns) {
                    group_exprs.push(col(Column::new_unqualified(&node.id)));
                    late.push(node);
                    continue;
                }
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
                    if let datafusion::logical_expr::Expr::AggregateFunction(f) = &mut aggregate {
                        // collect() gathers in the order of a preceding `WITH ... ORDER BY`.
                        if f.func.name() == "array_agg" && !f.params.distinct {
                            f.params.order_by = ordering.to_vec();
                        }
                        if f.func.name() == "count" {
                            let distinct = f.params.distinct;
                            count_values_without_ids(&mut f.params.args, distinct, &schema);
                        }
                    }
                    agg_exprs.push(aggregate.alias(column));
                }
                outer.push(self.unresolved_value_expr(&rewritten).alias(alias));
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
                let expr = self.unresolved_value_expr(&p.expression);
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

        let mut aggregate = LogicalPlanBuilder::from(input_plan)
            .aggregate(group_exprs, agg_exprs)
            .and_then(|b| b.build())
            .map_err(|e| self.plan_error("Failed to build aggregate", e))?;
        for node in late {
            aggregate = self.fetch_late_node(aggregate, node)?;
        }
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

impl DataFusionPlanner {
    /// `plan` with `node`'s other columns looked up by its id, under their original names.
    /// A null node (an unmatched OPTIONAL MATCH) gets null columns.
    fn fetch_late_node(&self, plan: LogicalPlan, node: LateNode) -> Result<LogicalPlan> {
        let lookup =
            NodeLookupNode::try_new(plan, node.id, node.variable, node.rows, &node.columns)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(lookup),
        }))
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

    #[test]
    fn count_of_a_relationship_counts_rows_where_it_matched() {
        use arrow_schema::{DataType, Field, Schema};
        let schema = datafusion::common::DFSchema::try_from(Schema::new(vec![
            Field::new("r__src", DataType::Int64, true),
            Field::new("r__since", DataType::Int64, true),
            Field::new("p__id", DataType::Int64, true),
        ]))
        .unwrap();
        let count = |distinct| {
            let mut args = vec![datafusion::logical_expr::col("r__id")];
            super::count_values_without_ids(&mut args, distinct, &schema);
            args[0].to_string()
        };
        let plain = count(false);
        assert!(
            plain.contains("r__src IS NOT NULL OR r__since IS NOT NULL"),
            "{plain}"
        );
        assert!(!plain.contains("p__id"), "{plain}");
        assert!(count(true).contains("named_struct"), "{}", count(true));
    }
}
