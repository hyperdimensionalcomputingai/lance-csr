// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Basic operations: Filter, Project, Distinct, Sort, Limit, Offset

use crate::datafusion_planner::analysis::PlanningContext;
use crate::datafusion_planner::DataFusionPlanner;
use crate::error::Result;
use crate::logical_plan::*;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, SortExpr};

impl DataFusionPlanner {
    /// UNION ALL of alternation branches. Each branch is planned with its own variable
    /// labels (the analysis keeps only one per variable), then all branches are projected
    /// to the same columns by name: a column some branch lacks (a property only one label
    /// has) is a typed null there, and differing types are cast to the first branch's.
    pub(crate) fn build_union(
        &self,
        ctx: &mut PlanningContext,
        inputs: &[LogicalOperator],
    ) -> Result<LogicalPlan> {
        use datafusion::common::{Column, ScalarValue};
        use datafusion::logical_expr::{cast, lit, Expr};

        let mut plans = Vec::with_capacity(inputs.len());
        for input in inputs {
            let mut analysis = ctx.analysis.clone();
            Self::collect_labels(input, &mut analysis.var_to_label);
            let mut branch = PlanningContext::new(&analysis);
            branch.relationship_instance_idx = ctx.relationship_instance_idx.clone();
            plans.push(self.build_operator(&mut branch, input)?);
            // Branches consume distinct relationship instances, in analysis order.
            ctx.relationship_instance_idx = branch.relationship_instance_idx;
        }
        let mut columns: Vec<(String, arrow_schema::DataType)> = Vec::new();
        for plan in &plans {
            for field in plan.schema().fields() {
                if !columns.iter().any(|(name, _)| name == field.name()) {
                    columns.push((field.name().clone(), field.data_type().clone()));
                }
            }
        }
        let mut union: Option<LogicalPlanBuilder> = None;
        for plan in plans {
            let schema = plan.schema().clone();
            let mut exprs = Vec::with_capacity(columns.len());
            for (name, data_type) in &columns {
                let expr = match schema.iter().find(|(_, f)| f.name() == name) {
                    Some((qualifier, field)) => {
                        let column = Expr::Column(Column::new(qualifier.cloned(), field.name()));
                        if field.data_type() == data_type {
                            column
                        } else {
                            cast(column, data_type.clone())
                        }
                    }
                    None => lit(ScalarValue::try_from(data_type)
                        .map_err(|e| self.plan_error("Failed to build a null column", e))?),
                };
                exprs.push(expr.alias(name));
            }
            let branch = LogicalPlanBuilder::from(plan)
                .project(exprs)
                .and_then(|b| b.build())
                .map_err(|e| self.plan_error("Failed to align UNION branch", e))?;
            union = Some(match union {
                None => LogicalPlanBuilder::from(branch),
                Some(b) => b
                    .union(branch)
                    .map_err(|e| self.plan_error("Failed to UNION branches", e))?,
            });
        }
        union
            .ok_or_else(|| crate::error::GraphError::PlanError {
                message: "empty UNION".to_string(),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?
            .build()
            .map_err(|e| self.plan_error("Failed to build UNION", e))
    }

    pub(crate) fn build_filter(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
        predicate: &crate::ast::BooleanExpression,
    ) -> Result<LogicalPlan> {
        let input_plan = self.build_operator(ctx, input)?;
        let expr = self.boolean_expr(predicate, input_plan.schema())?;
        LogicalPlanBuilder::from(input_plan)
            .filter(expr)
            .map_err(|e| self.plan_error("Failed to build filter", e))?
            .build()
            .map_err(|e| self.plan_error("Failed to build plan", e))
    }

    pub(crate) fn build_project(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
        projections: &[ProjectionItem],
    ) -> Result<LogicalPlan> {
        // Check if any projection contains an aggregate function
        let has_aggregates = projections
            .iter()
            .any(|p| super::super::expression::contains_aggregate(&p.expression));

        if !has_aggregates {
            let input_plan = self.build_operator(ctx, input)?;
            return self.build_simple_project(input_plan, projections);
        }
        // An ORDER BY right before an aggregation only orders what collect() gathers, as in
        // Neo4j (LDBC IC7 takes `head(collect(...))` of sorted rows): it becomes the
        // aggregate's own ORDER BY instead of a sort of its input.
        if let LogicalOperator::Sort {
            input: inner,
            sort_items,
        } = input
        {
            let input_plan = self.build_operator(ctx, inner)?;
            let ordering = self
                .sort_exprs(&input_plan, sort_items)
                .into_iter()
                .map(|sort| {
                    Ok(SortExpr {
                        expr: self.resolve_expr(sort.expr, input_plan.schema())?,
                        ..sort
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            return self.build_project_with_aggregates(input_plan, projections, &ordering);
        }
        let input_plan = self.build_operator(ctx, input)?;
        self.build_project_with_aggregates(input_plan, projections, &[])
    }

    /// The columns of a node or relationship variable (`b` -> `b__id`, `b__name`, ...),
    /// renamed for `alias`, or `None` when `expr` is not a bare variable of that kind.
    /// Lets `WITH b` carry a whole node to later clauses; RETURN never reaches here
    /// with a node, as the logical planner expands those to their configured properties.
    pub(crate) fn entity_columns(
        schema: &datafusion::common::DFSchema,
        expr: &crate::ast::ValueExpression,
        alias: Option<&str>,
    ) -> Option<Vec<datafusion::logical_expr::Expr>> {
        let crate::ast::ValueExpression::Variable(var) = expr else {
            return None;
        };
        let var = var.to_lowercase();
        if schema.has_column_with_unqualified_name(&var) {
            return None; // a scalar (an alias from an earlier WITH, an UNWIND value)
        }
        let prefix = format!("{var}__");
        let columns: Vec<_> = schema
            .iter()
            .filter(|(_, f)| f.name().starts_with(&prefix))
            .map(|(qualifier, f)| {
                let name = match alias {
                    Some(a) => format!("{}__{}", a.to_lowercase(), &f.name()[prefix.len()..]),
                    None => f.name().clone(),
                };
                datafusion::logical_expr::Expr::Column(datafusion::common::Column::new(
                    qualifier.cloned(),
                    f.name(),
                ))
                .alias(name)
            })
            .collect();
        (!columns.is_empty()).then_some(columns)
    }

    pub(crate) fn build_simple_project(
        &self,
        input_plan: LogicalPlan,
        projections: &[ProjectionItem],
    ) -> Result<LogicalPlan> {
        let schema = input_plan.schema().clone();
        let mut exprs: Vec<datafusion::logical_expr::Expr> = Vec::new();
        for p in projections {
            if let Some(columns) = Self::entity_columns(&schema, &p.expression, p.alias.as_deref())
            {
                exprs.extend(columns);
                continue;
            }
            let expr = self.value_expr(&p.expression, &schema)?;
            // Apply alias if provided, otherwise use Cypher dot notation
            // Normalize alias to lowercase for case-insensitive behavior
            exprs.push(if let Some(alias) = &p.alias {
                expr.alias(alias.to_lowercase())
            } else {
                // Convert to Cypher dot notation (e.g., p__name -> p.name)
                let cypher_name = super::super::expression::to_cypher_column_name(&p.expression);
                expr.alias(cypher_name)
            });
        }
        LogicalPlanBuilder::from(input_plan)
            .project(exprs)
            .map_err(|e| self.plan_error("Failed to build projection", e))?
            .build()
            .map_err(|e| self.plan_error("Failed to build plan", e))
    }

    pub(crate) fn build_distinct(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
    ) -> Result<LogicalPlan> {
        let input_plan = self.build_operator(ctx, input)?;
        LogicalPlanBuilder::from(input_plan)
            .distinct()
            .map_err(|e| self.plan_error("Failed to build distinct", e))?
            .build()
            .map_err(|e| self.plan_error("Failed to build plan", e))
    }

    pub(crate) fn build_sort(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
        sort_items: &[SortItem],
    ) -> Result<LogicalPlan> {
        let input_plan = self.build_operator(ctx, input)?;
        let sort_exprs = self.sort_exprs(&input_plan, sort_items);

        LogicalPlanBuilder::from(input_plan)
            .sort(sort_exprs)
            .map_err(|e| self.plan_error("Failed to build sort", e))?
            .build()
            // The sort builder first restores input columns omitted by RETURN.
            .and_then(|plan| plan.resolve_lambda_variables().map(|p| p.data))
            .map_err(|e| self.plan_error("Failed to build plan", e))
    }

    /// `sort_items` as DataFusion sort expressions over `input_plan`'s columns.
    pub(crate) fn sort_exprs(
        &self,
        input_plan: &LogicalPlan,
        sort_items: &[SortItem],
    ) -> Vec<SortExpr> {
        sort_items
            .iter()
            .map(|item| {
                let bound =
                    super::super::expression::bind_value(&item.expression, input_plan.schema());
                SortExpr {
                    expr: super::super::expression::to_df_value_expr(&bound),
                    asc: matches!(item.direction, crate::ast::SortDirection::Ascending),
                    nulls_first: true,
                }
            })
            .collect()
    }

    pub(crate) fn build_limit(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
        count: &u64,
    ) -> Result<LogicalPlan> {
        let input_plan = self.build_operator(ctx, input)?;
        LogicalPlanBuilder::from(input_plan)
            .limit(0, Some((*count) as usize))
            .map_err(|e| self.plan_error("Failed to build limit", e))?
            .build()
            .map_err(|e| self.plan_error("Failed to build plan", e))
    }

    pub(crate) fn build_offset(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
        offset: &u64,
    ) -> Result<LogicalPlan> {
        let input_plan = self.build_operator(ctx, input)?;
        LogicalPlanBuilder::from(input_plan)
            .limit((*offset) as usize, None)
            .map_err(|e| self.plan_error("Failed to build offset", e))?
            .build()
            .map_err(|e| self.plan_error("Failed to build plan", e))
    }

    pub(crate) fn build_unwind(
        &self,
        ctx: &mut PlanningContext,
        input: &Option<Box<LogicalOperator>>,
        expression: &crate::ast::ValueExpression,
        alias: &str,
        label: Option<&str>,
    ) -> Result<LogicalPlan> {
        let input_plan = if let Some(input_op) = input {
            self.build_operator(ctx, input_op)?
        } else {
            // Create an empty relation that produces one row for UNWIND [..]
            LogicalPlanBuilder::empty(true)
                .build()
                .map_err(|e| self.plan_error("Failed to create empty relation", e))?
        };

        // Convert expression to DataFusion Expr
        let df_expr = self.value_expr(expression, input_plan.schema())?;
        // A list of node ids: unnest under a temporary name, then join each id back to the
        // node table so the alias carries the node's columns like any matched node.
        let unnested = match label {
            Some(_) => format!("__unwind_{}", alias.to_lowercase()),
            None => alias.to_string(),
        };
        let alias = unnested.as_str();

        // We project the list expression first (aliased as the target alias temporarily)
        // DataFusion unnest takes a column name.
        let builder = LogicalPlanBuilder::from(input_plan);

        let builder = builder
            .project(vec![
                datafusion::logical_expr::wildcard(),
                datafusion::logical_expr::select_expr::SelectExpr::Expression(df_expr.alias(alias)),
            ])
            .map_err(|e| self.plan_error("Failed to project unwind expression", e))?;

        let builder = builder
            .unnest_column(alias)
            .map_err(|e| self.plan_error("Failed to build unnest", e))?;

        let Some(label) = label else {
            return builder
                .build()
                .map_err(|e| self.plan_error("Failed to build unwind plan", e));
        };
        let variable = &alias["__unwind_".len()..];
        let mapping = self.config.get_node_mapping(label).ok_or_else(|| {
            crate::error::GraphError::PlanError {
                message: format!("No node mapping for '{label}'"),
                location: snafu::Location::new(file!(), line!(), column!()),
            }
        })?;
        let nodes = self.build_scan(ctx, variable, label, &Default::default())?;
        let id = crate::case_insensitive::qualify_column(variable, &mapping.id_field);
        let joined = builder
            .join(
                nodes,
                datafusion::logical_expr::JoinType::Inner,
                (vec![alias.to_string()], vec![id]),
                None,
            )
            .map_err(|e| self.plan_error("Failed to join unwound ids to nodes", e))?
            .build()
            .map_err(|e| self.plan_error("Failed to build unwind plan", e))?;
        let kept: Vec<datafusion::logical_expr::Expr> = joined
            .schema()
            .columns()
            .into_iter()
            .filter(|c| c.name != alias)
            .map(datafusion::logical_expr::Expr::Column)
            .collect();
        LogicalPlanBuilder::from(joined)
            .project(kept)
            .map_err(|e| self.plan_error("Failed to project unwound nodes", e))?
            .build()
            .map_err(|e| self.plan_error("Failed to build unwind plan", e))
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{
        BooleanExpression, ComparisonOperator, PropertyRef, PropertyValue, SortDirection,
        ValueExpression,
    };
    use crate::datafusion_planner::{
        test_fixtures::{make_catalog, person_config, person_scan},
        DataFusionPlanner, GraphPhysicalPlanner,
    };
    use crate::logical_plan::{LogicalOperator, ProjectionItem, SortItem};

    #[test]
    fn test_df_planner_scan_filter_project() {
        let scan = person_scan("n");

        let pred = BooleanExpression::Comparison {
            left: ValueExpression::Property(PropertyRef {
                variable: "n".to_string(),
                property: "age".to_string(),
            }),
            operator: ComparisonOperator::GreaterThan,
            right: ValueExpression::Literal(PropertyValue::Integer(30)),
        };

        let filter = LogicalOperator::Filter {
            input: Box::new(scan),
            predicate: pred,
        };

        let project = LogicalOperator::Project {
            input: Box::new(filter),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "n".into(),
                    property: "name".into(),
                }),
                alias: None,
            }],
        };

        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());
        let df_plan = planner.plan(&project).unwrap();

        let s = format!("{:?}", df_plan);
        assert!(s.contains("Projection"), "plan missing Projection: {}", s);
        assert!(s.contains("Filter"), "plan missing Filter: {}", s);
        assert!(s.contains("TableScan"), "plan missing TableScan: {}", s);
        assert!(
            s.contains("Person") || s.contains("person"),
            "plan missing table name: {}",
            s
        );
    }

    #[test]
    fn test_distinct_and_order_with_qualified_columns() {
        let scan = person_scan("n");
        let project = LogicalOperator::Project {
            input: Box::new(scan),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef::new("n", "name")),
                alias: None,
            }],
        };
        let sort = LogicalOperator::Sort {
            input: Box::new(project),
            sort_items: vec![SortItem {
                expression: ValueExpression::Property(PropertyRef::new("n", "name")),
                direction: SortDirection::Ascending,
            }],
        };
        let distinct = LogicalOperator::Distinct {
            input: Box::new(sort),
        };
        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());
        let df_plan = planner.plan(&distinct).unwrap();
        let s = format!("{:?}", df_plan);
        assert!(s.contains("Distinct"), "plan missing Distinct: {}", s);
        assert!(s.contains("Sort"), "plan missing Sort: {}", s);
    }

    #[test]
    fn test_skip_limit_after_aliasing() {
        let scan = person_scan("n");
        let project = LogicalOperator::Project {
            input: Box::new(scan),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef::new("n", "name")),
                alias: Some("person_name".to_string()),
            }],
        };
        let limit = LogicalOperator::Limit {
            input: Box::new(project),
            count: 10,
        };
        let offset = LogicalOperator::Offset {
            input: Box::new(limit),
            offset: 5,
        };
        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());
        let df_plan = planner.plan(&offset).unwrap();
        let s = format!("{:?}", df_plan);
        assert!(s.contains("Limit"), "plan missing Limit: {}", s);
    }

    #[test]
    fn test_order_by_single_column_asc() {
        use crate::ast::{PropertyRef, SortDirection, ValueExpression};
        use crate::logical_plan::{LogicalOperator, ProjectionItem, SortItem};

        let scan = person_scan("n");
        let project = LogicalOperator::Project {
            input: Box::new(scan),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef::new("n", "name")),
                alias: None,
            }],
        };
        let sort = LogicalOperator::Sort {
            input: Box::new(project),
            sort_items: vec![SortItem {
                expression: ValueExpression::Property(PropertyRef::new("n", "age")),
                direction: SortDirection::Ascending,
            }],
        };

        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());
        let df_plan = planner.plan(&sort).unwrap();
        let s = format!("{:?}", df_plan);
        assert!(s.contains("Sort"), "plan missing Sort: {}", s);
    }

    #[test]
    fn test_order_by_multiple_columns() {
        let scan = person_scan("n");
        let project = LogicalOperator::Project {
            input: Box::new(scan),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef::new("n", "name")),
                alias: None,
            }],
        };
        let sort = LogicalOperator::Sort {
            input: Box::new(project),
            sort_items: vec![
                SortItem {
                    expression: ValueExpression::Property(PropertyRef::new("n", "name")),
                    direction: SortDirection::Ascending,
                },
                SortItem {
                    expression: ValueExpression::Property(PropertyRef::new("n", "age")),
                    direction: SortDirection::Descending,
                },
            ],
        };

        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());
        let df_plan = planner.plan(&sort).unwrap();
        let s = format!("{:?}", df_plan);
        assert!(s.contains("Sort"), "plan missing Sort: {}", s);
    }

    #[test]
    fn test_order_by_with_limit() {
        use crate::ast::{PropertyRef, SortDirection, ValueExpression};
        use crate::logical_plan::{LogicalOperator, ProjectionItem, SortItem};

        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());

        let scan = LogicalOperator::ScanByLabel {
            variable: "n".to_string(),
            label: "Person".to_string(),
            properties: Default::default(),
        };

        let project = LogicalOperator::Project {
            input: Box::new(scan),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "n".to_string(),
                    property: "name".to_string(),
                }),
                alias: None,
            }],
        };

        let sort = LogicalOperator::Sort {
            input: Box::new(project),
            sort_items: vec![SortItem {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "n".to_string(),
                    property: "name".to_string(),
                }),
                direction: SortDirection::Ascending,
            }],
        };

        let limit = LogicalOperator::Limit {
            input: Box::new(sort),
            count: 10,
        };

        let df_plan = planner.plan(&limit).unwrap();
        let s = format!("{:?}", df_plan);

        // Should contain both Limit and Sort
        assert!(s.contains("Limit") || s.contains("limit"));
        assert!(s.contains("Sort") || s.contains("sort"));
        assert!(s.contains("n__name"));
    }

    #[test]
    fn test_project_with_alias() {
        use crate::ast::{PropertyRef, ValueExpression};
        use crate::logical_plan::{LogicalOperator, ProjectionItem};

        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());

        let scan = LogicalOperator::ScanByLabel {
            variable: "n".to_string(),
            label: "Person".to_string(),
            properties: Default::default(),
        };

        let project = LogicalOperator::Project {
            input: Box::new(scan),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "n".to_string(),
                    property: "name".to_string(),
                }),
                alias: Some("person_name".to_string()),
            }],
        };

        let df_plan = planner.plan(&project).unwrap();
        let s = format!("{:?}", df_plan);

        // Should contain the alias
        assert!(s.contains("person_name"));
    }

    #[test]
    fn test_project_with_multiple_aliases() {
        use crate::ast::{PropertyRef, ValueExpression};
        use crate::logical_plan::{LogicalOperator, ProjectionItem};

        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());

        let scan = person_scan("p");

        let project = LogicalOperator::Project {
            input: Box::new(scan),
            projections: vec![
                ProjectionItem {
                    expression: ValueExpression::Property(PropertyRef {
                        variable: "p".to_string(),
                        property: "name".to_string(),
                    }),
                    alias: Some("name".to_string()),
                },
                ProjectionItem {
                    expression: ValueExpression::Property(PropertyRef {
                        variable: "p".to_string(),
                        property: "age".to_string(),
                    }),
                    alias: Some("age".to_string()),
                },
            ],
        };

        let df_plan = planner.plan(&project).unwrap();
        let s = format!("{:?}", df_plan);

        // Should contain both aliases
        assert!(s.contains("name"));
        assert!(s.contains("age"));
    }

    #[test]
    fn test_project_mixed_with_and_without_alias() {
        use crate::ast::{PropertyRef, ValueExpression};
        use crate::logical_plan::{LogicalOperator, ProjectionItem};

        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());

        let scan = LogicalOperator::ScanByLabel {
            variable: "p".to_string(),
            label: "Person".to_string(),
            properties: Default::default(),
        };

        let project = LogicalOperator::Project {
            input: Box::new(scan),
            projections: vec![
                ProjectionItem {
                    expression: ValueExpression::Property(PropertyRef {
                        variable: "p".to_string(),
                        property: "name".to_string(),
                    }),
                    alias: Some("full_name".to_string()),
                },
                ProjectionItem {
                    expression: ValueExpression::Property(PropertyRef {
                        variable: "p".to_string(),
                        property: "age".to_string(),
                    }),
                    alias: None, // No alias - should use qualified name
                },
            ],
        };

        let df_plan = planner.plan(&project).unwrap();
        let s = format!("{:?}", df_plan);

        // Should contain the alias and the qualified name
        assert!(s.contains("full_name"));
        assert!(s.contains("p__age"));
    }

    #[test]
    fn test_cypher_dot_notation_simple_property() {
        // Test that projections without aliases use Cypher dot notation
        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());

        let scan = LogicalOperator::ScanByLabel {
            variable: "p".to_string(),
            label: "Person".to_string(),
            properties: Default::default(),
        };

        // Project without alias - should use Cypher dot notation
        let project = LogicalOperator::Project {
            input: Box::new(scan),
            projections: vec![ProjectionItem {
                expression: ValueExpression::Property(PropertyRef {
                    variable: "p".to_string(),
                    property: "name".to_string(),
                }),
                alias: None, // No explicit alias
            }],
        };

        let df_plan = planner.plan(&project).unwrap();
        let plan_str = format!("{:?}", df_plan);

        // Should contain Cypher dot notation "p.name", not "p__name"
        assert!(
            plan_str.contains("p.name"),
            "Plan should contain Cypher dot notation 'p.name': {}",
            plan_str
        );
        assert!(
            !plan_str.contains("p__name AS"),
            "Plan should not contain DataFusion qualified name 'p__name AS': {}",
            plan_str
        );
    }

    #[test]
    fn test_cypher_dot_notation_multiple_properties() {
        // Test multiple properties from the same variable
        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());

        let scan = LogicalOperator::ScanByLabel {
            variable: "p".to_string(),
            label: "Person".to_string(),
            properties: Default::default(),
        };

        // Project multiple properties without aliases
        let project = LogicalOperator::Project {
            input: Box::new(scan),
            projections: vec![
                ProjectionItem {
                    expression: ValueExpression::Property(PropertyRef {
                        variable: "p".to_string(),
                        property: "name".to_string(),
                    }),
                    alias: None,
                },
                ProjectionItem {
                    expression: ValueExpression::Property(PropertyRef {
                        variable: "p".to_string(),
                        property: "age".to_string(),
                    }),
                    alias: None,
                },
            ],
        };

        let df_plan = planner.plan(&project).unwrap();
        let plan_str = format!("{:?}", df_plan);

        // Should contain both Cypher dot notations
        assert!(
            plan_str.contains("p.name"),
            "Plan should contain 'p.name': {}",
            plan_str
        );
        assert!(
            plan_str.contains("p.age"),
            "Plan should contain 'p.age': {}",
            plan_str
        );
    }

    #[test]
    fn test_cypher_dot_notation_mixed_with_and_without_alias() {
        // Test mix of aliased and non-aliased projections
        let cfg = person_config();
        let planner = DataFusionPlanner::with_catalog(cfg, make_catalog());

        let scan = LogicalOperator::ScanByLabel {
            variable: "p".to_string(),
            label: "Person".to_string(),
            properties: Default::default(),
        };

        let project = LogicalOperator::Project {
            input: Box::new(scan),
            projections: vec![
                ProjectionItem {
                    expression: ValueExpression::Property(PropertyRef {
                        variable: "p".to_string(),
                        property: "name".to_string(),
                    }),
                    alias: Some("full_name".to_string()), // Explicit alias
                },
                ProjectionItem {
                    expression: ValueExpression::Property(PropertyRef {
                        variable: "p".to_string(),
                        property: "age".to_string(),
                    }),
                    alias: None, // No alias - should use dot notation
                },
            ],
        };

        let df_plan = planner.plan(&project).unwrap();
        let plan_str = format!("{:?}", df_plan);

        // Should contain explicit alias
        assert!(
            plan_str.contains("full_name"),
            "Plan should contain explicit alias 'full_name': {}",
            plan_str
        );
        // Should contain Cypher dot notation for non-aliased property
        assert!(
            plan_str.contains("p.age"),
            "Plan should contain Cypher dot notation 'p.age': {}",
            plan_str
        );
    }
}
