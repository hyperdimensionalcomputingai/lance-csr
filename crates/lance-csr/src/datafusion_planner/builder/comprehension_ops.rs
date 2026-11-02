// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! List comprehensions: `[x IN list WHERE keep | map]`, per row.

use std::collections::HashSet;

use arrow_schema::DataType;
use datafusion::common::{Column, ScalarValue};
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions_aggregate::array_agg::array_agg;
use datafusion::functions_aggregate::count::count;
use datafusion::functions_nested::expr_fn::{array_length, array_slice, gen_series, make_array};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{
    col, lit, when, Cast, Expr, ExprFunctionExt, ExprSchemable, JoinType, LogicalPlan,
    LogicalPlanBuilder,
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
        // The list is unnested beside its positions, so an element row carries its item,
        // not a copy of the whole list. A list read from an input column is rebuilt once
        // per input row below instead of being grouped on, so unless the comprehension
        // reads it, projection pushdown drops it before the unnest.
        let (items, len, is_null) = (
            format!("{output}__items"),
            format!("{output}__len"),
            format!("{output}__null"),
        );
        let iterated = match &list {
            Expr::Column(c) if numbered.schema().has_column(c) => Some(c.name.clone()),
            _ => None,
        };
        let select = |e: Expr| datafusion::logical_expr::select_expr::SelectExpr::Expression(e);
        let exploded = LogicalPlanBuilder::from(numbered.clone())
            .project(vec![
                datafusion::logical_expr::wildcard(),
                select(positions.alias(&pos)),
                select(list.clone().alias(&items)),
                select(length.alias(&len)),
                select(list.is_null().alias(&is_null)),
            ])
            .and_then(|b| {
                b.unnest_columns_with_options(
                    vec![Column::from_name(&pos), Column::from_name(&items)],
                    Default::default(),
                )
            })
            .map_err(error("Failed to unnest comprehension list"))?;
        let length = col(&len);
        let item = col(&items);
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
                let with_ids = exploded
                    .project(vec![
                        datafusion::logical_expr::wildcard(),
                        datafusion::logical_expr::select_expr::SelectExpr::Expression(
                            item.alias(&id_column),
                        ),
                    ])
                    .map_err(error("Failed to join comprehension elements to nodes"))?;
                // An in-memory node index looks each element up, at a cost that follows the
                // elements rather than the node table, as for `late_node`; Lance tables join.
                let rows = self
                    .adjacency
                    .as_ref()
                    .and_then(|a| a.nodes(label))
                    .filter(|rows| rows.in_memory());
                let prefix = format!("{}__", variable.to_lowercase());
                let wanted: Option<Vec<(String, String)>> = rows.and_then(|rows| {
                    nodes
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| {
                            let suffix = f.name().strip_prefix(&prefix)?;
                            let column = rows
                                .schema()
                                .fields()
                                .iter()
                                .find(|c| c.name().eq_ignore_ascii_case(suffix))?;
                            Some((f.name().clone(), column.name().clone()))
                        })
                        .collect()
                });
                if let (Some(rows), Some(wanted)) = (rows, wanted) {
                    let lookup = crate::csr_expand::NodeLookupNode::try_new(
                        with_ids
                            .build()
                            .map_err(error("Failed to look up comprehension elements"))?,
                        id_column,
                        variable.to_lowercase(),
                        rows.clone(),
                        &wanted,
                    )?;
                    LogicalPlan::Extension(datafusion::logical_expr::Extension {
                        node: std::sync::Arc::new(lookup),
                    })
                } else {
                    let joined = with_ids
                        .join(
                            nodes,
                            JoinType::Left,
                            (
                                vec![id_column.clone()],
                                vec![qualify_column(variable, &mapping.id_field)],
                            ),
                            None,
                        )
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
        let mut aggregates = vec![collected];
        let rebuilt = format!("{output}__list");
        if iterated.is_some() {
            // The padding row of an empty list sorts last and is sliced off.
            aggregates.push(
                array_agg(col(&items))
                    .order_by(vec![col(&pos).sort(true, false)])
                    .build()
                    .map_err(error("Failed to rebuild comprehension list"))?
                    .alias(&rebuilt),
            );
        }
        // One group per input row: its number plus every input column, all equal within it.
        let names: HashSet<String> = numbered
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .chain([len.clone(), is_null.clone()])
            .filter(|name| Some(name) != iterated.as_ref())
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
            .map(|f| {
                if Some(f.name()) != iterated.as_ref() {
                    return Ok(Expr::Column(Column::new_unqualified(f.name())));
                }
                let list = array_slice(col(&rebuilt), lit(1_i64), col(&len), None);
                when(col(&is_null), lit(ScalarValue::Null))
                    .otherwise(list)
                    .map(|e| {
                        datafusion::logical_expr::cast(e, f.data_type().clone()).alias(f.name())
                    })
                    .map_err(error("Failed to rebuild comprehension list"))
            })
            .collect::<Result<_>>()?;
        let value = if count_only {
            col(output)
        } else {
            coalesce(vec![col(output), make_array(vec![])])
        };
        out.push(value.alias(output));
        LogicalPlanBuilder::from(elements)
            .aggregate(group, aggregates)
            .and_then(|b| b.project(out))
            .and_then(|b| b.build())
            .map_err(error("Failed to build list comprehension"))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{types::Int64Type, Array, ArrayRef, Int64Array, ListArray, RecordBatch};
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
            // The unused rebuild of `p.values` is pruned before execution.
            let physical = plan.split("physical_plan").nth(1).unwrap();
            assert!(physical.contains("count("), "{plan}");
            assert!(!physical.contains("array_agg("), "{plan}");
            let mapped = make_query("| x")
                .explain(data())
                .await
                .unwrap()
                .to_lowercase();
            assert!(mapped.contains("array_agg("), "{mapped}");
        }
    }

    #[tokio::test]
    async fn iterated_list_is_returned_unchanged() {
        let config = crate::GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();
        let run = |query: &str| {
            let query = crate::CypherQuery::new(query)
                .unwrap()
                .with_config(config.clone());
            async move { query.execute(data(), None).await.unwrap() }
        };
        let expected = run("MATCH (p:Person) RETURN p.id AS id, p.values AS v ORDER BY id").await;
        for comprehension in [
            "size([x IN p.values WHERE x > 1])",
            "[x IN p.values WHERE x > 1 | x * 10]",
            // The body reads the iterated list itself.
            "[x IN p.values | size(p.values)]",
        ] {
            let actual = run(&format!(
                "MATCH (p:Person) RETURN p.id AS id, p.values AS v, {comprehension} AS c ORDER BY id"
            ))
            .await;
            assert_eq!(actual.column(1), expected.column(1), "{comprehension}");
        }
        let reads_list = run(
            "MATCH (p:Person) RETURN p.id AS id, [x IN p.values | size(p.values)] AS c ORDER BY id",
        )
        .await;
        let c = reads_list
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let sizes: Vec<_> = (0..c.len())
            .map(|i| {
                c.value(i)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(sizes, vec![vec![4; 4], vec![], vec![], vec![1]]);
    }
}
