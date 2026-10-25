// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Expression Translation
//!
//! Converts AST expressions to DataFusion expressions

use crate::ast::{BooleanExpression, PropertyValue, ValueExpression};
use crate::case_insensitive::qualify_column;
use crate::datafusion_planner::udf;
use datafusion::functions::string::lower;
use datafusion::functions::string::upper;
use datafusion::logical_expr::{col, lit, BinaryExpr, Expr, Operator};
use datafusion_functions_aggregate::array_agg::array_agg;
use datafusion_functions_aggregate::average::avg;
use datafusion_functions_aggregate::count::count;
use datafusion_functions_aggregate::count::count_distinct;
use datafusion_functions_aggregate::min_max::max;
use datafusion_functions_aggregate::min_max::min;
use datafusion_functions_aggregate::sum::sum;

impl super::DataFusionPlanner {
    pub(crate) fn resolve_expr(
        &self,
        expr: Expr,
        schema: &datafusion::common::DFSchema,
    ) -> crate::error::Result<Expr> {
        self.type_parameters(expr)
            .resolve_lambda_variables(schema)
            .map(|e| e.data)
            .map_err(|e| self.plan_error("Failed to resolve expression scope", e))
    }

    pub(crate) fn value_expr(
        &self,
        expr: &ValueExpression,
        schema: &datafusion::common::DFSchema,
    ) -> crate::error::Result<Expr> {
        self.resolve_expr(to_df_value_expr(&bind_value(expr, schema)), schema)
    }

    pub(crate) fn boolean_expr(
        &self,
        expr: &BooleanExpression,
        schema: &datafusion::common::DFSchema,
    ) -> crate::error::Result<Expr> {
        self.resolve_expr(to_df_boolean_expr(&bind_boolean(expr, schema)), schema)
    }
    pub(crate) fn unresolved_value_expr(&self, value: &ValueExpression) -> Expr {
        self.type_parameters(to_df_value_expr(value))
    }

    fn type_parameters(&self, expression: Expr) -> Expr {
        use datafusion::common::tree_node::{Transformed, TreeNode};
        if self.parameter_types.is_empty() {
            return expression;
        }
        expression
            .transform_up(|expression| {
                if let Expr::Placeholder(mut parameter) = expression {
                    parameter.field = self
                        .parameter_types
                        .get(parameter.id.trim_start_matches('$'))
                        .map(|data_type| {
                            std::sync::Arc::new(arrow_schema::Field::new(
                                "",
                                data_type.clone(),
                                true,
                            ))
                        });
                    Ok(Transformed::yes(Expr::Placeholder(parameter)))
                } else {
                    Ok(Transformed::no(expression))
                }
            })
            .expect("typing placeholders does not fail")
            .data
    }
}

/// Helper function to create LIKE expressions with consistent settings
fn create_like_expr(expression: &ValueExpression, pattern: &str, case_insensitive: bool) -> Expr {
    Expr::Like(datafusion::logical_expr::Like {
        negated: false,
        expr: Box::new(to_df_value_expr(expression)),
        pattern: Box::new(lit(pattern.to_string())),
        escape_char: None,
        case_insensitive,
    })
}

/// Convert BooleanExpression to DataFusion Expr
pub(crate) fn to_df_boolean_expr(expr: &BooleanExpression) -> Expr {
    use crate::ast::{BooleanExpression as BE, ComparisonOperator as CO};
    match expr {
        BE::PatternExists { .. } => {
            unreachable!("the logical planner rewrites EXISTS into a mark column")
        }
        BE::Comparison {
            left,
            operator,
            right,
        } => {
            let l = to_df_value_expr(left);
            let r = to_df_value_expr(right);
            let op = match operator {
                CO::Equal => Operator::Eq,
                CO::NotEqual => Operator::NotEq,
                CO::LessThan => Operator::Lt,
                CO::LessThanOrEqual => Operator::LtEq,
                CO::GreaterThan => Operator::Gt,
                CO::GreaterThanOrEqual => Operator::GtEq,
            };
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(l),
                op,
                right: Box::new(r),
            })
        }
        BE::In { expression, list } => {
            use datafusion::logical_expr::expr::InList as DFInList;
            let expr = to_df_value_expr(expression);
            let list_exprs = list.iter().map(to_df_value_expr).collect::<Vec<_>>();
            Expr::InList(DFInList::new(Box::new(expr), list_exprs, false))
        }
        BE::Member { expression, list } => datafusion::functions_nested::expr_fn::array_has(
            to_df_value_expr(list),
            to_df_value_expr(expression),
        ),
        BE::And(l, r) => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(to_df_boolean_expr(l)),
            op: Operator::And,
            right: Box::new(to_df_boolean_expr(r)),
        }),
        BE::Or(l, r) => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(to_df_boolean_expr(l)),
            op: Operator::Or,
            right: Box::new(to_df_boolean_expr(r)),
        }),
        BE::Not(inner) => Expr::Not(Box::new(to_df_boolean_expr(inner))),
        BE::Exists(prop) => Expr::IsNotNull(Box::new(to_df_value_expr(
            &ValueExpression::Property(prop.clone()),
        ))),
        BE::IsNull(expression) => Expr::IsNull(Box::new(to_df_value_expr(expression))),
        BE::IsNotNull(expression) => Expr::IsNotNull(Box::new(to_df_value_expr(expression))),
        BE::Like {
            expression,
            pattern,
        } => create_like_expr(expression, pattern, false),
        BE::ILike {
            expression,
            pattern,
        } => create_like_expr(expression, pattern, true),
        BE::Contains {
            expression,
            substring,
        } => {
            // CONTAINS is equivalent to LIKE '%substring%'
            let pattern = format!("%{}%", substring);
            create_like_expr(expression, &pattern, false)
        }
        BE::StartsWith { expression, prefix } => {
            // STARTS WITH is equivalent to LIKE 'prefix%'
            let pattern = format!("{}%", prefix);
            create_like_expr(expression, &pattern, false)
        }
        BE::EndsWith { expression, suffix } => {
            // ENDS WITH is equivalent to LIKE '%suffix'
            let pattern = format!("%{}", suffix);
            create_like_expr(expression, &pattern, false)
        }
    }
}

/// Convert ValueExpression to DataFusion Expr
pub(crate) fn to_df_value_expr(expr: &ValueExpression) -> Expr {
    use crate::ast::{PropertyValue as PV, ValueExpression as VE};
    match expr {
        VE::Property(prop) => {
            // Create qualified column name: variable__property (lowercase for case-insensitivity)
            col(qualify_column(&prop.variable, &prop.property))
        }
        VE::Variable(v) => col(v.to_lowercase()),
        VE::LocalVariable(v) => Expr::LambdaVariable(
            datafusion::logical_expr::expr::LambdaVariable::new(v.to_lowercase(), None),
        ),
        VE::Reduce {
            accumulator,
            initial,
            variable,
            list,
            expression,
        } => super::list_folds::reduce(
            to_df_value_expr(list),
            to_df_value_expr(initial),
            vec![accumulator.clone(), variable.clone()],
            to_df_value_expr(expression),
        ),
        VE::ListPredicate {
            kind,
            variable,
            list,
            predicate,
        } => super::list_folds::predicate(
            kind,
            to_df_value_expr(list),
            variable.clone(),
            to_df_value_expr(predicate),
        ),
        VE::Literal(PV::String(s)) => lit(s.clone()),
        VE::Literal(PV::Integer(i)) => lit(*i),
        VE::Literal(PV::Float(f)) => lit(*f),
        VE::Literal(PV::Boolean(b)) => lit(*b),
        VE::Literal(PV::Null) => {
            datafusion::logical_expr::Expr::Literal(datafusion::scalar::ScalarValue::Null, None)
        }
        VE::Literal(PV::Variable(v)) => col(v.to_lowercase()),
        VE::Literal(PV::Parameter(name)) => {
            datafusion::logical_expr::placeholder(format!("${}", name.to_lowercase()))
        }
        VE::Literal(PV::Property(prop)) => {
            // Create qualified column name: variable__property (lowercase for case-insensitivity)
            col(qualify_column(&prop.variable, &prop.property))
        }
        VE::ScalarFunction { name, args } => {
            match name.to_lowercase().as_str() {
                "tolower" | "lower" => {
                    if args.len() == 1 {
                        let arg_expr = to_df_value_expr(&args[0]);
                        lower().call(vec![arg_expr])
                    } else {
                        // Invalid argument count - return NULL
                        Expr::Literal(datafusion::scalar::ScalarValue::Null, None)
                    }
                }
                "toupper" | "upper" => {
                    if args.len() == 1 {
                        let arg_expr = to_df_value_expr(&args[0]);
                        upper().call(vec![arg_expr])
                    } else {
                        // Invalid argument count - return NULL
                        Expr::Literal(datafusion::scalar::ScalarValue::Null, None)
                    }
                }
                "coalesce" => datafusion::functions::core::expr_fn::coalesce(
                    args.iter().map(to_df_value_expr).collect(),
                ),
                "__list" => super::cypher_udf::list(args.iter().map(to_df_value_expr).collect()),
                "tostring" => Expr::Cast(datafusion::logical_expr::Cast::new(
                    Box::new(to_df_value_expr(&args[0])),
                    arrow_schema::DataType::Utf8,
                )),
                "head" => datafusion::functions_nested::expr_fn::array_element(
                    to_df_value_expr(&args[0]),
                    lit(1_i64),
                ),
                "tointeger" => cast_to(to_df_value_expr(&args[0]), arrow_schema::DataType::Int64),
                "tofloat" => cast_to(to_df_value_expr(&args[0]), arrow_schema::DataType::Float64),
                "date" => cast_to(to_df_value_expr(&args[0]), arrow_schema::DataType::Date32),
                "datetime" => cast_to(
                    to_df_value_expr(&args[0]),
                    arrow_schema::DataType::Timestamp(
                        arrow_schema::TimeUnit::Nanosecond,
                        Some("UTC".into()),
                    ),
                ),
                // A duration between two timestamps, as the whole seconds it spans.
                "duration.inseconds" => datafusion::functions::core::expr_fn::named_struct(vec![
                    lit("seconds"),
                    (epoch_ms(to_df_value_expr(&args[1])) - epoch_ms(to_df_value_expr(&args[0])))
                        / lit(1000_i64),
                ]),
                // A path's length and node ids are columns of its variable (see shortest_path).
                "length" | "nodes" | "relationships" | "__path_node_records" => match &args[0] {
                    VE::Variable(path) => col(qualify_column(path, &name.to_lowercase())),
                    _ => Expr::Literal(datafusion::scalar::ScalarValue::Null, None),
                },
                "abs" | "round" | "floor" | "ceil" | "sqrt" => {
                    super::math_functions::call(name, args.iter().map(to_df_value_expr).collect())
                }
                "left" | "right" | "substring" | "replace" | "split" | "trim" | "ltrim"
                | "rtrim" | "size" | "reverse" | "last" | "tail" | "keys" | "__entity_keys"
                | "labels" | "startnode" | "endnode" | "__predicate_all" | "__predicate_any"
                | "__predicate_none" | "__predicate_single" => {
                    super::scalar_functions::call(name, args.iter().map(to_df_value_expr).collect())
                }
                "range" => {
                    let mut args: Vec<_> = args.iter().map(to_df_value_expr).collect();
                    if args.len() == 2 {
                        args.push(lit(1_i64));
                    }
                    super::scalar_functions::call(name, args)
                }
                // list[i] (0-based in Cypher, 1-based in DataFusion's array_element).
                "__index" => datafusion::functions_nested::expr_fn::array_element(
                    to_df_value_expr(&args[0]),
                    to_df_value_expr(&args[1]) + lit(1_i64),
                ),
                // date_part('month', d): the part name is a string literal, as in Kuzu and SQL.
                "date_part" => datafusion::functions::datetime::expr_fn::date_part(
                    to_df_value_expr(&args[0]),
                    to_df_value_expr(&args[1]),
                ),
                // Milliseconds since the Unix epoch, as Kuzu's to_epoch_ms: timestamps of any unit
                // are first cast to millisecond precision, then to their raw Int64 value.
                "to_epoch_ms" => epoch_ms(to_df_value_expr(&args[0])),
                _ => {
                    // Unknown scalar function - return NULL
                    Expr::Literal(datafusion::scalar::ScalarValue::Null, None)
                }
            }
        }
        VE::AggregateFunction {
            name,
            args,
            distinct,
        } => {
            match name.to_lowercase().as_str() {
                "count" => {
                    if args.len() == 1 {
                        let arg_expr = if let VE::Variable(v) = &args[0] {
                            if v == "*" {
                                // COUNT(*) - count all rows including NULLs
                                lit(1)
                            } else {
                                // COUNT(p) - count non-NULL rows by using a representative column
                                // Use <variable>__id as a null-sensitive column
                                // This ensures optional matches with NULL variables are not counted
                                //
                                // LIMITATION: This assumes all bindings expose a <var>__id column.
                                // This is true for ScanByLabel and Expand, but NOT for:
                                // - UNWIND-created variables (e.g., UNWIND [1,2,3] AS x)
                                // - WITH-projected aliases (e.g., WITH 1 AS x)
                                // Those cases will produce a runtime "column not found" error.
                                // TODO: Consider using COUNT(1) for non-node variables, or add
                                // semantic validation to reject COUNT(variable) for non-node types.
                                // Use qualify_column for consistent case-insensitive normalization
                                col(qualify_column(v, "id"))
                            }
                        } else {
                            // COUNT(p.property) - count non-null values of that property
                            to_df_value_expr(&args[0])
                        };

                        // Use DataFusion's count or count_distinct
                        if *distinct {
                            count_distinct(arg_expr)
                        } else {
                            count(arg_expr)
                        }
                    } else {
                        // Invalid argument count - return placeholder
                        lit(0)
                    }
                }
                "sum" => {
                    if args.len() == 1 {
                        let arg_expr = to_df_value_expr(&args[0]);
                        sum(arg_expr)
                    } else {
                        lit(0)
                    }
                }
                "avg" => {
                    if args.len() == 1 {
                        let arg_expr = to_df_value_expr(&args[0]);
                        avg(arg_expr)
                    } else {
                        lit(0)
                    }
                }
                "min" => {
                    if args.len() == 1 {
                        let arg_expr = to_df_value_expr(&args[0]);
                        min(arg_expr)
                    } else {
                        lit(0)
                    }
                }
                "max" => {
                    if args.len() == 1 {
                        let arg_expr = to_df_value_expr(&args[0]);
                        max(arg_expr)
                    } else {
                        lit(0)
                    }
                }
                "collect" => {
                    if args.len() == 1 {
                        // Cypher's collect skips nulls; DataFusion's array_agg keeps them.
                        use datafusion::logical_expr::ExprFunctionExt;
                        let arg_expr = to_df_value_expr(&args[0]);
                        // A map literal is never null; repeating it in a FILTER would make
                        // DataFusion extract it as a common expression, which loses the
                        // ordering of `collect` after `WITH ... ORDER BY` (LDBC IC7).
                        let agg = match &args[0] {
                            VE::Map(_) => array_agg(arg_expr).filter(lit(true)),
                            _ => array_agg(arg_expr.clone()).filter(arg_expr.is_not_null()),
                        };
                        let agg = if *distinct { agg.distinct() } else { agg };
                        agg.build().expect("FILTER and DISTINCT apply to array_agg")
                    } else {
                        lit(0)
                    }
                }
                _ => {
                    // Unsupported aggregate function - return NULL which coerces to any type
                    // This prevents type coercion errors in both string and numeric contexts
                    //
                    // TODO(#107): Now that semantic analysis rejects unknown functions, consider
                    // upgrading this to a hard internal error (e.g. `unreachable!()` or returning
                    // a planner/execution error) to catch validator regressions early.
                    Expr::Literal(datafusion::scalar::ScalarValue::Null, None)
                }
            }
        }
        VE::Case { whens, otherwise } => {
            let mut whens = whens.iter();
            let (cond, then) = whens.next().expect("the parser requires one WHEN");
            let mut case =
                datafusion::logical_expr::when(to_df_boolean_expr(cond), to_df_value_expr(then));
            for (cond, then) in whens {
                case = case.when(to_df_boolean_expr(cond), to_df_value_expr(then));
            }
            match otherwise {
                Some(e) => case.otherwise(to_df_value_expr(e)),
                None => case.end(),
            }
            .expect("a CASE with at least one WHEN builds")
        }
        VE::Arithmetic {
            left,
            operator,
            right,
        } => {
            use crate::ast::ArithmeticOperator as AO;
            let l = to_df_value_expr(left);
            let r = to_df_value_expr(right);
            let op = match operator {
                AO::Add => Operator::Plus,
                AO::Subtract => Operator::Minus,
                AO::Multiply => Operator::Multiply,
                AO::Divide => Operator::Divide,
                AO::Modulo => Operator::Modulo,
            };
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(l),
                op,
                right: Box::new(r),
            })
        }
        VE::VectorDistance {
            left,
            right,
            metric,
        } => {
            // Create UDF for vector distance computation
            let udf = udf::create_vector_distance_udf(metric);
            let left_expr = to_df_value_expr(left);
            let right_expr = to_df_value_expr(right);
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                udf,
                vec![left_expr, right_expr],
            ))
        }
        VE::VectorSimilarity {
            left,
            right,
            metric,
        } => {
            // Create UDF for vector similarity computation
            let udf = udf::create_vector_similarity_udf(metric);
            let left_expr = to_df_value_expr(left);
            let right_expr = to_df_value_expr(right);
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                udf,
                vec![left_expr, right_expr],
            ))
        }
        VE::VectorLiteral(values) => {
            // Convert Vec<f32> to DataFusion scalar FixedSizeList
            use arrow::array::{FixedSizeListArray, Float32Array};
            use arrow::datatypes::{DataType, Field};
            use datafusion::scalar::ScalarValue;
            use std::sync::Arc;

            let dim = values.len() as i32;
            let field = Arc::new(Field::new("item", DataType::Float32, true));
            let float_array = Arc::new(Float32Array::from(values.clone()));

            let list_array = FixedSizeListArray::try_new(field.clone(), dim, float_array, None)
                .expect("Failed to create FixedSizeListArray for vector literal");

            let scalar = ScalarValue::try_from_array(&list_array, 0)
                .expect("Failed to create scalar from array");

            lit(scalar)
        }
        VE::Parameter(name) => {
            datafusion::logical_expr::placeholder(format!("${}", name.to_lowercase()))
        }
        VE::Boolean(b) => to_df_boolean_expr(b),
        VE::Map(entries) if entries.is_empty() => lit(datafusion::common::ScalarValue::Struct(
            std::sync::Arc::new(arrow::array::StructArray::new_empty_fields(1, None)),
        )),
        VE::Map(entries) => datafusion::functions::core::expr_fn::named_struct(
            entries
                .iter()
                .flat_map(|(k, v)| [lit(k.clone()), to_df_value_expr(v)])
                .collect(),
        ),
        VE::Field { base, field } => super::cypher_udf::field(to_df_value_expr(base), field),
        VE::ListComprehension { .. } | VE::PatternComprehension { .. } => {
            unreachable!("the logical planner turns comprehensions into operators")
        }
    }
}

/// Milliseconds since the Unix epoch of a timestamp of any unit.
fn epoch_ms(expr: Expr) -> Expr {
    use arrow_schema::{DataType, TimeUnit};
    let ms = Expr::Cast(datafusion::logical_expr::Cast::new(
        Box::new(expr),
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
    ));
    Expr::Cast(datafusion::logical_expr::Cast::new(
        Box::new(ms),
        DataType::Int64,
    ))
}

fn cast_to(expr: Expr, data_type: arrow_schema::DataType) -> Expr {
    Expr::Cast(datafusion::logical_expr::Cast::new(
        Box::new(expr),
        data_type,
    ))
}

/// `expr` with names resolved against `schema`, the input it is evaluated on:
/// `x.y` where `x` is a value column (a map or date from WITH, not a node) becomes a field
/// access, and a node inside a map literal becomes a map of its property columns.
pub(crate) fn bind_value(
    expr: &ValueExpression,
    schema: &datafusion::common::DFSchema,
) -> ValueExpression {
    let has = |name: &str| schema.has_column_with_unqualified_name(name);
    let entity_properties = |var: &str| {
        let var = var.to_lowercase();
        let prefix = format!("{var}__");
        if has(&var) {
            return Vec::new();
        }
        schema
            .fields()
            .iter()
            .filter_map(|f| f.name().strip_prefix(&prefix))
            .map(|property| {
                (
                    property.to_string(),
                    ValueExpression::Property(crate::ast::PropertyRef {
                        variable: var.clone(),
                        property: property.to_string(),
                    }),
                )
            })
            .collect::<Vec<_>>()
    };
    let mut expr = expr.clone();
    crate::ast::transform_value(&mut expr, &mut |e| match e {
        ValueExpression::Property(p)
            if !has(&qualify_column(&p.variable, &p.property))
                && has(&p.variable.to_lowercase()) =>
        {
            *e = ValueExpression::Field {
                base: Box::new(ValueExpression::Variable(p.variable.clone())),
                field: p.property.clone(),
            };
        }
        ValueExpression::ScalarFunction { name, args }
            if matches!(
                name.to_lowercase().as_str(),
                "length" | "nodes" | "relationships" | "__path_node_records"
            ) =>
        {
            if let [base] = args.as_slice() {
                let value = match base {
                    ValueExpression::Variable(var) => has(&var.to_lowercase()),
                    _ => true,
                };
                if value {
                    *e = ValueExpression::Field {
                        base: Box::new(base.clone()),
                        field: if name == "__path_node_records" {
                            "node_records".into()
                        } else {
                            name.to_lowercase()
                        },
                    };
                }
            }
        }
        ValueExpression::ScalarFunction { name, args } if name.eq_ignore_ascii_case("keys") => {
            if let [ValueExpression::Variable(var)] = args.as_slice() {
                let properties = entity_properties(var);
                if !properties.is_empty() {
                    *e = ValueExpression::ScalarFunction {
                        name: "__entity_keys".to_string(),
                        args: vec![ValueExpression::Map(properties)],
                    };
                }
            }
        }
        ValueExpression::Map(entries) => {
            for (_, value) in entries.iter_mut() {
                let ValueExpression::Variable(var) = value else {
                    continue;
                };
                let properties = entity_properties(var);
                if !properties.is_empty() {
                    *value = ValueExpression::Map(properties);
                }
            }
        }
        _ => {}
    });
    expr
}

/// [`bind_value`] for every value in a predicate.
pub(crate) fn bind_boolean(
    expr: &BooleanExpression,
    schema: &datafusion::common::DFSchema,
) -> BooleanExpression {
    let mut expr = expr.clone();
    crate::ast::transform_boolean_values(&mut expr, &mut |v| *v = bind_value(v, schema));
    expr
}

/// Check if a ValueExpression contains an aggregate function
pub(crate) fn contains_aggregate(expr: &ValueExpression) -> bool {
    let mut found = false;
    crate::ast::transform_value(&mut expr.clone(), &mut |value| {
        found |= matches!(value, ValueExpression::AggregateFunction { .. });
    });
    found
}

/// This is used when no explicit alias is provided in RETURN clauses.
pub(crate) fn to_cypher_column_name(expr: &ValueExpression) -> String {
    use crate::ast::ValueExpression as VE;
    match expr {
        VE::Property(prop) => {
            // Convert to Cypher dot notation: variable.property
            format!("{}.{}", prop.variable, prop.property)
        }
        VE::Variable(v) => v.to_lowercase(),
        VE::Literal(PropertyValue::Property(prop)) => {
            // Handle nested property references
            format!("{}.{}", prop.variable, prop.property)
        }
        VE::ScalarFunction { name, args } | VE::AggregateFunction { name, args, .. } => {
            let distinct_str = if let VE::AggregateFunction { distinct: true, .. } = expr {
                "DISTINCT "
            } else {
                ""
            };

            if args.len() == 1 {
                let arg_repr = match &args[0] {
                    VE::Variable(v) => v.clone(),
                    VE::Property(prop) => format!("{}.{}", prop.variable, prop.property),
                    _ => "expr".to_string(),
                };
                format!("{}({}{})", name.to_lowercase(), distinct_str, arg_repr)
            } else if args.is_empty() {
                format!("{}()", name.to_lowercase())
            } else {
                // Multiple args - just use function name
                name.to_lowercase()
            }
        }
        VE::Field { base, field } => format!("{}.{}", to_cypher_column_name(base), field),
        _ => {
            // For other expressions (literals, arithmetic), use a generic name
            // In practice, these should always have explicit aliases
            "expr".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{BooleanExpression, PropertyRef, PropertyValue, ValueExpression};
    use datafusion::logical_expr::Expr;

    // ========================================================================
    // Unit tests for to_df_boolean_expr()
    // ========================================================================

    #[test]
    fn test_boolean_expr_comparison_equal() {
        use crate::ast::ComparisonOperator;
        let expr = BooleanExpression::Comparison {
            left: ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "age".into(),
            }),
            operator: ComparisonOperator::Equal,
            right: ValueExpression::Literal(PropertyValue::Integer(30)),
        };

        let df_expr = to_df_boolean_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(s.contains("p__age"), "Should contain qualified column");
        assert!(
            s.contains("30") || s.contains("Int64(30)"),
            "Should contain value"
        );
    }

    #[test]
    fn test_boolean_expr_comparison_operators() {
        use crate::ast::ComparisonOperator;

        let operators = vec![
            (ComparisonOperator::NotEqual, "!="),
            (ComparisonOperator::LessThan, "<"),
            (ComparisonOperator::LessThanOrEqual, "<="),
            (ComparisonOperator::GreaterThan, ">"),
            (ComparisonOperator::GreaterThanOrEqual, ">="),
        ];

        for (op, _op_str) in operators {
            let expr = BooleanExpression::Comparison {
                left: ValueExpression::Property(PropertyRef {
                    variable: "p".into(),
                    property: "age".into(),
                }),
                operator: op,
                right: ValueExpression::Literal(PropertyValue::Integer(30)),
            };

            let df_expr = to_df_boolean_expr(&expr);
            // Should successfully translate without panicking
            assert!(format!("{:?}", df_expr).contains("p__age"));
        }
    }

    #[test]
    fn test_boolean_expr_and() {
        use crate::ast::ComparisonOperator;
        let expr = BooleanExpression::And(
            Box::new(BooleanExpression::Comparison {
                left: ValueExpression::Property(PropertyRef {
                    variable: "p".into(),
                    property: "age".into(),
                }),
                operator: ComparisonOperator::GreaterThan,
                right: ValueExpression::Literal(PropertyValue::Integer(20)),
            }),
            Box::new(BooleanExpression::Comparison {
                left: ValueExpression::Property(PropertyRef {
                    variable: "p".into(),
                    property: "age".into(),
                }),
                operator: ComparisonOperator::LessThan,
                right: ValueExpression::Literal(PropertyValue::Integer(50)),
            }),
        );

        let df_expr = to_df_boolean_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(s.contains("And"), "Should contain AND operator");
        assert!(s.contains("p__age"), "Should contain column reference");
    }

    #[test]
    fn test_boolean_expr_or() {
        use crate::ast::ComparisonOperator;
        let expr = BooleanExpression::Or(
            Box::new(BooleanExpression::Comparison {
                left: ValueExpression::Property(PropertyRef {
                    variable: "p".into(),
                    property: "name".into(),
                }),
                operator: ComparisonOperator::Equal,
                right: ValueExpression::Literal(PropertyValue::String("Alice".into())),
            }),
            Box::new(BooleanExpression::Comparison {
                left: ValueExpression::Property(PropertyRef {
                    variable: "p".into(),
                    property: "name".into(),
                }),
                operator: ComparisonOperator::Equal,
                right: ValueExpression::Literal(PropertyValue::String("Bob".into())),
            }),
        );

        let df_expr = to_df_boolean_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(s.contains("Or"), "Should contain OR operator");
    }

    #[test]
    fn test_boolean_expr_not() {
        use crate::ast::ComparisonOperator;
        let expr = BooleanExpression::Not(Box::new(BooleanExpression::Comparison {
            left: ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "active".into(),
            }),
            operator: ComparisonOperator::Equal,
            right: ValueExpression::Literal(PropertyValue::Boolean(true)),
        }));

        let df_expr = to_df_boolean_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(s.contains("Not"), "Should contain NOT operator");
    }

    #[test]
    fn test_boolean_expr_exists() {
        let expr = BooleanExpression::Exists(PropertyRef {
            variable: "p".into(),
            property: "email".into(),
        });

        let df_expr = to_df_boolean_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(
            s.contains("IsNotNull") || s.contains("p__email"),
            "Should translate EXISTS to IsNotNull"
        );
    }

    #[test]
    fn test_boolean_expr_in_list() {
        let expr = BooleanExpression::In {
            expression: ValueExpression::Property(PropertyRef {
                variable: "rel".into(),
                property: "relationship_type".into(),
            }),
            list: vec![
                ValueExpression::Literal(PropertyValue::String("WORKS_FOR".into())),
                ValueExpression::Literal(PropertyValue::String("PART_OF".into())),
            ],
        };

        if let Expr::InList(in_list) = to_df_boolean_expr(&expr) {
            assert!(!in_list.negated);
            assert_eq!(in_list.list.len(), 2);
            match *in_list.expr {
                Expr::Column(ref col_expr) => {
                    assert_eq!(col_expr.name(), "rel__relationship_type");
                }
                other => panic!("Expected column expression, got {:?}", other),
            }
        } else {
            panic!("Expected InList expression");
        }
    }

    #[test]
    fn test_boolean_expr_like() {
        let expr = BooleanExpression::Like {
            expression: ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "name".into(),
            }),
            pattern: "A%".into(),
        };

        if let Expr::Like(like_expr) = to_df_boolean_expr(&expr) {
            assert!(!like_expr.negated, "Should not be negated");
            assert!(!like_expr.case_insensitive, "Should be case sensitive");
            assert_eq!(like_expr.escape_char, None, "Should have no escape char");
            match *like_expr.expr {
                Expr::Column(ref col_expr) => {
                    assert_eq!(col_expr.name(), "p__name");
                }
                other => panic!("Expected column expression, got {:?}", other),
            }
            // Check pattern is a literal
            match *like_expr.pattern {
                Expr::Literal(..) => {} // Success
                other => panic!("Expected literal pattern, got {:?}", other),
            }
        } else {
            panic!("Expected Like expression");
        }
    }

    #[test]
    fn test_boolean_expr_ilike() {
        let expr = BooleanExpression::ILike {
            expression: ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "name".into(),
            }),
            pattern: "alice%".into(),
        };

        if let Expr::Like(like_expr) = to_df_boolean_expr(&expr) {
            assert!(!like_expr.negated, "Should not be negated");
            assert!(
                like_expr.case_insensitive,
                "ILIKE should be case insensitive"
            );
            assert_eq!(like_expr.escape_char, None, "Should have no escape char");
            match *like_expr.expr {
                Expr::Column(ref col_expr) => {
                    assert_eq!(col_expr.name(), "p__name");
                }
                other => panic!("Expected column expression, got {:?}", other),
            }
            // Check pattern is a literal
            match *like_expr.pattern {
                Expr::Literal(ref scalar, _) => {
                    let s = format!("{:?}", scalar);
                    assert!(
                        s.contains("alice%"),
                        "Pattern should be 'alice%', got: {}",
                        s
                    );
                }
                other => panic!("Expected literal pattern, got {:?}", other),
            }
        } else {
            panic!("Expected Like expression");
        }
    }

    #[test]
    fn test_boolean_expr_like_vs_ilike_case_sensitivity() {
        // Test LIKE (case-sensitive)
        let like_expr = BooleanExpression::Like {
            expression: ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "name".into(),
            }),
            pattern: "Test%".into(),
        };

        if let Expr::Like(like) = to_df_boolean_expr(&like_expr) {
            assert!(
                !like.case_insensitive,
                "LIKE should be case-sensitive (case_insensitive = false)"
            );
        } else {
            panic!("Expected Like expression");
        }

        // Test ILIKE (case-insensitive)
        let ilike_expr = BooleanExpression::ILike {
            expression: ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "name".into(),
            }),
            pattern: "Test%".into(),
        };

        if let Expr::Like(ilike) = to_df_boolean_expr(&ilike_expr) {
            assert!(
                ilike.case_insensitive,
                "ILIKE should be case-insensitive (case_insensitive = true)"
            );
        } else {
            panic!("Expected Like expression");
        }
    }

    #[test]
    fn test_boolean_expr_like_with_wildcard() {
        let expr = BooleanExpression::Like {
            expression: ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "email".into(),
            }),
            pattern: "%@example.com".into(),
        };

        let df_expr = to_df_boolean_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(
            s.contains("Like") || s.contains("like"),
            "Should be a LIKE expression"
        );
        assert!(s.contains("p__email"), "Should contain column reference");
    }

    #[test]
    fn test_boolean_expr_contains() {
        let expr = BooleanExpression::Contains {
            expression: ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "name".into(),
            }),
            substring: "ali".into(),
        };

        if let Expr::Like(like_expr) = to_df_boolean_expr(&expr) {
            assert!(!like_expr.negated, "Should not be negated");
            assert!(!like_expr.case_insensitive, "Should be case sensitive");
            assert_eq!(like_expr.escape_char, None, "Should have no escape char");

            // Check the expression is the column
            match *like_expr.expr {
                Expr::Column(ref col_expr) => {
                    assert_eq!(col_expr.name(), "p__name");
                }
                other => panic!("Expected column expression, got {:?}", other),
            }

            // Check pattern is '%ali%'
            match *like_expr.pattern {
                Expr::Literal(ref scalar, _) => {
                    let s = format!("{:?}", scalar);
                    assert!(s.contains("%ali%"), "Pattern should be '%ali%', got: {}", s);
                }
                other => panic!("Expected literal pattern, got {:?}", other),
            }
        } else {
            panic!("Expected Like expression");
        }
    }

    #[test]
    fn test_boolean_expr_starts_with() {
        let expr = BooleanExpression::StartsWith {
            expression: ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "email".into(),
            }),
            prefix: "admin".into(),
        };

        if let Expr::Like(like_expr) = to_df_boolean_expr(&expr) {
            assert!(!like_expr.negated, "Should not be negated");
            assert!(!like_expr.case_insensitive, "Should be case sensitive");

            // Check the expression is the column
            match *like_expr.expr {
                Expr::Column(ref col_expr) => {
                    assert_eq!(col_expr.name(), "p__email");
                }
                other => panic!("Expected column expression, got {:?}", other),
            }

            // Check pattern is 'admin%'
            match *like_expr.pattern {
                Expr::Literal(ref scalar, _) => {
                    let s = format!("{:?}", scalar);
                    assert!(
                        s.contains("admin%"),
                        "Pattern should be 'admin%', got: {}",
                        s
                    );
                }
                other => panic!("Expected literal pattern, got {:?}", other),
            }
        } else {
            panic!("Expected Like expression");
        }
    }

    #[test]
    fn test_boolean_expr_ends_with() {
        let expr = BooleanExpression::EndsWith {
            expression: ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "email".into(),
            }),
            suffix: "@example.com".into(),
        };

        if let Expr::Like(like_expr) = to_df_boolean_expr(&expr) {
            assert!(!like_expr.negated, "Should not be negated");
            assert!(!like_expr.case_insensitive, "Should be case sensitive");

            // Check the expression is the column
            match *like_expr.expr {
                Expr::Column(ref col_expr) => {
                    assert_eq!(col_expr.name(), "p__email");
                }
                other => panic!("Expected column expression, got {:?}", other),
            }

            // Check pattern is '%@example.com'
            match *like_expr.pattern {
                Expr::Literal(ref scalar, _) => {
                    let s = format!("{:?}", scalar);
                    assert!(
                        s.contains("%@example.com"),
                        "Pattern should be '%@example.com', got: {}",
                        s
                    );
                }
                other => panic!("Expected literal pattern, got {:?}", other),
            }
        } else {
            panic!("Expected Like expression");
        }
    }

    #[test]
    fn test_boolean_expr_contains_case_sensitivity() {
        // Test that CONTAINS is case-sensitive (case_insensitive = false)
        let expr = BooleanExpression::Contains {
            expression: ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "name".into(),
            }),
            substring: "Test".into(),
        };

        if let Expr::Like(like_expr) = to_df_boolean_expr(&expr) {
            assert!(
                !like_expr.case_insensitive,
                "CONTAINS should be case-sensitive by default"
            );
        } else {
            panic!("Expected Like expression");
        }
    }

    #[test]
    fn test_boolean_expr_string_operators_with_variable() {
        // Test that string operators work with variable references, not just properties
        let expr = BooleanExpression::Contains {
            expression: ValueExpression::Variable("name".into()),
            substring: "test".into(),
        };

        if let Expr::Like(like_expr) = to_df_boolean_expr(&expr) {
            match *like_expr.expr {
                Expr::Column(ref col_expr) => {
                    assert_eq!(
                        col_expr.name(),
                        "name",
                        "Should reference variable directly"
                    );
                }
                other => panic!("Expected column expression, got {:?}", other),
            }
        } else {
            panic!("Expected Like expression");
        }
    }

    // ========================================================================
    // Unit tests for to_df_value_expr()
    // ========================================================================

    #[test]
    fn test_value_expr_property() {
        let expr = ValueExpression::Property(PropertyRef {
            variable: "person".into(),
            property: "name".into(),
        });

        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert_eq!(
            s,
            "Column(Column { relation: None, name: \"person__name\" })"
        );
    }

    #[test]
    fn test_value_expr_literal_integer() {
        let expr = ValueExpression::Literal(PropertyValue::Integer(42));
        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(s.contains("42") || s.contains("Int64(42)"));
    }

    #[test]
    fn test_value_expr_literal_float() {
        let expr = ValueExpression::Literal(PropertyValue::Float(std::f64::consts::PI));
        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(s.contains("3.14") || s.contains("Float64"));
    }

    #[test]
    fn test_value_expr_literal_string() {
        let expr = ValueExpression::Literal(PropertyValue::String("hello".into()));
        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(s.contains("hello") || s.contains("Utf8"));
    }

    #[test]
    fn test_value_expr_literal_boolean() {
        let expr = ValueExpression::Literal(PropertyValue::Boolean(true));
        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(s.contains("true") || s.contains("Boolean"));
    }

    #[test]
    fn test_value_expr_literal_null() {
        let expr = ValueExpression::Literal(PropertyValue::Null);
        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        // Null literals are translated to Literal with Null value
        assert!(s.contains("Literal"), "Should be a Literal expression");
    }

    #[test]
    fn test_value_expr_arithmetic_add() {
        use crate::ast::ArithmeticOperator;
        let expr = ValueExpression::Arithmetic {
            left: Box::new(ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "age".into(),
            })),
            operator: ArithmeticOperator::Add,
            right: Box::new(ValueExpression::Literal(PropertyValue::Integer(5))),
        };

        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        // Arithmetic expressions should now return a BinaryExpr with Plus operator
        assert!(s.contains("BinaryExpr"), "Should be a BinaryExpr");
        assert!(s.contains("Plus"), "Should contain Plus operator");
        assert!(
            s.contains("p__age"),
            "Should contain the left operand column"
        );
    }

    #[test]
    fn test_value_expr_arithmetic_operators() {
        use crate::ast::ArithmeticOperator;

        let operators = vec![
            (ArithmeticOperator::Add, "Plus"),
            (ArithmeticOperator::Subtract, "Minus"),
            (ArithmeticOperator::Multiply, "Multiply"),
            (ArithmeticOperator::Divide, "Divide"),
        ];

        for (op, expected_op_str) in operators {
            let expr = ValueExpression::Arithmetic {
                left: Box::new(ValueExpression::Literal(PropertyValue::Integer(10))),
                operator: op,
                right: Box::new(ValueExpression::Literal(PropertyValue::Integer(2))),
            };

            let df_expr = to_df_value_expr(&expr);
            let s = format!("{:?}", df_expr);
            // Should translate to BinaryExpr with the correct operator
            assert!(
                s.contains("BinaryExpr"),
                "Arithmetic should produce BinaryExpr"
            );
            assert!(
                s.contains(expected_op_str),
                "Expected operator {} in expression: {}",
                expected_op_str,
                s
            );
        }
    }

    #[test]
    fn test_value_expr_function_count_star() {
        let expr = ValueExpression::AggregateFunction {
            name: "COUNT".into(),
            args: vec![ValueExpression::Literal(PropertyValue::String("*".into()))],
            distinct: false,
        };

        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(
            s.contains("count") || s.contains("Count"),
            "Should be COUNT function"
        );
    }

    #[test]
    fn test_value_expr_function_count_property() {
        let expr = ValueExpression::AggregateFunction {
            name: "COUNT".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "id".into(),
            })],
            distinct: false,
        };

        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(
            s.contains("count") || s.contains("Count"),
            "Should be COUNT function"
        );
        assert!(s.contains("p__id"), "Should contain column reference");
    }

    #[test]
    fn test_value_expr_function_sum() {
        let expr = ValueExpression::AggregateFunction {
            name: "SUM".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "amount".into(),
            })],
            distinct: false,
        };

        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(
            s.contains("sum") || s.contains("Sum"),
            "Should be SUM function"
        );
        assert!(s.contains("p__amount"), "Should contain column reference");
    }

    #[test]
    fn test_value_expr_function_avg() {
        let expr = ValueExpression::AggregateFunction {
            name: "AVG".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "amount".into(),
            })],
            distinct: false,
        };

        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(
            s.contains("avg") || s.contains("Avg"),
            "Should be AVG function"
        );
        assert!(s.contains("p__amount"), "Should contain column reference");
    }

    #[test]
    fn test_value_expr_function_min() {
        let expr = ValueExpression::AggregateFunction {
            name: "MIN".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "amount".into(),
            })],
            distinct: false,
        };

        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(
            s.contains("min") || s.contains("Min"),
            "Should be MIN function"
        );
        assert!(s.contains("p__amount"), "Should contain column reference");
    }

    #[test]
    fn test_value_expr_function_max() {
        let expr = ValueExpression::AggregateFunction {
            name: "MAX".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "amount".into(),
            })],
            distinct: false,
        };

        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(
            s.contains("max") || s.contains("Max"),
            "Should be MAX function"
        );
        assert!(s.contains("p__amount"), "Should contain column reference");
    }

    #[test]
    fn test_value_expr_function_tolower() {
        let expr = ValueExpression::ScalarFunction {
            name: "toLower".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "name".into(),
            })],
        };

        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        // Should be a ScalarFunction with lower
        assert!(
            s.contains("lower") || s.contains("Lower"),
            "Should use lower function, got: {}",
            s
        );
        assert!(s.contains("p__name"), "Should contain column reference");
    }

    #[test]
    fn test_value_expr_function_toupper() {
        let expr = ValueExpression::ScalarFunction {
            name: "toUpper".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "name".into(),
            })],
        };

        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        // Should be a ScalarFunction with upper
        assert!(
            s.contains("upper") || s.contains("Upper"),
            "Should use upper function, got: {}",
            s
        );
        assert!(s.contains("p__name"), "Should contain column reference");
    }

    #[test]
    fn test_value_expr_function_lower_alias() {
        // Test that 'lower' also works (SQL-style alias)
        let expr = ValueExpression::ScalarFunction {
            name: "lower".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "name".into(),
            })],
        };

        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(
            s.contains("lower") || s.contains("Lower"),
            "Should use lower function, got: {}",
            s
        );
    }

    #[test]
    fn test_value_expr_function_upper_alias() {
        // Test that 'upper' also works (SQL-style alias)
        let expr = ValueExpression::ScalarFunction {
            name: "upper".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "name".into(),
            })],
        };

        let df_expr = to_df_value_expr(&expr);
        let s = format!("{:?}", df_expr);
        assert!(
            s.contains("upper") || s.contains("Upper"),
            "Should use upper function, got: {}",
            s
        );
    }

    #[test]
    fn test_tolower_with_contains_produces_valid_like() {
        // This is the bug scenario: toLower(s.name) CONTAINS 'offer'
        // Previously returned lit(0) which caused type coercion error
        let tolower_expr = ValueExpression::ScalarFunction {
            name: "toLower".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "s".into(),
                property: "name".into(),
            })],
        };

        let contains_expr = BooleanExpression::Contains {
            expression: tolower_expr,
            substring: "offer".into(),
        };

        let df_expr = to_df_boolean_expr(&contains_expr);
        let s = format!("{:?}", df_expr);

        // Should be a Like expression with lower() on the column, not lit(0)
        assert!(
            s.contains("Like"),
            "Should be a LIKE expression, got: {}",
            s
        );
        assert!(
            s.contains("lower") || s.contains("Lower"),
            "Should contain lower function, got: {}",
            s
        );
        assert!(
            !s.contains("Int32(0)") && !s.contains("Int64(0)") && !s.contains("Utf8(\"\")"),
            "Should NOT contain literal 0 or empty string (placeholder bugs), got: {}",
            s
        );
    }

    // ========================================================================
    // Unit tests for contains_aggregate()
    // ========================================================================

    #[test]
    fn test_contains_aggregate_count() {
        let expr = ValueExpression::AggregateFunction {
            name: "COUNT".into(),
            args: vec![ValueExpression::Literal(PropertyValue::String("*".into()))],
            distinct: false,
        };

        assert!(
            contains_aggregate(&expr),
            "COUNT should be detected as aggregate"
        );
    }

    #[test]
    fn test_contains_aggregate_sum() {
        let expr = ValueExpression::AggregateFunction {
            name: "SUM".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "value".into(),
            })],
            distinct: false,
        };

        assert!(
            contains_aggregate(&expr),
            "SUM should be detected as aggregate"
        );
    }

    #[test]
    fn test_contains_aggregate_min() {
        let expr = ValueExpression::AggregateFunction {
            name: "MIN".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "value".into(),
            })],
            distinct: false,
        };

        assert!(
            contains_aggregate(&expr),
            "MIN should be detected as aggregate"
        );
    }

    #[test]
    fn test_contains_aggregate_max() {
        let expr = ValueExpression::AggregateFunction {
            name: "MAX".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "value".into(),
            })],
            distinct: false,
        };

        assert!(
            contains_aggregate(&expr),
            "MAX should be detected as aggregate"
        );
    }

    #[test]
    fn test_contains_aggregate_property() {
        let expr = ValueExpression::Property(PropertyRef {
            variable: "p".into(),
            property: "name".into(),
        });

        assert!(
            !contains_aggregate(&expr),
            "Property should not be aggregate"
        );
    }

    #[test]
    fn test_contains_aggregate_literal() {
        let expr = ValueExpression::Literal(PropertyValue::Integer(42));
        assert!(
            !contains_aggregate(&expr),
            "Literal should not be aggregate"
        );
    }

    #[test]
    fn test_contains_aggregate_arithmetic_with_aggregate() {
        use crate::ast::ArithmeticOperator;
        let expr = ValueExpression::Arithmetic {
            left: Box::new(ValueExpression::AggregateFunction {
                name: "COUNT".into(),
                args: vec![ValueExpression::Literal(PropertyValue::String("*".into()))],
                distinct: false,
            }),
            operator: ArithmeticOperator::Multiply,
            right: Box::new(ValueExpression::Literal(PropertyValue::Integer(2))),
        };

        assert!(
            contains_aggregate(&expr),
            "Arithmetic with COUNT should be detected as aggregate"
        );
    }

    #[test]
    fn test_contains_aggregate_arithmetic_without_aggregate() {
        use crate::ast::ArithmeticOperator;
        let expr = ValueExpression::Arithmetic {
            left: Box::new(ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "age".into(),
            })),
            operator: ArithmeticOperator::Add,
            right: Box::new(ValueExpression::Literal(PropertyValue::Integer(5))),
        };

        assert!(
            !contains_aggregate(&expr),
            "Arithmetic without aggregates should not be detected as aggregate"
        );
    }

    #[test]
    fn test_contains_aggregate_nested_function() {
        let expr = ValueExpression::ScalarFunction {
            name: "UPPER".into(),
            args: vec![ValueExpression::AggregateFunction {
                name: "COUNT".into(),
                args: vec![ValueExpression::Literal(PropertyValue::String("*".into()))],
                distinct: false,
            }],
        };

        assert!(
            contains_aggregate(&expr),
            "Nested function with COUNT should be detected as aggregate"
        );
    }

    // ========================================================================
    // Unit tests for to_cypher_column_name()
    // ========================================================================

    #[test]
    fn test_cypher_column_name_property() {
        let expr = ValueExpression::Property(PropertyRef {
            variable: "person".into(),
            property: "name".into(),
        });

        let name = to_cypher_column_name(&expr);
        assert_eq!(name, "person.name", "Should convert to Cypher dot notation");
    }

    #[test]
    fn test_cypher_column_name_function_count_star() {
        let expr = ValueExpression::AggregateFunction {
            name: "COUNT".into(),
            args: vec![ValueExpression::Literal(PropertyValue::String("*".into()))],
            distinct: false,
        };

        let name = to_cypher_column_name(&expr);
        // Literal arguments (including "*") are converted to "expr" by the implementation
        // This is expected behavior since we can't distinguish "*" from other literals
        assert_eq!(
            name, "count(expr)",
            "COUNT with literal arg should be 'count(expr)'"
        );
    }

    #[test]
    fn test_cypher_column_name_function_count_property() {
        let expr = ValueExpression::AggregateFunction {
            name: "COUNT".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "id".into(),
            })],
            distinct: false,
        };

        let name = to_cypher_column_name(&expr);
        assert_eq!(name, "count(p.id)", "Should format as count(p.id)");
    }

    #[test]
    fn test_cypher_column_name_function_sum() {
        let expr = ValueExpression::AggregateFunction {
            name: "SUM".into(),
            args: vec![ValueExpression::Property(PropertyRef {
                variable: "order".into(),
                property: "amount".into(),
            })],
            distinct: false,
        };

        let name = to_cypher_column_name(&expr);
        assert_eq!(
            name, "sum(order.amount)",
            "Should format as sum(order.amount)"
        );
    }

    #[test]
    fn test_cypher_column_name_literal() {
        let expr = ValueExpression::Literal(PropertyValue::Integer(42));
        let name = to_cypher_column_name(&expr);
        assert_eq!(name, "expr", "Literals should use generic name");
    }

    #[test]
    fn test_cypher_column_name_arithmetic() {
        use crate::ast::ArithmeticOperator;
        let expr = ValueExpression::Arithmetic {
            left: Box::new(ValueExpression::Property(PropertyRef {
                variable: "p".into(),
                property: "age".into(),
            })),
            operator: ArithmeticOperator::Add,
            right: Box::new(ValueExpression::Literal(PropertyValue::Integer(5))),
        };

        let name = to_cypher_column_name(&expr);
        assert_eq!(name, "expr", "Arithmetic should use generic name");
    }
}
