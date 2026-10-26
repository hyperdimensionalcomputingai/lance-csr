use crate::ast::*;
use crate::error::{GraphError, Result};
use std::collections::HashMap;

/// Substitute parameters with literal values in the AST
pub fn substitute_parameters(
    query: &mut CypherQuery,
    parameters: &HashMap<String, serde_json::Value>,
) -> Result<()> {
    for branch in &mut query.union_branches {
        substitute_parameters(&mut branch.query, parameters)?;
    }
    substitute_row_count(&mut query.skip, parameters)?;
    substitute_row_count(&mut query.limit, parameters)?;

    // Substitute in READING clauses
    for reading_clause in &mut query.reading_clauses {
        substitute_in_reading_clause(reading_clause, parameters)?;
    }

    // Substitute in WHERE clause
    if let Some(where_clause) = &mut query.where_clause {
        substitute_in_where_clause(where_clause, parameters)?;
    }

    // Substitute in WITH clause
    if let Some(with_clause) = &mut query.with_clause {
        substitute_in_with_clause(with_clause, parameters)?;
    }

    // Substitute in post-WITH READING clauses
    for reading_clause in &mut query.post_with_reading_clauses {
        substitute_in_reading_clause(reading_clause, parameters)?;
    }

    // Substitute in post-WITH WHERE clause
    if let Some(post_where) = &mut query.post_with_where_clause {
        substitute_in_where_clause(post_where, parameters)?;
    }

    for stage in &mut query.further_with_stages {
        substitute_in_with_clause(&mut stage.with_clause, parameters)?;
        for reading_clause in &mut stage.reading_clauses {
            substitute_in_reading_clause(reading_clause, parameters)?;
        }
        if let Some(where_clause) = &mut stage.where_clause {
            substitute_in_where_clause(where_clause, parameters)?;
        }
    }

    // Substitute in RETURN clause
    substitute_in_return_clause(&mut query.return_clause, parameters)?;

    // Substitute in ORDER BY clause
    if let Some(order_by) = &mut query.order_by {
        substitute_in_order_by_clause(order_by, parameters)?;
    }

    Ok(())
}

fn substitute_row_count(
    count: &mut Option<RowCount>,
    parameters: &HashMap<String, serde_json::Value>,
) -> Result<()> {
    if let Some(RowCount::Parameter(name)) = count {
        let value = parameters.get(&name.to_lowercase()).and_then(serde_json::Value::as_u64)
            .filter(|v| usize::try_from(*v).is_ok())
            .ok_or_else(|| GraphError::PlanError {
                message: format!("Pagination parameter ${name} must be a supplied nonnegative integer fitting the platform row count"),
                location: snafu::Location::new(file!(), line!(), column!()),
            })?;
        *count = Some(RowCount::Literal(value));
    }
    Ok(())
}

fn substitute_in_reading_clause(
    clause: &mut ReadingClause,
    parameters: &HashMap<String, serde_json::Value>,
) -> Result<()> {
    match clause {
        ReadingClause::Match(match_clause) => {
            for pattern in &mut match_clause.patterns {
                substitute_in_graph_pattern(pattern, parameters)?;
            }
            if let Some(where_clause) = &mut match_clause.where_clause {
                substitute_in_where_clause(where_clause, parameters)?;
            }
        }
        ReadingClause::Unwind(unwind_clause) => {
            substitute_in_value_expression(&mut unwind_clause.expression, parameters)?;
        }
    }
    Ok(())
}

fn substitute_in_graph_pattern(
    pattern: &mut GraphPattern,
    parameters: &HashMap<String, serde_json::Value>,
) -> Result<()> {
    match pattern {
        GraphPattern::Node(node) => {
            for value in node.properties.values_mut() {
                substitute_in_property_value(value, parameters)?;
            }
        }
        GraphPattern::Path(path) => {
            substitute_in_node_pattern(&mut path.start_node, parameters)?;
            for segment in &mut path.segments {
                substitute_in_relationship_pattern(&mut segment.relationship, parameters)?;
                substitute_in_node_pattern(&mut segment.end_node, parameters)?;
            }
        }
    }
    Ok(())
}

fn substitute_in_node_pattern(
    node: &mut NodePattern,
    parameters: &HashMap<String, serde_json::Value>,
) -> Result<()> {
    for value in node.properties.values_mut() {
        substitute_in_property_value(value, parameters)?;
    }
    Ok(())
}

fn substitute_in_relationship_pattern(
    rel: &mut RelationshipPattern,
    parameters: &HashMap<String, serde_json::Value>,
) -> Result<()> {
    for value in rel.properties.values_mut() {
        substitute_in_property_value(value, parameters)?;
    }
    Ok(())
}

fn substitute_in_property_value(
    value: &mut PropertyValue,
    parameters: &HashMap<String, serde_json::Value>,
) -> Result<()> {
    if let PropertyValue::Parameter(name) = value {
        let param_value =
            parameters
                .get(&name.to_lowercase())
                .ok_or_else(|| GraphError::PlanError {
                    message: format!("Missing parameter: ${}", name),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })?;

        *value = json_to_property_value(param_value)?;
    }
    Ok(())
}

fn substitute_in_where_clause(
    where_clause: &mut WhereClause,
    parameters: &HashMap<String, serde_json::Value>,
) -> Result<()> {
    substitute_in_boolean_expression(&mut where_clause.expression, parameters)
}

fn substitute_in_with_clause(
    with_clause: &mut WithClause,
    parameters: &HashMap<String, serde_json::Value>,
) -> Result<()> {
    substitute_row_count(&mut with_clause.limit, parameters)?;
    for item in &mut with_clause.items {
        substitute_in_value_expression(&mut item.expression, parameters)?;
    }
    if let Some(order_by) = &mut with_clause.order_by {
        substitute_in_order_by_clause(order_by, parameters)?;
    }
    if let Some(where_clause) = &mut with_clause.where_clause {
        substitute_in_where_clause(where_clause, parameters)?;
    }
    Ok(())
}

fn substitute_in_return_clause(
    return_clause: &mut ReturnClause,
    parameters: &HashMap<String, serde_json::Value>,
) -> Result<()> {
    for item in &mut return_clause.items {
        substitute_in_value_expression(&mut item.expression, parameters)?;
    }
    Ok(())
}

fn substitute_in_order_by_clause(
    order_by: &mut OrderByClause,
    parameters: &HashMap<String, serde_json::Value>,
) -> Result<()> {
    for item in &mut order_by.items {
        substitute_in_value_expression(&mut item.expression, parameters)?;
    }
    Ok(())
}

fn substitute_in_boolean_expression(
    expr: &mut BooleanExpression,
    parameters: &HashMap<String, serde_json::Value>,
) -> Result<()> {
    match expr {
        BooleanExpression::Comparison { left, right, .. } => {
            substitute_in_value_expression(left, parameters)?;
            substitute_in_value_expression(right, parameters)?;
        }
        BooleanExpression::And(left, right) | BooleanExpression::Or(left, right) => {
            substitute_in_boolean_expression(left, parameters)?;
            substitute_in_boolean_expression(right, parameters)?;
        }
        BooleanExpression::Not(inner) => {
            substitute_in_boolean_expression(inner, parameters)?;
        }
        BooleanExpression::Exists(_) => {}
        BooleanExpression::PatternExists {
            patterns,
            where_clause,
        } => {
            for pattern in patterns {
                substitute_in_graph_pattern(pattern, parameters)?;
            }
            if let Some(w) = where_clause {
                substitute_in_boolean_expression(w, parameters)?;
            }
        }
        BooleanExpression::In { expression, list } => {
            substitute_in_value_expression(expression, parameters)?;
            for item in list {
                substitute_in_value_expression(item, parameters)?;
            }
        }
        BooleanExpression::Member { expression, list } => {
            substitute_in_value_expression(expression, parameters)?;
            substitute_in_value_expression(list, parameters)?;
        }
        BooleanExpression::Like { expression, .. }
        | BooleanExpression::ILike { expression, .. }
        | BooleanExpression::Contains { expression, .. }
        | BooleanExpression::StartsWith { expression, .. }
        | BooleanExpression::EndsWith { expression, .. }
        | BooleanExpression::IsNull(expression)
        | BooleanExpression::IsNotNull(expression) => {
            substitute_in_value_expression(expression, parameters)?;
        }
    }
    Ok(())
}

fn substitute_in_value_expression(
    expr: &mut ValueExpression,
    parameters: &HashMap<String, serde_json::Value>,
) -> Result<()> {
    match expr {
        ValueExpression::Parameter(name) => {
            let param_value =
                parameters
                    .get(&name.to_lowercase())
                    .ok_or_else(|| GraphError::PlanError {
                        message: format!("Missing parameter: ${}", name),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    })?;

            *expr = json_to_expression(param_value)?;
        }
        ValueExpression::ScalarFunction { args, .. }
        | ValueExpression::AggregateFunction { args, .. } => {
            for arg in args {
                substitute_in_value_expression(arg, parameters)?;
            }
        }
        ValueExpression::Arithmetic { left, right, .. } => {
            substitute_in_value_expression(left, parameters)?;
            substitute_in_value_expression(right, parameters)?;
        }
        ValueExpression::VectorDistance { left, right, .. }
        | ValueExpression::VectorSimilarity { left, right, .. } => {
            substitute_in_value_expression(left, parameters)?;
            substitute_in_value_expression(right, parameters)?;
        }
        ValueExpression::Case { whens, otherwise } => {
            for (condition, then) in whens {
                substitute_in_boolean_expression(condition, parameters)?;
                substitute_in_value_expression(then, parameters)?;
            }
            if let Some(otherwise) = otherwise {
                substitute_in_value_expression(otherwise, parameters)?;
            }
        }
        ValueExpression::Boolean(b) => substitute_in_boolean_expression(b, parameters)?,
        ValueExpression::Map(entries) => {
            for (_, v) in entries {
                substitute_in_value_expression(v, parameters)?;
            }
        }
        ValueExpression::Field { base, .. } => substitute_in_value_expression(base, parameters)?,
        ValueExpression::PatternComprehension { filter, map, .. } => {
            if let Some(b) = filter {
                substitute_in_boolean_expression(b, parameters)?;
            }
            substitute_in_value_expression(map, parameters)?;
        }
        ValueExpression::ListComprehension {
            list, filter, map, ..
        } => {
            substitute_in_value_expression(list, parameters)?;
            if let Some(b) = filter {
                substitute_in_boolean_expression(b, parameters)?;
            }
            if let Some(m) = map {
                substitute_in_value_expression(m, parameters)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Keep container parameters in the same representation as literal lists and maps.
fn json_to_expression(value: &serde_json::Value) -> Result<ValueExpression> {
    match value {
        serde_json::Value::Array(values) => Ok(ValueExpression::ScalarFunction {
            name: "__list".to_string(),
            args: values
                .iter()
                .map(json_to_expression)
                .collect::<Result<_>>()?,
        }),
        serde_json::Value::Object(values) => Ok(ValueExpression::Map(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), json_to_expression(value)?)))
                .collect::<Result<_>>()?,
        )),
        _ => Ok(ValueExpression::Literal(json_to_property_value(value)?)),
    }
}

fn json_to_property_value(value: &serde_json::Value) -> Result<PropertyValue> {
    match value {
        serde_json::Value::Null => Ok(PropertyValue::Null),
        serde_json::Value::Bool(b) => Ok(PropertyValue::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(PropertyValue::Integer(i))
            } else if n.is_u64() {
                Err(GraphError::PlanError {
                    message: "Integer parameters must fit signed 64-bit values".to_string(),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })
            } else if let Some(f) = n.as_f64() {
                Ok(PropertyValue::Float(f))
            } else {
                Err(GraphError::PlanError {
                    message: format!(
                        "Number parameter could not be converted to i64 or f64: {}",
                        n
                    ),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })
            }
        }
        serde_json::Value::String(s) => Ok(PropertyValue::String(s.clone())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Err(GraphError::PlanError {
            message:
                "List and map parameters must be used as expressions, not inline property values."
                    .to_string(),
            location: snafu::Location::new(file!(), line!(), column!()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn pagination_requires_nonnegative_integral_parameters() {
        let mut query = crate::parser::parse_cypher_query("RETURN 1 LIMIT $n").unwrap();
        substitute_parameters(&mut query, &HashMap::from([("n".to_string(), json!(0))])).unwrap();
        assert_eq!(query.limit, Some(RowCount::Literal(0)));
        for value in [
            json!(null),
            json!(-1),
            json!(1.5),
            json!(true),
            json!("1"),
            json!([]),
        ] {
            let mut query = crate::parser::parse_cypher_query("RETURN 1 LIMIT $n").unwrap();
            assert!(
                substitute_parameters(&mut query, &HashMap::from([("n".to_string(), value)]))
                    .is_err()
            );
        }
    }

    #[test]
    fn container_expressions_preserve_ids_and_nesting() {
        let list = json_to_expression(&json!([19791209300775_i64, null])).unwrap();
        assert_eq!(
            list,
            ValueExpression::ScalarFunction {
                name: "__list".to_string(),
                args: vec![
                    ValueExpression::Literal(PropertyValue::Integer(19791209300775)),
                    ValueExpression::Literal(PropertyValue::Null)
                ],
            }
        );
        let map = json_to_expression(&json!({"ids": [19791209300775_i64]})).unwrap();
        assert!(
            matches!(map, ValueExpression::Map(entries) if entries.len() == 1 && entries[0].0 == "ids")
        );
        assert!(json_to_expression(&json!([])).is_ok());
        assert!(json_to_expression(&json!({})).is_ok());
        assert!(json_to_expression(&json!(u64::MAX)).is_err());
    }
}
