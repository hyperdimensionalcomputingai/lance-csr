// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! Conservative expression checks against the current catalog and lexical scope.

use super::*;
use arrow_schema::{DataType, TimeUnit};
use datafusion_expr::binary::BinaryTypeCoercer;
use datafusion_expr::Operator;

#[derive(Clone, Debug, PartialEq)]
pub(super) enum ExpressionType {
    Unknown,
    Scalar(DataType),
    Entity(VariableType, Vec<String>),
    List(Box<Self>),
    Map(Vec<(String, Self)>),
}

impl ExpressionType {
    pub(super) fn element(self) -> Self {
        match self {
            Self::List(element) => *element,
            Self::Scalar(
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _),
            ) => Self::Scalar(field.data_type().clone()),
            _ => Self::Unknown,
        }
    }

    fn known(&self) -> bool {
        !matches!(self, Self::Unknown | Self::Scalar(DataType::Null))
    }
}

impl SemanticAnalyzer {
    fn variable_expression_type(&self, name: &str) -> ExpressionType {
        if let Some(value_type) = self.expression_types.get(&name.to_lowercase()) {
            return value_type.clone();
        }
        match self.variables.get_ci(name) {
            Some(info) if info.variable_type != VariableType::Property => {
                ExpressionType::Entity(info.variable_type.clone(), info.labels.clone())
            }
            _ => ExpressionType::Unknown,
        }
    }

    fn property_expression_type(&self, base: ExpressionType, property: &str) -> ExpressionType {
        match base {
            ExpressionType::Entity(kind, labels) => {
                let Some(catalog) = &self.catalog else {
                    return ExpressionType::Unknown;
                };
                let types = labels
                    .iter()
                    .map(|label| {
                        let source = match kind {
                            VariableType::Node => catalog.node_source(label),
                            VariableType::Relationship => catalog.relationship_source(label),
                            _ => None,
                        }?;
                        source
                            .schema()
                            .fields()
                            .iter()
                            .find(|field| field.name().eq_ignore_ascii_case(property))
                            .map(|field| ExpressionType::Scalar(field.data_type().clone()))
                    })
                    .collect::<Option<Vec<_>>>();
                types.map(common_type).unwrap_or(ExpressionType::Unknown)
            }
            ExpressionType::Map(fields) => fields
                .into_iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(property))
                .map(|(_, value_type)| value_type)
                .unwrap_or(ExpressionType::Unknown),
            ExpressionType::Scalar(DataType::Struct(fields)) => fields
                .iter()
                .find(|field| field.name().eq_ignore_ascii_case(property))
                .map(|field| ExpressionType::Scalar(field.data_type().clone()))
                .unwrap_or(ExpressionType::Unknown),
            // Date component access and function-specific fields remain with DataFusion.
            _ => ExpressionType::Unknown,
        }
    }

    pub(super) fn expression_type(&self, expression: &ValueExpression) -> ExpressionType {
        use ExpressionType::{List, Map, Scalar, Unknown};
        match expression {
            ValueExpression::Variable(name) | ValueExpression::LocalVariable(name) => {
                self.variable_expression_type(name)
            }
            ValueExpression::Property(property)
            | ValueExpression::Literal(PropertyValue::Property(property)) => self
                .property_expression_type(
                    self.variable_expression_type(&property.variable),
                    &property.property,
                ),
            ValueExpression::Literal(value) => match value {
                PropertyValue::Integer(_) => Scalar(DataType::Int64),
                PropertyValue::Float(_) => Scalar(DataType::Float64),
                PropertyValue::String(_) => Scalar(DataType::Utf8),
                PropertyValue::Boolean(_) => Scalar(DataType::Boolean),
                PropertyValue::Null => Scalar(DataType::Null),
                PropertyValue::Variable(name) => self.variable_expression_type(name),
                _ => Unknown,
            },
            ValueExpression::Arithmetic {
                left,
                operator,
                right,
            } => match (self.expression_type(left), self.expression_type(right)) {
                (Scalar(left), Scalar(right)) => {
                    BinaryTypeCoercer::new(&left, &arithmetic_operator(operator), &right)
                        .get_result_type()
                        .map(Scalar)
                        .unwrap_or(Unknown)
                }
                _ => Unknown,
            },
            ValueExpression::ScalarFunction { name, args }
            | ValueExpression::AggregateFunction { name, args, .. } => {
                let first = || {
                    args.first()
                        .map(|value| self.expression_type(value))
                        .unwrap_or(Unknown)
                };
                match name.to_lowercase().as_str() {
                    "count" | "size" | "length" | "tointeger" | "to_epoch_ms" => {
                        Scalar(DataType::Int64)
                    }
                    "tofloat" => Scalar(DataType::Float64),
                    "tolower" | "lower" | "toupper" | "upper" | "tostring" => {
                        Scalar(DataType::Utf8)
                    }
                    "date" => Scalar(DataType::Date32),
                    "datetime" => Scalar(DataType::Timestamp(
                        TimeUnit::Millisecond,
                        Some("UTC".into()),
                    )),
                    "collect" => List(Box::new(first())),
                    "head" | "__index" => first().element(),
                    "range" => List(Box::new(Scalar(DataType::Int64))),
                    "nodes" => List(Box::new(Unknown)),
                    "__list" => List(Box::new(common_type(
                        args.iter()
                            .map(|value| self.expression_type(value))
                            .collect(),
                    ))),
                    "coalesce" => common_type(
                        args.iter()
                            .map(|value| self.expression_type(value))
                            .collect(),
                    ),
                    // SUM/AVG output widths depend on DataFusion's accumulator type.
                    "min" | "max" => first(),
                    _ => Unknown,
                }
            }
            ValueExpression::Case { whens, otherwise } => common_type(
                whens
                    .iter()
                    .map(|(_, value)| self.expression_type(value))
                    .chain(otherwise.iter().map(|value| self.expression_type(value)))
                    .collect(),
            ),
            ValueExpression::Boolean(_) | ValueExpression::ListPredicate { .. } => {
                Scalar(DataType::Boolean)
            }
            ValueExpression::Map(fields) => Map(fields
                .iter()
                .map(|(name, value)| (name.clone(), self.expression_type(value)))
                .collect()),
            ValueExpression::Field { base, field } => {
                self.property_expression_type(self.expression_type(base), field)
            }
            ValueExpression::VectorLiteral(_) => List(Box::new(Scalar(DataType::Float32))),
            ValueExpression::VectorDistance { .. } | ValueExpression::VectorSimilarity { .. } => {
                Scalar(DataType::Float64)
            }
            // Their local scopes have ended; do not infer from similarly named outer variables.
            ValueExpression::Reduce { .. }
            | ValueExpression::ListComprehension { .. }
            | ValueExpression::PatternComprehension { .. }
            | ValueExpression::Parameter(_) => Unknown,
        }
    }

    pub(super) fn validate_binary(
        &self,
        left: &ValueExpression,
        operator: Operator,
        right: &ValueExpression,
    ) -> Result<()> {
        use ExpressionType::{Entity, Scalar};
        let left_type = self.expression_type(left);
        let right_type = self.expression_type(right);
        if !left_type.known() || !right_type.known() {
            return Ok(());
        }
        let comparison = matches!(
            operator,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        );
        let compatible = match (&left_type, &right_type) {
            (Scalar(left), Scalar(right)) => {
                let string_numeric = (left.is_numeric()
                    && matches!(
                        right,
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    ))
                    || (right.is_numeric()
                        && matches!(
                            left,
                            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                        ));
                !string_numeric
                    && BinaryTypeCoercer::new(left, &operator, right)
                        .get_result_type()
                        .is_ok()
            }
            (Entity(left, _), Entity(right, _)) => comparison && left == right,
            // Collection comparison rules remain with the planner; arithmetic on entities does not.
            (Entity(..), _) | (_, Entity(..)) => false,
            _ => comparison,
        };
        if compatible {
            Ok(())
        } else {
            Err(GraphError::PlanError {
                message: format!(
                    "Type mismatch in '{} {} {}': {} and {}",
                    display_value(left),
                    operator,
                    display_value(right),
                    display_type(&left_type),
                    display_type(&right_type)
                ),
                location: snafu::Location::new(file!(), line!(), column!()),
            })
        }
    }
}

fn common_type(types: Vec<ExpressionType>) -> ExpressionType {
    let mut known = types
        .into_iter()
        .filter(|value| !matches!(value, ExpressionType::Scalar(DataType::Null)));
    let Some(first) = known.next() else {
        return ExpressionType::Unknown;
    };
    if known.all(|value| value == first) {
        first
    } else {
        ExpressionType::Unknown
    }
}

fn display_type(value: &ExpressionType) -> String {
    match value {
        ExpressionType::Scalar(data_type) => data_type.to_string(),
        ExpressionType::Entity(kind, _) => format!("{kind:?}"),
        _ => format!("{value:?}"),
    }
}

fn display_value(value: &ValueExpression) -> String {
    match value {
        ValueExpression::Property(property) => {
            format!("{}.{}", property.variable, property.property)
        }
        ValueExpression::Variable(name) => name.clone(),
        ValueExpression::Literal(PropertyValue::Integer(value)) => value.to_string(),
        ValueExpression::Literal(PropertyValue::Float(value)) => value.to_string(),
        ValueExpression::Literal(PropertyValue::String(value)) => format!("{value:?}"),
        ValueExpression::Arithmetic {
            left,
            operator,
            right,
        } => format!(
            "({} {} {})",
            display_value(left),
            arithmetic_operator(operator),
            display_value(right)
        ),
        _ => format!("{value:?}"),
    }
}

pub(super) fn arithmetic_operator(operator: &ArithmeticOperator) -> Operator {
    match operator {
        ArithmeticOperator::Add => Operator::Plus,
        ArithmeticOperator::Subtract => Operator::Minus,
        ArithmeticOperator::Multiply => Operator::Multiply,
        ArithmeticOperator::Divide => Operator::Divide,
        ArithmeticOperator::Modulo => Operator::Modulo,
    }
}

pub(super) fn comparison_operator(operator: &ComparisonOperator) -> Operator {
    match operator {
        ComparisonOperator::Equal => Operator::Eq,
        ComparisonOperator::NotEqual => Operator::NotEq,
        ComparisonOperator::LessThan => Operator::Lt,
        ComparisonOperator::LessThanOrEqual => Operator::LtEq,
        ComparisonOperator::GreaterThan => Operator::Gt,
        ComparisonOperator::GreaterThanOrEqual => Operator::GtEq,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use lance_graph_catalog::{InMemoryCatalog, SimpleTableSource};

    fn analyze(query: &str) -> SemanticResult {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
            Field::new(
                "created",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        let catalog = InMemoryCatalog::new()
            .with_node_source("Person", Arc::new(SimpleTableSource::new(schema)));
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();
        SemanticAnalyzer::new(config)
            .with_catalog(Arc::new(catalog))
            .analyze(
                &crate::parser::parse_cypher_query(query).unwrap(),
                &HashMap::new(),
            )
            .unwrap()
    }

    #[test]
    fn rejects_known_operand_types_in_their_scope() {
        for (query, expression, types) in [
            ("MATCH (p:Person) WHERE p.name = 1 RETURN p.id", "p.name = 1", ["Utf8", "Int64"]),
            ("MATCH (p:Person) RETURN p + 1", "p + 1", ["Node", "Int64"]),
            ("MATCH (p:Person) WITH p.name AS value RETURN value * 2", "value * 2", ["Utf8", "Int64"]),
            ("MATCH (p:Person) WITH collect(p.name) AS names UNWIND names AS name RETURN name / 2", "name / 2", ["Utf8", "Int64"]),
            ("MATCH (p:Person) RETURN [p IN [p.name] | p + 1]", "p + 1", ["Utf8", "Int64"]),
        ] {
            let errors = analyze(query).errors.join("\n");
            assert!(errors.contains("Type mismatch") && errors.contains(expression)
                && types.iter().all(|name| errors.contains(name)), "{query}: {errors}");
        }
    }

    #[test]
    fn preserves_coercions_nulls_and_unknown_types() {
        for query in [
            "MATCH (p:Person) RETURN p.age + 0.5",
            "MATCH (p:Person) WHERE p.created > '2020-01-01T00:00:00Z' RETURN p.id",
            "MATCH (p:Person) RETURN p.age + null",
            "MATCH (p:Person) RETURN p.unknown + 1",
            "MATCH (p:Person) WITH p AS q RETURN q.age + 1",
            "MATCH (p:Person) RETURN [p IN [p.age] | p + 1], p.name",
            "MATCH (p:Person) RETURN [p IN [p.name, p.age] | p + 1]",
            "MATCH (p:Person) WITH collect(p) AS people UNWIND people AS person RETURN person.age + 1",
        ] {
            let errors = analyze(query).errors;
            assert!(errors.is_empty(), "{query}: {errors:?}");
        }
    }
}
