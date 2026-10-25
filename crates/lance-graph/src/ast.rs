// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Abstract Syntax Tree for Cypher queries
//!
//! This module defines the AST nodes for representing parsed Cypher queries.
//! The AST is designed to capture the essential graph patterns while being
//! simple enough to translate to SQL efficiently.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A pagination count, resolved to a nonnegative literal before logical planning.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RowCount {
    Literal(u64),
    Parameter(String),
}

impl RowCount {
    /// Return a resolved count; unresolved parameters are a planning error.
    pub fn value(&self) -> crate::error::Result<u64> {
        match self {
            Self::Literal(value) => Ok(*value),
            Self::Parameter(name) => Err(crate::error::GraphError::PlanError {
                message: format!("Unresolved pagination parameter: ${name}"),
                location: snafu::Location::new(file!(), line!(), column!()),
            }),
        }
    }
}

/// A complete Cypher query
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CypherQuery {
    /// READING clauses (MATCH, UNWIND, etc.)
    pub reading_clauses: Vec<ReadingClause>,
    /// WHERE clause (optional, before WITH if present)
    pub where_clause: Option<WhereClause>,
    /// WITH clause (optional) - intermediate projection/aggregation
    pub with_clause: Option<WithClause>,
    /// Post-WITH READING clauses
    pub post_with_reading_clauses: Vec<ReadingClause>,
    /// WHERE clause after WITH (optional) - filters the WITH results
    pub post_with_where_clause: Option<WhereClause>,
    /// Second and later `WITH ... [MATCH ...] [WHERE ...]` stages, in order. The fields
    /// above hold the first stage, as upstream's single-WITH AST did.
    #[serde(default)]
    pub further_with_stages: Vec<WithStage>,
    /// RETURN clause
    pub return_clause: ReturnClause,
    /// LIMIT clause (optional)
    pub limit: Option<RowCount>,
    /// ORDER BY clause (optional)
    pub order_by: Option<OrderByClause>,
    /// SKIP/OFFSET clause (optional)
    pub skip: Option<RowCount>,
}

impl CypherQuery {
    /// Extract all node labels referenced in the query
    pub fn get_node_labels(&self) -> Vec<String> {
        let mut labels = Vec::new();
        // Iterate all match clauses directly
        for clause in &self.reading_clauses {
            if let ReadingClause::Match(match_clause) = clause {
                for pattern in &match_clause.patterns {
                    match pattern {
                        GraphPattern::Node(node) => {
                            for label in &node.labels {
                                if !labels.contains(label) {
                                    labels.push(label.clone());
                                }
                            }
                        }
                        GraphPattern::Path(path) => {
                            for label in &path.start_node.labels {
                                if !labels.contains(label) {
                                    labels.push(label.clone());
                                }
                            }
                            for segment in &path.segments {
                                for label in &segment.end_node.labels {
                                    if !labels.contains(label) {
                                        labels.push(label.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        labels
    }

    /// Extract all relationship types referenced in the query
    pub fn get_relationship_types(&self) -> Vec<String> {
        let mut types = Vec::new();
        for clause in &self.reading_clauses {
            if let ReadingClause::Match(match_clause) = clause {
                for pattern in &match_clause.patterns {
                    self.collect_relationship_types_from_pattern(pattern, &mut types);
                }
            }
        }
        types
    }

    fn collect_relationship_types_from_pattern(
        &self,
        pattern: &GraphPattern,
        types: &mut Vec<String>,
    ) {
        if let GraphPattern::Path(path) = pattern {
            for segment in &path.segments {
                for rel_type in &segment.relationship.types {
                    if !types.contains(rel_type) {
                        types.push(rel_type.clone());
                    }
                }
            }
        }
    }
}

/// A clause that reads from the graph (MATCH, UNWIND)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReadingClause {
    Match(MatchClause),
    Unwind(UnwindClause),
}

/// A MATCH clause containing graph patterns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatchClause {
    /// Graph patterns to match
    pub patterns: Vec<GraphPattern>,
    /// `OPTIONAL MATCH`: rows with no match are kept, with nulls for the new variables
    #[serde(default)]
    pub optional: bool,
    /// A WHERE belonging to this clause: always the one right after an OPTIONAL MATCH
    /// (it filters inside the optional part), and for a plain MATCH one followed by
    /// another reading clause. A plain MATCH's last WHERE stays in the query's slot.
    #[serde(default)]
    pub where_clause: Option<WhereClause>,
}

/// An UNWIND clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnwindClause {
    /// Expression to unwind
    pub expression: ValueExpression,
    /// Alias for the unwound values
    pub alias: String,
}

/// A graph pattern (nodes and relationships)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GraphPattern {
    /// A single node pattern
    Node(NodePattern),
    /// A path pattern (node-relationship-node sequence)
    Path(PathPattern),
}

/// A node pattern in a graph query
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodePattern {
    /// Variable name for the node (e.g., 'n' in (n:Person))
    pub variable: Option<String>,
    /// Node labels (e.g., ['Person', 'Employee'])
    pub labels: Vec<String>,
    /// Property constraints (e.g., {name: 'John', age: 30})
    pub properties: HashMap<String, PropertyValue>,
}

/// A path pattern connecting nodes through relationships
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathPattern {
    /// Named ordinary path (`p = (a)-[:R]->(b)`).
    #[serde(default)]
    pub variable: Option<String>,
    /// Starting node
    pub start_node: NodePattern,
    /// Relationships and intermediate nodes
    pub segments: Vec<PathSegment>,
    /// `p = shortestPath(...)` / `allShortestPaths(...)`: one variable-length segment,
    /// matched only by its shortest paths.
    #[serde(default)]
    pub shortest: Option<ShortestPathSpec>,
}

/// How a shortest-path pattern matches, and the path variable it binds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShortestPathSpec {
    /// `allShortestPaths`: every path of the minimum length, not just one.
    pub all: bool,
    /// The path variable (`p` in `p = shortestPath(...)`), used with `length(p)` and `nodes(p)`.
    pub variable: Option<String>,
}

/// A segment of a path (relationship + end node)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathSegment {
    /// The relationship in this segment
    pub relationship: RelationshipPattern,
    /// The end node of this segment
    pub end_node: NodePattern,
}

/// A relationship pattern
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationshipPattern {
    /// Variable name for the relationship (e.g., 'r' in [r:KNOWS])
    pub variable: Option<String>,
    /// Relationship types (e.g., ['KNOWS', 'FRIEND_OF'])
    pub types: Vec<String>,
    /// Direction of the relationship
    pub direction: RelationshipDirection,
    /// Property constraints on the relationship
    pub properties: HashMap<String, PropertyValue>,
    /// Length constraints (for variable-length paths)
    pub length: Option<LengthRange>,
}

/// Direction of a relationship
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RelationshipDirection {
    /// Outgoing relationship (->)
    Outgoing,
    /// Incoming relationship (<-)
    Incoming,
    /// Undirected relationship (-)
    Undirected,
}

/// Length range for variable-length paths
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LengthRange {
    /// Minimum length (inclusive)
    pub min: Option<u32>,
    /// Maximum length (inclusive)
    pub max: Option<u32>,
}

/// Property value in patterns and expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropertyValue {
    /// String literal
    String(String),
    /// Integer literal
    Integer(i64),
    /// Float literal
    Float(f64),
    /// Boolean literal
    Boolean(bool),
    /// Null value
    Null,
    /// Parameter reference (e.g., $param)
    Parameter(String),
    /// Property reference (e.g., node.property)
    Property(PropertyRef),
    /// A variable from an earlier clause, as an inline property value (`{id: tagId}`).
    /// Like `Property` values, it is moved into the clause's WHERE before planning.
    Variable(String),
}

/// Reference to a property of a node or relationship
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyRef {
    /// Variable name (e.g., 'n' in n.name)
    pub variable: String,
    /// Property name
    pub property: String,
}

/// WHERE clause for filtering
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhereClause {
    /// Boolean expression for filtering
    pub expression: BooleanExpression,
}

/// Boolean expressions in WHERE clauses
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BooleanExpression {
    /// Comparison operation (=, <>, <, >, <=, >=)
    Comparison {
        left: ValueExpression,
        operator: ComparisonOperator,
        right: ValueExpression,
    },
    /// Logical AND
    And(Box<BooleanExpression>, Box<BooleanExpression>),
    /// Logical OR
    Or(Box<BooleanExpression>, Box<BooleanExpression>),
    /// Logical NOT
    Not(Box<BooleanExpression>),
    /// Property existence check
    Exists(PropertyRef),
    /// `EXISTS { [MATCH] pattern [WHERE ...] }`, or a bare pattern used as a predicate
    /// (`NOT (a)-[:R]-(b)`): true when the pattern matches from this row's variables.
    PatternExists {
        patterns: Vec<GraphPattern>,
        where_clause: Option<Box<BooleanExpression>>,
    },
    /// IN clause
    In {
        expression: ValueExpression,
        list: Vec<ValueExpression>,
    },
    /// `x IN list` where the list is any expression, such as a collected alias
    /// (`friend IN friends`) rather than a literal `[...]`.
    Member {
        expression: ValueExpression,
        list: ValueExpression,
    },
    /// LIKE pattern matching (case-sensitive)
    Like {
        expression: ValueExpression,
        pattern: String,
    },
    /// ILIKE pattern matching (case-insensitive)
    ILike {
        expression: ValueExpression,
        pattern: String,
    },
    /// CONTAINS substring matching
    Contains {
        expression: ValueExpression,
        substring: String,
    },
    /// STARTS WITH prefix matching
    StartsWith {
        expression: ValueExpression,
        prefix: String,
    },
    /// ENDS WITH suffix matching
    EndsWith {
        expression: ValueExpression,
        suffix: String,
    },
    /// IS NULL pattern matching
    IsNull(ValueExpression),
    /// IS NOT NULL pattern matching
    IsNotNull(ValueExpression),
}

/// Distance metric for vector similarity
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum DistanceMetric {
    /// Euclidean distance (L2)
    L2,
    /// Cosine similarity (1 - cosine distance)
    #[default]
    Cosine,
    /// Dot product
    Dot,
}

/// Comparison operators
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

/// Value expressions (for comparisons and return values)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValueExpression {
    /// Variable reference
    Variable(String),
    /// Property reference
    Property(PropertyRef),
    /// Literal value
    Literal(PropertyValue),
    /// `CASE WHEN cond THEN value ... [ELSE value] END`. The simple form
    /// `CASE x WHEN v THEN ...` is parsed into this one, with `x = v` conditions.
    Case {
        whens: Vec<(BooleanExpression, ValueExpression)>,
        otherwise: Option<Box<ValueExpression>>,
    },
    /// Scalar function call (toLower, upper, etc.)
    /// These are row-level functions that operate on individual values
    ScalarFunction {
        name: String,
        args: Vec<ValueExpression>,
    },
    /// Aggregate function call (COUNT, SUM, AVG, MIN, MAX, COLLECT)
    /// These functions operate across multiple rows and support DISTINCT
    AggregateFunction {
        name: String,
        args: Vec<ValueExpression>,
        /// Whether DISTINCT keyword was specified (e.g., COUNT(DISTINCT x))
        distinct: bool,
    },
    /// Arithmetic operation
    Arithmetic {
        left: Box<ValueExpression>,
        operator: ArithmeticOperator,
        right: Box<ValueExpression>,
    },
    /// Vector distance function: vector_distance(left, right, metric)
    /// Returns the distance as a float (lower = more similar for L2/Cosine)
    VectorDistance {
        left: Box<ValueExpression>,
        right: Box<ValueExpression>,
        metric: DistanceMetric,
    },
    /// Vector similarity function: vector_similarity(left, right, metric)
    /// Returns the similarity score as a float (higher = more similar)
    VectorSimilarity {
        left: Box<ValueExpression>,
        right: Box<ValueExpression>,
        metric: DistanceMetric,
    },
    /// Parameter reference for query parameters (e.g., $query_vector)
    Parameter(String),
    /// A predicate used as a value: `path IS NULL`, `NOT (a)-[:R]-(b) AS isNew`.
    Boolean(Box<BooleanExpression>),
    /// A map literal `{key: value, ...}`; a node as a value inside it keeps its properties.
    Map(Vec<(String, ValueExpression)>),
    /// `value.field` on something other than a node or relationship variable: a map key,
    /// or a component of a date or timestamp (`birthday.month`).
    Field {
        base: Box<ValueExpression>,
        field: String,
    },
    /// `[(a)-[:R]->(b) WHERE predicate | value]`: the values for each match of the pattern
    /// from this row's bound variables.
    PatternComprehension {
        pattern: Box<PathPattern>,
        filter: Option<Box<BooleanExpression>>,
        map: Box<ValueExpression>,
    },
    /// `[x IN list WHERE predicate | value]`, both parts optional.
    ListComprehension {
        variable: String,
        list: Box<ValueExpression>,
        filter: Option<Box<BooleanExpression>>,
        map: Option<Box<ValueExpression>>,
    },
    /// A lexically scoped variable in a reduce or list predicate body.
    LocalVariable(String),
    /// `reduce(accumulator = initial, variable IN list | expression)`.
    Reduce {
        accumulator: String,
        initial: Box<ValueExpression>,
        variable: String,
        list: Box<ValueExpression>,
        expression: Box<ValueExpression>,
    },
    /// `all/any/none/single(variable IN list WHERE predicate)`.
    ListPredicate {
        kind: String,
        variable: String,
        list: Box<ValueExpression>,
        predicate: Box<ValueExpression>,
    },
    /// Vector literal: [0.1, 0.2, 0.3]
    /// Represents an inline vector for similarity search
    VectorLiteral(Vec<f32>),
}

/// Function type classification
#[derive(Debug, Clone, PartialEq)]
pub enum FunctionType {
    /// Aggregate function (operates across multiple rows)
    Aggregate,
    /// Scalar function (operates on individual values)
    Scalar,
    /// Unknown function type
    Unknown,
}

/// Classify a function by name
pub fn classify_function(name: &str) -> FunctionType {
    match name.to_lowercase().as_str() {
        "count" | "sum" | "avg" | "min" | "max" | "collect" => FunctionType::Aggregate,
        "tolower" | "lower" | "toupper" | "upper" | "coalesce" | "date_part" | "to_epoch_ms"
        | "length" | "nodes" | "size" | "range" | "__index" | "__list" | "tostring" | "head"
        | "tointeger" | "tofloat" | "date" | "datetime" | "duration.inseconds" => {
            FunctionType::Scalar
        }
        // Vector functions are handled separately as special variants
        _ => FunctionType::Unknown,
    }
}

/// Arithmetic operators
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

/// WITH clause for intermediate projections/aggregations
///
/// WITH acts as a query stage boundary, projecting results that become
/// the input for subsequent clauses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WithClause {
    /// Whether DISTINCT was specified
    #[serde(default)]
    pub distinct: bool,
    /// Items to project (similar to RETURN)
    pub items: Vec<ReturnItem>,
    /// Optional ORDER BY within WITH
    pub order_by: Option<OrderByClause>,
    /// Optional LIMIT within WITH
    pub limit: Option<RowCount>,
    /// `WITH ... WHERE`: filters the WITH output, before any following MATCH
    #[serde(default)]
    pub where_clause: Option<WhereClause>,
}

/// One `WITH` with the reading clauses and `WHERE` that follow it, before the next `WITH`
/// or the `RETURN`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WithStage {
    pub with_clause: WithClause,
    pub reading_clauses: Vec<ReadingClause>,
    pub where_clause: Option<WhereClause>,
}

/// RETURN clause specifying what to return
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReturnClause {
    /// Whether DISTINCT was specified
    pub distinct: bool,
    /// Items to return
    pub items: Vec<ReturnItem>,
}

/// An item in the RETURN clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReturnItem {
    /// The expression to return
    pub expression: ValueExpression,
    /// Alias for the returned value
    pub alias: Option<String>,
}

/// ORDER BY clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByClause {
    /// Items to order by
    pub items: Vec<OrderByItem>,
}

/// An item in the ORDER BY clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByItem {
    /// Expression to order by
    pub expression: ValueExpression,
    /// Sort direction
    pub direction: SortDirection,
}

/// Sort direction for ORDER BY
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SortDirection {
    Ascending,
    Descending,
}

impl NodePattern {
    /// Create a new node pattern
    pub fn new(variable: Option<String>) -> Self {
        Self {
            variable,
            labels: Vec::new(),
            properties: HashMap::new(),
        }
    }

    /// Add a label to the node pattern
    pub fn with_label<S: Into<String>>(mut self, label: S) -> Self {
        self.labels.push(label.into());
        self
    }

    /// Add a property constraint to the node pattern
    pub fn with_property<S: Into<String>>(mut self, key: S, value: PropertyValue) -> Self {
        self.properties.insert(key.into(), value);
        self
    }
}

impl RelationshipPattern {
    /// Create a new relationship pattern
    pub fn new(direction: RelationshipDirection) -> Self {
        Self {
            variable: None,
            types: Vec::new(),
            direction,
            properties: HashMap::new(),
            length: None,
        }
    }

    /// Set the variable name for the relationship
    pub fn with_variable<S: Into<String>>(mut self, variable: S) -> Self {
        self.variable = Some(variable.into());
        self
    }

    /// Add a type to the relationship pattern
    pub fn with_type<S: Into<String>>(mut self, rel_type: S) -> Self {
        self.types.push(rel_type.into());
        self
    }

    /// Add a property constraint to the relationship pattern
    pub fn with_property<S: Into<String>>(mut self, key: S, value: PropertyValue) -> Self {
        self.properties.insert(key.into(), value);
        self
    }
}

impl PropertyRef {
    /// Create a new property reference
    pub fn new<S: Into<String>>(variable: S, property: S) -> Self {
        Self {
            variable: variable.into(),
            property: property.into(),
        }
    }
}

/// Apply `f` to every value inside `expr`, children before parents, including the values
/// inside predicates. Patterns of EXISTS subqueries are left alone.
pub fn transform_value(expr: &mut ValueExpression, f: &mut dyn FnMut(&mut ValueExpression)) {
    match expr {
        ValueExpression::ScalarFunction { args, .. }
        | ValueExpression::AggregateFunction { args, .. } => {
            args.iter_mut().for_each(|a| transform_value(a, f))
        }
        ValueExpression::Arithmetic { left, right, .. }
        | ValueExpression::VectorDistance { left, right, .. }
        | ValueExpression::VectorSimilarity { left, right, .. } => {
            transform_value(left, f);
            transform_value(right, f);
        }
        ValueExpression::Case { whens, otherwise } => {
            for (condition, then) in whens {
                transform_boolean_values(condition, f);
                transform_value(then, f);
            }
            if let Some(e) = otherwise {
                transform_value(e, f);
            }
        }
        ValueExpression::Boolean(b) => transform_boolean_values(b, f),
        ValueExpression::Map(entries) => {
            entries.iter_mut().for_each(|(_, v)| transform_value(v, f))
        }
        ValueExpression::Field { base, .. } => transform_value(base, f),
        ValueExpression::ListComprehension {
            list, filter, map, ..
        } => {
            transform_value(list, f);
            if let Some(b) = filter {
                transform_boolean_values(b, f);
            }
            if let Some(m) = map {
                transform_value(m, f);
            }
        }
        ValueExpression::PatternComprehension { filter, map, .. } => {
            if let Some(b) = filter {
                transform_boolean_values(b, f);
            }
            transform_value(map, f);
        }
        ValueExpression::Reduce {
            initial,
            list,
            expression,
            ..
        } => {
            transform_value(initial, f);
            transform_value(list, f);
            transform_value(expression, f);
        }
        ValueExpression::ListPredicate {
            list, predicate, ..
        } => {
            transform_value(list, f);
            transform_value(predicate, f);
        }
        ValueExpression::Variable(_)
        | ValueExpression::LocalVariable(_)
        | ValueExpression::Property(_)
        | ValueExpression::Literal(_)
        | ValueExpression::Parameter(_)
        | ValueExpression::VectorLiteral(_) => {}
    }
    f(expr);
}

// Mark only the body's local references; the input and initializer retain outer scope.
pub(crate) fn mark_locals(expr: &mut ValueExpression, names: &[&str]) {
    transform_value(expr, &mut |value| match value {
        ValueExpression::Variable(name) if names.iter().any(|n| n.eq_ignore_ascii_case(name)) => {
            *value = ValueExpression::LocalVariable(name.to_lowercase());
        }
        ValueExpression::Property(p)
            if names.iter().any(|n| n.eq_ignore_ascii_case(&p.variable)) =>
        {
            *value = ValueExpression::Field {
                base: Box::new(ValueExpression::LocalVariable(p.variable.to_lowercase())),
                field: p.property.clone(),
            };
        }
        _ => {}
    });
}

/// Expressions that need relational planning rather than scalar lambda execution.
pub(crate) fn needs_relational_evaluation(expr: &ValueExpression) -> bool {
    fn boolean_has_pattern(expr: &BooleanExpression) -> bool {
        match expr {
            BooleanExpression::PatternExists { .. } => true,
            BooleanExpression::And(a, b) | BooleanExpression::Or(a, b) => {
                boolean_has_pattern(a) || boolean_has_pattern(b)
            }
            BooleanExpression::Not(b) => boolean_has_pattern(b),
            _ => false,
        }
    }
    let mut found = false;
    transform_value(&mut expr.clone(), &mut |value| {
        found |= match value {
            ValueExpression::PatternComprehension { .. }
            | ValueExpression::ListComprehension { .. } => true,
            ValueExpression::Boolean(b) => boolean_has_pattern(b),
            ValueExpression::Case { whens, .. } => {
                whens.iter().any(|(b, _)| boolean_has_pattern(b))
            }
            _ => false,
        };
    });
    found
}

/// [`transform_value`] over every value in a predicate.
pub fn transform_boolean_values(
    expr: &mut BooleanExpression,
    f: &mut dyn FnMut(&mut ValueExpression),
) {
    match expr {
        BooleanExpression::Comparison { left, right, .. } => {
            transform_value(left, f);
            transform_value(right, f);
        }
        BooleanExpression::And(a, b) | BooleanExpression::Or(a, b) => {
            transform_boolean_values(a, f);
            transform_boolean_values(b, f);
        }
        BooleanExpression::Not(inner) => transform_boolean_values(inner, f),
        BooleanExpression::In { expression, list } => {
            transform_value(expression, f);
            list.iter_mut().for_each(|v| transform_value(v, f));
        }
        BooleanExpression::Member { expression, list } => {
            transform_value(expression, f);
            transform_value(list, f);
        }
        BooleanExpression::Like { expression, .. }
        | BooleanExpression::ILike { expression, .. }
        | BooleanExpression::Contains { expression, .. }
        | BooleanExpression::StartsWith { expression, .. }
        | BooleanExpression::EndsWith { expression, .. }
        | BooleanExpression::IsNull(expression)
        | BooleanExpression::IsNotNull(expression) => transform_value(expression, f),
        BooleanExpression::Exists(_) | BooleanExpression::PatternExists { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_pattern_creation() {
        let node = NodePattern::new(Some("n".to_string()))
            .with_label("Person")
            .with_property("name", PropertyValue::String("John".to_string()));

        assert_eq!(node.variable, Some("n".to_string()));
        assert_eq!(node.labels, vec!["Person"]);
        assert_eq!(node.properties.len(), 1);
    }

    #[test]
    fn test_relationship_pattern_creation() {
        let rel = RelationshipPattern::new(RelationshipDirection::Outgoing)
            .with_variable("r")
            .with_type("KNOWS");

        assert_eq!(rel.variable, Some("r".to_string()));
        assert_eq!(rel.types, vec!["KNOWS"]);
        assert_eq!(rel.direction, RelationshipDirection::Outgoing);
    }

    #[test]
    fn test_property_ref() {
        let prop_ref = PropertyRef::new("n", "name");
        assert_eq!(prop_ref.variable, "n");
        assert_eq!(prop_ref.property, "name");
    }
}
