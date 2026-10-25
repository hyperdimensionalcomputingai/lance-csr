// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Cypher query parser
//!
//! This module provides parsing functionality for Cypher queries using nom parser combinators.
//! It supports a subset of Cypher syntax focused on graph pattern matching and property access.

use crate::ast::*;
use crate::error::{GraphError, Result};
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while, take_while1},
    character::complete::{char, digit0, digit1, multispace0, multispace1, one_of, satisfy},
    combinator::{map, map_res, not, opt, peek, recognize},
    multi::{many0, many1, separated_list0, separated_list1},
    sequence::{delimited, pair, preceded, terminated, tuple},
    IResult,
};
use std::collections::HashMap;

/// Parse a complete Cypher query
pub fn parse_cypher_query(input: &str) -> Result<CypherQuery> {
    let (remaining, query) = cypher_query(input).map_err(|e| GraphError::ParseError {
        message: format!("Failed to parse Cypher query: {}", e),
        position: 0,
        location: snafu::Location::new(file!(), line!(), column!()),
    })?;

    if !remaining.trim().is_empty() {
        return Err(GraphError::ParseError {
            message: format!("Unexpected input after query: {}", remaining),
            position: input.len() - remaining.len(),
            location: snafu::Location::new(file!(), line!(), column!()),
        });
    }

    let mut query = query;
    let stages = query
        .further_with_stages
        .iter_mut()
        .flat_map(|s| &mut s.reading_clauses);
    let mut anonymous = 0;
    for clause in query
        .reading_clauses
        .iter_mut()
        .chain(&mut query.post_with_reading_clauses)
        .chain(stages)
    {
        if let ReadingClause::Match(m) = clause {
            move_variable_properties_to_where(m, &mut anonymous);
        }
    }
    Ok(query)
}

/// Inline node properties whose value is a variable or property from an earlier clause
/// (`(t:Tag {id: tagId})`) become `t.id = tagId` in the clause's own WHERE, since scans
/// filter only on constants. An unnamed node gets a name for the condition.
fn move_variable_properties_to_where(clause: &mut MatchClause, anonymous: &mut usize) {
    let mut conditions = Vec::new();
    for pattern in &mut clause.patterns {
        let nodes: Vec<&mut NodePattern> = match pattern {
            GraphPattern::Node(n) => vec![n],
            GraphPattern::Path(p) => std::iter::once(&mut p.start_node)
                .chain(p.segments.iter_mut().map(|s| &mut s.end_node))
                .collect(),
        };
        for node in nodes {
            let keys: Vec<String> = node
                .properties
                .iter()
                .filter(|(_, v)| {
                    matches!(v, PropertyValue::Variable(_) | PropertyValue::Property(_))
                })
                .map(|(k, _)| k.clone())
                .collect();
            for key in keys {
                let value = match node.properties.remove(&key) {
                    Some(PropertyValue::Variable(v)) => ValueExpression::Variable(v),
                    Some(PropertyValue::Property(p)) => ValueExpression::Property(p),
                    _ => unreachable!("filtered above"),
                };
                let variable = node.variable.get_or_insert_with(|| {
                    *anonymous += 1;
                    format!("__inline_{anonymous}")
                });
                conditions.push(BooleanExpression::Comparison {
                    left: ValueExpression::Property(PropertyRef {
                        variable: variable.clone(),
                        property: key,
                    }),
                    operator: ComparisonOperator::Equal,
                    right: value,
                });
            }
        }
    }
    let existing = clause.where_clause.take().map(|w| w.expression);
    clause.where_clause = conditions
        .into_iter()
        .chain(existing)
        .reduce(|a, b| BooleanExpression::And(Box::new(a), Box::new(b)))
        .map(|expression| WhereClause { expression });
}

// Top-level parser for a complete Cypher query
fn cypher_query(input: &str) -> IResult<&str, CypherQuery> {
    let (input, _) = multispace0(input)?;
    let (input, reading_clauses) = many0(reading_clause)(input)?;
    let (input, pre_with_where) = opt(where_clause)(input)?;

    // Optional WITH clause with optional post-WITH MATCH and WHERE
    let (input, with_result) = opt(with_clause)(input)?;
    // Only try to parse post-WITH clauses if we have a WITH clause
    let (input, post_with_reading_clauses, post_with_where) = match with_result {
        Some(_) => {
            let (input, readings) = many0(reading_clause)(input)?;
            let (input, where_cl) = opt(where_clause)(input)?;
            (input, readings, where_cl)
        }
        None => (input, vec![], None),
    };
    let (input, further_with_stages) = match with_result {
        Some(_) => many0(with_stage)(input)?,
        None => (input, vec![]),
    };

    let (input, return_clause) = return_clause(input)?;
    let (input, order_by) = opt(order_by_clause)(input)?;
    let (input, (skip, limit)) = pagination_clauses(input)?;
    let (input, _) = multispace0(input)?;

    Ok((
        input,
        CypherQuery {
            reading_clauses,
            where_clause: pre_with_where,
            with_clause: with_result,
            post_with_reading_clauses,
            post_with_where_clause: post_with_where,
            further_with_stages,
            return_clause,
            limit,
            order_by,
            skip,
        },
    ))
}

// Parse a reading clause (MATCH or UNWIND)
fn reading_clause(input: &str) -> IResult<&str, ReadingClause> {
    alt((
        map(match_clause, ReadingClause::Match),
        map(unwind_clause, ReadingClause::Unwind),
    ))(input)
}

// Parse a MATCH clause
fn match_clause(input: &str) -> IResult<&str, MatchClause> {
    let (input, _) = multispace0(input)?;
    let (input, optional) = opt(terminated(tag_no_case("OPTIONAL"), multispace1))(input)?;
    let (input, _) = tag_no_case("MATCH")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, patterns) = separated_list0(comma_ws, graph_pattern)(input)?;
    // See `MatchClause::where_clause` for which WHERE belongs to the clause.
    let (input, where_clause) = if optional.is_some() {
        opt(where_clause)(input)?
    } else {
        opt(terminated(where_clause, peek(reading_clause)))(input)?
    };

    Ok((
        input,
        MatchClause {
            patterns,
            optional: optional.is_some(),
            where_clause,
        },
    ))
}

// Parse an UNWIND clause
fn unwind_clause(input: &str) -> IResult<&str, UnwindClause> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("UNWIND")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, expression) = value_expression(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("AS")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, alias) = identifier(input)?;

    Ok((
        input,
        UnwindClause {
            expression,
            alias: alias.to_string(),
        },
    ))
}

// Parse a graph pattern (node or path)
fn graph_pattern(input: &str) -> IResult<&str, GraphPattern> {
    alt((
        map(shortest_path_pattern, GraphPattern::Path),
        map(named_path_pattern, GraphPattern::Path),
        map(path_pattern, GraphPattern::Path),
        map(node_pattern, GraphPattern::Node),
    ))(input)
}

// [p =] shortestPath((a)-[:R*min..max]-(b)) or allShortestPaths(...)
fn shortest_path_pattern(input: &str) -> IResult<&str, PathPattern> {
    let (input, _) = multispace0(input)?;
    let (input, variable) = opt(terminated(
        identifier,
        tuple((multispace0, char('='), multispace0)),
    ))(input)?;
    let (input, all) = alt((
        map(tag_no_case("allShortestPaths"), |_| true),
        map(tag_no_case("shortestPath"), |_| false),
    ))(input)?;
    let (input, mut path) = delimited(
        tuple((multispace0, char('('), multispace0)),
        path_pattern,
        tuple((multispace0, char(')'))),
    )(input)?;
    path.shortest = Some(ShortestPathSpec {
        all,
        variable: variable.map(str::to_string),
    });
    Ok((input, path))
}

// Named ordinary paths retain their textual node and relationship order.
fn named_path_pattern(input: &str) -> IResult<&str, PathPattern> {
    let (input, variable) = delimited(
        multispace0,
        identifier,
        tuple((multispace0, char('='), multispace0)),
    )(input)?;
    let (input, mut path) = path_pattern(input)?;
    path.variable = Some(variable.to_string());
    Ok((input, path))
}

// Parse a path pattern (only if there are segments)
fn path_pattern(input: &str) -> IResult<&str, PathPattern> {
    let (input, start_node) = node_pattern(input)?;
    let (input, segments) = many0(path_segment)(input)?;

    // Only succeed if we actually have path segments
    if segments.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }

    Ok((
        input,
        PathPattern {
            start_node,
            segments,
            shortest: None,
            variable: None,
        },
    ))
}

// Parse a path segment (relationship + node)
fn path_segment(input: &str) -> IResult<&str, PathSegment> {
    let (input, relationship) = relationship_pattern(input)?;
    let (input, end_node) = node_pattern(input)?;

    Ok((
        input,
        PathSegment {
            relationship,
            end_node,
        },
    ))
}

// Parse a node pattern: (variable:Label {prop: value})
fn node_pattern(input: &str) -> IResult<&str, NodePattern> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, variable) = opt(identifier)(input)?;
    let (input, labels) = many0(preceded(char(':'), identifier))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, properties) = opt(property_map)(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

    Ok((
        input,
        NodePattern {
            variable: variable.map(|s| s.to_string()),
            labels: labels.into_iter().map(|s| s.to_string()).collect(),
            properties: properties.unwrap_or_default(),
        },
    ))
}

// Parse a relationship pattern: -[variable:TYPE {prop: value}]->
fn relationship_pattern(input: &str) -> IResult<&str, RelationshipPattern> {
    let (input, _) = multispace0(input)?;

    // Parse direction and bracket content
    let (input, (direction, content)) = alt((
        // Outgoing: -[...]->
        map(
            tuple((
                char('-'),
                delimited(char('['), relationship_content, char(']')),
                tag("->"),
            )),
            |(_, content, _)| (RelationshipDirection::Outgoing, content),
        ),
        // Incoming: <-[...]-
        map(
            tuple((
                tag("<-"),
                delimited(char('['), relationship_content, char(']')),
                char('-'),
            )),
            |(_, content, _)| (RelationshipDirection::Incoming, content),
        ),
        // Undirected: -[...]-
        map(
            tuple((
                char('-'),
                delimited(char('['), relationship_content, char(']')),
                char('-'),
            )),
            |(_, content, _)| (RelationshipDirection::Undirected, content),
        ),
    ))(input)?;

    let (variable, types, properties, length) = content;

    Ok((
        input,
        RelationshipPattern {
            variable: variable.map(|s| s.to_string()),
            types: types.into_iter().map(|s| s.to_string()).collect(),
            direction,
            properties: properties.unwrap_or_default(),
            length,
        },
    ))
}

// Type alias for complex relationship content return type
type RelationshipContentResult<'a> = (
    Option<&'a str>,
    Vec<&'a str>,
    Option<HashMap<String, PropertyValue>>,
    Option<LengthRange>,
);

// Parse relationship content inside brackets
fn relationship_content(input: &str) -> IResult<&str, RelationshipContentResult<'_>> {
    let (input, _) = multispace0(input)?;
    let (input, variable) = opt(identifier)(input)?;
    // `:A`, alternation `:A|B` or `:A|:B`, and upstream's `:A:B` (also read as A or B).
    let (input, first) = opt(preceded(char(':'), identifier))(input)?;
    let (input, rest) = many0(preceded(
        alt((
            map(
                tuple((multispace0, char('|'), multispace0, opt(char(':')))),
                |_| (),
            ),
            map(char(':'), |_| ()),
        )),
        identifier,
    ))(input)?;
    let types: Vec<&str> = first.into_iter().chain(rest).collect();
    let (input, _) = multispace0(input)?;
    let (input, length) = opt(length_range)(input)?;
    let (input, _) = multispace0(input)?;
    let (input, properties) = opt(property_map)(input)?;
    let (input, _) = multispace0(input)?;

    Ok((input, (variable, types, properties, length)))
}

// Parse a property map: {key: value, key2: value2}
fn property_map(input: &str) -> IResult<&str, HashMap<String, PropertyValue>> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('{')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, pairs) = separated_list0(comma_ws, property_pair)(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('}')(input)?;

    Ok((input, pairs.into_iter().collect()))
}

// Parse a property key-value pair
fn property_pair(input: &str) -> IResult<&str, (String, PropertyValue)> {
    let (input, _) = multispace0(input)?;
    let (input, key) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(':')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = alt((
        property_value,
        map(property_reference, PropertyValue::Property),
        map(identifier, |v| PropertyValue::Variable(v.to_string())),
    ))(input)?;

    Ok((input, (key.to_string(), value)))
}

// Parse a property value
fn property_value(input: &str) -> IResult<&str, PropertyValue> {
    alt((
        map(string_literal, PropertyValue::String),
        map(float_literal, PropertyValue::Float), // Try float BEFORE integer (more specific)
        map(integer_literal, PropertyValue::Integer),
        map(boolean_literal, PropertyValue::Boolean),
        // NULL in any case, as a whole word (not the start of a name like `nullCount`).
        map(
            terminated(
                tag_no_case("null"),
                not(peek(satisfy(|c: char| c.is_alphanumeric() || c == '_'))),
            ),
            |_| PropertyValue::Null,
        ),
        map(parameter, PropertyValue::Parameter),
    ))(input)
}

// Parse a WHERE clause
fn where_clause(input: &str) -> IResult<&str, WhereClause> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("WHERE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, expression) = boolean_expression(input)?;

    Ok((input, WhereClause { expression }))
}

// Parse a boolean expression with OR precedence
fn boolean_expression(input: &str) -> IResult<&str, BooleanExpression> {
    boolean_or_expression(input)
}

fn boolean_or_expression(input: &str) -> IResult<&str, BooleanExpression> {
    let (input, first) = boolean_and_expression(input)?;
    let (input, rest) = many0(preceded(
        tuple((multispace0, tag_no_case("OR"), multispace1)),
        boolean_and_expression,
    ))(input)?;
    let expr = rest.into_iter().fold(first, |acc, item| {
        BooleanExpression::Or(Box::new(acc), Box::new(item))
    });
    Ok((input, expr))
}

fn boolean_and_expression(input: &str) -> IResult<&str, BooleanExpression> {
    let (input, first) = boolean_not_expression(input)?;
    let (input, rest) = many0(preceded(
        tuple((multispace0, tag_no_case("AND"), multispace1)),
        boolean_not_expression,
    ))(input)?;
    let expr = rest.into_iter().fold(first, |acc, item| {
        BooleanExpression::And(Box::new(acc), Box::new(item))
    });
    Ok((input, expr))
}

fn boolean_not_expression(input: &str) -> IResult<&str, BooleanExpression> {
    let (input, _) = multispace0(input)?;
    alt((
        map(
            preceded(
                tuple((tag_no_case("NOT"), multispace1)),
                boolean_not_expression,
            ),
            |expr| BooleanExpression::Not(Box::new(expr)),
        ),
        boolean_primary_expression,
    ))(input)
}

fn boolean_primary_expression(input: &str) -> IResult<&str, BooleanExpression> {
    let (input, _) = multispace0(input)?;
    alt((
        map(
            delimited(
                tuple((char('('), multispace0)),
                boolean_expression,
                tuple((multispace0, char(')'))),
            ),
            |expr| expr,
        ),
        exists_subquery,
        // A path used as a predicate: `(a)-[:R]-(b)` (one relationship at least).
        map(path_pattern, |path| BooleanExpression::PatternExists {
            patterns: vec![GraphPattern::Path(path)],
            where_clause: None,
        }),
        comparison_expression,
        map(list_predicate, |left| BooleanExpression::Comparison {
            left,
            operator: ComparisonOperator::Equal,
            right: ValueExpression::Literal(PropertyValue::Boolean(true)),
        }),
    ))(input)
}

// EXISTS { [MATCH] pattern, ... [WHERE ...] }
fn exists_subquery(input: &str) -> IResult<&str, BooleanExpression> {
    let (input, _) = tuple((tag_no_case("EXISTS"), multispace0, char('{'), multispace0))(input)?;
    let (input, _) = opt(tuple((tag_no_case("MATCH"), multispace1)))(input)?;
    let (input, patterns) = separated_list1(comma_ws, graph_pattern)(input)?;
    let (input, where_clause) = opt(where_clause)(input)?;
    let (input, _) = tuple((multispace0, char('}')))(input)?;
    Ok((
        input,
        BooleanExpression::PatternExists {
            patterns,
            where_clause: where_clause.map(|w| Box::new(w.expression)),
        },
    ))
}

fn comparison_expression(input: &str) -> IResult<&str, BooleanExpression> {
    let (input, _) = multispace0(input)?;
    let (input, left) = value_expression(input)?;
    let (input, _) = multispace0(input)?;
    let left_clone = left.clone();

    if let Ok((input_after_in, (_, _, list))) =
        tuple((tag_no_case("IN"), multispace0, value_expression_list))(input)
    {
        return Ok((
            input_after_in,
            BooleanExpression::In {
                expression: left,
                list,
            },
        ));
    }
    if let Ok((input_after_in, (_, _, list))) =
        tuple((tag_no_case("IN"), multispace1, value_expression))(input)
    {
        return Ok((
            input_after_in,
            BooleanExpression::Member {
                expression: left,
                list,
            },
        ));
    }
    // Match LIKE pattern
    if let Ok((input_after_like, (_, _, pattern))) =
        tuple((tag_no_case("LIKE"), multispace0, string_literal))(input)
    {
        return Ok((
            input_after_like,
            BooleanExpression::Like {
                expression: left,
                pattern,
            },
        ));
    }
    // Match ILIKE pattern (case-insensitive LIKE)
    if let Ok((input_after_ilike, (_, _, pattern))) =
        tuple((tag_no_case("ILIKE"), multispace0, string_literal))(input)
    {
        return Ok((
            input_after_ilike,
            BooleanExpression::ILike {
                expression: left,
                pattern,
            },
        ));
    }
    // Match CONTAINS substring
    if let Ok((input_after_contains, (_, _, substring))) =
        tuple((tag_no_case("CONTAINS"), multispace0, string_literal))(input)
    {
        return Ok((
            input_after_contains,
            BooleanExpression::Contains {
                expression: left,
                substring,
            },
        ));
    }
    // Match STARTS WITH prefix (note: multi-word operator)
    if let Ok((input_after_starts, (_, _, _, _, prefix))) = tuple((
        tag_no_case("STARTS"),
        multispace1,
        tag_no_case("WITH"),
        multispace0,
        string_literal,
    ))(input)
    {
        return Ok((
            input_after_starts,
            BooleanExpression::StartsWith {
                expression: left,
                prefix,
            },
        ));
    }
    // Match ENDS WITH suffix (note: multi-word operator)
    if let Ok((input_after_ends, (_, _, _, _, suffix))) = tuple((
        tag_no_case("ENDS"),
        multispace1,
        tag_no_case("WITH"),
        multispace0,
        string_literal,
    ))(input)
    {
        return Ok((
            input_after_ends,
            BooleanExpression::EndsWith {
                expression: left,
                suffix,
            },
        ));
    }
    // Match is null
    if let Ok((rest, ())) = is_null_comparison(input) {
        return Ok((rest, BooleanExpression::IsNull(left_clone)));
    }
    // Match is not null
    if let Ok((rest, ())) = is_not_null_comparison(input) {
        return Ok((rest, BooleanExpression::IsNotNull(left_clone)));
    }

    let (input, operator) = comparison_operator(input)?;
    let (input, _) = multispace0(input)?;
    let (mut input, mut right) = value_expression(input)?;
    let mut expr = BooleanExpression::Comparison {
        left: left_clone,
        operator,
        right: right.clone(),
    };
    // A chain `a < b <= c` means `a < b AND b <= c`.
    while let Ok((rest, (_, operator, _, next))) = tuple((
        multispace0,
        comparison_operator,
        multispace0,
        value_expression,
    ))(input)
    {
        expr = BooleanExpression::And(
            Box::new(expr),
            Box::new(BooleanExpression::Comparison {
                left: right,
                operator,
                right: next.clone(),
            }),
        );
        right = next;
        input = rest;
    }
    Ok((input, expr))
}

// Parse a comparison operator
fn comparison_operator(input: &str) -> IResult<&str, ComparisonOperator> {
    alt((
        map(tag("="), |_| ComparisonOperator::Equal),
        map(tag("<>"), |_| ComparisonOperator::NotEqual),
        map(tag("!="), |_| ComparisonOperator::NotEqual),
        map(tag("<="), |_| ComparisonOperator::LessThanOrEqual),
        map(tag(">="), |_| ComparisonOperator::GreaterThanOrEqual),
        map(tag("<"), |_| ComparisonOperator::LessThan),
        map(tag(">"), |_| ComparisonOperator::GreaterThan),
    ))(input)
}

// Parse a basic value expression (without vector functions to avoid circular dependency)
fn basic_value_expression(input: &str) -> IResult<&str, ValueExpression> {
    alt((
        parse_vector_literal, // Try vector literal first [0.1, 0.2]
        pattern_comprehension,
        list_comprehension,
        list_literal, // Then a list of any expressions [a.x, 'y'] (or [])
        map_literal,
        reduce_expression,
        list_predicate,
        parse_parameter,                               // Try $parameter
        function_call,                                 // Regular function calls
        map(property_value, ValueExpression::Literal), // Try literals BEFORE property references
        map(property_reference, ValueExpression::Property),
        map(identifier, |id| ValueExpression::Variable(id.to_string())),
    ))(input)
}

// A value where a predicate may also appear (projections, CASE, lists, arguments, maps):
// `path IS NULL`, `NOT (a)-[:R]-(b)`, `x IN list` are boolean values.
fn expression(input: &str) -> IResult<&str, ValueExpression> {
    alt((
        map(boolean_expression, |b| {
            ValueExpression::Boolean(Box::new(b))
        }),
        value_expression,
    ))(input)
}

// `[(a)-[:R]->(b) WHERE predicate | value]`
fn pattern_comprehension(input: &str) -> IResult<&str, ValueExpression> {
    let (input, _) = tuple((char('['), multispace0))(input)?;
    let (input, pattern) = path_pattern(input)?;
    let (input, filter) = opt(where_clause)(input)?;
    let (input, map) = preceded(tuple((multispace0, char('|'), multispace0)), expression)(input)?;
    let (input, _) = tuple((multispace0, char(']')))(input)?;
    Ok((
        input,
        ValueExpression::PatternComprehension {
            pattern: Box::new(pattern),
            filter: filter.map(|w| Box::new(w.expression)),
            map: Box::new(map),
        },
    ))
}

// `[x IN list WHERE predicate | value]`
fn list_comprehension(input: &str) -> IResult<&str, ValueExpression> {
    let (input, (_, _, variable, _, _, _, list)) = tuple((
        char('['),
        multispace0,
        identifier,
        multispace1,
        tag_no_case("IN"),
        multispace1,
        value_expression,
    ))(input)?;
    let (input, filter) = opt(where_clause)(input)?;
    let (input, map) = opt(preceded(
        tuple((multispace0, char('|'), multispace0)),
        expression,
    ))(input)?;
    let (input, _) = tuple((multispace0, char(']')))(input)?;
    Ok((
        input,
        ValueExpression::ListComprehension {
            variable: variable.to_string(),
            list: Box::new(list),
            filter: filter.map(|w| Box::new(w.expression)),
            map: map.map(Box::new),
        },
    ))
}

fn reduce_expression(input: &str) -> IResult<&str, ValueExpression> {
    let (input, _) = tuple((tag_no_case("reduce"), multispace0, char('('), multispace0))(input)?;
    let (input, accumulator) = identifier(input)?;
    let (input, initial) =
        preceded(tuple((multispace0, char('='), multispace0)), expression)(input)?;
    let (input, variable) = preceded(comma_ws, identifier)(input)?;
    let (input, list) = preceded(
        tuple((multispace1, tag_no_case("IN"), multispace1)),
        value_expression,
    )(input)?;
    let (input, mut body) =
        preceded(tuple((multispace0, char('|'), multispace0)), expression)(input)?;
    let (input, _) = tuple((multispace0, char(')')))(input)?;
    mark_locals(&mut body, &[accumulator, variable]);
    Ok((
        input,
        ValueExpression::Reduce {
            accumulator: accumulator.to_lowercase(),
            initial: Box::new(initial),
            variable: variable.to_lowercase(),
            list: Box::new(list),
            expression: Box::new(body),
        },
    ))
}

fn list_predicate(input: &str) -> IResult<&str, ValueExpression> {
    let (input, kind) = alt((
        tag_no_case("all"),
        tag_no_case("any"),
        tag_no_case("none"),
        tag_no_case("single"),
    ))(input)?;
    let (input, variable) =
        preceded(tuple((multispace0, char('('), multispace0)), identifier)(input)?;
    let (input, list) = preceded(
        tuple((multispace1, tag_no_case("IN"), multispace1)),
        value_expression,
    )(input)?;
    let (input, mut predicate) = preceded(
        tuple((multispace1, tag_no_case("WHERE"), multispace1)),
        expression,
    )(input)?;
    let (input, _) = tuple((multispace0, char(')')))(input)?;
    mark_locals(&mut predicate, &[variable]);
    Ok((
        input,
        ValueExpression::ListPredicate {
            kind: kind.to_lowercase(),
            variable: variable.to_lowercase(),
            list: Box::new(list),
            predicate: Box::new(predicate),
        },
    ))
}

// `{key: value, ...}`
fn map_literal(input: &str) -> IResult<&str, ValueExpression> {
    let entry = tuple((
        multispace0,
        identifier,
        multispace0,
        char(':'),
        multispace0,
        expression,
    ));
    let (input, entries) = delimited(
        tuple((char('{'), multispace0)),
        separated_list0(tuple((multispace0, char(','))), entry),
        tuple((multispace0, char('}'))),
    )(input)?;
    Ok((
        input,
        ValueExpression::Map(
            entries
                .into_iter()
                .map(|(_, k, _, _, _, v)| (k.to_string(), v))
                .collect(),
        ),
    ))
}

// Parse a value expression: arithmetic over primaries, `*` `/` `%` binding tighter than
// `+` `-`, all left-associative.
fn value_expression(input: &str) -> IResult<&str, ValueExpression> {
    arithmetic_level(
        input,
        &[
            ('+', ArithmeticOperator::Add),
            ('-', ArithmeticOperator::Subtract),
        ],
    )
}

fn arithmetic_level<'a>(
    input: &'a str,
    operators: &[(char, ArithmeticOperator)],
) -> IResult<&'a str, ValueExpression> {
    let tighter = |i: &'a str| {
        if operators[0].0 == '+' {
            arithmetic_level(
                i,
                &[
                    ('*', ArithmeticOperator::Multiply),
                    ('/', ArithmeticOperator::Divide),
                    ('%', ArithmeticOperator::Modulo),
                ],
            )
        } else {
            primary_value_expression(i)
        }
    };
    let (mut input, mut left) = tighter(input)?;
    loop {
        let Ok((after_op, (_, op, _))) = tuple((
            multispace0,
            one_of::<_, _, nom::error::Error<&str>>("+-*/%"),
            multispace0,
        ))(input) else {
            break;
        };
        let Some((_, operator)) = operators.iter().find(|(c, _)| *c == op) else {
            break;
        };
        // A right side that doesn't parse (e.g. `-[` of a pattern) ends the expression.
        let Ok((rest, right)) = tighter(after_op) else {
            break;
        };
        left = ValueExpression::Arithmetic {
            left: Box::new(left),
            operator: operator.clone(),
            right: Box::new(right),
        };
        input = rest;
    }
    Ok((input, left))
}

// `( expr )`, `CASE ... END`, or a basic value, then any `[index]` suffixes
fn primary_value_expression(input: &str) -> IResult<&str, ValueExpression> {
    let (mut input, mut value) = alt((
        delimited(
            tuple((char('('), multispace0)),
            value_expression,
            tuple((multispace0, char(')'))),
        ),
        case_expression,
        single_value_expression,
    ))(input)?;
    // `list[i]`, with the `[` right after the value (so `x IN [1, 2]` is not an index), and
    // `value.field`.
    loop {
        if let Ok((rest, index)) = delimited(
            tuple((char('['), multispace0)),
            value_expression,
            tuple((multispace0, char(']'))),
        )(input)
        {
            value = ValueExpression::ScalarFunction {
                name: "__index".to_string(),
                args: vec![value, index],
            };
            input = rest;
        } else if let Ok((rest, field)) = preceded(char('.'), identifier)(input) {
            value = ValueExpression::Field {
                base: Box::new(value),
                field: field.to_string(),
            };
            input = rest;
        } else {
            break;
        }
    }
    Ok((input, value))
}

// CASE [operand] WHEN ... THEN ... [ELSE ...] END
fn case_expression(input: &str) -> IResult<&str, ValueExpression> {
    let keyword = |k: &'static str| tuple((multispace0, tag_no_case(k), multispace1));
    let (input, _) = tuple((tag_no_case("CASE"), multispace1))(input)?;
    let (input, operand) = opt(terminated(expression, peek(keyword("WHEN"))))(input)?;
    let (input, whens) = match &operand {
        None => many1(tuple((
            preceded(keyword("WHEN"), boolean_expression),
            preceded(keyword("THEN"), expression),
        )))(input)?,
        Some(operand) => {
            let (input, pairs) = many1(tuple((
                preceded(keyword("WHEN"), expression),
                preceded(keyword("THEN"), expression),
            )))(input)?;
            let whens = pairs
                .into_iter()
                .map(|(value, then)| {
                    let condition = BooleanExpression::Comparison {
                        left: operand.clone(),
                        operator: ComparisonOperator::Equal,
                        right: value,
                    };
                    (condition, then)
                })
                .collect();
            (input, whens)
        }
    };
    let (input, otherwise) = opt(preceded(keyword("ELSE"), expression))(input)?;
    let (input, _) = tuple((multispace0, tag_no_case("END")))(input)?;
    Ok((
        input,
        ValueExpression::Case {
            whens,
            otherwise: otherwise.map(Box::new),
        },
    ))
}

// Optimization: Use peek to avoid expensive backtracking for non-vector queries
fn single_value_expression(input: &str) -> IResult<&str, ValueExpression> {
    // Peek at first identifier to dispatch to correct parser
    // This eliminates failed parser attempts for every non-vector expression
    if let Ok((_, first_ident)) = peek(identifier)(input) {
        let ident_lower = first_ident.to_lowercase();

        match ident_lower.as_str() {
            "vector_distance" => return parse_vector_distance(input),
            "vector_similarity" => return parse_vector_similarity(input),
            _ => {} // Not a vector function, continue to basic expressions
        }
    }

    // Fast path for common expressions
    basic_value_expression(input)
}

// Parse distance metric: cosine, l2, dot
fn parse_distance_metric(input: &str) -> IResult<&str, DistanceMetric> {
    alt((
        map(tag_no_case("cosine"), |_| DistanceMetric::Cosine),
        map(tag_no_case("l2"), |_| DistanceMetric::L2),
        map(tag_no_case("dot"), |_| DistanceMetric::Dot),
    ))(input)
}

// Parse vector_distance(expr, expr, metric)
fn parse_vector_distance(input: &str) -> IResult<&str, ValueExpression> {
    let (input, _) = tag_no_case("vector_distance")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse left expression - use basic_value_expression to avoid circular dependency
    let (input, left) = basic_value_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(',')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse right expression - use basic_value_expression to avoid circular dependency
    let (input, right) = basic_value_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(',')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse metric
    let (input, metric) = parse_distance_metric(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

    Ok((
        input,
        ValueExpression::VectorDistance {
            left: Box::new(left),
            right: Box::new(right),
            metric,
        },
    ))
}

// Parse vector_similarity(expr, expr, metric)
fn parse_vector_similarity(input: &str) -> IResult<&str, ValueExpression> {
    let (input, _) = tag_no_case("vector_similarity")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse left expression - use basic_value_expression to avoid circular dependency
    let (input, left) = basic_value_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(',')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse right expression - use basic_value_expression to avoid circular dependency
    let (input, right) = basic_value_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(',')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse metric
    let (input, metric) = parse_distance_metric(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

    Ok((
        input,
        ValueExpression::VectorSimilarity {
            left: Box::new(left),
            right: Box::new(right),
            metric,
        },
    ))
}

// Parse parameter reference: $name
fn parse_parameter(input: &str) -> IResult<&str, ValueExpression> {
    let (input, name) = parameter(input)?;
    Ok((input, ValueExpression::Parameter(name)))
}

// Parse a function call: function_name(args)
fn function_call(input: &str) -> IResult<&str, ValueExpression> {
    // Namespaced names such as `duration.inSeconds`.
    let (input, name) = recognize(pair(identifier, many0(preceded(char('.'), identifier))))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse optional DISTINCT keyword
    let (input, distinct) = opt(tag_no_case("DISTINCT"))(input)?;
    let distinct = distinct.is_some();
    let (input, _) = if distinct {
        multispace1(input)?
    } else {
        (input, "")
    };

    // Handle COUNT(*) special case - only allow * for COUNT function
    if let Ok((input_after_star, _)) = char::<_, nom::error::Error<&str>>('*')(input) {
        // Validate that this is COUNT function
        if name.to_lowercase() == "count" {
            let (input, _) = multispace0(input_after_star)?;
            let (input, _) = char(')')(input)?;
            return Ok((
                input,
                ValueExpression::AggregateFunction {
                    name: name.to_string(),
                    args: vec![ValueExpression::Variable("*".to_string())],
                    distinct,
                },
            ));
        } else {
            // Not COUNT - fail parsing to try regular argument parsing
            // This will naturally fail since * is not a valid value_expression
        }
    }

    // Parse regular function arguments
    let (input, args) =
        separated_list0(tuple((multispace0, char(','), multispace0)), expression)(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

    // Route based on function type
    use crate::ast::{classify_function, FunctionType};
    match classify_function(name) {
        FunctionType::Aggregate => Ok((
            input,
            ValueExpression::AggregateFunction {
                name: name.to_string(),
                args,
                distinct,
            },
        )),
        FunctionType::Scalar => {
            // Validate: reject DISTINCT on scalar functions at parse time
            if distinct {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            }
            Ok((
                input,
                ValueExpression::ScalarFunction {
                    name: name.to_string(),
                    args,
                },
            ))
        }
        FunctionType::Unknown => {
            // Default to ScalarFunction for unknown functions
            // They'll be handled as NULL in expression conversion
            if distinct {
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            }
            Ok((
                input,
                ValueExpression::ScalarFunction {
                    name: name.to_string(),
                    args,
                },
            ))
        }
    }
}

fn value_expression_list(input: &str) -> IResult<&str, Vec<ValueExpression>> {
    delimited(
        tuple((char('['), multispace0)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            value_expression,
        ),
        tuple((multispace0, char(']'))),
    )(input)
}

// Parse a float32 literal for vectors
fn float32_literal(input: &str) -> IResult<&str, f32> {
    map_res(
        recognize(tuple((
            opt(char('-')),
            alt((
                // Scientific notation: 1e-3, 2.5e2
                recognize(tuple((
                    digit1,
                    opt(tuple((char('.'), digit0))),
                    one_of("eE"),
                    opt(one_of("+-")),
                    digit1,
                ))),
                // Regular float: 1.23 or integer: 123
                recognize(tuple((digit1, opt(tuple((char('.'), digit0)))))),
            )),
        ))),
        |s: &str| s.parse::<f32>(),
    )(input)
}

// Parse vector literal: [0.1, 0.2, 0.3]
fn parse_vector_literal(input: &str) -> IResult<&str, ValueExpression> {
    let literal = input;
    let (input, _) = char('[')(input)?;
    let (input, _) = multispace0(input)?;

    let (input, values) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        float32_literal,
    )(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = char(']')(input)?;

    // Integer lists must retain Int64 precision outside vector operations.
    if !literal[..literal.len() - input.len()]
        .chars()
        .any(|c| matches!(c, '.' | 'e' | 'E'))
    {
        return Err(nom::Err::Error(nom::error::Error::new(
            literal,
            nom::error::ErrorKind::Verify,
        )));
    }

    Ok((input, ValueExpression::VectorLiteral(values)))
}

// `[expr, ...]` or `[]`: a list of any values (all-float lists parse as vectors first)
fn list_literal(input: &str) -> IResult<&str, ValueExpression> {
    let (input, items) = delimited(
        tuple((char('['), multispace0)),
        separated_list0(tuple((multispace0, char(','), multispace0)), expression),
        tuple((multispace0, char(']'))),
    )(input)?;
    Ok((
        input,
        ValueExpression::ScalarFunction {
            name: "__list".to_string(),
            args: items,
        },
    ))
}

// Parse a property reference: variable.property
fn property_reference(input: &str) -> IResult<&str, PropertyRef> {
    let (input, variable) = identifier(input)?;
    let (input, _) = char('.')(input)?;
    let (input, property) = identifier(input)?;

    Ok((
        input,
        PropertyRef {
            variable: variable.to_string(),
            property: property.to_string(),
        },
    ))
}

// Parse a WITH clause (intermediate projection/aggregation)
fn with_clause(input: &str) -> IResult<&str, WithClause> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("WITH")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, distinct) = opt(terminated(tag_no_case("DISTINCT"), multispace1))(input)?;
    let (input, items) = separated_list0(comma_ws, return_item)(input)?;
    let (input, order_by) = opt(order_by_clause)(input)?;
    let (input, limit) = opt(limit_clause)(input)?;
    let (input, where_clause) = opt(where_clause)(input)?;

    Ok((
        input,
        WithClause {
            distinct: distinct.is_some(),
            items,
            order_by,
            limit,
            where_clause,
        },
    ))
}

// A WITH after the first, with its own reading clauses and WHERE
fn with_stage(input: &str) -> IResult<&str, WithStage> {
    let (input, with_clause) = with_clause(input)?;
    let (input, reading_clauses) = many0(reading_clause)(input)?;
    let (input, where_clause) = opt(where_clause)(input)?;
    Ok((
        input,
        WithStage {
            with_clause,
            reading_clauses,
            where_clause,
        },
    ))
}

// Parse a RETURN clause
fn return_clause(input: &str) -> IResult<&str, ReturnClause> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("RETURN")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, distinct) = opt(tag_no_case("DISTINCT"))(input)?;
    let (input, _) = if distinct.is_some() {
        multispace1(input)?
    } else {
        (input, "")
    };
    let (input, items) = separated_list0(comma_ws, return_item)(input)?;

    Ok((
        input,
        ReturnClause {
            distinct: distinct.is_some(),
            items,
        },
    ))
}

// Parse a return item
fn return_item(input: &str) -> IResult<&str, ReturnItem> {
    let (input, expression) = expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, alias) = opt(preceded(
        tuple((tag_no_case("AS"), multispace1)),
        identifier,
    ))(input)?;

    Ok((
        input,
        ReturnItem {
            expression,
            alias: alias.map(|s| s.to_string()),
        },
    ))
}

// Match IS NULL in WHERE clause
fn is_null_comparison(input: &str) -> IResult<&str, ()> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("IS")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("NULL")(input)?;
    let (input, _) = multispace0(input)?;

    Ok((input, ()))
}

// Match IS NOT NULL in WHERE clause
fn is_not_null_comparison(input: &str) -> IResult<&str, ()> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("IS")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("NOT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("NULL")(input)?;
    let (input, _) = multispace0(input)?;

    Ok((input, ()))
}

// Parse an ORDER BY clause
fn order_by_clause(input: &str) -> IResult<&str, OrderByClause> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("ORDER")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("BY")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, items) = separated_list0(comma_ws, order_by_item)(input)?;

    Ok((input, OrderByClause { items }))
}

// Parse an order by item
fn order_by_item(input: &str) -> IResult<&str, OrderByItem> {
    let (input, expression) = value_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, direction) = opt(alt((
        map(tag_no_case("ASC"), |_| SortDirection::Ascending),
        map(tag_no_case("DESC"), |_| SortDirection::Descending),
    )))(input)?;

    Ok((
        input,
        OrderByItem {
            expression,
            direction: direction.unwrap_or(SortDirection::Ascending),
        },
    ))
}

// Parse a LIMIT clause
fn limit_clause(input: &str) -> IResult<&str, RowCount> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("LIMIT")(input)?;
    let (input, _) = multispace1(input)?;
    row_count(input)
}

// Parse a SKIP clause
fn skip_clause(input: &str) -> IResult<&str, RowCount> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("SKIP")(input)?;
    let (input, _) = multispace1(input)?;
    row_count(input)
}

fn row_count(input: &str) -> IResult<&str, RowCount> {
    if let Ok((remaining, name)) = parameter(input) {
        return Ok((remaining, RowCount::Parameter(name)));
    }
    let (remaining, value) = integer_literal(input)?;
    let value = u64::try_from(value).map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    Ok((remaining, RowCount::Literal(value)))
}

// Parse pagination clauses (SKIP and LIMIT)
fn pagination_clauses(input: &str) -> IResult<&str, (Option<RowCount>, Option<RowCount>)> {
    let (mut remaining, _) = multispace0(input)?;
    let mut skip: Option<RowCount> = None;
    let mut limit: Option<RowCount> = None;

    loop {
        let before = remaining;

        if skip.is_none() {
            if let Ok((i, s)) = skip_clause(remaining) {
                skip = Some(s);
                remaining = i;
                continue;
            }
        }

        if limit.is_none() {
            if let Ok((i, l)) = limit_clause(remaining) {
                limit = Some(l);
                remaining = i;
                continue;
            }
        }

        if before == remaining {
            break;
        }
    }

    Ok((remaining, (skip, limit)))
}

// Helper parsers

// Parse an identifier
fn identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)
}

// Parse a string literal
fn string_literal(input: &str) -> IResult<&str, String> {
    alt((double_quoted_string, single_quoted_string))(input)
}

fn double_quoted_string(input: &str) -> IResult<&str, String> {
    let (input, _) = char('"')(input)?;
    let (input, content) = take_while(|c| c != '"')(input)?;
    let (input, _) = char('"')(input)?;
    Ok((input, content.to_string()))
}

fn single_quoted_string(input: &str) -> IResult<&str, String> {
    let (input, _) = char('\'')(input)?;
    let (input, content) = take_while(|c| c != '\'')(input)?;
    let (input, _) = char('\'')(input)?;
    Ok((input, content.to_string()))
}

// Parse an integer literal
fn integer_literal(input: &str) -> IResult<&str, i64> {
    let (input, digits) = recognize(pair(
        opt(char('-')),
        take_while1(|c: char| c.is_ascii_digit()),
    ))(input)?;

    // Out-of-range literals fail to parse instead of panicking.
    let value = digits.parse().map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;
    Ok((input, value))
}

// Parse a float literal
fn float_literal(input: &str) -> IResult<&str, f64> {
    let (input, number) = recognize(tuple((
        opt(char('-')),
        take_while1(|c: char| c.is_ascii_digit()),
        char('.'),
        take_while1(|c: char| c.is_ascii_digit()),
    )))(input)?;

    let value: f64 = number.parse().map_err(|_| {
        nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Float))
    })?;
    if !value.is_finite() {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Float,
        )));
    }
    Ok((input, value))
}

// Parse a boolean literal
fn boolean_literal(input: &str) -> IResult<&str, bool> {
    alt((
        map(tag_no_case("true"), |_| true),
        map(tag_no_case("false"), |_| false),
    ))(input)
}

// Parse a parameter reference
fn parameter(input: &str) -> IResult<&str, String> {
    // Only support $param syntax
    map(preceded(char('$'), identifier), |s| s.to_string())(input)
}

// Parse comma with optional whitespace
fn comma_ws(input: &str) -> IResult<&str, ()> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char(',')(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, ()))
}

// Parse variable-length path syntax: *1..2, *..3, *2.., *
fn length_range(input: &str) -> IResult<&str, LengthRange> {
    let (input, _) = char('*')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse different length patterns
    alt((
        // *min..max (e.g., *1..3)
        map(
            tuple((
                nom::character::complete::u32,
                tag(".."),
                nom::character::complete::u32,
            )),
            |(min, _, max)| LengthRange {
                min: Some(min),
                max: Some(max),
            },
        ),
        // *..max (e.g., *..3)
        map(preceded(tag(".."), nom::character::complete::u32), |max| {
            LengthRange {
                min: None,
                max: Some(max),
            }
        }),
        // *min.. (e.g., *2..)
        map(
            tuple((nom::character::complete::u32, tag(".."))),
            |(min, _)| LengthRange {
                min: Some(min),
                max: None,
            },
        ),
        // *min (e.g., *2)
        map(nom::character::complete::u32, |min| LengthRange {
            min: Some(min),
            max: Some(min),
        }),
        // * (unlimited)
        map(multispace0, |_| LengthRange {
            min: None,
            max: None,
        }),
    ))(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{BooleanExpression, ComparisonOperator, PropertyValue, ValueExpression};

    #[test]
    fn test_parse_simple_node_query() {
        let query = "MATCH (n:Person) RETURN n.name";
        let result = parse_cypher_query(query).unwrap();

        assert_eq!(result.reading_clauses.len(), 1);
        assert_eq!(result.return_clause.items.len(), 1);
    }

    #[test]
    fn test_parse_node_with_properties() {
        let query = r#"MATCH (n:Person {name: "John", age: 30}) RETURN n"#;
        let result = parse_cypher_query(query).unwrap();

        if let ReadingClause::Match(match_clause) = &result.reading_clauses[0] {
            if let GraphPattern::Node(node) = &match_clause.patterns[0] {
                assert_eq!(node.labels, vec!["Person"]);
                assert_eq!(node.properties.len(), 2);
            } else {
                panic!("Expected node pattern");
            }
        } else {
            panic!("Expected match clause");
        }
    }

    #[test]
    fn test_parse_simple_relationship_query() {
        let query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name";
        let result = parse_cypher_query(query).unwrap();

        assert_eq!(result.reading_clauses.len(), 1);
        assert_eq!(result.return_clause.items.len(), 2);

        if let ReadingClause::Match(match_clause) = &result.reading_clauses[0] {
            if let GraphPattern::Path(path) = &match_clause.patterns[0] {
                assert_eq!(path.segments.len(), 1);
                assert_eq!(path.segments[0].relationship.types, vec!["KNOWS"]);
            } else {
                panic!("Expected path pattern");
            }
        } else {
            panic!("Expected match clause");
        }
    }

    #[test]
    fn test_parse_variable_length_path() {
        let query = "MATCH (a:Person)-[:FRIEND_OF*1..2]-(b:Person) RETURN a.name, b.name";
        let result = parse_cypher_query(query).unwrap();

        assert_eq!(result.reading_clauses.len(), 1);

        if let ReadingClause::Match(match_clause) = &result.reading_clauses[0] {
            if let GraphPattern::Path(path) = &match_clause.patterns[0] {
                assert_eq!(path.segments.len(), 1);
                assert_eq!(path.segments[0].relationship.types, vec!["FRIEND_OF"]);

                let length = path.segments[0].relationship.length.as_ref().unwrap();
                assert_eq!(length.min, Some(1));
                assert_eq!(length.max, Some(2));
            } else {
                panic!("Expected path pattern");
            }
        } else {
            panic!("Expected match clause");
        }
    }

    #[test]
    fn test_parse_query_with_where_clause() {
        let query = "MATCH (n:Person) WHERE n.age > 30 RETURN n.name";
        let result = parse_cypher_query(query).unwrap();

        assert!(result.where_clause.is_some());
    }

    #[test]
    fn test_parse_query_with_single_quoted_literal() {
        let query = "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name";
        let result = parse_cypher_query(query).unwrap();

        assert!(result.where_clause.is_some());
    }

    #[test]
    fn test_parse_query_with_and_conditions() {
        let query = "MATCH (src:Entity)-[rel:RELATIONSHIP]->(dst:Entity) WHERE rel.relationship_type = 'WORKS_ON' AND dst.name_lower = 'presto' RETURN src.name, src.entity_id";
        let result = parse_cypher_query(query).unwrap();

        let where_clause = result.where_clause.expect("Expected WHERE clause");
        match where_clause.expression {
            BooleanExpression::And(left, right) => {
                match *left {
                    BooleanExpression::Comparison {
                        left: ValueExpression::Property(ref prop),
                        operator,
                        right: ValueExpression::Literal(PropertyValue::String(ref value)),
                    } => {
                        assert_eq!(prop.variable, "rel");
                        assert_eq!(prop.property, "relationship_type");
                        assert_eq!(operator, ComparisonOperator::Equal);
                        assert_eq!(value, "WORKS_ON");
                    }
                    _ => panic!("Expected comparison for relationship_type filter"),
                }

                match *right {
                    BooleanExpression::Comparison {
                        left: ValueExpression::Property(ref prop),
                        operator,
                        right: ValueExpression::Literal(PropertyValue::String(ref value)),
                    } => {
                        assert_eq!(prop.variable, "dst");
                        assert_eq!(prop.property, "name_lower");
                        assert_eq!(operator, ComparisonOperator::Equal);
                        assert_eq!(value, "presto");
                    }
                    _ => panic!("Expected comparison for destination name filter"),
                }
            }
            other => panic!("Expected AND expression, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_query_with_in_clause() {
        let query = "MATCH (src:Entity)-[rel:RELATIONSHIP]->(dst:Entity) WHERE rel.relationship_type IN ['WORKS_FOR', 'PART_OF'] RETURN src.name";
        let result = parse_cypher_query(query).unwrap();

        let where_clause = result.where_clause.expect("Expected WHERE clause");
        match where_clause.expression {
            BooleanExpression::In { expression, list } => {
                match expression {
                    ValueExpression::Property(prop_ref) => {
                        assert_eq!(prop_ref.variable, "rel");
                        assert_eq!(prop_ref.property, "relationship_type");
                    }
                    _ => panic!("Expected property reference in IN expression"),
                }
                assert_eq!(list.len(), 2);
                match &list[0] {
                    ValueExpression::Literal(PropertyValue::String(val)) => {
                        assert_eq!(val, "WORKS_FOR");
                    }
                    _ => panic!("Expected first list item to be a string literal"),
                }
                match &list[1] {
                    ValueExpression::Literal(PropertyValue::String(val)) => {
                        assert_eq!(val, "PART_OF");
                    }
                    _ => panic!("Expected second list item to be a string literal"),
                }
            }
            other => panic!("Expected IN expression, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_query_with_is_null() {
        let query = "MATCH (n:Person) WHERE n.age IS NULL RETURN n.name";
        let result = parse_cypher_query(query).unwrap();

        let where_clause = result.where_clause.expect("Expected WHERE clause");

        match where_clause.expression {
            BooleanExpression::IsNull(expr) => match expr {
                ValueExpression::Property(prop_ref) => {
                    assert_eq!(prop_ref.variable, "n");
                    assert_eq!(prop_ref.property, "age");
                }
                _ => panic!("Expected property reference in IS NULL expression"),
            },
            other => panic!("Expected IS NULL expression, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_query_with_is_not_null() {
        let query = "MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n.name";
        let result = parse_cypher_query(query).unwrap();

        let where_clause = result.where_clause.expect("Expected WHERE clause");

        match where_clause.expression {
            BooleanExpression::IsNotNull(expr) => match expr {
                ValueExpression::Property(prop_ref) => {
                    assert_eq!(prop_ref.variable, "n");
                    assert_eq!(prop_ref.property, "age");
                }
                _ => panic!("Expected property reference in IS NOT NULL expression"),
            },
            other => panic!("Expected IS NOT NULL expression, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_query_with_limit() {
        let query = "MATCH (n:Person) RETURN n.name LIMIT 10";
        let result = parse_cypher_query(query).unwrap();

        assert_eq!(result.limit, Some(RowCount::Literal(10)));
    }

    #[test]
    fn test_parse_query_with_skip() {
        let query = "MATCH (n:Person) RETURN n.name SKIP 5";
        let result = parse_cypher_query(query).unwrap();

        assert_eq!(result.skip, Some(RowCount::Literal(5)));
        assert_eq!(result.limit, None);
    }

    #[test]
    fn test_parse_query_with_skip_and_limit() {
        let query = "MATCH (n:Person) RETURN n.name SKIP 5 LIMIT 10";
        let result = parse_cypher_query(query).unwrap();

        assert_eq!(result.skip, Some(RowCount::Literal(5)));
        assert_eq!(result.limit, Some(RowCount::Literal(10)));
    }

    #[test]
    fn test_parse_query_with_skip_and_order_by() {
        let query = "MATCH (n:Person) RETURN n.name ORDER BY n.age SKIP 5";
        let result = parse_cypher_query(query).unwrap();

        assert_eq!(result.skip, Some(RowCount::Literal(5)));
        assert!(result.order_by.is_some());
    }

    #[test]
    fn test_parse_query_with_skip_order_by_and_limit() {
        let query = "MATCH (n:Person) RETURN n.name ORDER BY n.age SKIP 5 LIMIT 10";
        let result = parse_cypher_query(query).unwrap();

        assert_eq!(result.skip, Some(RowCount::Literal(5)));
        assert_eq!(result.limit, Some(RowCount::Literal(10)));
        assert!(result.order_by.is_some());
    }

    #[test]
    fn test_parse_count_star() {
        let query = "MATCH (n:Person) RETURN count(*) AS total";
        let result = parse_cypher_query(query).unwrap();

        assert_eq!(result.return_clause.items.len(), 1);
        let item = &result.return_clause.items[0];
        assert_eq!(item.alias, Some("total".to_string()));

        match &item.expression {
            ValueExpression::AggregateFunction { name, args, .. } => {
                assert_eq!(name, "count");
                assert_eq!(args.len(), 1);
                match &args[0] {
                    ValueExpression::Variable(v) => assert_eq!(v, "*"),
                    _ => panic!("Expected Variable(*) in count(*)"),
                }
            }
            _ => panic!("Expected AggregateFunction expression"),
        }
    }

    #[test]
    fn test_parse_count_property() {
        let query = "MATCH (n:Person) RETURN count(n.age)";
        let result = parse_cypher_query(query).unwrap();

        assert_eq!(result.return_clause.items.len(), 1);
        let item = &result.return_clause.items[0];

        match &item.expression {
            ValueExpression::AggregateFunction { name, args, .. } => {
                assert_eq!(name, "count");
                assert_eq!(args.len(), 1);
                match &args[0] {
                    ValueExpression::Property(prop) => {
                        assert_eq!(prop.variable, "n");
                        assert_eq!(prop.property, "age");
                    }
                    _ => panic!("Expected Property in count(n.age)"),
                }
            }
            _ => panic!("Expected AggregateFunction expression"),
        }
    }

    #[test]
    fn test_parse_non_count_function_rejects_star() {
        // FOO(*) should fail to parse since * is only allowed for COUNT
        let query = "MATCH (n:Person) RETURN foo(*)";
        let result = parse_cypher_query(query);
        assert!(result.is_err(), "foo(*) should not parse successfully");
    }

    #[test]
    fn test_parse_count_with_multiple_args() {
        // COUNT with multiple arguments parses successfully
        // but will be rejected during semantic validation
        let query = "MATCH (n:Person) RETURN count(n.age, n.name)";
        let result = parse_cypher_query(query);
        assert!(
            result.is_ok(),
            "Parser should accept multiple args (validation happens in semantic phase)"
        );

        // Verify the AST structure
        let ast = result.unwrap();
        match &ast.return_clause.items[0].expression {
            ValueExpression::AggregateFunction { name, args, .. } => {
                assert_eq!(name, "count");
                assert_eq!(args.len(), 2);
            }
            _ => panic!("Expected AggregateFunction expression"),
        }
    }

    #[test]
    fn test_parser_rejects_distinct_on_scalar() {
        // Parser should reject DISTINCT on scalar functions at parse time
        let query = "RETURN toLower(DISTINCT p.name)";
        let result = parse_cypher_query(query);
        assert!(
            result.is_err(),
            "Parser should reject DISTINCT on scalar functions"
        );

        let query2 = "RETURN upper(DISTINCT p.name)";
        let result2 = parse_cypher_query(query2);
        assert!(
            result2.is_err(),
            "Parser should reject DISTINCT on scalar functions"
        );
    }

    #[test]
    fn test_parse_like_pattern() {
        let query = "MATCH (n:Person) WHERE n.name LIKE 'A%' RETURN n.name";
        let result = parse_cypher_query(query);
        assert!(result.is_ok(), "LIKE pattern should parse successfully");

        let ast = result.unwrap();
        let where_clause = ast.where_clause.expect("Expected WHERE clause");

        match where_clause.expression {
            BooleanExpression::Like {
                expression,
                pattern,
            } => {
                match expression {
                    ValueExpression::Property(prop) => {
                        assert_eq!(prop.variable, "n");
                        assert_eq!(prop.property, "name");
                    }
                    _ => panic!("Expected property expression"),
                }
                assert_eq!(pattern, "A%");
            }
            _ => panic!("Expected LIKE expression"),
        }
    }

    #[test]
    fn test_parse_like_with_double_quotes() {
        let query = r#"MATCH (n:Person) WHERE n.email LIKE "%@example.com" RETURN n.email"#;
        let result = parse_cypher_query(query);
        assert!(result.is_ok(), "LIKE with double quotes should parse");

        let ast = result.unwrap();
        let where_clause = ast.where_clause.expect("Expected WHERE clause");

        match where_clause.expression {
            BooleanExpression::Like { pattern, .. } => {
                assert_eq!(pattern, "%@example.com");
            }
            _ => panic!("Expected LIKE expression"),
        }
    }

    #[test]
    fn test_parse_like_in_complex_where() {
        let query = "MATCH (n:Person) WHERE n.age > 20 AND n.name LIKE 'J%' RETURN n.name";
        let result = parse_cypher_query(query);
        assert!(result.is_ok(), "LIKE in complex WHERE should parse");

        let ast = result.unwrap();
        let where_clause = ast.where_clause.expect("Expected WHERE clause");

        match where_clause.expression {
            BooleanExpression::And(left, right) => {
                // Left should be age > 20
                match *left {
                    BooleanExpression::Comparison { .. } => {}
                    _ => panic!("Expected comparison on left"),
                }
                // Right should be LIKE
                match *right {
                    BooleanExpression::Like { pattern, .. } => {
                        assert_eq!(pattern, "J%");
                    }
                    _ => panic!("Expected LIKE expression on right"),
                }
            }
            _ => panic!("Expected AND expression"),
        }
    }

    #[test]
    fn test_parse_contains() {
        let query = "MATCH (n:Person) WHERE n.name CONTAINS 'Jo' RETURN n.name";
        let result = parse_cypher_query(query);
        assert!(result.is_ok());

        let query = result.unwrap();
        assert!(query.where_clause.is_some());

        match &query.where_clause.unwrap().expression {
            BooleanExpression::Contains {
                expression,
                substring,
            } => {
                assert_eq!(substring, "Jo");
                match expression {
                    ValueExpression::Property(prop) => {
                        assert_eq!(prop.variable, "n");
                        assert_eq!(prop.property, "name");
                    }
                    _ => panic!("Expected property reference"),
                }
            }
            _ => panic!("Expected CONTAINS expression"),
        }
    }

    #[test]
    fn test_parse_starts_with() {
        let query = "MATCH (n:Person) WHERE n.name STARTS WITH 'Alice' RETURN n.name";
        let result = parse_cypher_query(query);
        assert!(result.is_ok());

        let query = result.unwrap();
        assert!(query.where_clause.is_some());

        match &query.where_clause.unwrap().expression {
            BooleanExpression::StartsWith { expression, prefix } => {
                assert_eq!(prefix, "Alice");
                match expression {
                    ValueExpression::Property(prop) => {
                        assert_eq!(prop.variable, "n");
                        assert_eq!(prop.property, "name");
                    }
                    _ => panic!("Expected property reference"),
                }
            }
            _ => panic!("Expected STARTS WITH expression"),
        }
    }

    #[test]
    fn test_parse_ends_with() {
        let query = "MATCH (n:Person) WHERE n.email ENDS WITH '@example.com' RETURN n.email";
        let result = parse_cypher_query(query);
        assert!(result.is_ok());

        let query = result.unwrap();
        assert!(query.where_clause.is_some());

        match &query.where_clause.unwrap().expression {
            BooleanExpression::EndsWith { expression, suffix } => {
                assert_eq!(suffix, "@example.com");
                match expression {
                    ValueExpression::Property(prop) => {
                        assert_eq!(prop.variable, "n");
                        assert_eq!(prop.property, "email");
                    }
                    _ => panic!("Expected property reference"),
                }
            }
            _ => panic!("Expected ENDS WITH expression"),
        }
    }

    #[test]
    fn test_parse_contains_case_insensitive_keyword() {
        let query = "MATCH (n:Person) WHERE n.name contains 'test' RETURN n.name";
        let result = parse_cypher_query(query);
        assert!(result.is_ok());

        match &result.unwrap().where_clause.unwrap().expression {
            BooleanExpression::Contains { substring, .. } => {
                assert_eq!(substring, "test");
            }
            _ => panic!("Expected CONTAINS expression"),
        }
    }

    #[test]
    fn test_parse_string_operators_in_complex_where() {
        let query =
            "MATCH (n:Person) WHERE n.name CONTAINS 'Jo' AND n.email ENDS WITH '.com' RETURN n";
        let result = parse_cypher_query(query);
        assert!(result.is_ok());

        match &result.unwrap().where_clause.unwrap().expression {
            BooleanExpression::And(left, right) => {
                // Left should be CONTAINS
                match **left {
                    BooleanExpression::Contains { ref substring, .. } => {
                        assert_eq!(substring, "Jo");
                    }
                    _ => panic!("Expected CONTAINS expression on left"),
                }
                // Right should be ENDS WITH
                match **right {
                    BooleanExpression::EndsWith { ref suffix, .. } => {
                        assert_eq!(suffix, ".com");
                    }
                    _ => panic!("Expected ENDS WITH expression on right"),
                }
            }
            _ => panic!("Expected AND expression"),
        }
    }

    #[test]
    fn test_parse_ilike_pattern() {
        let query = "MATCH (n:Person) WHERE n.name ILIKE 'alice%' RETURN n.name";
        let result = parse_cypher_query(query);
        assert!(result.is_ok(), "ILIKE pattern should parse successfully");

        let ast = result.unwrap();
        let where_clause = ast.where_clause.expect("Expected WHERE clause");

        match where_clause.expression {
            BooleanExpression::ILike {
                expression,
                pattern,
            } => {
                match expression {
                    ValueExpression::Property(prop) => {
                        assert_eq!(prop.variable, "n");
                        assert_eq!(prop.property, "name");
                    }
                    _ => panic!("Expected property expression"),
                }
                assert_eq!(pattern, "alice%");
            }
            _ => panic!("Expected ILIKE expression"),
        }
    }

    #[test]
    fn test_parse_like_and_ilike_together() {
        let query =
            "MATCH (n:Person) WHERE n.name LIKE 'Alice%' OR n.name ILIKE 'bob%' RETURN n.name";
        let result = parse_cypher_query(query);
        assert!(result.is_ok(), "LIKE and ILIKE together should parse");

        let ast = result.unwrap();
        let where_clause = ast.where_clause.expect("Expected WHERE clause");

        match where_clause.expression {
            BooleanExpression::Or(left, right) => {
                // Left should be LIKE (case-sensitive)
                match *left {
                    BooleanExpression::Like { pattern, .. } => {
                        assert_eq!(pattern, "Alice%");
                    }
                    _ => panic!("Expected LIKE expression on left"),
                }
                // Right should be ILIKE (case-insensitive)
                match *right {
                    BooleanExpression::ILike { pattern, .. } => {
                        assert_eq!(pattern, "bob%");
                    }
                    _ => panic!("Expected ILIKE expression on right"),
                }
            }
            _ => panic!("Expected OR expression"),
        }
    }

    #[test]
    fn test_parse_vector_distance() {
        let query = "MATCH (p:Person) WHERE vector_distance(p.embedding, $query_vec, cosine) < 0.5 RETURN p.name";
        let result = parse_cypher_query(query);
        assert!(result.is_ok(), "vector_distance should parse successfully");

        let ast = result.unwrap();
        let where_clause = ast.where_clause.expect("Expected WHERE clause");

        // Verify it's a comparison with vector_distance
        match where_clause.expression {
            BooleanExpression::Comparison { left, operator, .. } => {
                match left {
                    ValueExpression::VectorDistance {
                        left,
                        right,
                        metric,
                    } => {
                        assert_eq!(metric, DistanceMetric::Cosine);
                        // Verify left is property reference
                        assert!(matches!(*left, ValueExpression::Property(_)));
                        // Verify right is parameter
                        assert!(matches!(*right, ValueExpression::Parameter(_)));
                    }
                    _ => panic!("Expected VectorDistance"),
                }
                assert_eq!(operator, ComparisonOperator::LessThan);
            }
            _ => panic!("Expected comparison"),
        }
    }

    #[test]
    fn test_parse_vector_similarity() {
        let query =
            "MATCH (p:Person) WHERE vector_similarity(p.embedding, $vec, l2) > 0.8 RETURN p";
        let result = parse_cypher_query(query);
        assert!(
            result.is_ok(),
            "vector_similarity should parse successfully"
        );

        let ast = result.unwrap();
        let where_clause = ast.where_clause.expect("Expected WHERE clause");

        match where_clause.expression {
            BooleanExpression::Comparison { left, operator, .. } => {
                match left {
                    ValueExpression::VectorSimilarity { metric, .. } => {
                        assert_eq!(metric, DistanceMetric::L2);
                    }
                    _ => panic!("Expected VectorSimilarity"),
                }
                assert_eq!(operator, ComparisonOperator::GreaterThan);
            }
            _ => panic!("Expected comparison"),
        }
    }

    #[test]
    fn test_parse_parameter() {
        let query = "MATCH (p:Person) WHERE p.age = $min_age RETURN p";
        let result = parse_cypher_query(query);
        assert!(result.is_ok(), "Parameter should parse successfully");

        let ast = result.unwrap();
        let where_clause = ast.where_clause.expect("Expected WHERE clause");

        match where_clause.expression {
            BooleanExpression::Comparison { right, .. } => match right {
                ValueExpression::Parameter(name) => {
                    assert_eq!(name, "min_age");
                }
                _ => panic!("Expected Parameter"),
            },
            _ => panic!("Expected comparison"),
        }
    }

    #[test]
    fn test_parse_multiple_parameters() {
        let query = "MATCH (p:Person) WHERE p.age > $min_age AND p.age < $max_age RETURN p";
        let result = parse_cypher_query(query);
        assert!(
            result.is_ok(),
            "Multiple parameters should parse successfully"
        );

        let ast = result.unwrap();
        let where_clause = ast.where_clause.expect("Expected WHERE clause");

        match where_clause.expression {
            BooleanExpression::And(left, right) => {
                // Check left: p.age > $min_age
                match *left {
                    BooleanExpression::Comparison {
                        right: val_right, ..
                    } => match val_right {
                        ValueExpression::Parameter(name) => {
                            assert_eq!(name, "min_age");
                        }
                        _ => panic!("Expected Parameter min_age"),
                    },
                    _ => panic!("Expected comparison on left"),
                }

                // Check right: p.age < $max_age
                match *right {
                    BooleanExpression::Comparison {
                        right: val_right, ..
                    } => match val_right {
                        ValueExpression::Parameter(name) => {
                            assert_eq!(name, "max_age");
                        }
                        _ => panic!("Expected Parameter max_age"),
                    },
                    _ => panic!("Expected comparison on right"),
                }
            }
            _ => panic!("Expected AND expression"),
        }
    }

    #[test]
    fn test_parse_parameter_formats() {
        // Test $param (should succeed)
        let query = "MATCH (p:Person) WHERE p.age > $min_age RETURN p";
        let result = parse_cypher_query(query);
        assert!(result.is_ok(), "$param should parse successfully");
    }

    #[test]
    fn test_vector_distance_metrics() {
        for metric in &["cosine", "l2", "dot"] {
            let query = format!(
                "MATCH (p:Person) RETURN vector_distance(p.emb, $v, {}) AS dist",
                metric
            );
            let result = parse_cypher_query(&query);
            assert!(result.is_ok(), "Failed to parse metric: {}", metric);

            let ast = result.unwrap();
            let return_item = &ast.return_clause.items[0];

            match &return_item.expression {
                ValueExpression::VectorDistance {
                    metric: parsed_metric,
                    ..
                } => {
                    let expected = match *metric {
                        "cosine" => DistanceMetric::Cosine,
                        "l2" => DistanceMetric::L2,
                        "dot" => DistanceMetric::Dot,
                        _ => panic!("Unexpected metric"),
                    };
                    assert_eq!(*parsed_metric, expected);
                }
                _ => panic!("Expected VectorDistance"),
            }
        }
    }

    #[test]
    fn test_vector_search_in_order_by() {
        let query = "MATCH (p:Person) RETURN p.name ORDER BY vector_distance(p.embedding, $query_vec, cosine) ASC LIMIT 10";
        let result = parse_cypher_query(query);
        assert!(result.is_ok(), "vector_distance in ORDER BY should parse");

        let ast = result.unwrap();
        let order_by = ast.order_by.expect("Expected ORDER BY clause");

        assert_eq!(order_by.items.len(), 1);
        match &order_by.items[0].expression {
            ValueExpression::VectorDistance { .. } => {
                // Success
            }
            _ => panic!("Expected VectorDistance in ORDER BY"),
        }
    }

    #[test]
    fn test_hybrid_query_with_vector_and_property_filters() {
        let query = "MATCH (p:Person) WHERE p.age > 25 AND vector_similarity(p.embedding, $query_vec, cosine) > 0.7 RETURN p.name";
        let result = parse_cypher_query(query);
        assert!(result.is_ok(), "Hybrid query should parse");

        let ast = result.unwrap();
        let where_clause = ast.where_clause.expect("Expected WHERE clause");

        // Should be an AND expression
        match where_clause.expression {
            BooleanExpression::And(left, right) => {
                // Left should be age > 25
                match *left {
                    BooleanExpression::Comparison { .. } => {}
                    _ => panic!("Expected comparison on left"),
                }
                // Right should be vector_similarity > 0.7
                match *right {
                    BooleanExpression::Comparison { left, .. } => match left {
                        ValueExpression::VectorSimilarity { .. } => {}
                        _ => panic!("Expected VectorSimilarity"),
                    },
                    _ => panic!("Expected comparison on right"),
                }
            }
            _ => panic!("Expected AND expression"),
        }
    }

    #[test]
    fn pagination_accepts_parameters_without_losing_names() {
        let query = parse_cypher_query("RETURN 1 AS n SKIP $offset LIMIT $count").unwrap();
        assert_eq!(query.skip, Some(RowCount::Parameter("offset".to_string())));
        assert_eq!(query.limit, Some(RowCount::Parameter("count".to_string())));
        assert!(parse_cypher_query("RETURN 1 LIMIT -1").is_err());
        assert!(parse_cypher_query("RETURN 1 LIMIT 1.5").is_err());
    }

    #[test]
    fn test_integer_lists_preserve_precision() {
        for literal in ["[1, 2, 3]", "[-19791209300775, 16777217]", "[]"] {
            assert!(parse_vector_literal(literal).is_err());
            let (remaining, expression) = value_expression(literal).unwrap();
            assert!(remaining.is_empty());
            let ValueExpression::ScalarFunction { name, args } = expression else {
                panic!("expected an ordinary list");
            };
            assert_eq!(name, "__list");
            assert!(args
                .iter()
                .all(|v| matches!(v, ValueExpression::Literal(PropertyValue::Integer(_)))));
        }
        let (_, expression) = value_expression("[19791209300775]").unwrap();
        assert_eq!(
            expression,
            ValueExpression::ScalarFunction {
                name: "__list".to_string(),
                args: vec![ValueExpression::Literal(PropertyValue::Integer(
                    19791209300775
                ))],
            }
        );
        for literal in ["[1, 2.0]", "[1e-3, 2]", "[-1.0, 2]"] {
            assert!(matches!(
                value_expression(literal).unwrap().1,
                ValueExpression::VectorLiteral(_)
            ));
        }
    }

    #[test]
    fn test_parse_vector_literal() {
        let result = parse_vector_literal("[0.1, 0.2, 0.3]");
        assert!(result.is_ok());
        let (_, expr) = result.unwrap();
        match expr {
            ValueExpression::VectorLiteral(vec) => {
                assert_eq!(vec.len(), 3);
                assert_eq!(vec[0], 0.1);
                assert_eq!(vec[1], 0.2);
                assert_eq!(vec[2], 0.3);
            }
            _ => panic!("Expected VectorLiteral"),
        }
    }

    #[test]
    fn test_parse_vector_literal_with_negative_values() {
        let result = parse_vector_literal("[-0.1, 0.2, -0.3]");
        assert!(result.is_ok());
        let (_, expr) = result.unwrap();
        match expr {
            ValueExpression::VectorLiteral(vec) => {
                assert_eq!(vec.len(), 3);
                assert_eq!(vec[0], -0.1);
                assert_eq!(vec[2], -0.3);
            }
            _ => panic!("Expected VectorLiteral"),
        }
    }

    #[test]
    fn test_parse_vector_literal_scientific_notation() {
        let result = parse_vector_literal("[1e-3, 2.5e2, -3e-1]");
        assert!(result.is_ok());
        let (_, expr) = result.unwrap();
        match expr {
            ValueExpression::VectorLiteral(vec) => {
                assert_eq!(vec.len(), 3);
                assert!((vec[0] - 0.001).abs() < 1e-6);
                assert!((vec[1] - 250.0).abs() < 1e-6);
                assert!((vec[2] - (-0.3)).abs() < 1e-6);
            }
            _ => panic!("Expected VectorLiteral"),
        }
    }

    #[test]
    fn test_vector_distance_with_literal() {
        let query =
            "MATCH (p:Person) WHERE vector_distance(p.embedding, [0.1, 0.2], l2) < 0.5 RETURN p";
        let result = parse_cypher_query(query);
        assert!(result.is_ok());

        let ast = result.unwrap();
        let where_clause = ast.where_clause.expect("Expected WHERE clause");

        match where_clause.expression {
            BooleanExpression::Comparison { left, operator, .. } => {
                match left {
                    ValueExpression::VectorDistance {
                        left,
                        right,
                        metric,
                    } => {
                        // Left should be property reference
                        assert!(matches!(*left, ValueExpression::Property(_)));
                        // Right should be vector literal
                        match *right {
                            ValueExpression::VectorLiteral(vec) => {
                                assert_eq!(vec.len(), 2);
                                assert_eq!(vec[0], 0.1);
                                assert_eq!(vec[1], 0.2);
                            }
                            _ => panic!("Expected VectorLiteral"),
                        }
                        assert_eq!(metric, DistanceMetric::L2);
                    }
                    _ => panic!("Expected VectorDistance"),
                }
                assert_eq!(operator, ComparisonOperator::LessThan);
            }
            _ => panic!("Expected comparison"),
        }
    }

    // UNWIND parser tests
    #[test]
    fn test_parse_unwind_simple() {
        let query = "UNWIND [1, 2, 3] AS num RETURN num";
        let ast = parse_cypher_query(query);
        assert!(ast.is_ok(), "Failed to parse simple UNWIND query");
    }

    #[test]
    fn test_parse_unwind_after_match() {
        let query = "MATCH (n) UNWIND n.list AS item RETURN item";
        let ast = parse_cypher_query(query);
        assert!(ast.is_ok(), "Failed to parse UNWIND after MATCH");
    }

    #[test]
    fn out_of_range_numbers_are_parse_errors_not_panics() {
        for q in [
            "MATCH (n:Person) RETURN n LIMIT 99999999999999999999",
            "MATCH (n:Person) RETURN n LIMIT -1",
            "MATCH (n:Person) RETURN n SKIP -1",
            "MATCH (n:Person) WHERE n.age > 99999999999999999999 RETURN n",
        ] {
            assert!(parse_cypher_query(q).is_err(), "{q}");
        }
        let huge = format!(
            "MATCH (n:Person) WHERE n.x > {}.0 RETURN n",
            "9".repeat(400)
        );
        assert!(parse_cypher_query(&huge).is_err());
    }

    #[test]
    fn relationship_type_alternation() {
        for q in [
            "MATCH (a)-[:A|B]->(b) RETURN b",
            "MATCH (a)-[:A|:B]->(b) RETURN b",
            "MATCH (a)-[r:A | B *1..2]->(b) RETURN b",
            "MATCH (a)-[:A:B]->(b) RETURN b",
        ] {
            let query = parse_cypher_query(q).unwrap_or_else(|e| panic!("{q}: {e}"));
            let ReadingClause::Match(m) = &query.reading_clauses[0] else {
                panic!("{q}: not a MATCH");
            };
            let GraphPattern::Path(path) = &m.patterns[0] else {
                panic!("{q}: not a path");
            };
            assert_eq!(path.segments[0].relationship.types, vec!["A", "B"], "{q}");
        }
    }

    #[test]
    fn chained_with_stages_and_with_distinct() {
        let q = "MATCH (a)-[:R]->(b) WITH DISTINCT b WHERE b.x > 1 \
                 MATCH (b)-[:R]->(c) WITH c, count(*) AS n WITH c WHERE n > 2 RETURN c";
        let query = parse_cypher_query(q).unwrap();
        let first = query.with_clause.as_ref().unwrap();
        assert!(first.distinct && first.where_clause.is_some());
        assert_eq!(query.post_with_reading_clauses.len(), 1);
        assert_eq!(query.further_with_stages.len(), 2);
        let [second, third] = &query.further_with_stages[..] else {
            unreachable!()
        };
        assert!(!second.with_clause.distinct && second.reading_clauses.is_empty());
        assert!(third.with_clause.where_clause.is_some());
    }

    #[test]
    fn optional_match_owns_its_where_and_inner_where_attaches_to_match() {
        let q = "MATCH (a) WHERE a.x > 1 OPTIONAL MATCH (a)-[:R]->(b) WHERE b.y = 2 \
                 MATCH (a)-[:S]->(c) WHERE c.z = 3 RETURN a";
        let query = parse_cypher_query(q).unwrap();
        let clauses: Vec<&MatchClause> = query
            .reading_clauses
            .iter()
            .map(|c| match c {
                ReadingClause::Match(m) => m,
                ReadingClause::Unwind(_) => unreachable!(),
            })
            .collect();
        assert_eq!(clauses.len(), 3);
        assert!(!clauses[0].optional && clauses[0].where_clause.is_some());
        assert!(clauses[1].optional && clauses[1].where_clause.is_some());
        // The last plain MATCH's WHERE stays in the query's slot (re-rooting reads it).
        assert!(clauses[2].where_clause.is_none() && query.where_clause.is_some());
    }

    #[test]
    fn null_literal_any_case_but_not_a_name_prefix() {
        for q in [
            "MATCH (n) RETURN NULL AS x",
            "MATCH (n) RETURN null AS x",
            "MATCH (n) RETURN Null AS x",
        ] {
            let query = parse_cypher_query(q).unwrap();
            assert_eq!(
                query.return_clause.items[0].expression,
                ValueExpression::Literal(PropertyValue::Null),
                "{q}"
            );
        }
        let query = parse_cypher_query("MATCH (n) WITH 1 AS nullCount RETURN nullCount").unwrap();
        assert_eq!(
            query.return_clause.items[0].expression,
            ValueExpression::Variable("nullCount".into())
        );
    }
    #[test]
    fn reduce_keeps_inputs_in_outer_scope_and_marks_local_properties() {
        let q =
            parse_cypher_query("WITH {n:1} AS x RETURN reduce(s=x.n, x IN [x] | s+x.n)").unwrap();
        let ValueExpression::Reduce {
            initial,
            list,
            expression,
            ..
        } = &q.return_clause.items[0].expression
        else {
            panic!("expected reduce");
        };
        assert!(matches!(initial.as_ref(), ValueExpression::Property(p) if p.variable == "x"));
        let mut input_locals = 0;
        transform_value(&mut list.as_ref().clone(), &mut |v| {
            input_locals += usize::from(matches!(v, ValueExpression::LocalVariable(_)))
        });
        assert_eq!(input_locals, 0);
        let mut body_locals = Vec::new();
        transform_value(&mut expression.as_ref().clone(), &mut |v| {
            if let ValueExpression::LocalVariable(n) = v {
                body_locals.push(n.clone());
            }
        });
        assert_eq!(body_locals, ["s", "x"]);
    }
}
