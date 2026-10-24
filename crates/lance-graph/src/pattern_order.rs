// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Choose where a MATCH clause's patterns start, so traversal begins at the most
//! selective node.
//!
//! The logical planner scans each path's first node and expands left to right, so
//! `MATCH (f:Forum)-[:hasMember]->(p:Person) WHERE p.id = 42` expands every forum's
//! memberships before the filter on `p` runs. The join path partly recovers because
//! DataFusion pushes the filter below the join, but a CSR expansion cannot.
//! [`reorder_patterns`] rewrites the patterns into an
//! equivalent traversal that starts at the best-scoring node, walking segments
//! backwards where needed (an outgoing edge walked from its target is incoming).
//!
//! Semantics are unchanged: the same edges with the same endpoints, labels,
//! relationship properties and variables are matched, and the WHERE clause is still
//! applied in full above the match. Only the traversal order differs.

use std::collections::{HashMap, HashSet};

use crate::ast::{
    BooleanExpression, ComparisonOperator, GraphPattern, NodePattern, PathPattern, PathSegment,
    RelationshipDirection, RelationshipPattern, ValueExpression,
};
use crate::config::GraphConfig;

/// How strongly the conditions on one node narrow it down. Ordered: higher is more selective.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Selectivity {
    None,
    /// `IS NULL`, `IS NOT NULL`, `<>`, `NOT ...`
    Weak,
    /// Ranges, `IN`, `CONTAINS`, `STARTS WITH`, `LIKE`, ...
    Range,
    /// Equality on a non-id property
    Equality,
    /// Equality on the node's id column
    IdEquality,
}

/// The patterns rewritten to start at their most selective node, or `None` to keep them.
///
/// Returns `None` (plan as written) when no node beats the textual start, or when
/// the patterns are outside what this rewrite handles: variable-length segments,
/// more than one connected component, or bare node patterns.
pub(crate) fn reorder_patterns(
    patterns: &[GraphPattern],
    filter: Option<&BooleanExpression>,
    config: &GraphConfig,
) -> Option<Vec<GraphPattern>> {
    let graph = PatternGraph::from_patterns(patterns)?;
    if graph.edges.is_empty() || !graph.is_connected() {
        return None;
    }

    let mut scores: HashMap<&str, Selectivity> = HashMap::new();
    for node in &graph.nodes {
        let id_field = node
            .labels
            .first()
            .and_then(|label| config.get_node_mapping(label))
            .map(|m| m.id_field.as_str());
        let inline = node.properties.keys().map(|property| match id_field {
            Some(id) if id.eq_ignore_ascii_case(property) => Selectivity::IdEquality,
            _ => Selectivity::Equality,
        });
        let filtered = filter
            .into_iter()
            .flat_map(conjuncts)
            .filter_map(|conjunct| score_conjunct(conjunct, name(node), id_field));
        let best = inline.chain(filtered).max().unwrap_or(Selectivity::None);
        scores.insert(name(node), best);
    }

    // Ties keep the earliest node in the text, so equal scores never reorder.
    let textual_start = name(&graph.nodes[0]);
    let start = graph
        .nodes
        .iter()
        .map(name)
        .fold(textual_start, |best, candidate| {
            if scores[candidate] > scores[best] {
                candidate
            } else {
                best
            }
        });
    if start == textual_start {
        return None;
    }
    Some(graph.traverse_from(start))
}

/// A graph node's variable; [`PatternGraph`] names every node, anonymous ones included.
fn name(node: &NodePattern) -> &str {
    node.variable
        .as_deref()
        .expect("PatternGraph names every node")
}

/// The top-level `AND` conjuncts of a predicate.
fn conjuncts(expr: &BooleanExpression) -> Vec<&BooleanExpression> {
    match expr {
        BooleanExpression::And(left, right) => {
            let mut out = conjuncts(left);
            out.extend(conjuncts(right));
            out
        }
        other => vec![other],
    }
}

/// Selectivity of `conjunct` for `variable`, if it constrains only that variable.
fn score_conjunct(
    conjunct: &BooleanExpression,
    variable: &str,
    id_field: Option<&str>,
) -> Option<Selectivity> {
    let mut variables = HashSet::new();
    boolean_variables(conjunct, &mut variables);
    if variables.len() != 1 || !variables.contains(variable) {
        return None;
    }
    Some(match conjunct {
        BooleanExpression::PatternExists { .. } => return None,
        BooleanExpression::Comparison {
            left,
            operator: ComparisonOperator::Equal,
            right,
        } => {
            let property = match (left, right) {
                (ValueExpression::Property(p), other) | (other, ValueExpression::Property(p))
                    if is_constant(other) =>
                {
                    Some(p.property.as_str())
                }
                _ => None,
            };
            match (property, id_field) {
                (Some(p), Some(id)) if p.eq_ignore_ascii_case(id) => Selectivity::IdEquality,
                (Some(_), _) => Selectivity::Equality,
                (None, _) => Selectivity::Range,
            }
        }
        BooleanExpression::Comparison {
            operator: ComparisonOperator::NotEqual,
            ..
        } => Selectivity::Weak,
        BooleanExpression::Comparison { .. }
        | BooleanExpression::In { .. }
        | BooleanExpression::Member { .. }
        | BooleanExpression::Like { .. }
        | BooleanExpression::ILike { .. }
        | BooleanExpression::Contains { .. }
        | BooleanExpression::StartsWith { .. }
        | BooleanExpression::EndsWith { .. }
        | BooleanExpression::Or(..) => Selectivity::Range,
        BooleanExpression::Not(_)
        | BooleanExpression::IsNull(_)
        | BooleanExpression::IsNotNull(_)
        | BooleanExpression::Exists(_)
        | BooleanExpression::And(..) => Selectivity::Weak,
    })
}

fn is_constant(expr: &ValueExpression) -> bool {
    matches!(
        expr,
        ValueExpression::Literal(_) | ValueExpression::Parameter(_)
    )
}

pub(crate) fn boolean_variables<'a>(expr: &'a BooleanExpression, out: &mut HashSet<&'a str>) {
    match expr {
        // A subquery says nothing about how selective one variable is.
        BooleanExpression::PatternExists { .. } => {}
        BooleanExpression::Comparison { left, right, .. } => {
            value_variables(left, out);
            value_variables(right, out);
        }
        BooleanExpression::And(a, b) | BooleanExpression::Or(a, b) => {
            boolean_variables(a, out);
            boolean_variables(b, out);
        }
        BooleanExpression::Not(inner) => boolean_variables(inner, out),
        BooleanExpression::Exists(p) => {
            out.insert(p.variable.as_str());
        }
        BooleanExpression::In { expression, list } => {
            value_variables(expression, out);
            list.iter().for_each(|v| value_variables(v, out));
        }
        BooleanExpression::Member { expression, list } => {
            value_variables(expression, out);
            value_variables(list, out);
        }
        BooleanExpression::Like { expression, .. }
        | BooleanExpression::ILike { expression, .. }
        | BooleanExpression::Contains { expression, .. }
        | BooleanExpression::StartsWith { expression, .. }
        | BooleanExpression::EndsWith { expression, .. }
        | BooleanExpression::IsNull(expression)
        | BooleanExpression::IsNotNull(expression) => value_variables(expression, out),
    }
}

fn value_variables<'a>(expr: &'a ValueExpression, out: &mut HashSet<&'a str>) {
    match expr {
        ValueExpression::Variable(v) => {
            out.insert(v.as_str());
        }
        ValueExpression::Property(p) => {
            out.insert(p.variable.as_str());
        }
        ValueExpression::ScalarFunction { args, .. }
        | ValueExpression::AggregateFunction { args, .. } => {
            args.iter().for_each(|a| value_variables(a, out));
        }
        ValueExpression::Arithmetic { left, right, .. }
        | ValueExpression::VectorDistance { left, right, .. }
        | ValueExpression::VectorSimilarity { left, right, .. } => {
            value_variables(left, out);
            value_variables(right, out);
        }
        ValueExpression::Case { whens, otherwise } => {
            for (condition, then) in whens {
                boolean_variables(condition, out);
                value_variables(then, out);
            }
            if let Some(otherwise) = otherwise {
                value_variables(otherwise, out);
            }
        }
        ValueExpression::Literal(_)
        | ValueExpression::Parameter(_)
        | ValueExpression::VectorLiteral(_) => {}
    }
}

/// The node and relationship variables written in `patterns`.
pub(crate) fn pattern_variables(patterns: &[GraphPattern]) -> HashSet<&str> {
    let mut out = HashSet::new();
    for pattern in patterns {
        match pattern {
            GraphPattern::Node(n) => out.extend(n.variable.as_deref()),
            GraphPattern::Path(p) => {
                out.extend(p.start_node.variable.as_deref());
                for s in &p.segments {
                    out.extend(s.relationship.variable.as_deref());
                    out.extend(s.end_node.variable.as_deref());
                }
            }
        }
    }
    out
}

/// A MATCH clause's patterns as nodes (in first-mention order) and edges.
struct PatternGraph {
    /// One entry per variable, merging labels and inline properties from every mention.
    nodes: Vec<NodePattern>,
    edges: Vec<Edge>,
}

struct Edge {
    from: String,
    relationship: RelationshipPattern,
    to: String,
}

impl PatternGraph {
    /// `None` for shapes this rewrite does not handle: bare node patterns and
    /// variable-length segments.
    fn from_patterns(patterns: &[GraphPattern]) -> Option<Self> {
        let mut graph = Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        };
        for pattern in patterns {
            let GraphPattern::Path(path) = pattern else {
                return None;
            };
            let mut from = graph.add_node(&path.start_node);
            for segment in &path.segments {
                if segment.relationship.length.is_some() {
                    return None;
                }
                let to = graph.add_node(&segment.end_node);
                graph.edges.push(Edge {
                    from,
                    relationship: segment.relationship.clone(),
                    to: to.clone(),
                });
                from = to;
            }
        }
        Some(graph)
    }

    /// Register a mention of a node and return its variable, naming anonymous nodes.
    fn add_node(&mut self, node: &NodePattern) -> String {
        let variable = node
            .variable
            .clone()
            .unwrap_or_else(|| format!("_anon_{}", self.nodes.len()));
        match self
            .nodes
            .iter_mut()
            .find(|n| n.variable.as_deref() == Some(&variable))
        {
            Some(existing) => {
                if existing.labels.is_empty() {
                    existing.labels = node.labels.clone();
                }
                existing.properties.extend(node.properties.clone());
            }
            None => self.nodes.push(NodePattern {
                variable: Some(variable.clone()),
                ..node.clone()
            }),
        }
        variable
    }

    fn is_connected(&self) -> bool {
        let Some(first) = self.nodes.first().and_then(|n| n.variable.as_deref()) else {
            return true;
        };
        let mut reached: HashSet<&str> = HashSet::from([first]);
        let mut changed = true;
        while changed {
            changed = false;
            for edge in &self.edges {
                let (a, b) = (edge.from.as_str(), edge.to.as_str());
                if reached.contains(a) != reached.contains(b) {
                    reached.insert(a);
                    reached.insert(b);
                    changed = true;
                }
            }
        }
        reached.len() == self.nodes.len()
    }

    /// Re-emit every edge as paths that each start at an already-bound variable,
    /// beginning at `start`. Each path keeps extending from its last node while an
    /// unused edge touches it; edges are otherwise taken in their textual order.
    fn traverse_from(&self, start: &str) -> Vec<GraphPattern> {
        let mut bound: HashSet<String> = HashSet::from([start.to_string()]);
        let mut used = vec![false; self.edges.len()];
        let mut paths = Vec::new();
        // Inline properties are applied where a node is first bound, once.
        let first_mention = |variable: &str, bound: &HashSet<String>| -> NodePattern {
            let node = self.node(variable);
            if bound.contains(variable) {
                NodePattern {
                    properties: HashMap::new(),
                    ..node.clone()
                }
            } else {
                node.clone()
            }
        };

        let mut path_start = start.to_string();
        let mut start_node = self.node(start).clone();
        while used.iter().any(|u| !u) {
            let mut segments = Vec::new();
            let mut current = path_start.clone();
            while let Some(i) = self.next_edge(&current, &used) {
                used[i] = true;
                let edge = &self.edges[i];
                let (relationship, next) = if edge.from == current {
                    (edge.relationship.clone(), edge.to.clone())
                } else {
                    (reversed(&edge.relationship), edge.from.clone())
                };
                let end_node = first_mention(&next, &bound);
                bound.insert(next.clone());
                segments.push(PathSegment {
                    relationship,
                    end_node,
                });
                current = next;
            }
            if !segments.is_empty() {
                paths.push(GraphPattern::Path(PathPattern {
                    start_node,
                    segments,
                    shortest: None,
                }));
            }
            // The next path starts at a bound node that still has an unused edge.
            let Some(next_start) = self
                .edges
                .iter()
                .zip(&used)
                .filter(|(_, used)| !**used)
                .find_map(|(e, _)| {
                    [&e.from, &e.to]
                        .into_iter()
                        .find(|v| bound.contains(v.as_str()))
                        .cloned()
                })
            else {
                break;
            };
            start_node = first_mention(&next_start, &bound);
            path_start = next_start;
        }
        paths
    }

    /// The first unused edge touching `variable`, in textual order.
    fn next_edge(&self, variable: &str, used: &[bool]) -> Option<usize> {
        self.edges
            .iter()
            .enumerate()
            .find(|(i, e)| !used[*i] && (e.from == variable || e.to == variable))
            .map(|(i, _)| i)
    }

    fn node(&self, variable: &str) -> &NodePattern {
        self.nodes
            .iter()
            .find(|n| n.variable.as_deref() == Some(variable))
            .expect("every edge endpoint is a registered node")
    }
}

/// The same relationship pattern read from its other end.
pub(crate) fn reversed(relationship: &RelationshipPattern) -> RelationshipPattern {
    let direction = match relationship.direction {
        RelationshipDirection::Outgoing => RelationshipDirection::Incoming,
        RelationshipDirection::Incoming => RelationshipDirection::Outgoing,
        RelationshipDirection::Undirected => RelationshipDirection::Undirected,
    };
    RelationshipPattern {
        direction,
        ..relationship.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_cypher_query;

    fn config() -> GraphConfig {
        GraphConfig::builder()
            .with_node_label("Forum", "id")
            .with_node_label("Person", "id")
            .with_node_label("Tag", "id")
            .with_node_label("Tagclass", "id")
            .with_relationship("hasMember", "src", "dst")
            .with_relationship("forumHasTag", "src", "dst")
            .with_relationship("hasType", "src", "dst")
            .build()
            .unwrap()
    }

    fn reorder(cypher: &str) -> Option<String> {
        let ast = parse_cypher_query(cypher).unwrap();
        let crate::ast::ReadingClause::Match(clause) = &ast.reading_clauses[0] else {
            panic!("expected MATCH");
        };
        let filter = ast.where_clause.as_ref().map(|w| &w.expression);
        reorder_patterns(&clause.patterns, filter, &config()).map(|ps| describe(&ps))
    }

    /// `a>b,b<c` style summary: start variable then each hop's direction and end.
    fn describe(patterns: &[GraphPattern]) -> String {
        patterns
            .iter()
            .map(|p| {
                let GraphPattern::Path(path) = p else {
                    unreachable!()
                };
                let mut s = path.start_node.variable.clone().unwrap();
                for seg in &path.segments {
                    s.push(match seg.relationship.direction {
                        RelationshipDirection::Outgoing => '>',
                        RelationshipDirection::Incoming => '<',
                        RelationshipDirection::Undirected => '-',
                    });
                    s.push_str(seg.end_node.variable.as_deref().unwrap());
                }
                s
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    #[test]
    fn starts_at_the_filtered_end_and_reverses_segments() {
        let q = "MATCH (f:Forum)-[:forumHasTag]->(:Tag)-[:hasType]->(tc:Tagclass) \
                 WHERE tc.name = 'Athlete' RETURN f.id";
        assert_eq!(reorder(q).as_deref(), Some("tc<_anon_1<f"));
    }

    #[test]
    fn id_equality_beats_other_equality_and_patterns_restart_from_bound_nodes() {
        let q = "MATCH (f:Forum)-[:hasMember]->(p:Person), (f)-[:forumHasTag]->(t:Tag) \
                 WHERE p.id = 7 AND t.name = 'x' RETURN f.id";
        assert_eq!(reorder(q).as_deref(), Some("p<f>t"));
    }

    #[test]
    fn keeps_text_order_when_nothing_beats_the_first_node() {
        assert_eq!(
            reorder("MATCH (a:Person)-[:hasMember]->(b:Person) RETURN b.id"),
            None
        );
        assert_eq!(
            reorder(
                "MATCH (f:Forum)-[:hasMember]->(p:Person) WHERE f.id = 1 AND p.id = 2 RETURN f"
            ),
            None
        );
        // Conjuncts over two variables say nothing about either one.
        assert_eq!(
            reorder("MATCH (f:Forum)-[:hasMember]->(p:Person) WHERE p.id = f.id RETURN f"),
            None
        );
    }

    #[test]
    fn inline_properties_follow_their_node() {
        let q = "MATCH (f:Forum)-[:forumHasTag]->(t:Tag {name: 'x'}) RETURN f.id";
        let ast = parse_cypher_query(q).unwrap();
        let crate::ast::ReadingClause::Match(clause) = &ast.reading_clauses[0] else {
            panic!();
        };
        let out = reorder_patterns(&clause.patterns, None, &config()).unwrap();
        let GraphPattern::Path(path) = &out[0] else {
            panic!()
        };
        assert_eq!(path.start_node.variable.as_deref(), Some("t"));
        assert!(path.start_node.properties.contains_key("name"));
        assert!(path.segments[0].end_node.properties.is_empty());
    }

    #[test]
    fn closing_edges_and_unsupported_shapes() {
        // A cycle: the closing edge is emitted between two bound variables.
        let q = "MATCH (a:Person)-[:hasMember]->(f:Forum), (f)-[:forumHasTag]->(t:Tag), \
                 (a)-[:hasType]->(t) WHERE t.name = 'x' RETURN a";
        assert_eq!(reorder(q).as_deref(), Some("t<f<a>t"));
        // Variable-length segments and disconnected patterns are left alone.
        assert_eq!(
            reorder("MATCH (a:Person)-[:hasMember*1..2]->(b:Person) WHERE b.id = 1 RETURN a"),
            None
        );
        assert_eq!(
            reorder("MATCH (a:Person)-[:hasMember]->(b:Forum), (c:Tag)-[:hasType]->(d:Tagclass) WHERE d.id = 1 RETURN a"),
            None
        );
    }
}
