// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Logical planning for graph queries
//!
//! This module implements the logical planning phase of the query pipeline:
//! Parse → Semantic Analysis → **Logical Plan** → Physical Plan
//!
//! Logical plans describe WHAT operations to perform, not HOW to perform them.

use crate::case_insensitive::qualify_column;
use crate::error::{GraphError, Result};
use crate::{ast::*, GraphConfig};
use serde::{Deserialize, Serialize};
use snafu::Location;
use std::collections::HashMap;

/// A logical plan operator - describes what operation to perform
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogicalOperator {
    /// One row with no bound variables, for queries without reading clauses.
    SingleRow,
    /// Scan all nodes with a specific label
    ScanByLabel {
        variable: String,
        label: String,
        properties: HashMap<String, PropertyValue>,
    },

    /// Unwind a list into a sequence of rows
    Unwind {
        /// The input operator
        input: Option<Box<LogicalOperator>>,
        /// The expression to unwind (must yield a list)
        expression: ValueExpression,
        /// The alias for the unwound element
        alias: String,
        /// Set when the list holds node ids of this label (`collect(node)`): each unwound
        /// id is joined back to its node table, so `alias` is a node again.
        label: Option<String>,
    },

    /// Apply a filter predicate (WHERE clause)
    Filter {
        input: Box<LogicalOperator>,
        predicate: BooleanExpression,
    },

    /// Traverse relationships (the core graph operation)
    ///
    /// Represents a single-hop relationship traversal: (source)-[rel]->(target)
    Expand {
        /// The input operator (typically a node scan or previous expand)
        input: Box<LogicalOperator>,
        /// Variable name for the source node (e.g., "a" in (a)-[]->(b))
        source_variable: String,
        /// Variable name for the target node (e.g., "b" in (a)-[]->(b))
        target_variable: String,
        /// Label of the target node (e.g., "Person", "Book")
        /// This is essential for looking up the correct schema during planning
        target_label: String,
        /// Types of relationships to traverse (e.g., ["KNOWS", "FRIEND_OF"])
        relationship_types: Vec<String>,
        /// Direction of traversal (Outgoing, Incoming, or Undirected)
        direction: RelationshipDirection,
        /// Optional variable name for the relationship itself (e.g., "r" in -[r]->)
        relationship_variable: Option<String>,
        /// Property filters to apply on the relationship
        properties: HashMap<String, PropertyValue>,
        /// Property filters to apply on the target node
        target_properties: HashMap<String, PropertyValue>,
    },

    /// Variable-length path expansion (*1..2 syntax)
    ///
    /// Represents multi-hop relationship traversals: (source)-[rel*min..max]->(target)
    /// Uses iterative CSR traversal when indexed, otherwise fixed-length branches and a union.
    VariableLengthExpand {
        /// The input operator (typically a node scan)
        input: Box<LogicalOperator>,
        /// Variable name for the source node
        source_variable: String,
        /// Variable name for the target node (reachable in min..max hops)
        target_variable: String,
        /// The label of the final node, which may differ from intermediate nodes.
        target_label: String,
        /// Types of relationships to traverse in each hop
        relationship_types: Vec<String>,
        /// Direction of traversal for each hop
        direction: RelationshipDirection,
        /// Optional variable name for the relationship pattern
        relationship_variable: Option<String>,
        /// Minimum number of hops (defaults to 1 if None)
        min_length: Option<u32>,
        /// Maximum number of hops (defaults to system max if None)
        max_length: Option<u32>,
        /// Property filters to apply on target nodes
        target_properties: HashMap<String, PropertyValue>,
    },

    /// Project specific columns (RETURN clause)
    Project {
        input: Box<LogicalOperator>,
        projections: Vec<ProjectionItem>,
    },

    /// Join multiple disconnected patterns
    Join {
        left: Box<LogicalOperator>,
        right: Box<LogicalOperator>,
        join_type: JoinType,
    },

    /// Apply DISTINCT
    Distinct { input: Box<LogicalOperator> },

    /// Apply ORDER BY
    Sort {
        input: Box<LogicalOperator>,
        sort_items: Vec<SortItem>,
    },

    /// Apply SKIP/OFFSET
    Offset {
        input: Box<LogicalOperator>,
        offset: u64,
    },

    /// Apply LIMIT
    Limit {
        input: Box<LogicalOperator>,
        count: u64,
    },

    /// UNION ALL of the branches of one MATCH clause that uses relationship-type
    /// alternation or multi-label nodes (see `pattern_alternatives`). Branches bind the
    /// same variables, possibly with different labels; columns a branch lacks are null.
    Union { inputs: Vec<LogicalOperator> },

    /// `shortestPath` / `allShortestPaths` between two variables bound in `input`; adds
    /// `<path_variable>__length` and `<path_variable>__nodes` (node ids, source first).
    /// A named path retaining actual relationship occurrences and typed node records.
    PathMatch {
        input: Box<LogicalOperator>,
        path: PathPattern,
    },

    ShortestPath {
        input: Box<LogicalOperator>,
        source_variable: String,
        target_variable: String,
        relationship_type: String,
        direction: RelationshipDirection,
        min_hops: u32,
        max_hops: u32,
        all: bool,
        path_variable: String,
    },

    /// OPTIONAL MATCH: LEFT JOIN of `input` with `optional` (which is planned on top of
    /// `input`'s distinct shared variables) on the shared variables' id columns `keys`.
    OptionalMatch {
        input: Box<LogicalOperator>,
        optional: Box<LogicalOperator>,
        keys: Vec<String>,
    },

    /// `[variable IN list WHERE keep | map]` for each row of `input`, into the column
    /// `output`. The list is unnested with element positions (an empty or null list keeps
    /// its row), `element` is planned over those element rows (its leaf is
    /// [`LogicalOperator::ComprehensionElement`]; EXISTS marks of `keep` join onto it), and
    /// the kept values are collected back per row in position order. With `label`, the
    /// list holds node ids and each element is joined to its node.
    ListComprehension {
        input: Box<LogicalOperator>,
        list: ValueExpression,
        variable: String,
        label: Option<String>,
        element: Box<LogicalOperator>,
        keep: Option<BooleanExpression>,
        map: ValueExpression,
        output: String,
    },

    /// The element rows of the enclosing [`LogicalOperator::ListComprehension`].
    ComprehensionElement,
}

/// Projection item for SELECT/RETURN clauses
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionItem {
    pub expression: ValueExpression,
    pub alias: Option<String>,
}

/// Join types for combining multiple patterns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Sort specification for ORDER BY
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortItem {
    pub expression: ValueExpression,
    pub direction: SortDirection,
}

/// Mean neighbours per key, by (lowercase relationship type, outgoing): edges divided by
/// the distinct source ids (outgoing) or target ids (incoming). Planning estimates only.
pub type FanOut = HashMap<(String, bool), f64>;

/// Logical plan builder - converts AST to logical plan
pub struct LogicalPlanner<'a> {
    /// Track variables in scope
    variables: HashMap<String, String>, // variable -> label
    config: &'a GraphConfig,
    fan_out: Option<&'a FanOut>,
    adjacency: Option<&'a crate::csr_expand::AdjacencyCatalog>,
    /// Mark columns created for EXISTS subqueries so far (names `__exists_<n>`).
    exists_marks: usize,
    /// Aliases holding lists of node ids (`collect(node) AS friends`), with the label.
    node_lists: HashMap<String, String>,
    /// List comprehensions planned so far (output columns `__list_<n>`).
    comprehensions: usize,
    /// The node label of each path variable's nodes, for `nodes(path)`.
    path_labels: HashMap<String, String>,
    rich_paths: bool,
    /// The rows a projection or WHERE started from, while its EXISTS marks and
    /// comprehensions are being added: each is seeded from these, not from the rows with
    /// the previous ones joined on, so planned copies grow linearly, not exponentially.
    seed_source: Option<LogicalOperator>,
}

impl<'a> LogicalPlanner<'a> {
    pub fn new(config: &'a GraphConfig) -> Self {
        Self {
            variables: HashMap::new(),
            config,
            fan_out: None,
            adjacency: None,
            exists_marks: 0,
            node_lists: HashMap::new(),
            comprehensions: 0,
            path_labels: HashMap::new(),
            rich_paths: false,
            seed_source: None,
        }
    }

    /// Use `fan_out` to pick where a path that continues earlier rows starts.
    pub fn with_fan_out(mut self, fan_out: Option<&'a FanOut>) -> Self {
        self.fan_out = fan_out;
        self
    }

    pub(crate) fn with_adjacency(
        mut self,
        adjacency: Option<&'a crate::csr_expand::AdjacencyCatalog>,
    ) -> Self {
        self.adjacency = adjacency;
        self
    }

    fn retain_variable_alternation(&self, patterns: &[GraphPattern]) -> bool {
        if patterns
            .iter()
            .all(|p| matches!(p, GraphPattern::Path(path) if path.variable.is_some()))
        {
            return true;
        }
        let Some(catalog) = self.adjacency else {
            return false;
        };
        let labels = |n: &NodePattern| -> Vec<String> {
            n.variable
                .as_ref()
                .and_then(|v| self.variables.get(v))
                .map(|l| vec![l.clone()])
                .unwrap_or_else(|| n.labels.clone())
        };
        patterns.iter().all(|p| {
            let GraphPattern::Path(path) = p else {
                return true;
            };
            let mut previous = &path.start_node;
            path.segments.iter().all(|segment| {
                let start = previous;
                previous = &segment.end_node;
                let rel = &segment.relationship;
                let Some(range) = &rel.length else {
                    return true;
                };
                if rel.types.len() <= 1 {
                    return true;
                }
                if rel.variable.is_some()
                    || !rel.properties.is_empty()
                    || rel.direction == RelationshipDirection::Undirected
                    || (range.min == Some(1) && range.max == Some(1))
                {
                    return false;
                }
                let (starts, ends) = (labels(start), labels(&segment.end_node));
                let (Some(start), Some(end)) = (starts.first(), ends.first()) else {
                    return false;
                };
                if starts
                    .iter()
                    .chain(&ends)
                    .any(|l| catalog.nodes(l).is_none())
                {
                    return false;
                }
                crate::csr_traversal::Traversal::new(
                    self.config,
                    catalog,
                    &rel.types,
                    &rel.direction,
                    start,
                    end,
                    0,
                    0,
                )
                .is_some()
            })
        })
    }

    /// For a path that continues earlier rows, the path read from the other end when that
    /// is the better start: always when only the far end is bound (expansion must start
    /// from a bound node), and, with both ends bound, when the far end's first hop has the
    /// smaller fan-out (LDBC IC5: from `forum`, about 11 posts each, not `friend`, about
    /// 100). `None` keeps the written order: start already right, no estimates, or a
    /// variable-length segment.
    fn cheaper_orientation(&self, path: &PathPattern) -> Option<PathPattern> {
        if path.variable.is_some() || path.shortest.is_some() {
            return None;
        }
        let bound = |n: &NodePattern| {
            n.variable
                .as_ref()
                .is_some_and(|v| self.variables.contains_key(v))
        };
        let (first, last) = (path.segments.first()?, path.segments.last()?);
        if !bound(&last.end_node)
            || path
                .segments
                .iter()
                .any(|s| s.relationship.length.is_some())
        {
            return None;
        }
        if !bound(&path.start_node) {
            return Some(Self::reversed_path(path));
        }
        let fan_out = self.fan_out?;
        // Fan-out of a hop expanded from its left end (`forward`) or its right end.
        let hop = |rel: &RelationshipPattern, forward: bool| -> Option<f64> {
            let rel_type = rel.types.first()?.to_lowercase();
            let get = |outgoing: bool| fan_out.get(&(rel_type.clone(), outgoing)).copied();
            match (&rel.direction, forward) {
                (RelationshipDirection::Outgoing, true)
                | (RelationshipDirection::Incoming, false) => get(true),
                (RelationshipDirection::Incoming, true)
                | (RelationshipDirection::Outgoing, false) => get(false),
                (RelationshipDirection::Undirected, _) => Some(get(true)? + get(false)?),
            }
        };
        let from_start = hop(&first.relationship, true)?;
        let from_end = hop(&last.relationship, false)?;
        (from_end < from_start).then(|| Self::reversed_path(path))
    }

    /// The same path read backwards: nodes in reverse order, each relationship flipped.
    fn reversed_path(path: &PathPattern) -> PathPattern {
        let mut nodes: Vec<&NodePattern> = std::iter::once(&path.start_node)
            .chain(path.segments.iter().map(|s| &s.end_node))
            .collect();
        nodes.reverse();
        let segments = path
            .segments
            .iter()
            .rev()
            .zip(nodes.iter().skip(1))
            .map(|(segment, end)| PathSegment {
                relationship: crate::pattern_order::reversed(&segment.relationship),
                end_node: (*end).clone(),
            })
            .collect();
        PathPattern {
            start_node: nodes[0].clone(),
            segments,
            shortest: path.shortest.clone(),
            variable: path.variable.clone(),
        }
    }

    /// Convert a Cypher AST to a logical plan
    pub fn plan(&mut self, query: &CypherQuery) -> Result<LogicalOperator> {
        self.rich_paths = crate::path_value::needs_values(query);
        // Plan main MATCH clauses; the WHERE clause guides where traversal starts.
        let where_expr = query.where_clause.as_ref().map(|w| &w.expression);
        let mut plan = self.plan_reading_clauses(None, &query.reading_clauses, where_expr)?;

        // Apply WHERE clause if present (before WITH)
        if let Some(where_clause) = &query.where_clause {
            plan = self.filter(plan, &where_clause.expression)?;
        }

        // Apply WITH clause if present (intermediate projection/aggregation)
        if let Some(with_clause) = &query.with_clause {
            plan = self.plan_with_clause(with_clause, plan)?;
        }

        // Plan post-WITH MATCH clauses
        if !query.post_with_reading_clauses.is_empty() {
            plan = self.plan_reading_clauses(Some(plan), &query.post_with_reading_clauses, None)?;
        }

        // Apply post-WITH WHERE clause if present
        if let Some(post_where) = &query.post_with_where_clause {
            plan = self.filter(plan, &post_where.expression)?;
        }

        // Later WITH stages chain the same way as the first.
        for stage in &query.further_with_stages {
            plan = self.plan_with_clause(&stage.with_clause, plan)?;
            if !stage.reading_clauses.is_empty() {
                plan = self.plan_reading_clauses(Some(plan), &stage.reading_clauses, None)?;
            }
            if let Some(where_clause) = &stage.where_clause {
                plan = self.filter(plan, &where_clause.expression)?;
            }
        }

        // Apply RETURN clause
        plan = self.plan_return_clause(&query.return_clause, plan)?;

        // Apply ORDER BY if present
        if let Some(order_by) = &query.order_by {
            plan = LogicalOperator::Sort {
                input: Box::new(plan),
                sort_items: self.sort_items(order_by),
            };
        }

        // Apply SKIP/OFFSET if present
        if let Some(skip) = &query.skip {
            plan = LogicalOperator::Offset {
                input: Box::new(plan),
                offset: skip.value()?,
            };
        }

        // Apply LIMIT if present
        if let Some(limit) = &query.limit {
            plan = LogicalOperator::Limit {
                input: Box::new(plan),
                count: limit.value()?,
            };
        }

        Ok(plan)
    }

    fn plan_reading_clauses(
        &mut self,
        base_plan: Option<LogicalOperator>,
        reading_clauses: &[ReadingClause],
        filter: Option<&BooleanExpression>,
    ) -> Result<LogicalOperator> {
        let mut plan = base_plan;

        if reading_clauses.is_empty() && plan.is_none() {
            plan = Some(LogicalOperator::SingleRow);
        }

        for clause in reading_clauses {
            plan = Some(self.plan_reading_clause_with_base(plan, clause, filter)?);
        }

        plan.ok_or_else(|| GraphError::PlanError {
            message: "Failed to plan clauses".to_string(),
            location: snafu::Location::new(file!(), line!(), column!()),
        })
    }

    /// Plan a single READING clause, optionally starting from an existing base plan
    fn plan_reading_clause_with_base(
        &mut self,
        base: Option<LogicalOperator>,
        clause: &ReadingClause,
        filter: Option<&BooleanExpression>,
    ) -> Result<LogicalOperator> {
        match clause {
            ReadingClause::Match(match_clause) if match_clause.optional => {
                self.plan_optional_match(base, match_clause, None)
            }
            ReadingClause::Match(match_clause) => {
                let plan = self.plan_match_clause_with_base(base, match_clause, filter)?;
                Ok(match &match_clause.where_clause {
                    Some(w) => self.filter(plan, &w.expression)?,
                    None => plan,
                })
            }
            ReadingClause::Unwind(unwind_clause) => {
                self.plan_unwind_clause_with_base(base, unwind_clause)
            }
        }
    }

    /// Plan an UNWIND clause
    fn plan_unwind_clause_with_base(
        &mut self,
        base: Option<LogicalOperator>,
        unwind_clause: &UnwindClause,
    ) -> Result<LogicalOperator> {
        let label = match &unwind_clause.expression {
            ValueExpression::Variable(list) => self.node_lists.get(list).cloned(),
            _ => None,
        };
        // A list of node ids unwinds to nodes; anything else to plain values.
        let kind = label.clone().unwrap_or_else(|| "Unwound".to_string());
        self.variables.insert(unwind_clause.alias.clone(), kind);
        let mut expression = unwind_clause.expression.clone();
        self.resolve_value(&mut expression);

        Ok(LogicalOperator::Unwind {
            input: base.map(Box::new),
            expression,
            alias: unwind_clause.alias.clone(),
            label,
        })
    }

    /// Node and path variables used as values: a node is its id (so `a = b`, `a IN list`,
    /// `collect(a)`, `count(DISTINCT a)` and `a IS NULL` work on ids) and a path is its
    /// length (so `path IS NULL` is true when no path was found). Bare items of WITH and
    /// RETURN, which carry whole nodes, are not resolved.
    fn resolve_value(&self, expr: &mut ValueExpression) {
        match expr {
            ValueExpression::Variable(var) => {
                let property = match self.variables.get(var).map(String::as_str) {
                    Some("Path") => None,
                    Some(label) => self
                        .config
                        .get_node_mapping(label)
                        .map(|m| m.id_field.clone()),
                    None => None,
                };
                if let Some(property) = property {
                    *expr = ValueExpression::Property(PropertyRef {
                        variable: var.clone(),
                        property,
                    });
                }
            }
            ValueExpression::ScalarFunction { name, args }
                if name.eq_ignore_ascii_case("labels") =>
            {
                if let ValueExpression::Variable(var) = &args[0] {
                    if let Some(mapping) = self
                        .variables
                        .get(var)
                        .and_then(|label| self.config.get_node_mapping(label))
                    {
                        *expr = ValueExpression::Case {
                            whens: vec![(
                                BooleanExpression::IsNull(ValueExpression::Property(PropertyRef {
                                    variable: var.clone(),
                                    property: mapping.id_field.clone(),
                                })),
                                ValueExpression::Literal(PropertyValue::Null),
                            )],
                            otherwise: Some(Box::new(ValueExpression::ScalarFunction {
                                name: "__list".to_string(),
                                args: vec![ValueExpression::Literal(PropertyValue::String(
                                    mapping.label.clone(),
                                ))],
                            })),
                        };
                        return;
                    }
                }
                args.iter_mut().for_each(|arg| self.resolve_value(arg));
            }
            // keys(node/relationship) binds the entity's property columns, not its ID.
            ValueExpression::ScalarFunction { name, args }
                if name.eq_ignore_ascii_case("keys")
                    && matches!(args.as_slice(), [ValueExpression::Variable(_)]) => {}
            // Path functions take the path variable itself.
            ValueExpression::ScalarFunction { name, args }
                if matches!(
                    name.to_lowercase().as_str(),
                    "length" | "nodes" | "relationships" | "__path_node_records"
                ) => {}
            ValueExpression::ScalarFunction { args, .. }
            | ValueExpression::AggregateFunction { args, .. } => {
                args.iter_mut().for_each(|a| self.resolve_value(a))
            }
            ValueExpression::Arithmetic { left, right, .. } => {
                self.resolve_value(left);
                self.resolve_value(right);
            }
            ValueExpression::Case { whens, otherwise } => {
                for (condition, then) in whens {
                    self.resolve_boolean(condition);
                    self.resolve_value(then);
                }
                if let Some(e) = otherwise {
                    self.resolve_value(e);
                }
            }
            ValueExpression::Reduce {
                initial,
                list,
                expression,
                ..
            } => {
                self.resolve_value(initial);
                self.resolve_value(list);
                self.resolve_value(expression);
            }
            ValueExpression::ListPredicate {
                list, predicate, ..
            } => {
                self.resolve_value(list);
                self.resolve_value(predicate);
            }
            ValueExpression::Boolean(b) => self.resolve_boolean(b),
            // A node inside a map keeps its properties (see `bind_value`), not just its id.
            ValueExpression::Map(entries) => entries
                .iter_mut()
                .filter(|(_, v)| !matches!(v, ValueExpression::Variable(_)))
                .for_each(|(_, v)| self.resolve_value(v)),
            ValueExpression::Field { base, .. } => self.resolve_value(base),
            _ => {}
        }
    }

    /// [`Self::resolve_value`] for every value in a predicate.
    fn resolve_boolean(&self, expr: &mut BooleanExpression) {
        match expr {
            BooleanExpression::Comparison { left, right, .. } => {
                self.resolve_value(left);
                self.resolve_value(right);
            }
            BooleanExpression::And(a, b) | BooleanExpression::Or(a, b) => {
                self.resolve_boolean(a);
                self.resolve_boolean(b);
            }
            BooleanExpression::Not(inner) => self.resolve_boolean(inner),
            BooleanExpression::In { expression, list } => {
                self.resolve_value(expression);
                list.iter_mut().for_each(|v| self.resolve_value(v));
            }
            BooleanExpression::Member { expression, list } => {
                self.resolve_value(expression);
                self.resolve_value(list);
            }
            BooleanExpression::Like { expression, .. }
            | BooleanExpression::ILike { expression, .. }
            | BooleanExpression::Contains { expression, .. }
            | BooleanExpression::StartsWith { expression, .. }
            | BooleanExpression::EndsWith { expression, .. }
            | BooleanExpression::IsNull(expression)
            | BooleanExpression::IsNotNull(expression) => self.resolve_value(expression),
            BooleanExpression::Exists(_) | BooleanExpression::PatternExists { .. } => {}
        }
    }

    /// Sort items with node and path values resolved (see [`Self::resolve_value`]).
    fn sort_items(&self, order_by: &OrderByClause) -> Vec<SortItem> {
        order_by
            .items
            .iter()
            .map(|item| {
                let mut expression = item.expression.clone();
                self.resolve_value(&mut expression);
                SortItem {
                    expression,
                    direction: item.direction.clone(),
                }
            })
            .collect()
    }

    /// Plan a single MATCH clause, optionally starting from an existing base plan.
    ///
    /// A clause that starts a plan may be re-rooted at its most selective node, using
    /// `filter` (the query's WHERE clause); see [`crate::pattern_order`]. A clause that
    /// continues an existing plan is planned as written.
    fn plan_match_clause_with_base(
        &mut self,
        base: Option<LogicalOperator>,
        match_clause: &MatchClause,
        filter: Option<&BooleanExpression>,
    ) -> Result<LogicalOperator> {
        if match_clause.patterns.is_empty() {
            return Err(GraphError::PlanError {
                message: "MATCH clause must have at least one pattern".to_string(),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }
        let inferred = MatchClause {
            patterns: crate::pattern_alternatives::infer_labels(
                &match_clause.patterns,
                self.config,
                &self.variables,
            ),
            ..match_clause.clone()
        };
        let match_clause = &inferred;
        let retain_variable = self.retain_variable_alternation(&match_clause.patterns);
        if crate::pattern_alternatives::needs_expansion(&match_clause.patterns, retain_variable) {
            let branches = crate::pattern_alternatives::expand(
                &match_clause.patterns,
                self.config,
                &self.variables,
                retain_variable,
            )?;
            return self.plan_match_branches(base, branches, filter);
        }

        let reordered = base
            .is_none()
            .then(|| {
                crate::pattern_order::reorder_patterns(&match_clause.patterns, filter, self.config)
            })
            .flatten();
        let oriented: Option<Vec<GraphPattern>> = base.is_some().then(|| {
            match_clause
                .patterns
                .iter()
                .map(|p| match p {
                    GraphPattern::Path(path) => self
                        .cheaper_orientation(path)
                        .map_or_else(|| p.clone(), GraphPattern::Path),
                    _ => p.clone(),
                })
                .collect()
        });
        let patterns = reordered
            .as_deref()
            .or(oriented.as_deref())
            .unwrap_or(&match_clause.patterns);

        let mut plan = base;
        for pattern in patterns {
            match pattern {
                GraphPattern::Node(node) => {
                    let already_bound = node
                        .variable
                        .as_deref()
                        .is_some_and(|v| self.variables.contains_key(v));

                    match (already_bound, plan.as_ref()) {
                        (true, _) => { /* no-op */ }
                        (false, None) => plan = Some(self.plan_node_scan(node)?),
                        (false, Some(_)) => {
                            let right = self.plan_node_scan(node)?;
                            plan = Some(LogicalOperator::Join {
                                left: Box::new(plan.unwrap()),
                                right: Box::new(right),
                                join_type: JoinType::Cross, // TODO: infer better join type based on shared vars
                            });
                        }
                    }
                }
                GraphPattern::Path(path) if path.variable.is_some() => {
                    plan = Some(self.plan_named_path(plan, path)?)
                }
                GraphPattern::Path(path) if path.shortest.is_some() => {
                    plan = Some(self.plan_shortest_path(plan, path)?)
                }
                GraphPattern::Path(path)
                    if self.rich_paths
                        && path
                            .segments
                            .iter()
                            .any(|s| s.relationship.variable.is_some()) =>
                {
                    let mut path = path.clone();
                    path.variable = Some(format!("_path_value_{}", self.variables.len()));
                    plan = Some(self.plan_named_path(plan, &path)?);
                }
                GraphPattern::Path(path) => plan = Some(self.plan_path(plan, path)?),
            }
        }

        plan.ok_or_else(|| GraphError::PlanError {
            message: "Failed to plan MATCH clause".to_string(),
            location: snafu::Location::new(file!(), line!(), column!()),
        })
    }

    /// `input` filtered by `predicate`, after turning any EXISTS in it into mark columns.
    fn filter(
        &mut self,
        input: LogicalOperator,
        predicate: &BooleanExpression,
    ) -> Result<LogicalOperator> {
        let mut predicate = predicate.clone();
        let saved = self.seed_source.replace(input.clone());
        let input = self.rewrite_exists(input, &mut predicate);
        self.seed_source = saved;
        let input = input?;
        self.resolve_boolean(&mut predicate);
        Ok(LogicalOperator::Filter {
            input: Box::new(input),
            predicate,
        })
    }

    /// Replace every pattern EXISTS in `expr` with `__exists_<n> IS NOT NULL`, adding to
    /// `input` the LEFT JOIN that computes that mark: the pattern planned on top of the
    /// distinct shared variables (as for OPTIONAL MATCH, so it can use the CSR), one row
    /// per distinct binding of them with `true` as the mark. A row with no match gets a
    /// null mark, so `NOT EXISTS` is true for it, as Cypher requires.
    fn rewrite_exists(
        &mut self,
        mut input: LogicalOperator,
        expr: &mut BooleanExpression,
    ) -> Result<LogicalOperator> {
        match expr {
            BooleanExpression::And(a, b) | BooleanExpression::Or(a, b) => {
                input = self.rewrite_exists(input, a)?;
                self.rewrite_exists(input, b)
            }
            BooleanExpression::Not(inner) => self.rewrite_exists(input, inner),
            BooleanExpression::Comparison { left, right, .. } => {
                input = self.rewrite_exists_in_value(input, left)?;
                self.rewrite_exists_in_value(input, right)
            }
            BooleanExpression::PatternExists {
                patterns,
                where_clause,
            } => {
                self.exists_marks += 1;
                let mark = format!("__exists_{}", self.exists_marks);
                let inner = MatchClause {
                    patterns: patterns.clone(),
                    optional: true,
                    where_clause: where_clause.as_ref().map(|w| WhereClause {
                        expression: (**w).clone(),
                    }),
                };
                // The pattern's own new variables are local to the subquery.
                let before = self.variables.clone();
                let seed = self.seed_source.clone();
                let planned = self.plan_optional_match(Some(input), &inner, seed);
                self.variables = before;
                let LogicalOperator::OptionalMatch {
                    input,
                    optional,
                    keys,
                } = planned?
                else {
                    unreachable!("plan_optional_match returns an OptionalMatch")
                };
                let shared: Vec<String> = keys
                    .iter()
                    .map(|k| k.split("__").next().unwrap_or(k).to_string())
                    .collect();
                let mut projections: Vec<ProjectionItem> = shared
                    .iter()
                    .map(|var| ProjectionItem {
                        expression: ValueExpression::Variable(var.clone()),
                        alias: None,
                    })
                    .collect();
                projections.push(ProjectionItem {
                    expression: ValueExpression::Literal(PropertyValue::Boolean(true)),
                    alias: Some(mark.clone()),
                });
                let marked = LogicalOperator::Distinct {
                    input: Box::new(LogicalOperator::Project {
                        input: optional,
                        projections,
                    }),
                };
                *expr = BooleanExpression::IsNotNull(ValueExpression::Variable(mark));
                Ok(LogicalOperator::OptionalMatch {
                    input,
                    optional: Box::new(marked),
                    keys,
                })
            }
            _ => Ok(input),
        }
    }

    /// [`Self::rewrite_exists`] for the conditions of a CASE inside a value.
    fn rewrite_exists_in_value(
        &mut self,
        mut input: LogicalOperator,
        expr: &mut ValueExpression,
    ) -> Result<LogicalOperator> {
        match expr {
            ValueExpression::ScalarFunction { name, args }
                if name.eq_ignore_ascii_case("labels")
                    && matches!(args.as_slice(), [ValueExpression::Variable(var)] if self.variables.get(var).is_some_and(|label| label.contains('|'))) =>
            {
                Err(GraphError::UnsupportedFeature {
                    feature: "labels() on a mixed-label binding is not supported".to_string(),
                    location: Location::new(file!(), line!(), column!()),
                })
            }
            ValueExpression::Case { whens, otherwise } => {
                for (condition, then) in whens.iter_mut() {
                    input = self.rewrite_exists(input, condition)?;
                    input = self.rewrite_exists_in_value(input, then)?;
                }
                match otherwise {
                    Some(e) => self.rewrite_exists_in_value(input, e),
                    None => Ok(input),
                }
            }
            ValueExpression::ScalarFunction { args, .. }
            | ValueExpression::AggregateFunction { args, .. } => {
                for arg in args.iter_mut() {
                    input = self.rewrite_exists_in_value(input, arg)?;
                }
                Ok(input)
            }
            ValueExpression::Arithmetic { left, right, .. } => {
                input = self.rewrite_exists_in_value(input, left)?;
                self.rewrite_exists_in_value(input, right)
            }
            ValueExpression::Reduce {
                initial,
                list,
                expression,
                ..
            } => {
                input = self.rewrite_exists_in_value(input, initial)?;
                input = self.rewrite_exists_in_value(input, list)?;
                self.rewrite_exists_in_value(input, expression)
            }
            ValueExpression::ListPredicate {
                kind,
                variable,
                list,
                predicate,
            } if crate::ast::needs_relational_evaluation(predicate) => {
                input = self.rewrite_exists_in_value(input, list)?;
                let original_list = (**list).clone();
                let mut map = (**predicate).clone();
                // The relational comprehension supplies this iterator as a column. Restore
                // inner lambda bindings after replacing references to the outer iterator.
                crate::ast::transform_value(&mut map, &mut |value| match value {
                    ValueExpression::LocalVariable(name) if name == variable => {
                        *value = ValueExpression::Variable(name.clone());
                    }
                    ValueExpression::Field { base, field } if matches!(base.as_ref(), ValueExpression::Variable(name) if name == variable) =>
                    {
                        *value = ValueExpression::Property(PropertyRef::new(
                            variable.clone(),
                            field.clone(),
                        ));
                    }
                    ValueExpression::Reduce {
                        accumulator,
                        variable,
                        expression,
                        ..
                    } => {
                        crate::ast::mark_locals(expression, &[accumulator, variable]);
                    }
                    ValueExpression::ListPredicate {
                        variable,
                        predicate,
                        ..
                    } => {
                        crate::ast::mark_locals(predicate, &[variable]);
                    }
                    _ => {}
                });
                let mut comprehension = ValueExpression::ListComprehension {
                    variable: variable.clone(),
                    list: list.clone(),
                    filter: None,
                    map: Some(Box::new(map)),
                };
                input = self.rewrite_exists_in_value(input, &mut comprehension)?;
                *expr = ValueExpression::Case {
                    whens: vec![(
                        BooleanExpression::IsNull(original_list),
                        ValueExpression::Literal(PropertyValue::Null),
                    )],
                    otherwise: Some(Box::new(ValueExpression::ScalarFunction {
                        name: format!("__predicate_{kind}"),
                        args: vec![comprehension],
                    })),
                };
                Ok(input)
            }
            ValueExpression::ListPredicate {
                list, predicate, ..
            } => {
                input = self.rewrite_exists_in_value(input, list)?;
                self.rewrite_exists_in_value(input, predicate)
            }
            ValueExpression::Boolean(b) => self.rewrite_exists(input, b),
            ValueExpression::Map(entries) => {
                for (_, v) in entries.iter_mut() {
                    input = self.rewrite_exists_in_value(input, v)?;
                }
                Ok(input)
            }
            ValueExpression::Field { base, .. } => self.rewrite_exists_in_value(input, base),
            ValueExpression::PatternComprehension {
                pattern,
                filter,
                map,
            } => {
                // As an EXISTS mark, but collecting `map` per binding of the shared
                // variables instead of marking `true`.
                self.comprehensions += 1;
                let column = format!("__pattern_{}", self.comprehensions);
                let inner = MatchClause {
                    patterns: vec![GraphPattern::Path((**pattern).clone())],
                    optional: true,
                    where_clause: filter.as_ref().map(|w| WhereClause {
                        expression: (**w).clone(),
                    }),
                };
                let before = self.variables.clone();
                let planned = (|| -> Result<(LogicalOperator, ValueExpression)> {
                    let seed = self.seed_source.clone();
                    let planned = self.plan_optional_match(Some(input), &inner, seed)?;
                    let mut value = (**map).clone();
                    let LogicalOperator::OptionalMatch {
                        input,
                        optional,
                        keys,
                    } = planned
                    else {
                        unreachable!("plan_optional_match returns an OptionalMatch")
                    };
                    let saved = self.seed_source.replace((*optional).clone());
                    let optional = self.rewrite_exists_in_value(*optional, &mut value);
                    self.seed_source = saved;
                    let optional = optional?;
                    self.resolve_value(&mut value);
                    let mut projections: Vec<ProjectionItem> = keys
                        .iter()
                        .map(|k| ProjectionItem {
                            expression: ValueExpression::Variable(
                                k.split("__").next().unwrap_or(k).to_string(),
                            ),
                            alias: None,
                        })
                        .collect();
                    projections.push(ProjectionItem {
                        expression: ValueExpression::AggregateFunction {
                            name: "collect".to_string(),
                            args: vec![value],
                            distinct: false,
                        },
                        alias: Some(column.clone()),
                    });
                    let collected = LogicalOperator::Project {
                        input: Box::new(optional),
                        projections,
                    };
                    let joined = LogicalOperator::OptionalMatch {
                        input,
                        optional: Box::new(collected),
                        keys,
                    };
                    // No match: the left join gives null, which is an empty list.
                    let result = ValueExpression::ScalarFunction {
                        name: "coalesce".to_string(),
                        args: vec![
                            ValueExpression::Variable(column.clone()),
                            ValueExpression::ScalarFunction {
                                name: "__list".to_string(),
                                args: vec![],
                            },
                        ],
                    };
                    Ok((joined, result))
                })();
                self.variables = before;
                let (joined, result) = planned?;
                *expr = result;
                Ok(joined)
            }
            ValueExpression::ListComprehension {
                variable,
                list,
                filter,
                map,
            } => {
                input = self.rewrite_exists_in_value(input, list)?;
                self.resolve_value(list);
                if self.rich_paths {
                    if let ValueExpression::ScalarFunction { name, .. } = list.as_mut() {
                        if name.eq_ignore_ascii_case("nodes") {
                            *name = "__path_node_records".into();
                        }
                    }
                }
                let label = match list.as_ref() {
                    ValueExpression::Variable(l) => self.node_lists.get(l).cloned(),
                    ValueExpression::ScalarFunction { name, args }
                        if name.eq_ignore_ascii_case("nodes") =>
                    {
                        match args.as_slice() {
                            [ValueExpression::Variable(p)] => self.path_labels.get(p).cloned(),
                            _ => None,
                        }
                    }
                    _ => None,
                };
                self.comprehensions += 1;
                let output = format!("__list_{}", self.comprehensions);
                // The variable is local: an element of the list, a node when it has a label.
                let before = self.variables.clone();
                let saved_seed = self.seed_source.clone();
                let kind = label.clone().unwrap_or_else(|| "Unwound".to_string());
                self.variables.insert(variable.clone(), kind);
                let lowered = (|| -> Result<LogicalOperator> {
                    let mut element = LogicalOperator::ComprehensionElement;
                    self.seed_source = Some(LogicalOperator::ComprehensionElement);
                    let mut keep = filter.as_deref().cloned();
                    if let Some(k) = &mut keep {
                        element = self.rewrite_exists(element, k)?;
                        self.resolve_boolean(k);
                    }
                    let mut value = map
                        .as_deref()
                        .cloned()
                        .unwrap_or_else(|| ValueExpression::Variable(variable.clone()));
                    element = self.rewrite_exists_in_value(element, &mut value)?;
                    self.resolve_value(&mut value);
                    Ok(LogicalOperator::ListComprehension {
                        input: Box::new(input),
                        list: (**list).clone(),
                        variable: variable.clone(),
                        label,
                        element: Box::new(element),
                        keep,
                        map: value,
                        output: output.clone(),
                    })
                })();
                self.variables = before;
                self.seed_source = saved_seed;
                *expr = ValueExpression::Variable(output);
                lowered
            }
            _ => Ok(input),
        }
    }

    /// A `shortestPath` pattern: end nodes not bound yet are scanned (and cross-joined with
    /// the rows so far), then the search runs per row between the two bound ids.
    fn plan_shortest_path(
        &mut self,
        base: Option<LogicalOperator>,
        path: &PathPattern,
    ) -> Result<LogicalOperator> {
        let spec = path.shortest.as_ref().expect("a shortest-path pattern");
        let unsupported = |what: &str| GraphError::UnsupportedFeature {
            feature: format!("shortestPath {what}"),
            location: Location::new(file!(), line!(), column!()),
        };
        let [segment] = path.segments.as_slice() else {
            return Err(unsupported("over more than one relationship pattern"));
        };
        let rel = &segment.relationship;
        let [rel_type] = rel.types.as_slice() else {
            return Err(unsupported("needs exactly one relationship type"));
        };
        if rel.variable.is_some() || !rel.properties.is_empty() {
            return Err(unsupported("with a relationship variable or properties"));
        }
        let mut plan = base;
        let mut ends = Vec::with_capacity(2);
        for node in [&path.start_node, &segment.end_node] {
            let Some(var) = &node.variable else {
                return Err(unsupported("with an unnamed end node"));
            };
            if !self.variables.contains_key(var) {
                let scan = self.plan_node_scan(node)?;
                plan = Some(match plan {
                    None => scan,
                    Some(p) => LogicalOperator::Join {
                        left: Box::new(p),
                        right: Box::new(scan),
                        join_type: JoinType::Cross,
                    },
                });
            }
            ends.push(var.clone());
        }
        let path_variable = spec
            .variable
            .clone()
            .unwrap_or_else(|| format!("_path_{}", self.variables.len()));
        // Not a node: the marker keeps it in WITH scope and matches no node mapping.
        self.variables
            .insert(path_variable.clone(), "Path".to_string());
        if let Some(label) = self.variables.get(&ends[0]).cloned() {
            self.path_labels.insert(path_variable.clone(), label);
        }
        let length = rel.length.as_ref();
        if self.rich_paths {
            return Ok(LogicalOperator::PathMatch {
                input: Box::new(plan.expect("both ends planned")),
                path: path.clone(),
            });
        }
        Ok(LogicalOperator::ShortestPath {
            input: Box::new(plan.expect("both end nodes are planned")),
            source_variable: ends[0].clone(),
            target_variable: ends[1].clone(),
            relationship_type: rel_type.clone(),
            direction: rel.direction.clone(),
            min_hops: length.and_then(|l| l.min).unwrap_or(1),
            max_hops: length.and_then(|l| l.max).unwrap_or(u32::MAX),
            all: spec.all,
            path_variable,
        })
    }

    /// OPTIONAL MATCH: the pattern is planned on top of the distinct values of the node
    /// variables it shares with `base` (so expansion starts there, CSR included), filtered
    /// by its own WHERE, then left-joined back to `base` on those variables' ids.
    // shortcut: `base` is planned and run twice (left side and seed), since DataFusion
    // plans are trees; fine for the small inputs OPTIONAL MATCH usually sees.
    fn plan_optional_match(
        &mut self,
        base: Option<LogicalOperator>,
        clause: &MatchClause,
        seed_source: Option<LogicalOperator>,
    ) -> Result<LogicalOperator> {
        let unsupported = |what: &str| GraphError::UnsupportedFeature {
            feature: format!("OPTIONAL MATCH {what}"),
            location: Location::new(file!(), line!(), column!()),
        };
        let base = base.ok_or_else(|| unsupported("as the first clause"))?;
        let mut shared = Vec::new();
        for pattern in &clause.patterns {
            let nodes: Vec<&NodePattern> = match pattern {
                GraphPattern::Node(n) => vec![n],
                GraphPattern::Path(p) => std::iter::once(&p.start_node)
                    .chain(p.segments.iter().map(|s| &s.end_node))
                    .collect(),
            };
            for var in nodes.into_iter().filter_map(|n| n.variable.as_ref()) {
                if self.variables.contains_key(var) && !shared.contains(var) {
                    shared.push(var.clone());
                }
            }
        }
        let mut keys = Vec::with_capacity(shared.len());
        for var in &shared {
            let label = &self.variables[var];
            let mapping = self.config.get_node_mapping(label).ok_or_else(|| {
                unsupported(&format!("from '{var}', whose label '{label}' is ambiguous"))
            })?;
            keys.push(qualify_column(var, &mapping.id_field));
        }
        // The seed also carries the earlier scalars its WHERE reads (`friend IN friends`).
        let mut carried = shared.clone();
        if let Some(w) = &clause.where_clause {
            let mut names = std::collections::HashSet::new();
            crate::pattern_order::boolean_variables(&w.expression, &mut names);
            let local = crate::pattern_order::pattern_variables(&clause.patterns);
            let mut scalars: Vec<String> = names
                .into_iter()
                .filter(|n| !local.contains(*n) && !shared.iter().any(|v| v == *n))
                .map(str::to_string)
                .collect();
            scalars.sort();
            for variable in &scalars {
                // Binding variables distinguish iterator contexts. Untracked aggregate
                // captures can be re-evaluated in a different order by the seed plan.
                if !self.variables.contains_key(variable) && !shared.is_empty() {
                    continue;
                }
                keys.push(
                    self.variables
                        .get(variable)
                        .and_then(|label| self.config.get_node_mapping(label))
                        .map(|mapping| qualify_column(variable, &mapping.id_field))
                        .unwrap_or_else(|| variable.to_lowercase()),
                );
            }
            carried.extend(scalars);
        }
        if carried.is_empty() {
            return Err(unsupported("without a variable bound by an earlier clause"));
        }
        let seed = LogicalOperator::Distinct {
            input: Box::new(LogicalOperator::Project {
                input: Box::new(seed_source.unwrap_or_else(|| base.clone())),
                projections: carried
                    .iter()
                    .map(|var| ProjectionItem {
                        expression: ValueExpression::Variable(var.clone()),
                        alias: None,
                    })
                    .collect(),
            }),
        };
        let inner = MatchClause {
            patterns: clause.patterns.clone(),
            optional: false,
            where_clause: None,
        };
        let mut optional = self.plan_match_clause_with_base(Some(seed), &inner, None)?;
        if let Some(w) = &clause.where_clause {
            optional = self.filter(optional, &w.expression)?;
        }
        Ok(LogicalOperator::OptionalMatch {
            input: Box::new(base),
            optional: Box::new(optional),
            keys,
        })
    }

    /// Plan each single-type, single-label branch like an ordinary MATCH and UNION ALL them.
    // shortcut: a `base` from earlier clauses is planned (and run) once per branch; fine for
    // the few branches alternation produces, revisit if bases get expensive.
    fn plan_match_branches(
        &mut self,
        base: Option<LogicalOperator>,
        branches: Vec<Vec<GraphPattern>>,
        filter: Option<&BooleanExpression>,
    ) -> Result<LogicalOperator> {
        let before = self.variables.clone();
        let mut inputs = Vec::new();
        let mut labels: HashMap<String, Vec<String>> = HashMap::new();
        for patterns in branches {
            self.variables = before.clone();
            let clause = MatchClause {
                patterns,
                optional: false,
                where_clause: None,
            };
            inputs.push(self.plan_match_clause_with_base(base.clone(), &clause, filter)?);
            for (var, label) in &self.variables {
                let seen = labels.entry(var.clone()).or_default();
                if !seen.contains(label) {
                    seen.push(label.clone());
                }
            }
        }
        // A variable with different labels per branch gets a combined name that matches no
        // node mapping, so a later clause expanding from it fails instead of guessing.
        self.variables = labels
            .into_iter()
            .map(|(var, labels)| (var, labels.join("|")))
            .collect();
        Ok(if inputs.len() == 1 {
            inputs.pop().expect("one branch")
        } else {
            LogicalOperator::Union { inputs }
        })
    }

    /// Plan a node scan (ScanByLabel)
    fn plan_node_scan(&mut self, node: &NodePattern) -> Result<LogicalOperator> {
        let variable = node
            .variable
            .clone()
            .unwrap_or_else(|| format!("_node_{}", self.variables.len()));

        // Validate label consistency if variable already exists
        self.validate_variable_label(&variable, &node.labels)?;

        // Reuse existing label or derive from AST (default to "Node")
        let label = self
            .variables
            .get(&variable)
            .cloned()
            .or_else(|| node.labels.first().cloned())
            .unwrap_or_else(|| "Node".to_string());

        // Register variable with its label
        self.variables.insert(variable.clone(), label.clone());

        Ok(LogicalOperator::ScanByLabel {
            variable,
            label,
            properties: node.properties.clone(),
        })
    }

    /// Validate that a node's explicit label (if any) matches the already-registered
    /// label for this variable. Returns `Ok(())` if the variable is new or if labels
    /// are consistent, and an error if they conflict.
    fn validate_variable_label(&self, variable: &str, ast_labels: &[String]) -> Result<()> {
        if let Some(existing_label) = self.variables.get(variable) {
            if let Some(ast_label) = ast_labels.first() {
                if ast_label != existing_label {
                    return Err(GraphError::PlanError {
                        message: format!(
                            "Variable '{}' already has label '{}', cannot redefine as '{}'",
                            variable, existing_label, ast_label
                        ),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    });
                }
            }
        }
        Ok(())
    }

    /// Plan a full path pattern, respecting the starting variable if provided
    fn plan_path(
        &mut self,
        base: Option<LogicalOperator>,
        path: &PathPattern,
    ) -> Result<LogicalOperator> {
        // Establish a base plan. A path that continues earlier rows from a node they don't
        // bind starts with a scan of that node, crossed with those rows.
        let start_bound = path
            .start_node
            .variable
            .as_ref()
            .is_some_and(|v| self.variables.contains_key(v));
        let (mut plan, mut current_src) = match base {
            Some(p) if start_bound => {
                let var = path
                    .start_node
                    .variable
                    .clone()
                    .expect("bound start is named");
                (p, var)
            }
            base => {
                let scan = self.plan_node_scan(&path.start_node)?;
                let var = self.extract_variable_from_plan(&scan)?;
                let plan = match base {
                    Some(p) => LogicalOperator::Join {
                        left: Box::new(p),
                        right: Box::new(scan),
                        join_type: JoinType::Cross,
                    },
                    None => scan,
                };
                (plan, var)
            }
        };

        // Validate start node label consistency if variable already exists
        if let Some(start_var) = &path.start_node.variable {
            self.validate_variable_label(start_var, &path.start_node.labels)?;
        }

        // For each segment, add an expand
        for segment in &path.segments {
            // Determine / register target variable
            let target_variable = segment
                .end_node
                .variable
                .clone()
                .unwrap_or_else(|| format!("_node_{}", self.variables.len()));

            // Validate label consistency for already-registered variables
            self.validate_variable_label(&target_variable, &segment.end_node.labels)?;

            // Reuse existing label or derive from AST (default to "Node")
            let target_label = self
                .variables
                .get(&target_variable)
                .cloned()
                .or_else(|| segment.end_node.labels.first().cloned())
                .unwrap_or_else(|| "Node".to_string());

            self.variables
                .insert(target_variable.clone(), target_label.clone());

            // Optimize fixed-length var-length expansions (*1 or *1..1)
            let next_plan = match segment.relationship.length.as_ref() {
                Some(length_range)
                    if length_range.min == Some(1) && length_range.max == Some(1) =>
                {
                    LogicalOperator::Expand {
                        input: Box::new(plan),
                        source_variable: current_src.clone(),
                        target_variable: target_variable.clone(),
                        target_label: target_label.clone(),
                        relationship_types: segment.relationship.types.clone(),
                        direction: segment.relationship.direction.clone(),
                        relationship_variable: segment.relationship.variable.clone(),
                        properties: segment.relationship.properties.clone(),
                        target_properties: segment.end_node.properties.clone(),
                    }
                }
                Some(length_range) => LogicalOperator::VariableLengthExpand {
                    input: Box::new(plan),
                    source_variable: current_src.clone(),
                    target_variable: target_variable.clone(),
                    target_label: target_label.clone(),
                    relationship_types: segment.relationship.types.clone(),
                    direction: segment.relationship.direction.clone(),
                    relationship_variable: segment.relationship.variable.clone(),
                    min_length: length_range.min,
                    max_length: length_range.max,
                    target_properties: segment.end_node.properties.clone(),
                },
                None => LogicalOperator::Expand {
                    input: Box::new(plan),
                    source_variable: current_src.clone(),
                    target_variable: target_variable.clone(),
                    target_label: target_label.clone(),
                    relationship_types: segment.relationship.types.clone(),
                    direction: segment.relationship.direction.clone(),
                    relationship_variable: segment.relationship.variable.clone(),
                    properties: segment.relationship.properties.clone(),
                    target_properties: segment.end_node.properties.clone(),
                },
            };

            plan = next_plan;
            current_src = target_variable;
        }

        Ok(plan)
    }

    fn plan_named_path(
        &mut self,
        base: Option<LogicalOperator>,
        path: &PathPattern,
    ) -> Result<LogicalOperator> {
        let mut path = path.clone();
        if path.start_node.variable.is_none() {
            path.start_node.variable = Some(format!("_path_node_{}", self.variables.len()));
        }
        let start = path.start_node.variable.as_ref().unwrap();
        let base = if self.variables.contains_key(start) {
            base.expect("bound variables have input rows")
        } else {
            let scan = self.plan_node_scan(&path.start_node)?;
            match base {
                Some(input) => LogicalOperator::Join {
                    left: Box::new(input),
                    right: Box::new(scan),
                    join_type: JoinType::Cross,
                },
                None => scan,
            }
        };
        for segment in &mut path.segments {
            if segment.end_node.variable.is_none() {
                segment.end_node.variable = Some(format!("_path_node_{}", self.variables.len()));
            }
            let variable = segment.end_node.variable.as_ref().unwrap();
            self.validate_variable_label(variable, &segment.end_node.labels)?;
            let label = self
                .variables
                .get(variable)
                .cloned()
                .or_else(|| segment.end_node.labels.first().cloned())
                .ok_or_else(|| GraphError::UnsupportedFeature {
                    feature: "named path boundary needs a resolved node label".into(),
                    location: Location::new(file!(), line!(), column!()),
                })?;
            self.variables.insert(variable.clone(), label);
        }
        self.variables
            .insert(path.variable.clone().unwrap(), "Path".into());
        Ok(LogicalOperator::PathMatch {
            input: Box::new(base),
            path,
        })
    }

    /// Extract the main variable from a logical plan (for chaining)
    #[allow(clippy::only_used_in_recursion)]
    fn extract_variable_from_plan(&self, plan: &LogicalOperator) -> Result<String> {
        match plan {
            LogicalOperator::SingleRow => Err(GraphError::PlanError {
                message: "a singleton row binds no pattern variable".to_string(),
                location: snafu::Location::new(file!(), line!(), column!()),
            }),
            LogicalOperator::ScanByLabel { variable, .. } => Ok(variable.clone()),
            LogicalOperator::Unwind { alias, .. } => Ok(alias.clone()),
            LogicalOperator::Expand {
                target_variable, ..
            } => Ok(target_variable.clone()),
            LogicalOperator::VariableLengthExpand {
                target_variable, ..
            } => Ok(target_variable.clone()),
            LogicalOperator::Filter { input, .. } => self.extract_variable_from_plan(input),
            LogicalOperator::Project { input, .. } => self.extract_variable_from_plan(input),
            LogicalOperator::Distinct { input } => self.extract_variable_from_plan(input),
            LogicalOperator::Sort { input, .. } => self.extract_variable_from_plan(input),
            LogicalOperator::Offset { input, .. } => self.extract_variable_from_plan(input),
            LogicalOperator::Limit { input, .. } => self.extract_variable_from_plan(input),
            LogicalOperator::Join { left, right, .. } => {
                // Prefer the right branch's tail variable, else fall back to left
                self.extract_variable_from_plan(right)
                    .or_else(|_| self.extract_variable_from_plan(left))
            }
            LogicalOperator::OptionalMatch { optional, .. } => {
                self.extract_variable_from_plan(optional)
            }
            LogicalOperator::ListComprehension { input, .. } => {
                self.extract_variable_from_plan(input)
            }
            LogicalOperator::ComprehensionElement => Err(GraphError::PlanError {
                message: "a comprehension's element rows bind no pattern variable".to_string(),
                location: snafu::Location::new(file!(), line!(), column!()),
            }),
            LogicalOperator::PathMatch { path, .. } => Ok(path
                .segments
                .last()
                .unwrap()
                .end_node
                .variable
                .clone()
                .unwrap()),
            LogicalOperator::ShortestPath {
                target_variable, ..
            } => Ok(target_variable.clone()),
            LogicalOperator::Union { inputs } => match inputs.first() {
                Some(first) => self.extract_variable_from_plan(first),
                None => Err(GraphError::PlanError {
                    message: "empty UNION".to_string(),
                    location: snafu::Location::new(file!(), line!(), column!()),
                }),
            },
        }
    }

    /// Plan RETURN clause (Project)
    fn plan_return_clause(
        &mut self,
        return_clause: &ReturnClause,
        input: LogicalOperator,
    ) -> Result<LogicalOperator> {
        let mut projections: Vec<ProjectionItem> = Vec::new();
        let mut items = return_clause.items.clone();
        let mut input = input;
        let saved = self.seed_source.replace(input.clone());
        for item in &mut items {
            let rewritten = self.rewrite_exists_in_value(input, &mut item.expression);
            input = match rewritten {
                Ok(input) => input,
                Err(e) => {
                    self.seed_source = saved;
                    return Err(e);
                }
            };
        }
        self.seed_source = saved;

        for item in &items {
            let alias = &item.alias;
            match &item.expression {
                ValueExpression::Variable(var) => {
                    match self.variables.get(var) {
                        // if it is a node variable, expand to all properties
                        Some(label) if label != "Unwound" && label != "Path" => {
                            let mapping = self.config.get_node_mapping(label).ok_or_else(|| {
                                GraphError::PlanError {
                                    message: format!("Node label '{}' doesn't exist", label),
                                    location: Location::new(file!(), line!(), column!()),
                                }
                            })?;

                            projections.push(ProjectionItem {
                                expression: ValueExpression::Property(PropertyRef {
                                    variable: var.clone(),
                                    property: mapping.id_field.clone(),
                                }),
                                alias: alias
                                    .clone()
                                    .map(|name| format!("{}.{}", name, mapping.id_field)),
                            });

                            for prop in &mapping.property_fields {
                                projections.push(ProjectionItem {
                                    expression: ValueExpression::Property(PropertyRef {
                                        variable: var.clone(),
                                        property: prop.clone(),
                                    }),
                                    alias: alias.clone().map(|name| format!("{}.{}", name, prop)),
                                });
                            }
                        }
                        _ => {
                            projections.push(ProjectionItem {
                                expression: item.expression.clone(),
                                alias: alias.clone(),
                            });
                        }
                    }
                }
                _ => {
                    let mut expression = item.expression.clone();
                    self.resolve_value(&mut expression);
                    projections.push(ProjectionItem {
                        expression,
                        alias: alias.clone(),
                    });
                }
            }
        }

        let mut plan = LogicalOperator::Project {
            input: Box::new(input),
            projections,
        };

        // Add DISTINCT if needed
        if return_clause.distinct {
            plan = LogicalOperator::Distinct {
                input: Box::new(plan),
            };
        }

        Ok(plan)
    }

    /// Plan WITH clause - intermediate projection/aggregation with optional ORDER BY and LIMIT
    fn plan_with_clause(
        &mut self,
        with_clause: &WithClause,
        input: LogicalOperator,
    ) -> Result<LogicalOperator> {
        // WITH creates a projection (like RETURN), but its output must stay addressable by
        // later clauses: an unaliased `b.name` keeps the internal name `b__name`, and a
        // bare node variable passes all its columns through (see `build_simple_project`).
        let mut projections = Vec::with_capacity(with_clause.items.len());
        let mut items = with_clause.items.clone();
        let mut input = input;
        let saved = self.seed_source.replace(input.clone());
        for item in &mut items {
            let rewritten = self.rewrite_exists_in_value(input, &mut item.expression);
            input = match rewritten {
                Ok(input) => input,
                Err(e) => {
                    self.seed_source = saved;
                    return Err(e);
                }
            };
        }
        self.seed_source = saved;
        for item in &items {
            let alias = match (&item.alias, &item.expression) {
                (Some(alias), ValueExpression::Variable(var)) => {
                    // `WITH b AS x` renames a node: x keeps b's label.
                    if let Some(label) = self.variables.get(var).cloned() {
                        self.variables.insert(alias.clone(), label);
                    }
                    Some(alias.clone())
                }
                (Some(alias), _) => Some(alias.clone()),
                (None, ValueExpression::Property(prop)) => {
                    Some(qualify_column(&prop.variable, &prop.property))
                }
                // A carried scalar (`WITH a, bCount`) keeps its own name, lowercased like every
                // alias, so later clauses find it (RETURN's display names keep the written case).
                (None, ValueExpression::Variable(var)) => Some(var.clone()),
                (None, _) => None,
            };
            let mut expression = item.expression.clone();
            if !matches!(expression, ValueExpression::Variable(_)) {
                self.resolve_value(&mut expression);
            }
            projections.push(ProjectionItem { expression, alias });
        }
        // Which outputs are lists of node ids: `collect(node)`, or such a list carried on.
        let mut node_lists = HashMap::new();
        for item in &items {
            let label = match &item.expression {
                ValueExpression::AggregateFunction { name, args, .. }
                    if name.eq_ignore_ascii_case("collect") =>
                {
                    match args.as_slice() {
                        [ValueExpression::Variable(v)] => self
                            .variables
                            .get(v)
                            .filter(|l| self.config.get_node_mapping(l).is_some())
                            .cloned(),
                        _ => None,
                    }
                }
                ValueExpression::Variable(v) => self.node_lists.get(v).cloned(),
                _ => None,
            };
            let name = match (&item.alias, &item.expression) {
                (Some(alias), _) => Some(alias.clone()),
                (None, ValueExpression::Variable(v)) => Some(v.clone()),
                _ => None,
            };
            if let (Some(label), Some(name)) = (label, name) {
                node_lists.insert(name, label);
            }
        }
        self.node_lists = node_lists;
        // WITH ends the scope of everything it does not project: a later MATCH reusing a
        // dropped name binds a new variable instead of expecting columns that are gone.
        let kept: Vec<&str> = with_clause
            .items
            .iter()
            .filter_map(|item| match (&item.alias, &item.expression) {
                (Some(alias), ValueExpression::Variable(_)) => Some(alias.as_str()),
                (None, ValueExpression::Variable(var)) => Some(var.as_str()),
                _ => None,
            })
            .collect();
        self.variables.retain(|var, _| kept.contains(&var.as_str()));

        let mut plan = LogicalOperator::Project {
            input: Box::new(input),
            projections,
        };
        if with_clause.distinct {
            plan = LogicalOperator::Distinct {
                input: Box::new(plan),
            };
        }

        // Apply ORDER BY within WITH if present
        if let Some(order_by) = &with_clause.order_by {
            plan = LogicalOperator::Sort {
                input: Box::new(plan),
                sort_items: self.sort_items(order_by),
            };
        }

        // Apply LIMIT within WITH if present
        if let Some(limit) = &with_clause.limit {
            plan = LogicalOperator::Limit {
                input: Box::new(plan),
                count: limit.value()?,
            };
        }

        if let Some(where_clause) = &with_clause.where_clause {
            plan = self.filter(plan, &where_clause.expression)?;
        }

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parser::parse_cypher_query, NodeMapping};

    #[test]
    fn test_relationship_query_logical_plan_structure() {
        let query_text = r#"MATCH (p:Person {name: "Alice"})-[:KNOWS]->(f:Person) WHERE f.age > 30 RETURN f.name"#;

        // Parse the query
        let ast = parse_cypher_query(query_text).unwrap();

        // Plan to logical operators
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical_plan = planner.plan(&ast).unwrap();

        // Verify the overall structure is a projection
        match &logical_plan {
            LogicalOperator::Project { input, projections } => {
                // Verify projection is f.name
                assert_eq!(projections.len(), 1);
                match &projections[0].expression {
                    ValueExpression::Property(prop_ref) => {
                        assert_eq!(prop_ref.variable, "f");
                        assert_eq!(prop_ref.property, "name");
                    }
                    _ => panic!("Expected property reference for f.name"),
                }

                // Verify input is a filter for f.age > 30
                match input.as_ref() {
                    LogicalOperator::Filter {
                        predicate,
                        input: filter_input,
                    } => {
                        // Verify the predicate is f.age > 30
                        match predicate {
                            BooleanExpression::Comparison {
                                left,
                                operator,
                                right,
                            } => {
                                match left {
                                    ValueExpression::Property(prop_ref) => {
                                        assert_eq!(prop_ref.variable, "f");
                                        assert_eq!(prop_ref.property, "age");
                                    }
                                    _ => panic!("Expected property reference for f.age"),
                                }
                                assert_eq!(*operator, ComparisonOperator::GreaterThan);
                                match right {
                                    ValueExpression::Literal(PropertyValue::Integer(val)) => {
                                        assert_eq!(*val, 30);
                                    }
                                    _ => panic!("Expected integer literal 30"),
                                }
                            }
                            _ => panic!("Expected comparison expression"),
                        }

                        // Verify the input to the filter is an expand operation
                        match filter_input.as_ref() {
                            LogicalOperator::Expand {
                                input: expand_input,
                                source_variable,
                                target_variable,
                                relationship_types,
                                direction,
                                ..
                            } => {
                                assert_eq!(source_variable, "p");
                                assert_eq!(target_variable, "f");
                                assert_eq!(relationship_types, &vec!["KNOWS".to_string()]);
                                assert_eq!(*direction, RelationshipDirection::Outgoing);

                                // Verify the input to expand is a scan with properties for p.name = 'Alice'
                                match expand_input.as_ref() {
                                    LogicalOperator::ScanByLabel {
                                        variable,
                                        label,
                                        properties,
                                    } => {
                                        assert_eq!(variable, "p");
                                        assert_eq!(label, "Person");

                                        // Verify the properties contain name = "Alice"
                                        assert_eq!(properties.len(), 1);
                                        match properties.get("name") {
                                            Some(PropertyValue::String(val)) => {
                                                assert_eq!(val, "Alice");
                                            }
                                            _ => {
                                                panic!("Expected name property with value 'Alice'")
                                            }
                                        }
                                    }
                                    _ => panic!("Expected ScanByLabel with properties for Person"),
                                }
                            }
                            _ => panic!("Expected Expand operation"),
                        }
                    }
                    _ => panic!("Expected Filter for f.age > 30"),
                }
            }
            _ => panic!("Expected Project at the top level"),
        }
    }

    #[test]
    fn test_simple_node_query_logical_plan() {
        let query_text = "MATCH (n:Person) RETURN n.name";

        let ast = parse_cypher_query(query_text).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical_plan = planner.plan(&ast).unwrap();

        // Should be: Project { input: ScanByLabel }
        match &logical_plan {
            LogicalOperator::Project { input, projections } => {
                assert_eq!(projections.len(), 1);
                match input.as_ref() {
                    LogicalOperator::ScanByLabel {
                        variable, label, ..
                    } => {
                        assert_eq!(variable, "n");
                        assert_eq!(label, "Person");
                    }
                    _ => panic!("Expected ScanByLabel"),
                }
            }
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn test_node_with_properties_logical_plan() {
        let query_text = "MATCH (n:Person {age: 25}) RETURN n.name";

        let ast = parse_cypher_query(query_text).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical_plan = planner.plan(&ast).unwrap();

        // Should be: Project { input: ScanByLabel with properties }
        // Properties from MATCH clause are pushed down to the scan level
        match &logical_plan {
            LogicalOperator::Project { input, .. } => {
                match input.as_ref() {
                    LogicalOperator::ScanByLabel {
                        variable,
                        label,
                        properties,
                    } => {
                        assert_eq!(variable, "n");
                        assert_eq!(label, "Person");

                        // Verify the properties are in the scan
                        assert_eq!(properties.len(), 1);
                        match properties.get("age") {
                            Some(PropertyValue::Integer(25)) => {}
                            _ => panic!("Expected age property with value 25"),
                        }
                    }
                    _ => panic!("Expected ScanByLabel with properties"),
                }
            }
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn test_variable_length_path_logical_plan() {
        let query_text = "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN b.name";

        let ast = parse_cypher_query(query_text).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical_plan = planner.plan(&ast).unwrap();

        // Should be: Project { input: VariableLengthExpand { input: ScanByLabel } }
        match &logical_plan {
            LogicalOperator::Project { input, .. } => match input.as_ref() {
                LogicalOperator::VariableLengthExpand {
                    input: expand_input,
                    source_variable,
                    target_variable,
                    relationship_types,
                    min_length,
                    max_length,
                    ..
                } => {
                    assert_eq!(source_variable, "a");
                    assert_eq!(target_variable, "b");
                    assert_eq!(relationship_types, &vec!["KNOWS".to_string()]);
                    assert_eq!(*min_length, Some(1));
                    assert_eq!(*max_length, Some(2));

                    match expand_input.as_ref() {
                        LogicalOperator::ScanByLabel {
                            variable, label, ..
                        } => {
                            assert_eq!(variable, "a");
                            assert_eq!(label, "Person");
                        }
                        _ => panic!("Expected ScanByLabel"),
                    }
                }
                _ => panic!("Expected VariableLengthExpand"),
            },
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn test_where_clause_logical_plan() {
        // Note: Current parser only supports simple comparisons, not AND/OR
        let query_text = r#"MATCH (n:Person) WHERE n.age > 25 RETURN n.name"#;

        let ast = parse_cypher_query(query_text).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical_plan = planner.plan(&ast).unwrap();

        // Should be: Project { input: Filter { input: ScanByLabel } }
        match &logical_plan {
            LogicalOperator::Project { input, .. } => {
                match input.as_ref() {
                    LogicalOperator::Filter {
                        predicate,
                        input: scan_input,
                    } => {
                        // Verify it's a simple comparison: n.age > 25
                        match predicate {
                            BooleanExpression::Comparison {
                                left,
                                operator,
                                right: _,
                            } => {
                                match left {
                                    ValueExpression::Property(prop_ref) => {
                                        assert_eq!(prop_ref.variable, "n");
                                        assert_eq!(prop_ref.property, "age");
                                    }
                                    _ => panic!("Expected property reference for age"),
                                }
                                assert_eq!(*operator, ComparisonOperator::GreaterThan);
                            }
                            _ => panic!("Expected comparison expression"),
                        }

                        match scan_input.as_ref() {
                            LogicalOperator::ScanByLabel { .. } => {}
                            _ => panic!("Expected ScanByLabel"),
                        }
                    }
                    _ => panic!("Expected Filter"),
                }
            }
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn test_multiple_node_patterns_join_in_match() {
        let query_text = "MATCH (a:Person), (b:Company) RETURN a.name, b.name";

        let ast = parse_cypher_query(query_text).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical_plan = planner.plan(&ast).unwrap();

        // Expect: Project { input: Join { left: Scan(a:Person), right: Scan(b:Company) } }
        match &logical_plan {
            LogicalOperator::Project { input, projections } => {
                assert_eq!(projections.len(), 2);
                match input.as_ref() {
                    LogicalOperator::Join {
                        left,
                        right,
                        join_type,
                    } => {
                        assert!(matches!(join_type, JoinType::Cross));
                        match left.as_ref() {
                            LogicalOperator::ScanByLabel {
                                variable, label, ..
                            } => {
                                assert_eq!(variable, "a");
                                assert_eq!(label, "Person");
                            }
                            _ => panic!("Expected left ScanByLabel for a:Person"),
                        }
                        match right.as_ref() {
                            LogicalOperator::ScanByLabel {
                                variable, label, ..
                            } => {
                                assert_eq!(variable, "b");
                                assert_eq!(label, "Company");
                            }
                            _ => panic!("Expected right ScanByLabel for b:Company"),
                        }
                    }
                    _ => panic!("Expected Join after Project"),
                }
            }
            _ => panic!("Expected Project at top level"),
        }
    }

    #[test]
    fn test_shared_variable_chained_paths_in_match() {
        let query_text =
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (b)-[:LIKES]->(c:Thing) RETURN c.name";

        let ast = parse_cypher_query(query_text).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical_plan = planner.plan(&ast).unwrap();

        // Expect: Project { input: Expand (b->c) { input: Expand (a->b) { input: Scan(a) } } }
        match &logical_plan {
            LogicalOperator::Project { input, .. } => match input.as_ref() {
                LogicalOperator::Expand {
                    source_variable: src2,
                    target_variable: tgt2,
                    input: inner2,
                    ..
                } => {
                    assert_eq!(src2, "b");
                    assert_eq!(tgt2, "c");
                    match inner2.as_ref() {
                        LogicalOperator::Expand {
                            source_variable: src1,
                            target_variable: tgt1,
                            input: inner1,
                            ..
                        } => {
                            assert_eq!(src1, "a");
                            assert_eq!(tgt1, "b");
                            match inner1.as_ref() {
                                LogicalOperator::ScanByLabel {
                                    variable, label, ..
                                } => {
                                    assert_eq!(variable, "a");
                                    assert_eq!(label, "Person");
                                }
                                _ => panic!("Expected ScanByLabel for a:Person"),
                            }
                        }
                        _ => panic!("Expected first Expand a->b"),
                    }
                }
                _ => panic!("Expected second Expand b->c at top of input"),
            },
            _ => panic!("Expected Project at top level"),
        }
    }

    #[test]
    fn test_fixed_length_variable_path_is_expand() {
        let query_text = "MATCH (a:Person)-[:KNOWS*1..1]->(b:Person) RETURN b.name";

        let ast = parse_cypher_query(query_text).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical_plan = planner.plan(&ast).unwrap();

        match &logical_plan {
            LogicalOperator::Project { input, .. } => match input.as_ref() {
                LogicalOperator::Expand {
                    source_variable,
                    target_variable,
                    ..
                } => {
                    assert_eq!(source_variable, "a");
                    assert_eq!(target_variable, "b");
                }
                _ => panic!("Expected Expand for fixed-length *1..1"),
            },
            _ => panic!("Expected Project at top level"),
        }
    }

    #[test]
    fn test_distinct_and_order_limit_wrapping() {
        // DISTINCT should wrap Project with Distinct
        let q1 = "MATCH (n:Person) RETURN DISTINCT n.name";
        let ast1 = parse_cypher_query(q1).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical1 = planner.plan(&ast1).unwrap();
        match logical1 {
            LogicalOperator::Distinct { input } => match *input {
                LogicalOperator::Project { .. } => {}
                _ => panic!("Expected Project under Distinct"),
            },
            _ => panic!("Expected Distinct at top level"),
        }

        // ORDER BY + LIMIT should be Limit(Sort(Project(..)))
        let q2 = "MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT 10";
        let ast2 = parse_cypher_query(q2).unwrap();
        let mut planner2 = LogicalPlanner::new(&config);
        let logical2 = planner2.plan(&ast2).unwrap();
        match logical2 {
            LogicalOperator::Limit { input, count } => {
                assert_eq!(count, 10);
                match *input {
                    LogicalOperator::Sort { input: inner, .. } => match *inner {
                        LogicalOperator::Project { .. } => {}
                        _ => panic!("Expected Project under Sort"),
                    },
                    _ => panic!("Expected Sort under Limit"),
                }
            }
            _ => panic!("Expected Limit at top level"),
        }
    }

    #[test]
    fn test_order_skip_limit_wrapping() {
        // ORDER BY + SKIP + LIMIT should be Limit(Offset(Sort(Project(..))))
        let q = "MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 5 LIMIT 10";
        let ast = parse_cypher_query(q).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical = planner.plan(&ast).unwrap();
        match logical {
            LogicalOperator::Limit { input, count } => {
                assert_eq!(count, 10);
                match *input {
                    LogicalOperator::Offset {
                        input: inner,
                        offset,
                    } => {
                        assert_eq!(offset, 5);
                        match *inner {
                            LogicalOperator::Sort { input: inner2, .. } => match *inner2 {
                                LogicalOperator::Project { .. } => {}
                                _ => panic!("Expected Project under Sort"),
                            },
                            _ => panic!("Expected Sort under Offset"),
                        }
                    }
                    _ => panic!("Expected Offset under Limit"),
                }
            }
            _ => panic!("Expected Limit at top level"),
        }
    }

    #[test]
    fn test_skip_only_wrapping() {
        // SKIP only should be Offset(Project(..))
        let q = "MATCH (n:Person) RETURN n.name SKIP 3";
        let ast = parse_cypher_query(q).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical = planner.plan(&ast).unwrap();
        match logical {
            LogicalOperator::Offset { input, offset } => {
                assert_eq!(offset, 3);
                match *input {
                    LogicalOperator::Project { .. } => {}
                    _ => panic!("Expected Project under Offset"),
                }
            }
            _ => panic!("Expected Offset at top level"),
        }
    }

    #[test]
    fn test_relationship_properties_pushed_into_expand() {
        let q = "MATCH (a)-[:KNOWS {since: 2020}]->(b) RETURN b.name";
        let ast = parse_cypher_query(q).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical = planner.plan(&ast).unwrap();
        match logical {
            LogicalOperator::Project { input, .. } => match *input {
                LogicalOperator::Expand { properties, .. } => match properties.get("since") {
                    Some(PropertyValue::Integer(2020)) => {}
                    _ => panic!("Expected relationship property since=2020 in Expand"),
                },
                _ => panic!("Expected Expand under Project"),
            },
            _ => panic!("Expected Project at top level"),
        }
    }

    #[test]
    fn test_multiple_match_clauses_cross_join() {
        let q = "MATCH (a:Person) MATCH (b:Company) RETURN a.name, b.name";
        let ast = parse_cypher_query(q).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical = planner.plan(&ast).unwrap();
        match logical {
            LogicalOperator::Project { input, .. } => match *input {
                LogicalOperator::Join {
                    left,
                    right,
                    join_type,
                } => {
                    assert!(matches!(join_type, JoinType::Cross));
                    match (*left, *right) {
                        (
                            LogicalOperator::ScanByLabel {
                                variable: va,
                                label: la,
                                ..
                            },
                            LogicalOperator::ScanByLabel {
                                variable: vb,
                                label: lb,
                                ..
                            },
                        ) => {
                            assert_eq!(va, "a");
                            assert_eq!(la, "Person");
                            assert_eq!(vb, "b");
                            assert_eq!(lb, "Company");
                        }
                        _ => panic!("Expected two scans under Join"),
                    }
                }
                _ => panic!("Expected Join under Project"),
            },
            _ => panic!("Expected Project at top level"),
        }
    }

    #[test]
    fn test_variable_only_node_default_label() {
        let q = "MATCH (x) RETURN x.name";
        let ast = parse_cypher_query(q).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical = planner.plan(&ast).unwrap();
        match logical {
            LogicalOperator::Project { input, .. } => match *input {
                LogicalOperator::ScanByLabel {
                    variable, label, ..
                } => {
                    assert_eq!(variable, "x");
                    assert_eq!(label, "Node");
                }
                _ => panic!("Expected ScanByLabel under Project"),
            },
            _ => panic!("Expected Project at top level"),
        }
    }

    #[test]
    fn test_multi_label_node_is_a_union_of_its_labels() {
        // Upstream silently scanned only the first label.
        let q = "MATCH (n:Person:Employee) RETURN n.name";
        let ast = parse_cypher_query(q).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical = planner.plan(&ast).unwrap();
        let LogicalOperator::Project { input, .. } = logical else {
            panic!("Expected Project at top level");
        };
        let LogicalOperator::Union { inputs } = *input else {
            panic!("Expected Union under Project");
        };
        let labels: Vec<_> = inputs
            .iter()
            .map(|op| match op {
                LogicalOperator::ScanByLabel { label, .. } => label.as_str(),
                other => panic!("Expected ScanByLabel, got {other:?}"),
            })
            .collect();
        assert_eq!(labels, ["Person", "Employee"]);
    }

    #[test]
    fn test_open_ended_and_partial_var_length_ranges() {
        // * (unbounded)
        let q1 = "MATCH (a)-[:R*]->(b:Node) RETURN b.name";
        let ast1 = parse_cypher_query(q1).unwrap();
        let config = GraphConfig::default();
        let mut planner1 = LogicalPlanner::new(&config);
        let plan1 = planner1.plan(&ast1).unwrap();
        match plan1 {
            LogicalOperator::Project { input, .. } => match *input {
                LogicalOperator::VariableLengthExpand {
                    min_length,
                    max_length,
                    ..
                } => {
                    assert_eq!(min_length, None);
                    assert_eq!(max_length, None);
                }
                _ => panic!("Expected VariableLengthExpand for *"),
            },
            _ => panic!("Expected Project at top level"),
        }

        // *2.. (min only)
        let q2 = "MATCH (a)-[:R*2..]->(b) RETURN b.name";
        let ast2 = parse_cypher_query(q2).unwrap();
        let mut planner2 = LogicalPlanner::new(&config);
        let plan2 = planner2.plan(&ast2).unwrap();
        match plan2 {
            LogicalOperator::Project { input, .. } => match *input {
                LogicalOperator::VariableLengthExpand {
                    min_length,
                    max_length,
                    ..
                } => {
                    assert_eq!(min_length, Some(2));
                    assert_eq!(max_length, None);
                }
                _ => panic!("Expected VariableLengthExpand for *2.."),
            },
            _ => panic!("Expected Project at top level"),
        }

        // *..3 (max only)
        let q3 = "MATCH (a)-[:R*..3]->(b) RETURN b.name";
        let ast3 = parse_cypher_query(q3).unwrap();
        let mut planner3 = LogicalPlanner::new(&config);
        let plan3 = planner3.plan(&ast3).unwrap();
        match plan3 {
            LogicalOperator::Project { input, .. } => match *input {
                LogicalOperator::VariableLengthExpand {
                    min_length,
                    max_length,
                    ..
                } => {
                    assert_eq!(min_length, None);
                    assert_eq!(max_length, Some(3));
                }
                _ => panic!("Expected VariableLengthExpand for *..3"),
            },
            _ => panic!("Expected Project at top level"),
        }
    }

    #[test]
    fn test_variable_reuse_across_patterns() {
        let query_text =
            "MATCH (a:Person)-[:KNOWS]->(shared:Person), (shared)-[:KNOWS]->(b:Person) RETURN b.name";

        let ast = parse_cypher_query(query_text).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let logical_plan = planner.plan(&ast).unwrap();

        // Expect: Project { Expand(shared->b) { Expand(a->shared) { Scan(a) } } }
        match &logical_plan {
            LogicalOperator::Project { input, .. } => match input.as_ref() {
                LogicalOperator::Expand {
                    input: inner,
                    source_variable,
                    target_variable,
                    ..
                } => {
                    assert_eq!(source_variable, "shared");
                    assert_eq!(target_variable, "b");

                    match inner.as_ref() {
                        LogicalOperator::Expand {
                            source_variable: first_src,
                            target_variable: first_dst,
                            ..
                        } => {
                            assert_eq!(first_src, "a");
                            assert_eq!(first_dst, "shared");
                        }
                        _ => panic!("Expected first Expand (a->shared)"),
                    }
                }
                _ => panic!("Expected second Expand (shared->b)"),
            },
            _ => panic!("Expected Project at top level"),
        }
    }

    #[test]
    fn test_variable_reuse_with_conflicting_labels() {
        let query_text =
            "MATCH (a:Person)-[:KNOWS]->(shared:Person), (shared:Company)-[:EMPLOYS]->(b:Person) RETURN b.name";

        let ast = parse_cypher_query(query_text).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let err = planner.plan(&ast).unwrap_err();
        let err_msg = err.to_string();

        assert!(
            err_msg.contains("already has label 'Person'")
                && err_msg.contains("cannot redefine as 'Company'"),
            "Expected error about label conflict, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_return_node_variable() {
        let query_text = "MATCH (a:Person) RETURN a";

        let ast = parse_cypher_query(query_text).unwrap();
        let config = GraphConfig::builder()
            .with_node_mapping(NodeMapping {
                label: "Person".to_string(),
                id_field: "id".to_string(),
                property_fields: vec!["name".to_string(), "age".to_string()],
                filter_conditions: None,
            })
            .build()
            .unwrap();
        let mut planner = LogicalPlanner::new(&config);
        let logical_plan = planner.plan(&ast).unwrap();

        match &logical_plan {
            LogicalOperator::Project { projections, .. } => {
                assert_eq!(projections.len(), 3);
                match &projections[0].expression {
                    ValueExpression::Property(prop_ref) => {
                        assert_eq!(prop_ref.variable, "a");
                        assert_eq!(prop_ref.property, "id");
                    }
                    _ => panic!("Expected property reference for a.id"),
                }
                match &projections[1].expression {
                    ValueExpression::Property(prop_ref) => {
                        assert_eq!(prop_ref.variable, "a");
                        assert_eq!(prop_ref.property, "name");
                    }
                    _ => panic!("Expected property reference for a.name"),
                }
                match &projections[2].expression {
                    ValueExpression::Property(prop_ref) => {
                        assert_eq!(prop_ref.variable, "a");
                        assert_eq!(prop_ref.property, "age");
                    }
                    _ => panic!("Expected property reference for a.age"),
                }
            }
            _ => panic!("Expected Project at the top level"),
        }
    }

    #[test]
    fn test_return_node_variable_with_alias() {
        let query_text = "MATCH (a:Person) RETURN a AS b";

        let ast = parse_cypher_query(query_text).unwrap();
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .build()
            .unwrap();
        let mut planner = LogicalPlanner::new(&config);
        let logical_plan = planner.plan(&ast).unwrap();

        match &logical_plan {
            LogicalOperator::Project { projections, .. } => {
                assert_eq!(projections.len(), 1);
                match &projections[0].expression {
                    ValueExpression::Property(prop_ref) => {
                        assert_eq!(prop_ref.variable, "a");
                        assert_eq!(prop_ref.property, "id");
                    }
                    _ => panic!("Expected property reference for a.id"),
                }
                match &projections[0].alias {
                    Some(alias) => assert_eq!(alias, "b.id"),
                    None => panic!("Expected alias for a.id as b.id"),
                }
            }
            _ => panic!("Expected Project at the top level"),
        }
    }

    #[test]
    fn test_return_node_variable_no_label() {
        let query_text = "MATCH (a:Person) RETURN a";

        let ast = parse_cypher_query(query_text).unwrap();
        let config = GraphConfig::default();
        let mut planner = LogicalPlanner::new(&config);
        let err = planner.plan(&ast).unwrap_err();
        let err_msg = err.to_string();

        assert!(
            err_msg.contains("Node label 'Person' doesn't exist"),
            "Expected error about missing label 'Person', got: {}",
            err_msg
        );
    }

    /// Relationship types of the Expands under `op`, innermost (first executed) first.
    fn expand_order(op: &LogicalOperator, out: &mut Vec<String>) {
        match op {
            LogicalOperator::Expand {
                input,
                relationship_types,
                ..
            } => {
                expand_order(input, out);
                out.push(relationship_types[0].clone());
            }
            LogicalOperator::Filter { input, .. }
            | LogicalOperator::Project { input, .. }
            | LogicalOperator::Distinct { input }
            | LogicalOperator::Sort { input, .. }
            | LogicalOperator::Limit { input, .. }
            | LogicalOperator::Offset { input, .. } => expand_order(input, out),
            LogicalOperator::OptionalMatch { optional, .. } => expand_order(optional, out),
            _ => {}
        }
    }

    #[test]
    fn continued_path_starts_from_the_bound_end_with_smaller_fan_out() {
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .node_table("Post", "id")
            .node_table("Forum", "id")
            .rel_table("postHasCreator", "Post", "Person", "src", "dst")
            .rel_table("containerOf", "Forum", "Post", "src", "dst")
            .build()
            .unwrap();
        let q = "MATCH (friend:Person), (forum:Forum) WITH friend, forum \
                 OPTIONAL MATCH (friend)<-[:postHasCreator]-(post:Post)<-[:containerOf]-(forum) \
                 RETURN count(post.id) AS n";
        let ast = parse_cypher_query(q).unwrap();
        let first_hop = |creator_in: f64, container_out: f64| {
            let fan_out: FanOut = [
                (("posthascreator".to_string(), false), creator_in),
                (("posthascreator".to_string(), true), 1.0),
                (("containerof".to_string(), true), container_out),
                (("containerof".to_string(), false), 1.0),
            ]
            .into_iter()
            .collect();
            let plan = LogicalPlanner::new(&config)
                .with_fan_out(Some(&fan_out))
                .plan(&ast)
                .unwrap();
            let mut order = Vec::new();
            expand_order(&plan, &mut order);
            order[0].clone()
        };
        // Forums hold ~11 posts, people create ~100: start from the forum.
        assert_eq!(first_hop(100.0, 11.0), "containerOf");
        // The other way round keeps the written order.
        assert_eq!(first_hop(11.0, 100.0), "postHasCreator");
    }

    #[test]
    fn continued_path_bound_only_at_its_far_end_starts_there() {
        // After `WITH DISTINCT b`, `a` is a new variable: expansion must start from `b`.
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table("KNOWS", "Person", "Person", "src", "dst")
            .build()
            .unwrap();
        let q = "MATCH (a:Person)-[:KNOWS]->(b:Person) WITH DISTINCT b \
                 MATCH (a:Person)-[:KNOWS]->(b) RETURN a.id, b.id";
        let plan = LogicalPlanner::new(&config)
            .plan(&parse_cypher_query(q).unwrap())
            .unwrap();
        let text = format!("{plan:?}");
        let last_expand = text.rfind("Expand {").unwrap();
        assert!(
            text[last_expand..].contains("source_variable: \"b\""),
            "{plan:#?}"
        );
    }
}
