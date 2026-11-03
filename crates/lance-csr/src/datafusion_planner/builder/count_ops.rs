// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! `count(*)` over a two-hop pattern, computed from per-node degrees.
//!
//! The paths through a middle node `m` number `in(m) × out(m)`, so the count is
//! `Σ_m n1(m) · n2(m)`, where `n1` and `n2` count each hop's edges at `m` that pass that
//! hop's filters (eager aggregation, Yan and Larson). Each hop is planned as a one-hop
//! pattern of its own from `m` and grouped down to one row per `m` before the join, so
//! the work follows the edges, not the paths. On the CSR the grouping is the expansion
//! itself: it counts each `m`'s neighbours in place (`HopDegreesNode`).

use std::collections::HashSet;

use crate::ast::{BooleanExpression, ComparisonOperator, RelationshipDirection, ValueExpression};
use crate::case_insensitive::qualify_column;
use crate::csr_expand::HopDegreesNode;
use crate::datafusion_planner::analysis::PlanningContext;
use crate::datafusion_planner::DataFusionPlanner;
use crate::error::Result;
use crate::logical_plan::*;
use datafusion::common::Column;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::{count, sum};
use datafusion::logical_expr::{
    col, lit, when, Expr, Extension, JoinType, LogicalPlan, LogicalPlanBuilder,
};

/// One `Expand` of the pattern, with its edge's endpoints in stored direction.
struct Hop<'a> {
    op: &'a LogicalOperator,
    source: &'a str,
    target: &'a str,
    relationship: Option<&'a str>,
    rel_type: &'a str,
    /// `(src, dst)` of the edge: the traversal reversed for an incoming hop.
    edge: (&'a str, &'a str),
}

impl<'a> Hop<'a> {
    fn of(op: &'a LogicalOperator) -> Option<(Self, &'a LogicalOperator)> {
        let LogicalOperator::Expand {
            input,
            source_variable,
            target_variable,
            relationship_types,
            direction,
            relationship_variable,
            ..
        } = op
        else {
            return None;
        };
        let (source, target) = (source_variable.as_str(), target_variable.as_str());
        let edge = match direction {
            RelationshipDirection::Outgoing => (source, target),
            RelationshipDirection::Incoming => (target, source),
            RelationshipDirection::Undirected => return None,
        };
        (relationship_types.len() == 1 && source != target).then_some((
            Self {
                op,
                source,
                target,
                relationship: relationship_variable.as_deref(),
                rel_type: &relationship_types[0],
                edge,
            },
            input.as_ref(),
        ))
    }

    fn variables(&self) -> HashSet<&'a str> {
        [self.source, self.target]
            .into_iter()
            .chain(self.relationship)
            .collect()
    }

    /// Mean edges per `middle` node, from the engine's key statistics.
    fn fan_out(&self, fan_out: &FanOut, middle: &str) -> Option<f64> {
        let outgoing = self.edge.0 == middle;
        fan_out
            .get(&(self.rel_type.to_lowercase(), outgoing))
            .copied()
    }

    /// The endpoint that is not `middle`.
    fn other(&self, middle: &str) -> &'a str {
        if self.source == middle {
            self.target
        } else {
            self.source
        }
    }
}

/// Names of the variables `expr` reads; `None` for an `EXISTS` pattern, whose variables
/// this does not track.
fn referenced(expr: &BooleanExpression, out: &mut HashSet<String>) -> Option<()> {
    match expr {
        BooleanExpression::And(a, b) | BooleanExpression::Or(a, b) => {
            referenced(a, out)?;
            referenced(b, out)
        }
        BooleanExpression::Not(inner) => referenced(inner, out),
        BooleanExpression::Exists(p) => {
            out.insert(p.variable.clone());
            Some(())
        }
        BooleanExpression::PatternExists { .. } => None,
        leaf => {
            crate::ast::transform_boolean_values(&mut leaf.clone(), &mut |value| match value {
                ValueExpression::Variable(v) => {
                    out.insert(v.clone());
                }
                ValueExpression::Property(p) => {
                    out.insert(p.variable.clone());
                }
                ValueExpression::Literal(crate::ast::PropertyValue::Property(p)) => {
                    out.insert(p.variable.clone());
                }
                _ => {}
            });
            Some(())
        }
    }
}

fn conjuncts<'a>(expr: &'a BooleanExpression, out: &mut Vec<&'a BooleanExpression>) {
    match expr {
        BooleanExpression::And(a, b) => {
            conjuncts(a, out);
            conjuncts(b, out);
        }
        other => out.push(other),
    }
}

impl DataFusionPlanner {
    /// The node id column of `variable` (`b` -> `b__id`).
    pub(crate) fn id_column(&self, ctx: &PlanningContext, variable: &str) -> Option<String> {
        let label = ctx.analysis.var_to_label.get(variable)?;
        let mapping = self.config.get_node_mapping(label)?;
        Some(qualify_column(variable, &mapping.id_field))
    }

    /// Whether `expr` is `NOT (x1 = y1 AND x2 = y2)` over node ids, with `(x1, y1)` and
    /// `(x2, y2)` being `pairs`: the predicate the logical planner adds so that two hops of
    /// one type never bind the same edge (`distinct_relationships`).
    fn is_distinct_edges(
        &self,
        ctx: &PlanningContext,
        expr: &BooleanExpression,
        pairs: [(&str, &str); 2],
    ) -> bool {
        let BooleanExpression::Not(inner) = expr else {
            return false;
        };
        let BooleanExpression::And(first, second) = inner.as_ref() else {
            return false;
        };
        let is_id = |value: &ValueExpression, variable: &str| {
            matches!(value, ValueExpression::Property(p)
                if p.variable == variable
                    && self.id_column(ctx, variable)
                        == Some(qualify_column(variable, &p.property)))
        };
        let equal = |expr: &BooleanExpression, (x, y): (&str, &str)| {
            matches!(expr, BooleanExpression::Comparison {
                left, operator: ComparisonOperator::Equal, right
            } if is_id(left, x) && is_id(right, y))
        };
        equal(first, pairs[0]) && equal(second, pairs[1])
    }

    /// `RETURN count(*)` (or `count(v)` of a pattern variable, never null) over
    /// `(x)-[r1]-(m)-[r2]-(y)`, as `Σ_m n1(m) · n2(m) - s1(m) · s2(m)` without building
    /// any path. `None` (plan as usual) for any other query.
    ///
    /// Applies when every `WHERE` conjunct reads the variables of only one hop, so it can
    /// filter that hop's edges before grouping, and the hops' mean fan-outs at `m` say
    /// there are many more paths than edges (not so for `(i)<--(p)-->(city)`, one city
    /// per person, where grouping only adds work). When both hops have the same type the
    /// logical planner also requires that they not have the same endpoints; for a chain
    /// (`m` the target of one edge and the source of the other) that only excludes pairs
    /// of self-loops at `m`, which `s1` and `s2` count. Endpoints are compared, not edge
    /// ids, so k parallel self-loops exclude k² pairs, as on the join path.
    // shortcut: two same-type hops that share an end (`(a)-->(m)<--(c)`) keep the join
    // plan, as their correction needs grouping by both ends; add it if that shape matters.
    pub(crate) fn try_build_two_hop_count(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
        projections: &[ProjectionItem],
    ) -> Result<Option<LogicalPlan>> {
        let [item] = projections else {
            return Ok(None);
        };
        let ValueExpression::AggregateFunction {
            name,
            args,
            distinct: false,
        } = &item.expression
        else {
            return Ok(None);
        };
        if !name.eq_ignore_ascii_case("count") {
            return Ok(None);
        }
        // `count(b)` of a node arrives as `count(b.id)`; matched node ids are never null.
        let counted = match args.as_slice() {
            [ValueExpression::Variable(v)] => v.as_str(),
            [ValueExpression::Property(p)]
                if self.id_column(ctx, &p.variable)
                    == Some(qualify_column(&p.variable, &p.property)) =>
            {
                p.variable.as_str()
            }
            _ => return Ok(None),
        };

        let mut predicates = Vec::new();
        let mut op = input;
        while let LogicalOperator::Filter { input, predicate } = op {
            conjuncts(predicate, &mut predicates);
            op = input;
        }
        let Some((second, op)) = Hop::of(op) else {
            return Ok(None);
        };
        let Some((first, op)) = Hop::of(op) else {
            return Ok(None);
        };
        let LogicalOperator::ScanByLabel {
            variable: root,
            label: root_label,
            properties: root_properties,
        } = op
        else {
            return Ok(None);
        };
        if first.source != root.as_str() {
            return Ok(None);
        }
        // The second hop starts from the root (`m` re-rooted) or the first hop's target.
        let middle = second.source;
        let (middle_label, middle_properties) = if middle == root.as_str() {
            (root_label, root_properties)
        } else if middle == first.target {
            let LogicalOperator::Expand {
                target_label,
                target_properties,
                ..
            } = first.op
            else {
                unreachable!("Hop::of matched an Expand")
            };
            (target_label, target_properties)
        } else {
            return Ok(None);
        };
        let (first_vars, second_vars) = (first.variables(), second.variables());
        // A closing edge (cycle) or a relationship bound twice is not a free second hop.
        if first_vars.contains(second.target)
            || second.relationship.is_some_and(|r| first_vars.contains(r))
            || first.relationship.is_some_and(|r| second_vars.contains(r))
        {
            return Ok(None);
        }
        if counted != "*" && !first_vars.contains(counted) && !second_vars.contains(counted) {
            return Ok(None);
        }
        // Grouping pays off only when paths through `m` outnumber the edges grouped there:
        // at least twice, by mean fan-out. Without statistics, keep the join plan.
        let fan_out = |hop: &Hop| self.fan_out.as_ref().and_then(|f| hop.fan_out(f, middle));
        let (Some(d1), Some(d2)) = (fan_out(&first), fan_out(&second)) else {
            return Ok(None);
        };
        if d1 * d2 < 2.0 * (d1 + d2) {
            return Ok(None);
        }
        let ids = [middle, first.other(middle), second.other(middle)]
            .map(|variable| self.id_column(ctx, variable));
        let [Some(middle_id), Some(first_other), Some(second_other)] = ids else {
            return Ok(None);
        };

        let pairs = [(first.edge.0, second.edge.0), (first.edge.1, second.edge.1)];
        let chain = first.edge.1 == second.edge.0 || first.edge.0 == second.edge.1;
        let (mut middle_filter, mut first_filter, mut second_filter) = (vec![], vec![], vec![]);
        let mut self_loops = false;
        // Variables the kept filters read: a relationship no filter reads needs no edge.
        let mut read = HashSet::new();
        for predicate in predicates {
            if self.is_distinct_edges(ctx, predicate, pairs) {
                if !chain {
                    return Ok(None);
                }
                self_loops = true;
                continue;
            }
            let mut names = HashSet::new();
            if referenced(predicate, &mut names).is_none() {
                return Ok(None);
            }
            if names.iter().all(|n| n == middle) {
                middle_filter.push(predicate.clone());
            } else if names.iter().all(|n| first_vars.contains(n.as_str())) {
                first_filter.push(predicate.clone());
            } else if names.iter().all(|n| second_vars.contains(n.as_str())) {
                second_filter.push(predicate.clone());
            } else {
                return Ok(None);
            }
            read.extend(names);
        }
        // The distinct-edges predicate became the self-loop correction above, so edge
        // identity is needed only by a filter on the relationship; an unread one is left
        // unbound, and the CSR does not fetch its edge rows.
        let bound = |relationship: Option<&str>| {
            relationship
                .filter(|r| read.contains(*r))
                .map(str::to_string)
        };

        let filtered = |op: LogicalOperator, predicates: Vec<BooleanExpression>| match predicates
            .into_iter()
            .reduce(|a, b| BooleanExpression::And(Box::new(a), Box::new(b)))
        {
            Some(predicate) => LogicalOperator::Filter {
                input: Box::new(op),
                predicate,
            },
            None => op,
        };
        // Each hop is re-rooted at `m`, after the filters on `m` alone, so that on the CSR
        // it reads `m`'s neighbours in one direction and can count them in place.
        let from_middle = |hop: &Hop, predicates: Vec<BooleanExpression>| {
            let LogicalOperator::Expand {
                target_label,
                relationship_types,
                direction,
                relationship_variable,
                properties,
                target_properties,
                ..
            } = hop.op
            else {
                unreachable!("Hop::of matched an Expand")
            };
            let (other_label, other_properties, direction) = if hop.source == middle {
                (target_label, target_properties, direction.clone())
            } else {
                // Only the first hop ends at `m`, and its source is then the root.
                let reversed = match direction {
                    RelationshipDirection::Outgoing => RelationshipDirection::Incoming,
                    RelationshipDirection::Incoming => RelationshipDirection::Outgoing,
                    RelationshipDirection::Undirected => RelationshipDirection::Undirected,
                };
                (root_label, root_properties, reversed)
            };
            let scan = LogicalOperator::ScanByLabel {
                variable: middle.to_string(),
                label: middle_label.clone(),
                properties: middle_properties.clone(),
            };
            let expand = LogicalOperator::Expand {
                input: Box::new(filtered(scan, middle_filter.clone())),
                source_variable: middle.to_string(),
                target_variable: hop.other(middle).to_string(),
                target_label: other_label.clone(),
                relationship_types: relationship_types.clone(),
                direction,
                relationship_variable: bound(relationship_variable.as_deref()),
                properties: properties.clone(),
                target_properties: other_properties.clone(),
            };
            filtered(expand, predicates)
        };
        // Built in the original order, so each hop takes the same relationship instance.
        let first_plan = self.build_operator(ctx, &from_middle(&first, first_filter))?;
        let second_plan = self.build_operator(ctx, &from_middle(&second, second_filter))?;

        let column = |name: &str| col(Column::new_unqualified(name));
        let per_middle = |plan: LogicalPlan, other: &str, n: usize| -> Result<LogicalPlan> {
            let (paths, loops) = (format!("__n{n}"), format!("__s{n}"));
            let mut aggregates = vec![count(lit(1)).alias(&paths)];
            if self_loops {
                let is_loop = column(other).eq(column(&middle_id));
                aggregates
                    .push(sum(when(is_loop, lit(1_i64)).otherwise(lit(0_i64))?).alias(&loops));
            }
            let aggregate = LogicalPlanBuilder::from(plan)
                .aggregate(
                    vec![column(&middle_id).alias(format!("__m{n}"))],
                    aggregates,
                )?
                .build()
                .map_err(|e| self.plan_error("Failed to group a hop by its middle node", e))?;
            if self.adjacency.is_none() {
                return Ok(aggregate);
            }
            // On the CSR, the optimizer may count each `m`'s neighbours in place instead.
            let hop = HopDegreesNode::new(aggregate, &middle_id, &paths, &loops);
            Ok(LogicalPlan::Extension(Extension {
                node: std::sync::Arc::new(hop),
            }))
        };
        let first_plan = per_middle(first_plan, &first_other, 1)?;
        let second_plan = per_middle(second_plan, &second_other, 2)?;

        let mut paths = column("__n1") * column("__n2");
        if self_loops {
            paths = paths - column("__s1") * column("__s2");
        }
        let output = item.alias.as_ref().map_or_else(
            || super::super::expression::to_cypher_column_name(&item.expression),
            |alias| alias.to_lowercase(),
        );
        let plan = LogicalPlanBuilder::from(first_plan)
            .join(
                second_plan,
                JoinType::Inner,
                (vec!["__m1"], vec!["__m2"]),
                None,
            )?
            .aggregate(Vec::<Expr>::new(), vec![sum(paths).alias("__paths")])?
            .project(vec![
                coalesce(vec![column("__paths"), lit(0_i64)]).alias(output)
            ])?
            .build()
            .map_err(|e| self.plan_error("Failed to build two-hop count", e))?;
        Ok(Some(plan))
    }
}

impl DataFusionPlanner {
    /// `RETURN m.name, count(x)` (or `count(*)`) over `(x)-[r]->(m)...`, where nothing else
    /// reads `x` or `r`: on the CSR, count each `m`'s neighbours `x` once (`n(m)`, in place,
    /// see [`HopDegreesNode`]), plan the rest of the pattern from `m`, and sum `n(m)` over
    /// its rows. The hop's edges are read once to count them, rather than carried, one row
    /// each, through the rest of the pattern and into the aggregate (eager aggregation,
    /// as in [`Self::try_build_two_hop_count`]). `None` (plan as usual) for any other query.
    ///
    /// Applies when the hop's mean fan-out at `m` is at least two, every aggregate is a
    /// plain count of rows, and the group keys are properties or variables other than
    /// `x` and `r`. `WHERE` conjuncts over `x` alone filter the counted neighbours; any
    /// other conjunct that reads `x` or `r` keeps the usual plan.
    pub(crate) fn try_build_root_hop_count(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalOperator,
        projections: &[ProjectionItem],
    ) -> Result<Option<LogicalPlan>> {
        if self.adjacency.is_none() {
            return Ok(None);
        }
        let mut predicates = Vec::new();
        let mut op = input;
        while let LogicalOperator::Filter { input, predicate } = op {
            conjuncts(predicate, &mut predicates);
            op = input;
        }
        // The pattern's hops, last first, down to the one from the root.
        let mut hops = vec![op];
        while let LogicalOperator::Expand { input, .. } = op {
            if matches!(input.as_ref(), LogicalOperator::ScanByLabel { .. }) {
                break;
            }
            op = input;
            hops.push(op);
        }
        let Some((first, root_op)) = Hop::of(op) else {
            return Ok(None);
        };
        let LogicalOperator::ScanByLabel {
            variable: root,
            label: root_label,
            properties: root_properties,
        } = root_op
        else {
            return Ok(None);
        };
        let (x, m) = (root.as_str(), first.target);
        let counted = |v: &str| v == x || v == "*";
        let hidden: HashSet<&str> = [x].into_iter().chain(first.relationship).collect();
        // `x` and `r` appear in no later hop.
        for hop in &hops[..hops.len() - 1] {
            let LogicalOperator::Expand {
                source_variable,
                target_variable,
                relationship_variable,
                ..
            } = hop
            else {
                return Ok(None);
            };
            let names = [
                Some(source_variable),
                Some(target_variable),
                relationship_variable.as_ref(),
            ];
            if names
                .into_iter()
                .flatten()
                .any(|n| hidden.contains(n.as_str()))
            {
                return Ok(None);
            }
        }
        let mut counts = false;
        for p in projections {
            match &p.expression {
                ValueExpression::AggregateFunction {
                    name,
                    args,
                    distinct: false,
                } if name.eq_ignore_ascii_case("count") => match args.as_slice() {
                    [ValueExpression::Variable(v)] if counted(v) => counts = true,
                    // `count(x)` of a node arrives as `count(x.id)`, never null.
                    [ValueExpression::Property(p)]
                        if p.variable == x
                            && self.id_column(ctx, x) == Some(qualify_column(x, &p.property)) =>
                    {
                        counts = true
                    }
                    _ => return Ok(None),
                },
                ValueExpression::Property(p) if !hidden.contains(p.variable.as_str()) => {}
                ValueExpression::Variable(v) if !hidden.contains(v.as_str()) => {}
                _ => return Ok(None),
            }
        }
        if !counts {
            return Ok(None);
        }
        let fan_out = self.fan_out.as_ref().and_then(|f| first.fan_out(f, m));
        if !fan_out.is_some_and(|d| d >= 2.0) {
            return Ok(None);
        }
        let Some(middle_id) = self.id_column(ctx, m) else {
            return Ok(None);
        };
        let (mut far, mut rest) = (vec![], vec![]);
        for predicate in predicates {
            let mut names = HashSet::new();
            if referenced(predicate, &mut names).is_none() {
                return Ok(None);
            }
            if names.iter().all(|n| n == x) {
                far.push(predicate.clone());
            } else if names.iter().any(|n| hidden.contains(n.as_str())) {
                return Ok(None);
            } else {
                rest.push(predicate.clone());
            }
        }

        let LogicalOperator::Expand {
            target_label,
            relationship_types,
            direction,
            properties,
            target_properties,
            ..
        } = first.op
        else {
            unreachable!("Hop::of matched an Expand")
        };
        let scan = LogicalOperator::ScanByLabel {
            variable: m.to_string(),
            label: target_label.clone(),
            properties: target_properties.clone(),
        };
        let reversed = match direction {
            RelationshipDirection::Outgoing => RelationshipDirection::Incoming,
            _ => RelationshipDirection::Outgoing,
        };
        let hop = LogicalOperator::Expand {
            input: Box::new(scan.clone()),
            source_variable: m.to_string(),
            target_variable: x.to_string(),
            target_label: root_label.clone(),
            relationship_types: relationship_types.clone(),
            direction: reversed,
            relationship_variable: None,
            properties: properties.clone(),
            target_properties: root_properties.clone(),
        };
        // The rest of the pattern, from `m` instead of the hop.
        let mut rest_op = scan;
        for hop in hops[..hops.len() - 1].iter().rev() {
            let mut hop = (*hop).clone();
            if let LogicalOperator::Expand { input, .. } = &mut hop {
                **input = rest_op;
            }
            rest_op = hop;
        }
        let filtered = |op: LogicalOperator, predicates: Vec<BooleanExpression>| match predicates
            .into_iter()
            .reduce(|a, b| BooleanExpression::And(Box::new(a), Box::new(b)))
        {
            Some(predicate) => LogicalOperator::Filter {
                input: Box::new(op),
                predicate,
            },
            None => op,
        };
        // Built in the original order, so each hop takes the same relationship instance.
        let hop_plan = self.build_operator(ctx, &filtered(hop, far))?;
        let rest_plan = self.build_operator(ctx, &filtered(rest_op, rest))?;

        let column = |name: &str| col(Column::new_unqualified(name));
        let degrees = LogicalPlanBuilder::from(hop_plan)
            .aggregate(
                vec![column(&middle_id).alias("__m0")],
                vec![count(lit(1)).alias("__n0")],
            )?
            .build()
            .map_err(|e| self.plan_error("Failed to count a hop per node", e))?;
        let degrees = LogicalPlan::Extension(Extension {
            node: std::sync::Arc::new(HopDegreesNode::new(degrees, &middle_id, "__n0", "__s0")),
        });
        let joined = LogicalPlanBuilder::from(rest_plan)
            .join(
                degrees,
                JoinType::Inner,
                (vec![middle_id.as_str()], vec!["__m0"]),
                None,
            )?
            .build()
            .map_err(|e| self.plan_error("Failed to join a hop's counts", e))?;
        // Each count of rows becomes the sum of the counted neighbours, 0 over no rows.
        let summed = ValueExpression::ScalarFunction {
            name: "coalesce".to_string(),
            args: vec![
                ValueExpression::AggregateFunction {
                    name: "sum".to_string(),
                    args: vec![ValueExpression::Variable("__n0".to_string())],
                    distinct: false,
                },
                ValueExpression::Literal(crate::ast::PropertyValue::Integer(0)),
            ],
        };
        let projections: Vec<ProjectionItem> = projections
            .iter()
            .map(|p| match &p.expression {
                ValueExpression::AggregateFunction { .. } => ProjectionItem {
                    expression: summed.clone(),
                    alias: Some(p.alias.clone().unwrap_or_else(|| {
                        super::super::expression::to_cypher_column_name(&p.expression)
                    })),
                },
                _ => p.clone(),
            })
            .collect();
        self.build_project_with_aggregates(ctx, joined, &projections, &[])
            .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use crate::{CypherEngine, EngineSource, GraphConfig};

    fn batch(columns: &[(&str, Vec<i64>)]) -> RecordBatch {
        let fields = columns
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>();
        let arrays = columns
            .iter()
            .map(|(_, values)| Arc::new(Int64Array::from(values.clone())) as _)
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    fn edges(pairs: &[(i64, i64)]) -> RecordBatch {
        batch(&[
            ("src", pairs.iter().map(|p| p.0).collect()),
            ("dst", pairs.iter().map(|p| p.1).collect()),
        ])
    }

    /// An engine over Persons 1–4 (ages 10, 30, 60, 40) and the given edges. Each type
    /// also gets a dense block of 50 edges between other nodes, which adds no two-hop
    /// path but gives the mean fan-out at which the rewrite pays off; `LIVES_IN` has one
    /// edge per person, so a hop over it never does.
    async fn engine(follows: &[(i64, i64)], likes: &[(i64, i64)], use_csr: bool) -> CypherEngine {
        let config = GraphConfig::builder()
            .node_table("Person", "id")
            .rel_table("FOLLOWS", "Person", "Person", "src", "dst")
            .rel_table("LIKES", "Person", "Person", "src", "dst")
            .rel_table("LIVES_IN", "Person", "Person", "src", "dst")
            .build()
            .unwrap();
        let mut ids = vec![1, 2, 3, 4];
        let mut ages = vec![10, 30, 60, 40];
        ids.extend(10..50);
        ages.extend(10..50);
        let dense = |from: i64, to: i64| (0..50).map(move |i| (from + i % 5, to + i / 10));
        let follows: Vec<_> = follows.iter().copied().chain(dense(10, 20)).collect();
        let likes: Vec<_> = likes.iter().copied().chain(dense(30, 40)).collect();
        let lives_in: Vec<_> = (1..50).map(|i| (i, 1)).collect();
        let sources = HashMap::from([
            (
                "Person".to_string(),
                EngineSource::Arrow(batch(&[("id", ids), ("age", ages)])),
            ),
            ("FOLLOWS".to_string(), EngineSource::Arrow(edges(&follows))),
            ("LIKES".to_string(), EngineSource::Arrow(edges(&likes))),
            (
                "LIVES_IN".to_string(),
                EngineSource::Arrow(edges(&lives_in)),
            ),
        ]);
        CypherEngine::new(config, sources, use_csr).await.unwrap()
    }

    /// The count from `query`, checking it uses the rewrite, equals the join plan's (forced
    /// by `+ 0`, which the rewrite does not match), and is the same with the CSR.
    async fn count(follows: &[(i64, i64)], likes: &[(i64, i64)], query: &str) -> i64 {
        let close = query.find("count(").unwrap()
            + query[query.find("count(").unwrap()..].find(')').unwrap()
            + 1;
        let join = format!("{} + 0{}", &query[..close], &query[close..]);
        let mut counts = Vec::new();
        for use_csr in [false, true] {
            let engine = engine(follows, likes, use_csr).await;
            for (q, rewritten) in [(query, true), (join.as_str(), false)] {
                let plan = engine.explain(q, HashMap::new()).await.unwrap();
                assert_eq!(plan.contains("__paths"), rewritten, "{plan}");
                let result = engine.execute(q, HashMap::new()).await.unwrap();
                let column = result.column(0).as_any().downcast_ref::<Int64Array>();
                counts.push(column.unwrap().value(0));
            }
        }
        assert!(counts.iter().all(|c| *c == counts[0]), "{counts:?}");
        counts[0]
    }

    const CHAIN: &str =
        "MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person) RETURN count(*) AS n";

    #[tokio::test]
    async fn self_loops_at_the_middle_node() {
        // in(2) × out(2) = 4 includes the self-loop paired with itself.
        assert_eq!(count(&[(1, 2), (2, 3), (2, 2)], &[], CHAIN).await, 3);
        // Endpoints are compared, so two parallel self-loops exclude all four of their pairs.
        assert_eq!(count(&[(2, 2), (2, 2)], &[], CHAIN).await, 0);
        assert_eq!(
            count(&[(1, 2), (2, 3), (2, 2), (2, 2)], &[], CHAIN).await,
            5
        );
        // Parallel edges between different nodes count like any others.
        assert_eq!(
            count(&[(1, 2), (1, 2), (2, 3), (2, 3)], &[], CHAIN).await,
            4
        );
        // No paths at all is 0, not null.
        assert_eq!(count(&[(1, 2)], &[], CHAIN).await, 0);
    }

    #[tokio::test]
    async fn filters_decide_which_self_loops_are_subtracted() {
        let follows = [(1, 2), (2, 3), (2, 2), (3, 3), (3, 4)];
        // The self-loop at 2 fails `c.age > 50` (2 is 30), so nothing is subtracted for it.
        let query = CHAIN.replace("RETURN", "WHERE c.age > 50 RETURN");
        assert_eq!(count(&follows, &[], &query).await, 3);
        // q9's filters, which re-root the pattern at b.
        let query = CHAIN.replace("RETURN", "WHERE b.age < 50 AND c.age > 25 RETURN");
        assert_eq!(count(&follows, &[], &query).await, 3);
        let query = CHAIN.replace("RETURN", "WHERE a.age < b.age AND r2.src <> 4 RETURN");
        assert_eq!(count(&follows, &[], &query).await, 4);
    }

    #[tokio::test]
    async fn only_relationships_a_filter_reads_fetch_edges() {
        let engine = engine(&[(1, 2), (2, 3)], &[], true).await;
        let plan = engine.explain(CHAIN, HashMap::new()).await.unwrap();
        assert!(
            plan.contains("__paths") && !plan.contains("edge="),
            "{plan}"
        );
        let query = CHAIN.replace("RETURN", "WHERE r2.src <> 4 RETURN");
        let plan = engine.explain(&query, HashMap::new()).await.unwrap();
        assert_eq!(plan.matches("edge=").count(), 2, "{plan}");
    }

    #[tokio::test]
    async fn csr_hops_count_degrees_in_place() {
        // Edges to and from 99, which has no Person row, match nothing on either path.
        let dangling = [(1, 2), (2, 3), (2, 99), (99, 2), (2, 2)];
        assert_eq!(count(&dangling, &[], CHAIN).await, 3);
        let engine = engine(&dangling, &[], true).await;
        for (filter, degrees) in [
            ("", 2),
            // Filters on the far ends and the middle node count in place.
            ("WHERE b.age < 50 AND c.age > 25 AND a.age > 5", 2),
            // A filter across a hop's ends, or on its edge, needs that hop's rows.
            ("WHERE a.age < b.age", 1),
            ("WHERE r2.src <> 4", 1),
        ] {
            let query = CHAIN.replace("RETURN", &format!("{filter} RETURN"));
            let plan = engine.explain(&query, HashMap::new()).await.unwrap();
            assert_eq!(plan.matches("CsrExpandExec").count(), 2, "{plan}");
            // Lowered in the optimizer: only the physical plan counts degrees.
            assert_eq!(plan.matches("degrees=").count(), degrees, "{query}: {plan}");
        }
        // A far-end filter with a parameter, bound into a cached plan after planning.
        let literal = CHAIN.replace("RETURN", "WHERE c.age > 25 RETURN");
        assert_eq!(count(&dangling, &[], &literal).await, 3);
        let query = CHAIN.replace("RETURN", "WHERE c.age > $age RETURN");
        let params = HashMap::from([("age".to_string(), serde_json::Value::from(25))]);
        for _ in 0..2 {
            let result = engine.execute(&query, params.clone()).await.unwrap();
            let column = result.column(0).as_any().downcast_ref::<Int64Array>();
            assert_eq!(column.unwrap().value(0), 3);
        }
    }

    /// Rows of `query` as sorted strings, equal on the join path and the CSR; on the CSR,
    /// whether the plan counts a hop in place matches `in_place`.
    async fn grouped(follows: &[(i64, i64)], query: &str, in_place: bool) -> Vec<String> {
        let mut answers = Vec::new();
        for use_csr in [false, true] {
            let engine = engine(follows, &[], use_csr).await;
            let plan = engine.explain(query, HashMap::new()).await.unwrap();
            assert_eq!(
                plan.contains("degrees="),
                use_csr && in_place,
                "{query}: {plan}"
            );
            let result = engine.execute(query, HashMap::new()).await.unwrap();
            let mut rows: Vec<String> = (0..result.num_rows())
                .map(|i| {
                    let values = result
                        .columns()
                        .iter()
                        .map(|c| arrow::util::display::array_value_to_string(c, i).unwrap());
                    values.collect::<Vec<_>>().join(",")
                })
                .collect();
            rows.sort();
            answers.push(rows);
        }
        assert_eq!(answers[0], answers[1], "{query}");
        answers.pop().unwrap()
    }

    #[tokio::test]
    async fn root_hop_counted_per_node() {
        // 1, 2 and 3 follow 2; 2 follows 3 and itself; 99 has no Person row.
        let follows = [(1, 2), (3, 2), (2, 2), (99, 2), (2, 3), (2, 99)];
        let one = "MATCH (a:Person)-[:FOLLOWS]->(b:Person) ";
        let query = format!("{one}RETURN b.id AS b, count(a.id) AS n");
        assert!(grouped(&follows, &query, true)
            .await
            .contains(&"2,3".to_string()));
        let query = format!("{one}WHERE a.age > 15 AND b.age < 50 RETURN b.age, count(*)");
        assert!(grouped(&follows, &query, true)
            .await
            .contains(&"30,2".to_string()));
        // Nothing left to count is 0, not null.
        let query = format!("{one}WHERE b.age > 1000 RETURN count(*) AS n");
        assert_eq!(grouped(&follows, &query, true).await, ["0"]);
        // The rest of the pattern continues from b, with b's count on each of its rows.
        let two = "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:LIVES_IN]->(c:Person) ";
        let query = format!("{two}RETURN c.id AS c, b.age AS age, count(a) AS n");
        assert!(grouped(&follows, &query, true)
            .await
            .contains(&"1,30,3".to_string()));
        // Anything else that reads a, or a different aggregate, keeps the usual plan.
        for query in [
            format!("{one}RETURN a.age, count(*)"),
            format!("{one}RETURN b.id, count(a.age)"),
            format!("{one}RETURN b.id, count(DISTINCT a)"),
            format!("{one}RETURN b.id, sum(b.age)"),
            format!("{one}WHERE a.age < b.age RETURN b.id, count(*)"),
            format!("{two}WHERE c.id <> a.id RETURN c.id, count(*)"),
        ] {
            grouped(&follows, &query, false).await;
        }
    }

    #[tokio::test]
    async fn other_two_hop_shapes() {
        // Different types never bind the same edge: the self-loops pair up.
        let mixed = "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:LIKES]->(c:Person) RETURN count(*)";
        assert_eq!(count(&[(2, 2)], &[(2, 2), (2, 3)], mixed).await, 2);
        let shared = "MATCH (a:Person)-[:FOLLOWS]->(b:Person)<-[:LIKES]-(c:Person) RETURN count(b)";
        assert_eq!(count(&[(1, 2), (3, 2)], &[(1, 2)], shared).await, 2);
        let reversed =
            "MATCH (c:Person)<-[:FOLLOWS]-(b:Person)<-[:FOLLOWS]-(a:Person) RETURN count(*)";
        assert_eq!(count(&[(1, 2), (2, 3), (2, 2)], &[], reversed).await, 3);
    }

    #[tokio::test]
    async fn other_queries_keep_the_join_plan() {
        let engine = engine(&[(1, 2), (2, 3)], &[], false).await;
        for query in [
            // A filter across the two hops.
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(c:Person) \
             WHERE a.age < c.age RETURN count(*)",
            // Same-type hops sharing an end: the correction needs both ends.
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person)<-[:FOLLOWS]-(c:Person) RETURN count(*)",
            "MATCH (a:Person)-[:FOLLOWS]-(b:Person)-[:FOLLOWS]->(c:Person) RETURN count(*)",
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(a) RETURN count(*)",
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(c:Person) \
             RETURN count(DISTINCT c)",
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(c:Person) \
             RETURN b.id, count(*)",
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(c:Person) \
             RETURN count(c.age)",
            "MATCH (a:Person)-[:FOLLOWS*2]->(c:Person) RETURN count(*)",
            // One edge per person on one side: no more paths than edges.
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:LIVES_IN]->(c:Person) RETURN count(*)",
        ] {
            let plan = engine.explain(query, HashMap::new()).await.unwrap();
            assert!(!plan.contains("__paths"), "{query}: {plan}");
        }
    }
}
