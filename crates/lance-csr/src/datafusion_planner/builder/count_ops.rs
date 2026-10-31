// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! `count(*)` over a two-hop pattern, computed from per-node degrees.
//!
//! The paths through a middle node `m` number `in(m) × out(m)`, so the count is
//! `Σ_m n1(m) · n2(m)`, where `n1` and `n2` count each hop's edges at `m` that pass that
//! hop's filters (eager aggregation, Yan and Larson). Each hop is planned as a one-hop
//! pattern of its own and grouped down to one row per `m` before the join, so the work
//! follows the edges, not the paths.

use std::collections::HashSet;

use crate::ast::{BooleanExpression, ComparisonOperator, RelationshipDirection, ValueExpression};
use crate::case_insensitive::qualify_column;
use crate::datafusion_planner::analysis::PlanningContext;
use crate::datafusion_planner::DataFusionPlanner;
use crate::error::Result;
use crate::logical_plan::*;
use datafusion::common::Column;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::{count, sum};
use datafusion::logical_expr::{col, lit, when, Expr, JoinType, LogicalPlan, LogicalPlanBuilder};

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
    fn id_column(&self, ctx: &PlanningContext, variable: &str) -> Option<String> {
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
        let (mut first_filter, mut second_filter, mut self_loops) = (vec![], vec![], false);
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
            if names.iter().all(|n| first_vars.contains(n.as_str())) {
                first_filter.push(predicate.clone());
            } else if names.iter().all(|n| second_vars.contains(n.as_str())) {
                second_filter.push(predicate.clone());
            } else {
                return Ok(None);
            }
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
        let LogicalOperator::Expand {
            target_variable,
            target_label,
            relationship_types,
            direction,
            relationship_variable,
            properties,
            target_properties,
            ..
        } = second.op
        else {
            unreachable!("Hop::of matched an Expand")
        };
        let second_op = LogicalOperator::Expand {
            input: Box::new(LogicalOperator::ScanByLabel {
                variable: middle.to_string(),
                label: middle_label.clone(),
                properties: middle_properties.clone(),
            }),
            source_variable: middle.to_string(),
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            relationship_types: relationship_types.clone(),
            direction: direction.clone(),
            relationship_variable: relationship_variable.clone(),
            properties: properties.clone(),
            target_properties: target_properties.clone(),
        };
        // Built in the original order, so each hop takes the same relationship instance.
        let first_plan = self.build_operator(ctx, &filtered(first.op.clone(), first_filter))?;
        let second_plan = self.build_operator(ctx, &filtered(second_op, second_filter))?;

        let column = |name: &str| col(Column::new_unqualified(name));
        let per_middle = |plan: LogicalPlan, other: &str, n: usize| -> Result<LogicalPlan> {
            let mut aggregates = vec![count(lit(1)).alias(format!("__n{n}"))];
            if self_loops {
                let is_loop = column(other).eq(column(&middle_id));
                aggregates.push(
                    sum(when(is_loop, lit(1_i64)).otherwise(lit(0_i64))?).alias(format!("__s{n}")),
                );
            }
            LogicalPlanBuilder::from(plan)
                .aggregate(
                    vec![column(&middle_id).alias(format!("__m{n}"))],
                    aggregates,
                )?
                .build()
                .map_err(|e| self.plan_error("Failed to group a hop by its middle node", e))
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
