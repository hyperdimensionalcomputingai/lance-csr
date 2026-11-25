// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! Relational fallback for a variable-length segment. The stored endpoints and an occurrence
//! number among parallel edges distinguish a hop from earlier hops of the same type.

use crate::ast::{LengthRange, PropertyValue, RelationshipDirection};
use crate::case_insensitive::qualify_column;
use crate::datafusion_planner::analysis::{PlanningContext, RelationshipInstance};
use crate::datafusion_planner::join_ops::{SourceJoinParams, TargetJoinParams};
use crate::datafusion_planner::scan_ops::TrailIdentity;
use crate::datafusion_planner::DataFusionPlanner;
use crate::error::{GraphError, Result};
use datafusion::common::Column;
use datafusion::logical_expr::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder};
use std::collections::HashMap;

/// One segment of a pattern planned as a single trail: its relationships, hop range and
/// the boundary node at its end.
pub(super) struct ChainSegment<'a> {
    pub types: &'a [String],
    pub direction: &'a RelationshipDirection,
    pub range: [u32; 2],
    pub properties: &'a HashMap<String, PropertyValue>,
    pub end: &'a str,
    pub end_label: &'a str,
}

/// Unrolled branches (one per combination of segment lengths) above which a pattern of
/// several segments is left to the edge inventory instead of the relational chain.
const MAX_CHAIN_BRANCHES: usize = 64;

#[derive(Clone)]
struct TrailStep {
    kind: String,
    to: String,
    direction: RelationshipDirection,
}

/// The columns that say which edge one hop of a path walked: the endpoints as walked, and
/// the edge's number among its parallel edges.
struct HopEdge {
    source: Expr,
    target: Expr,
    rank: Expr,
}

impl HopEdge {
    fn new(alias: &str, rel_map: &crate::config::RelationshipMapping) -> Self {
        Self {
            source: col(qualify_column(alias, &rel_map.source_id_field)),
            target: col(qualify_column(alias, &rel_map.target_id_field)),
            rank: col(Column::new_unqualified(Self::rank_column(alias))),
        }
    }

    fn rank_column(alias: &str) -> String {
        format!("#trail rank {alias}")
    }

    /// Whether both hops walked the same edge: the same endpoints and the same number. An
    /// undirected hop may have walked an edge from either end. Which parallel edge holds a
    /// number differs between hops, but each number is held once, so a path has exactly as
    /// many rows as it has choices of distinct edges.
    fn is_same_edge(&self, other: &Self, undirected: bool) -> Expr {
        let same = self
            .source
            .clone()
            .eq(other.source.clone())
            .and(self.target.clone().eq(other.target.clone()));
        let ends = if undirected {
            same.or(self
                .source
                .clone()
                .eq(other.target.clone())
                .and(self.target.clone().eq(other.source.clone())))
        } else {
            same
        };
        ends.and(self.rank.clone().eq(other.rank.clone()))
    }
}

impl DataFusionPlanner {
    fn missing_table(&self, name: &str) -> GraphError {
        GraphError::ConfigError {
            message: format!("No mapping or table source found for '{name}'"),
            location: snafu::Location::new(file!(), line!(), column!()),
        }
    }

    /// All nonempty sequences compatible with the labels at both ends of this segment.
    fn trail_sequences(
        &self,
        types: &[String],
        direction: &RelationshipDirection,
        [start, end]: [&str; 2],
        [min, max]: [u32; 2],
    ) -> Result<Vec<Vec<TrailStep>>> {
        let [kind] = types else {
            let range = LengthRange {
                min: Some(min),
                max: Some(max),
            };
            let sequences = crate::pattern_alternatives::alternation_sequences(
                types,
                direction,
                &range,
                &[start.to_string()],
                &[end.to_string()],
                &self.config,
            )?;
            return Ok(sequences
                .into_iter()
                .map(|seq| {
                    seq.into_iter()
                        .map(|(kind, to)| TrailStep {
                            kind,
                            to,
                            direction: direction.clone(),
                        })
                        .collect()
                })
                .collect());
        };
        let map = self.get_relationship_mapping(kind)?;
        let ends = map.source_label.as_deref().zip(map.target_label.as_deref());
        let mut sequences = Vec::new();
        let mut steps = Vec::new();
        let mut at = start;
        for depth in 1..=max {
            use RelationshipDirection::{Incoming, Outgoing};
            let (to, walk) = match ends {
                Some((a, b)) if a.eq_ignore_ascii_case(b) && at.eq_ignore_ascii_case(a) => {
                    (b, direction)
                }
                Some((a, b)) if at.eq_ignore_ascii_case(a) && *direction != Incoming => {
                    (b, &Outgoing)
                }
                Some((a, b)) if at.eq_ignore_ascii_case(b) && *direction != Outgoing => {
                    (a, &Incoming)
                }
                Some(_) => break,
                // With no endpoint mapping intermediate nodes use the source table.
                None => (start, direction),
            };
            steps.push(TrailStep {
                kind: kind.clone(),
                to: to.to_string(),
                direction: walk.clone(),
            });
            at = to;
            if depth >= min && (ends.is_none() || at.eq_ignore_ascii_case(end)) {
                let mut branch = steps.clone();
                branch.last_mut().expect("positive depth").to = end.to_string();
                sequences.push(branch);
            }
        }
        Ok(sequences)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_trail_paths(
        &self,
        ctx: &mut PlanningContext,
        input: &LogicalPlan,
        [source, target]: [&str; 2],
        types: &[String],
        direction: &RelationshipDirection,
        range: [u32; 2],
        properties: &HashMap<String, PropertyValue>,
        target_properties: &HashMap<String, PropertyValue>,
    ) -> Result<Vec<LogicalPlan>> {
        let (start, _) = self.get_target_node_mapping(ctx, source)?;
        let (end, _) = self.get_target_node_mapping(ctx, target)?;
        let first = types
            .first()
            .ok_or_else(|| GraphError::internal("a variable-length relationship type"))?;
        let instance = ctx.next_relationship_instance(first)?;
        let sequences = self.trail_sequences(types, direction, [&start, &end], range)?;
        let mut plans = Vec::new();
        if range[0] == 0 || sequences.is_empty() {
            let identity = range[0] == 0 && start.eq_ignore_ascii_case(&end);
            plans.push(self.trail_endpoints(
                ctx,
                input.clone(),
                [source, target],
                target_properties,
                identity,
            )?);
        }
        // All internal columns are projected away before the union. A caller can use
        // arbitrary quoted names, so check both incoming columns and the new target.
        let mut n = instance.id;
        let prefix = loop {
            let name = format!("_trail{n}_");
            if !target.to_lowercase().starts_with(&name)
                && !input.schema().fields().iter().any(|f| {
                    f.name().starts_with(&name)
                        || f.name().starts_with(&HopEdge::rank_column(&name))
                })
            {
                break name;
            }
            n += 1;
        };
        let no_properties = HashMap::new();
        for seq in sequences {
            let mut plan = input.clone();
            let mut previous = source.to_string();
            let mut from = start.as_str();
            let mut walked = HashMap::<&str, Vec<HopEdge>>::new();
            for (i, step) in seq.iter().enumerate() {
                let last = i + 1 == seq.len();
                let next = if last {
                    target.to_string()
                } else {
                    format!("{prefix}n{i}")
                };
                let repeated = seq.iter().filter(|s| s.kind == step.kind).nth(1).is_some();
                let history = repeated.then(|| walked.entry(&step.kind).or_default());
                let hop = RelationshipInstance {
                    id: i + 1,
                    rel_type: step.kind.clone(),
                    source_var: previous.clone(),
                    target_var: next.clone(),
                    direction: step.direction.clone(),
                    alias: format!("{prefix}r{i}"),
                };
                plan = self.build_trail_hop(
                    plan,
                    [&previous, &next],
                    [from, &step.to],
                    &hop,
                    properties,
                    if last {
                        target_properties
                    } else {
                        &no_properties
                    },
                    history,
                )?;
                previous = next;
                from = &step.to;
            }
            plans.push(plan);
        }
        Ok(plans)
    }

    /// Enumerate typed, oriented hop sequences from this boundary. Bound both the
    /// frontier and the completed alternatives before allocating a wider join plan.
    /// Heterogeneous undirected types can change orientation at every hop.
    fn chain_sequences(
        &self,
        segment: &ChainSegment,
        start: &str,
        budget: usize,
    ) -> Result<Option<Vec<Vec<TrailStep>>>> {
        use RelationshipDirection::{Incoming, Outgoing, Undirected};

        let mut mappings = Vec::new();
        for kind in segment.types {
            let mapping = self.get_relationship_mapping(kind)?;
            if !mappings
                .iter()
                .any(|(name, _, _)| *name == &mapping.relationship_type)
            {
                let Some((source, target)) = mapping
                    .source_label
                    .as_deref()
                    .zip(mapping.target_label.as_deref())
                else {
                    return Ok(None);
                };
                mappings.push((&mapping.relationship_type, source, target));
            }
        }
        let [min, max] = segment.range;
        let mut sequences = Vec::new();
        let mut frontier: Vec<Vec<TrailStep>> = vec![Vec::new()];
        for depth in 0..=max {
            let mut next = Vec::new();
            for prefix in frontier {
                let at = prefix.last().map_or(start, |s| s.to.as_str());
                if depth >= min && at.eq_ignore_ascii_case(segment.end_label) {
                    if sequences.len() == budget {
                        return Ok(None);
                    }
                    sequences.push(prefix.clone());
                }
                if depth == max {
                    continue;
                }
                for &(kind, source, target) in &mappings {
                    let forward = at.eq_ignore_ascii_case(source) && *segment.direction != Incoming;
                    let reverse = at.eq_ignore_ascii_case(target) && *segment.direction != Outgoing;
                    let (to, direction) = match (forward, reverse) {
                        (true, true) => (target, Undirected),
                        (true, false) => (target, Outgoing),
                        (false, true) => (source, Incoming),
                        (false, false) => continue,
                    };
                    if next.len() == budget {
                        return Ok(None);
                    }
                    let mut steps = prefix.clone();
                    steps.push(TrailStep {
                        kind: kind.clone(),
                        to: to.to_owned(),
                        direction,
                    });
                    next.push(steps);
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        Ok(Some(sequences))
    }

    /// A pattern of several segments that share relationship types, as one trail of joins.
    /// Each combination of segment lengths is one branch of hops, and every hop of a type
    /// the branch walks more than once is kept off the edges earlier hops of that type
    /// walked, across segment boundaries too. The branches are projected to `input`'s
    /// columns and the boundary nodes' columns, and unioned. `None` when the branches
    /// would exceed [`MAX_CHAIN_BRANCHES`] or some segment can never match.
    pub(super) fn build_trail_chain(
        &self,
        input: &LogicalPlan,
        [start, start_label]: [&str; 2],
        segments: &[ChainSegment],
    ) -> Result<Option<LogicalPlan>> {
        let mut options: Vec<Vec<Vec<TrailStep>>> = Vec::with_capacity(segments.len());
        let mut from = start_label;
        let mut branches = 1_usize;
        for segment in segments {
            let Some(sequences) =
                self.chain_sequences(segment, from, MAX_CHAIN_BRANCHES / branches)?
            else {
                return Ok(None);
            };
            if sequences.is_empty() {
                return Ok(None);
            }
            branches *= sequences.len();
            options.push(sequences);
            from = segment.end_label;
        }
        // Internal columns are projected away before the union, but must not collide
        // with incoming columns or boundary variables meanwhile.
        let names: Vec<String> = segments.iter().map(|s| s.end.to_lowercase()).collect();
        let prefix = (0..)
            .map(|n| format!("_trail{n}_chain_"))
            .find(|name| {
                !names.iter().any(|v| v.starts_with(name.as_str()))
                    && !input.schema().fields().iter().any(|f| {
                        f.name().starts_with(name.as_str())
                            || f.name().starts_with(&HopEdge::rank_column(name))
                    })
            })
            .expect("an unused prefix");
        let projection = self.chain_projection(input, start, segments)?;
        let no_properties = HashMap::new();
        let mut choice = vec![0_usize; segments.len()];
        let mut plans = Vec::with_capacity(branches);
        loop {
            let seqs: Vec<&Vec<TrailStep>> = options
                .iter()
                .zip(&choice)
                .map(|(sequences, &i)| &sequences[i])
                .collect();
            let mut counts = HashMap::<String, usize>::new();
            for step in seqs.iter().flat_map(|seq| seq.iter()) {
                *counts.entry(step.kind.to_lowercase()).or_default() += 1;
            }
            let mut plan = input.clone();
            let mut previous = start.to_string();
            let mut from = start_label;
            let mut walked = HashMap::<String, Vec<HopEdge>>::new();
            let mut hop = 0_usize;
            for (segment, seq) in segments.iter().zip(&seqs) {
                if seq.is_empty() {
                    plan =
                        self.trail_identity(plan, [&previous, segment.end], segment.end_label)?;
                }
                for (i, step) in seq.iter().enumerate() {
                    let next = if i + 1 == seq.len() {
                        segment.end.to_string()
                    } else {
                        format!("{prefix}n{hop}")
                    };
                    let kind = step.kind.to_lowercase();
                    let repeated = counts.get(&kind).is_some_and(|&n| n > 1);
                    let history = repeated.then(|| walked.entry(kind).or_default());
                    let instance = RelationshipInstance {
                        id: hop + 1,
                        rel_type: step.kind.clone(),
                        source_var: previous.clone(),
                        target_var: next.clone(),
                        direction: step.direction.clone(),
                        alias: format!("{prefix}r{hop}"),
                    };
                    plan = self.build_trail_hop(
                        plan,
                        [&previous, &next],
                        [from, &step.to],
                        &instance,
                        segment.properties,
                        &no_properties,
                        history,
                    )?;
                    previous = next;
                    from = &step.to;
                    hop += 1;
                }
                previous = segment.end.to_string();
                from = segment.end_label;
            }
            plans.push(
                LogicalPlanBuilder::from(plan)
                    .project(projection.clone())
                    .and_then(|b| b.build())
                    .map_err(|e| self.plan_error("Failed to project a trail branch", e))?,
            );
            // The next combination of segment lengths, last segment fastest.
            let Some(s) = (0..choice.len())
                .rev()
                .find(|&s| choice[s] + 1 < options[s].len())
            else {
                break;
            };
            choice[s] += 1;
            choice[s + 1..].iter_mut().for_each(|c| *c = 0);
        }
        let mut plans = plans.into_iter();
        let mut union = plans
            .next()
            .ok_or_else(|| GraphError::internal("at least one trail branch"))?;
        for plan in plans {
            union = LogicalPlanBuilder::from(union)
                .union(plan)
                .and_then(|b| b.build())
                .map_err(|e| self.plan_error("Failed to union trail branches", e))?;
        }
        Ok(Some(union))
    }

    /// `input`'s columns, then the node columns of each boundary not already bound.
    fn chain_projection(
        &self,
        input: &LogicalPlan,
        start: &str,
        segments: &[ChainSegment],
    ) -> Result<Vec<Expr>> {
        let mut projection: Vec<Expr> = input
            .schema()
            .iter()
            .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field.as_ref()))))
            .collect();
        let mut seen = vec![start.to_lowercase()];
        for segment in segments {
            let end = segment.end.to_lowercase();
            if seen.contains(&end) {
                continue;
            }
            seen.push(end);
            let map = self
                .config
                .get_node_mapping(segment.end_label)
                .ok_or_else(|| self.missing_table(segment.end_label))?;
            if input
                .schema()
                .field_with_unqualified_name(&qualify_column(segment.end, &map.id_field))
                .is_ok()
            {
                continue;
            }
            let source = self
                .get_catalog()?
                .node_source(segment.end_label)
                .ok_or_else(|| self.missing_table(segment.end_label))?;
            projection.extend(source.schema().fields().iter().map(|f| {
                Expr::Column(Column::new_unqualified(qualify_column(
                    segment.end,
                    f.name(),
                )))
            }));
        }
        Ok(projection)
    }

    /// A zero-length segment: its end is the node it starts from.
    fn trail_identity(
        &self,
        plan: LogicalPlan,
        [previous, end]: [&str; 2],
        label: &str,
    ) -> Result<LogicalPlan> {
        if previous.eq_ignore_ascii_case(end) {
            return Ok(plan);
        }
        let map = self
            .config
            .get_node_mapping(label)
            .ok_or_else(|| self.missing_table(label))?;
        let (from, to) = (
            qualify_column(previous, &map.id_field),
            qualify_column(end, &map.id_field),
        );
        let builder = LogicalPlanBuilder::from(plan);
        let builder = if builder.schema().field_with_unqualified_name(&to).is_ok() {
            builder.filter(col(Column::new_unqualified(from)).eq(col(Column::new_unqualified(to))))
        } else {
            let source = self
                .get_catalog()?
                .node_source(label)
                .ok_or_else(|| self.missing_table(label))?;
            let scan = self.build_target_scan(source, label, end, &HashMap::new())?;
            builder.join(
                scan,
                datafusion::logical_expr::JoinType::Inner,
                (vec![from], vec![to]),
                None,
            )
        };
        builder
            .and_then(|b| b.build())
            .map_err(|e| self.plan_error("Failed to bind a zero-length segment", e))
    }

    /// Endpoint schema for a zero-hop identity or for a label combination that cannot match.
    fn trail_endpoints(
        &self,
        ctx: &PlanningContext,
        input: LogicalPlan,
        [source, target]: [&str; 2],
        props: &HashMap<String, PropertyValue>,
        identity: bool,
    ) -> Result<LogicalPlan> {
        let (_, source_map) = self.get_target_node_mapping(ctx, source)?;
        let (end, target_map) = self.get_target_node_mapping(ctx, target)?;
        let src = col(qualify_column(source, &source_map.id_field));
        let dst = qualify_column(target, &target_map.id_field);
        let mut builder = LogicalPlanBuilder::from(input);
        if builder.schema().field_with_unqualified_name(&dst).is_ok() {
            if identity && !props.is_empty() {
                return Err(GraphError::PlanError {
                    message: format!(
                        "Property filters on reused variable '{target}' are not yet supported"
                    ),
                    location: snafu::Location::new(file!(), line!(), column!()),
                });
            }
            builder = builder
                .filter(if identity {
                    src.eq(col(&dst))
                } else {
                    lit(false)
                })
                .map_err(|e| self.plan_error("Failed to filter path endpoints", e))?;
        } else {
            let source = self
                .get_catalog()?
                .node_source(&end)
                .ok_or_else(|| self.missing_table(&end))?;
            let scan = self.build_target_scan(source, &end, target, props)?;
            let predicate = if identity {
                src.eq(col(&dst))
            } else {
                lit(false)
            };
            builder = builder
                .join_on(scan, datafusion::logical_expr::JoinType::Inner, [predicate])
                .map_err(|e| self.plan_error("Failed to join path endpoints", e))?;
        }
        builder
            .build()
            .map_err(|e| self.plan_error("Failed to build path endpoints", e))
    }

    /// One hop of a path of several hops, joined to its relationship scan. Cypher never
    /// walks an edge twice in a path, so rows where this hop walked an edge an earlier hop
    /// (`walked`) walked are removed, before the target join.
    #[allow(clippy::too_many_arguments)]
    fn build_trail_hop(
        &self,
        left_plan: LogicalPlan,
        [source_variable, target_variable]: [&str; 2],
        [source_label, target_label]: [&str; 2],
        instance: &RelationshipInstance,
        relationship_properties: &HashMap<String, PropertyValue>,
        target_properties: &HashMap<String, PropertyValue>,
        walked: Option<&mut Vec<HopEdge>>,
    ) -> Result<LogicalPlan> {
        let direction = &instance.direction;
        let rel_map = self.get_relationship_mapping(&instance.rel_type)?;
        let cat = self.get_catalog()?;
        let identity_source = if walked.is_some() {
            cat.relationship_occurrence_source(&instance.rel_type)?
        } else {
            None
        };
        let (rel_source, stored) = match identity_source {
            Some((source, id)) => (source, Some(id)),
            None => (
                cat.relationship_source(&instance.rel_type)
                    .ok_or_else(|| self.missing_table(&instance.rel_type))?,
                None,
            ),
        };
        let node_mapping = |label: &str| {
            self.config
                .get_node_mapping(label)
                .ok_or_else(|| self.missing_table(label))
        };
        let source_map = node_mapping(source_label)?;
        let target_map = node_mapping(target_label)?;
        let rank = walked
            .as_ref()
            .map(|_| HopEdge::rank_column(&instance.alias));
        let rel_scan = self.build_relationship_scan(
            instance,
            rel_source,
            relationship_properties,
            rel_map,
            direction,
            rank.as_deref().map(|output| TrailIdentity {
                output,
                stored: stored.as_deref(),
            }),
        )?;
        let mut builder = self.join_source_to_relationship(
            left_plan,
            rel_scan,
            &SourceJoinParams {
                source_variable,
                rel_qualifier: &instance.alias,
                node_id_field: &source_map.id_field,
                rel_map,
                direction,
            },
        )?;
        if let Some(walked) = walked {
            let edge = HopEdge::new(&instance.alias, rel_map);
            let undirected = *direction == RelationshipDirection::Undirected;
            if let Some(repeated) = walked
                .iter()
                .map(|prior| {
                    if stored.is_some() {
                        edge.rank.clone().eq(prior.rank.clone())
                    } else {
                        edge.is_same_edge(prior, undirected)
                    }
                })
                .reduce(Expr::or)
            {
                builder = builder
                    .filter(!repeated)
                    .map_err(|e| self.plan_error("Failed to filter repeated relationships", e))?;
            }
            walked.push(edge);
        }
        self.join_relationship_to_target(
            builder,
            cat,
            &TargetJoinParams {
                target_variable,
                target_label,
                rel_qualifier: &instance.alias,
                node_map: target_map,
                rel_map,
                direction,
                target_properties,
            },
        )
    }
}
