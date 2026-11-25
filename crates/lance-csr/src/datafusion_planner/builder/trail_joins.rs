// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

//! Relational fallback for variable-length segments: one variable-length relationship, or a
//! chain of segments that must not share an edge, unrolled into one branch of joins per
//! combination of hop sequences. The stored endpoints and an occurrence number among
//! parallel edges distinguish a hop from earlier hops of the same type.

use crate::ast::{PropertyValue, RelationshipDirection};
use crate::case_insensitive::qualify_column;
use crate::datafusion_planner::analysis::RelationshipInstance;
use crate::datafusion_planner::join_ops::{SourceJoinParams, TargetJoinParams};
use crate::datafusion_planner::scan_ops::TrailIdentity;
use crate::datafusion_planner::DataFusionPlanner;
use crate::error::{GraphError, Result};
use crate::pattern_alternatives::{segment_sequences, Enumerated, TrailStep};
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

use crate::pattern_alternatives::MAX_BRANCHES as MAX_CHAIN_BRANCHES;

fn rank_column(alias: &str) -> String {
    format!("#trail rank {alias}")
}

impl DataFusionPlanner {
    fn missing_table(&self, name: &str) -> GraphError {
        GraphError::ConfigError {
            message: format!("No mapping or table source found for '{name}'"),
            location: snafu::Location::new(file!(), line!(), column!()),
        }
    }

    /// One join branch per combination of hop sequences. Every hop in a branch
    /// shares its type's edge history. An oversized multi-segment pattern falls back
    /// to ordinary expansions, whose individual joins keep their own history.
    pub(super) fn build_trail_chain(
        &self,
        input: &LogicalPlan,
        [start, start_label]: [&str; 2],
        segments: &[ChainSegment],
        end_properties: &HashMap<String, PropertyValue>,
    ) -> Result<Option<LogicalPlan>> {
        let single = segments.len() == 1;
        let mut options = Vec::with_capacity(segments.len());
        let mut from = start_label;
        let mut branches = 1;
        for segment in segments {
            // A single-segment alternation counts only positive lengths toward its cap.
            let identity =
                single && segment.range[0] == 0 && from.eq_ignore_ascii_case(segment.end_label);
            let enumerated = segment_sequences(
                &self.config,
                segment.types,
                segment.direction,
                segment.range,
                [&[from], &[segment.end_label]],
                MAX_CHAIN_BRANCHES / branches + usize::from(identity),
                single,
            )?;
            let sequences = match enumerated {
                Enumerated::Found(sequences) => sequences,
                Enumerated::OverBudget if single => return Err(crate::pattern_alternatives::too_many()),
                Enumerated::Unlabeled(kind) if single => return Err(GraphError::PlanError {
                    message: format!("relationship type '{kind}' needs endpoint labels for alternation in a variable-length pattern"),
                    location: snafu::Location::new(file!(), line!(), column!()),
                }),
                _ => return Ok(None),
            };
            if sequences.is_empty() {
                if single
                    && segment.types.len() > 1
                    && segment.range[0] > 0
                    && *segment.direction != RelationshipDirection::Undirected
                {
                    return Err(GraphError::PlanError {
                        message: "no sequence of relationship types in the variable-length pattern chains its endpoint labels".into(),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    });
                }
                return self.trail_no_match(input, start, segments).map(Some);
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
                            || f.name().starts_with(&rank_column(name))
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
            let mut walked = HashMap::<String, Vec<Expr>>::new();
            let mut hop = 0_usize;
            for (s, (segment, seq)) in segments.iter().zip(&seqs).enumerate() {
                let end_properties = if s + 1 == segments.len() {
                    end_properties
                } else {
                    &no_properties
                };
                if seq.is_empty() {
                    plan = self.trail_identity(
                        plan,
                        [&previous, segment.end],
                        segment.end_label,
                        end_properties,
                    )?;
                }
                for (i, step) in seq.iter().enumerate() {
                    let last = i + 1 == seq.len();
                    let next = if last {
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
                        segments,
                        segment.properties,
                        if last { end_properties } else { &no_properties },
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
        if plans.len() == 1 {
            return Ok(plans.pop());
        }
        // A flat union keeps the optimizer's recursion depth independent of the branch count.
        let union = datafusion::logical_expr::Union::try_new_with_loose_types(
            plans.into_iter().map(std::sync::Arc::new).collect(),
        )
        .map_err(|e| self.plan_error("Failed to union trail branches", e))?;
        Ok(Some(LogicalPlan::Union(union)))
    }

    /// No rows, with the normal boundary columns, when a segment cannot reach its end label.
    fn trail_no_match(
        &self,
        input: &LogicalPlan,
        start: &str,
        segments: &[ChainSegment],
    ) -> Result<LogicalPlan> {
        let mut builder = LogicalPlanBuilder::from(input.clone());
        for segment in segments {
            let map = self
                .config
                .get_node_mapping(segment.end_label)
                .ok_or_else(|| self.missing_table(segment.end_label))?;
            let key = qualify_column(segment.end, &map.id_field);
            if builder.schema().field_with_unqualified_name(&key).is_ok() {
                continue;
            }
            let source = self
                .get_catalog()?
                .node_source(segment.end_label)
                .ok_or_else(|| self.missing_table(segment.end_label))?;
            let scan =
                self.build_target_scan(source, segment.end_label, segment.end, &HashMap::new())?;
            builder = builder
                .join_on(
                    scan,
                    datafusion::logical_expr::JoinType::Inner,
                    [lit(false)],
                )
                .map_err(|e| self.plan_error("Failed to join path endpoints", e))?;
        }
        let projection = self.chain_projection(input, start, segments)?;
        builder
            .filter(lit(false))
            .and_then(|b| b.project(projection))
            .and_then(|b| b.build())
            .map_err(|e| self.plan_error("Failed to build path endpoints", e))
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

    /// A zero-length segment: its end is the node it starts from. `properties` filter the
    /// end when it is joined here, as a target join does; a bound end cannot take them.
    fn trail_identity(
        &self,
        plan: LogicalPlan,
        [previous, end]: [&str; 2],
        label: &str,
        properties: &HashMap<String, PropertyValue>,
    ) -> Result<LogicalPlan> {
        let map = self
            .config
            .get_node_mapping(label)
            .ok_or_else(|| self.missing_table(label))?;
        let (from, to) = (
            qualify_column(previous, &map.id_field),
            qualify_column(end, &map.id_field),
        );
        let builder = LogicalPlanBuilder::from(plan);
        let bound = previous.eq_ignore_ascii_case(end)
            || builder.schema().field_with_unqualified_name(&to).is_ok();
        if bound && !properties.is_empty() {
            return Err(GraphError::PlanError {
                message: format!(
                    "Property filters on reused variable '{end}' are not yet supported"
                ),
                location: snafu::Location::new(file!(), line!(), column!()),
            });
        }
        if previous.eq_ignore_ascii_case(end) {
            return builder
                .build()
                .map_err(|e| self.plan_error("Failed to bind a zero-length segment", e));
        }
        let builder = if bound {
            builder.filter(col(Column::new_unqualified(from)).eq(col(Column::new_unqualified(to))))
        } else {
            let source = self
                .get_catalog()?
                .node_source(label)
                .ok_or_else(|| self.missing_table(label))?;
            let scan = self.build_target_scan(source, label, end, properties)?;
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
        segments: &[ChainSegment],
        relationship_properties: &HashMap<String, PropertyValue>,
        target_properties: &HashMap<String, PropertyValue>,
        walked: Option<&mut Vec<Expr>>,
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
        let rank = walked.as_ref().map(|_| rank_column(&instance.alias));
        let rel_scan = self.build_relationship_scan(
            instance,
            rel_source,
            relationship_properties,
            rel_map,
            direction,
            rank.as_deref().map(|output| TrailIdentity {
                output,
                stored: stored.as_deref(),
                filter_fields: if stored.is_none() {
                    let mut names: Vec<String> = segments
                        .iter()
                        .filter(|s| {
                            s.types
                                .iter()
                                .any(|t| t.eq_ignore_ascii_case(&instance.rel_type))
                        })
                        .flat_map(|s| s.properties.keys().map(|k| k.to_ascii_lowercase()))
                        .collect();
                    names.sort_unstable();
                    names.dedup();
                    names
                } else {
                    Vec::new()
                },
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
            let edge = col(Column::new_unqualified(rank_column(&instance.alias)));
            if let Some(repeated) = walked
                .iter()
                .map(|prior| edge.clone().eq(prior.clone()))
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
