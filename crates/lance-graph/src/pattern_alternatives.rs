// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Relationship-type alternation (`-[:A|B]->`) and multi-label nodes (`(m:Post:Comment)`).
//!
//! A MATCH clause that uses either is rewritten into branches with exactly one type per
//! relationship and at most one label per node. Only combinations consistent with the
//! configured endpoint labels ([`RelationshipMapping::source_label`]) are kept: in LDBC,
//! `(:Post:Comment)<-[:replyOfPost|replyOfComment]-(c)` gives two branches, not four.
//! The planner plans each branch as an ordinary pattern (so each keeps the CSR and
//! re-rooting) and UNION ALLs them. As in Kuzu, a multi-label node means "any of these
//! labels". Combinations that the configuration cannot pin down are an error rather
//! than a guess, because joining an edge type to the wrong node table can silently match
//! wrong rows whenever two labels share ids.
//!
//! A variable-length segment with several types (`-[:hasType|isSubclassOf*1..20]->`) is
//! first unrolled into one fixed-length path per sequence of types whose endpoint labels
//! chain (LDBC IC12: `hasType` from the Tag, then only `isSubclassOf` between Tagclasses),
//! and each of those paths then goes through the same branching.
//!
//! [`RelationshipMapping::source_label`]: crate::config::RelationshipMapping::source_label

use std::collections::HashMap;

use crate::ast::{
    GraphPattern, NodePattern, PathSegment, RelationshipDirection, RelationshipPattern,
};
use crate::config::GraphConfig;
use crate::error::{GraphError, Result};

/// Branch limit, so a pattern full of alternatives can't explode the plan.
const MAX_BRANCHES: usize = 64;

fn zero_hop(rel: &RelationshipPattern) -> bool {
    rel.length
        .as_ref()
        .is_some_and(|r| r.min == Some(0) && r.max == Some(0))
}

/// `patterns` with labels filled in for unlabeled nodes that the configured endpoint labels
/// pin down: `(friend)<-[:postHasCreator]-(post)` makes `post` a Post when
/// `postHasCreator` goes from Post to Person. Labels flow along paths and between
/// patterns through shared variables; `bound` holds labels from earlier clauses. Nodes
/// next to alternatives whose labels differ are left for [`expand`] to branch on.
pub fn infer_labels(
    patterns: &[GraphPattern],
    config: &GraphConfig,
    bound: &HashMap<String, String>,
) -> Vec<GraphPattern> {
    let mut patterns = patterns.to_vec();
    let mut known: HashMap<String, String> = bound
        .iter()
        .filter(|(_, l)| config.get_node_mapping(l).is_some())
        .map(|(v, l)| (v.clone(), l.clone()))
        .collect();
    // The label one end of `rel` has, if every type agrees: `source` side or target side.
    let end_label = |rel: &RelationshipPattern, source: bool| -> Option<String> {
        let mut labels = rel.types.iter().map(|t| {
            let m = config.get_relationship_mapping(t)?;
            if source {
                m.source_label.clone()
            } else {
                m.target_label.clone()
            }
        });
        let first = labels.next()??;
        labels.all(|l| l.as_ref() == Some(&first)).then_some(first)
    };
    loop {
        let mut changed = false;
        for pattern in &mut patterns {
            let GraphPattern::Path(path) = pattern else {
                continue;
            };
            let rels: Vec<RelationshipPattern> = path
                .segments
                .iter()
                .map(|s| s.relationship.clone())
                .collect();
            let mut nodes: Vec<&mut NodePattern> = std::iter::once(&mut path.start_node)
                .chain(path.segments.iter_mut().map(|s| &mut s.end_node))
                .collect();
            let label_of =
                |n: &NodePattern, known: &HashMap<String, String>| match n.labels.as_slice() {
                    [l] => Some(l.clone()),
                    [] => n.variable.as_ref().and_then(|v| known.get(v).cloned()),
                    _ => None,
                };
            for (i, rel) in rels.iter().enumerate() {
                if rel.types.is_empty() {
                    continue;
                }
                // (left side, right side) as written.
                let (left, right) = match rel.direction {
                    RelationshipDirection::Outgoing => {
                        (end_label(rel, true), end_label(rel, false))
                    }
                    RelationshipDirection::Incoming => {
                        (end_label(rel, false), end_label(rel, true))
                    }
                    RelationshipDirection::Undirected => {
                        let (s, t) = (end_label(rel, true), end_label(rel, false));
                        if s == t {
                            (s.clone(), t)
                        } else {
                            (None, None)
                        }
                    }
                };
                for (slot, label) in [(i, left), (i + 1, right)] {
                    let Some(label) = label else { continue };
                    let node = &mut nodes[slot];
                    if !node.labels.is_empty() || label_of(node, &known).is_some() {
                        continue;
                    }
                    match &node.variable {
                        Some(v) => {
                            known.insert(v.clone(), label);
                        }
                        None => node.labels = vec![label],
                    }
                    changed = true;
                }
            }
            // Written labels of named nodes inform the other patterns.
            for node in nodes {
                if let (Some(v), [l]) = (&node.variable, node.labels.as_slice()) {
                    changed |= known.insert(v.clone(), l.clone()).is_none();
                }
            }
        }
        if !changed {
            break;
        }
    }
    // Write inferred labels onto the unlabeled occurrences of named, unbound variables.
    for pattern in &mut patterns {
        let nodes: Vec<&mut NodePattern> = match pattern {
            GraphPattern::Node(n) => vec![n],
            GraphPattern::Path(p) => std::iter::once(&mut p.start_node)
                .chain(p.segments.iter_mut().map(|s| &mut s.end_node))
                .collect(),
        };
        for node in nodes {
            if let Some(v) = &node.variable {
                if node.labels.is_empty() && !bound.contains_key(v) {
                    if let Some(l) = known.get(v) {
                        node.labels = vec![l.clone()];
                    }
                }
            }
        }
    }
    patterns
}

/// True when any relationship has several types or any node several labels.
pub fn needs_expansion(patterns: &[GraphPattern], retain_variable: bool) -> bool {
    let nodes_multi = |n: &NodePattern| n.labels.len() > 1;
    patterns.iter().any(|p| match p {
        GraphPattern::Node(n) => nodes_multi(n),
        GraphPattern::Path(path) => {
            nodes_multi(&path.start_node)
                || path.segments.iter().any(|s| {
                    (s.relationship.types.len() > 1
                        && !(retain_variable && s.relationship.length.is_some()))
                        || nodes_multi(&s.end_node)
                })
        }
    })
}

/// One relationship occurrence: the node slots at its two ends, as written.
struct Rel {
    left: usize,
    right: usize,
    direction: RelationshipDirection,
    types: Vec<String>,
}

/// Rewrite `patterns` into its consistent single-type, single-label branches.
///
/// `bound` holds labels of variables bound by earlier clauses; they are fixed.
pub fn expand(
    patterns: &[GraphPattern],
    config: &GraphConfig,
    bound: &HashMap<String, String>,
    retain_variable: bool,
) -> Result<Vec<Vec<GraphPattern>>> {
    if let Some(variants) = if retain_variable {
        None
    } else {
        unroll_variable_length_alternation(patterns, config, bound)?
    } {
        let mut branches = Vec::new();
        for variant in variants {
            branches.extend(expand(&variant, config, bound, retain_variable)?);
            if branches.len() > MAX_BRANCHES {
                return Err(too_many());
            }
        }
        return Ok(branches);
    }
    // Node slots: one per named variable, one per anonymous node occurrence.
    let mut allowed: Vec<Option<Vec<String>>> = Vec::new();
    let mut named: HashMap<String, usize> = HashMap::new();
    let mut occurrence_slots: Vec<usize> = Vec::new();
    let mut slot = |node: &NodePattern, allowed: &mut Vec<Option<Vec<String>>>| -> usize {
        let id = match &node.variable {
            Some(v) => *named.entry(v.clone()).or_insert_with(|| {
                allowed.push(bound.get(v).map(|l| vec![l.clone()]));
                allowed.len() - 1
            }),
            None => {
                allowed.push(None);
                allowed.len() - 1
            }
        };
        if !node.labels.is_empty() {
            allowed[id] = Some(match allowed[id].take() {
                None => node.labels.clone(),
                Some(prev) => prev
                    .into_iter()
                    .filter(|l| node.labels.iter().any(|n| n.eq_ignore_ascii_case(l)))
                    .collect(),
            });
        }
        occurrence_slots.push(id);
        id
    };
    let mut rels = Vec::new();
    for pattern in patterns {
        match pattern {
            GraphPattern::Node(n) => {
                slot(n, &mut allowed);
            }
            GraphPattern::Path(path) => {
                let mut left = slot(&path.start_node, &mut allowed);
                for segment in &path.segments {
                    let rel = &segment.relationship;
                    let right = slot(&segment.end_node, &mut allowed);
                    if !(zero_hop(rel) || retain_variable && rel.length.is_some()) {
                        rels.push(Rel {
                            left,
                            right,
                            direction: rel.direction.clone(),
                            types: rel.types.clone(),
                        });
                    }
                    left = right;
                }
            }
        }
    }

    let endpoints = |t: &str| {
        config
            .get_relationship_mapping(t)
            .and_then(|m| Some((m.source_label.clone()?, m.target_label.clone()?)))
    };
    // Candidate labels per slot: as written, else every endpoint label adjacent types allow.
    let candidates: Vec<Vec<Option<String>>> = allowed
        .iter()
        .enumerate()
        .map(|(id, allowed)| match allowed {
            Some(labels) => labels.iter().cloned().map(Some).collect(),
            None => {
                let mut labels: Vec<Option<String>> = Vec::new();
                for rel in rels.iter().filter(|r| r.left == id || r.right == id) {
                    for (src, dst) in rel.types.iter().filter_map(|t| endpoints(t)) {
                        for label in end_labels(rel, id, src, dst) {
                            if !labels
                                .iter()
                                .flatten()
                                .any(|l| l.eq_ignore_ascii_case(&label))
                            {
                                labels.push(Some(label));
                            }
                        }
                    }
                }
                if labels.is_empty() {
                    labels.push(None); // unlabeled, as written
                }
                labels
            }
        })
        .collect();
    for (id, labels) in candidates.iter().enumerate() {
        if labels.is_empty() {
            return Err(plan_error(
                "a node's labels contradict an earlier binding".into(),
            ));
        }
        // Several candidates must be told apart by endpoint labels on every adjacent type.
        if labels.len() > 1 {
            for rel in rels.iter().filter(|r| r.left == id || r.right == id) {
                if let Some(t) = rel.types.iter().find(|t| endpoints(t).is_none()) {
                    return Err(plan_error(format!(
                        "relationship type '{t}' needs endpoint labels (GraphConfig rel_table's \
                         from_table and to_table) to be planned next to a node that can \
                         have several labels"
                    )));
                }
            }
        }
    }

    let total = rels
        .iter()
        .map(|r| r.types.len())
        .chain(candidates.iter().map(Vec::len))
        .try_fold(1usize, |acc, n| acc.checked_mul(n))
        .filter(|&n| n <= MAX_BRANCHES)
        .ok_or_else(too_many)?;

    let mut branches = Vec::new();
    for combo in 0..total {
        // Mixed-radix decode: relationship types first, then slot labels.
        let mut rest = combo;
        let mut pick = |n: usize| {
            let i = rest % n;
            rest /= n;
            i
        };
        let types: Vec<&str> = rels
            .iter()
            .map(|r| r.types[pick(r.types.len())].as_str())
            .collect();
        let labels: Vec<Option<&str>> = candidates
            .iter()
            .map(|c| c[pick(c.len())].as_deref())
            .collect();
        let consistent = rels.iter().zip(&types).all(|(rel, t)| match endpoints(t) {
            None => true,
            Some((src, dst)) => {
                let fits = |slot: usize, want: &str| {
                    labels[slot].is_none_or(|l| l.eq_ignore_ascii_case(want))
                };
                match rel.direction {
                    RelationshipDirection::Outgoing => {
                        fits(rel.left, &src) && fits(rel.right, &dst)
                    }
                    RelationshipDirection::Incoming => {
                        fits(rel.left, &dst) && fits(rel.right, &src)
                    }
                    RelationshipDirection::Undirected => {
                        (fits(rel.left, &src) && fits(rel.right, &dst))
                            || (fits(rel.left, &dst) && fits(rel.right, &src))
                    }
                }
            }
        });
        if consistent {
            branches.push(rewrite(
                patterns,
                &occurrence_slots,
                &labels,
                &types,
                retain_variable,
            ));
        }
    }
    if branches.is_empty() {
        return Err(plan_error(
            "no combination of relationship types and labels in this pattern is consistent \
             with the configured endpoint labels"
                .into(),
        ));
    }
    Ok(branches)
}

/// The first variable-length segment with several types, replaced by each fixed-length path
/// it can match: per hop count in its range, every sequence of types whose endpoint labels
/// chain from the segment's start node to its end node. `None` when there is no such
/// segment. Every type needs endpoint labels, since they decide which types may follow.
fn unroll_variable_length_alternation(
    patterns: &[GraphPattern],
    config: &GraphConfig,
    bound: &HashMap<String, String>,
) -> Result<Option<Vec<Vec<GraphPattern>>>> {
    let found = patterns
        .iter()
        .enumerate()
        .find_map(|(p, pattern)| match pattern {
            GraphPattern::Path(path) => path
                .segments
                .iter()
                .position(|s| s.relationship.length.is_some() && s.relationship.types.len() > 1)
                .map(|i| (p, path, i)),
            GraphPattern::Node(_) => None,
        });
    let Some((p, path, i)) = found else {
        return Ok(None);
    };
    let rel = &path.segments[i].relationship;
    if rel.variable.is_some() || rel.direction == RelationshipDirection::Undirected {
        return Err(unsupported(
            "relationship-type alternation in an undirected or named variable-length pattern",
        ));
    }
    let mut ends = Vec::with_capacity(rel.types.len());
    for t in &rel.types {
        let m = config.get_relationship_mapping(t);
        let (Some(src), Some(dst)) = (
            m.and_then(|m| m.source_label.clone()),
            m.and_then(|m| m.target_label.clone()),
        ) else {
            return Err(plan_error(format!(
                "relationship type '{t}' needs endpoint labels (GraphConfig rel_table's \
                 from_table and to_table) for alternation in a variable-length pattern"
            )));
        };
        // Labels in traversal order: the segment is read from its left node.
        ends.push(match rel.direction {
            RelationshipDirection::Incoming => (t.clone(), dst, src),
            _ => (t.clone(), src, dst),
        });
    }
    let node_labels = |node: &NodePattern| -> Vec<String> {
        match node.variable.as_ref().and_then(|v| bound.get(v)) {
            Some(label) => vec![label.clone()],
            None => node.labels.clone(),
        }
    };
    let left = if i == 0 {
        &path.start_node
    } else {
        &path.segments[i - 1].end_node
    };
    let (starts, finishes) = (node_labels(left), node_labels(&path.segments[i].end_node));
    let fits = |labels: &[String], label: &str| {
        labels.is_empty() || labels.iter().any(|l| l.eq_ignore_ascii_case(label))
    };
    let range = rel.length.as_ref().expect("a variable-length segment");
    let min = range.min.unwrap_or(1);
    let max = range.max.unwrap_or(crate::MAX_VARIABLE_LENGTH_HOPS);
    if max > crate::MAX_VARIABLE_LENGTH_HOPS {
        return Err(unsupported(&format!(
            "variable-length paths longer than {} hops",
            crate::MAX_VARIABLE_LENGTH_HOPS
        )));
    }
    // Depth-first over (type, label after the hop) sequences.
    let mut sequences: Vec<Vec<(String, String)>> = Vec::new();
    let mut stack: Vec<Vec<(String, String)>> = vec![vec![]];
    while let Some(prefix) = stack.pop() {
        let hops = prefix.len() as u32;
        let at = prefix.last().map(|(_, label)| label.clone());
        if hops >= min && at.as_deref().is_some_and(|l| fits(&finishes, l)) {
            sequences.push(prefix.clone());
            if sequences.len() > MAX_BRANCHES {
                return Err(too_many());
            }
        }
        if hops == max {
            continue;
        }
        for (t, from, to) in &ends {
            let chains = match &at {
                None => fits(&starts, from),
                Some(label) => label.eq_ignore_ascii_case(from),
            };
            if chains {
                let mut next = prefix.clone();
                next.push((t.clone(), to.clone()));
                stack.push(next);
            }
        }
    }
    if sequences.is_empty() && min != 0 {
        return Err(plan_error(
            "no sequence of relationship types in the variable-length pattern chains its \
             endpoint labels"
                .into(),
        ));
    }
    let mut variants = Vec::new();
    if min == 0 {
        let mut identity = path.clone();
        identity.segments[i].relationship.types = vec![rel.types[0].clone()];
        identity.segments[i].relationship.length = Some(crate::ast::LengthRange {
            min: Some(0),
            max: Some(0),
        });
        let mut variant = patterns.to_vec();
        variant[p] = GraphPattern::Path(identity);
        variants.push(variant);
    }
    variants.extend(sequences.into_iter().map(|sequence| {
        let last = sequence.len() - 1;
        let hops = sequence
            .into_iter()
            .enumerate()
            .map(|(h, (t, label))| PathSegment {
                relationship: RelationshipPattern {
                    types: vec![t],
                    length: None,
                    ..rel.clone()
                },
                end_node: if h == last {
                    path.segments[i].end_node.clone()
                } else {
                    NodePattern {
                        variable: None,
                        labels: vec![label],
                        properties: HashMap::new(),
                    }
                },
            });
        let mut unrolled = path.clone();
        unrolled.segments.splice(i..=i, hops);
        let mut variant = patterns.to_vec();
        variant[p] = GraphPattern::Path(unrolled);
        variant
    }));
    Ok(Some(variants))
}

fn too_many() -> GraphError {
    plan_error(format!(
        "pattern has more than {MAX_BRANCHES} combinations of relationship types and labels"
    ))
}

/// The labels slot `id` may take at its end of `rel`, given the type's endpoints.
fn end_labels(rel: &Rel, id: usize, src: String, dst: String) -> Vec<String> {
    let (left, right) = match rel.direction {
        RelationshipDirection::Outgoing => (vec![src], vec![dst]),
        RelationshipDirection::Incoming => (vec![dst], vec![src]),
        RelationshipDirection::Undirected => (vec![src.clone(), dst.clone()], vec![src, dst]),
    };
    if rel.left == id {
        left
    } else {
        right
    }
}

/// `patterns` with each node occurrence given its slot's label and each relationship its type.
fn rewrite(
    patterns: &[GraphPattern],
    occurrence_slots: &[usize],
    labels: &[Option<&str>],
    types: &[&str],
    retain_variable: bool,
) -> Vec<GraphPattern> {
    let mut occurrences = occurrence_slots.iter();
    let mut types = types.iter();
    let mut relabel = |node: &NodePattern| NodePattern {
        labels: labels[*occurrences.next().expect("one slot per node occurrence")]
            .map(|l| vec![l.to_string()])
            .unwrap_or_default(),
        ..node.clone()
    };
    patterns
        .iter()
        .map(|p| match p {
            GraphPattern::Node(n) => GraphPattern::Node(relabel(n)),
            GraphPattern::Path(path) => {
                let mut path = path.clone();
                path.start_node = relabel(&path.start_node);
                for segment in &mut path.segments {
                    if !(zero_hop(&segment.relationship)
                        || retain_variable && segment.relationship.length.is_some())
                    {
                        segment.relationship.types =
                            vec![types.next().expect("one type per relationship").to_string()];
                    }
                    segment.end_node = relabel(&segment.end_node);
                }
                GraphPattern::Path(path)
            }
        })
        .collect()
}

fn plan_error(message: String) -> GraphError {
    GraphError::PlanError {
        message,
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

fn unsupported(feature: &str) -> GraphError {
    GraphError::UnsupportedFeature {
        feature: feature.to_string(),
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::ReadingClause;
    use crate::parser::parse_cypher_query;

    fn ldbc_config() -> GraphConfig {
        let mut b = GraphConfig::builder();
        for label in ["Person", "Post", "Comment"] {
            b = b.node_table(label, "id");
        }
        for (t, s, d) in [
            ("postHasCreator", "Post", "Person"),
            ("commentHasCreator", "Comment", "Person"),
            ("replyOfPost", "Comment", "Post"),
            ("replyOfComment", "Comment", "Comment"),
        ] {
            b = b.rel_table(t, s, d, "src", "dst");
        }
        // Without endpoint labels, which only the deprecated builder can still configure.
        #[allow(deprecated)]
        {
            b = b.with_relationship("knows", "src", "dst");
        }
        b.build().unwrap()
    }

    fn branches(query: &str) -> Result<Vec<String>> {
        let ast = parse_cypher_query(query).unwrap();
        let ReadingClause::Match(m) = &ast.reading_clauses[0] else {
            panic!("not a MATCH")
        };
        let out = expand(&m.patterns, &ldbc_config(), &HashMap::new(), false)?;
        Ok(out.iter().map(|b| describe(b)).collect())
    }

    /// Compact "label-type-label..." form of one branch's first path.
    fn describe(patterns: &[GraphPattern]) -> String {
        let label = |n: &NodePattern| n.labels.first().cloned().unwrap_or("_".into());
        match &patterns[0] {
            GraphPattern::Node(n) => label(n),
            GraphPattern::Path(p) => p.segments.iter().fold(label(&p.start_node), |acc, s| {
                format!("{acc}-{}-{}", s.relationship.types[0], label(&s.end_node))
            }),
        }
    }

    #[test]
    fn alternation_pairs_types_with_their_endpoint_labels() {
        let mut got = branches(
            "MATCH (p:Person)<-[:postHasCreator|commentHasCreator]-(m)\
             <-[:replyOfPost|replyOfComment]-(c:Comment) RETURN c",
        )
        .unwrap();
        got.sort();
        assert_eq!(
            got,
            [
                "Person-commentHasCreator-Comment-replyOfComment-Comment",
                "Person-postHasCreator-Post-replyOfPost-Comment",
            ]
        );
    }

    #[test]
    fn multi_label_node_alone_is_a_union_of_scans() {
        assert_eq!(
            branches("MATCH (m:Post:Comment) RETURN m").unwrap(),
            ["Post", "Comment"]
        );
    }

    #[test]
    fn written_labels_restrict_the_branches() {
        assert_eq!(
            branches("MATCH (p:Person)<-[:postHasCreator|commentHasCreator]-(m:Post) RETURN m")
                .unwrap(),
            ["Person-postHasCreator-Post"]
        );
    }

    #[test]
    fn missing_endpoints_and_variable_length_alternation_are_errors() {
        // `knows` has no endpoint labels, so the multi-label end can't be resolved.
        assert!(branches("MATCH (a:Person)-[:knows]->(m:Post:Comment) RETURN m").is_err());
        // Undirected variable-length alternation is not supported.
        assert!(branches("MATCH (a)-[:replyOfPost|replyOfComment*1..2]-(m) RETURN m").is_err());
        // Contradictory: postHasCreator never starts at a Comment.
        assert!(branches(
            "MATCH (p:Person)<-[:postHasCreator|postHasCreator]-(m:Comment) RETURN m"
        )
        .is_err());
    }

    #[test]
    fn variable_length_alternation_unrolls_chaining_type_sequences() {
        // Comment -replyOfComment-> Comment ... -replyOfPost-> Post: only sequences of
        // replyOfComment hops ending in one replyOfPost reach a Post.
        let mut got =
            branches("MATCH (c:Comment)-[:replyOfComment|replyOfPost*1..3]->(p:Post) RETURN p")
                .unwrap();
        got.sort();
        assert_eq!(
            got,
            [
                "Comment-replyOfComment-Comment-replyOfComment-Comment-replyOfPost-Post",
                "Comment-replyOfComment-Comment-replyOfPost-Post",
                "Comment-replyOfPost-Post",
            ]
        );
    }
}
