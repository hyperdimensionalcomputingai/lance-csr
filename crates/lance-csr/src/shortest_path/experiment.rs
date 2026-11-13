// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Opt-in research harness for #76. No planner selects this implementation.

use super::*;
use std::cell::Cell;

pub type SearchResult = (FastMap<i64, u32>, FastMap<i64, Vec<i64>>);
pub type Request = (i64, HashSet<i64>);

thread_local! {
    static COUNTERS: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}

// Counting revisits adjacency only in a separate, untimed profiling pass.
pub(super) fn record_scan(adjacency: &Neighbors, node: i64) {
    COUNTERS.with(|c| {
        if let Some((scans, edges)) = c.get() {
            let mut count = 0;
            adjacency.for_each_neighbor(node, |_| count += 1);
            c.set(Some((scans + 1, edges + count)));
        }
    });
}

pub fn profile(enabled: bool) -> (usize, usize) {
    COUNTERS.with(|c| c.replace(enabled.then_some((0, 0))).unwrap_or_default())
}

/// Invoke the existing searches, or the prototype, on the same adjacency and requests.
pub async fn run(
    adjacency: &Neighbors,
    reverse: &Neighbors,
    requests: &[Request],
    max_hops: u32,
    mode: &str,
) -> Vec<SearchResult> {
    assert!(matches!(
        mode,
        "forward" | "current" | "batched" | "masked" | "parallel"
    ));
    if mode == "masked" {
        let mut results = Vec::new();
        for batch in requests.chunks(64) {
            results.extend(masked(adjacency, batch, max_hops).await);
        }
        return results;
    }
    if mode == "batched" {
        let mut results = Vec::new();
        for batch in requests.chunks(64) {
            results.extend(batched(adjacency, batch, max_hops).await);
        }
        return results;
    }
    if mode == "parallel" {
        // Bounded source parallelism is a simpler competing experiment.
        let mut results = Vec::new();
        for batch in requests.chunks(8) {
            let mut tasks = Vec::new();
            for (source, targets) in batch {
                let (adj, rev, source, targets) =
                    (adjacency.clone(), reverse.clone(), *source, targets.clone());
                tasks.push(tokio::spawn(async move {
                    current(&adj, &rev, source, &targets, max_hops).await
                }));
            }
            for task in tasks {
                results.push(task.await.unwrap());
            }
        }
        return results;
    }
    let mut results = Vec::new();
    for (source, targets) in requests {
        results.push(if mode == "current" {
            current(adjacency, reverse, *source, targets, max_hops).await
        } else {
            search(adjacency, *source, targets, max_hops).await
        });
    }
    results
}

async fn current(
    adjacency: &Neighbors,
    reverse: &Neighbors,
    source: i64,
    targets: &HashSet<i64>,
    max_hops: u32,
) -> SearchResult {
    if targets.len() == 1 {
        search_bidirectional(
            adjacency,
            reverse,
            source,
            *targets.iter().next().unwrap(),
            max_hops,
        )
        .await
    } else {
        search(adjacency, source, targets, max_hops).await
    }
}

async fn batched(adjacency: &Neighbors, requests: &[Request], max_hops: u32) -> Vec<SearchResult> {
    assert!(requests.len() <= 64);
    let mut results: Vec<SearchResult> = requests.iter().map(|_| Default::default()).collect();
    let mut remaining: Vec<HashSet<i64>> = requests
        .iter()
        .map(|(s, ts)| ts.iter().copied().filter(|t| t != s).collect())
        .collect();
    let mut seen: FastMap<i64, u64> = FastMap::default();
    let mut frontier: FastMap<i64, u64> = FastMap::default();
    let mut active = 0_u64;
    for (i, (source, _)) in requests.iter().enumerate() {
        let bit = 1_u64 << i;
        results[i].0.insert(*source, 0);
        *seen.entry(*source).or_default() |= bit;
        if !remaining[i].is_empty() {
            active |= bit;
            *frontier.entry(*source).or_default() |= bit;
        }
    }
    let mut work = 0_usize;
    for level in 1..=max_hops {
        if frontier.is_empty() || active == 0 {
            break;
        }
        let mut next: FastMap<i64, u64> = FastMap::default();
        for (node, mask) in frontier {
            let mask = mask & active;
            if mask == 0 {
                continue;
            }
            record_scan(adjacency, node);
            work += 1;
            if work.is_multiple_of(1024) {
                crate::csr_expand::cooperative_yield().await;
            }
            let mut position = crate::csr_expand::NeighborPosition::default();
            for candidate in adjacency.candidates_from::<false>(node, &mut position) {
                work += 1;
                if work.is_multiple_of(1024) {
                    crate::csr_expand::cooperative_yield().await;
                }
                let Some((neighbor, _)) = candidate else {
                    continue;
                };
                let neighbor = neighbor as i64;
                // Seen is updated only after a complete level, preserving tied paths and parallel edges.
                let mut lanes = mask & !seen.get(&neighbor).copied().unwrap_or(0);
                if lanes == 0 {
                    continue;
                }
                *next.entry(neighbor).or_default() |= lanes;
                while lanes != 0 {
                    let lane = lanes.trailing_zeros() as usize;
                    lanes &= lanes - 1;
                    results[lane].0.insert(neighbor, level);
                    results[lane].1.entry(neighbor).or_default().push(node);
                    remaining[lane].remove(&neighbor);
                }
            }
        }
        for (&node, &mask) in &next {
            *seen.entry(node).or_default() |= mask;
        }
        for (i, targets) in remaining.iter().enumerate() {
            if targets.is_empty() {
                active &= !(1_u64 << i);
            }
        }
        frontier = next;
    }
    results
}

#[derive(Default)]
struct Shared {
    seen: u64,
    next: u64,
    /// `(predecessor, lanes it is a shortest-path predecessor for)`, one entry per edge.
    parents: Vec<(i64, u64)>,
}

/// Control for `batched`: traversal state is one lane mask per vertex and one
/// `(predecessor, mask)` per useful edge, shared by all lanes. Per-source maps are
/// built afterwards, and only for vertices on a shortest path to a requested target.
async fn masked(adjacency: &Neighbors, requests: &[Request], max_hops: u32) -> Vec<SearchResult> {
    assert!(requests.len() <= 64);
    let mut results: Vec<SearchResult> = requests.iter().map(|_| Default::default()).collect();
    let mut remaining: Vec<Vec<i64>> = requests
        .iter()
        .map(|(s, ts)| ts.iter().copied().filter(|t| t != s).collect())
        .collect();
    let mut state: FastMap<i64, Shared> = FastMap::default();
    let mut frontier: Vec<(i64, u64)> = Vec::new();
    let mut active = 0_u64;
    for (i, (source, _)) in requests.iter().enumerate() {
        results[i].0.insert(*source, 0);
        state.entry(*source).or_default().seen |= 1 << i;
        if !remaining[i].is_empty() {
            active |= 1 << i;
        }
    }
    for (i, (source, _)) in requests.iter().enumerate() {
        // Repeated sources share one frontier entry.
        if active & (1 << i) != 0 && !frontier.iter().any(|(n, _)| n == source) {
            frontier.push((*source, state[source].seen & active));
        }
    }
    let mut work = 0_usize;
    for level in 1..=max_hops {
        if frontier.is_empty() || active == 0 {
            break;
        }
        let mut touched = Vec::new();
        for &(node, mask) in &frontier {
            let mask = mask & active;
            if mask == 0 {
                continue;
            }
            record_scan(adjacency, node);
            let mut position = crate::csr_expand::NeighborPosition::default();
            for candidate in adjacency.candidates_from::<false>(node, &mut position) {
                work += 1;
                if work.is_multiple_of(1024) {
                    crate::csr_expand::cooperative_yield().await;
                }
                let Some((neighbor, _)) = candidate else {
                    continue;
                };
                let neighbor = neighbor as i64;
                let shared = state.entry(neighbor).or_default();
                // `seen` changes only after the level, so ties and parallel edges are kept.
                let lanes = mask & !shared.seen;
                if lanes == 0 {
                    continue;
                }
                if shared.next == 0 {
                    touched.push(neighbor);
                }
                shared.next |= lanes;
                shared.parents.push((node, lanes));
            }
        }
        frontier.clear();
        for node in touched {
            let shared = state.get_mut(&node).expect("touched");
            shared.seen |= shared.next;
            frontier.push((node, std::mem::take(&mut shared.next)));
        }
        for (i, targets) in remaining.iter_mut().enumerate() {
            let bit = 1_u64 << i;
            if active & bit == 0 {
                continue;
            }
            targets.retain(|t| {
                let found = state.get(t).is_some_and(|s| s.seen & bit != 0);
                if found {
                    results[i].0.insert(*t, level);
                }
                !found
            });
            if targets.is_empty() {
                active &= !bit;
            }
        }
    }
    for (i, (depth, parents)) in results.iter_mut().enumerate() {
        let bit = 1_u64 << i;
        let mut stack: Vec<i64> = depth.keys().copied().collect();
        while let Some(node) = stack.pop() {
            work += 1;
            if work.is_multiple_of(1024) {
                crate::csr_expand::cooperative_yield().await;
            }
            if parents.contains_key(&node) {
                continue;
            }
            let own: Vec<i64> = state[&node]
                .parents
                .iter()
                .filter(|(_, lanes)| lanes & bit != 0)
                .map(|(parent, _)| *parent)
                .collect();
            // The source has none; an empty entry would hide the path's start.
            if !own.is_empty() {
                stack.extend(&own);
                parents.insert(node, own);
            }
        }
    }
    results
}

/// Materialize target paths with the production reconstruction routine.
pub async fn paths(
    results: &[SearchResult],
    requests: &[Request],
    all: bool,
) -> Vec<Vec<Vec<i64>>> {
    let mut rows = Vec::new();
    for ((depth, parents), (_, targets)) in results.iter().zip(requests) {
        let mut targets: Vec<_> = targets.iter().copied().collect();
        targets.sort();
        for target in targets {
            let mut paths = if depth.contains_key(&target) {
                paths_to(target, parents, all).await
            } else {
                vec![]
            };
            paths.sort();
            rows.push(paths);
        }
    }
    rows
}
