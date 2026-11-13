// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Synthetic traversal experiment, not an end-to-end query benchmark.
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use lance_csr::csr_expand::{Adjacency, Neighbors, RemovedRows};
use lance_csr::shortest_path::experiment::{self, Request};

async fn graph(edges: &[(i64, i64)], undirected: bool, delta: bool) -> (Neighbors, Neighbors) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("src", DataType::Int64, false),
        Field::new("dst", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(
                edges.iter().map(|p| p.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                edges.iter().map(|p| p.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let stream = || {
        Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok::<_, datafusion::error::DataFusionError>(
                batch.clone(),
            )]),
        ))
    };
    let mut out = Adjacency::build(stream(), "src", "dst").await.unwrap();
    let mut inc = Adjacency::build(stream(), "dst", "src").await.unwrap();
    if delta {
        let mut removed = RemovedRows::default();
        removed.rows(0, [0]);
        out = out.with_changes(&[(0, 3, 999)], &removed);
        inc = inc.with_changes(&[(3, 0, 999)], &removed);
    }
    let (out, inc) = (Arc::new(out), Arc::new(inc));
    if undirected {
        let both = Neighbors::undirected(out, inc);
        (both.clone(), both)
    } else {
        (Neighbors::directed(out), Neighbors::directed(inc))
    }
}

async fn check() {
    for seed in 0..8_i64 {
        let mut edges = vec![(0, 0), (0, 1), (0, 1), (1, 3), (0, 2), (2, 3)];
        edges.extend((0..6).flat_map(|a| {
            (0..6).filter_map(move |b| ((a * 13 + b * 7 + seed) % 11 < 2).then_some((a, b)))
        }));
        for undirected in [false, true] {
            for delta in [false, true] {
                let (adj, rev) = graph(&edges, undirected, delta).await;
                // 65 requests cross the machine-word boundary and deliberately repeat sources.
                let requests: Vec<Request> = (0..65)
                    .map(|s| (s % 8, HashSet::from([s % 8, (s + 3) % 8, 99])))
                    .collect();
                for max in [0, 1, 2, 4, 8] {
                    let expected = experiment::run(&adj, &rev, &requests, max, "forward").await;
                    let actual = experiment::run(&adj, &rev, &requests, max, "batched").await;
                    for (expected, actual) in expected.iter().zip(&actual) {
                        assert_eq!(expected.0, actual.0);
                        let sorted = |p: &std::collections::HashMap<i64, Vec<i64>, _>| {
                            let mut p: Vec<_> = p
                                .iter()
                                .map(|(k, v)| {
                                    let mut v = v.clone();
                                    v.sort();
                                    (*k, v)
                                })
                                .collect();
                            p.sort();
                            p
                        };
                        assert_eq!(sorted(&expected.1), sorted(&actual.1));
                    }
                    assert_eq!(
                        experiment::paths(&expected, &requests, true).await,
                        experiment::paths(&actual, &requests, true).await
                    );
                    let masked = experiment::run(&adj, &rev, &requests, max, "masked").await;
                    assert_eq!(
                        experiment::paths(&expected, &requests, true).await,
                        experiment::paths(&masked, &requests, true).await
                    );
                    let pairs: Vec<Request> = requests
                        .iter()
                        .map(|(s, _)| (*s, HashSet::from([(*s + 3) % 8])))
                        .collect();
                    let current = experiment::run(&adj, &rev, &pairs, max, "current").await;
                    let batched = experiment::run(&adj, &rev, &pairs, max, "batched").await;
                    assert_eq!(
                        experiment::paths(&current, &pairs, true).await,
                        experiment::paths(&batched, &pairs, true).await
                    );
                }
            }
        }
    }
    // Different lanes finish at different levels; retain all ties at each stopping level.
    let (adj, rev) = graph(&[(0, 1), (0, 2), (1, 3), (2, 3), (3, 4)], false, false).await;
    let requests = vec![
        (0, HashSet::from([1, 2])),
        (0, HashSet::from([3, 4])),
        (3, HashSet::from([3, 4])),
        (4, HashSet::from([4])),
        (0, HashSet::new()),
    ];
    let expected = experiment::run(&adj, &rev, &requests, 8, "forward").await;
    for mode in ["batched", "masked"] {
        let actual = experiment::run(&adj, &rev, &requests, 8, mode).await;
        assert_eq!(
            experiment::paths(&expected, &requests, true).await,
            experiment::paths(&actual, &requests, true).await
        );
    }
    // Raw keys need not be dense, positive, or fit in 32 bits.
    let key = |n: i64| {
        if n % 2 == 0 {
            -100 * n - 3
        } else {
            (1_i64 << 45) + n
        }
    };
    let edges = [(0, 1), (0, 1), (1, 2), (0, 3), (3, 2)].map(|(a, b)| (key(a), key(b)));
    let (adj, rev) = graph(&edges, false, false).await;
    let requests = vec![
        (key(0), HashSet::from([key(2), key(4)])),
        (key(1), HashSet::from([key(2)])),
    ];
    let expected = experiment::run(&adj, &rev, &requests, 8, "current").await;
    for mode in ["batched", "masked", "parallel"] {
        let actual = experiment::run(&adj, &rev, &requests, 8, mode).await;
        assert_eq!(
            experiment::paths(&expected, &requests, true).await,
            experiment::paths(&actual, &requests, true).await
        );
    }
    println!("self-check passed: 160 graph/direction/delta/depth cases, 65 requests each; all-path multiplicity and bidirectional equivalence");
}

fn workload(
    name: &str,
    count: usize,
    targets: usize,
    n: i64,
) -> (Vec<(i64, i64)>, Vec<Request>, bool) {
    assert!(n >= 4096 && n % 512 == 0);
    let width = n / 8;
    let mut edges = Vec::new();
    match name {
        "hub" => {
            for i in 1..n {
                edges.extend([(i, 0), (0, i), (i, (i + 1) % n)]);
            }
        }
        "uniform" => {
            for i in 0..n {
                for j in 1..=8 {
                    edges.push((i, (i * 17 + j * 131) % n));
                }
            }
        }
        "chain" => {
            for i in 0..n - 1 {
                edges.push((i, i + 1));
            }
        }
        "tree" | "tree_near" => {
            for i in 1..n {
                edges.push(((i - 1) / 2, i));
            }
        }
        "disconnected" => {
            for i in 0..n {
                for j in 1..=4 {
                    edges.push((i, i / 64 * 64 + (i + j) % 64));
                }
            }
        }
        "layered" => {
            for i in 0..n - width {
                for j in 0..8 {
                    edges.push((i, (i / width + 1) * width + (i + j) % width));
                }
            }
        }
        _ => panic!("unknown workload"),
    }
    let requests = (0..count)
        .map(|i| {
            let source = match name {
                "chain" | "disconnected" => (i as i64 * 53) % (n - 32),
                "tree" | "tree_near" => n / 2 + i as i64 % (n / 2),
                _ => 1 + i as i64 % 256,
            };
            let ts = (0..targets)
                .map(|j| match name {
                    "chain" => source + 8 + j as i64,
                    "disconnected" => (source + 2048 + j as i64) % n,
                    "layered" => 7 * width + (source + j as i64 * 8) % width,
                    "tree" => 3 * n / 4 + (i as i64 + j as i64 * 31) % (n / 4),
                    "tree_near" => n / 2 + (i as i64 + 64 + j as i64 * 31) % 256,
                    _ => n - 1 - j as i64,
                })
                .collect();
            (source, ts)
        })
        .collect();
    (edges, requests, matches!(name, "tree" | "tree_near"))
}

#[tokio::main(worker_threads = 8)]
async fn main() {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).is_some_and(|s| s == "check") {
        check().await;
        return;
    }
    assert_eq!(args.len(), 5, "usage: multi_source_bfs <hub|uniform|chain|tree|tree_near|disconnected|layered> <sources> <targets/source> <current|forward|batched|masked|parallel>");
    let count = args[2].parse().unwrap();
    let targets = args[3].parse().unwrap();
    let mode = &args[4];
    let n = std::env::var("MSBFS_VERTICES").map_or(4096, |s| s.parse().unwrap());
    let (edges, requests, undirected) = workload(&args[1], count, targets, n);
    let (adj, rev) = graph(&edges, undirected, false).await;
    let mut samples = Vec::new();
    let mut reconstruction = Vec::new();
    // One warm-up, seven measured executions. Initialization is excluded.
    for repeat in 0..8 {
        let start = Instant::now();
        let results = experiment::run(&adj, &rev, &requests, 16, mode).await;
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        let start = Instant::now();
        let paths = experiment::paths(&results, &requests, false).await;
        std::hint::black_box(&paths);
        if repeat > 0 {
            samples.push(elapsed);
            reconstruction.push(start.elapsed().as_secs_f64() * 1000.0);
        }
    }
    // Profiling and reference evaluation occur after timings; use time -l only as whole-process RSS.
    experiment::profile(mode != "parallel");
    let results = experiment::run(&adj, &rev, &requests, 16, mode).await;
    let (scans, visits) = experiment::profile(false);
    if std::env::var_os("MSBFS_VERIFY").is_some() {
        let expected = experiment::run(&adj, &rev, &requests, 16, "current").await;
        for ((actual, reference), (_, ts)) in results.iter().zip(&expected).zip(&requests) {
            for target in ts {
                assert_eq!(actual.0.get(target), reference.0.get(target));
            }
        }
    }
    let checksum: u64 = results
        .iter()
        .zip(&requests)
        .flat_map(|(r, (_, ts))| {
            ts.iter()
                .map(|t| r.0.get(t).map_or(0, |d| u64::from(*d) + 1))
        })
        .sum();
    let output = experiment::paths(&results, &requests, false).await;
    for ((source, targets), paths) in requests
        .iter()
        .flat_map(|(s, ts)| {
            let mut ts: Vec<_> = ts.iter().copied().collect();
            ts.sort();
            ts.into_iter().map(move |t| (s, t))
        })
        .zip(&output)
    {
        for path in paths {
            assert_eq!(path.first(), Some(source));
            assert_eq!(path.last(), Some(&targets));
            for pair in path.windows(2) {
                let mut found = false;
                adj.for_each_neighbor(pair[0], |n| found |= n as i64 == pair[1]);
                assert!(found);
            }
        }
    }
    let visited: usize = results.iter().map(|r| r.0.len()).sum();
    let parents: usize = results
        .iter()
        .flat_map(|r| r.1.values())
        .map(Vec::len)
        .sum();
    let output_nodes: usize = output.iter().flatten().map(Vec::len).sum();
    samples.sort_by(f64::total_cmp);
    reconstruction.sort_by(f64::total_cmp);
    println!(
        "{},{},{},{},{n},{:.6},{:.6},{scans},{visits},{visited},{parents},{output_nodes},{checksum}",
        args[1], count, targets, mode, samples[3], reconstruction[3]
    );
}
