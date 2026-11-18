# Benchmarks

!!! note "What these numbers are for"

    These numbers aren't meant to show that one engine is better or worse than another. They compare lance-csr
    with well-known graph systems on the same workloads. Every engine here, lance-csr included, ships new
    releases often and independently of the others, so each comparison is a snapshot that moves with every
    release. Read the ratios as rough orders of magnitude rather than fixed facts: the summaries are rounded for
    that reason, and the per-query tables and pinned commits give the exact figures of the latest run. We publish
    them with the code and evaluation harness that produced them so that anyone can reproduce them.

The benchmark runners and raw results live in two sibling repositories, next to the other engines' runners, so
every number on this page can be reproduced from there. lance-csr runs with `use_csr=True` in two modes: *in
memory* holds every table as Arrow in memory; *lazy* reads the Lance datasets from disk as queries need them (see
[Lance datasets](lance.md)). Neo4j runs as a server in a Docker VM; the other engines run embedded in the Python
process. All numbers were measured through the Python package; the Rust crate runs the same engine and has no
separate benchmark results.

The tables are generated from the repositories' committed results by `docs/sync_benchmarks.py`, which pins each
repository to a commit. They are refreshed from time to time rather than for every release, and the benchmark
repositories record which version of each engine produced them.

The Graph Data Council (GDC), formerly known as the Linked Data Benchmark Council (LDBC), maintains the
Social Network Benchmark (SNB) used by both workloads below.

<a id="ldbc-snb-interactive-complex-queries"></a>

## GDC SNB Interactive complex queries

[GDC Interactive benchmark repository](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc-complex) runs the
14 complex read queries (IC1–IC14) of the GDC Social Network Benchmark Interactive v1 workload over the SF1
dataset, with one fixed parameter set per query and every result checked against a golden answer. lance-csr's
[runner](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc-complex/tree/main/lance_csr) runs
Neo4j's Cypher text for IC1–IC13 unchanged; IC14 replaces `reduce()`, which openCypher leaves out, with pattern
comprehensions.

!!! warning "Independent, unaudited measurements"

    These are not official GDC Benchmark Results. They were measured on one machine; your hardware, data and
    queries will differ. Use the benchmark repository to reproduce them.

Over the 14 queries, lance-csr's speed relative to each engine:

<!-- bench:ic:summary -->

| lance-csr mode | vs Neo4j | vs Kuzu | vs Ladybug |
| --- | ---: | ---: | ---: |
| In memory | ~4.5× faster | ~7.5× faster | ~1.8× faster |
| Lazy (Lance on disk) | ~2.5× faster | ~3.5× faster | ~1.1× slower |

<!-- /bench:ic:summary -->

??? info "Per-query mean latency (ms, lower is better, fastest in bold)"

    <!-- bench:ic:table -->

    | Query | lance-csr, in memory | lance-csr, lazy | Neo4j | Kuzu | Ladybug |
    | --- | ---: | ---: | ---: | ---: | ---: |
    | IC1 | 24.53 | 34.42 | 51.39 | **13.51** | 36.79 |
    | IC2 | **2.46** | 25.06 | 44.73 | 24.97 | 6.09 |
    | IC3 | **25.01** | 163.58 | 894.91 | 105.21 | 116.33 |
    | IC4 | 5.84 | 13.19 | 13.05 | 29.66 | **5.58** |
    | IC5 | **72.08** | 94.56 | 1,022.62 | 747.75 | 262.98 |
    | IC6 | 19.79 | 24.27 | 231.25 | 1,114.76 | **9.65** |
    | IC7 | 9.76 | 14.02 | **7.14** | 453.74 | 304.75 |
    | IC8 | 2.28 | 3.30 | 3.00 | 73.44 | **0.87** |
    | IC9 | **35.63** | 232.40 | 802.28 | 130.96 | 57.12 |
    | IC10 | 20.75 | 30.02 | 91.68 | 74.90 | **11.55** |
    | IC11 | 2.93 | 5.54 | 11.02 | 4.71 | **1.69** |
    | IC12 | **15.93** | 20.12 | 133.87 | 92.26 | 22.75 |
    | IC13 | 1.07 | 1.42 | **0.92** | 14.16 | 48.49 |
    | IC14 | 261.27 | 264.95 | 522.56 | 1,871.19 | **87.96** |

    <!-- /bench:ic:table -->

<!-- bench:ic:notes -->

- Run on Apple M5, 24 GB, macOS 26.6.2, from the benchmark repository at commit [`a332324`](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc-complex/tree/a332324f2d4660b03d1d429d8ef99767af3981e5). lance-csr was measured on 2026-11-15 and the other engines between 2026-10-24 and 2026-11-01, on the same machine, with the repository's `run_benchmarks.sh` settings; see its README to reproduce the runs. The engine versions measured are recorded there too.
- Ratios are geometric means of the per-query ratios, over the queries both engines answered correctly.

<!-- /bench:ic:notes -->

<a id="ldbc-snb-sf1-30-query-workload"></a>

## GDC SNB SF1, 30-query workload

[GDC SF1 benchmark repository](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc) runs 30 custom Cypher
queries over the GDC Social Network Benchmark SF1 dataset (3.18M nodes, 17.3M relationships), with every result
checked against an expected answer. lance-csr's
[runner](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc/tree/main/lance_csr) runs the same
query text and Lance datasets as the [lance-graph](https://github.com/lance-format/lance-graph) runner.

!!! warning "Independent, unaudited measurements"

    These are not official GDC Benchmark Results. They were measured on one machine; your hardware, data and
    queries will differ. Use the benchmark repository to reproduce them.

Over the 30 queries, lance-csr's speed relative to each engine:

<!-- bench:q30:summary -->

| lance-csr mode | vs Neo4j | vs Kuzu | vs Ladybug | vs lance-graph |
| --- | ---: | ---: | ---: | ---: |
| In memory | ~3.5× faster | ~4× faster | ~1.3× faster | ~4.5× faster |
| Lazy (Lance on disk) | ~2× faster | ~2.5× faster | ~1.3× slower | ~2.5× faster |

<!-- /bench:q30:summary -->

??? info "Per-query mean latency (ms, lower is better, fastest in bold)"

    <!-- bench:q30:table -->

    | Query | lance-csr, in memory | lance-csr, lazy | Neo4j | Kuzu | Ladybug | lance-graph |
    | --- | ---: | ---: | ---: | ---: | ---: | ---: |
    | Q1 | 0.66 | 1.15 | 1.28 | 1.73 | **0.63** | 1.30 |
    | Q2 | **0.54** | 1.40 | 3.26 | 1.32 | 0.66 | 2.34 |
    | Q3 | 0.67 | 0.81 | 0.66 | 1.03 | **0.40** | 1.86 |
    | Q4 | 0.56 | 0.79 | 2.66 | 0.85 | **0.34** | 2.98 |
    | Q5 | **0.84** | 2.25 | 2.78 | 3.44 | 3.96 | 2.04 |
    | Q6 | **0.43** | 0.74 | 2.60 | 0.66 | 0.56 | 0.73 |
    | Q7 | 1.02 | 1.08 | **0.43** | 27.73 | 5.59 | 15.68 |
    | Q8 | **0.63** | 1.75 | 7.29 | 2.72 | 0.99 | 1.14 |
    | Q9 | 0.82 | 1.32 | **0.76** | 1.73 | 1.30 | 1.89 |
    | Q10 | **0.72** | 0.93 | 3.79 | 1.55 | 1.41 | 26.69 |
    | Q11 | 1.51 | 1.94 | 10.28 | 7.96 | **1.44** | 3.08 |
    | Q12 | **0.74** | 6.94 | 2.55 | 17.16 | 1.76 | 18.68 |
    | Q13 | **0.78** | 1.05 | 6.65 | 42.66 | 1.11 | 9.62 |
    | Q14 | 0.53 | 0.69 | **0.46** | 1.56 | 0.56 | 2.91 |
    | Q15 | **0.56** | 0.77 | 2.37 | 2.38 | 0.79 | 3.02 |
    | Q16 | **0.66** | 1.09 | 1.26 | 1.78 | 0.76 | 4.33 |
    | Q17 | **0.88** | 1.39 | 3.09 | 2.62 | 1.20 | 2.30 |
    | Q18 | **0.69** | 1.15 | 3.11 | 1.51 | 1.00 | 1.76 |
    | Q19 | **0.93** | 1.34 | 4.86 | 11.66 | 1.48 | 19.93 |
    | Q20 | 2.24 | 17.02 | 421.92 | 13.25 | **0.63** | 3.13 |
    | Q21 | 0.77 | 0.82 | 0.51 | 0.44 | **0.43** | 1.46 |
    | Q22 | **0.71** | 0.94 | 2.66 | 23.34 | 1.55 | 13.41 |
    | Q23 | 0.73 | 1.25 | 3.13 | 1.19 | **0.64** | 2.42 |
    | Q24 | 0.69 | 1.07 | 0.72 | 1.23 | **0.63** | 1.75 |
    | Q25 | 0.80 | 1.18 | 2.86 | 1.45 | **0.79** | 1.47 |
    | Q26 | 0.74 | 1.07 | **0.65** | 3.37 | 0.72 | 2.93 |
    | Q27 | **0.86** | 1.31 | 2.22 | 17.96 | 1.92 | 24.84 |
    | Q28 | **1.33** | 1.99 | 2.37 | 1.51 | 1.51 | 2.50 |
    | Q29 | 0.69 | 1.02 | 2.62 | 1.02 | **0.52** | 2.96 |
    | Q30 | **20.30** | 20.61 | 1,359.98 | 146.52 | 43.09 | 34.31 |

    <!-- /bench:q30:table -->

<!-- bench:q30:notes -->

- Run on Apple M5, 24 GB, macOS 26.6.2, from the benchmark repository at commit [`1998520`](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc/tree/199852076ba8dceb345e18eeb2f8ac04c73f3ea1). lance-csr was measured on 2026-11-15 and the other engines between 2026-10-24 and 2026-11-01, on the same machine, with the repository's `run_benchmarks.sh` settings; see its README to reproduce the runs. The engine versions measured are recorded there too.
- Ratios are geometric means of the per-query ratios, over the queries both engines answered correctly.

<!-- /bench:q30:notes -->

## Engine micro-benchmarks

`python/benchmarks/` holds scripts that measure the engine's own mechanics. Build a release extension first
(debug builds are much slower), then run them from `python/`:

```bash
cd python
uv run --no-sync maturin develop --release
uv run --no-sync python benchmarks/csr_expand.py --data /tmp/csr-bench --edges 100000 1000000
```

| Script | Measures |
| --- | --- |
| `csr_expand.py` | CSR build cost, single-hop latency against the join path, `checkout_latest()` cost and peak RSS, per edge count |
| `named_hop_columns.py` | A named hop that returns a relationship column, against the join path, as the number of start nodes grows; a fixed-width and a string column |
| `csr_delta.py` | `checkout_latest()` with a delta against a full rebuild, after one append or delete commit (uses `csr_expand.py`'s data) |
| `csr_delta_accumulated.py` | `checkout_latest()` cost as deltas accumulate over many small commits (the basis of the 25% rebuild threshold) |
| `csr_persist.py` | Engine start-up with a stored CSR (`persist_csr=True`) against building it, including a delta on top of the stored version |
| `shortest_path_batch.py` | Latency and peak memory of shortest paths between many bound endpoint pairs, in memory and over Lance |
| `engine_cache.py` | Engine construction, first-query, warm-query and `checkout_latest()` timings; can compare against upstream `lance_graph` |
| `bounded_memory.py` | Peak memory of streamed scans over Lance data larger than a memory cap (`generate`, then `run`) |

Each script's docstring lists its options. When you change performance-sensitive code, include before and
after numbers from the relevant script in your pull request.
