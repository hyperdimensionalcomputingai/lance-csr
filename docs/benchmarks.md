# Benchmarks

!!! note "What these numbers are for"

    These numbers aren't meant to show that one engine is better or worse than another. They compare lance-csr
    with well-known graph systems on the same workloads, and they keep changing as every engine ships its own
    updates. We publish them with the code and evaluation harness that produced them so that anyone can
    reproduce them.

The benchmark runners and raw results live in two sibling repositories, next to the other engines' runners, so
every number on this page can be reproduced from there. lance-csr runs with `use_csr=True` in two modes: *in
memory* holds every table as Arrow in memory; *lazy* reads the Lance datasets from disk as queries need them (see
[Lance datasets](lance.md)). Neo4j runs as a server in a Docker VM; the other engines run embedded in the Python
process. All numbers were measured through the Python package; the Rust crate runs the same engine and has no
separate benchmark results.

The tables are generated from the repositories' committed results by `docs/sync_benchmarks.py`, which pins each
repository to a commit.

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
| In memory | 4.0× faster | 6.2× faster | 3.3× faster |
| Lazy (Lance on disk) | 1.8× faster | 2.8× faster | 1.3× faster |

<!-- /bench:ic:summary -->

??? info "Per-query mean latency (ms, lower is better, fastest in bold)"

    <!-- bench:ic:table -->

    | Query | lance-csr 0.3.1, in memory | lance-csr 0.3.1, lazy | Neo4j 2026.09.0 | Kuzu 0.11.3 | Ladybug 0.21.2 |
    | --- | ---: | ---: | ---: | ---: | ---: |
    | IC1 | 26.33 | 32.80 | 51.39 | **13.51** | n/a |
    | IC2 | **2.61** | 26.25 | 44.73 | 24.97 | 15.56 |
    | IC3 | **58.70** | 417.80 | 894.91 | 105.21 | 100.16 |
    | IC4 | **6.23** | 13.18 | 13.05 | 29.66 | 11.83 |
    | IC5 | **77.07** | 110.05 | 1,022.62 | 747.75 | 309.02 |
    | IC6 | **22.96** | 26.19 | 231.25 | 1,114.76 | 234.21 |
    | IC7 | 20.95 | 32.71 | **7.14** | 453.74 | 370.55 |
    | IC8 | **2.02** | 3.10 | 3.00 | 73.44 | 4.95 |
    | IC9 | **38.17** | 819.62 | 802.28 | 130.96 | 62.14 |
    | IC10 | 104.99 | 138.36 | 91.68 | 74.90 | **13.69** |
    | IC11 | 3.66 | 5.92 | 11.02 | 4.71 | **2.51** |
    | IC12 | **19.92** | 28.94 | 133.87 | 92.26 | 133.84 |
    | IC13 | 1.07 | 1.50 | **0.92** | 14.16 | 47.55 |
    | IC14 | **47.69** | 48.57 | 522.56 | 1,871.19 | n/a |

    <!-- /bench:ic:table -->

<!-- bench:ic:notes -->

- Run on Apple M5, 24 GB, macOS 26.6.2, from the benchmark repository at commit [`6a582ba`](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc-complex/tree/6a582bad2ab447777d6f010e61405e838645ba33). lance-csr was measured on 2026-10-27 on battery power and the other engines on 2026-10-24, on the same machine, with the repository's `run_benchmarks.sh` settings; see its README to reproduce the runs.
- Ratios are geometric means of the per-query ratios, over the queries both engines answered correctly.
- Ladybug 0.21.2: IC1 returned a wrong answer; IC14 failed (RuntimeError), so they are not timed.

<!-- /bench:ic:notes -->

<a id="ldbc-snb-sf1-30-query-workload"></a>

## GDC SNB SF1, 30-query workload

[GDC SF1 benchmark repository](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc) runs 30 custom Cypher
queries over the GDC Social Network Benchmark SF1 dataset (3.18M nodes, 17.3M relationships), with every result
checked against an expected answer. lance-csr's
[runner](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc/tree/main/lance_csr) runs the same
query text and Lance datasets as the lance-graph runner.

!!! warning "Independent, unaudited measurements"

    These are not official GDC Benchmark Results. They were measured on one machine; your hardware, data and
    queries will differ. Use the benchmark repository to reproduce them.

Over the 30 queries, lance-csr's speed relative to each engine:

<!-- bench:q30:summary -->

| lance-csr mode | vs Neo4j | vs Kuzu | vs Ladybug | vs lance-graph |
| --- | ---: | ---: | ---: | ---: |
| In memory | 3.5× faster | 4.0× faster | 2.5× faster | 4.5× faster |
| Lazy (Lance on disk) | 2.0× faster | 2.2× faster | 1.4× faster | 2.5× faster |

<!-- /bench:q30:summary -->

??? info "Per-query mean latency (ms, lower is better, fastest in bold)"

    <!-- bench:q30:table -->

    | Query | lance-csr 0.3.1, in memory | lance-csr 0.3.1, lazy | Neo4j 2026.09.0 | Kuzu 0.11.3 | Ladybug 0.21.2 | lance-graph 0.5.4 |
    | --- | ---: | ---: | ---: | ---: | ---: | ---: |
    | Q1 | **0.69** | 1.19 | 1.28 | 1.73 | 1.22 | 1.30 |
    | Q2 | **0.50** | 1.42 | 3.26 | 1.32 | 1.12 | 2.34 |
    | Q3 | 0.69 | 0.80 | **0.66** | 1.03 | 0.79 | 1.86 |
    | Q4 | 0.50 | 0.78 | 2.66 | 0.85 | **0.39** | 2.98 |
    | Q5 | **0.86** | 2.38 | 2.78 | 3.44 | 4.00 | 2.04 |
    | Q6 | **0.44** | 0.82 | 2.60 | 0.66 | 0.79 | 0.73 |
    | Q7 | 1.00 | 1.06 | **0.43** | 27.73 | 25.11 | 15.68 |
    | Q8 | **0.62** | 1.74 | 7.29 | 2.72 | 3.38 | 1.14 |
    | Q9 | 0.79 | 1.42 | **0.76** | 1.73 | 1.34 | 1.89 |
    | Q10 | **0.70** | 0.95 | 3.79 | 1.55 | 1.78 | 26.69 |
    | Q11 | **1.47** | 2.07 | 10.28 | 7.96 | 4.63 | 3.08 |
    | Q12 | **0.68** | 7.08 | 2.55 | 17.16 | 7.84 | 18.68 |
    | Q13 | **0.73** | 1.01 | 6.65 | 42.66 | 5.84 | 9.62 |
    | Q14 | 0.48 | 0.64 | **0.46** | 1.56 | 0.90 | 2.91 |
    | Q15 | **0.51** | 0.74 | 2.37 | 2.38 | 1.16 | 3.02 |
    | Q16 | **0.64** | 1.08 | 1.26 | 1.78 | 1.04 | 4.33 |
    | Q17 | **0.93** | 1.41 | 3.09 | 2.62 | 1.74 | 2.30 |
    | Q18 | **0.65** | 1.10 | 3.11 | 1.51 | 1.22 | 1.76 |
    | Q19 | **0.97** | 1.36 | 4.86 | 11.66 | 3.43 | 19.93 |
    | Q20 | **2.15** | 17.09 | 421.92 | 13.25 | 14.15 | 3.13 |
    | Q21 | 0.71 | 0.80 | 0.51 | **0.44** | 0.50 | 1.46 |
    | Q22 | **0.65** | 0.99 | 2.66 | 23.34 | 7.46 | 13.41 |
    | Q23 | **0.66** | 1.19 | 3.13 | 1.19 | 0.79 | 2.42 |
    | Q24 | **0.62** | 1.02 | 0.72 | 1.23 | 0.81 | 1.75 |
    | Q25 | **0.72** | 1.22 | 2.86 | 1.45 | 1.27 | 1.47 |
    | Q26 | 0.70 | 1.06 | **0.65** | 3.37 | 1.15 | 2.93 |
    | Q27 | **0.80** | 1.39 | 2.22 | 17.96 | 3.71 | 24.84 |
    | Q28 | **1.30** | 2.27 | 2.37 | 1.51 | 1.78 | 2.50 |
    | Q29 | **0.67** | 0.98 | 2.62 | 1.02 | 0.72 | 2.96 |
    | Q30 | 49.64 | 50.47 | 1,359.98 | 146.52 | 97.77 | **34.31** |

    <!-- /bench:q30:table -->

<!-- bench:q30:notes -->

- Run on Apple M5, 24 GB, macOS 26.6.2, from the benchmark repository at commit [`1fedd8b`](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc/tree/1fedd8bd213d071adf3a3c1c60c6da1c518ed121). lance-csr was measured on 2026-10-27 on battery power and the other engines on 2026-10-24, on the same machine, with the repository's `run_benchmarks.sh` settings; see its README to reproduce the runs.
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
| `csr_expand.py` | CSR build cost, single-hop latency against the join path, refresh cost and peak RSS, per edge count |
| `csr_delta.py` | `refresh()` with a delta against a full rebuild, after one append or delete commit (uses `csr_expand.py`'s data) |
| `csr_delta_accumulated.py` | Refresh cost as deltas accumulate over many small commits (the basis of the 25% rebuild threshold) |
| `engine_cache.py` | Engine construction, first-query, warm-query and refresh timings; can compare against upstream `lance_graph` |
| `bounded_memory.py` | Peak memory of streamed scans over Lance data larger than a memory cap (`generate`, then `run`) |

Each script's docstring lists its options. When you change performance-sensitive code, include before and
after numbers from the relevant script in your pull request.
