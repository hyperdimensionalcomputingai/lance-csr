# Benchmarks

## LDBC SNB SF1

lance-csr is benchmarked with
[graph-benchmark-ldbc](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc): 30 Cypher queries
over the LDBC Social Network Benchmark SF1 dataset (3.18M nodes, 17.3M relationships), with every result checked
against an expected answer.

!!! warning "Independent, unaudited measurements"

    These are not official LDBC Benchmark Results. They were measured on one machine; your hardware, data and
    queries will differ. Use the benchmark repository to reproduce them.

Over the 30 queries (geometric mean of the per-query ratios):

| lance-csr mode | vs Neo4j | vs Kùzu | vs Ladybug | vs lance-graph |
| --- | ---: | ---: | ---: | ---: |
| In memory | 5.1× faster | 4.4× faster | 2.5× faster | 4.6× faster |
| Lazy (Lance on disk) | 3.0× faster | 2.6× faster | 1.5× faster | 2.7× faster |

Both modes run with `use_csr=True`. *In memory* holds every table as Arrow in memory; *lazy* reads the Lance
datasets from disk as queries need them (see [Lance datasets](lance.md)).

??? info "Per-query mean latency (ms, lower is better, fastest in bold)"

    Apple M5 MacBook Pro, macOS 26.6.

    | Query | lance-csr 0.1.0, in memory | lance-csr 0.1.0, lazy | Neo4j 2026.09.0 | Kuzu 0.11.3 | Ladybug 0.21.2 | lance-graph 0.5.4 |
    | --- | ---: | ---: | ---: | ---: | ---: | ---: |
    | Q1 | **0.64** | 1.34 | 3.14 | 3.25 | 1.00 | 1.40 |
    | Q2 | **0.55** | 1.45 | 4.66 | 1.58 | 1.17 | 2.29 |
    | Q3 | **0.66** | 0.80 | 1.59 | 1.61 | 0.78 | 1.95 |
    | Q4 | 0.52 | 0.81 | 3.11 | 1.07 | **0.37** | 3.02 |
    | Q5 | **0.79** | 2.15 | 3.51 | 3.99 | 3.98 | 2.04 |
    | Q6 | **0.45** | 0.72 | 2.52 | 1.04 | 0.79 | 0.72 |
    | Q7 | 1.07 | 1.08 | **0.91** | 28.76 | 25.46 | 16.59 |
    | Q8 | **0.66** | 1.77 | 13.51 | 2.80 | 3.52 | 1.61 |
    | Q9 | **0.84** | 1.36 | 1.62 | 1.91 | 1.33 | 2.33 |
    | Q10 | **0.75** | 0.96 | 5.11 | 1.60 | 1.56 | 32.58 |
    | Q11 | **1.50** | 1.98 | 14.94 | 8.93 | 4.08 | 3.27 |
    | Q12 | **0.72** | 6.81 | 3.71 | 21.03 | 8.10 | 21.67 |
    | Q13 | **0.76** | 1.01 | 9.45 | 44.76 | 5.70 | 9.79 |
    | Q14 | **0.50** | 0.64 | 0.83 | 1.76 | 0.89 | 2.50 |
    | Q15 | **0.53** | 0.71 | 2.71 | 3.03 | 1.10 | 2.17 |
    | Q16 | **0.66** | 1.08 | 1.45 | 2.05 | 1.00 | 4.19 |
    | Q17 | **0.93** | 1.40 | 3.61 | 3.16 | 1.72 | 2.39 |
    | Q18 | **0.66** | 1.06 | 3.72 | 1.48 | 1.20 | 1.89 |
    | Q19 | **0.96** | 1.33 | 6.82 | 16.76 | 3.15 | 20.96 |
    | Q20 | **2.05** | 16.54 | 478.58 | 11.64 | 13.61 | 2.98 |
    | Q21 | 0.72 | 0.80 | 1.00 | **0.43** | 0.52 | 1.51 |
    | Q22 | **0.68** | 0.88 | 3.14 | 28.00 | 7.88 | 14.27 |
    | Q23 | **0.70** | 1.22 | 4.78 | 1.17 | 0.73 | 2.47 |
    | Q24 | **0.66** | 1.03 | 1.16 | 1.16 | 0.79 | 1.79 |
    | Q25 | **0.76** | 1.14 | 3.74 | 1.43 | 1.37 | 1.35 |
    | Q26 | **0.71** | 1.02 | 1.18 | 3.46 | 1.15 | 2.88 |
    | Q27 | **0.82** | 1.26 | 2.77 | 14.32 | 4.50 | 24.70 |
    | Q28 | **1.29** | 2.25 | 4.12 | 1.48 | 2.32 | 2.50 |
    | Q29 | **0.65** | 0.98 | 3.36 | 1.00 | 0.76 | 2.73 |
    | Q30 | **36.17** | 37.09 | 1,255.75 | 149.55 | 92.89 | 38.17 |

Methodology notes:

- Neo4j, Kùzu, Ladybug and lance-graph numbers are from graph-benchmark-ldbc's 2026-10-16 run. lance-csr 0.1.0
  was measured on 2026-10-23 on the same machine with the same pytest-benchmark settings (the faster of two
  passes); its runner is not yet part of that repository.
- Neo4j runs as a server in a Docker VM; the other engines run embedded in the Python process.
- See the benchmark repository for each engine's configuration and how to reproduce the runs.

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
