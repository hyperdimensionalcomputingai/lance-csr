# Lance Graph Benchmarks

Performance benchmarks for the `lance-csr` crate.

## Running Benchmarks

From the repository root:

```bash
# Run all benchmarks
cargo bench -p lance-csr-benches

# Run specific benchmark
cargo bench -p lance-csr-benches --bench graph_execution
```

## Benchmarks

- **graph_execution**: End-to-end query execution benchmarks
  - Basic node filtering and projection
  - Single-hop relationship expansion
  - Two-hop relationship expansion
  - Tests with datasets of varying sizes (100, 10K, 1M rows)

## Note

This crate is not published to crates.io and is excluded from releases.
It exists solely for performance testing during development.


## Query template reuse

Measure fresh parsing/planning/execution against reuse of an unoptimized scalar-parameter template on the
same 10,000-row Arrow snapshot. Both paths optimize and build a physical plan for each execution. The harness
checks identical Arrow results, row count and ID checksum before timing; catalog construction is excluded.

```bash
cargo bench -p lance-csr-benches --bench graph_execution -- query_templates --warm-up-time 1
```

For a stage breakdown on a separate fixed 1,000-row scan, run the optional core diagnostic:

```bash
cargo test --release -p lance-csr profile_query_planning_stages -- --ignored --nocapture
```

It reports elapsed means for 100 iterations of parsing, semantic analysis, graph logical planning,
DataFusion logical planning, optimization, physical planning, execution and cached binding. Physical timing
calls DataFusion's query planner directly so optimization is counted only once. It verifies 990 result rows
and their ID checksum. These local measurements do not replace the two SF1 suites required for performance
submissions; record runtime, hardware, revision and build profile with any reported numbers. The engine's
Python cache also includes dictionary conversion and lookup, which this Rust microbenchmark does not time.


## Comprehension counts

Compare `size([x IN list WHERE predicate])` with the equivalent identity-mapped comprehension, which keeps the
ordinary list-building path. Both queries run against 1,000 input rows with list lengths 16 and 256; results,
row count and summed counts must match before timing. This measures planning plus execution after parsing.

```bash
cargo bench -p lance-csr-benches --bench graph_execution -- comprehension_count --warm-up-time 1
```

The count path removes ordered list aggregation. It still numbers and unnests rows, evaluates predicates and
groups by input row, so these measurements do not establish a general bound on planning or memory use.

## Grouped two-hop counts

`grouped_two_hop` compares grouped count queries with an equivalent `count(*) + 0` query that retains path
enumeration. Both use the same CSR engine, including its cached planning, and every result is checked before
timing. It covers middle and outer grouping by ids and repeated property values, balanced and skewed degrees,
same-type self-loop corrections, selective filters and a low-fan-out control. Inputs are generated deterministically
in the harness; no external dataset is needed.

```bash
cargo bench -p lance-csr-benches --bench graph_execution -- grouped_two_hop --warm-up-time 1 --measurement-time 1 --sample-size 10
```

These operator-focused comparisons supplement the unchanged SF1 suites required by `CONTRIBUTING.md`.
The reference can be faster on small or selective queries; report all cases rather than only improvements.

The [2026-11-12 comparison](results/grouped-two-hop-2026-11-12.md) records every SF1 query in both modes,
repeated main/branch runs, focused latency and memory measurements, and the selective-query safeguard.
The [profiling helper](scripts/grouped_two_hop.py) emits JSON for one generated case, including a separate
`EXPLAIN ANALYZE` through `GRAPH_TABLE` on the same engine. Its row metrics are rounded by DataFusion;
peak process RSS is captured before profiling and the reference-answer self-check.
