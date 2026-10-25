# Lance Graph Benchmarks

Performance benchmarks for the `lance-graph` crate.

## Running Benchmarks

From the repository root:

```bash
# Run all benchmarks
cargo bench -p lance-graph-benches

# Run specific benchmark
cargo bench -p lance-graph-benches --bench graph_execution
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
cargo bench -p lance-graph-benches --bench graph_execution -- query_templates --warm-up-time 1
```

For a stage breakdown on a separate fixed 1,000-row scan, run the optional core diagnostic:

```bash
cargo test --release -p lance-graph profile_query_planning_stages -- --ignored --nocapture
```

It reports elapsed means for 100 iterations of parsing, semantic analysis, graph logical planning,
DataFusion logical planning, optimization, physical planning, execution and cached binding. Physical timing
calls DataFusion's query planner directly so optimization is counted only once. It verifies 990 result rows
and their ID checksum. These local measurements do not replace the two SF1 suites required for performance
submissions; record runtime, hardware, revision and build profile with any reported numbers. The engine's
Python cache also includes dictionary conversion and lookup, which this Rust microbenchmark does not time.
