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

## Comprehension counts

Compare `size([x IN list WHERE predicate])` with the equivalent identity-mapped comprehension, which keeps the
ordinary list-building path. Both queries run against 1,000 input rows with list lengths 16 and 256; results,
row count and summed counts must match before timing. This measures planning plus execution after parsing.

```bash
cargo bench -p lance-graph-benches --bench graph_execution -- comprehension_count --warm-up-time 1
```

The count path removes ordered list aggregation. It still numbers and unnests rows, evaluates predicates and
groups by input row, so these measurements do not establish a general bound on planning or memory use.
