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
