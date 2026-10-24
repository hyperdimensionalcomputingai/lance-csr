# Project structure

lance-csr is a Cargo workspace with Python bindings. The Rust crates keep the `lance-graph*` names of the project
it was forked from.

```
.
├── Cargo.toml                    # Workspace: lance-graph, lance-graph-catalog, lance-graph-benches
├── crates/
│   ├── lance-graph/              # The engine: Cypher parser, semantic analysis, logical planner,
│   │                             # DataFusion planner, CSR index and traversal operators
│   ├── lance-graph-catalog/      # Graph source catalog and DirNamespace
│   ├── lance-graph-python/       # PyO3 bindings (built by maturin; outside the workspace)
│   └── lance-graph-benches/      # Criterion benchmarks (not published)
├── python/
│   ├── pyproject.toml            # The lance-csr package, built with maturin
│   ├── python/lance_csr/         # Python package that wraps the bindings
│   ├── python/tests/             # Python tests
│   └── benchmarks/               # Engine benchmarks (catalog reuse, bounded memory, CSR)
├── examples/                     # Runnable Python examples
├── ci/check_wheel.py             # Checks a built wheel in a clean environment, without Rust
└── docs/                         # Documentation
```

A query goes through four stages, all in `crates/lance-graph/src/`:

1. **Parse** (`parser.rs`) the Cypher text into an AST (`ast.rs`).
2. **Analyze** (`semantic.rs`) variables and scopes, and substitute parameters.
3. **Plan** (`logical_plan.rs`) graph operators: scans, expansions, variable-length paths, OPTIONAL MATCH,
   comprehensions and shortest paths.
4. **Build** (`datafusion_planner/`) a DataFusion plan from them, with CSR expansions (`csr_expand.rs`) and the
   shortest-path search (`shortest_path.rs`) as DataFusion extension operators.
