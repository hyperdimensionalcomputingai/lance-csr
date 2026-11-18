# Project structure

lance-csr is a Cargo workspace with Python bindings. The Rust crates are named `lance-csr*`; only the engine
crate, `lance-csr`, is published on crates.io.

```
.
├── Cargo.toml                    # Workspace: lance-csr, lance-csr-benches
├── crates/
│   ├── lance-csr/                # The engine: Cypher parser, semantic analysis, logical planner,
│   │                             # DataFusion planner, CSR index and traversal operators
│   ├── lance-csr-python/         # PyO3 bindings (built by maturin; outside the workspace)
│   └── lance-csr-benches/        # Criterion benchmarks (not published)
├── python/
│   ├── pyproject.toml            # The lance-csr package, built with maturin
│   ├── python/lance_csr/         # Python package that wraps the bindings
│   ├── python/tests/             # Python tests
│   └── benchmarks/               # Engine micro-benchmarks (see Benchmarks)
├── examples/                     # Runnable Python examples
├── ci/check_wheel.py             # Checks a built wheel in a clean environment, without Rust
└── docs/                         # Documentation
```

A query goes through five stages, all in `crates/lance-csr/src/`:

1. **Parse** (`parser.rs`) the Cypher text into an AST (`ast.rs`).
2. **Analyze** (`semantic.rs`) variables and scopes, and substitute parameters.
3. **Rewrite** (`pattern_order.rs`, `pattern_alternatives.rs`) each pattern to start at its most selective node,
   and split alternations and multi-label nodes into branches.
4. **Plan** (`logical_plan.rs`) graph operators: scans, expansions, variable-length paths, OPTIONAL MATCH,
   comprehensions and shortest paths.
5. **Build** (`datafusion_planner/`) a DataFusion plan from them, with graph operators such as CSR expansions
   (`csr_expand.rs`) and the shortest-path search (`shortest_path.rs`) as DataFusion extension operators.

[DataFusion internals](internals/index.md) describes each extension operator, optimizer rule and graph rewrite, with
a map of the source files.
