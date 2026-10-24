# lance-csr

A fork of [lance-graph](https://github.com/lance-format/lance-graph): a Cypher graph query engine over Lance
datasets, built in Rust on Apache DataFusion, with Python bindings. It adds a CSR adjacency index for fast
traversal, a lazy and refreshable engine over Lance snapshots, and much wider Cypher support. Not affiliated with
or endorsed by the lance-graph maintainers.

This repository contains:

- `crates/lance-graph` – the Cypher-capable query engine implemented in Rust
- `python/` – PyO3 bindings and Python packages:
  - `lance_csr` – thin wrapper around the Rust query engine

See `docs/project_structure.md` for the proposed workspace-based structure from
issue #92.

## Prerequisites

- Rust toolchain (1.82 or newer recommended)
- Python 3.12 or newer
- [`uv`](https://docs.astral.sh/uv/) available on your `PATH`

## Rust crate quick start

```bash
cd crates/lance-graph
cargo check
cargo test
```

## Install

```bash
pip install lance-csr
```

Prebuilt wheels cover Linux x86-64 and native Apple Silicon (CPython 3.12+); no Rust toolchain is needed.
See [`python/README.md`](python/README.md) for details and usage.

## Python package quick start (from source)

```bash
cd python
uv venv --python 3.12 .venv      # create the local virtualenv
source .venv/bin/activate         # activate the virtual environment
uv pip install 'maturin[patchelf]' # install build tool
uv pip install -e '.[tests]'     # editable install with test extras
maturin develop                   # build and install the Rust extension
pytest python/tests/ -v          # run the test suite
```

> If another virtual environment is already active, run `deactivate` (or
> `unset VIRTUAL_ENV`) before the `uv run` command so uv binds to `.venv`.

## Python example: Cypher query

```python
import pyarrow as pa
from lance_csr import CypherQuery, GraphConfig

people = pa.table({
    "person_id": [1, 2, 3, 4],
    "name": ["Alice", "Bob", "Carol", "David"],
    "age": [28, 34, 29, 42],
})

config = (
    GraphConfig.builder()
    .with_node_label("Person", "person_id")
    .build()
)

query = (
    CypherQuery("MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name, p.age AS age")
    .with_config(config)
)
result = query.execute({"Person": people})
print(result.to_pydict())  # {'name': ['Bob', 'David'], 'age': [34, 42]}
```

## Python example: Direct SQL query

For data analytics workflows where you prefer standard SQL, use `SqlQuery` or `SqlEngine`. No `GraphConfig` is needed:

```python
import pyarrow as pa
from lance_csr import SqlQuery, SqlEngine

person = pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Carol"],
    "age": [28, 34, 29],
})

# One-off query
result = SqlQuery(
    "SELECT name, age FROM person WHERE age > 30"
).execute({"person": person})
print(result.to_pydict())  # {'name': ['Bob'], 'age': [34]}

# Multi-query with cached context
engine = SqlEngine({"person": person})
r1 = engine.execute("SELECT COUNT(*) AS cnt FROM person")
r2 = engine.execute("SELECT name FROM person ORDER BY age DESC LIMIT 2")
```

## Benchmarks

- Requirements:
  - protoc: install `protobuf-compiler` (Debian/Ubuntu: `sudo apt-get install -y protobuf-compiler`).
  - Optional: gnuplot for Criterion's gnuplot backend; otherwise the plotters backend is used.

- Run (from `crates/lance-graph`):

```bash
cargo bench --bench graph_execution

# Quicker local run (shorter warm-up/measurement):
cargo bench --bench graph_execution -- --warm-up-time 1 --measurement-time 2 --sample-size 10
```

- Reports:
  - Global index: `crates/lance-graph/target/criterion/report/index.html`
  - Group index: `crates/lance-graph/target/criterion/cypher_execution/report/index.html`

- Typical results (x86_64, quick run: warm-up 1s, measurement 2s, sample size 10):

| Benchmark                | Size      | Median time | Approx. throughput |
|--------------------------|-----------|-------------|--------------------|
| basic_node_filter        | 100       | ~680 µs     | ~147 Kelem/s       |
| basic_node_filter        | 10,000    | ~715 µs     | ~13.98 Melem/s     |
| basic_node_filter        | 1,000,000 | ~743 µs     | ~1.35 Gelem/s      |
| single_hop_expand        | 100       | ~2.79 ms    | ~35.9 Kelem/s      |
| single_hop_expand        | 10,000    | ~3.77 ms    | ~2.65 Melem/s      |
| single_hop_expand        | 1,000,000 | ~3.70 ms    | ~270 Melem/s       |
| two_hop_expand           | 100       | ~4.52 ms    | ~22.1 Kelem/s      |
| two_hop_expand           | 10,000    | ~6.41 ms    | ~1.56 Melem/s      |
| two_hop_expand           | 1,000,000 | ~6.16 ms    | ~162 Melem/s       |

Numbers are illustrative; your hardware, compiler, and runtime load will affect results.

## External Wiki

For additional documentation, architecture, and examples, see the DeepWiki page: [DeepWiki — lance-graph](https://deepwiki.com/lancedb/lance-graph)
