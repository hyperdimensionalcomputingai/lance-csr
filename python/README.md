# lance-csr

A fork of [lance-graph](https://github.com/lance-format/lance-graph): a Rust Cypher query engine over
Lance datasets and Arrow tables, built on Apache DataFusion, with Python bindings. Not affiliated with or
endorsed by the lance-graph maintainers.

## Installation

Add it to a [uv](https://docs.astral.sh/uv/) project:

```bash
uv add lance-csr
```

Or run a script with it, without a project:

```bash
uv run --with lance-csr python your_script.py
```

Prebuilt wheels are published for Linux x86-64 (glibc 2.28 or newer: RHEL 8, Ubuntu 20.04 and later)
and native Apple Silicon (macOS 11 or newer), for CPython 3.12 and later. Installing from a wheel needs
no Rust toolchain; to make sure uv never falls back to compiling from source, use:

```bash
uv add lance-csr --no-build-package lance-csr
```

Other platforms install from the source distribution, which needs Rust and `protoc`.

## Quick Start

### 1. Simple Cypher Query

```python
import pyarrow as pa
from lance_csr import CypherQuery, GraphConfig

cfg = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .node_table("City", "id")
    .rel_table("lives_in", "Person", "City", "src", "dst")
    .build()
)

datasets = {
    "Person": pa.table({"id": [1, 2], "name": ["Alice", "Bob"]}),
    "City": pa.table({"id": [10, 20], "name": ["London", "Sydney"]}),
    "lives_in": pa.table({"src": [1, 2], "dst": [10, 20]}),
}

query = """
    MATCH (p:Person)-[:lives_in]->(c:City)
    RETURN p.name, c.name
"""

result = CypherQuery(query).with_config(cfg).execute(datasets)
print(result.to_pylist())

[{'p.name': 'Alice', 'c.name': 'London'}, {'p.name': 'Bob', 'c.name': 'Sydney'}]
```

### 2. Multi-Query Execution with CypherEngine

For executing multiple queries against the same datasets, use `CypherEngine` to cache the catalog and achieve better performance:

```python
import pyarrow as pa
from lance_csr import CypherEngine, GraphConfig

cfg = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .node_table("City", "id")
    .rel_table("lives_in", "Person", "City", "src", "dst")
    .build()
)

datasets = {
    "Person": pa.table({"id": [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]}),
    "City": pa.table({"id": [10, 20], "name": ["London", "Sydney"]}),
    "lives_in": pa.table({"src": [1, 2], "dst": [10, 20]}),
}

# Create engine once - builds catalog
engine = CypherEngine(cfg, datasets)

# Execute multiple queries efficiently - catalog is reused
result1 = engine.execute("MATCH (p:Person) WHERE p.age > 25 RETURN p.name")
result2 = engine.execute("MATCH (p:Person)-[:lives_in]->(c:City) RETURN p.name, c.name")
result3 = engine.execute("MATCH (p:Person) RETURN count(*) as total")

print(result1.to_pylist())
# [{'p.name': 'Alice'}]

# $parameters, including datetime and date values
result4 = engine.execute(
    "MATCH (p:Person) WHERE p.age >= $min_age RETURN p.name", {"min_age": 30}
)
```

A node table maps a label to the dataset of the same name and its primary key. A
relationship table maps a type to its dataset, the node tables at its two ends
(`from_table` and `to_table`, like `FROM Person TO City` in Kuzu), and the columns that
hold each end's key (`src` and `dst`). Knowing the ends lets queries leave nodes
unlabeled where the relationship decides the label (`(p:Person)-[:lives_in]->(c)`), and
lets relationship-type alternation (`[:likes_post|likes_comment]`) pick the right table.

`with_node_label` and `with_relationship`, the names in lance-csr 0.1, still work but are
deprecated and will be removed in a later release.

### 3. Lance datasets: snapshots, refresh and streaming

`CypherEngine` reads Lance datasets lazily from storage (projections and filters are pushed into the
scan). Each dataset is pinned to the version you pass in; later writes are visible only after
`refresh()`. Use `execute_batches()` to stream large results instead of materializing a table.

```python
import lance
from lance_csr import CypherEngine, GraphConfig

config = GraphConfig.builder().node_table("Person", "id").build()
engine = CypherEngine(config, {"Person": lance.dataset("people.lance")})

engine.execute("MATCH (p:Person) RETURN count(*)")   # sees the pinned version
engine.refresh()                                       # move to the latest version
for batch in engine.execute_batches("MATCH (p:Person) RETURN p.id"):
    ...                                                # pyarrow.RecordBatch, one at a time
```

Experimental: `CypherEngine(config, datasets, use_csr=True)` answers single-hop patterns over integer ids
with an in-memory CSR adjacency, and fetches target rows by position instead of joining. The CSR is
rebuilt on `refresh()` when the relationship table changes, and the per-label id index when its node
table changes. Results are identical; peak memory grows by about 70 bytes per edge (both directions).

Write Lance data with `pylance` 13.x: the engine is built against Lance 13 and older or newer
majors may write files it cannot read.

### 4. Tables created with LanceDB

LanceDB tables are Lance datasets on disk. Pass the dataset behind each table with
`table.to_lance()`. Passing the LanceDB `Table` object itself raises a `TypeError`.

```python
import lancedb
from lance_csr import CypherEngine, GraphConfig

db = lancedb.connect("./mydb")
people, knows = db.open_table("Person"), db.open_table("KNOWS")

config = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("KNOWS", "Person", "Person", "src", "dst")
    .build()
)
engine = CypherEngine(config, {"Person": people.to_lance(), "KNOWS": knows.to_lance()})

knows.add(new_edges)   # keep writing with the LanceDB API
engine.refresh()       # the engine sees the new version
```

- Reads are lazy and pinned to the version you pass in, exactly as with `lance.dataset(...)`.
- `table.to_arrow()` also works, but loads the whole table into memory and never refreshes.
- Tested with LanceDB 0.40.0 on local tables, which write Lance file format 2.2. A LanceDB release that
  bundles a newer Lance major may write files this engine cannot read.
- Not supported: LanceDB Cloud (remote tables have no `to_lance()`), and tables opened through a
  namespace connection, which are rejected rather than reopened without their credentials.
  Object-store tables (S3 and similar) have not been tested.

### 5. Direct SQL Queries

For data analytics workflows where you prefer standard SQL over Cypher, use `SqlQuery` or `SqlEngine`. No `GraphConfig` is needed — write explicit JOINs against your tables directly:

```python
import pyarrow as pa
from lance_csr import SqlQuery, SqlEngine

person = pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Carol"],
    "age": [28, 34, 29],
})
knows = pa.table({"src_id": [1, 1, 2], "dst_id": [2, 3, 3]})
datasets = {"person": person, "knows": knows}

# One-off query
result = SqlQuery(
    "SELECT p.name, p.age FROM person p WHERE p.age > 30"
).execute(datasets)
print(result.to_pylist())
# [{'name': 'Bob', 'age': 34}]

# Multi-query with cached context
engine = SqlEngine(datasets)
r1 = engine.execute("SELECT COUNT(*) AS cnt FROM person")
r2 = engine.execute(
    "SELECT p1.name AS person, p2.name AS friend "
    "FROM person p1 "
    "JOIN knows k ON p1.id = k.src_id "
    "JOIN person p2 ON p2.id = k.dst_id"
)
```

## Requirements

- Python 3.12 or newer (3.12, 3.13 and 3.14 tested); macOS arm64 or Linux x86-64
- `pylance` 13.x for writing Lance datasets

## Contributing

Building from source needs Rust (through rustup), `protoc` and uv; see [DEVELOPMENT.md](./DEVELOPMENT.md).

```bash
cd python
make install    # uv sync --locked --no-install-project, then a debug build with maturin develop
make test       # pytest python/tests
make lint       # ruff and pyright
```

Re-run `make install` after changing Rust code; Python changes apply immediately.

## Repository layout

- `crates/lance-graph-python/src/` – PyO3 bridge that exposes graph APIs to Python
- `python/python/lance_csr/` – pure-Python wrapper and `__init__`
- `python/benchmarks/` – catalog-reuse and bounded-memory benchmarks
- `python/python/tests/` – graph-centric functional tests

For more information on development setup, building from source, running tests, and code quality guidelines, see [DEVELOPMENT.md](./DEVELOPMENT.md).

## License

Apache 2.0

## Links

- Upstream: [lance-format/lance-graph](https://github.com/lance-format/lance-graph)
