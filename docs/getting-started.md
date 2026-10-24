# Getting started

This page takes you from an empty environment to a working graph query. You'll install lance-csr, describe a
small graph, and query it with Cypher.

## Install

lance-csr is published on PyPI as `lance-csr`. Use [uv](https://docs.astral.sh/uv/):

=== "In a uv project"

    ```bash
    uv add lance-csr
    ```

=== "For a single script"

    ```bash
    uv run --with lance-csr python your_script.py
    ```

=== "Wheels only, never compile"

    ```bash
    uv add lance-csr --no-build-package lance-csr
    ```

Prebuilt wheels cover **Linux x86-64** (glibc 2.28+: RHEL 8, Ubuntu 20.04 and later) and **Apple Silicon**
(macOS 11+), for **CPython 3.12, 3.13 and 3.14**. Installing a wheel needs no Rust toolchain. Other platforms
build from the source distribution, which needs Rust and `protoc` (see [Contributing](contributing.md)).

The package depends on `pyarrow>=14` and `pylance` 13.x, which uv installs for you.

!!! note "Docs track the `main` branch"

    These pages describe the code on `main`. The `node_table` and `rel_table` builder methods arrived in
    0.2.0; on 0.1.0 use `with_node_label` and `with_relationship` (see
    [Graph schema](graph-schema.md#deprecated-builder-methods)).

Check the install:

```python
import lance_csr

print(lance_csr.__version__)
```

## Your first graph

A graph in lance-csr is a set of ordinary tables plus a `GraphConfig` that says which tables are nodes,
which are relationships, and which columns link them. Here are people, the cities they live in, and who
knows whom:

```python
import pyarrow as pa

datasets = {
    "Person": pa.table({
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Carol", "David"],
        "age": [28, 34, 29, 42],
    }),
    "City": pa.table({"id": [10, 20], "name": ["London", "Sydney"]}),
    "KNOWS": pa.table({"src": [1, 1, 2, 3], "dst": [2, 3, 4, 4]}),
    "LIVES_IN": pa.table({"src": [1, 2, 3, 4], "dst": [10, 10, 20, 20]}),
}
```

Each key of `datasets` is a node label or relationship type. Now describe how they connect:

```python
from lance_csr import GraphConfig

config = (
    GraphConfig.builder()
    .node_table("Person", "id")  # (1)!
    .node_table("City", "id")
    .rel_table("KNOWS", "Person", "Person", "src", "dst")  # (2)!
    .rel_table("LIVES_IN", "Person", "City", "src", "dst")
    .build()
)
```

1. A node table: the label `Person`, read from the dataset named `Person`, identified by its `id` column.
2. A relationship table: `KNOWS` edges go from `Person` to `Person`; the `src` column holds the source
   node's `id` and `dst` the target's.

## Run a query

`CypherEngine` registers the datasets once and runs as many queries as you like against them:

```python
from lance_csr import CypherEngine

engine = CypherEngine(config, datasets)

result = engine.execute("""
    MATCH (p:Person)-[:LIVES_IN]->(c:City {name: 'London'})
    RETURN p.name AS name, p.age AS age
    ORDER BY age
""")
print(result.to_pylist())
# [{'name': 'Alice', 'age': 28}, {'name': 'Bob', 'age': 34}]
```

Results come back as a [`pyarrow.Table`](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html),
so `to_pylist()`, `to_pydict()`, `to_pandas()` and Polars' `pl.from_arrow()` all work.

Pass values with `$parameters` instead of formatting them into the query string:

```python
older = engine.execute(
    "MATCH (p:Person) WHERE p.age >= $min_age RETURN p.name AS name ORDER BY name",
    {"min_age": 30},
)
print(older.to_pydict())  # {'name': ['Bob', 'David']}
```

## Go multi-hop with the CSR

Pass `use_csr=True` and the engine builds a CSR adjacency index for each relationship type. Multi-hop
patterns then become array lookups instead of joins:

```python
engine = CypherEngine(config, datasets, use_csr=True)

friends_of_friends = engine.execute("""
    MATCH (a:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(c:Person)
    RETURN DISTINCT c.name AS friend_of_friend
""")
print(friends_of_friends.to_pylist())  # [{'friend_of_friend': 'David'}]
```

!!! tip "When should I turn the CSR on?"

    Turn it on when your queries traverse relationships, and especially for two or more hops. Node keys and
    edge endpoints must be integer columns to benefit; anything else quietly uses joins. Read
    [The CSR index](csr.md) for the full picture.

## One-off queries

For a single query you don't plan to repeat, `CypherQuery` skips the engine:

```python
from lance_csr import CypherQuery

result = (
    CypherQuery("MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name ORDER BY name")
    .with_config(config)
    .execute(datasets)
)
print(result.to_pydict())  # {'name': ['Bob', 'David']}
```

`CypherQuery` rebuilds its context on every `execute()` call, so prefer `CypherEngine` whenever you run more
than one query against the same data.

## Next steps

- [Graph schema](graph-schema.md): every option of `GraphConfig`.
- [Querying with Cypher](cypher.md): parameters, streaming, `explain()` and the supported Cypher.
- [Lance datasets](lance.md): query data on disk without loading it, and pick up new writes with `refresh()`.
- The [`examples/`](https://github.com/hyperdimensionalcomputingai/lance-csr/tree/main/examples) directory has
  runnable scripts.
