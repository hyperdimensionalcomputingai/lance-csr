# API reference

Everything below is importable from `lance_csr`. The classes are implemented in Rust (PyO3); this page is
written by hand from their signatures and docstrings, and every behaviour it states is checked against the
code. The guides link here for exact signatures and link back for examples.

| Class | Purpose |
| --- | --- |
| [`GraphConfig`](#graphconfig), [`GraphConfigBuilder`](#graphconfigbuilder) | Map tables to node labels and relationship types |
| [`CypherEngine`](#cypherengine) | Run many Cypher queries over registered datasets |
| [`CypherQuery`](#cypherquery) | Run one Cypher query |
| [`SqlEngine`](#sqlengine), [`SqlQuery`](#sqlquery) | Run SQL through DataFusion |
| [`VectorSearch`](#vectorsearch), [`DistanceMetric`](#distancemetric) | Vector similarity search and reranking |
| [`DirNamespace`](#dirnamespace) | Resolve table names to Lance datasets in a directory |
| [`ExecutionStrategy`](#executionstrategy), [`SqlDialect`](#sqldialect) | Enums |

`lance_csr.__version__` is the package version.

## Type checking

The package ships type stubs for every public class, including the compiled extension, and a `py.typed`
marker. Editors and type checkers can follow builder methods, parameters, query results and statistics.
The stubs describe the current API; they do not change runtime validation or exceptions.

PyArrow distributes its type information separately. For strict checks of Arrow inputs and results, add
`pyarrow-stubs` to your development dependencies:

```bash
uv add --dev pyright pyarrow-stubs
uv run pyright your_script.py
```

```python
from typing import assert_type

import pyarrow as pa
from lance_csr import CypherEngine, GraphConfig

config = GraphConfig.builder().node_table("Person", "id").build()
engine = CypherEngine(config, {"Person": pa.table({"id": [1, 2]})})
result = engine.execute("MATCH (p:Person) RETURN p.id ORDER BY p.id")
assert_type(result, pa.Table)
print(result.to_pylist())  # [{'p.id': 1}, {'p.id': 2}]
```

This reference remains maintained by hand alongside the stubs and Rust bindings.

## Datasets

Methods that take `datasets` take a `dict[str, object]` mapping each node label or relationship type (or SQL
table name) to one of:

- `lance.LanceDataset`: read lazily and version-pinned by `CypherEngine`; read into memory by the other
  classes;
- `pyarrow.Table`, `pyarrow.RecordBatch`, `pyarrow.RecordBatchReader`, or any object exporting the Arrow C
  stream interface (`__arrow_c_stream__`).

Keys match labels case-insensitively.

## Exceptions

| Exception | When |
| --- | --- |
| `ValueError` | Invalid query, incompatible known operand types (with expression and type details), unknown label, missing dataset or column, unsupported parameter type, empty `datasets` |
| `NotImplementedError` | A Cypher feature that parses but isn't supported |
| `RuntimeError` | Execution or I/O failures |
| `TypeError` | A dataset value of an unsupported type, such as a LanceDB `Table` |

---

## `GraphConfig`

An immutable mapping from tables to a property graph. Create one with `GraphConfig.builder()`.

`#!python GraphConfig.builder() -> GraphConfigBuilder` *(static)*
:   A new, empty builder.

`#!python node_labels() -> list[str]`
:   The configured node labels, lower-cased, in no particular order.

`#!python relationship_types() -> list[str]`
:   The configured relationship types, lower-cased, in no particular order.

## `GraphConfigBuilder`

`#!python GraphConfigBuilder()`
:   Same as `GraphConfig.builder()`. Every method returns a new builder.

`#!python node_table(label: str, primary_key: str) -> GraphConfigBuilder`
:   Nodes labelled `label`, one per row of the dataset named `label`, identified by the `primary_key`
    column.

`#!python rel_table(rel_type: str, from_table: str, to_table: str, src: str, dst: str) -> GraphConfigBuilder`
:   Edges of type `rel_type`, one per row of the dataset named `rel_type`, from `from_table` nodes to
    `to_table` nodes. `src` and `dst` hold the endpoints' primary keys.

`#!python build() -> GraphConfig`
:   Validates and returns the config. Raises `ValueError` if a relationship's endpoint label has no node
    table.

`#!python with_node_label(label: str, id_field: str)` *(deprecated)*
:   Use `node_table`. Emits `DeprecationWarning`.

`#!python with_relationship(rel_type: str, source_field: str, target_field: str, source_label: str | None = None, target_label: str | None = None)` *(deprecated)*
:   Use `rel_table`. Emits `DeprecationWarning`.

See [Graph schema](graph-schema.md).

---

## `CypherEngine`

<!-- skip-check -->
```python
CypherEngine(config: GraphConfig, datasets: dict, use_csr: bool = False)
```

Registers `datasets` once and reuses the catalog, DataFusion session and (with `use_csr=True`) CSR index
across queries. Safe to share between threads. `execute()` and `execute_batches()` reuse up to 128
unoptimized query templates per snapshot, keyed by exact text and scalar parameter names/types. Values are
bound before optimization on every call. Containers, `CALL` subqueries, parameterized `SKIP`/`LIMIT` and unsupported template
shapes use ordinary planning. `explain()` always plans directly; successful `refresh()` clears the cache.
See [Repeated queries](lance.md#repeated-queries) for the scope and limits.

| Parameter | Description |
| --- | --- |
| `config` | The graph mapping. |
| `datasets` | See [Datasets](#datasets). Lance datasets are pinned to the version passed in and not read here. |
| `use_csr` | Build the experimental [CSR index](csr.md) for relationship types with integer endpoints, and node indexes for labels with unique integer keys. Default `False`. |

Raises `ValueError` if `datasets` is empty, a configured key or endpoint column is missing, or a Lance dataset
was opened through a namespace client and can't be reopened by URI. Raises `RuntimeError` if a dataset can't
be opened.

`#!python execute(query: str, params: dict | None = None) -> pyarrow.Table`
:   Runs `query` and returns the whole result. `params` binds `$name` parameters: `int`, `float`, `str`,
    `bool`, `None`, lists, string-keyed nested maps, `datetime.datetime` and `datetime.date`.
    Integers must fit signed 64-bit values; nesting beyond 128 levels, cycles and unsupported values raise `ValueError`.
    Direct counts from `size()` of an unmapped list comprehension have a non-nullable Arrow field;
    see [Cypher functions](cypher.md#functions).

`#!python execute_batches(query: str, params: dict | None = None) -> pyarrow.RecordBatchReader`
:   Streams the result. Planning errors raise immediately; execution errors raise while reading. The reader
    keeps its snapshot alive across `refresh()`.

`#!python explain(query: str, params: dict | None = None) -> str`
:   The graph logical plan, DataFusion logical plan and physical plan.

`#!python refresh() -> None`
:   Moves every Lance dataset to its latest version and updates the CSR, node indexes and statistics.
    All-or-nothing; running queries keep their snapshot. If another refresh publishes first, retries from
    that snapshot so an older candidate cannot replace it. See [Lance datasets](lance.md#refresh).

`#!python dataset_versions() -> dict[str, int]`
:   The pinned version of each Lance dataset. PyArrow inputs are not listed.

`#!python csr_stats() -> dict[str, dict]`
:   Per relationship type with a CSR: `edges`, `delta_edges`, `tombstones`, `update`, `version`,
    `build_ms`. Empty without `use_csr=True`. See [Inspecting the CSR](csr.md#inspecting-the-csr).

`#!python node_index_stats() -> dict[str, dict]`
:   Per node label with a node index: `rows`, `version`, `build_ms`.

`#!python config() -> GraphConfig`
:   The engine's config.

`#!python execute_with_vector_rerank(query: str, vector_search: VectorSearch) -> pyarrow.Table`
:   Runs `query`, then ranks its rows with `vector_search`. See [Vector search](vector-search.md).

---

Cypher named paths can be returned as Arrow structs containing `length`, `nodes`, `node_records` and
`relationships`. They retain their values through `collect()` and `UNWIND`; `nodes(path)` returns integer
keys, and `relationships(path)` returns typed relationship records. `startNode(r)` and `endNode(r)` return
the stored endpoint node records. Their occurrence identities are query-local. See
[path values and their memory cost](cypher.md#path-values).

## `CypherQuery`

<!-- skip-check -->
```python
CypherQuery(query_text: str)
```

A parsed Cypher query. Raises `ValueError` if `query_text` doesn't parse. Builder methods return a new query.

`#!python with_config(config: GraphConfig) -> CypherQuery`
:   Sets the graph config. Required before executing.

`#!python with_parameter(key: str, value: object) -> CypherQuery`
:   Binds `$key` to `value`.

`#!python execute(datasets: dict, strategy: ExecutionStrategy | None = None) -> pyarrow.Table`
:   Runs the query. Datasets are read into memory for this call.

`#!python execute_with_namespace(namespace: DirNamespace, strategy: ExecutionStrategy | None = None) -> pyarrow.Table`
:   Runs the query, resolving each label to `<base_uri>/<label>.lance`.

`#!python explain(datasets: dict) -> str`
:   The plans, as for `CypherEngine.explain`.

`#!python to_sql(datasets: dict, dialect: SqlDialect | None = None) -> str`
:   The equivalent SQL, as joins. See [From Cypher to SQL](sql.md#from-cypher-to-sql).

`#!python execute_with_vector_rerank(datasets: dict, vector_search: VectorSearch) -> pyarrow.Table`
:   Runs the query, then reranks by vector similarity.

`#!python query_text() -> str`, `#!python parameters() -> dict`
:   The query text and the bound parameters.

`#!python variables() -> list[str]`, `#!python node_labels() -> list[str]`, `#!python relationship_types() -> list[str]`
:   Names referenced by the query, including independently scoped UNION branches. List ordering is not guaranteed.

---

## `SqlEngine`

<!-- skip-check -->
```python
SqlEngine(datasets: dict)
```

Registers `datasets` (names lower-cased) in a DataFusion session for repeated SQL queries. Raises
`ValueError` if `datasets` is empty.

`#!python execute(sql: str) -> pyarrow.Table`
:   Runs a SQL query.

## `SqlQuery`

<!-- skip-check -->
```python
SqlQuery(sql: str)
```

`#!python execute(datasets: dict) -> pyarrow.Table`
:   Registers `datasets` and runs the query.

`#!python explain(datasets: dict) -> str`
:   DataFusion's logical and physical plans.

`#!python sql() -> str`
:   The query text.

---

## `VectorSearch`

<!-- skip-check -->
```python
VectorSearch(column: str)
```

An immutable builder for vector similarity search. `column` is the vector column; for reranking a Cypher
result, use `alias.property` (`"d.embedding"`).

| Method | Default | Description |
| --- | --- | --- |
| `#!python query_vector(vector: list[float])` | required | The query vector |
| `#!python metric(metric: DistanceMetric)` | `DistanceMetric.L2` | Distance metric |
| `#!python top_k(k: int)` | `10` | Number of rows returned |
| `#!python include_distance(include: bool)` | `True` | Add the distance column |
| `#!python distance_column_name(name: str)` | `"_distance"` | Name of the distance column |
| `#!python use_lance_index(enabled: bool)` | `False` | Query a Lance ANN index first, then run the Cypher query on the top-k rows; can change results when the query also filters |

`#!python search(table: pyarrow.Table) -> pyarrow.Table`
:   Brute-force search, sorted by distance, at most `top_k` rows. Raises `ValueError` if no query vector is
    set or the column doesn't exist.

## `DistanceMetric`

| Member | Distance |
| --- | --- |
| `DistanceMetric.L2` | Euclidean |
| `DistanceMetric.Cosine` | `1 - cosine similarity` |
| `DistanceMetric.Dot` | Negative dot product |

---

## `DirNamespace`

<!-- skip-check -->
```python
DirNamespace(base_uri: str)
```

Resolves table names to Lance datasets at `<base_uri>/<name>.lance`, for
`CypherQuery.execute_with_namespace`.

`#!python base_uri -> str` *(property)*
:   The directory.

## `ExecutionStrategy`

| Member | Meaning |
| --- | --- |
| `ExecutionStrategy.DataFusion` | The default and only implemented strategy |
| `ExecutionStrategy.LanceNative` | Reserved; raises `NotImplementedError` |

## `SqlDialect`

`SqlDialect.Default` (DataFusion), `SqlDialect.Spark`, `SqlDialect.PostgreSql`, `SqlDialect.MySql`,
`SqlDialect.Sqlite`. Used by `CypherQuery.to_sql`.
