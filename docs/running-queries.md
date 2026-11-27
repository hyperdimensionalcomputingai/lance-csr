# Running queries

This page covers how to run, stream, time-bound, inspect and tune Cypher queries in Python. For the Cypher
clauses, patterns and functions the engine supports, see the [Cypher reference](cypher.md).

There are two entry points, and most code wants the first:

| | `CypherEngine` | `CypherQuery` |
| --- | --- | --- |
| Use it for | Many queries over the same data | One query, once, or exporting SQL |
| Setup cost | Paid once, in the constructor | Paid on every `execute()` |
| Lance datasets | Read lazily, pinned to a version, `checkout_latest()` | Read into memory |
| CSR index | `use_csr=True` | Not available |
| Streaming results | `execute_batches()` | No |

A query may end with one optional semicolon (whitespace may follow it); each call runs one query, so repeated
semicolons or multiple statements are rejected.

The examples on this page share this graph:

```python
import pyarrow as pa
from lance_csr import CypherEngine, GraphConfig

config = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .node_table("City", "id")
    .rel_table("KNOWS", "Person", "Person", "src", "dst")
    .rel_table("LIVES_IN", "Person", "City", "src", "dst")
    .build()
)
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
engine = CypherEngine(config, datasets, use_csr=True)
```

## `CypherEngine`

<!-- skip-check -->
```python
CypherEngine(config, datasets, use_csr=False, *, target_partitions=None, persist_csr=False)
```

The constructor validates `datasets` against `config`, registers them with a DataFusion session, and with
`use_csr=True` builds the [CSR index](csr.md). Every query after that reuses the same catalog, indexes and
session.

!!! tip "Share one engine across threads"

    An engine is safe to share across Python threads: queries read from an immutable snapshot, and the GIL is
    released while they execute. Build it once at startup rather than per request.

### `execute(query, params=None)`

Runs the query to completion and returns a `pyarrow.Table`:

```python
table = engine.execute("""
    MATCH (p:Person)-[:LIVES_IN]->(c:City)
    RETURN c.name AS city, collect(p.name) AS people, avg(p.age) AS avg_age
    ORDER BY city
""")
print(table.to_pylist())
# [{'city': 'London', 'people': ['Alice', 'Bob'], 'avg_age': 31.0},
#  {'city': 'Sydney', 'people': ['Carol', 'David'], 'avg_age': 35.5}]
```

Output columns take their `AS` alias, or the expression text (`p.name`) when there is none.

### Parameters

Write `$name` in the query and pass a dict as the second argument. Parameterizing values keeps queries safe from
string-formatting bugs and lets the engine reuse [cached plan templates](#repeated-queries).

```python
import datetime

print(
    engine.execute(
        "MATCH (p:Person) WHERE p.age >= $min_age AND p.name <> $skip RETURN p.name AS name",
        {"min_age": 30, "skip": "Bob"},
    ).to_pylist()
)  # [{'name': 'David'}]
```

Parameters accept numbers, strings, booleans, `None`, lists, string-keyed maps, and `datetime.datetime` or
`datetime.date` values. Lists work with `IN`, strings work with `STARTS WITH` / `ENDS WITH` / `CONTAINS`, and
non-negative integers work in `SKIP` and `LIMIT`:

```python
print(
    engine.execute(
        "MATCH (p:Person) WHERE p.id IN $ids RETURN p.name AS name ORDER BY name",
        {"ids": [1, 3]},
    ).to_pylist()
)  # [{'name': 'Alice'}, {'name': 'Carol'}]

print(
    engine.execute(
        "MATCH (p:Person) WHERE p.name STARTS WITH $prefix RETURN p.name AS name",
        {"prefix": "Ali"},
    ).to_pylist()
)  # [{'name': 'Alice'}]
```

Parameters bind values, not identifiers: `p.name = $name` and `(p:Person {id: $id})` are valid; `p.$property` is
not.

??? note "Parameter types, coercion and limits"

    - **Integers** must fit signed 64-bit values; lists of integers preserve signed 64-bit IDs.
    - **Maps** require string keys and may contain nested lists, maps and nulls, up to 128 levels of nesting.
      Cyclic containers and unsupported Python objects raise `ValueError`.
    - **List coercion:** Arrow lists hold one element type, so mixed string/numeric lists coerce to strings, as
      with literal lists. Inline node/relationship property maps require scalar parameters.
    - **Dates and timestamps:** `datetime.datetime` compares against timestamp columns, and `datetime.date`
      against date columns.
    - **Text matching:** `CONTAINS`, `STARTS WITH` and `ENDS WITH` are case-sensitive (use `toLower()` on both
      sides for case-insensitive matching), and `%`, `_` and `\` are literal characters. An empty search string
      matches all non-null strings; a null or non-string operand evaluates to null. Use
      `($name IS NULL OR p.name CONTAINS $name)` for an optional filter.
    - **Pagination:** `SKIP $n`, `RETURN ... LIMIT $take` and `WITH ... LIMIT $take` accept non-negative
      integer parameters. Missing, null, boolean, fractional, negative or overflowing counts raise `ValueError`;
      arbitrary expressions and `WITH SKIP` are unsupported.

### Streaming with `execute_batches()` { #streaming }

`execute()` holds the whole result table in memory. For large outputs, `execute_batches(query, params=None)`
returns a `pyarrow.RecordBatchReader` that computes batches as you read them:

```python
reader = engine.execute_batches("MATCH (p:Person) RETURN p.id AS id, p.name AS name")
rows = 0
for batch in reader:  # each batch is a pyarrow.RecordBatch
    rows += batch.num_rows
print(rows)  # 4
```

- **Bounded streaming:** scans, filters, projections and CSR expansions yield a batch at a time, even when one
  node has many neighbours. Joins, sorts and aggregations may still buffer their inputs; large named-relationship
  hops over Lance may read fixed-width edge columns whole (see
  [Relationship variables](csr.md#relationship-variables)).
- **Fail-fast planning:** syntax and planning errors raise when `execute_batches()` is called; execution errors
  raise while iterating.
- **Snapshot isolation:** the reader holds onto the snapshot it started with, so a concurrent
  [`checkout_latest()`](lance.md#checkout-latest) doesn't affect it.
- **Early stop:** closing or dropping the reader cancels any remaining work.

### Query deadlines { #query-deadlines }

Pass `timeout=5.0` (seconds) to `execute`, `execute_batches`, `CypherQuery.execute` or `SqlEngine.execute` to set
one deadline for planning, execution and—for streams—pauses between reads. The default is `None` (no deadline).

```python
try:
    engine.execute("RETURN 1 AS n", timeout=0)
except TimeoutError:
    print("Query timeout exceeded")  # Query timeout exceeded
```

If the deadline expires before a reader is returned, the call raises `TimeoutError`. If it expires while iterating
a reader, the next read raises `pyarrow.ArrowInvalid` containing `Query timeout exceeded`; any batches you have
already read remain valid. Close readers when you stop consuming them.

Cancellation is cooperative: traversal loops, cross joins and nested-loop joins check the deadline regularly
(even when no rows match). See [Timeouts](api-reference.md#timeouts) in the API reference for validation rules
and limits, and [Timeouts](rust.md#timeouts) for the Rust equivalent (`with_execution_deadline`).

### Inspecting plans with `explain()` { #explain }

`explain(query, params=None, *, analyze=False)` prints the graph logical plan, DataFusion's logical plan and the
physical plan as a table. It is the quickest way to confirm that a hop used the CSR (`CsrExpandExec`) instead of
a hash join (`HashJoinExec`), and that filters reached the scan:

```python
plan = engine.explain("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN b.name")
print("CsrExpandExec" in plan)  # True
```

A plan shows what *will* run, not where the time goes. Pass `analyze=True` (like SQL's `EXPLAIN ANALYZE`) to run
the query, discard the rows, and attach each physical operator's runtime metrics under
`physical_plan_with_metrics`:

```python
analyzed = engine.explain(
    "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN b.name",
    analyze=True,
)
expand = next(line for line in analyzed.splitlines() if "CsrExpandExec" in line)
print("output_rows=" in expand)  # True
```

Reading the `physical_plan_with_metrics` section from bottom to top shows how rows flow and where time is spent
(timings vary by run):

```text
ProjectionExec: expr=[b__name@3 as b.name], metrics=[output_rows=2, elapsed_compute=1.2µs, ...]
  CsrExpandExec: key=a__id, edges=4, take=b[b__name], metrics=[output_rows=2, elapsed_compute=1.5ms, ...]
    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, metrics=[output_rows=1, ...]
      ProjectionExec: expr=[id@0 as a__id], metrics=[output_rows=1, ...]
        FilterExec: name@1 = Alice, projection=[id@0], metrics=[output_rows=1, selectivity=25% (1/4), ...]
          KeyStatisticsExec: distinct=[id=4], metrics=[output_rows=4, ...]
            DataSourceExec: partitions=1, partition_sizes=[1], metrics=[]
```

- `FilterExec` narrows `Person` from 4 rows to 1 (`selectivity=25% (1/4)`) *before* the hop runs.
- `CsrExpandExec` replaces two hash joins: `key=a__id` looks up Alice's neighbours in the CSR, `take=b[b__name]`
  fetches only `b.name` by row position, and `output_rows=2` confirms two neighbours matched. `elapsed_compute`
  reports each operator's own time, excluding operators below it.

`explain(..., analyze=True)` plans through the same path as `execute()`, so its metrics reflect what `execute()`
actually runs. A query that fails at runtime raises `RuntimeError`. Lines longer than 1,000 characters (for
example, a long list or vector parameter) extend past the table's right border rather than widening every row.

### Repeated queries { #repeated-queries }

When your application runs the same query shape many times with different `$parameters`, `execute()` and
`execute_batches()` automatically skip parsing and graph planning by reusing a cached plan template. Each call
binds its own parameter values before optimization and physical planning; rows and results are never cached.

??? note "Cache keys, eviction and bypass cases"

    - Each engine snapshot keeps up to **128** templates (least recently used evicted first), keyed by the exact
      query text and parameter names/types (with nulls kept separate from non-null values).
    - List and map parameters, parameterized `SKIP`/`LIMIT` counts, `CALL { ... }` subqueries and shapes that
      require literal values at planning time bypass the cache and plan normally.
    - `explain()` plans values directly; `explain(..., analyze=True)` uses the cache as `execute()` does.
    - A successful [`checkout_latest()`](lance.md#checkout-latest) starts the new snapshot with an empty cache,
      even if no dataset changed; active readers keep their existing plans.
    - Caching applies with and without the CSR and does not guarantee a speedup for every workload.

### Query parallelism { #query-parallelism }

`CypherEngine(..., target_partitions=None)` keeps DataFusion's default partitioning (typically one partition per
CPU core). If you have measured your workload, you can set a target between `1` and `1024`:

```python
serial_engine = CypherEngine(config, datasets, use_csr=True, target_partitions=1)
print(serial_engine.execute("MATCH (p:Person) RETURN count(*) AS n").to_pylist())  # [{'n': 4}]
```

`target_partitions` is a planning hint, not a thread or memory cap; individual operators may still choose a
different partition count. Lower values can reduce overhead on tiny queries while slowing down larger scans and
joins, so benchmark your full workload in the storage mode you deploy before changing the default.
`checkout_latest()` preserves your setting. Booleans and non-integers (other than `None`) raise `TypeError`;
values outside `1..1024` raise `ValueError`.

### Other `CypherEngine` methods

| Method | Purpose |
| --- | --- |
| `config()` | Return the engine's `GraphConfig`. |
| `checkout_latest()` | Advance every Lance dataset to its latest version. See [Lance datasets](lance.md#checkout-latest). |
| `dataset_versions()` | `dict[str, int]` of the pinned version of each Lance dataset. |
| `optimize_datasets(...)` | Compact fragments and clean up old Lance versions. See [Compaction and old versions](lance.md#compaction-and-old-versions). |
| `csr_stats()`, `node_index_stats()` | Inspect CSR and node-index state. See [Inspecting the CSR](csr.md#inspecting-the-csr). |
| `optimize_csr()`, `delete_stored_csr()` | Rebuild delta CSRs or remove stored `_lance_csr` files. See [The CSR index](csr.md#rebuilding-the-csr). |
| `export_graph(...)`, `export_subgraph(...)`, `write_node_column(...)` | Export dense CSR arrays for graph algorithms and write scores back. See [Graph algorithms](graph-algorithms.md). |
| `execute_with_vector_rerank(query, vector_search)` | Rerank query rows by vector similarity. See [Vector search](vector-search.md). |

See the [Python API reference](api-reference.md) for full signatures and return types.

## `CypherQuery`

For a single one-off query, exporting SQL with `to_sql()`, or querying a [`DirNamespace`](lance.md#directory-namespaces),
`CypherQuery` parses a query up front and runs it against whatever datasets you pass in. Each builder method
returns a new query:

```python
from lance_csr import CypherQuery

query = (
    CypherQuery("MATCH (p:Person) WHERE p.age > $age RETURN p.name AS name ORDER BY name")
    .with_config(config)
    .with_parameter("age", 30)
)
print(query.execute(datasets).to_pydict())  # {'name': ['Bob', 'David']}
```

| Method | What it does |
| --- | --- |
| `CypherQuery(query_text)` | Parse the query; raises `ValueError` on a syntax error. |
| `with_config(config)` | Attach a `GraphConfig`. |
| `with_parameter(key, value)` | Bind one `$key` parameter. |
| `execute(datasets, strategy=None, *, timeout=None)` | Run the query and return a `pyarrow.Table`. |
| `execute_with_namespace(namespace, strategy=None)` | Run against a [`DirNamespace`](lance.md#directory-namespaces). |
| `explain(datasets)` | Return the plan as text. For runtime metrics, use `CypherEngine.explain(..., analyze=True)`. |
| `to_sql(datasets, dialect=None)` | Export the equivalent join-based SQL. See [From Cypher to SQL](sql.md#from-cypher-to-sql). |
| `execute_with_vector_rerank(datasets, vector_search)` | Rerank results or pre-filter with a Lance ANN index. See [Vector search](vector-search.md). |
| `query_text()`, `parameters()`, `variables()`, `node_labels()`, `relationship_types()` | Inspect the parsed query. |

`strategy` takes an `ExecutionStrategy`. `ExecutionStrategy.DataFusion` is the default and only implemented
strategy; `ExecutionStrategy.LanceNative` is reserved and raises an error.

## Errors

| Exception | Raised when |
| --- | --- |
| `ValueError` | Syntax error, unknown label, missing configured column, unsupported function, invalid parameter, or incompatible known operand types in comparisons/arithmetic (names the expression and operand types; checked through `WITH` and `UNWIND` scopes). |
| `NotImplementedError` | Valid Cypher using an unsupported feature, such as a write clause (`CREATE`) or a variable-length range above 20 hops. |
| `RuntimeError` | Execution or I/O failure, such as an unreadable Lance dataset or a failed runtime cast. |
| `TimeoutError` | A query passes its `timeout`. See [Query deadlines](#query-deadlines). |
| `TypeError` | Wrong argument type (for example, a non-`GraphConfig`, an unsupported dataset type, or a non-integer `target_partitions`). |

??? note "Pre-planning type checks"

    Known comparison and arithmetic operand types are validated against the current dataset schemas before
    planning (for example, adding a number to a string property raises `ValueError` naming both types). Mixed
    numeric types and supported date/timestamp coercions are allowed. Comparing a string with a number delegates
    to DataFusion's cast, so `p.age = '40'` matches `40`, while a non-numeric string raises `RuntimeError` at
    execution time. Expressions whose types can't be determined in scope are left to the planner.
