# Querying with Cypher

There are two ways to run Cypher, and most code wants the first:

| | `CypherEngine` | `CypherQuery` |
| --- | --- | --- |
| Use it for | Many queries over the same data | One query, once |
| Setup cost | Paid once, in the constructor | Paid on every `execute()` |
| Lance datasets | Read lazily, pinned to a version, `refresh()` | Read into memory |
| CSR index | `use_csr=True` | Not available |
| Streaming results | `execute_batches()` | No |

The examples on this page share this setup:

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

```python
CypherEngine(config, datasets, use_csr=False)
```

The constructor validates the datasets against the config, registers them with a DataFusion session, and,
with `use_csr=True`, builds the [CSR index](csr.md). After that, every query reuses the same catalog and
session.

!!! tip "Share one engine"

    An engine is safe to use from several threads: queries run on a shared, read-only snapshot, and the GIL
    is released while they execute. Build it once at startup, not per request.

### `execute(query, params=None)`

Runs the query and returns the whole result as a `pyarrow.Table`:

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

Output columns are named by their `AS` alias, or by the expression text (`p.name`) when there is none.

### Parameters

Write `$name` in the query and pass a dict as the second argument. Parameter values can be numbers,
strings, booleans, `None`, and `datetime.datetime` or `datetime.date` values:

```python
import datetime

engine.execute(
    "MATCH (p:Person) WHERE p.age >= $min_age AND p.name <> $skip RETURN p.name AS name",
    {"min_age": 30, "skip": "Bob"},
).to_pylist()  # [{'name': 'David'}]
```

!!! warning "No list or map parameters"

    `$ids` bound to `[1, 2, 3]` raises `ValueError: Unsupported parameter type` on `CypherEngine`. Write the
    list literally in the query (`WHERE p.id IN [1, 2, 3]`) for now.

Datetimes compare against timestamp columns, and dates against date columns. Parameters are substituted
before planning, so they are also allowed in inline property maps: `(p:Person {id: $id})`.

### `execute_batches(query, params=None)`

Streams the result as a `pyarrow.RecordBatchReader` instead of building one table:

```python
reader = engine.execute_batches("MATCH (p:Person) RETURN p.id AS id, p.name AS name")
rows = 0
for batch in reader:  # each batch is a pyarrow.RecordBatch
    rows += batch.num_rows
print(rows)  # 4
```

- Batches are computed as you read them. Scans, filters and projections stream with bounded memory; joins,
  sorts and aggregations may still buffer their inputs.
- CSR expansion visits neighbours and fetches their properties a chunk at a time, including when one node
  has many neighbours. See [CSR memory cost](csr.md#turning-it-on) for what it keeps in memory.
- Planning errors (a syntax error, an unknown label) raise immediately. Execution errors raise while reading.
- The reader keeps its snapshot alive, so a concurrent [`refresh()`](lance.md#refresh) doesn't affect it.
- Dropping the reader cancels the remaining work.

### `explain(query, params=None)`

Returns the plan as text: the graph logical plan, DataFusion's logical plan and the physical plan.
It's the quickest way to check whether a hop used the CSR (`CsrExpandExec`) or a join (`HashJoinExec`), and
whether filters reached the Lance scan:

```python
plan = engine.explain("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name")
print("CsrExpandExec" in plan)  # True
```

### Other methods

| Method | Returns |
| --- | --- |
| `config()` | The engine's `GraphConfig`. |
| `refresh()` | `None`. Moves every Lance dataset to its latest version. See [Lance datasets](lance.md#refresh). |
| `dataset_versions()` | `dict[str, int]`: the pinned version of each Lance dataset. |
| `csr_stats()`, `node_index_stats()` | CSR statistics. See [The CSR index](csr.md#inspecting-the-csr). |
| `execute_with_vector_rerank(query, vector_search)` | A table reranked by vector similarity. See [Vector search](vector-search.md). |

## `CypherQuery`

`CypherQuery` parses a query up front and runs it against whatever datasets you hand it. Each builder
method returns a new query:

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
| `CypherQuery(query_text)` | Parses the query; raises `ValueError` on a syntax error. |
| `with_config(config)` | Sets the `GraphConfig`. |
| `with_parameter(key, value)` | Binds one `$key` parameter. |
| `execute(datasets, strategy=None)` | Runs it and returns a `pyarrow.Table`. |
| `execute_with_namespace(namespace, strategy=None)` | Runs it against a [`DirNamespace`](lance.md#directory-namespaces). |
| `explain(datasets)` | The plan as text. |
| `to_sql(datasets, dialect=None)` | The equivalent SQL. See [SQL queries](sql.md#from-cypher-to-sql). |
| `execute_with_vector_rerank(datasets, vector_search)` | See [Vector search](vector-search.md). |
| `query_text()`, `parameters()` | The query string and bound parameters. |
| `variables()`, `node_labels()`, `relationship_types()` | Names the query refers to. |

`strategy` takes an `ExecutionStrategy`. `ExecutionStrategy.DataFusion` is the default and the only one
implemented; `ExecutionStrategy.LanceNative` is reserved and raises an error.

## Supported Cypher

The Graph Data Council (GDC), formerly known as the Linked Data Benchmark Council (LDBC), maintains the
[GDC SNB Interactive](https://ldbcouncil.org/benchmarks/snb/) complex queries. lance-csr implements a large
read-only subset of openCypher, enough to run all 14 (IC14 without `reduce()`).

### Clauses

| Clause | Notes |
| --- | --- |
| `MATCH` | Comma-separated patterns are allowed. |
| `OPTIONAL MATCH` | Must share a variable with an earlier clause, and can't be the first clause. |
| `WHERE` | After `MATCH`, `OPTIONAL MATCH` (filters inside the optional part) or `WITH`. |
| `WITH [DISTINCT]` | With `ORDER BY`, `LIMIT` and `WHERE`; chain as many as you need. No `SKIP` inside `WITH`. |
| `UNWIND list AS x` | |
| `RETURN [DISTINCT]` | With `AS` aliases. |
| `ORDER BY`, `SKIP`, `LIMIT` | `SKIP` and `LIMIT` take integer literals, not parameters. |

!!! failure "Not supported"

    Writes (`CREATE`, `MERGE`, `SET`, `DELETE`, `REMOVE`), `UNION`, `CALL`, `FOREACH`, and queries with
    neither `MATCH` nor `UNWIND`.

### Patterns

```cypher
// Direction: outgoing, incoming or either
MATCH (a:Person)-[:KNOWS]->(b)     // outgoing
MATCH (a:Person)<-[:KNOWS]-(b)     // incoming
MATCH (a:Person)-[:KNOWS]-(b)      // either direction

// Variable length: *, *n, *n..m, *..m, *n..
MATCH (a:Person {id: 1})-[:KNOWS*1..3]->(b:Person)

// Alternation, also with variable length
MATCH (p:Person)-[:LIKES_POST|LIKES_COMMENT]->(m)

// Relationship variables and properties
MATCH (a:Person)-[k:KNOWS]->(b:Person) WHERE k.since > 2020

// Shortest paths (one relationship type, both ends bound)
MATCH p = shortestPath((a:Person {id: 1})-[:KNOWS*]-(b:Person {id: 4}))
RETURN length(p), nodes(p)
```

- **Variable length** defaults to a minimum of 1 and a maximum of 20 hops; a maximum above 20 is rejected,
  and zero-length paths (`*0..`) are not supported. Each length becomes one branch of a union.
- **Undirected variable-length paths** skip immediate backtracking (`a-b-a`).
- **`shortestPath` and `allShortestPaths`** need exactly one relationship type, no relationship variable,
  and named end nodes. Their length is unbounded unless you give a maximum. `nodes(p)` returns the node keys.
  With no path, `MATCH` drops the row and `OPTIONAL MATCH` returns `null`.
- **Named paths** (`p = ...`) are only supported with `shortestPath` and `allShortestPaths`.

### Expressions and predicates

| Kind | Supported |
| --- | --- |
| Arithmetic | `+ - * / %` |
| Comparison | `= <> != < <= > >=`, and chains such as `18 <= p.age < 65` |
| Boolean | `AND OR NOT` |
| Strings | `STARTS WITH`, `ENDS WITH`, `CONTAINS`, and `LIKE` / `ILIKE` with SQL wildcards |
| Lists | `IN`, literals `[1, 2]`, indexing `list[0]`, list comprehensions <code>[x IN list WHERE ... &#124; ...]</code> |
| Maps | Literals `{k: v}` and key access `m.k` |
| Patterns | `EXISTS { MATCH ... WHERE ... }`, bare pattern predicates `NOT (a)-[:R]->(b)`, pattern comprehensions <code>[(a)--&gt;(b) &#124; b.name]</code> |
| Conditionals | `CASE WHEN ... THEN ... ELSE ... END` and `CASE x WHEN ...` |
| Nulls | `IS NULL`, `IS NOT NULL` |
| Temporal | `d.year`, `d.month`, `d.day`, `d.hour` and other component access |

!!! note "String-matching operators use SQL `LIKE`"

    `STARTS WITH`, `ENDS WITH` and `CONTAINS` need a string literal on the right and compile to `LIKE`, so a
    `%` or `_` inside that literal acts as a wildcard. Regular expressions (`=~`) are not supported.

### Functions

| Group | Functions |
| --- | --- |
| Aggregates | `count` (including `count(*)` and `count(DISTINCT x)`), `sum`, `avg`, `min`, `max`, `collect` (including `DISTINCT`; skips nulls) |
| Strings | `toLower` / `lower`, `toUpper` / `upper`, `toString` |
| Conversion | `toInteger`, `toFloat`, `coalesce` |
| Lists | `size`, `head`, `range(start, end)` (inclusive, no step) |
| Temporal | `date`, `datetime`, `date_part('month', d)`, `to_epoch_ms`, `duration.inSeconds(a, b).seconds` |
| Paths | `length(p)`, `nodes(p)` on `shortestPath` variables |
| Vectors | `vector_distance(a, b, metric)`, `vector_similarity(a, b, metric)`; see [Vector search](vector-search.md) |

Any other function raises `NotImplementedError: Cypher function 'name' is not implemented`. Notably
missing: math functions (`abs`, `round`, `sqrt`, ...), most string and list functions, `reduce`, the list
predicates `any` / `all` / `none` / `single`, and `relationships(p)`.

### Example: a little of everything

```python
result = engine.execute("""
    MATCH (p:Person)
    OPTIONAL MATCH (p)-[:KNOWS]->(f:Person)
    WITH p, count(f) AS friends
    WHERE EXISTS { MATCH (p)-[:LIVES_IN]->(:City {name: 'London'}) }
    RETURN p.name AS name,
           friends,
           CASE WHEN p.age >= 30 THEN 'senior' ELSE 'junior' END AS band
    ORDER BY name
""")
print(result.to_pylist())
# [{'name': 'Alice', 'friends': 2, 'band': 'junior'},
#  {'name': 'Bob', 'friends': 1, 'band': 'senior'}]
```

## Case sensitivity

Keywords, function names, variables, labels, relationship types and property names are all
case-insensitive. `match (P:person) return p.NAME` works, and the output column keeps the spelling you wrote.

!!! bug "Known issue: an alias that shadows a variable"

    `RETURN p.name AS p` reuses the variable name `p` as an output alias, which can fail to plan once the
    query also aggregates or uses `OPTIONAL MATCH`. Pick a different alias, such as `AS name`.

## Errors

| Exception | Raised when |
| --- | --- |
| `ValueError` | The query doesn't parse, refers to an unknown label, or a dataset is missing a configured column. |
| `NotImplementedError` | The query parses but uses an unsupported feature (for example a function that isn't implemented, or more than 20 hops). |
| `RuntimeError` | Execution fails, for example when a Lance dataset can't be read. |
