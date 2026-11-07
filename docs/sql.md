# SQL queries

Not every question is a graph question. lance-csr also runs plain SQL, through DataFusion, over the same
PyArrow tables and Lance datasets. No `GraphConfig` is needed: you write the joins yourself.

```python
import pyarrow as pa
from lance_csr import SqlEngine, SqlQuery

datasets = {
    "person": pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"], "age": [28, 34, 29]}),
    "knows": pa.table({"src_id": [1, 1, 2], "dst_id": [2, 3, 3]}),
}
```

## `SqlQuery`: one query

```python
result = SqlQuery("SELECT name, age FROM person WHERE age > 30").execute(datasets)
print(result.to_pylist())  # [{'name': 'Bob', 'age': 34}]
```

| Method | What it does |
| --- | --- |
| `SqlQuery(sql)` | Holds the query text. |
| `execute(datasets)` | Registers the datasets, runs the query and returns a `pyarrow.Table`. |
| `explain(datasets)` | The DataFusion logical and physical plans, as text. |
| `sql()` | The query text. |

## `SqlEngine`: many queries

`SqlEngine` registers the datasets once and keeps them for every query. Lance datasets are scanned in place,
pinned to the version passed in, with the query's columns and filters pushed into the scan; PyArrow tables are
held in memory. `$name` parameters take their values from a dict, as in Cypher:

```python
engine = SqlEngine(datasets)

print(engine.execute("SELECT COUNT(*) AS cnt FROM person").to_pylist())  # [{'cnt': 3}]

friends = engine.execute("""
    SELECT p1.name AS person, p2.name AS friend
    FROM person p1
    JOIN knows k ON p1.id = k.src_id
    JOIN person p2 ON p2.id = k.dst_id
    WHERE p1.age < $max_age
    ORDER BY person, friend
""", {"max_age": 40})
print(friends.to_pylist())
# [{'person': 'Alice', 'friend': 'Bob'}, {'person': 'Alice', 'friend': 'Carol'},
#  {'person': 'Bob', 'friend': 'Carol'}]
```

| Method | What it does |
| --- | --- |
| `SqlEngine(datasets, config=None, use_csr=False)` | Registers the datasets; `config` and `use_csr` add a [graph](#graph-patterns-in-sql). |
| `SqlEngine.from_engine(engine)` | SQL over a `CypherEngine`'s snapshot, sharing its datasets and CSR. |
| `execute(sql, params=None)` | Runs a query and returns a `pyarrow.Table`. |
| `explain(sql, params=None)` | The optimized logical and physical plans, as text. |
| `checkout_latest()` | Moves every Lance dataset to its latest version, as for [`CypherEngine`](lance.md). |

!!! note "Table names are lower-cased"

    `SqlEngine` lower-cases dataset names when it registers them, so a `"Person"` key is queried as `person`.
    A quoted table name must be lower-case too: a `"Device Type"` key is queried as `"device type"`.
    Constructing an engine with an empty dict raises `ValueError`.

The engine keeps up to 128 plans per snapshot, keyed by the query text and the names and types of its scalar
parameters, and binds the values on every call, as [`CypherEngine` does for Cypher](lance.md#repeated-queries).
A list or map parameter is planned on every call, and `checkout_latest()` clears the cache. Only queries run:
`CREATE`, `INSERT`, `COPY` and `SET` raise `ValueError`, as does a `$name` with no value.

A Lance dataset opened through a namespace can't be reopened by its URI, so without a `config` `SqlEngine` reads
it into memory, as `SqlQuery` reads every dataset on each call; with a `config` it raises `ValueError`, as
`CypherEngine` does. The SQL dialect is DataFusion's; see the
[DataFusion SQL reference](https://datafusion.apache.org/user-guide/sql/index.html) for what it supports.

## Graph patterns in SQL

Plain SQL doesn't know the graph, so every hop is a join you write. Given a `GraphConfig`, `SqlEngine` adds a
`GRAPH_TABLE` clause, modelled on SQL/PGQ and [DuckPGQ](https://duckpgq.org/), whose pattern the Cypher planner
plans. With `use_csr=True`, those hops run on the [CSR](csr.md), and the rest of the SQL query joins, filters and
groups the pattern's rows like any other table. This is experimental.

```python
from lance_csr import CypherEngine, GraphConfig

config = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id")
    .build()
)
graph = {"Person": datasets["person"], "KNOWS": datasets["knows"]}

engine = SqlEngine(graph, config, use_csr=True)
print(engine.execute("""
    SELECT g.person, p.age AS friend_age
    FROM GRAPH_TABLE (
        MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age < $max_age
        COLUMNS (a.name AS person, b.id AS friend_id)
    ) g
    JOIN person p ON p.id = g.friend_id
    ORDER BY person, friend_age
""", {"max_age": 30}).to_pylist())
# [{'person': 'Alice', 'friend_age': 29}, {'person': 'Alice', 'friend_age': 34}]
```

Inside `GRAPH_TABLE`, the `MATCH` pattern, the `WHERE` condition and the `COLUMNS` list are Cypher with SQL's
quoting: the clause runs as `MATCH ... WHERE ... RETURN <columns>` would, so any pattern `CypherEngine.execute` supports works there, and
the columns take the names their aliases give them. One `params` dict serves the SQL and every pattern. Only the
clause's shape comes from SQL/PGQ. There is no `CREATE PROPERTY GRAPH`: the graph is the engine's `GraphConfig`,
and a graph name before `MATCH` is accepted and ignored. `graph_table('<Cypher query>')` is the same table written
as a function call. Without a `config`, `GRAPH_TABLE` raises `ValueError`.

As in the rest of the SQL query, strings inside `GRAPH_TABLE` use single quotes, and double quotes name a label,
relationship type, variable, property or column that contains spaces or other characters, or that matches a
keyword, such as `x."asset id"` or `(x:"Device Type")`. Inside a double-quoted name, `""` stands for one `"`.
Cypher's backticks work too. The direct `graph_table('...')` call takes Cypher text as written, so there `"..."`
is a string.

```python
device_config = GraphConfig.builder().node_table("Device Type", "asset id").build()
devices = {
    "Device Type": pa.table({"asset id": ["a-1", "b-2"], "status": ["active", "retired"]})
}
device_engine = SqlEngine(devices, device_config)
device_engine.execute("""
    SELECT g."asset id" FROM GRAPH_TABLE (
        MATCH (x:"Device Type") WHERE x.status = 'active'
        COLUMNS (x."asset id" AS "asset id")
    ) g
""").to_pylist()  # [{'asset id': 'a-1'}]
```

To run Cypher and SQL over the same data, build the `CypherEngine` and share its snapshot. Nothing is rebuilt, and
because the two engines share one snapshot, `checkout_latest()` on either moves both:

```python
cypher = CypherEngine(config, graph, use_csr=True)
sql = SqlEngine.from_engine(cypher)
print(sql.execute(
    "SELECT count(*) AS n FROM GRAPH_TABLE (MATCH (a:Person)-[:KNOWS]->(b) COLUMNS (b.id))"
).to_pylist())  # [{'n': 3}]
```

`explain(sql, params)` shows the pushed-down `LanceRead` filters and any `CsrExpandExec` in the plan.

## From Cypher to SQL

`CypherQuery.to_sql(datasets, dialect=None)` exports supported Cypher expressions as SQL. It's handy
for understanding a query, or for running a graph pattern on another SQL engine:

```python
from lance_csr import CypherQuery, SqlDialect

query = CypherQuery(
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
).with_config(config)

print(query.to_sql(graph))
print(query.to_sql(graph, SqlDialect.PostgreSql))
```

`SqlDialect` has `Default` (DataFusion's), `Spark`, `PostgreSql`, `MySql` and `Sqlite`.

Export is experimental and supports a subset of Cypher execution. Runtime-only Cypher functions
raise `NotImplementedError` when no faithful SQL expression is available. This includes math wrappers such
as `round` and `abs`, and string functions such as `left` and string `size`, when they depend on
table values. Cypher `round(-1.5)` produces `-1.0`; DataFusion SQL `round(-1.5)` produces `-2.0`.
Integer `abs` also normalizes its result to Int64 in Cypher, while SQL preserves the input width.
Use `CypherQuery.execute` or `CypherEngine.execute` for these expressions.

Constant-folded typed nulls use DataFusion `arrow_cast` in the Default dialect so their Arrow
result types are preserved. Other dialects have no `arrow_cast`, so typed nulls export as a bare SQL
`NULL`: the value is the same, but its column type is left to the target database.

List `size` and inclusive `range` retain SQL export support through native expressions, including
null arguments, integer casts and the optional range step. Constant expressions that the optimizer
evaluates completely can also export their resulting literals. These checks establish DataFusion
semantics; other SQL dialects still require validation on the destination engine.

!!! info "This is the join path"

    The generated SQL expresses each hop as a join. It describes what the query computes, not how
    `CypherEngine` runs it: with `use_csr=True`, hops run on the [CSR](csr.md) instead.
