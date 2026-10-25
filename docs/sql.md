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

`SqlEngine` registers the datasets once and reuses the DataFusion session:

```python
engine = SqlEngine(datasets)

print(engine.execute("SELECT COUNT(*) AS cnt FROM person").to_pylist())  # [{'cnt': 3}]

friends = engine.execute("""
    SELECT p1.name AS person, p2.name AS friend
    FROM person p1
    JOIN knows k ON p1.id = k.src_id
    JOIN person p2 ON p2.id = k.dst_id
    ORDER BY person, friend
""")
print(friends.to_pylist())
# [{'person': 'Alice', 'friend': 'Bob'}, {'person': 'Alice', 'friend': 'Carol'},
#  {'person': 'Bob', 'friend': 'Carol'}]
```

!!! note "Table names are lower-cased"

    `SqlEngine` lower-cases dataset names when it registers them, so a `"Person"` key is queried as `person`.
    Constructing an engine with an empty dict raises `ValueError`.

The SQL dialect is DataFusion's; see the
[DataFusion SQL reference](https://datafusion.apache.org/user-guide/sql/index.html) for what it supports.
`SqlEngine` has no `refresh()` or CSR: those are `CypherEngine` features.

## From Cypher to SQL

`CypherQuery.to_sql(datasets, dialect=None)` exports supported Cypher expressions as SQL. It's handy
for understanding a query, or for running a graph pattern on another SQL engine:

```python
from lance_csr import CypherQuery, GraphConfig, SqlDialect

config = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id")
    .build()
)
query = CypherQuery(
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
).with_config(config)
graph = {"Person": datasets["person"], "KNOWS": datasets["knows"]}

print(query.to_sql(graph))
print(query.to_sql(graph, SqlDialect.PostgreSql))
```

`SqlDialect` has `Default` (DataFusion's), `Spark`, `PostgreSql`, `MySql` and `Sqlite`.

Export is experimental and supports a subset of Cypher execution. Runtime-only Cypher functions
raise `NotImplementedError` when no faithful SQL expression is available. This includes math wrappers such
as `round` and `abs` when they depend on table values. Cypher `round(-1.5)` produces `-1.0`;
DataFusion SQL `round(-1.5)` produces `-2.0`.
Integer `abs` also normalizes its result to Int64 in Cypher, while SQL preserves the input width.
Use `CypherQuery.execute` or `CypherEngine.execute` for these expressions.

Constant-folded typed nulls use DataFusion `arrow_cast` in the Default dialect so their Arrow
result types are preserved. Other dialects raise `NotImplementedError` for typed-null literals;
a bare Cypher `null` remains an untyped SQL `NULL`.

Existing native SQL functions remain exportable. Constant expressions that the optimizer
evaluates completely can also export their resulting literals. These checks establish DataFusion
semantics; other SQL dialects still require validation on the destination engine.

!!! info "This is the join path"

    The generated SQL expresses each hop as a join. It describes what the query computes, not how
    `CypherEngine` runs it: with `use_csr=True`, hops run on the [CSR](csr.md) instead.
