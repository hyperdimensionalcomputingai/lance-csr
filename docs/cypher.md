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
strings, booleans, `None`, lists, string-keyed maps, and `datetime.datetime` or `datetime.date` values:

```python
import datetime

engine.execute(
    "MATCH (p:Person) WHERE p.age >= $min_age AND p.name <> $skip RETURN p.name AS name",
    {"min_age": 30, "skip": "Bob"},
).to_pylist()  # [{'name': 'David'}]
```

Lists retain integer IDs, and maps can contain nested lists/maps and nulls:

```python
engine.execute(
    "MATCH (p:Person) WHERE p.id IN $ids RETURN p.name AS name ORDER BY name",
    {"ids": [1, 3]},
).to_pylist()  # [{'name': 'Alice'}, {'name': 'Carol'}]
```

Pagination also accepts parameters, for example `RETURN 1 AS n LIMIT $take` with `{"take": 1}`.
`WITH ... LIMIT $take` uses the same rules. Missing, null, boolean, fractional, negative and overflowing
counts raise `ValueError`; arbitrary expressions and `WITH SKIP` remain unsupported.

Map keys must be strings, integer parameters must fit signed 64-bit values, and nesting is limited to 128
levels. Cyclic containers and unsupported Python objects raise `ValueError`. Lists follow literal-list
coercion: mixed string/numeric values become strings because Arrow lists have one element type. Containers
are expression values; inline node/relationship property filters still require scalar parameters.

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
| `UNION`, `UNION ALL` | Combine independent queries with matching output names/order and compatible Arrow types. |
| `CALL { ... RETURN ... }` | Read-only subquery. Import outer variables with a leading plain `WITH`; see below. |
| `RETURN [DISTINCT]` | With `AS` aliases. Without reading clauses, expressions run on one implicit row. |
| `ORDER BY`, `SKIP`, `LIMIT` | `SKIP` and `LIMIT` take nonnegative integer literals or parameters. |

!!! failure "Not supported"

    Writes (`CREATE`, `MERGE`, `SET`, `DELETE`, `REMOVE`), procedure calls (`CALL name(...)` / `YIELD`), and `FOREACH`.

Integer list literals retain signed 64-bit values: `RETURN [1, 2, 3] AS ids` returns integers, and
`UNWIND [1131, 19791209300775] AS id RETURN id` preserves both IDs exactly. Numeric literals with a decimal
point or exponent, such as `[0.1, 1e-3]`, form float32 vectors. Whole-number lists can also be used as query
vectors; conversion to float32 happens inside the vector operation. A configured engine is still required
for standalone `RETURN` queries.

`UNION` removes duplicate rows; `UNION ALL` preserves them. Branches have independent variable scopes and
keep their own `ORDER BY`, `SKIP` and `LIMIT`. Combined output has no guaranteed order. Mixed UNION modes
are evaluated from left to right. Numeric columns can promote to a common numeric type; incompatible
column counts, names or types raise `ValueError`. This follows the existing homogeneous Arrow value model.

```python
engine.execute("RETURN 1 AS n UNION RETURN 1 AS n").to_pylist()  # [{'n': 1}]
```

### Read-only subqueries

Use `CALL { ... }` when a match, aggregate or limit should run separately for each incoming row:

```python
print(engine.execute("""
    MATCH (p:Person)
    CALL {
        WITH p
        MATCH (p)-[:KNOWS]->(f:Person)
        RETURN f AS friend ORDER BY friend.age DESC LIMIT 1
    }
    RETURN p.name AS person, friend.name AS friend ORDER BY person
""").to_pylist())
# [{'person': 'Alice', 'friend': 'Bob'}, {'person': 'Bob', 'friend': 'David'},
#  {'person': 'Carol', 'friend': 'David'}]
```

The leading importing `WITH` accepts only existing variable names, without aliases, expressions,
`DISTINCT`, filters or pagination. Use a second `WITH` to transform or filter them. A body without an
importing `WITH` has its own independent scope. Calls can nest and can appear after `WITH` or `UNWIND`.

A CALL body can combine branches with `UNION` or `UNION ALL`. Each branch needs its own importing
`WITH` and may import a different subset of the outer variables. Branches cannot see variables created
by sibling branches. Deduplication happens within each incoming row, so repeated outer rows remain
repeated. Node and node-list exports must retain the same labels in every branch; mixed-label entity
outputs are not represented by the current binding metadata.

Every body must end with `RETURN`. Give expressions an `AS` alias; bare variables keep their names.
Returned names must be distinct and must not already exist outside the call. Returning a node or
relationship retains its properties, so later clauses can use the returned entity. Names that the body
only uses internally do not become visible outside it.

Each child result row is combined with its incoming row. Duplicate results remain duplicates. An empty
child removes that incoming row; an aggregate such as `count()` without grouping still returns a row
with zero. An empty outer input never executes the child. `ORDER BY` and `LIMIT` inside the body apply
separately to each incoming row.

CALL currently binds, optimizes and physically plans a child for every incoming row. It streams the
child's batches before advancing that row, but operators such as sorting and aggregation can still
buffer their inputs. CALL queries bypass the query-template cache. For large outer inputs, measure the
query on representative data before choosing this form. Procedures, `YIELD`, and writes are unsupported.

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

- **Variable length** defaults to a minimum of 1 and a maximum of 20 hops; a maximum above 20 is rejected.
  `*0..` also matches the source itself, once per input row; `*0..0` matches only that identity. The start
  and end must have the same label and key for a zero-hop match. Eligible CSR paths use one iterative
  expansion for the whole range; the join fallback uses one branch per length.
- **Variable-length alternation** follows each relationship type's configured endpoint labels. With
  suitable CSR and node indexes, a mixed path such as `[:hasType|isSubclassOf*1..3]` stays in one operator.
  Undirected or named variable-length alternation remains unsupported.
- **Undirected variable-length paths** skip immediate backtracking (`a-b-a`).
- **`shortestPath` and `allShortestPaths`** need exactly one relationship type, no relationship variable,
  and named end nodes. Their length is unbounded unless you give a maximum. `nodes(p)` returns the node keys.
  With no path, `MATCH` drops the row and `OPTIONAL MATCH` returns `null`. `shortestPath` chooses one path
  when several have the same minimum length; `allShortestPaths` returns every shortest path, including
  duplicate node sequences that use different parallel edges. A single source-target pair searches from
  both ends when reverse adjacency is available. Several targets for the same source in an input batch
  share one forward search.
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
| Strings | `toLower` / `lower`, `toUpper` / `upper`, `toString`, `left`, `right`, `substring`, `replace`, `split`, `trim`, `lTrim`, `rTrim`, `reverse`, `size` |
| Conversion | `toInteger`, `toFloat`, `coalesce` |
| Lists | `size`, `head`, `last`, `tail`, `reverse`, `range(start, end [, step])` (inclusive) |
| Math | `abs`, `round`, `floor`, `ceil`, `sqrt` |
| Temporal | `date`, `datetime`, `date_part('month', d)`, `to_epoch_ms`, `duration.inSeconds(a, b).seconds` |
| Paths | `length(p)`, `nodes(p)` on `shortestPath` variables |
| Vectors | `vector_distance(a, b, metric)`, `vector_similarity(a, b, metric)`; see [Vector search](vector-search.md) |

The string and list functions follow the [Cypher v9 reference](https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf),
within the list representation described above. `size` counts Unicode characters in a string or the
outer elements of a list. `left(text, length)`, `right(text, length)` and
`substring(text, start [, length])` count characters; the substring start is zero-based. Lengths and
starts must be non-negative integers. A null input string returns null; a null start or length on a
non-null string raises an error. `trim`, `lTrim` and `rTrim` remove whitespace, and `replace` replaces
all literal matches. `split` preserves empty fields; an empty delimiter splits into characters.
Splitting an empty string returns `['']`.

`last([])` returns null; `tail([])` and `reverse([])` return empty lists. `reverse` also accepts strings.
`range(start, end [, step])` includes the end when reached, defaults to step 1, and accepts negative
steps. A step pointing away from the end gives an empty list; zero steps and non-integer arguments
raise an error. Null arguments return null.

`abs` preserves integer results; the other math functions return floating point values. Integer
absolute-value overflow raises an error. `round` accepts one argument, with ties toward positive
infinity (`round(-1.5)` is `-1.0`), as specified in the
[Neo4j 4.4 numeric reference](https://neo4j.com/docs/cypher-manual/4.4/functions/mathematical-numeric/).
`sqrt` of a negative number returns NaN. The math functions return null for null input and reject
non-numeric values.

```python
result = engine.execute("""
    RETURN substring('a猫😀z', 1, 2) AS text,
           reverse(range(1, 5, 2)) AS numbers,
           round(-1.5) AS rounded
""")
print(result.to_pylist())  # [{'text': '猫😀', 'numbers': [5, 3, 1], 'rounded': -1.0}]
```

Any other function raises `ValueError` with `Cypher function 'name' is not implemented`. This is a
bounded function set, not a claim of full Cypher conformance. Remaining functions include `keys`,
`labels`, `reduce`, the list predicates `any` / `all` / `none` / `single`, and `relationships(p)`.

When `size()` consumes an unmapped list comprehension, such as `size([x IN values WHERE x > 0])`, the planner
counts retained elements directly. Each duplicate and each retained null element still contributes one;
an empty or null input list produces zero, as for the existing comprehension. A mapped comprehension
(`| expression`) or a pattern comprehension keeps the ordinary list-building path. The count path still
unnests and groups rows; `explain()` shows a filtered count in place of ordered list aggregation. A direct
count output is marked non-nullable in its Arrow schema; the previous list-size expression was marked nullable.

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

An output alias may reuse an input variable's name. Expressions in the same `RETURN` still use the input
variable; the following `ORDER BY` uses the output alias. For example,
`RETURN p.name AS p, p.id AS id ORDER BY p` returns names and IDs, sorted by name. This also works with
aggregation and `OPTIONAL MATCH`.

## Errors

| Exception | Raised when |
| --- | --- |
| `ValueError` | The query doesn't parse, refers to an unknown label, a dataset is missing a configured column, or a comparison/arithmetic expression has incompatible known types. Type errors name the expression and its operand types. |
| `NotImplementedError` | The query parses but uses an unsupported feature (for example a function that isn't implemented, or more than 20 hops). |
| `RuntimeError` | Execution fails, for example when a Lance dataset can't be read. |

Known comparison and arithmetic types are checked against the current dataset schemas before planning.
For example, comparing a string property with a number reports the expression and both types in a
`ValueError`. Mixed numeric types and supported date/timestamp coercions remain valid. Types that cannot
be determined in the current scope are left to the planner, so these checks are not a complete type system.
