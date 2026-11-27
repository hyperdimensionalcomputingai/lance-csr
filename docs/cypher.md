# Cypher reference

lance-csr implements a read-only subset of [openCypher](https://opencypher.org/) rich enough to run the
[GDC SNB Interactive](https://ldbcouncil.org/benchmarks/snb/) complex queries (IC1–IC14), including
`OPTIONAL MATCH`, `EXISTS`, `CALL { ... }`, variable-length trails, named path values, `shortestPath`,
comprehensions and `reduce`.

This reference applies equally across [Python](running-queries.md), [Rust](rust.md) and
[TypeScript](typescript.md). For engine setup, parameters, streaming, deadlines and `explain()`, see
[Running queries](running-queries.md).

A query may end with one optional semicolon; each execution call accepts a single statement. The runnable
examples below use this graph:

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

## Clauses { #supported-cypher }

| Clause | Notes |
| --- | --- |
| `MATCH` | Comma-separated patterns are supported. |
| `OPTIONAL MATCH` | Must share a variable with an earlier clause; cannot be the first clause. |
| `WHERE` | Follows `MATCH`, `OPTIONAL MATCH` (filters inside the optional part) or `WITH`. |
| `WITH [DISTINCT]` | Supports `ORDER BY`, `LIMIT` and `WHERE`; chain as many as needed. `WITH SKIP` is unsupported. |
| `UNWIND list AS x` | Expands a list into one row per element. |
| `UNION`, `UNION ALL` | Combines independent queries with matching column names/order and compatible Arrow types. |
| `CALL { ... RETURN ... }` | Read-only subquery evaluated per incoming row. See [Read-only subqueries](#read-only-subqueries). |
| `RETURN [DISTINCT]` | Supports `AS` aliases. Without a `MATCH`, expressions run on one implicit row (a configured engine is still required). |
| `ORDER BY`, `SKIP`, `LIMIT` | Nulls sort last ascending and first descending, as in openCypher. `SKIP` and `LIMIT` take non-negative integer literals or `$parameters`. |

!!! failure "Not supported"

    Write clauses (`CREATE`, `MERGE`, `SET`, `DELETE`, `DETACH DELETE`, `REMOVE`, `FOREACH`, `LOAD CSV`) and
    procedure calls (`CALL name(...)` / `YIELD`). Write clauses raise `NotImplementedError` naming the clause
    and its line and column.

### Labels are required or inferred

Because each node label maps to a table in [`GraphConfig`](graph-schema.md#labels-in-patterns), every node in a
pattern must either state a label (`(n:Person)`) or inherit one from a connected relationship (`(p:Person)-[:LIVES_IN]->(c)`
infers `City`, and untyped `MATCH (a)-[]-(b)` ranges over every configured relationship's endpoints). An
unlabeled standalone node such as `MATCH (n) RETURN n` raises `ValueError`:

```text
Query planning error: A node pattern has no label and no relationship to infer one from: add a label, e.g. `(n:Person)`
```

### `UNION` and `UNION ALL`

`UNION` removes duplicate rows; `UNION ALL` keeps them. Each branch has its own variable scope, `ORDER BY`,
`SKIP` and `LIMIT`, and combined output has no guaranteed order. As in openCypher, one query cannot mix `UNION`
and `UNION ALL`. Compatible numeric columns promote to a common type; mismatched column counts, names or types
raise `ValueError`.

```python
print(engine.execute("RETURN 1 AS n UNION RETURN 1 AS n").to_pylist())  # [{'n': 1}]
```

### List literals

Integer list literals preserve signed 64-bit values (so `UNWIND [1131, 19791209300775] AS id RETURN id` keeps
exact IDs). Lists with a decimal point or exponent (`[0.1, 1e-3]`) form `float32` vectors; whole-number lists
also work in [vector functions](vector-search.md#vector-functions-in-cypher) and convert to `float32` inside the
vector operation.

## Read-only subqueries

Use `CALL { ... }` when a match, aggregation or `LIMIT` must run independently for each incoming row, for
example to find each person's oldest friend:

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

Key rules:

- **Importing variables:** start the body with a plain `WITH a, b` naming existing outer variables (no aliases,
  expressions, `DISTINCT`, `WHERE` or `LIMIT`; add a second `WITH` for those). A body without an importing `WITH`
  has an independent scope.
- **Scoping and `RETURN`:** every body ends with `RETURN`. Expressions need an `AS` alias; bare variables keep
  their names. Returned names must be distinct and must not collide with outer variables; internal names stay
  private. Returned nodes and relationships keep their properties for later clauses.
- **Row semantics:** each child row pairs with its incoming row (preserving duplicates). An empty child drops
  that incoming row, whereas an ungrouped aggregate like `count()` still returns a row with `0`. An empty outer
  input skips the child. `ORDER BY` and `LIMIT` apply per incoming row.
- **Nesting and `UNION`:** `CALL` blocks can nest, appear after `WITH`/`UNWIND`, and combine branches with
  `UNION` or `UNION ALL` (each branch writes its own importing `WITH`, deduplication is per incoming row, and
  exported nodes/node-lists must share the same labels across branches).

!!! tip "Performance on large outer inputs"

    `CALL` binds, optimizes and plans its child separately for each incoming row, and bypasses the
    [plan template cache](running-queries.md#repeated-queries). Measure on representative data before using `CALL`
    over large outer inputs.

## Patterns

```cypher
// Direction: outgoing, incoming or either
MATCH (a:Person)-[:KNOWS]->(b)     // outgoing
MATCH (a:Person)<-[:KNOWS]-(b)     // incoming
MATCH (a:Person)-[:KNOWS]-(b)      // either direction

// Variable length: *, *n, *n..m, *..m, *n..
MATCH (a:Person {id: 1})-[:KNOWS*1..3]->(b:Person)

// Alternation (single-hop or variable-length)
MATCH (p:Person)-[:LIKES_POST|LIKES_COMMENT]->(m)

// Relationship variables and properties
MATCH (a:Person)-[k:KNOWS]->(b:Person) WHERE k.since > 2020

// Shortest paths (one relationship type, both ends bound)
MATCH p = shortestPath((a:Person {id: 1})-[:KNOWS*]-(b:Person {id: 4}))
RETURN length(p), nodes(p)
```

### Variable-length paths

- **Bounds:** ranges default to a minimum of `1` and a maximum of `20` hops; anything above `20` raises
  `NotImplementedError`. `*0..` includes a zero-hop identity match when start and end share the same label and
  key; `*0..0` matches only that identity.
- **Trail semantics (edge uniqueness):** each stored relationship can be used at most once per variable-length
  segment, in either direction. A path may revisit a node through a different edge, and parallel edges between
  the same two nodes produce distinct trails.
- **Execution:** eligible CSR paths run as one iterative expansion across the full range (see
  [Variable-length traversal](csr.md#variable-length-traversal)); the join fallback uses one branch per length.
- **Filtering edges along the path:** an inline property map such as `[:KNOWS*1..3 {since: 2020}]` must hold for
  every edge on the path (this form uses the join fallback). On a named path with the CSR,
  `WHERE all(r IN relationships(p) WHERE ...)` and `none(r IN relationships(p) WHERE ...)` prune failing edges
  *during* the walk whenever the predicate only reads `r`'s properties, parameters and literals (comparisons,
  `IN`, `OR`, `IS NULL`); other predicates (`any()`, `single()`, or `all()` under `OR`/`NOT`) run once each path
  is complete.

### Alternation and untyped relationships

- **Typed alternation** (`[:A|B]` and `[:hasType|isSubclassOf*1..3]`) follows each relationship type's configured
  endpoint labels. With CSR and node indexes on every participating type and label, variable-length alternation
  stays in one iterative operator. Standalone bounded undirected alternation (`[:KNOWS|WORKS_WITH*1..3]-`) can
  change direction at each hop (including in `OPTIONAL MATCH`) when endpoints have known labels and integer keys.
- **Untyped relationships** (`(a)-[]-(b)` or `path = (a)-[]-(b)`) range over all configured relationship types
  without mixing identities across node tables. Undirected matches include both orientations, self-loops appear
  once, and parallel edges stay distinct. Whole-node and path returns include the union of participating fields
  (with nulls where a label lacks a field); conflicting Arrow types for the same property name fail at planning
  time.

### Relationship uniqueness and reuse

- **Within one `MATCH`:** two single-hop relationships of the same type never bind the same edge, so
  `(a)-[:KNOWS]->(b)<-[:KNOWS]-(c)` never matches `c = a`. Separate `MATCH` clauses may reuse the same edge.
  For this single-hop check, edges compare by endpoints (parallel edges count as one).
- **Across segments of a path:** named paths track edge occurrences across all of their segments. Unnamed paths
  such as `(p)-[:KNOWS*1..2]-(f)-[:KNOWS]-(g)` do the same when the relationships are anonymous and typed, their
  boundary labels resolve, and a shared CSR walk or bounded join chain (at most 64 branches) applies; separate
  comma-separated paths in the same `MATCH` do not share history.
- **Reusing a relationship variable:** writing a bound variable again (such as `(a)-[r:KNOWS]->(b), (c)-[r]->(d)`,
  in a later `MATCH`, or after `WITH r`) matches that same edge (`c = a`, `d = b`). The type may be omitted; a
  conflicting type matches nothing. Undirected reuse matches both orientations; if the first binding was
  undirected, a later directed reuse follows the traversed direction. Reusing a relationship variable inside a
  variable-length segment is unsupported.
- **`count(r)`:** counts rows where `r` matched (an unmatched `OPTIONAL MATCH` counts `0`). `count(DISTINCT r)`
  compares all of `r`'s columns, so identical parallel edges count once.

### Shortest paths

`shortestPath` and `allShortestPaths` require one relationship type, no relationship variable, and named
endpoints. Lengths are unbounded unless you specify a maximum. When no path exists, `MATCH` drops the row and
`OPTIONAL MATCH` returns `null`. `shortestPath` returns one minimum-length path; `allShortestPaths` returns all
of them, including distinct parallel edges. With CSR indexes, single-target lookups search bidirectionally and
multi-source batches share forward searches (see [ShortestPath](internals/operators.md#shortestpath-breadth-first-search-over-the-adjacency)).

## Path values

A named path (`MATCH path = (a)-[:KNOWS*1..2]->(b)`) retains its ordered node and relationship records across
`WITH`, `collect()` and `UNWIND`, keeping parallel edges distinct even when their properties are identical:

```python
paths = engine.execute("""
    MATCH path = (a:Person {id: 1})-[:KNOWS]->(b:Person)
    WITH collect(path) AS paths
    UNWIND paths AS path
    RETURN [n IN nodes(path) | n.name] AS names,
           [r IN relationships(path) | endNode(r).id] AS targets
    ORDER BY names
""")
print(paths.to_pylist())
# [{'names': ['Alice', 'Bob'], 'targets': [2]},
#  {'names': ['Alice', 'Carol'], 'targets': [3]}]
```

- `nodes(path)` returns integer keys when returned directly; comprehensions over `nodes(path)` read the stored
  node records, keeping equal keys from different labels distinct.
- `relationships(path)` returns relationship records (binding a relationship variable inside a named
  variable-length segment is unsupported). `startNode(r)` and `endNode(r)` return the relationship's stored
  source and target even when the path walked it backwards.
- A zero-hop path has one node and an empty relationship list. All path functions return `null` for a `null` path.
- **Cost:** constructing path values reads participating node tables in full. With `use_csr=True`, named trail
  paths walk the CSR and read relationship properties only for returned paths; shortest paths and non-CSR
  relationship types build a query-local edge inventory whose memory grows with the graph. A named path whose
  variable is never read (such as `MATCH p = ... RETURN count(*)`) skips building path values.

??? info "Arrow struct layout and exact path semantics"

    - `RETURN path` yields an Arrow struct with fields `length`, `nodes`, `node_records` and `relationships`.
    - Entity records store user properties in `__lance_csr_properties` and `{label, id}` identity metadata in
      `__lance_csr_identity`. Relationship records also carry `__lance_csr_start` and `__lance_csr_end` node
      records. These metadata fields never collide with user properties of the same name.
    - Prefer Cypher property and endpoint functions (`startNode`, `endNode`, `keys`, `labels`) rather than
      relying on internal struct fields. Relationship identities distinguish occurrences within a single query
      only; they are not persistent IDs across queries or snapshots.
    - Rich paths require the same integer key types as the CSR (signed integers and `uint8`/`uint16`/`uint32`)
      and unique non-null node IDs per participating label (duplicates raise an error). Mixed labels may share
      compatible properties; conflicting Arrow types fail explicitly. Expanding a graph pattern from a
      mixed-label value is unsupported.
    - Queries reading only shortest-path length, node keys, path null checks or homogeneous node-property
      comprehensions (including through `WITH` aliases) stay on the fast CSR shortest-path route within a single
      label. Cross-label shortest paths (including inside `CALL`/`UNION` or when equal-label endpoints pass
      through another label) use typed identities so equal numeric keys from different labels never form a false
      zero-hop match.
    - Rich path queries can reuse cached plan templates, bind fresh parameters per execution, and honor snapshot
      isolation for open batch readers.

## Expressions and predicates

| Kind | Supported |
| --- | --- |
| Arithmetic | `+ - * / %` |
| Comparison | `= <> != < <= > >=`, and chains such as `18 <= p.age < 65` |
| Boolean | `AND OR NOT`; bare boolean properties/expressions work directly (`WHERE n.active`) |
| Strings | `STARTS WITH`, `ENDS WITH`, `CONTAINS`, and `LIKE` / `ILIKE` with SQL wildcards |
| Lists | `IN`, literals `[1, 2]`, indexing `list[0]`, list comprehensions <code>[x IN list WHERE ... &#124; ...]</code> |
| Maps | Literals `{k: v}` and key access `m.k` |
| Patterns | `EXISTS { MATCH ... WHERE ... }`, bare pattern predicates `NOT (a)-[:R]->(b)`, pattern comprehensions <code>[(a)--&gt;(b) &#124; b.name]</code> |
| Conditionals | `CASE WHEN ... THEN ... ELSE ... END` and `CASE x WHEN ...` |
| Nulls | `IS NULL`, `IS NOT NULL` |
| Temporal | `d.year`, `d.month`, `d.day`, `d.hour` and other component access |

!!! note "No regular expressions"

    Regular expressions (`=~`) are not supported. `STARTS WITH`, `ENDS WITH` and `CONTAINS` match literal text;
    use `LIKE` or `ILIKE` for wildcards.

Aggregate results can be compared in `RETURN` or `WITH`, combined with `AND`/`OR`/`NOT`, or used in `CASE`:

```cypher
MATCH (p:Person) WHERE p.id = 99
RETURN count(DISTINCT p.id) > 0 AS present
```

On empty input, a global `count` returns `0` (so the query above returns one row with `present = false`), and
`count(DISTINCT value)` skips nulls and duplicates, including when a filter fixes the counted property to a
constant.

## Functions

| Group | Functions |
| --- | --- |
| Aggregates | `count` (`count(*)`, `count(DISTINCT x)`), `sum`, `avg`, `min`, `max`, `collect` (supports `DISTINCT`; skips nulls) |
| Strings | `toLower` / `lower`, `toUpper` / `upper`, `toString`, `left`, `right`, `substring`, `replace`, `split`, `trim`, `lTrim`, `rTrim`, `reverse`, `size` |
| Conversion | `toInteger`, `toFloat`, `coalesce` |
| Properties | `keys(map)`, `keys(node)`, `keys(relationship)`, `labels(node)` |
| Lists | `size`, `head`, `last`, `tail`, `reverse`, `range(start, end [, step])` (inclusive), `reduce` |
| List predicates | `all`, `any`, `none`, `single` |
| Math | `abs`, `round`, `floor`, `ceil`, `sqrt` |
| Temporal | `date`, `datetime`, `date_part('month', d)`, `to_epoch_ms`, `duration.inSeconds(a, b).seconds` |
| Paths | `length(p)`, `nodes(p)`, `relationships(p)`; `startNode(r)`, `endNode(r)` on relationship values |
| Vectors | `vector_distance(a, b, metric)`, `vector_similarity(a, b, metric)`; see [Vector search](vector-search.md) |

Any unlisted function raises `ValueError` with `Cypher function 'name' is not implemented`.

```python
result = engine.execute("""
    RETURN substring('a猫😀z', 1, 2) AS text,
           reverse(range(1, 5, 2)) AS numbers,
           round(-1.5) AS rounded
""")
print(result.to_pylist())  # [{'text': '猫😀', 'numbers': [5, 3, 1], 'rounded': -1.0}]
```

`keys()` returns field names of a map (including null-valued keys) or the non-null exposed property names of a
node or relationship. `labels(node)` returns the canonical configured label of a single-label node binding
(preserving aliases through `WITH`, and returning `null` for an unmatched `OPTIONAL MATCH`):

```python
result = engine.execute("""
    MATCH (p:Person {name: 'Alice'}) WITH p AS person
    RETURN labels(person) AS labels, size(keys(person)) AS properties
""")
print(result.to_pylist())  # [{'labels': ['Person'], 'properties': 3}]
```

!!! note "Labels on mixed-label bindings"

    `labels(p)` requires a binding with one configured label. A multi-label pattern such as `(p:Person:City)`
    raises a mixed-label error (including after `WITH p AS x`), because the engine does not retain per-row labels
    across the union and never guesses a label from an ID.

??? note "Exact function semantics and edge cases"

    String and list functions follow the
    [openCypher 9 reference](https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf); math functions
    follow the [Neo4j 4.4 numeric reference](https://neo4j.com/docs/cypher-manual/4.4/functions/mathematical-numeric/).

    - **Strings:** `size`, `left`, `right` and `substring(text, start [, length])` count Unicode characters
      (`substring` starts at `0`). Starts and lengths must be non-negative integers; a null string returns null,
      while a null start or length on a non-null string raises an error. `trim`/`lTrim`/`rTrim` strip whitespace;
      `replace` replaces all literal matches; `split` preserves empty fields (`split('', ',')` returns `['']`,
      and an empty delimiter splits into characters).
    - **Lists:** `size` counts outer elements; `last([])` returns null; `tail([])` and `reverse([])` return `[]`;
      `reverse` also accepts strings. `range(start, end [, step])` is inclusive, defaults to step `1`, accepts
      negative steps, returns `[]` when the step points away from `end`, returns null on null arguments, and
      raises an error on step `0`, non-integers, or unsigned Arrow integers outside `Int64` range.
    - **Math:** `abs` preserves integers and raises on overflow or unsigned inputs outside `Int64` range. `round`,
      `floor`, `ceil` and `sqrt` convert numeric inputs (including `UInt64`) to `Float64`. `round` takes one
      argument and breaks ties toward $+\infty$ (`round(-1.5) == -1.0`). `sqrt` of a negative number returns `NaN`.
      All math functions return null for null input and reject non-numeric values.
    - **Properties:** `keys(entity)` returns normalized column spellings in unspecified order (including ID or
      endpoint columns); `keys({})` returns `[]`.
    - **Direct count of unmapped list comprehensions:** `size([x IN values WHERE x > 0])` counts retained elements
      directly instead of materializing the filtered list (duplicates and retained nulls still count as `1`, an
      empty or null list produces `0`, and the Arrow output column is non-nullable). Mapped comprehensions
      (`| expr`) and pattern comprehensions use the regular list-building path.

## Ordered `reduce` and list predicates

`reduce(total = initial, item IN list | expression)` folds a list in order, passing each step the previous
accumulator. The list predicates `all`, `any`, `none` and `single` test `function(item IN list WHERE predicate)`:

```python
result = engine.execute("""
    RETURN reduce(total = 0, item IN [1, 2, 3] | total * 10 + item) AS ordered,
           all(item IN [1, 2, 3] WHERE item > 0) AS positive,
           single(item IN [1, 2, 3] WHERE item = 2) AS exactly_one
""")
print(result.to_pylist())  # [{'ordered': 123, 'positive': True, 'exactly_one': True}]
```

- Both forms can capture outer variables and nest scalar folds or predicates.
- List predicates also support correlated graph predicates such as `EXISTS { MATCH ... }`.
- `reduce` accepts list comprehensions, pattern comprehensions or `collect(...)` as its input list; its body
  supports scalar expressions only (compute nested graph patterns, comprehensions or aggregates beforehand).

??? note "Empty-list, null and type semantics for `reduce` and list predicates"

    Semantics follow the pinned
    [Cypher 4.4 list](https://neo4j.com/docs/cypher-manual/4.4/functions/list/#functions-reduce) and
    [predicate](https://neo4j.com/docs/cypher-manual/4.4/functions/predicate/) references:

    - **`reduce`:** an empty list returns `initial`; a null list returns `null`. Null elements are passed to the
      body (use `coalesce()` to replace them). The accumulator and iterator names are local to the body while
      `initial` and `list` read outer variables. The accumulator must keep a compatible Arrow type (integers can
      promote to float).
    - **List predicates on empty/null lists:** on `[]`, `all` and `none` return `true`, while `any` and `single`
      return `false`. A null list returns `null`.
    - **Three-valued logic:** a null predicate result makes the outcome `null` unless a decisive element already
      fixes it (`false` decides `all`, `true` decides `any` and `none`, two `true`s decide `single`). Inspect
      null elements explicitly with `item IS NULL`.

## Putting it together

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

## Case sensitivity and quoted names

Keywords, function names, variables, labels, relationship types and property names are all **case-insensitive**:
`p.firstName`, `p.firstname` and `p.FIRSTNAME` refer to the same Arrow or Lance column (including in inline
filters and relationship properties). Source field names stay intact, unaliased `RETURN` items keep the spelling
you wrote, and explicit aliases follow the lowercase convention.

An output alias may reuse an input variable's name: expressions in the same `RETURN` still read the input
variable, while a following `ORDER BY` reads the output alias (for example,
`RETURN p.name AS p, p.id AS id ORDER BY p` sorts by name, including with aggregation and `OPTIONAL MATCH`).

Use backticks to quote labels, relationship types, variables, properties or aliases containing spaces, special
characters or keywords (double a backtick inside a quoted name). Quoting does not change case-insensitivity:

```python
device_config = GraphConfig.builder().node_table("Device Type", "asset id").build()
devices = {
    "Device Type": pa.table({"asset id": ["a-1", "b-2"], "status": ["active", "retired"]})
}
device_engine = CypherEngine(device_config, devices)
print(
    device_engine.execute(
        "MATCH (x:`Device Type`) WHERE x.`asset id` IN $keys RETURN x.`asset id` AS `asset id`",
        {"keys": ["a-1"]},
    ).to_pylist()
)  # [{'asset id': 'a-1'}]
```
