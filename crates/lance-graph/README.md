# Lance Graph Query Engine

A graph query engine for Lance datasets with Cypher syntax support. This crate enables querying Lance's columnar datasets using familiar graph query patterns, interpreting tabular data as property graphs.

## Features

- Cypher query parsing and AST construction
- Graph configuration for mapping Lance tables to nodes and relationships
- Semantic validation with typed `GraphError` diagnostics
- Pluggable execution strategies (DataFusion planner by default, Lance Native placeholder)
- Async query execution that returns Arrow `RecordBatch` results
- JSON-serializable parameter binding for reusable query templates
- Logical plan debugging via `CypherQuery::explain`

## Rust AST migration for pagination

`CypherQuery.skip`, `CypherQuery.limit` and `WithClause.limit` now use `Option<ast::RowCount>`.
Replace a directly constructed `Some(n)` with `Some(ast::RowCount::Literal(n))`; parameterized clauses
use `ast::RowCount::Parameter(name)`. Resolve parameter values before building a logical plan, using
the normal `CypherQuery` parameter-binding methods. Existing Cypher text with literal pagination is unchanged.

## Rust AST migration for list folds

`ast::ValueExpression` adds `Reduce`, `ListPredicate` and `LocalVariable` variants. Update exhaustive
matches and visitors to traverse the fold input and body while respecting iterator and accumulator
scope. Prefer parsing Cypher text to construct these expressions; the parser resolves local variable
references so they cannot accidentally bind to graph variables with the same name.

## Rust AST migration for path values

Named ordinary paths add `PathPattern.variable`; set it to `None` in existing unnamed-path struct
literals, or `Some(name)` for `p = (a)-[:R]->(b)`. Shortest-path names still use
`ShortestPathSpec.variable`. Exhaustive matches and visitors must handle `LogicalOperator::PathMatch`
by traversing its input and path pattern. It retains ordered node and relationship records, so a path
value must not be replaced by its length. Public `nodes(path)` still returns integer keys; the Cypher
guide describes the separate Arrow record returned by a path value.

## Quick Start

```rust
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance_graph::{CypherQuery, ExecutionStrategy, GraphConfig};

let config = GraphConfig::builder()
    .node_table("Person", "person_id")
    .rel_table("KNOWS", "Person", "Person", "src_person_id", "dst_person_id")
    .build()?;

let schema = Arc::new(Schema::new(vec![
    Field::new("person_id", DataType::Int32, false),
    Field::new("name", DataType::Utf8, false),
    Field::new("age", DataType::Int32, false),
]));
let batch = RecordBatch::try_new(
    schema,
    vec![
        Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef,
        Arc::new(Int32Array::from(vec![29, 35])) as ArrayRef,
    ],
)?;

let mut tables = HashMap::new();
tables.insert("Person".to_string(), batch);

let query = CypherQuery::new("MATCH (p:Person) WHERE p.age > $min RETURN p.name")?
    .with_config(config)
    .with_parameter("min", 30);

let runtime = tokio::runtime::Runtime::new()?;
// Use default DataFusion-based execution
let result = runtime.block_on(query.execute(tables, None))?;
```

The query expects a `HashMap<String, RecordBatch>` keyed by the labels and relationship types referenced in the Cypher text. Each record batch should expose the columns configured through `GraphConfig` (ID fields, property fields, etc.). Relationship mappings also expect a batch keyed by the relationship type (for example `KNOWS`) that contains the configured source/target ID columns and any optional property columns.

## Configuring Graph Mappings

Graph mappings are declared with `GraphConfig::builder()`:

```rust
use lance_graph::{GraphConfig, NodeMapping, RelationshipMapping};

let config = GraphConfig::builder()
    .node_table("Person", "person_id")
    .rel_table("KNOWS", "Person", "Person", "src_person_id", "dst_person_id")
    .build()?;
```

For finer control, build `NodeMapping` and `RelationshipMapping` instances explicitly:

```rust
let person = NodeMapping::new("Person", "person_id")
    .with_properties(vec!["name".into(), "age".into()])
    .with_filter("kind = 'person'");

let knows = RelationshipMapping::new("KNOWS", "src_person_id", "dst_person_id")
    .with_properties(vec!["since".into()]);

let config = GraphConfig::builder()
    .with_node_mapping(person)
    .with_relationship_mapping(knows)
    .build()?;
```

## Executing Cypher Queries

- `CypherQuery::new` parses Cypher text into the internal AST.
- `with_config` attaches the graph configuration used for validation and execution.
- `with_parameter` / `with_parameters` bind JSON-serializable values that can be referenced as `$param` in the Cypher text.
- `execute` is asynchronous and returns an Arrow `RecordBatch`. Pass `None` to use the default DataFusion planner. `ExecutionStrategy::LanceNative` is reserved for future native execution support and currently errors.
- `explain` is asynchronous and returns a formatted string containing the graph logical plan alongside the DataFusion logical and physical plans.

All queries use the DataFusion planner for optimization and execution.

A builder (`CypherQueryBuilder`) is also available for constructing queries programmatically without parsing text.

## Supported Cypher Surface

- Node patterns `(:Label)` with optional variables.
- Relationship patterns with fixed direction and type, including multi-hop paths.
- Property comparisons against literal values with `AND`/`OR`/`NOT`/`EXISTS`.
- RETURN lists of property accesses, optional `DISTINCT`, `ORDER BY`, `SKIP` (offset), and `LIMIT`.
- Positional and named parameters (e.g. `$min_age`).

Basic aggregations like `COUNT` are supported. Optional matches and subqueries are parsed but not executed yet.

## Crate Layout

- `ast` – Cypher AST definitions.
- `parser` – Nom-based Cypher parser.
- `semantic` – Lightweight semantic checks on the AST.
- `logical_plan` – Builders for graph logical plans.
- `datafusion_planner` – DataFusion-based execution planning.
- `config` – Graph configuration types and builders.
- `query` – High level `CypherQuery` API and runtime.
- `error` – `GraphError` and result helpers.
- `namespace` – Namespace helpers (re-exported from `lance-graph-catalog`).
- `source_catalog` – Catalog helpers for looking up table metadata (re-exported from `lance-graph-catalog`).

`lance-graph` re-exports the catalog and namespace types from the `lance-graph-catalog` crate for
API compatibility. You can depend on `lance-graph-catalog` directly if you only need catalog or
namespace utilities.

## Error Handling

Most APIs return `Result<T, GraphError>`. Errors include parsing failures, missing mappings, and execution issues surfaced from DataFusion.

## Constructing query ASTs directly

Prefer `CypherQuery::new` or the query builder when you do not need direct AST access. Pagination fields
now use `Option<ast::RowCount>`: wrap literal values with `RowCount::Literal(n)`, or use
`RowCount::Parameter(name)` before parameter substitution. AST struct literals also set
`union_branches: Vec::new()` for a single query. `UnionBranch` holds each following query and its `all`
flag; each branch has its own variable scope. Exhaustive matches on `LogicalOperator` must handle the
new `SingleRow` and `QueryUnion` variants. These are Rust source compatibility changes; Python method
signatures are unchanged by UNION.

## Testing

```bash
cargo test -p lance-graph
```

## Benchmarks

See the repository root `README.md` for benchmark setup, run commands, and report locations.

## Python Bindings

See the Python package docs for setup and development:

- Python package README: `python/README.md`
- Runnable examples (from repo root): `examples/README.md`

## License

Apache-2.0. See the top-level LICENSE file for details.
