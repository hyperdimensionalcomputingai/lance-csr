# lance-csr

A Cypher graph query engine for [Lance](https://lancedb.github.io/lance/) datasets, built on Apache DataFusion,
with a CSR (compressed sparse row) adjacency index for fast multi-hop traversal. It interprets Lance datasets or
Arrow tables as property graphs. The [lance-csr Python package](https://pypi.org/project/lance-csr/) wraps this
crate.

## Features

- Cypher query parsing and AST construction
- Graph configuration for mapping Lance tables to nodes and relationships
- Semantic validation with typed `GraphError` diagnostics
- Pluggable execution strategies (DataFusion planner by default, Lance Native placeholder)
- `CypherEngine`: register datasets once, optionally build the CSR, and serve many queries
- Lance datasets read lazily and pinned to a version until `refresh()`
- Async query execution that returns Arrow `RecordBatch` results or a stream
- JSON-serializable parameter binding for reusable query templates
- Logical plan debugging via `CypherQuery::explain`

## Quick start

```rust
use std::collections::HashMap;
use lance_csr::{CypherEngine, EngineSource, GraphConfig};

let config = GraphConfig::builder()
    .node_table("Person", "id")
    .rel_table("KNOWS", "Person", "Person", "src_id", "dst_id")
    .build()?;
let sources = HashMap::from([
    ("Person".to_string(), EngineSource::open_lance("person.lance", None, HashMap::new()).await?),
    ("KNOWS".to_string(), EngineSource::open_lance("knows.lance", None, HashMap::new()).await?),
]);
let engine = CypherEngine::new(config, sources, true).await?;
let batch = engine
    .execute("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name, b.name", HashMap::new())
    .await?;
```

`examples/embedded_query.rs` is a complete program that writes two Lance datasets, queries them, and picks up a
new write with `refresh()`:

```bash
cargo run -p lance-csr --example embedded_query
```

The [Rust guide](https://lance-csr.pages.dev/rust/) covers the API, and the rest of the
[documentation](https://lance-csr.pages.dev/) covers the graph schema, supported Cypher and the CSR index.
Building the crate needs `protoc` for Lance's dependencies.

For one query over in-memory batches, `CypherQuery` works without an engine:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance_csr::{CypherQuery, ExecutionStrategy, GraphConfig};

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
use lance_csr::{GraphConfig, NodeMapping, RelationshipMapping};

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

For Rust callers migrating away from direct mapping-field access, use `get_node_mapping(label)` and
`get_relationship_mapping(type_name)` for case-insensitive lookups. `iter_node_mappings()` and
`iter_relationship_mappings()` yield read-only `(normalized_key, mapping)` pairs; iteration order is
unspecified. Build mappings through `GraphConfigBuilder` so keys are normalized. Public fields remain
available for compatibility until a breaking release; replacing direct writes with builder calls avoids
that future migration. The serialized configuration and Python API are unchanged.

## Executing Cypher Queries

- `CypherQuery::new` parses Cypher text into the internal AST.
- `with_config` attaches the graph configuration used for validation and execution.
- `with_parameter` / `with_parameters` bind JSON-serializable values that can be referenced as `$param` in the Cypher text.
- `execute` is asynchronous and returns an Arrow `RecordBatch`. Pass `None` to use the default DataFusion planner. `ExecutionStrategy::LanceNative` is reserved for future native execution support and currently errors.
- `explain` is asynchronous and returns a formatted string containing the graph logical plan alongside the DataFusion logical and physical plans.

All queries use the DataFusion planner for optimization and execution.

A builder (`CypherQueryBuilder`) is also available for constructing queries programmatically without parsing text.

## Supported Cypher

See [Supported Cypher](https://lance-csr.pages.dev/cypher/#supported-cypher) in the documentation for the
clauses, patterns, expressions and functions the engine runs.

## Crate Layout

- `ast` – Cypher AST definitions.
- `parser` – Nom-based Cypher parser.
- `semantic` – Lightweight semantic checks on the AST.
- `logical_plan` – Builders for graph logical plans.
- `datafusion_planner` – DataFusion-based execution planning.
- `config` – Graph configuration types and builders.
- `engine` – `CypherEngine`: cached snapshot, CSR adjacencies, `refresh()`.
- `query` – High level `CypherQuery` API and runtime.
- `csr_index`, `csr_expand` – The CSR adjacency index and the traversal operators that use it.
- `error` – `GraphError` and result helpers.
- `namespace` – Namespace helpers (re-exported from `lance-csr-catalog`).
- `source_catalog` – Catalog helpers for looking up table metadata (re-exported from `lance-csr-catalog`).

`lance-csr` re-exports the catalog and namespace types from the `lance-csr-catalog` crate for
API compatibility. You can depend on `lance-csr-catalog` directly if you only need catalog or
namespace utilities.

## Error Handling

Most APIs return `Result<T, GraphError>`. Errors include parsing failures, missing mappings, and execution issues surfaced from DataFusion.

## Testing

```bash
cargo test -p lance-csr
```

## Benchmarks

All published numbers were measured through the Python package; see the
[benchmarks page](https://lance-csr.pages.dev/benchmarks/). Criterion micro-benchmarks live in the unpublished
`lance-csr-benches` crate.

## Python Bindings

See the Python package docs for setup and development:

- Python package README: `python/README.md`
- Runnable examples (from repo root): `examples/README.md`

## License

Apache-2.0. See `LICENSE` and `NOTICE`.
