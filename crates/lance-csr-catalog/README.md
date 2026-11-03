# lance-csr-catalog

The table lookups behind [lance-csr](https://crates.io/crates/lance-csr), a Cypher graph query engine for
[Lance](https://lancedb.github.io/lance/) datasets built on Apache DataFusion. lance-csr reads a graph from
ordinary tables: one per node label and one per relationship type. This crate answers the two questions that
raises: which DataFusion table holds a given label or type, and where a named Lance dataset lives.

- `GraphSourceCatalog` is the trait the lance-csr planner asks for a node label's or relationship type's
  DataFusion `TableSource`, so that it can build scans while planning. Lookups ignore case.
  `InMemoryCatalog` is a map-backed implementation, and `SimpleTableSource` is a table source with a fixed
  schema and no data, for planning and tests.
- `DirNamespace` implements Lance's `LanceNamespace` for a directory of datasets: the table `person` under
  `s3://bucket/graph` is `s3://bucket/graph/person.lance`. It works with any URI Lance can open.

lance-csr re-exports all four types, so most programs only depend on `lance-csr`. Depend on this crate directly to
implement your own catalog, for example over a metadata service, without pulling in the query engine.

```rust
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use lance_csr_catalog::{GraphSourceCatalog, InMemoryCatalog, SimpleTableSource};

let person = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
let catalog = InMemoryCatalog::new()
    .with_node_source("Person", Arc::new(SimpleTableSource::new(person)));
assert!(catalog.node_source("person").is_some());
```

The [Rust guide](https://lance-csr.hdclabs.ai/rust/) covers the engine that uses these types.

## License

Apache-2.0. See `LICENSE` and `NOTICE`.
