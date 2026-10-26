# Using lance-csr from Rust

lance-csr is a Rust library first. The Python package is a thin wrapper around the same engine, so everything
on the other pages (the [graph schema](graph-schema.md), the [supported Cypher](cypher.md#supported-cypher),
the [CSR index](csr.md) and [version pinning](lance.md)) applies to Rust too. This page covers what's
different: installing the crate, the async API, and the Rust types.

Every Rust snippet on this page comes from
[`examples/embedded_query.rs`](https://github.com/hyperdimensionalcomputingai/lance-csr/blob/main/crates/lance-csr/examples/embedded_query.rs),
which CI compiles and runs. Run it yourself from a checkout:

```bash
cargo run -p lance-csr --example embedded_query
```

## Install

The engine is published on crates.io as `lance-csr`:

```bash
cargo add lance-csr
```

Building it needs `protoc`, the Protocol Buffers compiler, for Lance's dependencies (see
[Contributing](contributing.md)). The engine is async, so your program also needs a runtime such as
[Tokio](https://tokio.rs/).

!!! warning "Use the same Lance, DataFusion and Arrow versions"

    lance-csr 0.3 builds on `lance` 13, `datafusion` 54 and `arrow` 58. If your program also uses those crates,
    for example to open datasets or read results, depend on the same major versions. Otherwise their types
    (`Dataset`, `RecordBatch`) won't match lance-csr's.

## Build an engine

`CypherEngine` is the same engine as in Python. It registers your datasets once, optionally builds the CSR,
and serves many queries:

```rust
--8<-- "crates/lance-csr/examples/embedded_query.rs:engine"
```

Each input is an `EngineSource`:

`EngineSource::open_lance(uri, version, storage_options)`
:   Opens a Lance dataset, pinned to `version` (`None` means the latest). The dataset is read lazily as
    queries need it, and `storage_options` are kept so `refresh()` can reopen it with the same credentials.
    See [Lance datasets](lance.md).

`EngineSource::Arrow(batch)`
:   An in-memory Arrow `RecordBatch`. It never changes; build a new engine to replace it.

The third argument of `CypherEngine::new` is `use_csr`. With `true`, relationship types with integer endpoints
get [CSR adjacencies](csr.md) and node labels with unique integer ids get a node index, exactly as with
`use_csr=True` in Python. `CypherEngine::with_target_partitions(config, sources, use_csr, Some(n))` also sets
DataFusion's query partition target (1 to 1024), like `target_partitions=` in Python; see
[Query parallelism](lance.md#query-parallelism).

## Run queries

`execute` returns the whole result as one `RecordBatch`. Parameters are a `HashMap` of `serde_json::Value`s,
one per `$name` in the query; pass `HashMap::new()` when there are none:

```rust
--8<-- "crates/lance-csr/examples/embedded_query.rs:query"
```

The other query methods take the same arguments:

| Method | Returns |
| --- | --- |
| `execute(query, params)` | `RecordBatch`, the whole result |
| `execute_stream(query, params)` | DataFusion `SendableRecordBatchStream`, computed as you poll it; see [streaming](lance.md#streaming-large-results) |
| `explain(query, params)` | `String`, the logical and physical plans |
| `execute_with_vector_rerank(query, &vector_search)` | `RecordBatch`, reranked by a [`VectorSearch`](vector-search.md) |

Each query runs on the snapshot that was current when it started.

## Pick up new writes

`refresh()` moves every Lance dataset to its latest version and updates the CSR, applying the new edges as a
delta when it can (see [Keeping the CSR current](csr.md#keeping-the-csr-current)):

```rust
--8<-- "crates/lance-csr/examples/embedded_query.rs:refresh"
```

`csr_stats()` and `node_index_stats()` return the same fields as in Python, as `CsrStats` and `NodeIndexStats`
structs keyed by relationship type and node label. `dataset_versions()` gives the pinned version of each Lance
dataset.

## Errors

Every method returns `lance_csr::Result<T>`, whose error is `GraphError`. The Python package raises a
different exception depending on the variant:

| `GraphError` variant | Meaning | Python exception |
| --- | --- | --- |
| `ParseError` | The Cypher text doesn't parse | `ValueError` |
| `ConfigError` | Bad graph configuration, no datasets, or a configured column is missing | `ValueError` |
| `PlanError`, `InvalidPattern` | The query can't be planned against the configuration | `ValueError` |
| `UnsupportedFeature` | Valid Cypher that lance-csr doesn't support | `NotImplementedError` |
| `ExecutionError`, `DataFusion`, `LanceCore`, `Arrow` | Opening, scanning or executing failed | `RuntimeError` |

## Async runtimes and cancellation

- Building the CSR in `new()` and `refresh()` is CPU-heavy work inside an async function, so it holds a
  runtime worker thread until it finishes. In an application with its own runtime (a desktop or web backend),
  run those two calls with `tokio::task::spawn_blocking` so other tasks keep running.
- Dropping a query's future, or the stream from `execute_stream`, cancels the rest of that query. There are no
  built-in timeouts; wrap a call in `tokio::time::timeout` if you need one.

## Python and Rust side by side

| Python | Rust |
| --- | --- |
| `CypherEngine(config, datasets, use_csr=True)` | `CypherEngine::new(config, sources, true).await` |
| `lance.dataset(uri)` in `datasets` | `EngineSource::open_lance(uri, None, options).await` |
| a PyArrow table in `datasets` | `EngineSource::Arrow(batch)` |
| `engine.execute(query, params)` → `pyarrow.Table` | `engine.execute(query, params).await` → `RecordBatch` |
| `engine.execute_batches(...)` → `RecordBatchReader` | `engine.execute_stream(...).await` → stream |
| `engine.refresh()` | `engine.refresh().await` |
| `engine.csr_stats()` → `dict` | `engine.csr_stats()` → `HashMap<String, CsrStats>` |
| `ValueError`, `RuntimeError`, ... | `GraphError` variants |

`CypherQuery` (one query over in-memory batches), `SqlQuery` and `DirNamespace` also exist in Rust. The
[API documentation on docs.rs](https://docs.rs/lance-csr) lists every type and method.

## Performance

The engine code is the same whichever language calls it. The [benchmarks](benchmarks.md) were all measured
through the Python package; there are no separate Rust benchmark results.
