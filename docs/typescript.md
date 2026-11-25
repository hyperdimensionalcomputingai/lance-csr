# Using lance-csr from TypeScript

The `lance-csr` npm package runs the same Rust engine as the Python package, inside a Node.js process. Its
classes and methods mirror the Python API one for one, with camel-case names, `async` methods and Arrow
tables from [apache-arrow](https://arrow.apache.org/docs/js/). Everything on the other pages (the
[graph schema](graph-schema.md), the [supported Cypher](cypher.md#supported-cypher), the
[CSR index](csr.md) and [version pinning](lance.md)) applies here too. This page covers what's different, and the
[TypeScript API reference](typescript-api.md) lists every class, method and option.

The package is for server-side JavaScript: an API server, an agent backend, or the server half of a web UI.
It doesn't run in a browser. The engine is native code that reads Lance datasets from disk or object
storage, so a browser app sends its queries to your server, which runs them with lance-csr and returns the
results (see [Serving a web UI](#serving-a-web-ui)).

Every TypeScript snippet on this page comes from
[`nodejs/examples/`](https://github.com/hyperdimensionalcomputingai/lance-csr/tree/main/nodejs/examples),
which CI runs, except the one that writes data with LanceDB.

## Install

=== "npm"

    ```bash
    npm install lance-csr apache-arrow
    ```

=== "pnpm"

    ```bash
    pnpm add lance-csr apache-arrow
    ```

The package ships prebuilt binaries for **Linux x86-64 and aarch64** (glibc 2.28+: RHEL 8, Ubuntu 20.04 and
later) and **Apple Silicon** (macOS 11+), on **Node.js 22 or newer**. The x86-64 binaries need a CPU with AVX2.
npm installs only the binary for your platform, and no Rust toolchain is needed. Unlike the Python package, there
is no Windows binary; on Windows, use WSL2, where the Linux binary installs. Other platforms build from a
checkout (see [Contributing](contributing.md)).

`apache-arrow` (version 21) is a peer dependency: you install it, and lance-csr uses your copy, so the tables
you pass in and the results you get back are the same `Table` class your code imports.

!!! note "New in 0.13.1"

    The npm package was first published with lance-csr 0.13.1.

## Build an engine

`CypherEngine.create()` registers your datasets, optionally builds the CSR, and returns an engine that serves
many queries. It's `async` because building indexes takes time: the work runs on a background thread, so your
server keeps handling requests meanwhile. The options are the Python keyword arguments in camel case:

```typescript
--8<-- "nodejs/examples/guide.ts:engine"
```

Each dataset is one of:

An apache-arrow `Table` or `RecordBatch`
:   Held in memory and never changed. Create a new engine to replace it.

A `LanceDataset`: `{ uri, version?, storageOptions? }`
:   A Lance dataset on disk or object storage, read lazily as queries need it and pinned to `version` (the
    latest when you leave it out) until `checkoutLatest()`. `storageOptions` are Lance's object-store
    settings, such as credentials and region. See [Lance datasets](lance.md).

lance-csr only reads datasets. In TypeScript, [LanceDB](https://lancedb.github.io/lancedb/)'s
`@lancedb/lancedb` package writes them: each LanceDB table is a Lance dataset at `<database>/<table>.lance`.

<!-- skip-check -->
```typescript
import * as lancedb from "@lancedb/lancedb";
import { Int64, Table, Utf8, vectorFromArray } from "apache-arrow";
import { CypherEngine } from "lance-csr";

const db = await lancedb.connect("data");
await db.createTable(
  "person",
  new Table({
    id: vectorFromArray([1n, 2n], new Int64()),
    name: vectorFromArray(["Alice", "Bob"], new Utf8()),
  }),
);
await db.createTable(
  "knows",
  new Table({ src: vectorFromArray([1n], new Int64()), dst: vectorFromArray([2n], new Int64()) }),
);

const engine = await CypherEngine.create(config, {
  Person: { uri: "data/person.lance" },
  KNOWS: { uri: "data/knows.lance" },
});
```

!!! warning "Give columns explicit Arrow types"

    JavaScript has one number type, so apache-arrow's `tableFromArrays()`, and LanceDB when it infers a
    schema from plain objects, store numbers as 64-bit floats. Node keys and `src`/`dst` columns should be
    integers, which the [CSR](csr.md) needs (see
    [Key types and traversal performance](graph-schema.md#key-types-and-traversal-performance)). Build them
    from a `BigInt64Array` or `Int32Array`, as in the engine example above, or give the type explicitly:

    ```typescript
    --8<-- "nodejs/examples/guide.ts:explicit-types"
    ```

    apache-arrow also dictionary-encodes string arrays unless you give the `Utf8` type. lance-csr decodes
    dictionary strings in the tables you pass in, but a Lance dataset written with them keeps the encoding,
    and string functions such as `substring()` don't accept it. Write strings as `Utf8`, as above.

## Run queries

`execute()` returns the whole result as an apache-arrow `Table`. Parameters are a plain object, one entry
per `$name` in the query:

```typescript
--8<-- "nodejs/examples/guide.ts:query"
```

Parameter values are sent to the engine as follows:

| JavaScript value | Sent as |
| --- | --- |
| A whole `number` (`3`) | A 64-bit integer |
| Any other finite `number` (`1.5`) | A 64-bit float |
| A `bigint` | A 64-bit integer; values outside the signed 64-bit range are rejected |
| `string`, `boolean`, `null` | Themselves; `undefined` is sent as `null` |
| A `Date` | ISO 8601 text, which compares with timestamp and date columns |
| An array, or a plain object with string keys | A list or a map, converted value by value |

`NaN`, `Infinity`, functions, symbols, `Map`, `Set` and other class instances are rejected, as are cyclic or
very deeply nested containers.

Results use apache-arrow's JavaScript types: a 64-bit integer column holds `bigint` values, a string column
holds strings, and lists and maps come back as Arrow vectors and structs (`toJSON()` turns a row into a plain
object). Use `getChild(name)` for one column:

```typescript
--8<-- "nodejs/examples/guide.ts:types"
```

The other query methods mirror Python's. `explain(query, params, { analyze: true })` runs the query and
shows each operator's metrics, and `executeWithVectorRerank(query, vectorSearch)` ranks the rows by vector
similarity with a [`VectorSearch`](vector-search.md).

### Streaming large results

`executeBatches()` returns a `RecordBatchStream` that computes batches as you read them, the counterpart of
Python's `execute_batches()`. Read it with `for await`:

```typescript
--8<-- "nodejs/examples/guide.ts:stream"
```

Breaking out of the loop, or calling `stream.close()`, stops the query and releases what it holds. The
stream keeps its snapshot, so a `checkoutLatest()` while you read doesn't change its rows. `stream.schema`
is the result's Arrow schema.

### Timeouts and errors

The `timeoutMs` option sets one deadline, in milliseconds, for the whole query: planning, execution and, for
a stream, the pauses between reads. Python's `timeout` is the same deadline in seconds. Past it, the
query stops and frees its memory, and the engine stays usable.

Errors from the engine are `LanceCsrError`s. Their `code` says what went wrong, matching the exception the
Python package raises:

| `code` | Python exception | Raised when |
| --- | --- | --- |
| `InvalidArgument` | `ValueError` | The query, config, a parameter or an option is invalid |
| `NotImplemented` | `NotImplementedError` | The query is valid Cypher that isn't supported, such as a write clause |
| `Timeout` | `TimeoutError` | The query runs past `timeoutMs` |
| `Runtime` | `RuntimeError` | A dataset can't be read, or execution fails |

```typescript
--8<-- "nodejs/examples/guide.ts:errors"
```

An argument of the wrong type, such as a plain object where a `GraphConfig` belongs, throws a `TypeError`
before anything runs.

## One-off queries and SQL

`CypherQuery` runs a single query against the tables you pass to each call, as in Python. Its builder methods
return a new query:

```typescript
--8<-- "nodejs/examples/guide.ts:query-builder"
```

`SqlEngine` and `SqlQuery` run [SQL](sql.md), with `GRAPH_TABLE` for graph patterns.
`SqlEngine.fromEngine()` shares a `CypherEngine`'s tables and CSR:

```typescript
--8<-- "nodejs/examples/guide.ts:sql"
```

## From Python to TypeScript

Each Python class has a TypeScript class of the same name. The package's type definitions document every
method, so your editor shows the details on hover.

| Python | TypeScript |
| --- | --- |
| `GraphConfig.builder().node_table(...).rel_table(...).build()` | `GraphConfig.builder().nodeTable(...).relTable(...).build()` |
| `config.node_labels()`, `config.relationship_types()` | `config.nodeLabels()`, `config.relationshipTypes()` |
| `CypherEngine(config, datasets, use_csr=True, target_partitions=4, persist_csr=True)` | `await CypherEngine.create(config, datasets, { useCsr: true, targetPartitions: 4, persistCsr: true })` |
| `persist_csr="load"`, and the `RuntimeWarning` for a failed write | `persistCsr: "load"`, and a `LanceCsrWarning` process warning |
| `engine.execute(query, params, timeout=2.5)` | `await engine.execute(query, params, { timeoutMs: 2500 })` |
| `engine.execute_batches(query, params, timeout=...)`, a `RecordBatchReader` | `await engine.executeBatches(query, params, { timeoutMs })`, an async iterable `RecordBatchStream` |
| `engine.explain(query, params, analyze=True)` | `await engine.explain(query, params, { analyze: true })` |
| `engine.checkout_latest()`, `optimize_csr()`, `delete_stored_csr()` | `await engine.checkoutLatest()`, `optimizeCsr()`, `deleteStoredCsr()` |
| `engine.optimize_datasets(cleanup_older_than=timedelta(days=1))` | `await engine.optimizeDatasets({ cleanupOlderThanMs: 86_400_000 })` |
| `engine.csr_stats()`, `node_index_stats()` (keys such as `delta_edges`, `build_ms`) | `engine.csrStats()`, `nodeIndexStats()` (keys such as `deltaEdges`, `buildMs`) |
| `engine.export_graph(rel_types, directed=False, weight="w", combine_edges="min")`, a `GraphExport` of pyarrow arrays | `await engine.exportGraph(relTypes, { directed: false, weight: "w", combineEdges: "min" })`, a `GraphExport` of apache-arrow vectors |
| `engine.export_graph(weight={"ROAD": "km"}, default_weight=1.0)` | `await engine.exportGraph(undefined, { weight: { ROAD: "km" }, defaultWeight: 1 })` |
| `engine.export_subgraph(query, params, directed=False, weight="cost")` | `await engine.exportSubgraph(query, params, { directed: false, weight: "cost" })` |
| `engine.dataset_versions()`, `engine.config()` | `engine.datasetVersions()`, `engine.config()` |
| `engine.execute_with_vector_rerank(query, search)` | `await engine.executeWithVectorRerank(query, search)` |
| `CypherQuery(text).with_config(config).with_parameter(key, value)` | `new CypherQuery(text).withConfig(config).withParameter(key, value)` |
| `query.execute(datasets, strategy, timeout=...)` | `await query.execute(datasets, { strategy, timeoutMs })` |
| `query.execute_with_namespace(DirNamespace(uri), strategy)` | `await query.executeWithNamespace(new DirNamespace(uri), strategy)` |
| `query.explain(datasets)`, `query.to_sql(datasets, dialect)` | `await query.explain(datasets)`, `await query.toSql(datasets, dialect)` |
| `query.execute_with_vector_rerank(datasets, search)` | `await query.executeWithVectorRerank(datasets, search)` |
| `query.variables()`, `node_labels()`, `relationship_types()`, `query_text()`, `parameters()` | `query.variables()`, `nodeLabels()`, `relationshipTypes()`, `queryText()`, `parameters()` |
| `SqlEngine(datasets, config, use_csr=True)` | `await SqlEngine.create(datasets, { config, useCsr: true })` |
| `SqlEngine.from_engine(engine)` | `SqlEngine.fromEngine(engine)` |
| `engine.execute(sql, params, timeout=...)`, `explain(sql, params, analyze=...)` on `SqlEngine` | `await engine.execute(sql, params, { timeoutMs })`, `explain(sql, params, { analyze })` |
| `SqlQuery(sql).execute(datasets)`, `.explain(datasets)` | `await new SqlQuery(sql).execute(datasets)`, `.explain(datasets)` |
| `VectorSearch(column).query_vector(v).metric(m).top_k(k)` | `new VectorSearch(column).queryVector(v).metric(m).topK(k)` |
| `.include_distance(False)`, `.distance_column_name(n)`, `.use_lance_index(True)` | `.includeDistance(false)`, `.distanceColumnName(n)`, `.useLanceIndex(true)` |
| `search.search(table)` | `await search.search(table)` |
| `ExecutionStrategy`, `SqlDialect`, `DistanceMetric` | The same names and members |
| `ValueError`, `NotImplementedError`, `TimeoutError`, `RuntimeError` | `LanceCsrError` with `code` `InvalidArgument`, `NotImplemented`, `Timeout`, `Runtime` |
| `lance_csr.__version__` | `version` |

`CypherEngine.write_node_column` is Python only.

Where Python takes a PyArrow table or a `lance.LanceDataset`, TypeScript takes an apache-arrow `Table` or
`RecordBatch`, or a `LanceDataset` object (`{ uri, version, storageOptions }`). Python's `datetime` parameters
are TypeScript `Date`s.

## Serving a web UI

A web app built on lance-csr has two halves. The server holds one `CypherEngine` per set of datasets,
created once at startup and shared by every request: queries run concurrently on a native thread pool, not
on Node's event loop. Each request runs its query with a `timeoutMs`, so one slow query can't hold the server.
The browser then gets the result either as JSON (`result.toArray().map((row) => row.toJSON())`, after
converting `bigint` values, which `JSON.stringify` rejects) or as Arrow IPC bytes from apache-arrow's
`tableToIPC(result)`, which the browser reads back with `tableFromIPC`. IPC keeps the column types and is
much smaller for large results. When your Lance datasets change, call `checkoutLatest()` on the shared
engine; requests already running finish on the snapshot they started with.

### Drawing the whole graph

A query result suits a page that shows some nodes and their neighbours. A view of the whole graph, such as a
network map of every node, needs every edge at once, and renderers that draw large graphs on the GPU, such as
cosmos.gl, don't take your node keys. They take the nodes as positions in an array, `0` to `n - 1`, and each link
as a pair of those positions.

`exportGraph()` builds that form on the server. It reads only the key and endpoint columns, from the same
dataset versions your queries see, and numbers the nodes `0` to `n - 1`: their *dense ids*. The edges come back as
compressed sparse rows (CSR): `outIndices` lists every node's neighbours, one node after another, and
`outIndptr` holds `n + 1` offsets into it, so node `i`'s neighbours run from `outIndptr[i]` up to
`outIndptr[i + 1]`. One loop turns that into link pairs:

```ts
--8<-- "nodejs/examples/guide.ts:export"
```

`nodeKeys` holds each dense id's key, so the browser can match the rows of a query result, such as names or
other properties to show, to their points. `labelRanges` gives each label's range of dense ids, for example to
colour points by label. Send `links` as raw bytes, which the browser reads back with `new Uint32Array(buffer)`; for
millions of edges, that's far smaller than JSON.

`exportGraph(relTypes)` takes the relationship types to include; left out, it includes every type whose dataset
has integer endpoints. Edges whose endpoint key isn't in its node table are left out and counted in
`droppedEdges`, and an edge listed twice appears once. Keys and endpoints must be integer columns up to `int64`.
