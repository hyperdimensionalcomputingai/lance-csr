## Classes

<a id="cypherengine"></a>

### CypherEngine

Run many Cypher queries over the same datasets.

The engine registers `datasets` once and reuses the catalog, the DataFusion session
and (with `useCsr`) the CSR index across queries. Queries can run concurrently.

Lance datasets are read lazily, pinned to the version given (the latest when left
out): later writes, by this or another process, aren't visible until
[CypherEngine.checkoutLatest](#checkoutlatest). Each dataset is pinned separately, so versions
across tables are not a transactional snapshot. Arrow inputs are held in memory and
never change; create a new engine to replace them.

`execute` and `executeBatches` reuse up to 128 unoptimized query templates per
snapshot, keyed by the exact query text and the names and types of scalar
parameters. Values are bound before optimization on every call. A successful
`checkoutLatest` clears the cache. See
[Repeated queries](https://lance-csr.hdclabs.ai/lance/#repeated-queries).

#### Example

```ts
import { tableFromArrays } from "apache-arrow";
import { CypherEngine, GraphConfig } from "lance-csr";

const config = GraphConfig.builder()
  .nodeTable("Person", "id")
  .relTable("KNOWS", "Person", "Person", "src_id", "dst_id")
  .build();
const engine = await CypherEngine.create(config, {
  Person: tableFromArrays({ id: Int32Array.from([1, 2]), name: ["Alice", "Bob"] }),
  KNOWS: tableFromArrays({ src_id: Int32Array.from([1]), dst_id: Int32Array.from([2]) }),
});
const result = await engine.execute(
  "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
);
result.toArray().map((row) => row.toJSON()); // [{ "a.name": "Alice", "b.name": "Bob" }]
```

#### Methods

<a id="checkoutlatest"></a>

##### checkoutLatest()

```ts
checkoutLatest(): Promise<void>;
```

Move every Lance dataset to its latest version.

Updates the CSR, node indexes and statistics to match. Datasets whose version and
commit time are unchanged keep their open handle. The replacement is validated in
full before it's swapped in: on error nothing changes and the previous snapshot
stays usable. Queries already running finish on the snapshot they started with.
Arrow inputs are unchanged. Concurrent calls run one at a time, and
`targetPartitions` is preserved. See
[Lance datasets](https://lance-csr.hdclabs.ai/lance/#checkout-latest).

###### Returns

`Promise`\<`void`\>

<a id="config"></a>

##### config()

```ts
config(): GraphConfig;
```

The engine's graph config.

###### Returns

[`GraphConfig`](#graphconfig)

<a id="csrstats"></a>

##### csrStats()

```ts
csrStats(): Record<string, CsrStats>;
```

Statistics for each CSR adjacency index (experimental; needs `useCsr`), keyed by
relationship type; empty without `useCsr`. An identical result after
`checkoutLatest` means the snapshot itself didn't change. See
[Inspecting the CSR](https://lance-csr.hdclabs.ai/csr/#inspecting-the-csr).

###### Returns

`Record`\<`string`, [`CsrStats`](#csrstats-1)\>

<a id="datasetversions"></a>

##### datasetVersions()

```ts
datasetVersions(): Record<string, number>;
```

The pinned version of each Lance dataset, keyed by dataset name. Arrow inputs aren't listed.

###### Returns

`Record`\<`string`, `number`\>

<a id="deletestoredcsr"></a>

##### deleteStoredCsr()

```ts
deleteStoredCsr(): Promise<number>;
```

Delete the CSR files stored with `persistCsr` (experimental).

Removes the `_lance_csr` folder of every Lance relationship dataset this engine
reads. The engine keeps the CSR it already holds in memory, so queries aren't
affected. See [Storing the CSR](https://lance-csr.hdclabs.ai/csr/#storing-the-csr).

###### Returns

`Promise`\<`number`\>

The number of files deleted; 0 when nothing was stored.

###### Throws

[LanceCsrError](#lancecsrerror) `InvalidArgument` if the engine was created with
  `persistCsr: "load"`, which never writes to its datasets.

<a id="execute"></a>

##### execute()

```ts
execute(
   query, 
   params?, 
   options?
): Promise<Table<any>>;
```

Run a Cypher query and return the whole result.

The result is held in memory; use [CypherEngine.executeBatches](#executebatches) for large
outputs.

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `query` | `string` | The Cypher query. |
| `params?` | `Record`\<`string`, [`Parameter`](#parameter)\> \| `null` | Values for `$name` parameters; see [Parameter](#parameter). |
| `options?` | [`ExecuteOptions`](#executeoptions) | `timeoutMs`: milliseconds for the whole query. |

###### Returns

`Promise`\<`Table`\<`any`\>\>

The query result.

###### Throws

[LanceCsrError](#lancecsrerror) with code `InvalidArgument` if the query is invalid,
refers to an unknown label, compares or combines incompatible types, or a parameter
is unsupported, nested beyond 128 levels or cyclic; `NotImplemented` if the query
uses a Cypher feature that parses but isn't supported; `Timeout` if it runs past
`timeoutMs`; `Runtime` if execution fails.

<a id="executebatches"></a>

##### executeBatches()

```ts
executeBatches(
   query, 
   params?, 
   options?
): Promise<RecordBatchStream>;
```

Run a Cypher query and stream the result.

Planning errors reject the returned promise; execution errors reject while reading.

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `query` | `string` | The Cypher query. |
| `params?` | `Record`\<`string`, [`Parameter`](#parameter)\> \| `null` | Values for `$name` parameters, as for [CypherEngine.execute](#execute). |
| `options?` | [`ExecuteOptions`](#executeoptions) | `timeoutMs`: milliseconds for the whole query, including pauses between reads. |

###### Returns

`Promise`\<[`RecordBatchStream`](#recordbatchstream)\>

The result, one batch at a time.

<a id="executewithvectorrerank"></a>

##### executeWithVectorRerank()

```ts
executeWithVectorRerank(query, vectorSearch): Promise<Table<any>>;
```

Run a Cypher query, then rank its rows by vector similarity.

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `query` | `string` | The Cypher query. It must return the column `vectorSearch` ranks by. |
| `vectorSearch` | [`VectorSearch`](#vectorsearch) | The ranking. |

###### Returns

`Promise`\<`Table`\<`any`\>\>

At most `topK` rows, closest first.

<a id="explain"></a>

##### explain()

```ts
explain(
   query, 
   params?, 
   options?
): Promise<string>;
```

The plans for a query, without running it unless `analyze` is set.

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `query` | `string` | The Cypher query. |
| `params?` | `Record`\<`string`, [`Parameter`](#parameter)\> \| `null` | Values for `$name` parameters, as for [CypherEngine.execute](#execute). |
| `options?` | [`ExplainOptions`](#explainoptions) | `analyze`: run the query and show each operator's metrics. |

###### Returns

`Promise`\<`string`\>

The graph logical plan, the DataFusion logical plan and the physical plan,
including pushed-down scans. With `analyze`, the physical plan's row is labelled
`physical_plan_with_metrics`.

<a id="exportgraph"></a>

##### exportGraph()

```ts
exportGraph(relTypes?, options?): Promise<GraphExport>;
```

The whole graph as typed arrays over dense node ids, for drawing it in a browser.

Nodes are numbered `0` to `numNodes - 1` and each edge is a pair of those numbers, the form
GPU graph renderers take. Reads the key column of each node table and the endpoint columns
of each relationship table from the engine's pinned snapshot, as queries see it, with or
without `useCsr`. Lance datasets are scanned for just those columns. See
[Drawing the whole graph](https://lance-csr.hdclabs.ai/typescript/#drawing-the-whole-graph).

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `relTypes?` | `string`[] | Relationship types to include, case-insensitive. Left out, every type whose dataset has integer endpoint columns. The nodes are those of the labels at either end of the included types. |
| `options?` | \{ `directed?`: `boolean`; \} | - |
| `options.directed?` | `boolean` | `true` (the default) returns out-edge and in-edge arrays; `false` returns one set of arrays listing every edge in both directions. |

###### Returns

`Promise`\<[`GraphExport`](#graphexport)\>

###### Throws

[LanceCsrError](#lancecsrerror) `InvalidArgument` if a relationship type or label isn't in
  the config, has no dataset, or a key or endpoint column is missing or isn't an integer
  type up to `int64`, or there's nothing to export; `Runtime` if a node key appears twice
  within a label, or a scan fails.

<a id="nodeindexstats"></a>

##### nodeIndexStats()

```ts
nodeIndexStats(): Record<string, NodeIndexStats>;
```

Statistics for each node index (experimental; needs `useCsr`).

A label is missing when its keys aren't unique integers; relationships that end at
it then use a join.

###### Returns

`Record`\<`string`, [`NodeIndexStats`](#nodeindexstats-1)\>

<a id="optimizecsr"></a>

##### optimizeCsr()

```ts
optimizeCsr(): Promise<void>;
```

Rebuild each CSR that carries a delta (experimental; needs `useCsr`).

`checkoutLatest` applies Lance writes as a delta over the base CSR, and rebuilds on
its own only once pending changes pass 25% of the base. This rebuilds every CSR that
carries a delta now, at the version the engine is pinned to. It scans every edge
again, so call it after a bulk load or at a quiet time. It doesn't move to newer
versions: call `checkoutLatest` first for that. Query results never change. See
[Rebuilding the CSR](https://lance-csr.hdclabs.ai/csr/#rebuilding-the-csr).

###### Returns

`Promise`\<`void`\>

<a id="optimizedatasets"></a>

##### optimizeDatasets()

```ts
optimizeDatasets(options?): Promise<Record<string, DatasetOptimizeStats>>;
```

Compact the engine's Lance datasets, move it onto them and delete old versions.

Every write to a Lance dataset makes a new version, and appends leave small fragments that
slow scans down. This runs the maintenance Lance and LanceDB recommend, and keeps the engine
in step with it. For each Lance dataset the engine reads, it opens the newest version
(including writes made through other handles, such as a LanceDB table), compacts small
fragments into larger ones, moves the engine to the new versions as `checkoutLatest` does,
then deletes versions older than `cleanupOlderThanMs`, except the latest and tagged ones.
The engine moves before the cleanup, so it never reads a deleted version. Lance indexes
aren't updated with new rows. See
[Compaction and old versions](https://lance-csr.hdclabs.ai/lance/#compaction-and-old-versions).

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `options?` | \{ `cleanupOlderThanMs?`: `number`; \} | - |
| `options.cleanupOlderThanMs?` | `number` | Delete versions older than this many milliseconds (default 7 days, as in LanceDB's `optimize()`). Other readers pinned to an older version, including queries already running on this engine, fail once their version is deleted, so use a short window only when nothing else reads the datasets. |

###### Returns

`Promise`\<`Record`\<`string`, [`DatasetOptimizeStats`](#datasetoptimizestats)\>\>

Per Lance dataset, what compaction and cleanup did. Arrow inputs aren't listed.

###### Throws

[LanceCsrError](#lancecsrerror) `InvalidArgument` if `cleanupOlderThanMs` is negative;
  `Runtime` if a dataset can't be opened, compacted or cleaned up. Datasets compacted before
  the failure stay compacted.

<a id="create"></a>

##### create()

```ts
static create(
   config, 
   datasets, 
   options?
): Promise<CypherEngine>;
```

Open the datasets and build the engine. Lance datasets are pinned, not read; with
`useCsr` the indexes are built here, on a background thread.

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `config` | [`GraphConfig`](#graphconfig) | The graph mapping. |
| `datasets` | `Record`\<`string`, [`Dataset`](#dataset)\> | The tables, keyed by node label or relationship type. |
| `options?` | [`CypherEngineOptions`](#cypherengineoptions) | CSR and partitioning options. |

###### Returns

`Promise`\<[`CypherEngine`](#cypherengine)\>

###### Throws

[LanceCsrError](#lancecsrerror) with code `InvalidArgument` if `datasets` is empty, a
configured key or endpoint column is missing, `targetPartitions` is outside 1 to
1024, `persistCsr` isn't `true`, `false` or `"load"`, or it is set without `useCsr`;
`Runtime` if a dataset can't be opened.

###### Throws

`TypeError` if `config` isn't a `GraphConfig` or a dataset has an
unsupported type.

***

<a id="cypherquery"></a>

### CypherQuery

One parsed Cypher query, run against the datasets passed to each call.

Builder methods return a new query. Datasets are read into memory for each call; use
[CypherEngine](#cypherengine) to register them once and run many queries.

#### Example

```ts
const result = await new CypherQuery("MATCH (p:Person) WHERE p.age > $min RETURN p.name")
  .withConfig(config)
  .withParameter("min", 30)
  .execute({ Person: people });
```

#### Constructors

<a id="constructor"></a>

##### Constructor

```ts
new CypherQuery(queryText): CypherQuery;
```

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `queryText` | `string` | The Cypher query. |

###### Returns

[`CypherQuery`](#cypherquery)

###### Throws

[LanceCsrError](#lancecsrerror) with code `InvalidArgument` if `queryText` doesn't parse.

#### Methods

<a id="execute-1"></a>

##### execute()

```ts
execute(datasets, options?): Promise<Table<any>>;
```

Run the query.

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `datasets` | `Record`\<`string`, [`Dataset`](#dataset)\> | The tables, keyed by node label or relationship type. They are read into memory for this call. |
| `options?` | [`ExecuteOptions`](#executeoptions) & `object` | `strategy` (default `ExecutionStrategy.DataFusion`) and `timeoutMs`. |

###### Returns

`Promise`\<`Table`\<`any`\>\>

The query result.

###### Throws

[LanceCsrError](#lancecsrerror) with code `InvalidArgument` if the query is invalid or a
dataset is missing; `NotImplemented` for an unsupported Cypher feature or
`ExecutionStrategy.LanceNative`; `Timeout` past `timeoutMs`; `Runtime` if execution
fails.

<a id="executewithnamespace"></a>

##### executeWithNamespace()

```ts
executeWithNamespace(namespace, strategy?): Promise<Table<any>>;
```

Run the query, reading each label from `<baseUri>/<label>.lance`.

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `namespace` | [`DirNamespace`](#dirnamespace) | Where the datasets are. |
| `strategy?` | `ExecutionStrategy` | How to run the query; the default is `ExecutionStrategy.DataFusion`. |

###### Returns

`Promise`\<`Table`\<`any`\>\>

<a id="executewithvectorrerank-1"></a>

##### executeWithVectorRerank()

```ts
executeWithVectorRerank(datasets, vectorSearch): Promise<Table<any>>;
```

Run the query, then rank its rows by vector similarity.

This is the common GraphRAG pattern: find candidates by graph traversal, then rerank
them by similarity. With `VectorSearch.useLanceIndex(true)` and Lance datasets, the
Lance index is searched first instead. See
[Vector search](https://lance-csr.hdclabs.ai/vector-search/).

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `datasets` | `Record`\<`string`, [`Dataset`](#dataset)\> | - |
| `vectorSearch` | [`VectorSearch`](#vectorsearch) | The ranking. Its column is a returned column, such as `"d.embedding"`. |

###### Returns

`Promise`\<`Table`\<`any`\>\>

At most `topK` rows, closest first.

<a id="explain-1"></a>

##### explain()

```ts
explain(datasets): Promise<string>;
```

The plans for the query, without running it.

###### Parameters

| Parameter | Type |
| ------ | ------ |
| `datasets` | `Record`\<`string`, [`Dataset`](#dataset)\> |

###### Returns

`Promise`\<`string`\>

The graph logical plan, the DataFusion logical plan and the physical plan.

<a id="nodelabels"></a>

##### nodeLabels()

```ts
nodeLabels(): string[];
```

The node labels the query uses, including `UNION` branches, unordered.

###### Returns

`string`[]

<a id="parameters"></a>

##### parameters()

```ts
parameters(): Record<string, Scalar>;
```

The bound parameters, as the engine receives them (a `Date` becomes ISO text).

###### Returns

`Record`\<`string`, [`Scalar`](#scalar)\>

<a id="querytext"></a>

##### queryText()

```ts
queryText(): string;
```

The query text.

###### Returns

`string`

<a id="relationshiptypes"></a>

##### relationshipTypes()

```ts
relationshipTypes(): string[];
```

The relationship types the query uses, with `UNION` branches, unordered.

###### Returns

`string`[]

<a id="tosql"></a>

##### toSql()

```ts
toSql(datasets, dialect?): Promise<string>;
```

Export the query as SQL joins. Only supported expressions are exported. See
[From Cypher to SQL](https://lance-csr.hdclabs.ai/sql/#from-cypher-to-sql).

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `datasets` | `Record`\<`string`, [`Dataset`](#dataset)\> | The tables, keyed by node label or relationship type; their schemas shape the SQL. |
| `dialect?` | `SqlDialect` | The SQL dialect; the default is `SqlDialect.Default`. |

###### Returns

`Promise`\<`string`\>

###### Throws

[LanceCsrError](#lancecsrerror) with code `NotImplemented` if a runtime-only Cypher
function has no faithful SQL translation.

<a id="tostring"></a>

##### toString()

```ts
toString(): string;
```

###### Returns

`string`

<a id="variables"></a>

##### variables()

```ts
variables(): string[];
```

The variables the query uses, including `UNION` branches, unordered.

###### Returns

`string`[]

<a id="withconfig"></a>

##### withConfig()

```ts
withConfig(config): CypherQuery;
```

Set the graph config. Required before running the query.

###### Parameters

| Parameter | Type |
| ------ | ------ |
| `config` | [`GraphConfig`](#graphconfig) |

###### Returns

[`CypherQuery`](#cypherquery)

A new query with the config set.

<a id="withparameter"></a>

##### withParameter()

```ts
withParameter(key, value): CypherQuery;
```

Bind `$key` to `value`.

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `key` | `string` | The parameter name, without `$`. |
| `value` | [`Parameter`](#parameter) | The value, of a type that [CypherEngine.execute](#execute) accepts in `params`. |

###### Returns

[`CypherQuery`](#cypherquery)

A new query with the parameter bound.

***

<a id="dirnamespace"></a>

### DirNamespace

Resolves each table name to the Lance dataset `<baseUri>/<name>.lance`.

Use it with `CypherQuery.executeWithNamespace`.

#### Constructors

<a id="constructor-1"></a>

##### Constructor

```ts
new DirNamespace(baseUri): DirNamespace;
```

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `baseUri` | `string` | The directory that holds the datasets. |

###### Returns

[`DirNamespace`](#dirnamespace)

#### Properties

<a id="baseuri"></a>

##### baseUri

```ts
readonly baseUri: string;
```

The directory that holds the datasets.

***

<a id="graphconfig"></a>

### GraphConfig

An immutable mapping from tables to a property graph.

Create one with [GraphConfig.builder](#builder); there is no public constructor. See
[Graph schema](https://lance-csr.hdclabs.ai/graph-schema/).

#### Methods

<a id="nodelabels-1"></a>

##### nodeLabels()

```ts
nodeLabels(): string[];
```

The configured node labels, lower-cased, in no particular order.

###### Returns

`string`[]

<a id="relationshiptypes-1"></a>

##### relationshipTypes()

```ts
relationshipTypes(): string[];
```

The configured relationship types, lower-cased, in no particular order.

###### Returns

`string`[]

<a id="tostring-1"></a>

##### toString()

```ts
toString(): string;
```

###### Returns

`string`

<a id="builder"></a>

##### builder()

```ts
static builder(): GraphConfigBuilder;
```

Start a new, empty builder: the same as `new GraphConfigBuilder()`.

###### Returns

[`GraphConfigBuilder`](#graphconfigbuilder-1)

***

<a id="graphconfigbuilder-1"></a>

### GraphConfigBuilder

Builds a [GraphConfig](#graphconfig) from node and relationship tables.

`new GraphConfigBuilder()` is the same as `GraphConfig.builder()`. Every method returns
a new builder and leaves this one unchanged.

#### Example

```ts
const config = GraphConfig.builder()
  .nodeTable("Person", "id")
  .relTable("KNOWS", "Person", "Person", "src_id", "dst_id")
  .build();
```

#### Constructors

<a id="constructor-2"></a>

##### Constructor

```ts
new GraphConfigBuilder(): GraphConfigBuilder;
```

###### Returns

[`GraphConfigBuilder`](#graphconfigbuilder-1)

#### Methods

<a id="build"></a>

##### build()

```ts
build(): GraphConfig;
```

Validate the tables and return the config.

###### Returns

[`GraphConfig`](#graphconfig)

###### Throws

[LanceCsrError](#lancecsrerror) with code `InvalidArgument` if the configuration is
invalid, for example when a relationship's endpoint label has no node table.

<a id="nodetable"></a>

##### nodeTable()

```ts
nodeTable(label, primaryKey): GraphConfigBuilder;
```

Add a node table.

The nodes labelled `label` are the rows of the dataset named `label`, each
identified by its `primaryKey` column.

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `label` | `string` | The node label, and the name of its dataset (for example `"Person"`). |
| `primaryKey` | `string` | The column that identifies each node. |

###### Returns

[`GraphConfigBuilder`](#graphconfigbuilder-1)

A new builder with the node table added.

<a id="reltable"></a>

##### relTable()

```ts
relTable(
   relType, 
   fromTable, 
   toTable, 
   src, 
   dst
): GraphConfigBuilder;
```

Add a relationship table.

The edges of type `relType` are the rows of the dataset named `relType`, from
`fromTable` nodes to `toTable` nodes. Both node tables must be added by the time
[GraphConfigBuilder.build](#build) runs.

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `relType` | `string` | The relationship type, and the name of its dataset (for example `"KNOWS"`). |
| `fromTable` | `string` | The node table at the source end. |
| `toTable` | `string` | The node table at the target end. |
| `src` | `string` | The column holding the source node's primary key. |
| `dst` | `string` | The column holding the target node's primary key. |

###### Returns

[`GraphConfigBuilder`](#graphconfigbuilder-1)

A new builder with the relationship table added.

***

<a id="lancecsrerror"></a>

### LanceCsrError

An error from the query engine. Arguments of the wrong type raise `TypeError` instead.

#### Extends

- `Error`

#### Constructors

<a id="constructor-3"></a>

##### Constructor

```ts
new LanceCsrError(
   code, 
   message, 
   options?
): LanceCsrError;
```

###### Parameters

| Parameter | Type |
| ------ | ------ |
| `code` | [`ErrorCode`](#errorcode) |
| `message` | `string` |
| `options?` | `ErrorOptions` |

###### Returns

[`LanceCsrError`](#lancecsrerror)

###### Overrides

`Error.constructor`

#### Properties

<a id="code"></a>

##### code

```ts
readonly code: ErrorCode;
```

What went wrong; see [ErrorCode](#errorcode).

***

<a id="recordbatchstream"></a>

### RecordBatchStream

A query result read one batch at a time: an async iterable of Arrow record batches.

Batches are computed as they are read. Scans, filters and projections stream with
bounded memory; joins, sorts and aggregations may still buffer. The stream keeps its
snapshot alive, so a concurrent `checkoutLatest` doesn't affect it. Breaking out of
a `for await` loop, or calling [RecordBatchStream.close](#close), stops the query.

#### Example

```ts
const stream = await engine.executeBatches("MATCH (p:Person) RETURN p.name");
for await (const batch of stream) {
  console.log(batch.numRows);
}
```

#### Implements

- `AsyncIterable`\<`RecordBatch`\>

#### Properties

<a id="schema"></a>

##### schema

```ts
readonly schema: Schema;
```

The schema of every batch.

#### Methods

<a id="asynciterator"></a>

##### \[asyncIterator\]()

```ts
[asyncIterator](): AsyncIterator<RecordBatch<any>>;
```

###### Returns

`AsyncIterator`\<`RecordBatch`\<`any`\>\>

###### Implementation of

`AsyncIterable.[asyncIterator]`

<a id="close"></a>

##### close()

```ts
close(): Promise<void>;
```

Stop the query and release what it holds. Reading afterwards returns `null`.

###### Returns

`Promise`\<`void`\>

<a id="next"></a>

##### next()

```ts
next(): Promise<RecordBatch<any> | null>;
```

The next batch, or `null` at the end.

###### Returns

`Promise`\<`RecordBatch`\<`any`\> \| `null`\>

###### Throws

[LanceCsrError](#lancecsrerror) with code `Timeout` once the query's deadline has
passed, or `Runtime` if execution fails.

***

<a id="sqlengine"></a>

### SqlEngine

Run many SQL queries on the same datasets, with graph patterns given a graph.

Each dataset is a table under its lower-cased name. Lance datasets are scanned in
place, pinned to the version given, with column and filter pushdown; Arrow inputs are
held in memory. With a `config`, a `GRAPH_TABLE` clause embeds a Cypher graph pattern,
and with `useCsr` its hops run on the CSR. See
[Graph patterns in SQL](https://lance-csr.hdclabs.ai/sql/#graph-patterns-in-sql).

Plans are cached per snapshot, as `CypherEngine` caches Cypher templates.

#### Example

```ts
const engine = await SqlEngine.create({
  person: tableFromArrays({ id: Int32Array.from([1, 2]), name: ["Alice", "Bob"] }),
  knows: tableFromArrays({ src: Int32Array.from([1]), dst: Int32Array.from([2]) }),
});
const result = await engine.execute(
  "SELECT p.name FROM person p JOIN knows k ON p.id = k.src WHERE k.dst = $dst",
  { dst: 2 },
);
```

#### Methods

<a id="checkoutlatest-1"></a>

##### checkoutLatest()

```ts
checkoutLatest(): Promise<void>;
```

Move every Lance dataset to its latest version, as `CypherEngine.checkoutLatest` does.

###### Returns

`Promise`\<`void`\>

<a id="execute-2"></a>

##### execute()

```ts
execute(
   sql, 
   params?, 
   options?
): Promise<Table<any>>;
```

Run a SQL query and return the whole result.

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `sql` | `string` | The SQL query. |
| `params?` | `Record`\<`string`, [`Parameter`](#parameter)\> \| `null` | Values for `$name` parameters, as for `CypherEngine.execute`. |
| `options?` | [`ExecuteOptions`](#executeoptions) | `timeoutMs`: milliseconds for the whole query. |

###### Returns

`Promise`\<`Table`\<`any`\>\>

###### Throws

[LanceCsrError](#lancecsrerror) with code `InvalidArgument` if the query is invalid,
`Timeout` past `timeoutMs`, or `Runtime` if execution fails.

<a id="explain-2"></a>

##### explain()

```ts
explain(
   sql, 
   params?, 
   options?
): Promise<string>;
```

The plans for a SQL query, without running it unless `analyze` is set.

###### Parameters

| Parameter | Type |
| ------ | ------ |
| `sql` | `string` |
| `params?` | `Record`\<`string`, [`Parameter`](#parameter)\> \| `null` |
| `options?` | [`ExplainOptions`](#explainoptions) |

###### Returns

`Promise`\<`string`\>

The logical and physical plans; with `analyze`, the physical plan with
each operator's metrics.

<a id="create-1"></a>

##### create()

```ts
static create(datasets, options?): Promise<SqlEngine>;
```

Open the datasets and build the engine.

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `datasets` | `Record`\<`string`, [`Dataset`](#dataset)\> | The tables, keyed by the names queries use; with a `config`, also by node label or relationship type, as for `CypherEngine`. |
| `options?` | [`SqlEngineOptions`](#sqlengineoptions) | - |

###### Returns

`Promise`\<[`SqlEngine`](#sqlengine)\>

###### Throws

[LanceCsrError](#lancecsrerror) with code `InvalidArgument` if `datasets` is empty,
`useCsr` is set without a `config`, or a configured column is missing.

<a id="fromengine"></a>

##### fromEngine()

```ts
static fromEngine(engine): SqlEngine;
```

SQL over a `CypherEngine`'s snapshot, sharing its datasets, CSR and statistics.

`checkoutLatest` on either engine moves only that engine: the other keeps the
snapshot it holds.

###### Parameters

| Parameter | Type |
| ------ | ------ |
| `engine` | [`CypherEngine`](#cypherengine) |

###### Returns

[`SqlEngine`](#sqlengine)

***

<a id="sqlquery"></a>

### SqlQuery

One SQL query, run against the datasets passed to each call.

Each dataset is a table under its lower-cased name, read into memory for the call.
Use [SqlEngine](#sqlengine) to register datasets once and run many queries.

#### Constructors

<a id="constructor-4"></a>

##### Constructor

```ts
new SqlQuery(sql): SqlQuery;
```

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `sql` | `string` | The SQL query. |

###### Returns

[`SqlQuery`](#sqlquery)

#### Methods

<a id="execute-3"></a>

##### execute()

```ts
execute(datasets): Promise<Table<any>>;
```

Run the query.

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `datasets` | `Record`\<`string`, [`Dataset`](#dataset)\> | The tables, keyed by the names the query uses. |

###### Returns

`Promise`\<`Table`\<`any`\>\>

###### Throws

[LanceCsrError](#lancecsrerror) with code `InvalidArgument` if the query is invalid, or
`Runtime` if execution fails.

<a id="explain-3"></a>

##### explain()

```ts
explain(datasets): Promise<string>;
```

The logical and physical plans for the query, without running it.

###### Parameters

| Parameter | Type |
| ------ | ------ |
| `datasets` | `Record`\<`string`, [`Dataset`](#dataset)\> |

###### Returns

`Promise`\<`string`\>

<a id="sql"></a>

##### sql()

```ts
sql(): string;
```

The query text.

###### Returns

`string`

<a id="tostring-2"></a>

##### toString()

```ts
toString(): string;
```

###### Returns

`string`

***

<a id="vectorsearch"></a>

### VectorSearch

An immutable builder for vector similarity search.

Every method except [VectorSearch.search](#search) returns a new builder. Pass the
builder to `search` to rank an Arrow table, or to `executeWithVectorRerank` on
`CypherEngine` or `CypherQuery` to rerank a Cypher result. This is separate from the
`vector_distance()` and `vector_similarity()` Cypher functions. See
[Vector search](https://lance-csr.hdclabs.ai/vector-search/).

#### Example

```ts
const search = new VectorSearch("d.embedding")
  .queryVector([0.1, 0.2, 0.3])
  .metric(DistanceMetric.Cosine)
  .topK(5);
```

#### Constructors

<a id="constructor-5"></a>

##### Constructor

```ts
new VectorSearch(column): VectorSearch;
```

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `column` | `string` | The vector column to rank by, such as `"embedding"` or `"d.embedding"`. |

###### Returns

[`VectorSearch`](#vectorsearch)

#### Methods

<a id="distancecolumnname"></a>

##### distanceColumnName()

```ts
distanceColumnName(name): VectorSearch;
```

Name the distance column; the default is `"_distance"`.

###### Parameters

| Parameter | Type |
| ------ | ------ |
| `name` | `string` |

###### Returns

[`VectorSearch`](#vectorsearch)

<a id="includedistance"></a>

##### includeDistance()

```ts
includeDistance(include): VectorSearch;
```

Add the distance as a column (the default), or leave it out.

###### Parameters

| Parameter | Type |
| ------ | ------ |
| `include` | `boolean` |

###### Returns

[`VectorSearch`](#vectorsearch)

<a id="metric"></a>

##### metric()

```ts
metric(metric): VectorSearch;
```

Set the distance metric; the default is `DistanceMetric.L2`.

###### Parameters

| Parameter | Type |
| ------ | ------ |
| `metric` | `DistanceMetric` |

###### Returns

[`VectorSearch`](#vectorsearch)

<a id="queryvector"></a>

##### queryVector()

```ts
queryVector(vector): VectorSearch;
```

Set the vector to compare with. Required before searching.

###### Parameters

| Parameter | Type |
| ------ | ------ |
| `vector` | `ArrayLike`\<`number`\> |

###### Returns

[`VectorSearch`](#vectorsearch)

<a id="search"></a>

##### search()

```ts
search(table): Promise<Table<any>>;
```

Brute-force search `table`.

###### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `table` | `Table`\<`any`\> \| `RecordBatch`\<`any`\> | The rows to search, with the vector column. |

###### Returns

`Promise`\<`Table`\<`any`\>\>

At most `topK` rows, sorted by distance, closest first.

###### Throws

[LanceCsrError](#lancecsrerror) with code `InvalidArgument` if no query vector is set or
the column doesn't exist, or `Runtime` if the search fails.

<a id="topk"></a>

##### topK()

```ts
topK(k): VectorSearch;
```

Keep at most `k` rows; the default is 10.

###### Parameters

| Parameter | Type |
| ------ | ------ |
| `k` | `number` |

###### Returns

[`VectorSearch`](#vectorsearch)

<a id="uselanceindex"></a>

##### useLanceIndex()

```ts
useLanceIndex(enabled): VectorSearch;
```

Set whether reranking queries a Lance ANN index first; `false` by default.

When the datasets are Lance datasets, `CypherQuery.executeWithVectorRerank`
searches the Lance approximate-nearest-neighbour index first and then runs the
Cypher query on the top-k rows. This can be much faster on large datasets, but it
can change the results when the query also filters, because the filter then applies
to the top-k rows only.

###### Parameters

| Parameter | Type |
| ------ | ------ |
| `enabled` | `boolean` |

###### Returns

[`VectorSearch`](#vectorsearch)

## Interfaces

<a id="csrstats-1"></a>

### CsrStats

Statistics for one CSR adjacency index, from `CypherEngine.csrStats()`.

#### Properties

| Property | Type | Description |
| ------ | ------ | ------ |
| <a id="property-baseversion"></a> `baseVersion?` | `number` | The relationship dataset version of the last full build; absent for Arrow input. |
| <a id="property-buildms"></a> `buildMs` | `number` | Wall time of that build or delta, both directions, in milliseconds. |
| <a id="property-deltaedges"></a> `deltaEdges` | `number` | Edges appended since the last full build, held outside the base CSR. |
| <a id="property-edges"></a> `edges` | `number` | Live indexed edges (null endpoints are skipped). |
| <a id="property-storeerror"></a> `storeError?` | `string` | Why the base CSR couldn't be stored with `persistCsr: true`; absent when it was, or when nothing is stored. It stays set while the engine keeps that base, through deltas. |
| <a id="property-tombstones"></a> `tombstones` | `number` | Edges removed since the last full build, held outside the base CSR. |
| <a id="property-unindexededges"></a> `unindexedEdges` | `number` | Live edges outside the base CSR: appended since its last full build and not since removed. |
| <a id="property-update"></a> `update` | `"build"` \| `"rebuild"` \| `"loaded"` \| `"delta"` \| `"reused"` | How the last build, `checkoutLatest()` or `optimizeCsr()` produced it. |
| <a id="property-version"></a> `version?` | `number` | The relationship dataset version it reflects; absent for Arrow input. |

***

<a id="cypherengineoptions"></a>

### CypherEngineOptions

Options for [CypherEngine.create](#create).

#### Properties

| Property | Type | Description |
| ------ | ------ | ------ |
| <a id="property-persistcsr"></a> `persistCsr?` | `boolean` \| `"load"` | Experimental, needs `useCsr` unless `false`. With `true`, store each relationship dataset's CSR inside the dataset's own directory, at `_lance_csr/<version>.arrow`, and load it instead of building when an engine starts. Changes since the stored version apply as a delta under the same 25% threshold as `checkoutLatest`; past it, the CSR is rebuilt and stored again. Arrow inputs aren't stored. A dataset that can't be written is built in memory, and the engine emits a process warning and reports the error as `storeError` in `csrStats()`. With `"load"`, load stored files the same way but never write to the datasets: a CSR that can't be loaded is built in memory, as with `false`. |
| <a id="property-targetpartitions"></a> `targetPartitions?` | `number` | A target from 1 to 1024 for DataFusion query partitioning; left out, DataFusion's default. `checkoutLatest` preserves it. It is not a thread or memory limit. |
| <a id="property-usecsr"></a> `useCsr?` | `boolean` | Build the experimental [CSR index](https://lance-csr.hdclabs.ai/csr/) for relationship types with integer endpoints, and node indexes for labels with unique integer keys. |

***

<a id="datasetoptimizestats"></a>

### DatasetOptimizeStats

What `CypherEngine.optimizeDatasets()` did to one Lance dataset.

#### Properties

| Property | Type | Description |
| ------ | ------ | ------ |
| <a id="property-bytesremoved"></a> `bytesRemoved` | `number` | Bytes of files cleanup deleted. |
| <a id="property-fragmentsadded"></a> `fragmentsAdded` | `number` | Fragments compaction wrote in their place. |
| <a id="property-fragmentsremoved"></a> `fragmentsRemoved` | `number` | Small fragments compaction rewrote. |
| <a id="property-oldversions"></a> `oldVersions` | `number` | Old versions cleanup deleted. |

***

<a id="executeoptions"></a>

### ExecuteOptions

Options for running a query.

#### Properties

| Property | Type | Description |
| ------ | ------ | ------ |
| <a id="property-timeoutms"></a> `timeoutMs?` | `number` | Milliseconds for the whole query; left out, no deadline. |

***

<a id="explainoptions"></a>

### ExplainOptions

Options for explaining a query.

#### Properties

| Property | Type | Description |
| ------ | ------ | ------ |
| <a id="property-analyze"></a> `analyze?` | `boolean` | Run the query, like SQL's `EXPLAIN ANALYZE`, and show each physical operator's metrics (output rows, elapsed compute time and more) in place of the plain physical plan. The query runs to completion; its rows are discarded. |

***

<a id="graphexport"></a>

### GraphExport

A graph as compressed sparse rows over dense node ids, from `CypherEngine.exportGraph()`.

Nodes are numbered `0` to `numNodes - 1`, and the neighbours of node `i` are
`outIndices[outIndptr[i]]` up to `outIndices[outIndptr[i + 1]]`. `nodeKeys` maps each dense
id back to its node's key. The vectors wrap typed arrays built in Rust. See
[Drawing the whole graph](https://lance-csr.hdclabs.ai/typescript/#drawing-the-whole-graph).

#### Properties

| Property | Type | Description |
| ------ | ------ | ------ |
| <a id="property-directed"></a> `directed` | `boolean` | Whether the arrays describe a directed graph. |
| <a id="property-droppededges"></a> `droppedEdges` | `number` | Edges left out because an endpoint key isn't in its node table. Edges with a null endpoint are skipped and not counted. |
| <a id="property-inindices"></a> `inIndices` | `Vector`\<`Uint64`\> \| `null` | Each node's in-neighbours, sorted, without duplicates; `null` for an undirected graph. |
| <a id="property-inindptr"></a> `inIndptr` | `Vector`\<`Uint64`\> \| `null` | Offsets into `inIndices`; `null` for an undirected graph. |
| <a id="property-labelranges"></a> `labelRanges` | `Record`\<`string`, \[`number`, `number`\]\> | Each node label's dense ids, as `[start, end]` with `end` excluded. Labels get consecutive ranges in name order, so equal keys in two labels stay distinct. |
| <a id="property-nodekeys"></a> `nodeKeys` | `Vector`\<`Int64`\> | The key of each dense id. Each label's nodes are in the order its table is scanned; rows with a null key are left out. |
| <a id="property-numnodes"></a> `numNodes` | `number` | Number of nodes. Dense ids run from 0 to `numNodes - 1`. |
| <a id="property-outindices"></a> `outIndices` | `Vector`\<`Uint64`\> | Each node's out-neighbours, sorted, without duplicates. An undirected graph lists every edge in both directions and a self-loop once. |
| <a id="property-outindptr"></a> `outIndptr` | `Vector`\<`Uint64`\> | Offsets into `outIndices`, `numNodes + 1` entries. |

***

<a id="lancedataset"></a>

### LanceDataset

A Lance dataset on local disk or object storage, opened by URI.

`CypherEngine` and `SqlEngine` read it lazily, pinned to `version` (the latest when
left out). `CypherQuery` and `SqlQuery` read it into memory for each call.

#### Properties

| Property | Type | Description |
| ------ | ------ | ------ |
| <a id="property-storageoptions"></a> `storageOptions?` | `Record`\<`string`, `string`\> | Lance storage options, such as object-store credentials and region. |
| <a id="property-uri"></a> `uri` | `string` | Where the dataset is, such as `"data/person.lance"` or `"s3://bucket/person.lance"`. |
| <a id="property-version-1"></a> `version?` | `number` | The version to read; the latest when left out. |

***

<a id="nodeindexstats-1"></a>

### NodeIndexStats

Statistics for one node index, from `CypherEngine.nodeIndexStats()`.

#### Properties

| Property | Type | Description |
| ------ | ------ | ------ |
| <a id="property-buildms-1"></a> `buildMs` | `number` | Wall time of the build, in milliseconds. |
| <a id="property-rows"></a> `rows` | `number` | Indexed node rows. |
| <a id="property-version-2"></a> `version?` | `number` | The node dataset version it reflects; absent for Arrow input. |

***

<a id="sqlengineoptions"></a>

### SqlEngineOptions

Options for [SqlEngine.create](#create-1).

#### Properties

| Property | Type | Description |
| ------ | ------ | ------ |
| <a id="property-config"></a> `config?` | [`GraphConfig`](#graphconfig) | The graph `GRAPH_TABLE` patterns match. Without one, `GRAPH_TABLE` fails with `InvalidArgument`. |
| <a id="property-usecsr-1"></a> `useCsr?` | `boolean` | Build the experimental CSR index, as for `CypherEngine`. Needs a `config`. |

## Type Aliases

<a id="dataset"></a>

### Dataset

```ts
type Dataset = Table | RecordBatch | LanceDataset;
```

A table to query: an Arrow `Table` or `RecordBatch` held in memory, or a Lance dataset.

***

<a id="distancemetric"></a>

### DistanceMetric

```ts
type DistanceMetric = native.DistanceMetric;
```

The distance [VectorSearch](#vectorsearch) ranks rows by; smaller is closer. `L2` is Euclidean
distance, `Cosine` is 1 − cosine similarity and `Dot` is the negative dot product.

***

<a id="errorcode"></a>

### ErrorCode

```ts
type ErrorCode = "InvalidArgument" | "NotImplemented" | "Timeout" | "Runtime";
```

The kinds of [LanceCsrError](#lancecsrerror), matching the Python package's exceptions:

- `InvalidArgument` (Python `ValueError`): an invalid query, config, parameter or option.
- `NotImplemented` (Python `NotImplementedError`): a Cypher feature that parses but isn't supported.
- `Timeout` (Python `TimeoutError`): the query ran past its `timeoutMs`.
- `Runtime` (Python `RuntimeError`): a dataset couldn't be read, or execution failed.

***

<a id="executionstrategy"></a>

### ExecutionStrategy

```ts
type ExecutionStrategy = native.ExecutionStrategy;
```

How `CypherQuery.execute` runs a query. `DataFusion` (the default and only implemented
one) plans and runs it with DataFusion; `LanceNative` is reserved and fails with
`NotImplemented`.

***

<a id="parameter"></a>

### Parameter

```ts
type Parameter = 
  | number
  | bigint
  | string
  | boolean
  | Date
  | null
  | Parameter[]
  | {
[key: string]: Parameter;
};
```

A value for a `$name` query parameter.

Whole numbers are sent as integers and other numbers as floats. A `bigint` must fit
in a signed 64-bit integer. A `Date` is sent as ISO 8601 text, which compares with
timestamp and date columns.

***

<a id="scalar"></a>

### Scalar

```ts
type Scalar = 
  | number
  | string
  | boolean
  | null
  | Scalar[]
  | {
[key: string]: Scalar;
};
```

A bound parameter value, as `CypherQuery.parameters()` returns it.

***

<a id="sqldialect"></a>

### SqlDialect

```ts
type SqlDialect = native.SqlDialect;
```

The SQL dialect `CypherQuery.toSql` writes: `Default` (generic DataFusion SQL),
`Spark`, `PostgreSql`, `MySql` or `Sqlite`.

## Variables

<a id="distancemetric-1"></a>

### DistanceMetric

```ts
const DistanceMetric: typeof DistanceMetric = native.DistanceMetric;
```

The distance [VectorSearch](#vectorsearch) ranks rows by; smaller is closer. `L2` is Euclidean
distance, `Cosine` is 1 − cosine similarity and `Dot` is the negative dot product.

***

<a id="executionstrategy-1"></a>

### ExecutionStrategy

```ts
const ExecutionStrategy: typeof ExecutionStrategy = native.ExecutionStrategy;
```

How `CypherQuery.execute` runs a query. `DataFusion` (the default and only implemented
one) plans and runs it with DataFusion; `LanceNative` is reserved and fails with
`NotImplemented`.

***

<a id="sqldialect-1"></a>

### SqlDialect

```ts
const SqlDialect: typeof SqlDialect = native.SqlDialect;
```

The SQL dialect `CypherQuery.toSql` writes: `Default` (generic DataFusion SQL),
`Spark`, `PostgreSql`, `MySql` or `Sqlite`.

***

<a id="version"></a>

### version

```ts
const version: string = VERSION;
```

The package version.
