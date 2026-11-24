/**
 * Running Cypher queries: one at a time, or many through a cached engine.
 *
 * @module
 */

import { type RecordBatch, type Schema, type Table, makeVector, tableFromIPC } from "apache-arrow";
import * as native from "../native.js";
import { GraphConfig } from "./config.ts";
import type { SqlDialect } from "./sql.ts";
import { DirNamespace } from "./namespace.ts";
import {
  type CsrStats,
  type Dataset,
  type DatasetOptimizeStats,
  type GraphExport,
  type NodeIndexStats,
  type Parameter,
  type Scalar,
  call,
  callSync,
  datasetInputs,
  parametersJson,
  timeoutOption,
  toTable,
} from "./types.ts";
import { VectorSearch } from "./vector.ts";

/**
 * How `CypherQuery.execute` runs a query. `DataFusion` (the default and only implemented
 * one) plans and runs it with DataFusion; `LanceNative` is reserved and fails with
 * `NotImplemented`.
 */
export const ExecutionStrategy = native.ExecutionStrategy;
export type ExecutionStrategy = native.ExecutionStrategy;

/** Options for {@link CypherEngine.create}. */
export interface CypherEngineOptions {
  /**
   * Build the experimental [CSR index](https://lance-csr.hdclabs.ai/csr/) for relationship
   * types with integer endpoints, and node indexes for labels with unique integer keys.
   */
  useCsr?: boolean;
  /**
   * A target from 1 to 1024 for DataFusion query partitioning; left out, DataFusion's
   * default. `checkoutLatest` preserves it. It is not a thread or memory limit.
   */
  targetPartitions?: number;
  /**
   * Experimental, needs `useCsr` unless `false`. With `true`, store each relationship
   * dataset's CSR inside the dataset's own directory, at `_lance_csr/<version>.arrow`,
   * and load it instead of building when an engine starts. Changes since the stored
   * version apply as a delta under the same 25% threshold as `checkoutLatest`; past it,
   * the CSR is rebuilt and stored again. Arrow inputs aren't stored. A dataset that
   * can't be written is built in memory, and the engine emits a process warning and
   * reports the error as `storeError` in `csrStats()`. With `"load"`, load stored files
   * the same way but never write to the datasets: a CSR that can't be loaded is built in
   * memory, as with `false`.
   */
  persistCsr?: boolean | "load";
}

/** Options for running a query. */
export interface ExecuteOptions {
  /** Milliseconds for the whole query; left out, no deadline. */
  timeoutMs?: number;
}

/** Options for explaining a query. */
export interface ExplainOptions {
  /**
   * Run the query, like SQL's `EXPLAIN ANALYZE`, and show each physical operator's
   * metrics (output rows, elapsed compute time and more) in place of the plain physical
   * plan. The query runs to completion; its rows are discarded.
   */
  analyze?: boolean;
}

/**
 * A query result read one batch at a time: an async iterable of Arrow record batches.
 *
 * Batches are computed as they are read. Scans, filters and projections stream with
 * bounded memory; joins, sorts and aggregations may still buffer. The stream keeps its
 * snapshot alive, so a concurrent `checkoutLatest` doesn't affect it. Breaking out of
 * a `for await` loop, or calling {@link RecordBatchStream.close}, stops the query.
 *
 * @example
 * ```ts
 * const stream = await engine.executeBatches("MATCH (p:Person) RETURN p.name");
 * for await (const batch of stream) {
 *   console.log(batch.numRows);
 * }
 * ```
 */
export class RecordBatchStream implements AsyncIterable<RecordBatch> {
  readonly #inner: native.RecordBatchStream;
  /** The schema of every batch. */
  readonly schema: Schema;

  /** @internal */
  constructor(inner: native.RecordBatchStream) {
    this.#inner = inner;
    this.schema = tableFromIPC(inner.schema()).schema;
  }

  /**
   * The next batch, or `null` at the end.
   *
   * @throws {@link LanceCsrError} with code `Timeout` once the query's deadline has
   * passed, or `Runtime` if execution fails.
   */
  async next(): Promise<RecordBatch | null> {
    const ipc = await call(() => this.#inner.next());
    if (ipc === null) {
      return null;
    }
    // A batch with no rows still arrives as one (empty) batch.
    return tableFromIPC(ipc).batches[0] ?? null;
  }

  /** Stop the query and release what it holds. Reading afterwards returns `null`. */
  async close(): Promise<void> {
    await this.#inner.close();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<RecordBatch> {
    try {
      for (let batch = await this.next(); batch !== null; batch = await this.next()) {
        yield batch;
      }
    } finally {
      await this.close();
    }
  }
}

function configSpec(config: unknown, name: string): native.GraphConfigSpec {
  if (!(config instanceof GraphConfig)) {
    throw new TypeError(`${name} must be a GraphConfig`);
  }
  return config._spec();
}

function vectorSpec(search: unknown): native.VectorSearchSpec {
  if (!(search instanceof VectorSearch)) {
    throw new TypeError("vectorSearch must be a VectorSearch");
  }
  return search._spec();
}

/**
 * Run many Cypher queries over the same datasets.
 *
 * The engine registers `datasets` once and reuses the catalog, the DataFusion session
 * and (with `useCsr`) the CSR index across queries. Queries can run concurrently.
 *
 * Lance datasets are read lazily, pinned to the version given (the latest when left
 * out): later writes, by this or another process, aren't visible until
 * {@link CypherEngine.checkoutLatest}. Each dataset is pinned separately, so versions
 * across tables are not a transactional snapshot. Arrow inputs are held in memory and
 * never change; create a new engine to replace them.
 *
 * `execute` and `executeBatches` reuse up to 128 unoptimized query templates per
 * snapshot, keyed by the exact query text and the names and types of scalar
 * parameters. Values are bound before optimization on every call. A successful
 * `checkoutLatest` clears the cache. See
 * [Repeated queries](https://lance-csr.hdclabs.ai/lance/#repeated-queries).
 *
 * @example
 * ```ts
 * import { tableFromArrays } from "apache-arrow";
 * import { CypherEngine, GraphConfig } from "lance-csr";
 *
 * const config = GraphConfig.builder()
 *   .nodeTable("Person", "id")
 *   .relTable("KNOWS", "Person", "Person", "src_id", "dst_id")
 *   .build();
 * const engine = await CypherEngine.create(config, {
 *   Person: tableFromArrays({ id: Int32Array.from([1, 2]), name: ["Alice", "Bob"] }),
 *   KNOWS: tableFromArrays({ src_id: Int32Array.from([1]), dst_id: Int32Array.from([2]) }),
 * });
 * const result = await engine.execute(
 *   "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
 * );
 * result.toArray().map((row) => row.toJSON()); // [{ "a.name": "Alice", "b.name": "Bob" }]
 * ```
 */
export class CypherEngine {
  readonly #inner: native.CypherEngine;
  readonly #config: GraphConfig;

  private constructor(inner: native.CypherEngine, config: GraphConfig) {
    this.#inner = inner;
    this.#config = config;
  }

  /**
   * Open the datasets and build the engine. Lance datasets are pinned, not read; with
   * `useCsr` the indexes are built here, on a background thread.
   *
   * @param config - The graph mapping.
   * @param datasets - The tables, keyed by node label or relationship type.
   * @param options - CSR and partitioning options.
   * @throws {@link LanceCsrError} with code `InvalidArgument` if `datasets` is empty, a
   * configured key or endpoint column is missing, `targetPartitions` is outside 1 to
   * 1024, `persistCsr` isn't `true`, `false` or `"load"`, or it is set without `useCsr`;
   * `Runtime` if a dataset can't be opened.
   * @throws `TypeError` if `config` isn't a `GraphConfig` or a dataset has an
   * unsupported type.
   */
  static async create(
    config: GraphConfig,
    datasets: Record<string, Dataset>,
    options: CypherEngineOptions = {},
  ): Promise<CypherEngine> {
    const spec = configSpec(config, "config");
    const inputs = datasetInputs(datasets);
    const { useCsr = false, targetPartitions, persistCsr = false } = options;
    if (targetPartitions !== undefined && typeof targetPartitions !== "number") {
      throw new TypeError("targetPartitions must be a number or undefined");
    }
    const modes = new Map<unknown, string>([
      [false, "off"],
      [true, "store"],
      ["load", "load"],
    ]);
    // Anything else reaches the native check as an unknown mode.
    const mode = modes.get(persistCsr) ?? "invalid";
    const inner = await call(() =>
      native.openCypherEngine(spec, inputs, useCsr, targetPartitions, mode),
    );
    const engine = new CypherEngine(inner, config);
    engine.#warnStoreErrors({});
    return engine;
  }

  // The engine only logs a failed CSR write; callers see it here once.
  #warnStoreErrors(before: Record<string, native.CsrStats>): void {
    for (const [relType, stats] of Object.entries(this.#inner.csrStats())) {
      const error = stats.storeError;
      if (error && error !== before[relType]?.storeError) {
        process.emitWarning(`Could not store the CSR of ${relType}: ${error}`, "LanceCsrWarning");
      }
    }
  }

  /** @internal The native engine, for `SqlEngine.fromEngine`. */
  _inner(): native.CypherEngine {
    return this.#inner;
  }

  /**
   * Run a Cypher query and return the whole result.
   *
   * The result is held in memory; use {@link CypherEngine.executeBatches} for large
   * outputs.
   *
   * @param query - The Cypher query.
   * @param params - Values for `$name` parameters; see {@link Parameter}.
   * @param options - `timeoutMs`: milliseconds for the whole query.
   * @returns The query result.
   * @throws {@link LanceCsrError} with code `InvalidArgument` if the query is invalid,
   * refers to an unknown label, compares or combines incompatible types, or a parameter
   * is unsupported, nested beyond 128 levels or cyclic; `NotImplemented` if the query
   * uses a Cypher feature that parses but isn't supported; `Timeout` if it runs past
   * `timeoutMs`; `Runtime` if execution fails.
   */
  async execute(
    query: string,
    params?: Record<string, Parameter> | null,
    options: ExecuteOptions = {},
  ): Promise<Table> {
    const json = parametersJson(params);
    const timeout = timeoutOption(options.timeoutMs);
    return toTable(await call(() => this.#inner.execute(query, json, timeout)));
  }

  /**
   * Run a Cypher query and stream the result.
   *
   * Planning errors reject the returned promise; execution errors reject while reading.
   *
   * @param query - The Cypher query.
   * @param params - Values for `$name` parameters, as for {@link CypherEngine.execute}.
   * @param options - `timeoutMs`: milliseconds for the whole query, including pauses
   * between reads.
   * @returns The result, one batch at a time.
   */
  async executeBatches(
    query: string,
    params?: Record<string, Parameter> | null,
    options: ExecuteOptions = {},
  ): Promise<RecordBatchStream> {
    const json = parametersJson(params);
    const timeout = timeoutOption(options.timeoutMs);
    return new RecordBatchStream(
      await call(() => this.#inner.executeBatches(query, json, timeout)),
    );
  }

  /**
   * The plans for a query, without running it unless `analyze` is set.
   *
   * @param query - The Cypher query.
   * @param params - Values for `$name` parameters, as for {@link CypherEngine.execute}.
   * @param options - `analyze`: run the query and show each operator's metrics.
   * @returns The graph logical plan, the DataFusion logical plan and the physical plan,
   * including pushed-down scans. With `analyze`, the physical plan's row is labelled
   * `physical_plan_with_metrics`.
   */
  async explain(
    query: string,
    params?: Record<string, Parameter> | null,
    options: ExplainOptions = {},
  ): Promise<string> {
    const json = parametersJson(params);
    return call(() => this.#inner.explain(query, json, options.analyze ?? false));
  }

  /**
   * Move every Lance dataset to its latest version.
   *
   * Updates the CSR, node indexes and statistics to match. Datasets whose version and
   * commit time are unchanged keep their open handle. The replacement is validated in
   * full before it's swapped in: on error nothing changes and the previous snapshot
   * stays usable. Queries already running finish on the snapshot they started with.
   * Arrow inputs are unchanged. Concurrent calls run one at a time, and
   * `targetPartitions` is preserved. See
   * [Lance datasets](https://lance-csr.hdclabs.ai/lance/#checkout-latest).
   */
  async checkoutLatest(): Promise<void> {
    const before = this.#inner.csrStats();
    await call(() => this.#inner.checkoutLatest());
    this.#warnStoreErrors(before);
  }

  /**
   * Compact the engine's Lance datasets, move it onto them and delete old versions.
   *
   * Every write to a Lance dataset makes a new version, and appends leave small fragments that
   * slow scans down. This runs the maintenance Lance and LanceDB recommend, and keeps the engine
   * in step with it. For each Lance dataset the engine reads, it opens the newest version
   * (including writes made through other handles, such as a LanceDB table), compacts small
   * fragments into larger ones, moves the engine to the new versions as `checkoutLatest` does,
   * then deletes versions older than `cleanupOlderThanMs`, except the latest and tagged ones.
   * The engine moves before the cleanup, so it never reads a deleted version. Lance indexes
   * aren't updated with new rows. See
   * [Compaction and old versions](https://lance-csr.hdclabs.ai/lance/#compaction-and-old-versions).
   *
   * @param options.cleanupOlderThanMs - Delete versions older than this many milliseconds
   *   (default 7 days, as in LanceDB's `optimize()`). Other readers pinned to an older version,
   *   including queries already running on this engine, fail once their version is deleted, so
   *   use a short window only when nothing else reads the datasets.
   * @returns Per Lance dataset, what compaction and cleanup did. Arrow inputs aren't listed.
   * @throws {@link LanceCsrError} `InvalidArgument` if `cleanupOlderThanMs` is negative;
   *   `Runtime` if a dataset can't be opened, compacted or cleaned up. Datasets compacted before
   *   the failure stay compacted.
   */
  async optimizeDatasets(
    options: { cleanupOlderThanMs?: number } = {},
  ): Promise<Record<string, DatasetOptimizeStats>> {
    const before = this.#inner.csrStats();
    const result = await call(() => this.#inner.optimizeDatasets(options.cleanupOlderThanMs));
    this.#warnStoreErrors(before);
    return result;
  }

  /**
   * Rebuild each CSR that carries a delta (experimental; needs `useCsr`).
   *
   * `checkoutLatest` applies Lance writes as a delta over the base CSR, and rebuilds on
   * its own only once pending changes pass 25% of the base. This rebuilds every CSR that
   * carries a delta now, at the version the engine is pinned to. It scans every edge
   * again, so call it after a bulk load or at a quiet time. It doesn't move to newer
   * versions: call `checkoutLatest` first for that. Query results never change. See
   * [Rebuilding the CSR](https://lance-csr.hdclabs.ai/csr/#rebuilding-the-csr).
   */
  async optimizeCsr(): Promise<void> {
    const before = this.#inner.csrStats();
    await call(() => this.#inner.optimizeCsr());
    this.#warnStoreErrors(before);
  }

  /**
   * Delete the CSR files stored with `persistCsr` (experimental).
   *
   * Removes the `_lance_csr` folder of every Lance relationship dataset this engine
   * reads. The engine keeps the CSR it already holds in memory, so queries aren't
   * affected. See [Storing the CSR](https://lance-csr.hdclabs.ai/csr/#storing-the-csr).
   *
   * @returns The number of files deleted; 0 when nothing was stored.
   * @throws {@link LanceCsrError} `InvalidArgument` if the engine was created with
   *   `persistCsr: "load"`, which never writes to its datasets.
   */
  async deleteStoredCsr(): Promise<number> {
    return call(() => this.#inner.deleteStoredCsr());
  }

  /**
   * Statistics for each node index (experimental; needs `useCsr`).
   *
   * A label is missing when its keys aren't unique integers; relationships that end at
   * it then use a join.
   */
  nodeIndexStats(): Record<string, NodeIndexStats> {
    return callSync(() => this.#inner.nodeIndexStats());
  }

  /**
   * Statistics for each CSR adjacency index (experimental; needs `useCsr`), keyed by
   * relationship type; empty without `useCsr`. An identical result after
   * `checkoutLatest` means the snapshot itself didn't change. See
   * [Inspecting the CSR](https://lance-csr.hdclabs.ai/csr/#inspecting-the-csr).
   */
  csrStats(): Record<string, CsrStats> {
    return callSync(() => this.#inner.csrStats()) as Record<string, CsrStats>;
  }

  /**
   * The whole graph as typed arrays over dense node ids, for drawing it in a browser.
   *
   * Nodes are numbered `0` to `numNodes - 1` and each edge is a pair of those numbers, the form
   * GPU graph renderers take. Reads the key column of each node table and the endpoint columns
   * of each relationship table from the engine's pinned snapshot, as queries see it, with or
   * without `useCsr`. Lance datasets are scanned for just those columns. See
   * [Drawing the whole graph](https://lance-csr.hdclabs.ai/typescript/#drawing-the-whole-graph).
   *
   * @param relTypes - Relationship types to include, case-insensitive. Left out, every type
   *   whose dataset has integer endpoint columns. The nodes are those of the labels at either
   *   end of the included types.
   * @param options.directed - `true` (the default) returns out-edge and in-edge arrays; `false`
   *   returns one set of arrays listing every edge in both directions.
   * @param options.weight - A numeric column of the relationship tables to export as float64
   *   edge weights, in `outWeights` and `inWeights`; every included type must have it,
   *   case-insensitive. Left out, no weights are exported, and the export needs no memory for
   *   them.
   * @param options.combineEdges - How an edge listed more than once combines its weights: `"sum"`
   *   (the default) adds them, as for counts or amounts; `"min"` keeps the smallest, as for
   *   distances; `"max"` the largest. In an undirected export, `a -> b` and `b -> a` are one edge
   *   and combine the same way. Ignored without `weight`.
   * @throws {@link LanceCsrError} `InvalidArgument` if a relationship type or label isn't in
   *   the config, has no dataset, or a key or endpoint column is missing or isn't an integer
   *   type up to `int64`, the weight column is missing or isn't numeric, `combineEdges` isn't
   *   one of the three rules, or there's nothing to export; `Runtime` if a node key appears twice within a label, a weight is null, or a scan
   *   fails.
   */
  async exportGraph(
    relTypes?: string[],
    options: { directed?: boolean; weight?: string; combineEdges?: "sum" | "min" | "max" } = {},
  ): Promise<GraphExport> {
    const g = await call(() =>
      this.#inner.exportGraph(
        relTypes,
        options.directed ?? true,
        options.weight,
        options.combineEdges,
      ),
    );
    return {
      numNodes: g.numNodes,
      directed: g.directed,
      nodeKeys: makeVector(g.nodeKeys),
      labelRanges: Object.fromEntries(
        Object.entries(g.labelRanges).map(([label, [start, end]]) => [label, [start!, end!]]),
      ),
      outIndptr: makeVector(g.outIndptr),
      outIndices: makeVector(g.outIndices),
      inIndptr: g.inIndptr ? makeVector(g.inIndptr) : null,
      inIndices: g.inIndices ? makeVector(g.inIndices) : null,
      outWeights: g.outWeights ? makeVector(g.outWeights) : null,
      inWeights: g.inWeights ? makeVector(g.inWeights) : null,
      droppedEdges: g.droppedEdges,
    };
  }

  /** The pinned version of each Lance dataset, keyed by dataset name. Arrow inputs aren't listed. */
  datasetVersions(): Record<string, number> {
    return callSync(() => this.#inner.datasetVersions());
  }

  /** The engine's graph config. */
  config(): GraphConfig {
    return this.#config;
  }

  /**
   * Run a Cypher query, then rank its rows by vector similarity.
   *
   * @param query - The Cypher query. It must return the column `vectorSearch` ranks by.
   * @param vectorSearch - The ranking.
   * @returns At most `topK` rows, closest first.
   */
  async executeWithVectorRerank(query: string, vectorSearch: VectorSearch): Promise<Table> {
    const spec = vectorSpec(vectorSearch);
    return toTable(await call(() => this.#inner.executeWithVectorRerank(query, spec)));
  }
}

/**
 * One parsed Cypher query, run against the datasets passed to each call.
 *
 * Builder methods return a new query. Datasets are read into memory for each call; use
 * {@link CypherEngine} to register them once and run many queries.
 *
 * @example
 * ```ts
 * const result = await new CypherQuery("MATCH (p:Person) WHERE p.age > $min RETURN p.name")
 *   .withConfig(config)
 *   .withParameter("min", 30)
 *   .execute({ Person: people });
 * ```
 */
export class CypherQuery {
  readonly #query: string;
  readonly #parsed: native.ParsedCypher;
  readonly #config: GraphConfig | undefined;
  readonly #parameters: Record<string, Parameter>;

  /**
   * @param queryText - The Cypher query.
   * @throws {@link LanceCsrError} with code `InvalidArgument` if `queryText` doesn't parse.
   */
  constructor(queryText: string);
  /** @internal */
  constructor(
    queryText: string,
    parsed: native.ParsedCypher,
    config: GraphConfig | undefined,
    parameters: Record<string, Parameter>,
  );
  constructor(
    queryText: string,
    parsed?: native.ParsedCypher,
    config?: GraphConfig,
    parameters: Record<string, Parameter> = {},
  ) {
    if (typeof queryText !== "string") {
      throw new TypeError("queryText must be a string");
    }
    this.#query = queryText;
    this.#parsed = parsed ?? callSync(() => native.parseCypher(queryText));
    this.#config = config;
    this.#parameters = parameters;
  }

  #spec(): native.CypherQuerySpec {
    return {
      query: this.#query,
      config: this.#config?._spec(),
      parameters: parametersJson(this.#parameters),
    };
  }

  /**
   * Set the graph config. Required before running the query.
   *
   * @returns A new query with the config set.
   */
  withConfig(config: GraphConfig): CypherQuery {
    configSpec(config, "config");
    return new CypherQuery(this.#query, this.#parsed, config, this.#parameters);
  }

  /**
   * Bind `$key` to `value`.
   *
   * @param key - The parameter name, without `$`.
   * @param value - The value, of a type that {@link CypherEngine.execute} accepts in `params`.
   * @returns A new query with the parameter bound.
   */
  withParameter(key: string, value: Parameter): CypherQuery {
    const parameters = { ...this.#parameters, [key]: value };
    parametersJson(parameters);
    return new CypherQuery(this.#query, this.#parsed, this.#config, parameters);
  }

  /** The query text. */
  queryText(): string {
    return this.#query;
  }

  /** The bound parameters, as the engine receives them (a `Date` becomes ISO text). */
  parameters(): Record<string, Scalar> {
    return JSON.parse(parametersJson(this.#parameters) ?? "{}") as Record<string, Scalar>;
  }

  /**
   * Run the query.
   *
   * @param datasets - The tables, keyed by node label or relationship type. They are
   * read into memory for this call.
   * @param options - `strategy` (default `ExecutionStrategy.DataFusion`) and `timeoutMs`.
   * @returns The query result.
   * @throws {@link LanceCsrError} with code `InvalidArgument` if the query is invalid or a
   * dataset is missing; `NotImplemented` for an unsupported Cypher feature or
   * `ExecutionStrategy.LanceNative`; `Timeout` past `timeoutMs`; `Runtime` if execution
   * fails.
   */
  async execute(
    datasets: Record<string, Dataset>,
    options: ExecuteOptions & { strategy?: ExecutionStrategy } = {},
  ): Promise<Table> {
    const spec = this.#spec();
    const inputs = datasetInputs(datasets);
    const timeout = timeoutOption(options.timeoutMs);
    return toTable(
      await call(() => native.cypherQueryExecute(spec, inputs, options.strategy, timeout)),
    );
  }

  /**
   * Run the query, reading each label from `<baseUri>/<label>.lance`.
   *
   * @param namespace - Where the datasets are.
   * @param strategy - How to run the query; the default is `ExecutionStrategy.DataFusion`.
   */
  async executeWithNamespace(
    namespace: DirNamespace,
    strategy?: ExecutionStrategy,
  ): Promise<Table> {
    if (!(namespace instanceof DirNamespace)) {
      throw new TypeError("namespace must be a DirNamespace");
    }
    const spec = this.#spec();
    return toTable(
      await call(() => native.cypherQueryExecuteWithNamespace(spec, namespace.baseUri, strategy)),
    );
  }

  /**
   * The plans for the query, without running it.
   *
   * @returns The graph logical plan, the DataFusion logical plan and the physical plan.
   */
  async explain(datasets: Record<string, Dataset>): Promise<string> {
    const spec = this.#spec();
    const inputs = datasetInputs(datasets);
    return call(() => native.cypherQueryExplain(spec, inputs));
  }

  /**
   * Export the query as SQL joins. Only supported expressions are exported. See
   * [From Cypher to SQL](https://lance-csr.hdclabs.ai/sql/#from-cypher-to-sql).
   *
   * @param datasets - The tables, keyed by node label or relationship type; their
   * schemas shape the SQL.
   * @param dialect - The SQL dialect; the default is `SqlDialect.Default`.
   * @throws {@link LanceCsrError} with code `NotImplemented` if a runtime-only Cypher
   * function has no faithful SQL translation.
   */
  async toSql(datasets: Record<string, Dataset>, dialect?: SqlDialect): Promise<string> {
    const spec = this.#spec();
    const inputs = datasetInputs(datasets);
    return call(() => native.cypherQueryToSql(spec, inputs, dialect));
  }

  /**
   * Run the query, then rank its rows by vector similarity.
   *
   * This is the common GraphRAG pattern: find candidates by graph traversal, then rerank
   * them by similarity. With `VectorSearch.useLanceIndex(true)` and Lance datasets, the
   * Lance index is searched first instead. See
   * [Vector search](https://lance-csr.hdclabs.ai/vector-search/).
   *
   * @param vectorSearch - The ranking. Its column is a returned column, such as `"d.embedding"`.
   * @returns At most `topK` rows, closest first.
   */
  async executeWithVectorRerank(
    datasets: Record<string, Dataset>,
    vectorSearch: VectorSearch,
  ): Promise<Table> {
    const spec = this.#spec();
    const inputs = datasetInputs(datasets);
    const search = vectorSpec(vectorSearch);
    return toTable(
      await call(() => native.cypherQueryExecuteWithVectorRerank(spec, inputs, search)),
    );
  }

  /** The variables the query uses, including `UNION` branches, unordered. */
  variables(): string[] {
    return [...this.#parsed.variables];
  }

  /** The node labels the query uses, including `UNION` branches, unordered. */
  nodeLabels(): string[] {
    return [...this.#parsed.nodeLabels];
  }

  /** The relationship types the query uses, with `UNION` branches, unordered. */
  relationshipTypes(): string[] {
    return [...this.#parsed.relationshipTypes];
  }

  toString(): string {
    return `CypherQuery(${JSON.stringify(this.#query)})`;
  }
}
