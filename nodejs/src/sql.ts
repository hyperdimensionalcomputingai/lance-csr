/**
 * Running SQL queries: one at a time, or many through a cached engine.
 *
 * @module
 */

import type { Table } from "apache-arrow";
import * as native from "../native.js";
import { GraphConfig } from "./config.ts";
import type { CypherEngine, ExecuteOptions, ExplainOptions } from "./cypher.ts";
import {
  type Dataset,
  type Parameter,
  call,
  datasetInputs,
  parametersJson,
  timeoutOption,
  toTable,
} from "./types.ts";

/**
 * The SQL dialect `CypherQuery.toSql` writes: `Default` (generic DataFusion SQL),
 * `Spark`, `PostgreSql`, `MySql` or `Sqlite`.
 */
export const SqlDialect = native.SqlDialect;
export type SqlDialect = native.SqlDialect;

/**
 * One SQL query, run against the datasets passed to each call.
 *
 * Each dataset is a table under its lower-cased name, read into memory for the call.
 * Use {@link SqlEngine} to register datasets once and run many queries.
 */
export class SqlQuery {
  readonly #sql: string;

  /** @param sql - The SQL query. */
  constructor(sql: string) {
    if (typeof sql !== "string") {
      throw new TypeError("sql must be a string");
    }
    this.#sql = sql;
  }

  /** The query text. */
  sql(): string {
    return this.#sql;
  }

  /**
   * Run the query.
   *
   * @param datasets - The tables, keyed by the names the query uses.
   * @throws {@link LanceCsrError} with code `InvalidArgument` if the query is invalid, or
   * `Runtime` if execution fails.
   */
  async execute(datasets: Record<string, Dataset>): Promise<Table> {
    const inputs = datasetInputs(datasets);
    return toTable(await call(() => native.sqlQueryExecute(this.#sql, inputs)));
  }

  /** The logical and physical plans for the query, without running it. */
  async explain(datasets: Record<string, Dataset>): Promise<string> {
    const inputs = datasetInputs(datasets);
    return call(() => native.sqlQueryExplain(this.#sql, inputs));
  }

  toString(): string {
    return `SqlQuery(${JSON.stringify(this.#sql)})`;
  }
}

/** Options for {@link SqlEngine.create}. */
export interface SqlEngineOptions {
  /** The graph `GRAPH_TABLE` patterns match. Without one, `GRAPH_TABLE` fails with `InvalidArgument`. */
  config?: GraphConfig;
  /** Build the experimental CSR index, as for `CypherEngine`. Needs a `config`. */
  useCsr?: boolean;
}

/**
 * Run many SQL queries on the same datasets, with graph patterns given a graph.
 *
 * Each dataset is a table under its lower-cased name. Lance datasets are scanned in
 * place, pinned to the version given, with column and filter pushdown; Arrow inputs are
 * held in memory. With a `config`, a `GRAPH_TABLE` clause embeds a Cypher graph pattern,
 * and with `useCsr` its hops run on the CSR. See
 * [Graph patterns in SQL](https://lance-csr.hdclabs.ai/sql/#graph-patterns-in-sql).
 *
 * Plans are cached per snapshot, as `CypherEngine` caches Cypher templates.
 *
 * @example
 * ```ts
 * const engine = await SqlEngine.create({
 *   person: tableFromArrays({ id: Int32Array.from([1, 2]), name: ["Alice", "Bob"] }),
 *   knows: tableFromArrays({ src: Int32Array.from([1]), dst: Int32Array.from([2]) }),
 * });
 * const result = await engine.execute(
 *   "SELECT p.name FROM person p JOIN knows k ON p.id = k.src WHERE k.dst = $dst",
 *   { dst: 2 },
 * );
 * ```
 */
export class SqlEngine {
  readonly #inner: native.SqlEngine;

  private constructor(inner: native.SqlEngine) {
    this.#inner = inner;
  }

  /**
   * Open the datasets and build the engine.
   *
   * @param datasets - The tables, keyed by the names queries use; with a `config`, also
   * by node label or relationship type, as for `CypherEngine`.
   * @throws {@link LanceCsrError} with code `InvalidArgument` if `datasets` is empty,
   * `useCsr` is set without a `config`, or a configured column is missing.
   */
  static async create(
    datasets: Record<string, Dataset>,
    options: SqlEngineOptions = {},
  ): Promise<SqlEngine> {
    const { config, useCsr = false } = options;
    if (config !== undefined && !(config instanceof GraphConfig)) {
      throw new TypeError("config must be a GraphConfig or undefined");
    }
    const inputs = datasetInputs(datasets);
    return new SqlEngine(await call(() => native.openSqlEngine(inputs, config?._spec(), useCsr)));
  }

  /**
   * SQL over a `CypherEngine`'s snapshot, sharing its datasets, CSR and statistics.
   *
   * `checkoutLatest` on either engine moves only that engine: the other keeps the
   * snapshot it holds.
   */
  static fromEngine(engine: CypherEngine): SqlEngine {
    // Checked structurally: importing CypherEngine here would make the modules circular.
    if (typeof (engine as { _inner?: unknown })?._inner !== "function") {
      throw new TypeError("engine must be a CypherEngine");
    }
    return new SqlEngine(native.SqlEngine.fromEngine(engine._inner()));
  }

  /**
   * Run a SQL query and return the whole result.
   *
   * @param sql - The SQL query.
   * @param params - Values for `$name` parameters, as for `CypherEngine.execute`.
   * @param options - `timeoutMs`: milliseconds for the whole query.
   * @throws {@link LanceCsrError} with code `InvalidArgument` if the query is invalid,
   * `Timeout` past `timeoutMs`, or `Runtime` if execution fails.
   */
  async execute(
    sql: string,
    params?: Record<string, Parameter> | null,
    options: ExecuteOptions = {},
  ): Promise<Table> {
    const json = parametersJson(params);
    const timeout = timeoutOption(options.timeoutMs);
    return toTable(await call(() => this.#inner.execute(sql, json, timeout)));
  }

  /**
   * The plans for a SQL query, without running it unless `analyze` is set.
   *
   * @returns The logical and physical plans; with `analyze`, the physical plan with
   * each operator's metrics.
   */
  async explain(
    sql: string,
    params?: Record<string, Parameter> | null,
    options: ExplainOptions = {},
  ): Promise<string> {
    const json = parametersJson(params);
    return call(() => this.#inner.explain(sql, json, options.analyze ?? false));
  }

  /** Move every Lance dataset to its latest version, as `CypherEngine.checkoutLatest` does. */
  async checkoutLatest(): Promise<void> {
    await call(() => this.#inner.checkoutLatest());
  }
}
