/**
 * Types shared by the public classes, and the helpers that convert them for the native module.
 *
 * @module
 */

import {
  type Int64,
  RecordBatch,
  Table,
  type Uint64,
  type Vector,
  tableFromIPC,
  tableToIPC,
} from "apache-arrow";
import type * as native from "../native.js";

/**
 * A Lance dataset on local disk or object storage, opened by URI.
 *
 * `CypherEngine` and `SqlEngine` read it lazily, pinned to `version` (the latest when
 * left out). `CypherQuery` and `SqlQuery` read it into memory for each call.
 */
export interface LanceDataset {
  /** Where the dataset is, such as `"data/person.lance"` or `"s3://bucket/person.lance"`. */
  uri: string;
  /** The version to read; the latest when left out. */
  version?: number;
  /** Lance storage options, such as object-store credentials and region. */
  storageOptions?: Record<string, string>;
}

/** A table to query: an Arrow `Table` or `RecordBatch` held in memory, or a Lance dataset. */
export type Dataset = Table | RecordBatch | LanceDataset;

/**
 * A value for a `$name` query parameter.
 *
 * Whole numbers are sent as integers and other numbers as floats. A `bigint` must fit
 * in a signed 64-bit integer. A `Date` is sent as ISO 8601 text, which compares with
 * timestamp and date columns.
 */
export type Parameter =
  number | bigint | string | boolean | Date | null | Parameter[] | { [key: string]: Parameter };

/** A bound parameter value, as `CypherQuery.parameters()` returns it. */
export type Scalar = number | string | boolean | null | Scalar[] | { [key: string]: Scalar };

/** Statistics for one node index, from `CypherEngine.nodeIndexStats()`. */
export interface NodeIndexStats {
  /** Indexed node rows. */
  rows: number;
  /** The node dataset version it reflects; absent for Arrow input. */
  version?: number;
  /** Wall time of the build, in milliseconds. */
  buildMs: number;
}

/** What `CypherEngine.optimizeDatasets()` did to one Lance dataset. */
export interface DatasetOptimizeStats {
  /** Small fragments compaction rewrote. */
  fragmentsRemoved: number;
  /** Fragments compaction wrote in their place. */
  fragmentsAdded: number;
  /** Old versions cleanup deleted. */
  oldVersions: number;
  /** Bytes of files cleanup deleted. */
  bytesRemoved: number;
}

/** Statistics for one CSR adjacency index, from `CypherEngine.csrStats()`. */
export interface CsrStats {
  /** Live indexed edges (null endpoints are skipped). */
  edges: number;
  /** Edges appended since the last full build, held outside the base CSR. */
  deltaEdges: number;
  /** Edges removed since the last full build, held outside the base CSR. */
  tombstones: number;
  /** Live edges outside the base CSR: appended since its last full build and not since removed. */
  unindexedEdges: number;
  /** How the last build, `checkoutLatest()` or `optimizeCsr()` produced it. */
  update: "build" | "rebuild" | "loaded" | "delta" | "reused";
  /** The relationship dataset version it reflects; absent for Arrow input. */
  version?: number;
  /** The relationship dataset version of the last full build; absent for Arrow input. */
  baseVersion?: number;
  /** Wall time of that build or delta, both directions, in milliseconds. */
  buildMs: number;
}

/**
 * A graph as compressed sparse rows over dense node ids, from `CypherEngine.exportGraph()`.
 *
 * Nodes are numbered `0` to `numNodes - 1`, and the neighbours of node `i` are
 * `outIndices[outIndptr[i]]` up to `outIndices[outIndptr[i + 1]]`. `nodeKeys` maps each dense
 * id back to its node's key. The vectors wrap typed arrays built in Rust. See
 * [Drawing the whole graph](https://lance-csr.hdclabs.ai/typescript/#drawing-the-whole-graph).
 */
export interface GraphExport {
  /** Number of nodes. Dense ids run from 0 to `numNodes - 1`. */
  numNodes: number;
  /** Whether the arrays describe a directed graph. */
  directed: boolean;
  /**
   * The key of each dense id. Each label's nodes are in the order its table is scanned; rows
   * with a null key are left out.
   */
  nodeKeys: Vector<Int64>;
  /**
   * Each node label's dense ids, as `[start, end]` with `end` excluded. Labels get consecutive
   * ranges in name order, so equal keys in two labels stay distinct.
   */
  labelRanges: Record<string, [number, number]>;
  /** Offsets into `outIndices`, `numNodes + 1` entries. */
  outIndptr: Vector<Uint64>;
  /**
   * Each node's out-neighbours, sorted, without duplicates. An undirected graph lists every
   * edge in both directions and a self-loop once.
   */
  outIndices: Vector<Uint64>;
  /** Offsets into `inIndices`; `null` for an undirected graph. */
  inIndptr: Vector<Uint64> | null;
  /**
   * Each node's in-neighbours, sorted, without duplicates; `null` for an undirected graph.
   */
  inIndices: Vector<Uint64> | null;
  /**
   * Edges left out because an endpoint key isn't in its node table. Edges with a null
   * endpoint are skipped and not counted.
   */
  droppedEdges: number;
}

/**
 * The kinds of {@link LanceCsrError}, matching the Python package's exceptions:
 *
 * - `InvalidArgument` (Python `ValueError`): an invalid query, config, parameter or option.
 * - `NotImplemented` (Python `NotImplementedError`): a Cypher feature that parses but isn't supported.
 * - `Timeout` (Python `TimeoutError`): the query ran past its `timeoutMs`.
 * - `Runtime` (Python `RuntimeError`): a dataset couldn't be read, or execution failed.
 */
export type ErrorCode = "InvalidArgument" | "NotImplemented" | "Timeout" | "Runtime";

/** An error from the query engine. Arguments of the wrong type raise `TypeError` instead. */
export class LanceCsrError extends Error {
  /** What went wrong; see {@link ErrorCode}. */
  readonly code: ErrorCode;

  constructor(code: ErrorCode, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "LanceCsrError";
    this.code = code;
  }
}

const CODED = /^\[(InvalidArgument|NotImplemented|Timeout|Runtime)\] ([\s\S]*)$/;

/** Turn the native module's coded errors into `LanceCsrError`; rethrow anything else. */
export function rethrow(err: unknown): never {
  const match = err instanceof Error ? CODED.exec(err.message) : null;
  if (match) {
    throw new LanceCsrError(match[1] as ErrorCode, match[2]!, { cause: err });
  }
  throw err;
}

/** Run a native call, converting its errors. */
export async function call<T>(run: () => Promise<T>): Promise<T> {
  try {
    return await run();
  } catch (err) {
    rethrow(err);
  }
}

/** Run a synchronous native call, converting its errors. */
export function callSync<T>(run: () => T): T {
  try {
    return run();
  } catch (err) {
    rethrow(err);
  }
}

export function toTable(ipc: Uint8Array): Table {
  return tableFromIPC(ipc);
}

export function arrowToIpc(value: Table | RecordBatch, name: string): Buffer {
  if (value instanceof Table) {
    return Buffer.from(tableToIPC(value, "stream"));
  }
  if (value instanceof RecordBatch) {
    return Buffer.from(tableToIPC(new Table(value), "stream"));
  }
  throw new TypeError(`${name} must be an apache-arrow Table or RecordBatch`);
}

function isLanceDataset(value: unknown): value is LanceDataset {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { uri?: unknown }).uri === "string"
  );
}

/** Convert the `datasets` argument for the native module. */
export function datasetInputs(
  datasets: Record<string, Dataset>,
): Record<string, native.DatasetInput> {
  if (typeof datasets !== "object" || datasets === null) {
    throw new TypeError("datasets must be an object keyed by node label or relationship type");
  }
  const inputs: Record<string, native.DatasetInput> = {};
  for (const [name, value] of Object.entries(datasets)) {
    if (value instanceof Table || value instanceof RecordBatch) {
      inputs[name] = { ipc: arrowToIpc(value, `datasets.${name}`) };
    } else if (isLanceDataset(value)) {
      const { uri, version, storageOptions } = value;
      inputs[name] = { uri, version, storageOptions };
    } else {
      throw new TypeError(
        `datasets.${name} must be an apache-arrow Table or RecordBatch, or a LanceDataset ({ uri })` +
          " (if it is a Table, check that only one copy of apache-arrow is installed)",
      );
    }
  }
  return inputs;
}

const MIN_I64 = -(2n ** 63n);
const MAX_I64 = 2n ** 63n - 1n;

// `JSON.rawJSON` (Node 21+) keeps a bigint's digits exact in the JSON text.
const rawJSON = (JSON as unknown as { rawJSON(text: string): unknown }).rawJSON;

function invalidParameter(message: string): LanceCsrError {
  return new LanceCsrError("InvalidArgument", message);
}

/** Serialize `$name` parameter values as JSON for the native module, checking each value. */
export function parametersJson(
  params: Record<string, Parameter> | undefined | null,
): string | undefined {
  if (params === undefined || params === null) {
    return undefined;
  }
  if (typeof params !== "object" || Array.isArray(params)) {
    throw new TypeError("params must be an object keyed by parameter name");
  }
  try {
    // `Date` values reach the replacer already converted to ISO text by their toJSON.
    return JSON.stringify(params, (_key, value: unknown) => {
      switch (typeof value) {
        case "bigint":
          if (value < MIN_I64 || value > MAX_I64) {
            throw invalidParameter("Integer parameters must fit signed 64-bit values");
          }
          return rawJSON(value.toString());
        case "number":
          if (!Number.isFinite(value)) {
            throw invalidParameter("Invalid float value");
          }
          return value;
        case "undefined":
          return null;
        case "function":
        case "symbol":
          throw invalidParameter("Unsupported parameter type");
        case "object":
          if (value !== null && !Array.isArray(value)) {
            const proto = Object.getPrototypeOf(value);
            if (proto !== Object.prototype && proto !== null) {
              throw invalidParameter("Unsupported parameter type");
            }
          }
          return value;
        default:
          return value;
      }
    });
  } catch (err) {
    if (err instanceof LanceCsrError) {
      throw err;
    }
    // JSON.stringify throws a TypeError on cycles.
    throw invalidParameter(
      `Parameters can't be serialized (possibly a cyclic container): ${(err as Error).message}`,
    );
  }
}

export function timeoutOption(timeoutMs: number | undefined | null): number | undefined {
  if (timeoutMs === undefined || timeoutMs === null) {
    return undefined;
  }
  if (typeof timeoutMs !== "number") {
    throw new TypeError("timeoutMs must be a number or undefined");
  }
  return timeoutMs;
}
