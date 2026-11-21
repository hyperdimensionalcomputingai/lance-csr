import { VERSION } from "../native.js";

export { GraphConfig, GraphConfigBuilder } from "./config.ts";
export {
  CypherEngine,
  type CypherEngineOptions,
  CypherQuery,
  type ExecuteOptions,
  ExecutionStrategy,
  type ExplainOptions,
  RecordBatchStream,
} from "./cypher.ts";
export { DirNamespace } from "./namespace.ts";
export { SqlDialect, SqlEngine, type SqlEngineOptions, SqlQuery } from "./sql.ts";
export {
  type CsrStats,
  type DatasetOptimizeStats,
  type Dataset,
  type ErrorCode,
  type GraphExport,
  type LanceDataset,
  LanceCsrError,
  type NodeIndexStats,
  type Parameter,
  type Scalar,
} from "./types.ts";
export { DistanceMetric, VectorSearch } from "./vector.ts";

/** The package version. */
export const version: string = VERSION;
