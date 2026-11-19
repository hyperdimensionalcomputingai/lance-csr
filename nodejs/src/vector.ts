/**
 * Ranking query results by vector similarity.
 *
 * @module
 */

import type { RecordBatch, Table } from "apache-arrow";
import * as native from "../native.js";
import { arrowToIpc, call, toTable } from "./types.ts";

/**
 * The distance {@link VectorSearch} ranks rows by; smaller is closer. `L2` is Euclidean
 * distance, `Cosine` is 1 − cosine similarity and `Dot` is the negative dot product.
 */
export const DistanceMetric = native.DistanceMetric;
export type DistanceMetric = native.DistanceMetric;

/**
 * An immutable builder for vector similarity search.
 *
 * Every method except {@link VectorSearch.search} returns a new builder. Pass the
 * builder to `search` to rank an Arrow table, or to `executeWithVectorRerank` on
 * `CypherEngine` or `CypherQuery` to rerank a Cypher result. This is separate from the
 * `vector_distance()` and `vector_similarity()` Cypher functions. See
 * [Vector search](https://lance-csr.hdclabs.ai/vector-search/).
 *
 * @example
 * ```ts
 * const search = new VectorSearch("d.embedding")
 *   .queryVector([0.1, 0.2, 0.3])
 *   .metric(DistanceMetric.Cosine)
 *   .topK(5);
 * ```
 */
export class VectorSearch {
  readonly #spec: native.VectorSearchSpec;

  /** @param column - The vector column to rank by, such as `"embedding"` or `"d.embedding"`. */
  constructor(column: string) {
    if (typeof column !== "string") {
      throw new TypeError("column must be a string");
    }
    this.#spec = { column };
  }

  #with(change: Partial<native.VectorSearchSpec>): VectorSearch {
    const search = new VectorSearch(this.#spec.column);
    Object.assign(search.#spec, this.#spec, change);
    return search;
  }

  /** @internal The spec the native module rebuilds the search from. */
  _spec(): native.VectorSearchSpec {
    return { ...this.#spec };
  }

  /** Set the vector to compare with. Required before searching. */
  queryVector(vector: ArrayLike<number>): VectorSearch {
    return this.#with({ queryVector: Array.from(vector) });
  }

  /** Set the distance metric; the default is `DistanceMetric.L2`. */
  metric(metric: DistanceMetric): VectorSearch {
    return this.#with({ metric });
  }

  /** Keep at most `k` rows; the default is 10. */
  topK(k: number): VectorSearch {
    return this.#with({ topK: k });
  }

  /** Add the distance as a column (the default), or leave it out. */
  includeDistance(include: boolean): VectorSearch {
    return this.#with({ includeDistance: include });
  }

  /** Name the distance column; the default is `"_distance"`. */
  distanceColumnName(name: string): VectorSearch {
    return this.#with({ distanceColumnName: name });
  }

  /**
   * Set whether reranking queries a Lance ANN index first; `false` by default.
   *
   * When the datasets are Lance datasets, `CypherQuery.executeWithVectorRerank`
   * searches the Lance approximate-nearest-neighbour index first and then runs the
   * Cypher query on the top-k rows. This can be much faster on large datasets, but it
   * can change the results when the query also filters, because the filter then applies
   * to the top-k rows only.
   */
  useLanceIndex(enabled: boolean): VectorSearch {
    return this.#with({ useLanceIndex: enabled });
  }

  /**
   * Brute-force search `table`.
   *
   * @param table - The rows to search, with the vector column.
   * @returns At most `topK` rows, sorted by distance, closest first.
   * @throws {@link LanceCsrError} with code `InvalidArgument` if no query vector is set or
   * the column doesn't exist, or `Runtime` if the search fails.
   */
  async search(table: Table | RecordBatch): Promise<Table> {
    const ipc = arrowToIpc(table, "table");
    return toTable(await call(() => native.vectorSearchSearch(this._spec(), ipc)));
  }
}
