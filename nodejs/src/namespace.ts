/**
 * Resolving table names to Lance datasets in a directory.
 *
 * @module
 */

/**
 * Resolves each table name to the Lance dataset `<baseUri>/<name>.lance`.
 *
 * Use it with `CypherQuery.executeWithNamespace`.
 */
export class DirNamespace {
  /** The directory that holds the datasets. */
  readonly baseUri: string;

  /** @param baseUri - The directory that holds the datasets. */
  constructor(baseUri: string) {
    if (typeof baseUri !== "string") {
      throw new TypeError("baseUri must be a string");
    }
    this.baseUri = baseUri;
  }
}
