/**
 * Graph configuration: which tables are nodes and which are relationships.
 *
 * @module
 */

import * as native from "../native.js";
import { callSync } from "./types.ts";

/**
 * An immutable mapping from tables to a property graph.
 *
 * Create one with {@link GraphConfig.builder}; there is no public constructor. See
 * [Graph schema](https://lance-csr.hdclabs.ai/graph-schema/).
 */
export class GraphConfig {
  readonly #spec: native.GraphConfigSpec;
  readonly #info: native.GraphConfigInfo;

  private constructor(spec: native.GraphConfigSpec, info: native.GraphConfigInfo) {
    this.#spec = spec;
    this.#info = info;
  }

  /** @internal Validate `spec` and wrap it; `GraphConfigBuilder.build()` calls this. */
  static _build(spec: native.GraphConfigSpec): GraphConfig {
    return new GraphConfig(
      spec,
      callSync(() => native.buildGraphConfig(spec)),
    );
  }

  /** @internal The spec the native module rebuilds the config from. */
  _spec(): native.GraphConfigSpec {
    return this.#spec;
  }

  /** Start a new, empty builder: the same as `new GraphConfigBuilder()`. */
  static builder(): GraphConfigBuilder {
    return new GraphConfigBuilder();
  }

  /** The configured node labels, lower-cased, in no particular order. */
  nodeLabels(): string[] {
    return [...this.#info.nodeLabels];
  }

  /** The configured relationship types, lower-cased, in no particular order. */
  relationshipTypes(): string[] {
    return [...this.#info.relationshipTypes];
  }

  toString(): string {
    return `GraphConfig(nodes=${JSON.stringify(this.#info.nodeLabels)}, relationships=${JSON.stringify(this.#info.relationshipTypes)})`;
  }
}

/**
 * Builds a {@link GraphConfig} from node and relationship tables.
 *
 * `new GraphConfigBuilder()` is the same as `GraphConfig.builder()`. Every method returns
 * a new builder and leaves this one unchanged.
 *
 * @example
 * ```ts
 * const config = GraphConfig.builder()
 *   .nodeTable("Person", "id")
 *   .relTable("KNOWS", "Person", "Person", "src_id", "dst_id")
 *   .build();
 * ```
 */
export class GraphConfigBuilder {
  readonly #spec: native.GraphConfigSpec;

  constructor() {
    this.#spec = { nodes: [], relationships: [] };
  }

  #with(spec: native.GraphConfigSpec): GraphConfigBuilder {
    const builder = new GraphConfigBuilder();
    builder.#spec.nodes.push(...spec.nodes);
    builder.#spec.relationships.push(...spec.relationships);
    return builder;
  }

  /**
   * Add a node table.
   *
   * The nodes labelled `label` are the rows of the dataset named `label`, each
   * identified by its `primaryKey` column.
   *
   * @param label - The node label, and the name of its dataset (for example `"Person"`).
   * @param primaryKey - The column that identifies each node.
   * @returns A new builder with the node table added.
   */
  nodeTable(label: string, primaryKey: string): GraphConfigBuilder {
    return this.#with({
      nodes: [...this.#spec.nodes, { label, primaryKey }],
      relationships: this.#spec.relationships,
    });
  }

  /**
   * Add a relationship table.
   *
   * The edges of type `relType` are the rows of the dataset named `relType`, from
   * `fromTable` nodes to `toTable` nodes. Both node tables must be added by the time
   * {@link GraphConfigBuilder.build} runs.
   *
   * @param relType - The relationship type, and the name of its dataset (for example `"KNOWS"`).
   * @param fromTable - The node table at the source end.
   * @param toTable - The node table at the target end.
   * @param src - The column holding the source node's primary key.
   * @param dst - The column holding the target node's primary key.
   * @returns A new builder with the relationship table added.
   */
  relTable(
    relType: string,
    fromTable: string,
    toTable: string,
    src: string,
    dst: string,
  ): GraphConfigBuilder {
    return this.#with({
      nodes: this.#spec.nodes,
      relationships: [...this.#spec.relationships, { relType, fromTable, toTable, src, dst }],
    });
  }

  /**
   * Validate the tables and return the config.
   *
   * @throws {@link LanceCsrError} with code `InvalidArgument` if the configuration is
   * invalid, for example when a relationship's endpoint label has no node table.
   */
  build(): GraphConfig {
    return GraphConfig._build({
      nodes: [...this.#spec.nodes],
      relationships: [...this.#spec.relationships],
    });
  }
}
