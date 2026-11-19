// VectorSearch: ranking tables, reranking Cypher results, and the Lance index path.
import assert from "node:assert/strict";
import { join } from "node:path";
import { describe, test } from "node:test";
import {
  Field,
  FixedSizeList,
  Float32,
  List,
  Table,
  makeData,
  makeVector,
  tableFromArrays,
} from "apache-arrow";
import {
  CypherEngine,
  CypherQuery,
  DistanceMetric,
  GraphConfig,
  VectorSearch,
} from "../src/index.ts";
import { columns, tempDir, writeLance } from "./helpers.ts";

const EMBEDDINGS = [
  [1.0, 0.0, 0.0],
  [0.9, 0.1, 0.0],
  [0.0, 1.0, 0.0],
  [0.0, 0.0, 1.0],
  [0.5, 0.5, 0.0],
];
const config = GraphConfig.builder().nodeTable("Document", "id").build();

/** Documents with a `list<float32>` (or fixed-size list, which Lance search needs) embedding. */
function documents(fixedSize = false): Table {
  const child = makeData({ type: new Float32(), data: Float32Array.from(EMBEDDINGS.flat()) });
  const item = new Field("item", new Float32(), true);
  const embedding = fixedSize
    ? makeVector(makeData({ type: new FixedSizeList(3, item), length: 5, nullCount: 0, child }))
    : makeVector(
        makeData({
          type: new List(item),
          length: 5,
          nullCount: 0,
          valueOffsets: Int32Array.from([0, 3, 6, 9, 12, 15]),
          child,
        }),
      );
  const base = tableFromArrays({
    id: BigInt64Array.from([1n, 2n, 3n, 4n, 5n]),
    name: ["Doc1", "Doc2", "Doc3", "Doc4", "Doc5"],
    category: ["tech", "tech", "science", "tech", "science"],
  });
  return new Table({
    id: base.getChild("id")!,
    name: base.getChild("name")!,
    category: base.getChild("category")!,
    embedding,
  });
}

const datasets = { Document: documents() };
const near = new VectorSearch("embedding").queryVector([1.0, 0.0, 0.0]);

describe("VectorSearch.search", () => {
  test("L2, with the distance column by default", async () => {
    const data = columns(await near.topK(3).search(datasets.Document));
    assert.deepEqual(data.name, ["Doc1", "Doc2", "Doc5"]);
    assert.ok(Math.abs((data._distance![0] as number) - 0) < 1e-6);
  });

  test("Cosine and Dot metrics", async () => {
    const cosine = columns(
      await near.metric(DistanceMetric.Cosine).topK(2).search(datasets.Document),
    );
    assert.deepEqual(cosine.name, ["Doc1", "Doc2"]);
    const dot = columns(await near.metric(DistanceMetric.Dot).topK(2).search(datasets.Document));
    assert.deepEqual(dot.name, ["Doc1", "Doc2"]);
  });

  test("custom distance column, or none", async () => {
    const named = await near.distanceColumnName("score").topK(1).search(datasets.Document);
    assert.ok(named.getChild("score") !== null);
    const none = await near.includeDistance(false).topK(1).search(datasets.Document);
    assert.equal(none.getChild("_distance"), null);
  });

  test("a missing query vector or column", async () => {
    await assert.rejects(new VectorSearch("embedding").search(datasets.Document), {
      code: "InvalidArgument",
    });
    await assert.rejects(
      new VectorSearch("nope").queryVector([1, 0, 0]).search(datasets.Document),
      (err: unknown) => (err as { code?: string }).code !== undefined,
    );
  });

  test("builders are immutable", () => {
    const base = new VectorSearch("embedding");
    const changed = base.topK(1).metric(DistanceMetric.Dot).useLanceIndex(true);
    assert.deepEqual(base._spec(), { column: "embedding" });
    assert.deepEqual(changed._spec(), {
      column: "embedding",
      topK: 1,
      metric: DistanceMetric.Dot,
      useLanceIndex: true,
    });
  });
});

describe("reranking Cypher results", () => {
  const rerank = new VectorSearch("d.embedding").queryVector([1.0, 0.0, 0.0]).topK(3);

  test("CypherQuery and CypherEngine agree", async () => {
    const query = "MATCH (d:Document) WHERE d.category = 'tech' RETURN d.name, d.embedding";
    const viaQuery = await new CypherQuery(query)
      .withConfig(config)
      .executeWithVectorRerank(datasets, rerank);
    const engine = await CypherEngine.create(config, datasets);
    const viaEngine = await engine.executeWithVectorRerank(query, rerank);
    assert.deepEqual(columns(viaQuery)["d.name"], ["Doc1", "Doc2", "Doc4"]);
    assert.deepEqual(columns(viaEngine), columns(viaQuery));
  });

  test("useLanceIndex searches the Lance dataset first", async () => {
    const uri = join(tempDir(), "Document.lance");
    await writeLance(uri, documents(true));
    const query = new CypherQuery("MATCH (d:Document) RETURN d.id, d.name, d.embedding").withConfig(
      config,
    );
    const results = await query.executeWithVectorRerank(
      { Document: { uri } },
      rerank.metric(DistanceMetric.L2).useLanceIndex(true),
    );
    assert.deepEqual(columns(results)["d.name"]!.slice(0, 2), ["Doc1", "Doc2"]);
    assert.equal(results.numRows, 3);

    // A WHERE clause falls back to the exact rerank.
    const filtered = await new CypherQuery(
      "MATCH (d:Document) WHERE d.category = 'science' RETURN d.name, d.embedding",
    )
      .withConfig(config)
      .executeWithVectorRerank({ Document: { uri } }, rerank.useLanceIndex(true));
    assert.deepEqual(columns(filtered)["d.name"], ["Doc5", "Doc3"]);

    await assert.rejects(
      query.executeWithVectorRerank(
        { Document: { uri } },
        new VectorSearch("d.embedding").useLanceIndex(true),
      ),
      { code: "InvalidArgument", message: /queryVector is required/ },
    );
  });

  test("arguments must be VectorSearch objects", async () => {
    const engine = await CypherEngine.create(config, datasets);
    await assert.rejects(
      engine.executeWithVectorRerank("MATCH (d:Document) RETURN d.embedding", {} as VectorSearch),
      TypeError,
    );
  });
});
