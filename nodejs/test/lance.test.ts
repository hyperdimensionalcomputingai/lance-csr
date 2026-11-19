// CypherEngine over on-disk Lance datasets: pinned snapshots, checkoutLatest, streaming and
// the stored CSR. Expected values are computed independently of the engine.
import assert from "node:assert/strict";
import { existsSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { describe, test } from "node:test";
import { Table, tableFromArrays } from "apache-arrow";
import { CypherEngine, CypherQuery, GraphConfig, type LanceDataset } from "../src/index.ts";
import { columns, rows, tempDir, writeLance } from "./helpers.ts";

// Past the 10,000-row cap an old conversion fallback silently applied.
const N = 12_000;
const config = GraphConfig.builder()
  .nodeTable("Person", "id")
  .relTable("KNOWS", "Person", "Person", "src", "dst")
  .build();

function people(start: number, stop: number) {
  const ids = Array.from({ length: stop - start }, (_, i) => BigInt(start + i));
  return tableFromArrays({ id: BigInt64Array.from(ids), name: ids.map((i) => `p${i}`) });
}

const sum = (values: unknown[]) => values.reduce<number>((a, b) => a + (b as number), 0);

async function graph() {
  const dir = tempDir();
  const person: LanceDataset = { uri: join(dir, "person.lance") };
  const knows: LanceDataset = { uri: join(dir, "knows.lance") };
  // Several fragments so scans produce multiple batches.
  await writeLance(person.uri, people(0, N), "create", 4_000);
  const src = BigInt64Array.from({ length: N - 1 }, (_, i) => BigInt(i));
  await writeLance(knows.uri, tableFromArrays({ src, dst: src.map((i) => i + 1n) }), "create");
  return { dir, person, knows };
}

const countPeople = async (engine: CypherEngine) =>
  (await engine.execute("MATCH (p:Person) RETURN p.id")).numRows;

describe("Lance datasets", () => {
  test("CypherQuery reads every row", async () => {
    const { person, knows } = await graph();
    const out = await new CypherQuery("MATCH (p:Person) RETURN p.id")
      .withConfig(config)
      .execute({ Person: person, KNOWS: knows });
    assert.equal(out.numRows, N);
    assert.equal(sum(columns(out)["p.id"]!), (N * (N - 1)) / 2);
  });

  test("the engine reads every row, traverses and mixes Lance with Arrow", async () => {
    const { person, knows } = await graph();
    const engine = await CypherEngine.create(config, { Person: person, KNOWS: knows });
    assert.equal(await countPeople(engine), N);
    const hops = await engine.execute(
      "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.id < 3 RETURN a.id, b.id ORDER BY a.id",
    );
    assert.deepEqual(rows(hops), [
      { "a.id": 0, "b.id": 1 },
      { "a.id": 1, "b.id": 2 },
      { "a.id": 2, "b.id": 3 },
    ]);
    const mixed = await CypherEngine.create(config, { Person: people(0, 5), KNOWS: knows });
    assert.equal(await countPeople(mixed), 5);
    assert.deepEqual(Object.keys(mixed.datasetVersions()), ["KNOWS"]);
  });

  test("the snapshot holds until checkoutLatest", async () => {
    const { person, knows } = await graph();
    const engine = await CypherEngine.create(config, { Person: person, KNOWS: knows });
    assert.deepEqual(engine.datasetVersions(), { Person: 1, KNOWS: 1 });
    const version = await writeLance(person.uri, people(N, N + 10), "append");
    assert.equal(version, 2);
    assert.equal(await countPeople(engine), N);
    await engine.checkoutLatest();
    assert.equal(await countPeople(engine), N + 10);
    assert.equal(engine.datasetVersions().Person, 2);
  });

  test("an explicit version is pinned; engines are independent", async () => {
    const { person, knows } = await graph();
    await writeLance(person.uri, people(N, N + 5), "append");
    const old = await CypherEngine.create(config, {
      Person: { ...person, version: 1 },
      KNOWS: knows,
    });
    const latest = await CypherEngine.create(config, { Person: person, KNOWS: knows });
    assert.equal(await countPeople(old), N);
    assert.equal(await countPeople(latest), N + 5);
  });

  test("checkoutLatest sees an overwrite with new columns", async () => {
    const { person, knows } = await graph();
    const engine = await CypherEngine.create(config, { Person: person, KNOWS: knows });
    await writeLance(
      person.uri,
      tableFromArrays({
        id: BigInt64Array.from([1n, 2n]),
        name: ["a", "b"],
        score: Float64Array.from([0.5, 1.5]),
      }),
      "overwrite",
    );
    await engine.checkoutLatest();
    const out = await engine.execute("MATCH (p:Person) RETURN p.score ORDER BY p.score");
    assert.deepEqual(columns(out)["p.score"], [0.5, 1.5]);
  });

  test("a missing dataset fails at create", async () => {
    await assert.rejects(
      CypherEngine.create(config, { Person: { uri: join(tempDir(), "missing.lance") } }),
      { code: "Runtime" },
    );
  });

  test("executeBatches streams every row in several batches", async () => {
    const { person, knows } = await graph();
    const engine = await CypherEngine.create(config, { Person: person, KNOWS: knows });
    const stream = await engine.executeBatches("MATCH (p:Person) RETURN p.id AS id");
    assert.deepEqual(
      stream.schema.fields.map((f) => f.name),
      ["id"],
    );
    let total = 0;
    let batches = 0;
    for await (const batch of stream) {
      total += batch.numRows;
      batches += 1;
    }
    assert.equal(total, N);
    assert.ok(batches > 1, `expected several batches, got ${batches}`);
    assert.equal(await stream.next(), null);
  });

  test("a stream read in part, then closed, frees the engine", async () => {
    const { person, knows } = await graph();
    const engine = await CypherEngine.create(config, { Person: person, KNOWS: knows });
    const stream = await engine.executeBatches("MATCH (p:Person) RETURN p.id");
    for await (const batch of stream) {
      assert.ok(batch.numRows > 0);
      break;
    }
    assert.equal(await stream.next(), null);
    assert.equal(await countPeople(engine), N);
  });

  test("a stream keeps its snapshot across checkoutLatest", async () => {
    for (const useCsr of [false, true]) {
      const { person, knows } = await graph();
      const engine = await CypherEngine.create(
        config,
        { Person: person, KNOWS: knows },
        { useCsr },
      );
      const stream = await engine.executeBatches("MATCH (p:Person) RETURN p.id");
      await writeLance(person.uri, people(N, N + 7), "append");
      await engine.checkoutLatest();
      const batches = await Array.fromAsync(stream);
      assert.equal(new Table(batches).numRows, N);
      assert.equal(await countPeople(engine), N + 7);
    }
  });

  test("planning errors reject executeBatches", async () => {
    const { person, knows } = await graph();
    const engine = await CypherEngine.create(config, { Person: person, KNOWS: knows });
    await assert.rejects(engine.executeBatches("MATCH (p:Nope) RETURN p"), {
      code: "InvalidArgument",
    });
  });

  test("the CSR follows appends as a delta, and optimizeCsr rebuilds it", async () => {
    const { person, knows } = await graph();
    const engine = await CypherEngine.create(
      config,
      { Person: person, KNOWS: knows },
      { useCsr: true },
    );
    assert.equal(engine.csrStats().KNOWS!.edges, N - 1);
    assert.equal(engine.csrStats().KNOWS!.version, 1);
    await writeLance(
      knows.uri,
      tableFromArrays({ src: BigInt64Array.from([0n]), dst: BigInt64Array.from([5n]) }),
      "append",
    );
    await engine.checkoutLatest();
    const delta = engine.csrStats().KNOWS!;
    assert.equal(delta.update, "delta");
    assert.equal(delta.deltaEdges, 1);
    assert.equal(delta.baseVersion, 1);
    await engine.optimizeCsr();
    const rebuilt = engine.csrStats().KNOWS!;
    assert.equal(rebuilt.update, "rebuild");
    assert.equal(rebuilt.unindexedEdges, 0);
    assert.equal(rebuilt.baseVersion, rebuilt.version);
    const out = await engine.execute(
      "MATCH (:Person {id: 0})-[:KNOWS]->(b) RETURN b.id ORDER BY b.id",
    );
    assert.deepEqual(columns(out)["b.id"], [1, 5]);
  });

  test("persistCsr stores the CSR, a later engine loads it, deleteStoredCsr removes it", async () => {
    const { person, knows } = await graph();
    const datasets = { Person: person, KNOWS: knows };
    const folder = join(knows.uri, "_lance_csr");
    const first = await CypherEngine.create(config, datasets, { useCsr: true, persistCsr: true });
    assert.equal(first.csrStats().KNOWS!.update, "build");
    assert.deepEqual(readdirSync(folder), ["1.arrow"]);
    const second = await CypherEngine.create(config, datasets, { useCsr: true, persistCsr: true });
    assert.equal(second.csrStats().KNOWS!.update, "loaded");
    assert.equal(await second.deleteStoredCsr(), 1);
    assert.equal(existsSync(folder), false);
    assert.equal(await second.deleteStoredCsr(), 0);
    assert.equal(await countPeople(second), N);
  });
});
