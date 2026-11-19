// One deadline covers planning, execution and the pauses between streamed reads.
import assert from "node:assert/strict";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { describe, test } from "node:test";
import { tableFromArrays } from "apache-arrow";
import { CypherEngine, CypherQuery, GraphConfig, SqlEngine } from "../src/index.ts";
import { rows, tempDir, writeLance } from "./helpers.ts";

const config = GraphConfig.builder().nodeTable("Person", "id").build();
const tables = {
  Person: tableFromArrays({ id: BigInt64Array.from({ length: 20_000 }, (_, i) => BigInt(i)) }),
};
const QUERY = "MATCH (p:Person) RETURN p.id AS id";

describe("timeouts", async () => {
  const engine = await CypherEngine.create(config, tables);
  const methods = {
    execute: (timeoutMs: number) => engine.execute(QUERY, null, { timeoutMs }),
    executeBatches: (timeoutMs: number) => engine.executeBatches(QUERY, null, { timeoutMs }),
    query: (timeoutMs: number) =>
      new CypherQuery(QUERY).withConfig(config).execute(tables, { timeoutMs }),
  };

  test("invalid timeouts", async () => {
    for (const [name, run] of Object.entries(methods)) {
      for (const timeoutMs of [-1, Number.NaN, Number.POSITIVE_INFINITY, 1e300]) {
        await assert.rejects(
          run(timeoutMs),
          { code: "InvalidArgument", message: /timeoutMs/ },
          name,
        );
      }
    }
    await assert.rejects(
      engine.execute(QUERY, null, { timeoutMs: "1" as unknown as number }),
      TypeError,
    );
  });

  test("a zero timeout fails, then the engine still works", async () => {
    for (const [name, run] of Object.entries(methods)) {
      await assert.rejects(run(0), { code: "Timeout", message: "Query timeout exceeded" }, name);
      assert.deepEqual(rows(await engine.execute("RETURN 1 AS n")), [{ n: 1 }]);
    }
  });

  test("a stream's deadline passes while it sits idle", async () => {
    const stream = await engine.executeBatches(QUERY, null, { timeoutMs: 500 });
    await sleep(550);
    await assert.rejects(stream.next(), { code: "Timeout" });
    await stream.close();
    assert.deepEqual(rows(await engine.execute("RETURN 1 AS n", null, { timeoutMs: 10_000 })), [
      { n: 1 },
    ]);
  });

  test("one deadline between reads", async () => {
    const uri = join(tempDir(), "people.lance");
    await writeLance(uri, tables.Person, "create", 4000);
    const lance = await CypherEngine.create(config, { Person: { uri } });
    const stream = await lance.executeBatches(QUERY, null, { timeoutMs: 500 });
    const first = await stream.next();
    assert.ok(first !== null && first.numRows > 0 && first.numRows < 20_000);
    await sleep(550);
    await assert.rejects(Array.fromAsync(stream), { code: "Timeout" });
  });

  test("no deadline, and ordinary errors under one", async () => {
    assert.equal((await engine.execute(QUERY)).numRows, 20_000);
    const stream = await engine.executeBatches(QUERY, null, { timeoutMs: 10_000 });
    let total = 0;
    for await (const batch of stream) total += batch.numRows;
    assert.equal(total, 20_000);
    await assert.rejects(engine.execute("RETURN unknown", null, { timeoutMs: 10_000 }), {
      code: "InvalidArgument",
    });
  });

  test("an execution error in a timed stream is not reported as a timeout", async () => {
    const stream = await engine.executeBatches("MATCH (p:Person) RETURN 1 / p.id AS n", null, {
      timeoutMs: 10_000,
    });
    // Only the batch holding p.id = 0 fails, and partitions finish in any order.
    await assert.rejects(Array.fromAsync(stream), (err: unknown) => {
      assert.equal((err as { code?: string }).code, "Runtime");
      assert.doesNotMatch((err as Error).message, /timeout/);
      return true;
    });
    assert.equal(await stream.next(), null);
    assert.deepEqual(rows(await engine.execute("RETURN 3 AS n")), [{ n: 3 }]);
  });

  test("an expensive query stops and the engine stays usable", async () => {
    const started = performance.now();
    await assert.rejects(
      engine.execute(
        "MATCH (a:Person), (b:Person), (c:Person) WHERE a.id + b.id + c.id > 30000 RETURN count(*) AS n",
        null,
        { timeoutMs: 20 },
      ),
      { code: "Timeout" },
    );
    assert.ok(performance.now() - started < 2000);
    assert.deepEqual(rows(await engine.execute("RETURN 2 AS n")), [{ n: 2 }]);
  });

  test("SqlEngine timeouts", async () => {
    const sql = await SqlEngine.create({ person: tables.Person });
    await assert.rejects(sql.execute("SELECT * FROM person", null, { timeoutMs: 0 }), {
      code: "Timeout",
    });
    // Nested-loop joins (no equality to hash on) check the deadline while they run.
    const big = tableFromArrays({ x: BigInt64Array.from({ length: 2000 }, (_, i) => BigInt(i)) });
    const slow = await SqlEngine.create({ a: big, b: big, c: big });
    const started = performance.now();
    await assert.rejects(
      slow.execute("SELECT count(*) FROM a JOIN b ON a.x < b.x JOIN c ON b.x < c.x", null, {
        timeoutMs: 200,
      }),
      { code: "Timeout" },
    );
    assert.ok(performance.now() - started < 5000);
    await assert.rejects(sql.execute("SELECT 1", null, { timeoutMs: -1 }), {
      code: "InvalidArgument",
    });
    assert.deepEqual(rows(await sql.execute("SELECT 1 AS n", null, { timeoutMs: 5000 })), [
      { n: 1 },
    ]);
  });
});
