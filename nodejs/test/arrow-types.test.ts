// Every result type the engine produces must decode in apache-arrow JS.
import assert from "node:assert/strict";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromArrays } from "apache-arrow";
import { CypherEngine, GraphConfig, SqlEngine } from "../src/index.ts";
import { rows, tempDir, writeLance } from "./helpers.ts";

const config = GraphConfig.builder()
  .nodeTable("Person", "id")
  .relTable("KNOWS", "Person", "Person", "src", "dst")
  .build();
const person = tableFromArrays({
  id: BigInt64Array.from([1n, 2n, 3n]),
  name: ["Alice", "Bob", "Carol"],
  score: Float64Array.from([1.5, 2.5, 3.5]),
});
const knows = tableFromArrays({
  src: BigInt64Array.from([1n, 2n]),
  dst: BigInt64Array.from([2n, 3n]),
  since: Int32Array.from([2020, 2021]),
});

const QUERIES = [
  "MATCH (p:Person) RETURN p.name, upper(p.name) AS u, substring(p.name, 1, 2) AS s, size(p.name) AS n",
  "MATCH (p:Person) RETURN collect(p.name) AS names, collect(p.id) AS ids, avg(p.score) AS a",
  "MATCH (p:Person) RETURN p AS node",
  "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r AS rel, a.name, b.name",
  "MATCH path = (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN path, length(path) AS hops",
  "RETURN {a: 1, b: 'x', c: [1, 2]} AS m, [[1], [2, 3]] AS nested, null AS nothing",
  "RETURN date('2024-01-02') AS d, datetime('2024-01-02T03:04:05') AS dt",
  "MATCH (p:Person) RETURN p.score / 3.0 AS f, p.id % 2 AS i, p.score > 2 AS b, toString(p.id) AS s",
];

for (const [kind, makeTables] of [
  ["arrow", async () => ({ Person: person, KNOWS: knows })],
  [
    "lance",
    async () => {
      const dir = tempDir();
      await writeLance(join(dir, "person.lance"), person);
      await writeLance(join(dir, "knows.lance"), knows);
      return {
        Person: { uri: join(dir, "person.lance") },
        KNOWS: { uri: join(dir, "knows.lance") },
      };
    },
  ],
] as const) {
  test(`result types decode (${kind} input)`, async () => {
    const engine = await CypherEngine.create(config, await makeTables());
    for (const query of QUERIES) {
      const result = await engine.execute(query);
      assert.ok(result.numRows > 0, query);
      // Reading every value checks the IPC bytes against the schema, not just the header.
      assert.doesNotThrow(() => rows(result), query);
      const batches = await Array.fromAsync(await engine.executeBatches(query));
      assert.equal(
        batches.reduce((n, b) => n + b.numRows, 0),
        result.numRows,
        query,
      );
    }
  });
}

test("SQL view types and string functions decode", async () => {
  const engine = await SqlEngine.create({ person });
  const result = await engine.execute(
    "SELECT arrow_cast(name, 'Utf8View') AS v, arrow_cast(name, 'LargeUtf8') AS l, " +
      "concat(name, '!') AS c FROM person ORDER BY id",
  );
  assert.deepEqual(rows(result)[0], { v: "Alice", l: "Alice", c: "Alice!" });
});
