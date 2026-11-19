// SqlQuery and SqlEngine: plain SQL, GRAPH_TABLE on the CSR, parameters and Lance pushdown.
import assert from "node:assert/strict";
import { join } from "node:path";
import { describe, test } from "node:test";
import { tableFromArrays } from "apache-arrow";
import {
  CypherEngine,
  type Dataset,
  GraphConfig,
  type LanceDataset,
  SqlEngine,
  SqlQuery,
} from "../src/index.ts";
import { rows, tempDir, writeLance } from "./helpers.ts";

const PEOPLE = tableFromArrays({
  id: BigInt64Array.from([1n, 2n, 3n, 4n]),
  name: ["Alice", "Bob", "Carol", "Dan"],
  age: BigInt64Array.from([28n, 34n, 29n, 42n]),
});
const KNOWS = tableFromArrays({
  src_id: BigInt64Array.from([1n, 1n, 2n, 3n, 4n]),
  dst_id: BigInt64Array.from([2n, 3n, 3n, 4n, 1n]),
});
const CONFIG = GraphConfig.builder()
  .nodeTable("Person", "id")
  .relTable("KNOWS", "Person", "Person", "src_id", "dst_id")
  .build();
const FRIENDS_OF_FRIENDS = `
  SELECT g.a, g.c, p.age
  FROM GRAPH_TABLE (
    MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
    WHERE a.age < $max_age
    COLUMNS (a.name AS a, c.name AS c, c.id AS c_id)
  ) g
  JOIN person p ON p.id = g.c_id
  WHERE p.age > $min_age
  ORDER BY g.a, g.c
`;

async function lanceTables(): Promise<Record<string, LanceDataset>> {
  const dir = tempDir();
  const person = { uri: join(dir, "Person.lance") };
  const knows = { uri: join(dir, "KNOWS.lance") };
  await writeLance(person.uri, PEOPLE);
  await writeLance(knows.uri, KNOWS);
  return { Person: person, KNOWS: knows };
}

describe("SqlQuery", () => {
  test("runs against the tables passed in, by lower-cased name", async () => {
    const query = new SqlQuery("SELECT name FROM person WHERE age > 30 ORDER BY name");
    assert.equal(query.sql(), "SELECT name FROM person WHERE age > 30 ORDER BY name");
    assert.deepEqual(rows(await query.execute({ Person: PEOPLE })), [
      { name: "Bob" },
      { name: "Dan" },
    ]);
    assert.match(await query.explain({ Person: PEOPLE }), /== Physical Plan ==/);
    assert.deepEqual(rows(await query.execute(await lanceTables())), [
      { name: "Bob" },
      { name: "Dan" },
    ]);
    await assert.rejects(new SqlQuery("SELECT FROM WHERE").execute({ Person: PEOPLE }), {
      code: "InvalidArgument",
    });
  });
});

for (const kind of ["arrow", "lance"] as const) {
  describe(`SqlEngine over ${kind} tables`, async () => {
    const datasets: Record<string, Dataset> =
      kind === "arrow" ? { Person: PEOPLE, KNOWS } : await lanceTables();

    for (const useCsr of [true, false]) {
      test(`GRAPH_TABLE with parameters (useCsr: ${useCsr})`, async () => {
        const engine = await SqlEngine.create(datasets, { config: CONFIG, useCsr });
        const result = await engine.execute(FRIENDS_OF_FRIENDS, { max_age: 40, MIN_AGE: 28 });
        assert.deepEqual(rows(result), [
          { a: "Alice", c: "Carol", age: 29 },
          { a: "Alice", c: "Dan", age: 42 },
          { a: "Bob", c: "Dan", age: 42 },
        ]);
        const plan = await engine.explain(FRIENDS_OF_FRIENDS, { max_age: 30, min_age: 0 });
        assert.equal(plan.includes("CsrExpandExec"), useCsr);
        const analyzed = await engine.explain(
          FRIENDS_OF_FRIENDS,
          { max_age: 40, min_age: 28 },
          { analyze: true },
        );
        assert.match(analyzed, /== Physical Plan with Metrics ==/);
        assert.match(analyzed, /output_rows=/);
      });
    }

    test("fromEngine shares the CypherEngine's snapshot", async () => {
      const cypher = await CypherEngine.create(CONFIG, datasets, { useCsr: true });
      const engine = SqlEngine.fromEngine(cypher);
      const result = await engine.execute(
        "SELECT count(*) AS n FROM graph_table(" +
          "'MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.name = $name RETURN a.id')",
        { name: "Carol" },
      );
      assert.deepEqual(rows(result), [{ n: 2 }]);
      await engine.checkoutLatest();
    });

    test("container parameters inside GRAPH_TABLE", async () => {
      const engine = await SqlEngine.create(datasets, { config: CONFIG });
      const result = await engine.execute(
        "SELECT * FROM GRAPH_TABLE (MATCH (a:Person) WHERE a.id IN $ids " +
          "COLUMNS (a.name AS name)) ORDER BY name",
        { ids: [3, 1] },
      );
      assert.deepEqual(rows(result), [{ name: "Alice" }, { name: "Carol" }]);
    });
  });
}

test("plain SQL pushes filters into the Lance scan", async () => {
  const datasets = await lanceTables();
  const query = "SELECT name FROM person WHERE age > $age ORDER BY name";
  for (const engine of [
    await SqlEngine.create(datasets),
    await SqlEngine.create(datasets, { config: CONFIG }),
  ]) {
    assert.deepEqual(rows(await engine.execute(query, { age: 30 })), [
      { name: "Bob" },
      { name: "Dan" },
    ]);
    assert.match(await engine.explain(query, { age: 30 }), /full_filter=age > Int64\(30\)/);
  }
});

test("writes, malformed GRAPH_TABLE clauses and missing values are rejected", async () => {
  const engine = await SqlEngine.create({ Person: PEOPLE, KNOWS }, { config: CONFIG });
  for (const query of [
    "CREATE TABLE t AS SELECT 1",
    "INSERT INTO person VALUES (9, 'Eve', 1)",
    "SET datafusion.execution.batch_size = 1",
    "SELECT * FROM GRAPH_TABLE (MATCH (a:Person))",
    "SELECT * FROM person WHERE age > $missing",
  ]) {
    await assert.rejects(engine.execute(query), { code: "InvalidArgument" }, query);
  }
});

test("GRAPH_TABLE and useCsr need a config", async () => {
  const engine = await SqlEngine.create({ Person: PEOPLE });
  await assert.rejects(
    engine.execute("SELECT * FROM GRAPH_TABLE (MATCH (a:Person) COLUMNS (a.id))"),
    { code: "InvalidArgument", message: /GRAPH_TABLE needs a graph/ },
  );
  await assert.rejects(SqlEngine.create({ Person: PEOPLE }, { useCsr: true }), {
    code: "InvalidArgument",
    message: /useCsr requires a config/,
  });
  await assert.rejects(
    SqlEngine.create({ Person: PEOPLE }, { config: {} as GraphConfig }),
    TypeError,
  );
  assert.throws(() => SqlEngine.fromEngine({} as CypherEngine), TypeError);
});
