// CypherQuery, GraphConfig and the builders: immutability, parsing, one-shot runs and SQL export.
import assert from "node:assert/strict";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromArrays } from "apache-arrow";
import {
  CypherQuery,
  DirNamespace,
  ExecutionStrategy,
  GraphConfig,
  GraphConfigBuilder,
  LanceCsrError,
  SqlDialect,
} from "../src/index.ts";
import { columns, rows, tempDir, writeLance } from "./helpers.ts";

const config = GraphConfig.builder()
  .nodeTable("Person", "id")
  .relTable("KNOWS", "Person", "Person", "src_id", "dst_id")
  .build();
const people = tableFromArrays({
  id: BigInt64Array.from([1n, 2n, 3n]),
  name: ["Alice", "Bob", "Carol"],
  age: BigInt64Array.from([28n, 34n, 29n]),
});
const knows = tableFromArrays({
  src_id: BigInt64Array.from([1n, 2n]),
  dst_id: BigInt64Array.from([2n, 3n]),
});
const datasets = { Person: people, KNOWS: knows };

test("GraphConfig has no public constructor and lower-cases names", () => {
  assert.equal(typeof GraphConfig.builder, "function");
  assert.ok(GraphConfig.builder() instanceof GraphConfigBuilder);
  assert.deepEqual(config.nodeLabels(), ["person"]);
  assert.deepEqual(config.relationshipTypes(), ["knows"]);
  assert.match(String(config), /GraphConfig\(nodes=\["person"\]/);
});

test("builders return new objects and leave the original unchanged", () => {
  const empty = new GraphConfigBuilder();
  const withPerson = empty.nodeTable("Person", "id");
  assert.notEqual(withPerson, empty);
  assert.deepEqual(withPerson.build().nodeLabels(), ["person"]);
  assert.deepEqual(empty.nodeTable("Company", "id").build().nodeLabels(), ["company"]);

  const query = new CypherQuery("RETURN $x AS x");
  const bound = query.withParameter("x", 1);
  assert.deepEqual(query.parameters(), {});
  assert.deepEqual(bound.parameters(), { x: 1 });
  assert.notEqual(bound.withConfig(config), bound);
});

test("rel_table endpoints must be node tables", () => {
  assert.throws(
    () => GraphConfig.builder().relTable("KNOWS", "Person", "Person", "a", "b").build(),
    (err: unknown) => err instanceof LanceCsrError && err.code === "InvalidArgument",
  );
});

test("parsing: syntax errors, variables, labels and types", () => {
  assert.throws(() => new CypherQuery("MATCH (p:Person RETURN p"), { code: "InvalidArgument" });
  assert.throws(() => new CypherQuery(42 as unknown as string), TypeError);
  const query = new CypherQuery(
    "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name UNION MATCH (c:Company) RETURN c.name",
  );
  assert.deepEqual(query.variables().sort(), ["a", "b", "c", "r"]);
  assert.deepEqual(query.nodeLabels().sort(), ["Company", "Person"]);
  assert.deepEqual(query.relationshipTypes(), ["KNOWS"]);
  assert.equal(query.queryText().startsWith("MATCH (a:Person)"), true);
});

test("execute with parameters and strategies", async () => {
  const query = new CypherQuery("MATCH (p:Person) WHERE p.age > $min RETURN p.name ORDER BY p.name")
    .withConfig(config)
    .withParameter("min", 28);
  assert.deepEqual(columns(await query.execute(datasets))["p.name"], ["Bob", "Carol"]);
  const viaDataFusion = await query.execute(datasets, { strategy: ExecutionStrategy.DataFusion });
  assert.equal(viaDataFusion.numRows, 2);
  await assert.rejects(query.execute(datasets, { strategy: ExecutionStrategy.LanceNative }), {
    code: "NotImplemented",
  });
});

test("execute without a config or with a missing dataset", async () => {
  await assert.rejects(new CypherQuery("MATCH (p:Person) RETURN p").execute(datasets), {
    code: "InvalidArgument",
  });
  await assert.rejects(
    new CypherQuery("MATCH (p:Person)-[:KNOWS]->(q) RETURN q.name")
      .withConfig(config)
      .execute({ Person: people }),
    LanceCsrError,
  );
});

test("an empty Arrow table", async () => {
  const empty = tableFromArrays({ id: new BigInt64Array(), name: [] as string[] });
  const out = await new CypherQuery("MATCH (p:Person) RETURN p.name")
    .withConfig(config)
    .execute({ Person: empty });
  assert.equal(out.numRows, 0);
});

test("a RecordBatch is accepted as a dataset", async () => {
  const out = await new CypherQuery("MATCH (p:Person) RETURN count(*) AS n")
    .withConfig(config)
    .execute({ Person: people.batches[0]! });
  assert.deepEqual(rows(out), [{ n: 3 }]);
});

test("explain", async () => {
  const plan = await new CypherQuery("MATCH (p:Person) WHERE p.age > 30 RETURN p.name")
    .withConfig(config)
    .explain(datasets);
  assert.match(plan, /physical_plan/);
});

test("toSql in each dialect", async () => {
  const query = new CypherQuery(
    "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30 RETURN a.name, b.name",
  ).withConfig(config);
  const sql = await query.toSql(datasets);
  assert.match(sql, /JOIN/i);
  for (const dialect of Object.values(SqlDialect).filter((v) => typeof v === "number")) {
    assert.match(await query.toSql(datasets, dialect as SqlDialect), /SELECT/i);
  }
});

test("executeWithNamespace reads <baseUri>/<label>.lance", async () => {
  const dir = tempDir();
  await writeLance(join(dir, "Person.lance"), people);
  await writeLance(join(dir, "KNOWS.lance"), knows);
  const out = await new CypherQuery(
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name",
  )
    .withConfig(config)
    .executeWithNamespace(new DirNamespace(dir));
  assert.deepEqual(rows(out), [
    { "a.name": "Alice", "b.name": "Bob" },
    { "a.name": "Bob", "b.name": "Carol" },
  ]);
  assert.equal(new DirNamespace(dir).baseUri, dir);
  await assert.rejects(
    new CypherQuery("RETURN 1").executeWithNamespace(dir as unknown as DirNamespace),
    TypeError,
  );
});
