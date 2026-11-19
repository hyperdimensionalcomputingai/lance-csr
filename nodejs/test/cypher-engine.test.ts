// CypherEngine over in-memory Arrow tables: queries, config access, options and stats.
import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { tableFromArrays } from "apache-arrow";
import { CypherEngine, CypherQuery, GraphConfig, LanceCsrError } from "../src/index.ts";
import { columns, rows } from "./helpers.ts";

const config = GraphConfig.builder()
  .nodeTable("Person", "person_id")
  .nodeTable("Company", "company_id")
  .relTable("WORKS_FOR", "Person", "Company", "person_id", "company_id")
  .relTable("FRIEND_OF", "Person", "Person", "person1_id", "person2_id")
  .build();

const datasets = {
  Person: tableFromArrays({
    person_id: BigInt64Array.from([1n, 2n, 3n, 4n]),
    name: ["Alice", "Bob", "Carol", "David"],
    age: BigInt64Array.from([28n, 34n, 29n, 42n]),
    city: ["New York", "San Francisco", "New York", "Chicago"],
  }),
  Company: tableFromArrays({
    company_id: BigInt64Array.from([101n, 102n, 103n]),
    company_name: ["TechCorp", "DataInc", "CloudSoft"],
  }),
  WORKS_FOR: tableFromArrays({
    person_id: BigInt64Array.from([1n, 2n, 3n, 4n]),
    company_id: BigInt64Array.from([101n, 101n, 102n, 103n]),
  }),
  FRIEND_OF: tableFromArrays({
    person1_id: BigInt64Array.from([1n, 1n, 2n, 3n]),
    person2_id: BigInt64Array.from([2n, 3n, 4n, 4n]),
  }),
};

describe("CypherEngine", async () => {
  const engine = await CypherEngine.create(config, datasets);

  test("basic query", async () => {
    const data = columns(await engine.execute("MATCH (p:Person) RETURN p.name, p.age"));
    assert.deepEqual(Object.keys(data).sort(), ["p.age", "p.name"]);
    assert.equal(data["p.name"]!.length, 4);
  });

  test("filter, traversal and aggregation", async () => {
    const older = columns(await engine.execute("MATCH (p:Person) WHERE p.age > 30 RETURN p.name"));
    assert.deepEqual(new Set(older["p.name"]), new Set(["Bob", "David"]));

    const jobs = await engine.execute(
      "MATCH (p:Person)-[:WORKS_FOR]->(c:Company) " +
        "RETURN p.name AS person, c.company_name AS company ORDER BY person",
    );
    assert.deepEqual(rows(jobs)[0], { person: "Alice", company: "TechCorp" });

    const stats = rows(
      await engine.execute("MATCH (p:Person) RETURN count(*) AS total, avg(p.age) AS avg_age"),
    );
    assert.equal(stats[0]!.total, 4);
    assert.equal(stats[0]!.avg_age, 33.25);
  });

  test("matches CypherQuery", async () => {
    const text = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name, p.age ORDER BY p.age";
    const viaQuery = await new CypherQuery(text).withConfig(config).execute(datasets);
    assert.deepEqual(rows(await engine.execute(text)), rows(viaQuery));
  });

  test("concurrent queries", async () => {
    const results = await Promise.all(
      Array.from({ length: 8 }, (_, i) =>
        engine.execute("MATCH (p:Person) WHERE p.age > $min RETURN count(*) AS n", { min: 25 + i }),
      ),
    );
    assert.deepEqual(
      results.map((t) => rows(t)[0]!.n),
      [4, 4, 4, 3, 2, 2, 2, 2],
    );
  });

  test("config is the GraphConfig passed in", () => {
    assert.equal(engine.config(), config);
    assert.ok(engine.config().nodeLabels().includes("person"));
    assert.ok(engine.config().relationshipTypes().includes("friend_of"));
  });

  test("explain, with and without analyze", async () => {
    const plan = await engine.explain("MATCH (p:Person) WHERE p.age > $a RETURN p.name", { a: 30 });
    assert.match(plan, /physical_plan/);
    const analyzed = await engine.explain("MATCH (p:Person) RETURN p.name", null, {
      analyze: true,
    });
    assert.match(analyzed, /physical_plan_with_metrics/);
    assert.match(analyzed, /output_rows/);
  });

  test("Arrow inputs have no Lance versions", () => {
    assert.deepEqual(engine.datasetVersions(), {});
    assert.deepEqual(engine.csrStats(), {});
    assert.deepEqual(engine.nodeIndexStats(), {});
  });

  test("errors carry the Python exception's kind as a code", async () => {
    await assert.rejects(engine.execute("MATCH (p:Person RETURN p"), (err: unknown) => {
      assert.ok(err instanceof LanceCsrError);
      assert.equal(err.code, "InvalidArgument");
      assert.ok(err.cause instanceof Error);
      return true;
    });
    await assert.rejects(engine.execute("RETURN unknown"), { code: "InvalidArgument" });
  });
});

describe("CypherEngine options", () => {
  test("useCsr builds indexes and reports stats", async () => {
    const engine = await CypherEngine.create(config, datasets, { useCsr: true });
    const stats = engine.csrStats();
    assert.deepEqual(Object.keys(stats).sort(), ["FRIEND_OF", "WORKS_FOR"]);
    assert.equal(stats.FRIEND_OF!.edges, 4);
    assert.equal(stats.FRIEND_OF!.update, "build");
    assert.equal(stats.FRIEND_OF!.version, undefined);
    assert.equal(engine.nodeIndexStats().Person!.rows, 4);
    const twoHop = await engine.execute(
      "MATCH (a:Person)-[:FRIEND_OF]->()-[:FRIEND_OF]->(c:Person) RETURN count(*) AS n",
    );
    assert.equal(rows(twoHop)[0]!.n, 2);
  });

  test("targetPartitions keeps results", async () => {
    for (const targetPartitions of [1, 3, 1024]) {
      const engine = await CypherEngine.create(config, datasets, { targetPartitions });
      const result = await engine.execute("MATCH (p:Person) RETURN p.name ORDER BY p.name");
      assert.deepEqual(columns(result)["p.name"], ["Alice", "Bob", "Carol", "David"]);
    }
  });

  test("targetPartitions out of range or not an integer", async () => {
    for (const targetPartitions of [0, -1, 1.5, 1025, Number.NaN]) {
      await assert.rejects(CypherEngine.create(config, datasets, { targetPartitions }), {
        code: "InvalidArgument",
        message: /targetPartitions|target_partitions/,
      });
    }
    await assert.rejects(
      CypherEngine.create(config, datasets, { targetPartitions: true as unknown as number }),
      TypeError,
    );
  });

  test("persistCsr needs useCsr", async () => {
    await assert.rejects(CypherEngine.create(config, datasets, { persistCsr: true }), {
      code: "InvalidArgument",
      message: /persistCsr requires useCsr/,
    });
  });

  test("argument types", async () => {
    await assert.rejects(CypherEngine.create({} as GraphConfig, datasets), TypeError);
    await assert.rejects(
      CypherEngine.create(config, { Person: [1, 2] as unknown as never }),
      TypeError,
    );
    await assert.rejects(CypherEngine.create(config, {}), { code: "InvalidArgument" });
  });

  test("a missing key column fails at create", async () => {
    await assert.rejects(
      CypherEngine.create(GraphConfig.builder().nodeTable("Person", "nope").build(), {
        Person: datasets.Person,
      }),
      LanceCsrError,
    );
  });
});
