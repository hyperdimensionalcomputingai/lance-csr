// `$name` parameters: JavaScript values sent as JSON, with the Python bindings' checks.
import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { Field, FixedSizeList, Float32, Int64, Table, makeData, makeVector } from "apache-arrow";
import { CypherEngine, CypherQuery, GraphConfig, type Parameter } from "../src/index.ts";
import { rows } from "./helpers.ts";

const BIG = 19791209300775n;
const config = GraphConfig.builder().nodeTable("Person", "id").build();

function personTable(): Table {
  const id = makeVector(BigInt64Array.from([1n, BIG]));
  const embedding = makeVector(
    makeData({
      type: new FixedSizeList(2, new Field("item", new Float32(), true)),
      length: 2,
      nullCount: 0,
      child: makeData({ type: new Float32(), data: Float32Array.from([1, 0, 0, 1]) }),
    }),
  );
  return new Table({ id, embedding });
}

describe("parameters", async () => {
  const tables = { Person: personTable() };
  assert.ok(tables.Person.schema.fields[0]!.type instanceof Int64);
  for (const useCsr of [false, true]) {
    const engine = await CypherEngine.create(config, tables, { useCsr });
    const label = useCsr ? "csr" : "join";

    test(`${label}: lists of 64-bit ids, in execute, streams and IN`, async () => {
      const query = "UNWIND $ids AS id MATCH (p:Person {id: id}) RETURN p.id AS id ORDER BY id";
      const params = { ids: [1, BIG] };
      const expected = [{ id: 1n }, { id: BIG }];
      const exact = (t: Table) => t.toArray().map((r) => r.toJSON());
      assert.deepEqual(exact(await engine.execute(query, params)), expected);
      const batches = await Array.fromAsync(await engine.executeBatches(query, params));
      assert.deepEqual(exact(new Table(batches)), expected);
      assert.deepEqual(
        exact(
          await engine.execute(
            "MATCH (p:Person) WHERE p.id IN $ids RETURN p.id AS id ORDER BY id",
            params,
          ),
        ),
        expected,
      );
      assert.match(await engine.explain(query, params), /Projection/);
    });

    test(`${label}: containers round-trip`, async () => {
      const values: Parameter[] = [
        [],
        [null],
        [1, 2],
        [[1, 2], []],
        {},
        { name: "a", ids: [1, 2], missing: null },
      ];
      for (const value of values) {
        assert.deepEqual(rows(await engine.execute("RETURN $value AS value", { value })), [
          { value },
        ]);
        const query = new CypherQuery("RETURN $value AS value")
          .withConfig(config)
          .withParameter("value", value);
        assert.deepEqual(rows(await query.execute(tables)), [{ value }]);
      }
    });

    test(`${label}: a mixed list follows literal coercion`, async () => {
      assert.deepEqual(rows(await engine.execute("RETURN $v AS v", { v: [1, "a"] })), [
        { v: ["1", "a"] },
      ]);
    });

    test(`${label}: vector parameters`, async () => {
      const out = await engine.execute(
        "MATCH (p:Person) RETURN p.id AS id ORDER BY vector_distance(p.embedding, $q, l2) LIMIT 1",
        { q: [0.1, 0.9] },
      );
      assert.deepEqual(rows(out), [{ id: Number(BIG) }]);
    });
  }

  const engine = await CypherEngine.create(config, tables);

  test("whole numbers are integers, others floats, booleans and strings as-is", async () => {
    const out = await engine.execute("RETURN $i AS i, $f AS f, $b AS b, $s AS s", {
      i: 3,
      f: 1.5,
      b: true,
      s: "x",
    });
    assert.deepEqual(
      out.schema.fields.map((f) => String(f.type)),
      ["Int64", "Float64", "Bool", "Utf8"],
    );
    assert.deepEqual(rows(out), [{ i: 3, f: 1.5, b: true, s: "x" }]);
  });

  test("a Date compares with timestamps", async () => {
    const out = await engine.execute("RETURN $d AS d, $d > '2024-01-01' AS after", {
      d: new Date("2024-01-02T03:04:05Z"),
    });
    assert.deepEqual(rows(out), [{ d: "2024-01-02T03:04:05.000Z", after: true }]);
    const bound = new CypherQuery("RETURN $d AS d").withParameter("d", new Date(0));
    assert.deepEqual(bound.parameters(), { d: "1970-01-01T00:00:00.000Z" });
  });

  test("bigints keep every digit and must fit 64 bits", async () => {
    const exact = (await engine.execute("RETURN $b AS b", { b: 2n ** 62n + 1n })).toArray()[0]!.b;
    assert.equal(exact, 2n ** 62n + 1n);
    for (const b of [2n ** 63n, -(2n ** 63n) - 1n, 2n ** 64n]) {
      await assert.rejects(engine.execute("RETURN $b AS b", { b }), {
        code: "InvalidArgument",
        message: /64-bit/,
      });
    }
  });

  test("unsupported values", async () => {
    const bad: unknown[] = [
      Number.NaN,
      Number.POSITIVE_INFINITY,
      [() => 1],
      { m: new Map() },
      Symbol("s"),
      new Set([1]),
    ];
    for (const v of bad) {
      await assert.rejects(engine.execute("RETURN $v AS v", { v } as Record<string, Parameter>), {
        code: "InvalidArgument",
      });
    }
    assert.throws(() => new CypherQuery("RETURN $v").withParameter("v", Number.NaN), {
      code: "InvalidArgument",
    });
    await assert.rejects(
      engine.execute("RETURN 1", [1] as unknown as Record<string, Parameter>),
      TypeError,
    );
  });

  test("cyclic and deeply nested containers are rejected", async () => {
    const cyclic: unknown[] = [];
    cyclic.push(cyclic);
    await assert.rejects(engine.execute("RETURN $v AS v", { v: cyclic as Parameter }), {
      code: "InvalidArgument",
      message: /cyclic/,
    });
    let deep: Parameter = 1;
    for (let i = 0; i < 200; i++) deep = [deep];
    await assert.rejects(engine.execute("RETURN $v AS v", { v: deep }), {
      code: "InvalidArgument",
    });
  });

  test("undefined is null", async () => {
    const out = await engine.execute("RETURN $v AS v, $m AS m", {
      v: undefined as unknown as Parameter,
      m: { a: undefined as unknown as Parameter, b: 1 },
    });
    assert.deepEqual(rows(out), [{ v: null, m: { a: null, b: 1 } }]);
  });
});
