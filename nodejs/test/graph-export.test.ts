// CypherEngine.exportGraph: conversions to apache-arrow vectors, options and errors.
import assert from "node:assert/strict";
import { test } from "node:test";
import { Type, tableFromArrays } from "apache-arrow";
import { CypherEngine, GraphConfig, LanceCsrError } from "../src/index.ts";

const config = GraphConfig.builder()
  .nodeTable("Person", "id")
  .relTable("KNOWS", "Person", "Person", "src", "dst")
  .build();

// Keys in shuffled order, an isolated node (40), a duplicate edge and an edge to a missing node.
const datasets = {
  Person: tableFromArrays({ id: BigInt64Array.from([30n, 10n, 20n, 40n]) }),
  KNOWS: tableFromArrays({
    src: BigInt64Array.from([10n, 10n, 20n, 30n, 10n]),
    dst: BigInt64Array.from([20n, 20n, 30n, 10n, 99n]),
  }),
};

const list = (v: { toArray(): ArrayLike<bigint> } | null) => Array.from(v!.toArray(), Number);

test("directed export", async () => {
  const engine = await CypherEngine.create(config, datasets);
  const g = await engine.exportGraph();
  assert.equal(g.numNodes, 4);
  assert.equal(g.directed, true);
  assert.equal(g.droppedEdges, 1);
  assert.deepEqual(g.labelRanges, { Person: [0, 4] });
  // Dense ids follow row order: 30->0, 10->1, 20->2, 40->3.
  assert.equal(g.nodeKeys.type.typeId, Type.Int);
  assert.deepEqual(list(g.nodeKeys), [30, 10, 20, 40]);
  assert.deepEqual(list(g.outIndptr), [0, 1, 2, 3, 3]);
  assert.deepEqual(list(g.outIndices), [1, 2, 0]);
  assert.deepEqual(list(g.inIndptr), [0, 1, 2, 3, 3]);
  assert.deepEqual(list(g.inIndices), [2, 0, 1]);
});

test("undirected export", async () => {
  const engine = await CypherEngine.create(config, datasets);
  const g = await engine.exportGraph(["knows"], { directed: false });
  assert.equal(g.inIndptr, null);
  assert.equal(g.inIndices, null);
  assert.deepEqual(list(g.outIndptr), [0, 2, 4, 6, 6]);
  assert.deepEqual(list(g.outIndices), [1, 2, 0, 2, 0, 1]);
});

test("unknown relationship type", async () => {
  const engine = await CypherEngine.create(config, datasets);
  await assert.rejects(
    engine.exportGraph(["LIKES"]),
    (err) => err instanceof LanceCsrError && err.code === "InvalidArgument",
  );
});
