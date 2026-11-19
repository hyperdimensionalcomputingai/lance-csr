// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The lance-csr Authors

// Exercise the installed npm packages outside the source tree, without Rust.
//
// Copy this file into a fresh project that installed the packed `lance-csr` and its
// platform package from tarballs, plus apache-arrow, and run it there. Covers both table
// placements (Arrow tables in memory, Lance datasets on disk), CSR traversal,
// shortestPath, streaming and `checkoutLatest()` picking up a new Lance version.

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, realpathSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { tableFromArrays, tableToIPC } from "apache-arrow";
import { CypherEngine, GraphConfig, LanceCsrError, version } from "lance-csr";

const require = createRequire(import.meta.url);
const TWO_HOPS =
  "MATCH (a:Person {id: 1})-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN c.name AS name";
const SHORTEST =
  "MATCH (a:Person {id: 1}), (b:Person {id: 4}) " +
  "MATCH p = shortestPath((a)-[:KNOWS*1..5]-(b)) RETURN length(p) AS hops";

for (const tool of ["cargo", "rustc"]) {
  let found = true;
  try {
    execFileSync(process.platform === "win32" ? "where" : "which", [tool], { stdio: "ignore" });
  } catch {
    found = false;
  }
  assert.equal(found, false, `Remove ${tool} from the smoke-test PATH`);
}

// The installed packages, not the source tree.
const root = realpathSync(join(require.resolve("lance-csr"), "..", ".."));
assert.ok(root.includes("node_modules"), root);
const manifest = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
assert.equal(version, manifest.version);
const platform = Object.keys(manifest.optionalDependencies ?? {}).find((name) => {
  try {
    require.resolve(name);
    return true;
  } catch {
    return false;
  }
});
assert.ok(platform, "a platform package from optionalDependencies is installed");
assert.equal(manifest.optionalDependencies[platform], manifest.version);

const config = GraphConfig.builder()
  .nodeTable("Person", "id")
  .relTable("KNOWS", "Person", "Person", "src", "dst")
  .build();
const people = tableFromArrays({
  id: BigInt64Array.from([1n, 2n, 3n, 4n]),
  name: ["Alice", "Bob", "Carol", "Dan"],
});
const knows = tableFromArrays({
  src: BigInt64Array.from([1n, 2n, 3n]),
  dst: BigInt64Array.from([2n, 3n, 4n]),
});
const values = (table, column) => [...table.getChild(column)].map((v) => v);

async function check(datasets) {
  const engine = await CypherEngine.create(config, datasets, { useCsr: true });
  assert.deepEqual(values(await engine.execute(TWO_HOPS), "name"), ["Carol"]);
  assert.deepEqual(values(await engine.execute(SHORTEST), "hops"), [3n]);
  let rows = 0;
  for await (const batch of await engine.executeBatches("MATCH (p:Person) RETURN p.id")) {
    rows += batch.numRows;
  }
  assert.equal(rows, 4);
  await assert.rejects(engine.execute("MATCH (p:Person RETURN p"), LanceCsrError);
  return engine;
}

await check({ Person: people, KNOWS: knows });

const dir = mkdtempSync(join(tmpdir(), "lance-csr-npm-"));
try {
  // The platform binary's fixture writer: not public API, but present in release builds.
  const write = (uri, table, mode) =>
    require(platform)._writeLanceDataset(
      uri,
      Buffer.from(tableToIPC(table, "stream")),
      mode,
      undefined,
    );
  const person = join(dir, "person.lance");
  await write(person, people, "create");
  await write(join(dir, "knows.lance"), knows, "create");
  const engine = await check({
    Person: { uri: person },
    KNOWS: { uri: join(dir, "knows.lance") },
  });
  await write(person, tableFromArrays({ id: BigInt64Array.from([5n]), name: ["Eve"] }), "append");
  await engine.checkoutLatest();
  assert.equal((await engine.execute("MATCH (p:Person) RETURN p.id")).numRows, 5);
  assert.deepEqual(engine.datasetVersions(), { Person: 2, KNOWS: 1 });
} finally {
  rmSync(dir, { recursive: true, force: true });
}

console.log(`lance-csr ${version} works from ${platform}`);
