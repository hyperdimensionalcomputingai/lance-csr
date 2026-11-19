// The TypeScript guide (docs/typescript.md) includes the sections of this file between the
// `--8<--` markers, and CI runs it, so the documented code works. Keep the output comments
// in step with what it prints.
//
// Run it from nodejs/ after `npm run build:native` and `npm run build`:
//   node examples/guide.ts

// --8<-- [start:engine]
import { tableFromArrays } from "apache-arrow";
import { CypherEngine, GraphConfig } from "lance-csr";

const config = GraphConfig.builder()
  .nodeTable("Person", "id")
  .relTable("KNOWS", "Person", "Person", "src", "dst")
  .build();

const people = tableFromArrays({
  id: BigInt64Array.from([1n, 2n, 3n]),
  name: ["Alice", "Bob", "Carol"],
  age: Int32Array.from([29, 35, 41]),
});
const knows = tableFromArrays({
  src: BigInt64Array.from([1n, 2n]),
  dst: BigInt64Array.from([2n, 3n]),
});

const engine = await CypherEngine.create(
  config,
  { Person: people, KNOWS: knows },
  { useCsr: true },
);
// --8<-- [end:engine]

// --8<-- [start:query]
const result = await engine.execute(
  "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > $min RETURN a.name, b.name ORDER BY a.name",
  { min: 30 },
);
console.log(result.toArray().map((row) => row.toJSON()));
// [ { 'a.name': 'Bob', 'b.name': 'Carol' } ]
// --8<-- [end:query]

// --8<-- [start:types]
const ids = await engine.execute("MATCH (p:Person) RETURN p.id AS id ORDER BY id");
console.log(ids.getChild("id")?.toArray());
// BigInt64Array(3) [ 1n, 2n, 3n ]
console.log(String(ids.schema.fields[0]?.type));
// Int64
// --8<-- [end:types]

// --8<-- [start:stream]
const stream = await engine.executeBatches("MATCH (p:Person) RETURN p.name");
let rows = 0;
for await (const batch of stream) {
  rows += batch.numRows;
}
console.log(rows);
// 3
// --8<-- [end:stream]

// --8<-- [start:errors]
import { LanceCsrError } from "lance-csr";

try {
  await engine.execute("MATCH (p:Person) RETURN p.name", null, { timeoutMs: 0 });
} catch (err) {
  if (err instanceof LanceCsrError) {
    console.log(err.code, err.message);
    // Timeout Query timeout exceeded
  }
}
// --8<-- [end:errors]

// --8<-- [start:query-builder]
import { CypherQuery } from "lance-csr";

const query = new CypherQuery("MATCH (p:Person) WHERE p.age < $max RETURN p.name ORDER BY p.name")
  .withConfig(config)
  .withParameter("max", 40);
console.log(query.variables(), query.parameters());
// [ 'p' ] { max: 40 }
const young = await query.execute({ Person: people });
console.log(young.getChild("p.name")?.toArray());
// [ 'Alice', 'Bob' ]
// --8<-- [end:query-builder]

// --8<-- [start:sql]
import { SqlEngine } from "lance-csr";

const sql = SqlEngine.fromEngine(engine);
const oldest = await sql.execute("SELECT name FROM person ORDER BY age DESC LIMIT 1");
console.log(oldest.getChild("name")?.toArray());
// [ 'Carol' ]
// --8<-- [end:sql]

// --8<-- [start:explicit-types]
import { Int64, Table, Utf8, vectorFromArray } from "apache-arrow";

const cities = new Table({
  id: vectorFromArray([10n, 20n], new Int64()),
  name: vectorFromArray(["London", "Sydney"], new Utf8()),
});
console.log(cities.schema.fields.map((field) => `${field.name}: ${field.type}`));
// [ 'id: Int64', 'name: Utf8' ]
// --8<-- [end:explicit-types]
