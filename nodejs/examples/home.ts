// The home page (docs/index.md) includes the section of this file between the `--8<--`
// markers, and CI runs it, so the documented code works. Keep the output comment in step
// with what it prints. Run it from nodejs/ after `npm run build:native` and `npm run build`:
//   node examples/home.ts

// --8<-- [start:quickstart]
import { tableFromArrays } from "apache-arrow";
import { CypherEngine, GraphConfig } from "lance-csr";

const config = GraphConfig.builder()
  .nodeTable("Person", "id")
  .relTable("KNOWS", "Person", "Person", "src", "dst")
  .build();
const engine = await CypherEngine.create(
  config,
  {
    Person: tableFromArrays({
      id: BigInt64Array.from([1n, 2n, 3n]),
      name: ["Alice", "Bob", "Carol"],
    }),
    KNOWS: tableFromArrays({
      src: BigInt64Array.from([1n, 2n]),
      dst: BigInt64Array.from([2n, 3n]),
    }),
  },
  { useCsr: true },
);
const result = await engine.execute(
  "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b) RETURN b.name AS friend ORDER BY friend",
);
console.log(result.toArray().map((row) => row.toJSON()));
// [ { friend: 'Bob' }, { friend: 'Carol' } ]
// --8<-- [end:quickstart]
