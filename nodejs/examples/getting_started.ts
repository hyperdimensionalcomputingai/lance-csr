// Getting started (docs/getting-started.md) includes the
// sections of this file between the `--8<--` markers, and CI runs it, so the documented code
// works. Keep the output comments in step with what it prints.
//
// Run it from nodejs/ after `npm run build:native` and `npm run build`:
//   node examples/getting_started.ts

// --8<-- [start:gs_datasets]
import { tableFromArrays } from "apache-arrow";

const datasets = {
  Person: tableFromArrays({
    id: BigInt64Array.from([1n, 2n, 3n, 4n]),
    name: ["Alice", "Bob", "Carol", "David"],
    age: BigInt64Array.from([28n, 34n, 29n, 42n]),
  }),
  City: tableFromArrays({ id: BigInt64Array.from([10n, 20n]), name: ["London", "Sydney"] }),
  KNOWS: tableFromArrays({
    src: BigInt64Array.from([1n, 1n, 2n, 3n]),
    dst: BigInt64Array.from([2n, 3n, 4n, 4n]),
  }),
  LIVES_IN: tableFromArrays({
    src: BigInt64Array.from([1n, 2n, 3n, 4n]),
    dst: BigInt64Array.from([10n, 10n, 20n, 20n]),
  }),
};
// --8<-- [end:gs_datasets]

// --8<-- [start:gs_config]
import { GraphConfig } from "lance-csr";

const config = GraphConfig.builder()
  .nodeTable("Person", "id")
  .nodeTable("City", "id")
  .relTable("KNOWS", "Person", "Person", "src", "dst")
  .relTable("LIVES_IN", "Person", "City", "src", "dst")
  .build();
// --8<-- [end:gs_config]

// --8<-- [start:gs_query]
import { CypherEngine } from "lance-csr";

let engine = await CypherEngine.create(config, datasets);

const result = await engine.execute(`
  MATCH (p:Person)-[:LIVES_IN]->(c:City {name: 'London'})
  RETURN p.name AS name, p.age AS age
  ORDER BY age
`);
console.log(result.toArray().map((row) => row.toJSON()));
// [ { name: 'Alice', age: 28n }, { name: 'Bob', age: 34n } ]
// --8<-- [end:gs_query]

// --8<-- [start:gs_params]
const older = await engine.execute(
  "MATCH (p:Person) WHERE p.age >= $min_age RETURN p.name AS name ORDER BY name",
  { min_age: 30 },
);
console.log(older.getChild("name")?.toArray());
// [ 'Bob', 'David' ]
// --8<-- [end:gs_params]

// --8<-- [start:gs_csr]
engine = await CypherEngine.create(config, datasets, { useCsr: true });

const friendsOfFriends = await engine.execute(`
  MATCH (a:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(c:Person)
  RETURN DISTINCT c.name AS friend_of_friend
`);
console.log(friendsOfFriends.toArray().map((row) => row.toJSON()));
// [ { friend_of_friend: 'David' } ]
// --8<-- [end:gs_csr]

// --8<-- [start:gs_one_off]
import { CypherQuery } from "lance-csr";

const oneOff = await new CypherQuery(
  "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name ORDER BY name",
)
  .withConfig(config)
  .execute(datasets);
console.log(oneOff.getChild("name")?.toArray());
// [ 'Bob', 'David' ]
// --8<-- [end:gs_one_off]
