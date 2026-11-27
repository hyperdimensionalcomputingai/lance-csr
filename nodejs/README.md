# lance-csr

A fast [openCypher](https://opencypher.org/) graph query engine over [Lance](https://lance.org/)
datasets and Arrow tables, for Node.js. It is written in Rust on Apache DataFusion, and its compressed sparse row
(CSR) adjacency index turns each relationship hop into an array lookup, which makes multi-hop traversals fast.
The same engine ships as the `lance-csr` Python package and Rust crate.

**Documentation:** [lance-csr.hdclabs.ai](https://lance-csr.hdclabs.ai/), with a
[TypeScript guide](https://lance-csr.hdclabs.ai/typescript/).

## Install

```bash
npm install lance-csr apache-arrow
```

Prebuilt binaries cover Linux x86-64 and aarch64 (glibc 2.28+) and Apple Silicon (macOS 11+), on Node.js 22 or
newer. The x86-64 binaries need a CPU with AVX2. On Windows, use WSL2, where the Linux binary installs. `apache-arrow` 21 is a peer dependency.

## Example

```typescript
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
```

Lance datasets on disk or object storage are passed as `{ uri, version?, storageOptions? }` and read lazily,
pinned to a version until `engine.checkoutLatest()`.

## License

Apache-2.0. lance-csr is a fork of [lance-graph](https://github.com/lance-format/lance-graph); see the
[repository](https://github.com/hyperdimensionalcomputingai/lance-csr).
