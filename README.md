# lance-csr

lance-csr is a Cypher graph query engine over [Lance](https://lancedb.github.io/lance/) datasets, built in Rust on
Apache DataFusion, with Python bindings. It is a fork of [lance-graph](https://github.com/lance-format/lance-graph)
and a research test bed: it pushes the limits of CSR indexes and DataFusion on top of the Lance format, with the
goal of making multi-hop path traversal fast.

## Goals

lance-csr is built for single-machine execution. It aims to:

- **Provide a fast, lightweight and portable query engine.** An openCypher-compatible engine on top of the Lance
  format that runs on one machine, inside your process, with no server to operate.
- **Enable multimodal knowledge graphs.** Lance stores images, video, audio and other media alongside vectors and
  structured columns. lance-csr runs fast path traversal queries over those same datasets, so a knowledge graph
  works on a single copy of the data instead of a second copy in a separate graph database.
- **Push the limits of DataFusion for graph traversal.** We want to find out how far DataFusion's execution engine
  can go in powering multi-hop graph traversal, an area with little open-source work so far.

### Out of scope: distributed execution

Graph queries across multiple machines are **explicitly out of scope** for this repository, to keep it free of
complexity that works against the goals above. **Issues and pull requests that target distributed execution will
be closed as out of scope.**

If your use case involves distributed graph query processing or graph algorithms, that is exactly the kind of
engine we're building at HDC Labs. [Reach out to us](https://hyperdimensionalcomputing.ai/contact/) to chat more.

## Why a CSR

lance-graph runs each relationship hop as a join between the edge and node tables. A multi-hop query repeats that
join at every hop, building a hash table and materializing every intermediate path each time.
[Compressed sparse row (CSR)](https://en.wikipedia.org/wiki/Sparse_matrix#Compressed_sparse_row_(CSR,_CRS_or_Yale_format))
is the standard compact layout for a sparse matrix, and so for a graph's adjacency: an offsets array indexed by node
points into one array of neighbor ids, so each node's neighbors sit next to each other. Expanding a hop becomes an
array lookup per node instead of a join. lance-csr builds this index from the relationship tables and runs it inside
DataFusion as extension operators, falling back to a join for any hop the CSR can't express.

## What this fork adds

- **CSR traversal** (`use_csr=True`) for single hops and variable-length paths, directed or undirected, updated
  incrementally when the underlying Lance datasets change.
- **A Lance-native engine** that keeps each table either on disk (read lazily from a Lance snapshot) or in memory,
  and `refresh()`es to new dataset versions.
- **Smarter planning**: patterns start at their most selective node, and DataFusion gets column statistics to size
  its joins.
- **Much wider Cypher**, enough for all 14 LDBC Interactive complex queries: `OPTIONAL MATCH`, `EXISTS`, chained
  `WITH`, relationship alternation, `shortestPath` and more.
- **Correctness fixes** to behavior inherited from lance-graph (for example, undirected patterns dropped incoming
  edges), each with a regression test.
- **Current dependencies** (Lance 13, DataFusion 54, Arrow 58, PyO3 0.28), with Delta Lake and Unity Catalog support
  removed.

## Repository layout

- `crates/lance-graph` – the Cypher-capable query engine implemented in Rust
- `python/` – PyO3 bindings and Python packages:
  - `lance_csr` – thin wrapper around the Rust query engine

See [`docs/project_structure.md`](docs/project_structure.md) for the full workspace layout.

## Prerequisites

- Rust toolchain (`rust-toolchain.toml` pins the version; rustup installs it automatically)
- Python 3.12 or newer
- [`uv`](https://docs.astral.sh/uv/) available on your `PATH`

## Rust crate quick start

```bash
cd crates/lance-graph
cargo check
cargo test
```

## Install

```bash
pip install lance-csr
```

Prebuilt wheels cover Linux x86-64 and native Apple Silicon (CPython 3.12+); no Rust toolchain is needed.
See [`python/README.md`](python/README.md) for details and usage.

## Python package quick start (from source)

```bash
cd python
uv venv --python 3.12 .venv      # create the local virtualenv
source .venv/bin/activate         # activate the virtual environment
uv pip install 'maturin[patchelf]' # install build tool
uv pip install -e '.[tests]'     # editable install with test extras
maturin develop                   # build and install the Rust extension
pytest python/tests/ -v          # run the test suite
```

> If another virtual environment is already active, run `deactivate` (or
> `unset VIRTUAL_ENV`) before the `uv run` command so uv binds to `.venv`.

## Python example: Cypher query

```python
import pyarrow as pa
from lance_csr import CypherQuery, GraphConfig

people = pa.table({
    "person_id": [1, 2, 3, 4],
    "name": ["Alice", "Bob", "Carol", "David"],
    "age": [28, 34, 29, 42],
})

config = (
    GraphConfig.builder()
    .with_node_label("Person", "person_id")
    .build()
)

query = (
    CypherQuery("MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name, p.age AS age")
    .with_config(config)
)
result = query.execute({"Person": people})
print(result.to_pydict())  # {'name': ['Bob', 'David'], 'age': [34, 42]}
```

## Python example: Direct SQL query

For data analytics workflows where you prefer standard SQL, use `SqlQuery` or `SqlEngine`. No `GraphConfig` is needed:

```python
import pyarrow as pa
from lance_csr import SqlQuery, SqlEngine

person = pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Carol"],
    "age": [28, 34, 29],
})

# One-off query
result = SqlQuery(
    "SELECT name, age FROM person WHERE age > 30"
).execute({"person": person})
print(result.to_pydict())  # {'name': ['Bob'], 'age': [34]}

# Multi-query with cached context
engine = SqlEngine({"person": person})
r1 = engine.execute("SELECT COUNT(*) AS cnt FROM person")
r2 = engine.execute("SELECT name FROM person ORDER BY age DESC LIMIT 2")
```

## Benchmarks

lance-csr is benchmarked with [graph-benchmark-ldbc](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc):
30 Cypher queries over the LDBC SNB SF1 dataset (3.18M nodes, 17.3M relationships), with every result checked
against an expected answer. These are independent, unaudited measurements, not LDBC Benchmark Results.

Mean query latency in milliseconds (lower is better, fastest per query in bold), on an Apple M5 MacBook Pro running
macOS 26.6. lance-csr runs with the CSR (`use_csr=True`) in two modes: *in memory* holds every table as Arrow in
memory, and *lazy* reads Lance datasets from disk as queries need them.

| Query | lance-csr 0.1.0, in memory | lance-csr 0.1.0, lazy | Neo4j 2026.09.0 | Kuzu 0.11.3 | Ladybug 0.21.2 | lance-graph 0.5.4 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | **0.64** | 1.34 | 3.14 | 3.25 | 1.00 | 1.40 |
| Q2 | **0.55** | 1.45 | 4.66 | 1.58 | 1.17 | 2.29 |
| Q3 | **0.66** | 0.80 | 1.59 | 1.61 | 0.78 | 1.95 |
| Q4 | 0.52 | 0.81 | 3.11 | 1.07 | **0.37** | 3.02 |
| Q5 | **0.79** | 2.15 | 3.51 | 3.99 | 3.98 | 2.04 |
| Q6 | **0.45** | 0.72 | 2.52 | 1.04 | 0.79 | 0.72 |
| Q7 | 1.07 | 1.08 | **0.91** | 28.76 | 25.46 | 16.59 |
| Q8 | **0.66** | 1.77 | 13.51 | 2.80 | 3.52 | 1.61 |
| Q9 | **0.84** | 1.36 | 1.62 | 1.91 | 1.33 | 2.33 |
| Q10 | **0.75** | 0.96 | 5.11 | 1.60 | 1.56 | 32.58 |
| Q11 | **1.50** | 1.98 | 14.94 | 8.93 | 4.08 | 3.27 |
| Q12 | **0.72** | 6.81 | 3.71 | 21.03 | 8.10 | 21.67 |
| Q13 | **0.76** | 1.01 | 9.45 | 44.76 | 5.70 | 9.79 |
| Q14 | **0.50** | 0.64 | 0.83 | 1.76 | 0.89 | 2.50 |
| Q15 | **0.53** | 0.71 | 2.71 | 3.03 | 1.10 | 2.17 |
| Q16 | **0.66** | 1.08 | 1.45 | 2.05 | 1.00 | 4.19 |
| Q17 | **0.93** | 1.40 | 3.61 | 3.16 | 1.72 | 2.39 |
| Q18 | **0.66** | 1.06 | 3.72 | 1.48 | 1.20 | 1.89 |
| Q19 | **0.96** | 1.33 | 6.82 | 16.76 | 3.15 | 20.96 |
| Q20 | **2.05** | 16.54 | 478.58 | 11.64 | 13.61 | 2.98 |
| Q21 | 0.72 | 0.80 | 1.00 | **0.43** | 0.52 | 1.51 |
| Q22 | **0.68** | 0.88 | 3.14 | 28.00 | 7.88 | 14.27 |
| Q23 | **0.70** | 1.22 | 4.78 | 1.17 | 0.73 | 2.47 |
| Q24 | **0.66** | 1.03 | 1.16 | 1.16 | 0.79 | 1.79 |
| Q25 | **0.76** | 1.14 | 3.74 | 1.43 | 1.37 | 1.35 |
| Q26 | **0.71** | 1.02 | 1.18 | 3.46 | 1.15 | 2.88 |
| Q27 | **0.82** | 1.26 | 2.77 | 14.32 | 4.50 | 24.70 |
| Q28 | **1.29** | 2.25 | 4.12 | 1.48 | 2.32 | 2.50 |
| Q29 | **0.65** | 0.98 | 3.36 | 1.00 | 0.76 | 2.73 |
| Q30 | **36.17** | 37.09 | 1,255.75 | 149.55 | 92.89 | 38.17 |

Over the 30 queries (geometric mean), lance-csr in memory is 5.1× faster than Neo4j, 4.4× faster than Kuzu, 2.5×
faster than Ladybug and 4.6× faster than lance-graph; in lazy mode it is 3.0×, 2.6×, 1.5× and 2.7× faster.

- Neo4j, Kuzu, Ladybug and lance-graph numbers are from graph-benchmark-ldbc's 2026-10-16 run. lance-csr was measured
  on 2026-10-23 on the same machine with the same pytest-benchmark settings (the faster of two passes); its runner
  is not yet part of that repository.
- Neo4j runs as a server in a Docker VM; the other engines run embedded in the Python process. See the benchmark
  repository for each engine's configuration and how to reproduce the runs.

## Credits

lance-csr builds on the work of two open-source projects:

- The maintainers and contributors of [lance-graph](https://github.com/lance-format/lance-graph), who built the
  foundation lance-csr stands on: the Cypher parser, semantic analysis, the DataFusion planner, the graph
  configuration and the Python bindings. See [`NOTICE`](NOTICE) for the fork point.
- The contributors of [Apache DataFusion](https://github.com/apache/datafusion), whose query engine does much of
  the work here: planning, optimization and vectorized execution, which lance-csr extends with its CSR traversal
  operators.
