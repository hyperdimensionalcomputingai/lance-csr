# Lance datasets

[Lance](https://lancedb.github.io/lance/) is a columnar format for ML and multimodal data, with versioning,
fast random access and vector indexes. lance-csr can query Lance datasets on disk without loading them into
memory first.

## Lazy, version-pinned reads

Pass `lance.dataset(...)` handles to `CypherEngine` and the engine reads them **lazily**: nothing is scanned at
construction, and each query reads only the columns and rows it needs, with projections and filters pushed
into the Lance scan.

```python
import lance
import pyarrow as pa
from lance_csr import CypherEngine, GraphConfig

lance.write_dataset(
    pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]}), "person.lance"
)
lance.write_dataset(pa.table({"src": [1, 2], "dst": [2, 3]}), "knows.lance")

config = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("KNOWS", "Person", "Person", "src", "dst")
    .build()
)
engine = CypherEngine(
    config,
    {"Person": lance.dataset("person.lance"), "KNOWS": lance.dataset("knows.lance")},
    use_csr=True,
)
print(engine.execute("MATCH (p:Person) RETURN count(*) AS n").to_pylist())  # [{'n': 3}]
print(engine.dataset_versions())  # {'Person': 1, 'KNOWS': 1}
```

Each dataset is **pinned to the version you passed in**. Writes made later, by your process or another one,
stay invisible until you call `refresh()`. That gives every query a stable snapshot.

!!! note "Snapshots are per dataset"

    Each dataset is pinned separately. The versions across tables are not a transactional multi-table
    snapshot; if a writer updates `Person` and `KNOWS` one after the other, refresh after both commits.

!!! warning "Use pylance 13.x to write"

    The engine is built against Lance 13. Datasets written by an older or newer major version of `pylance`
    may use a file format it can't read. Installing `lance-csr` pins a compatible `pylance` for you.

## `refresh()` { #refresh }

`refresh()` moves every Lance dataset to its latest version:

```python
lance.write_dataset(
    pa.table({"id": [4], "name": ["David"]}), "person.lance", mode="append"
)
lance.write_dataset(pa.table({"src": [3], "dst": [4]}), "knows.lance", mode="append")

print(engine.execute("MATCH (p:Person) RETURN count(*) AS n").to_pylist())  # [{'n': 3}]
engine.refresh()
print(engine.execute("MATCH (p:Person) RETURN count(*) AS n").to_pylist())  # [{'n': 4}]
print(engine.dataset_versions())  # {'Person': 2, 'KNOWS': 2}
```

What `refresh()` guarantees:

- **Unchanged datasets keep their handle.** A dataset whose version and commit time are unchanged is reused,
  along with its CSR, node index and statistics, so a no-op refresh is cheap.
- **Schema changes are picked up.** New columns are queryable after the refresh.
- **All or nothing.** The replacement is validated in full before it's swapped in. On error, nothing changes
  and the previous snapshot keeps working.
- **Concurrent refreshes cannot overwrite a newer published snapshot with an older candidate.** If another
  refresh publishes first, the overlapping refresh retries from that snapshot and reopens the latest datasets.
  The retry can add work when refreshes overlap; it does not block queries while the replacement is built.
- **Running queries are unaffected.** Queries and `execute_batches()` readers that started before the refresh
  finish on their original snapshot.
- **The CSR updates incrementally**, as described in [Keeping the CSR current](csr.md#keeping-the-csr-current).
- **PyArrow inputs don't change.** To replace in-memory data, build a new engine.

A refresh reopens each dataset by its URI with the same `storage_options` it was opened with, so object-store
credentials carry over.

## Repeated queries

`execute()` and `execute_batches()` keep up to 128 query templates per engine snapshot. Reuse the exact
query text with `$name` parameters to reuse parsing, semantic analysis, graph logical planning and
DataFusion logical planning. Every execution binds its own values before optimization and physical planning;
rows and results are never cached. Query text and parameter names/types identify each template, with nulls
separate from non-null values. Least recently used templates are evicted at the limit.

Lists, maps and parameterized `SKIP`/`LIMIT` counts use ordinary planning because their shape or value can
change the plan. Queries whose operators cannot produce a reusable template fall back to ordinary planning. `explain()` plans the current values directly. A successful `refresh()` starts an empty cache for the
new snapshot, including a refresh whose datasets did not change; active readers retain their existing plans.
These rules apply with and without CSR. They do not promise a speedup for a particular workload.

## Streaming large results

`execute()` returns one table, held in memory. For big outputs, stream:

```python
total = 0
for batch in engine.execute_batches("MATCH (p:Person) RETURN p.id AS id, p.name AS name"):
    total += batch.num_rows  # a pyarrow.RecordBatch at a time
print(total)  # 4
```

Combined with lazy Lance reads, a scan-filter-project query streams from disk to your code with bounded
memory. Joins, sorts and aggregations may still buffer their inputs.

## Memory: what's held where

| Data | Where it lives |
| --- | --- |
| Lance dataset rows | On disk; read per query, only the needed columns and rows |
| PyArrow inputs | In memory for the engine's lifetime, sliced across cores without copying |
| CSR adjacency (`use_csr=True`) | In memory, about 70 bytes per edge for both directions |
| Node index (`use_csr=True`) | In memory, one entry per node row |
| Join-key statistics | In memory, a few numbers per key column |
| Query results | In memory with `execute()`; streamed with `execute_batches()` |

The CSR and node index are built from the Lance data once and updated on `refresh()`; queries don't rescan
the edge table to traverse it.

## Tables created with LanceDB

A LanceDB table is a Lance dataset on disk. Pass the dataset behind it with `table.to_lance()`:

<!-- skip-check -->
```python
import lancedb
from lance_csr import CypherEngine, GraphConfig

db = lancedb.connect("./mydb")
people, knows = db.open_table("Person"), db.open_table("KNOWS")

config = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .rel_table("KNOWS", "Person", "Person", "src", "dst")
    .build()
)
engine = CypherEngine(config, {"Person": people.to_lance(), "KNOWS": knows.to_lance()})

knows.add(new_edges)  # keep writing with the LanceDB API
engine.refresh()      # the engine sees the new version
```

- Reads are lazy and version-pinned exactly as with `lance.dataset(...)`.
- Passing the LanceDB `Table` object itself raises a `TypeError`. `table.to_arrow()` works, but loads the
  whole table into memory and never refreshes.
- Tested with LanceDB 0.40.0 on local tables (Lance file format 2.2). A LanceDB release bundling a newer Lance
  major may write files this engine can't read.

!!! failure "Not supported"

    - **LanceDB Cloud**: remote tables have no `to_lance()`.
    - **Tables opened through a namespace connection**, whose credentials are vended at runtime. The engine
      can only reopen a dataset by URI and storage options, so it raises `ValueError` rather than read with
      the wrong identity.
    - Object-store tables (S3 and similar) have not been tested.

## Directory namespaces

`DirNamespace` resolves table names to Lance datasets in one directory, at `<base_uri>/<name>.lance`. Use it
with `CypherQuery` instead of building a `datasets` dict:

```python
from lance_csr import CypherQuery, DirNamespace

namespace = DirNamespace("graph")
lance.write_dataset(pa.table({"id": [1, 2], "name": ["Ann", "Ben"]}), "graph/Person.lance")
lance.write_dataset(pa.table({"src": [1], "dst": [2]}), "graph/KNOWS.lance")

result = (
    CypherQuery("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b")
    .with_config(config)
    .execute_with_namespace(namespace)
)
print(result.to_pylist())  # [{'a': 'Ann', 'b': 'Ben'}]
print(namespace.base_uri)  # graph
```

`CypherQuery` (with a namespace or a dict) reads the data for each `execute()`; use `CypherEngine` for lazy
reads, refresh and the CSR.

Rich path queries keep a fresh inventory for each execution and bypass scalar template caching. UNION branches share that query-local inventory; refreshing an engine does not change an already-open reader.
