# Lance datasets

[Lance](https://lance.org/) is a columnar format for ML and multimodal data, with versioning,
fast random access and vector indexes. lance-csr can query Lance datasets on disk without loading them into
memory first.

lance-csr reads Lance with its own built-in copy of the Lance Rust library, so it doesn't depend on
[`pylance`](https://pypi.org/project/pylance/), Lance's Python package. You still need `pylance` to write datasets
and to open the `lance.dataset(...)` handles you pass in, so add it next to lance-csr:

```bash
uv add lance-csr pylance
```

Any `pylance` version works as long as it writes a file format that lance-csr's built-in Lance library can read.
lance-csr updates to new Lance releases as they come out (its
[`Cargo.toml`](https://github.com/hyperdimensionalcomputingai/lance-csr/blob/main/crates/lance-csr/Cargo.toml)
names the version), and Lance reads datasets written by older versions, so any `pylance` whose major version is
the same as or older than lance-csr's is safe. Opening a dataset written in a newer format version than the
built-in Lance library supports fails with an error.

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
stay invisible until you call `checkout_latest()`. That gives every query a stable snapshot.

!!! note "Snapshots are per dataset"

    Each dataset is pinned separately. The versions across tables are not a transactional multi-table
    snapshot; if a writer updates `Person` and `KNOWS` one after the other, call `checkout_latest()` after both commits.

## `checkout_latest()` { #checkout-latest }

`checkout_latest()` moves every Lance dataset to its latest version. It's named after Lance's own
`LanceDataset.checkout_latest()`, which does the same for a single dataset, and it updates everything the engine
built from that data along with it: node indexes, join statistics and, with `use_csr=True`, the CSR (see
[Keeping the CSR current](csr.md#keeping-the-csr-current) for how the CSR keeps up):

```python
lance.write_dataset(
    pa.table({"id": [4], "name": ["David"]}), "person.lance", mode="append"
)
lance.write_dataset(pa.table({"src": [3], "dst": [4]}), "knows.lance", mode="append")

print(engine.execute("MATCH (p:Person) RETURN count(*) AS n").to_pylist())  # [{'n': 3}]
engine.checkout_latest()
print(engine.execute("MATCH (p:Person) RETURN count(*) AS n").to_pylist())  # [{'n': 4}]
print(engine.dataset_versions())  # {'Person': 2, 'KNOWS': 2}
```

What `checkout_latest()` guarantees:

- **Unchanged datasets keep their handle.** A dataset whose version and commit time are unchanged is reused,
  along with its CSR, node index and statistics, so calling it when nothing changed is cheap.
- **Schema changes are picked up.** New columns are queryable afterwards.
- **All or nothing.** The replacement is validated in full before it's swapped in. On error, nothing changes
  and the previous snapshot keeps working.
- **Concurrent calls cannot overwrite a newer published snapshot with an older candidate.** If another call
  publishes first, the overlapping one retries from that snapshot and reopens the latest datasets. The retry
  can add work when calls overlap; it does not block queries while the replacement is built.
- **Running queries are unaffected.** Queries and `execute_batches()` readers that started before the call
  finish on their original snapshot.
- **The CSR updates incrementally**, as described in [Keeping the CSR current](csr.md#keeping-the-csr-current).
- **PyArrow inputs don't change.** To replace in-memory data, build a new engine.

`checkout_latest()` reopens each dataset by its URI with the same `storage_options` it was opened with, so object-store
credentials carry over.

## Streaming large results

`execute()` returns one table, held in memory. For big outputs, stream with
[`execute_batches()`](running-queries.md#streaming):

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
| CSR adjacency (`use_csr=True`) | In memory, in proportion to the number of edges, plus an index entry per source node; see [the CSR's memory cost](csr.md#turning-it-on) |
| Node index (`use_csr=True`) | In memory, one entry per node row |
| Join-key statistics | In memory, a few numbers per key column |
| Query results | In memory with `execute()`; streamed with `execute_batches()` |

The CSR and node index are built from the Lance data once and updated on `checkout_latest()`; queries don't rescan
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
engine.checkout_latest()      # the engine sees the new version
```

- Reads are lazy and version-pinned exactly as with `lance.dataset(...)`.
- Passing the LanceDB `Table` object itself raises a `TypeError`. `table.to_arrow()` works, but loads the
  whole table into memory and never sees later versions.
- Tested on local LanceDB tables.

!!! warning "LanceDB bundles its own Lance"

    Each LanceDB release bundles its own copy of Lance, often newer than lance-csr's built-in copy. If LanceDB
    uses a newer Lance major version, it can write files that lance-csr fails to open, and lance-csr writes
    ([`write_node_column`](graph-algorithms.md#writing-results-back-to-lance),
    [`optimize_datasets`](#compaction-and-old-versions)) commit with lance-csr's Lance version. Before upgrading
    LanceDB, compare its bundled Lance version with lance-csr's
    [`Cargo.toml`](https://github.com/hyperdimensionalcomputingai/lance-csr/blob/main/crates/lance-csr/Cargo.toml),
    and test major-version upgrades on a copy of your tables first.

!!! failure "Not supported"

    - **LanceDB Cloud**: remote tables have no `to_lance()`.
    - **Tables opened through a namespace connection**, whose credentials are vended at runtime. The engine
      can only reopen a dataset by URI and storage options, so it raises `ValueError` rather than read with
      the wrong identity.
    - Object-store tables (S3 and similar) have not been tested.

## Compaction and old versions

Every write to a Lance dataset makes a new version, whether it appends rows, deletes them or adds a column, as
[`write_node_column`](graph-algorithms.md#writing-results-back-to-lance) does. Lance keeps the files of older
versions, so you can still check them out and readers pinned to them, such as this engine, keep working. Over
many writes, two things build up:

- **Small fragments.** Each append writes its own fragment, and scans slow down as they multiply. *Compaction*
  rewrites them into fewer, larger fragments.
- **Old versions.** Data that a newer version replaced or dropped stays on disk until a *cleanup* deletes the
  versions that still use it. A column replaced with `write_node_column(..., overwrite=True)` is one example.

Neither happens on its own with local datasets, and doing them by hand around an engine is error-prone. A manual
cleanup can delete the version that the engine is pinned to, causing the next query to fail with a "Not found"
error. Likewise, a LanceDB `Table` handle opened before lance-csr writes to disk stays pinned to its older
version, so calling `optimize()` on it fails with a commit conflict.
[`optimize_datasets`][lance_csr.CypherEngine.optimize_datasets] handles maintenance through the engine instead:
for each Lance dataset, it opens the newest version, compacts it, advances the engine onto the result (as
`checkout_latest()` does), and only then deletes old versions:

```python
from datetime import timedelta

stats = engine.optimize_datasets(cleanup_older_than=timedelta(0))  # (1)!
print(stats["KNOWS"]["fragments_removed"], stats["KNOWS"]["fragments_added"])  # 2 1
print(len(lance.dataset("knows.lance").versions()))  # 1
print(engine.execute("MATCH (p:Person) RETURN count(*) AS n").to_pylist())  # [{'n': 4}]
```

1. Deletes every version but the latest. The default, `None`, keeps the last 7 days, as LanceDB's `optimize()`
   does.

For each Lance dataset, the returned dict reports how many fragments compaction replaced (`fragments_removed`) and
created (`fragments_added`), along with the old versions and bytes removed by cleanup (`old_versions`,
`bytes_removed`). It works the same for datasets written with `pylance` and for LanceDB tables, since it opens
each dataset by its location rather than through your table object. LanceDB suggests compacting after about
100,000 changed rows or 20 write operations.

Two things to keep in mind:

- **Other readers.** If cleanup deletes a version that a reader is pinned to, that reader fails, including queries
  still running on this engine or other processes reading the same datasets. Keep the window longer than any of
  them stays pinned; a window of zero is only safe when nothing else reads the datasets.
- **Lance indexes aren't updated.** LanceDB's `optimize()` also adds new rows to vector and scalar indexes. If you
  keep such indexes, run `lance.dataset(uri).optimize.optimize_indices()` (or LanceDB's `optimize()`) for them,
  then `engine.checkout_latest()`.

Compacting a relationship table doesn't force a [CSR](csr.md#keeping-the-csr-current) rebuild. Compaction is a write
made with lance-csr's built-in Lance, so with LanceDB tables, mind the
[Lance version LanceDB bundles](#tables-created-with-lancedb).

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
reads, `checkout_latest()` and the CSR.
