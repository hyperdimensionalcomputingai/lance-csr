# Indexes and refresh

The [custom operators](operators.md) read two in-memory indexes. Both are built from the same underlying Lance dataset and point back into
it, so neither holds a copy of node or relationship properties. [The CSR index](../csr.md) covers what users see:
how to turn it on, `csr_stats()`, and when to call `refresh()` and `optimize_csr()`. This page covers how the indexes
are laid out and maintained.

The compact store of offsets and neighbours came from [lance-graph](https://github.com/lance-format/lance-graph),
whose query planner didn't use it. lance-csr added the edge rows, the delta and tombstones, the node index and
incremental refresh, and wired the store into query execution through the [custom operators](operators.md).

```text
                    Lance datasets (pinned version)
         ┌───────────────────────────┬───────────────────────────┐
         │  KNOWS (src, dst, since)  │  Person (id, name, age)   │
         └─────────────┬─────────────┴─────────────┬─────────────┘
     scan src, dst,    │                           │   scan id + row id
     _rowaddr          ▼                           ▼
 ┌─────────────────────────────────────┐   ┌───────────────────────────┐
 │ Adjacency (per type, per direction) │   │ NodeRows (per label)      │
 │   offsets    [0, 2, 3, 4]           │   │   id -> Lance row id      │
 │   neighbours [2, 3, 4, 4]  raw keys │   │   (unique integer ids)    │
 │   edge rows  [row addresses]        │   └─────────────┬─────────────┘
 │   + delta: appended edges,          │                 │
 │            tombstones               │                 │
 └──────────────────┬──────────────────┘                 │
                    │ neighbours(a.id)                   │ take_rows(positions, columns)
                    └─────────────► CsrExpandExec ◄──────┘
```

## The adjacency

`Adjacency` (in `csr_expand.rs`, over the store in `csr_index.rs`) holds one relationship type in one direction;
each type has an outgoing and an incoming one. It is built from the relationship table alone:

- source endpoint values map to dense CSR vertex positions through a hash map;
- neighbours store the raw target endpoint values, not node-table positions, so no node-table scan is needed and
  node-table writes never invalidate the adjacency;
- each neighbour also records the row its edge came from: its Lance row address (fragment id in the high 32 bits,
  offset in the low 32), or its row number for in-memory input.

That makes 16 bytes per edge in each direction. The edge row is what lets a hop fetch relationship properties with
`take`, and what lets a refresh find deleted edges without reading the old dataset version. Edges with a null
endpoint are skipped: they could never satisfy the equality join the CSR replaces.

## The node index

`NodeRows` maps each key of one node label to its Lance row id, or to its row index for an in-memory table. It is built
by scanning only the id column. A label whose ids aren't integers, or aren't unique, gets no node index: a duplicate id
matches several rows in a join, which a single position can't reproduce. The node index depends only on its node
table, so edge commits update only the adjacency and node commits rebuild only the node index.

## How a refresh diffs Lance versions

`refresh()` compares the pinned and latest versions of each relationship dataset fragment by fragment, from their
manifests:

| Fragment | Result |
| --- | --- |
| Only in the new version | Its rows are added |
| Only in the old version | Its rows are removed |
| Endpoint data files or overlays differ (a rewrite or an in-place column update) | Its old rows are removed and its new rows added |
| Only the deletion file differs | The rows in the new deletion vector are removed |
| Endpoint data and deletions unchanged | Skipped, so schema changes to other columns cost nothing |

Removals are row addresses, matched against the edge rows the adjacency already stores, so nothing is read from the
old version beyond its manifest. That is why compaction and `cleanup_old_versions()` don't force a rebuild. If an
endpoint column was dropped, re-created or retyped, the diff isn't meaningful and the adjacency is rebuilt.
[Keeping the CSR current](../csr.md#keeping-the-csr-current) shows the full decision, including the 25% rebuild
threshold.

## The delta

The base CSR is immutable and shared between snapshots through an `Arc`. A refresh applies changes as a delta on top
of it: appended edges per source key, and tombstones stored as sorted offsets into each source's neighbours. Offsets
let readers skip deleted occurrences without copying deletion state for each query. Identical edges stay distinct
occurrences.

Each refresh copies the whole delta, never the base. Snapshots must stay immutable because running queries keep using
the previous one, so the delta can't be mutated in place. The copy is bounded because the engine rebuilds once the
delta passes 25% of the base. Measured at 10 million edges with `python/benchmarks/csr_delta_accumulated.py` (twenty
appends of 1% each), a refresh grows from about 11 ms to about 80 ms as the delta reaches 20%, against 1.2 to 1.6 s
for a full rebuild.

??? note "Alternatives that were tried and rejected"

    - **Shared, immutable delta layers merged like a binary counter.** Median refresh fell from 74 to 32 ms, but merge
      cascades spiked to 298 ms, lookups got 5 to 25% slower, and the code was about 60 lines larger.
    - **A persistent hash map (`imbl`).** No spikes and one lookup, but about five new crates and `unsafe` code to
      trust.

    Revisit this if refreshes become frequent on graphs far larger than 10 million edges, or if the rebuild threshold
    is raised.

The base and delta map onto Lance's own index design: the delta plays the part of Lance's unindexed rows, and
`optimize_csr()` the part of `optimize_indices()`. [Folding the delta in](../csr.md#folding-the-delta-in) has the
full comparison.

## Stored CSR files

With `persist_csr=True` (see [Storing the CSR](../csr.md#storing-the-csr)), `csr_store.rs` writes the base of both
adjacencies after every full build of a Lance relationship dataset, to `_lance_csr/<version>.arrow` at the dataset's
root. It's one Arrow IPC file holding one record batch, one row per edge in CSR position order:

| Column | Type | Holds |
| --- | --- | --- |
| `out_source`, `in_source` | run-end encoded `Int64` | Source keys; the run ends are the CSR offsets |
| `out_neighbor`, `in_neighbor` | `UInt64` | Raw target keys |
| `out_edge_row`, `in_edge_row` | `UInt64` | Edge row addresses |

The schema metadata records the format, the dataset version, the endpoint column names, their Lance field ids and
types, and the fragments the base was built from. The delta is never stored.

When an engine starts, it reads the newest file at or before the pinned version. It then diffs that file's fragments against the pinned version, as a refresh diffs two versions (see
[How a refresh diffs Lance versions](#how-a-refresh-diffs-lance-versions)), so the file's own version doesn't need to
exist any more. An empty diff uses the file as it is; otherwise the changes become the delta, under the same 25%
threshold. Rebuilds after start-up (from `refresh()` or `optimize_csr()`) don't look for a file, since the stored base
is the one the engine already holds, but they do store the new one.

The arrays are compressed with zstd, Arrow IPC's own buffer compression, which made the SF1 files 4.7 times smaller
(711 to 150 MiB). `CsrIndex` keeps its arrays as Arrow `ScalarBuffer`s, so a loaded file's arrays are decompressed
straight into the buffers the CSR uses; only the source-key map is rebuilt, one insert per source. Decompression is
single-threaded, about 0.4 s of a load at SF1. Writes go through the dataset's object store with `put_if_absent`, which
publishes the whole file at once on local disk and object storage; the compressed file is encoded in memory first.
