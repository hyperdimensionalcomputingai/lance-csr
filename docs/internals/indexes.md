# Indexes and updates

The [custom operators](operators.md) read two in-memory indexes. Both are built from the same underlying Lance dataset and point back into
it, so neither holds a copy of node or relationship properties. [The CSR index](../csr.md) covers what users see:
how to turn it on, `csr_stats()`, and when to call `checkout_latest()` and `optimize_csr()`. This page covers how the indexes
are laid out and maintained.

The compact store of offsets and neighbours came from [lance-graph](https://github.com/lance-format/lance-graph),
whose query planner didn't use it. lance-csr added the edge rows, the delta and tombstones, the node index and
incremental updates, and wired the store into query execution through the [custom operators](operators.md).

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

The edge row is what lets a hop fetch relationship properties with `take`, and what lets `checkout_latest()` find deleted edges
without reading the old dataset version. Edges with a null endpoint are skipped: they could never satisfy the equality
join the CSR replaces.

### Packed edge rows

Unpacked, the neighbour and the edge row take 16 bytes per edge in each direction. Edge rows are stored packed, by
`PackedU64`: in blocks of 128 values, a block keeps its minimum and a bit width, and its values as bit-packed offsets
from that minimum. Neighbouring slots mostly come from nearby rows of the same fragment, so a block needs few bits; on
GDC SF1 edge rows take about 2 bytes per edge in each direction instead of 8, which cut the CSR's arrays from 613 to
419 MiB. Any position stays directly readable,
which traversal needs to resume part-way through a list, and reading a range loads each block's header once. Plain
hops never unpack edge rows; only hops that name their relationship, and deletes applied by `checkout_latest()`, do.

Neighbours stay a plain `u64` array. Packing them the same way roughly halved them on SF1 (their keys are sparse:
Comment keys span 2.2 trillion for 2 million rows, so a single width for the whole array, as a `u32` offset, wouldn't
fit at all), but every hop reads neighbours, and lookup-heavy queries got slower: custom Q30, which makes one lookup per comment
into lists of one neighbour, by about 22%. Each lookup read a block base, a header and a word instead of one value.

## The node index

`NodeRows` maps each key of one node label to its Lance row id, or to its row index for an in-memory table. It is built
by scanning only the id column. A label whose ids aren't integers, or aren't unique, gets no node index: a duplicate id
matches several rows in a join, which a single position can't reproduce. The node index depends only on its node
table, so edge commits update only the adjacency and node commits rebuild only the node index.

## How `checkout_latest()` diffs Lance versions { #how-checkout-latest-diffs-lance-versions }

`checkout_latest()` compares the pinned and latest versions of each relationship dataset fragment by fragment, from their
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

The base CSR is immutable and shared between snapshots through an `Arc`. `checkout_latest()` applies changes as a delta on top
of it: appended edges per source key, and tombstones stored as sorted offsets into each source's neighbours. Offsets
let readers skip deleted occurrences without copying deletion state for each query. Identical edges stay distinct
occurrences.

Each `checkout_latest()` call copies the whole delta, never the base. Snapshots must stay immutable because running queries keep using
the previous one, so the delta can't be mutated in place. The copy is bounded because the engine rebuilds once the
delta passes 25% of the base. Measured at 10 million edges with `python/benchmarks/csr_delta_accumulated.py` (twenty
appends of 1% each), a call grows from about 11 ms to about 80 ms as the delta reaches 20%, against 1.2 to 1.6 s
for a full rebuild.

??? note "Alternatives that were tried and rejected"

    - **Shared, immutable delta layers merged like a binary counter.** The median call fell from 74 to 32 ms, but merge
      cascades spiked to 298 ms, lookups got 5 to 25% slower, and the code was about 60 lines larger.
    - **A persistent hash map (`imbl`).** No spikes and one lookup, but about five new crates and `unsafe` code to
      trust.

    Revisit this if calls to `checkout_latest()` become frequent on graphs far larger than 10 million edges, or if the rebuild threshold
    is raised.

The base and delta map onto Lance's own index design: the delta plays the part of Lance's unindexed rows, and
`optimize_csr()` the part of `optimize_indices()`. [Rebuilding the CSR with `optimize_csr()`](../csr.md#rebuilding-the-csr) has the
full comparison.

## Stored CSR files

With `persist_csr=True` (see [Storing the CSR](../csr.md#storing-the-csr)), `csr_store.rs` writes the base of both
adjacencies after every full build of a Lance relationship dataset, to `_lance_csr/<version>.arrow` at the dataset's
root. The file holds the CSR exactly as it sits in memory, so a load neither builds nor packs anything. It's an
Arrow IPC file with one `UInt64` column, `values`, and twelve record batches of different lengths, one array each,
six per direction (outgoing first):

| Batch | Holds |
| --- | --- |
| 1 | Source keys (`i64` bit-cast) in CSR position order |
| 2 | Offsets |
| 3 | Neighbours |
| 4 to 6 | Packed edge rows: block bases, block headers (start bit and width), bit-packed words |

The arrays only make sense to `csr_store.rs`, which validates every one before use; reading the file with pyarrow gives
the raw parts, not neighbour lists. The schema metadata records the format, the dataset version, the endpoint column
names, their Lance field ids and types, and the fragments the base was built from. The delta is never stored.

When an engine starts, it reads the newest file at or before the pinned version. It then diffs that file's fragments against the pinned version, as `checkout_latest()` diffs two versions (see
[How `checkout_latest()` diffs Lance versions](#how-checkout-latest-diffs-lance-versions)), so the file's own version doesn't need to
exist any more. An empty diff uses the file as it is; otherwise the changes become the delta, under the same 25%
threshold. Rebuilds after start-up (from `checkout_latest()` or `optimize_csr()`) don't look for a file, since the stored base
is the one the engine already holds, but they do store the new one.

Every batch is compressed with zstd, Arrow IPC's own buffer compression, on top of the packing. A load decompresses
the arrays, checks them, and rebuilds only the source-key map, one insert per source. That work is CPU-bound, so
each load decodes on Tokio's blocking pool and relationship types load concurrently, up to one per core, while the
node indexes scan on the engine's task. Deltas and full builds gain nothing from overlapping there, so they still run
one type at a time, and each `build_ms` measures one build. Writes go through the dataset's object store with `put_if_absent`, which
publishes the whole file at once on local disk and object storage; the compressed file is encoded in memory first.
