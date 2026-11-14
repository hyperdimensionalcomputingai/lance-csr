# CSR memory experiment for issue #79

Baseline: `cbad7b8914e4149aa0465ca522eec9250d5034c2` (`main` when the experiment started). The work is on
`perf/csr-memory-79`.

In short: the index's allocated size on SF1 falls from 776.9 MiB to 417.1 MiB, resident memory after engine
start falls by 242 to 410 MiB on the two SF1 suites, one query that scans whole many-to-one relationships
runs in half the time, and no other suite query moves beyond run-to-run noise because of the new layout. On
four synthetic graphs of shapes the suites lack, the largest slowdown against `main` is 5.3%, on a 0.9 ms
query whose own runs differ by 6%.

## Where the memory is

A diagnostic built each of the 23 SF1 relationship types in both directions from the local Lance datasets,
reading `src`, `dst` and `_rowaddr`: 17,256,038 edges and 11,361,156 source entries across 46 adjacencies.

| Storage, both directions | `main` (MiB) | Branch (MiB) |
| --- | ---: | ---: |
| Source index (map, plus offsets on `main`) | 444.7 | 92.6 |
| Neighbours | 263.3 | 263.3 |
| Packed edge rows | 68.9 | 61.2 |
| Total | 776.9 | 417.1 |

The total is 46.3% smaller. Map figures are allocator-requested bytes worked out from each map's capacity
(17 bytes per bucket for a per-source entry, 25 for a 64-key word). They exclude allocator rounding, node
indexes, tables, deltas and query working memory. This is not process memory.

Per direction: [`main`](csr-memory-79-baseline.csv), [branch](csr-memory-79-branch.csv).

## What changed

Each adjacency maps a source key to a range of the neighbour array. On `main` that is a hash map from key to
vertex position plus an offsets array. The branch picks one of three layouts per direction; see
`docs/internals/indexes.md#source-layouts`.

1. **Ranges.** The map value holds the start and end of the range, so the offsets array is dropped. This is the
   default, and it accounts for 86.7 MiB of the saving.
2. **Words.** One map entry per run of 64 consecutive key values that contains a source: a bitmap of which keys
   exist and the position of the first. Used only when every source has exactly one edge, the direction has at
   least a million sources, and there are at least four sources per occupied run of 64. Seven SF1 directions
   qualify, and they shrink from 375.9 MiB to 40.6 MiB.
3. **Wide.** The `main` layout, kept for directions with more than `u32::MAX` edges.

Sources are numbered in the order the edge table first lists them, as on `main`. The words layout needs key
order, so the directions that use it are numbered by key instead. The first version of this work numbered
every direction by key, and that made one kind of scan 28% slower; see
[Source numbering](#source-numbering).

The build no longer does a hash insert per edge. It sorts the edges by source key and row, then orders the
sources by their first row. Each source's edges therefore sit in table order, and the packed edge rows shrink
from 68.9 to 61.2 MiB.

## Why the rule is this narrow

The thresholds were first seen on SF1 keys, then checked on synthetic keys that SF1 doesn't have, so that the
rule isn't fitted to one dataset. Both microbenchmarks are debug builds; figures are ratios to the per-source
hash map, and only the direction of each result should be relied on.

On real SF1 keys (two runs):

- Every key set up to 440,000 sources was 1.1 to 1.4 times slower with words. The per-source map still fits in
  the CPU cache at that size. This is why small directions keep ranges.
- From a million sources, words were 0.5 to 0.85 times the lookup time when every source has one edge.
- A hop that touches a small hot subset of a large map (a person's liked comments: 63,000 distinct keys of
  2.05 million) was 1.05 to 1.3 times slower with words.

On [synthetic keys](csr-memory-79-shape-sweep.csv) at 250,000, 1 million and 4 million sources (two runs),
for the directions the rule would switch:

| Case | Lookup time | Size |
| --- | --- | --- |
| One edge per source, four or more sources per run of 64 | Mostly 0.4 to 1.0; worst 1.15 | 0.02 to 0.37 |
| Several edges per source, four or more sources per run of 64 | 0.43 to 1.6 | 0.14 to 0.48 |
| Random ids | Mostly 1.1 to 1.65 | 1.5 to 1.6 |

Several edges per source needs an offset per source, and that second read lost on skewed and half-missing
lookups. SF1 has no large direction of that shape, so the SF1 runs could not have shown it. The layout was
therefore restricted to one edge per source, and the offsets were removed from it.

## Suite results

Release wheels, on one machine (Apple M5, 24 GB, macOS 26.6.2, AC power), following `CONTRIBUTING.md`: two rounds per wheel
per suite and mode, wheels alternated, load average between 2.5 and 6.5. Three wheels were built: `main` at
`cbad7b8`, the branch, and a control that is the branch with the words layout switched off by one constant.
Branch against control isolates the words layout; control against `main` shows the ranges layout and the new
build, plus whatever two builds differ by anyway. Every run passed its answer checks.
[Every query](csr-memory-79-suites.md) is listed separately.

| Suite | Mode | Queries | Branch vs `main` | Branch vs control | Control vs `main` |
| --- | --- | ---: | ---: | ---: | ---: |
| graph-benchmark-ldbc | memory | 30 | -1.6% | -2.1% | +0.5% |
| graph-benchmark-ldbc | lazy | 30 | -2.9% | -2.0% | -0.9% |
| graph-benchmark-ldbc-complex | memory | 14 | -2.0% | +0.1% | -2.1% |
| graph-benchmark-ldbc-complex | lazy | 14 | -1.3% | +0.3% | -1.6% |
| graph-benchmark | memory | 9 | -1.1% | +0.3% | -1.3% |

These are geometric means of the per-query change in mean latency. What stands behind them:

- **Custom query 30 runs in half the time:** 40.6 ms on `main`, 40.0 ms on the control and 19.0 ms on the
  branch in memory mode, and 41.1, 40.2 and 19.5 ms in lazy mode. It follows three many-to-one
  relationships (a comment's creator, the post it replies to, that post's creator) for every comment, so it
  does little but look up sources in the three largest directions the words layout applies to. It is this
  layout's best case and accounts for the whole of the first two rows' gain: without it, the 29 other custom
  queries average +0.8% against `main` in memory mode and -0.4% in lazy mode.
- **IC14 is 10% to 11% faster than `main`, and the gain is not claimed:** 273 ms on `main`, 244 ms on the
  control, 242 ms on the branch in memory mode. The control shares it, so it is not the words layout, and
  this query has moved by up to 39% between builds whose indexes hold the same contents; see
  [IC14](#ic14).
- **The sub-millisecond custom queries are up to 2.8% slower than `main` in memory mode,** at most 0.02 ms
  each (query 26: 0.72 to 0.74 ms). `main`'s own two rounds differ by 1% to 7% on the same queries, and lazy
  mode shows no such shift, so this is within noise.
- **The custom suite (`graph-benchmark`)** has no direction above 100,000 sources, so the branch and the
  control use the same layouts there and agree to 0.3%.

Resident memory of a process after it builds the engine, three processes per wheel (mean, with the range):

| Suite | Mode | `main` (MiB) | Control (MiB) | Branch (MiB) | Branch vs `main` |
| --- | --- | ---: | ---: | ---: | ---: |
| graph-benchmark-ldbc | memory | 2269 (2253–2286) | 2154 (2144–2162) | 1858 (1832–1875) | -410 (-18.1%) |
| graph-benchmark-ldbc | lazy | 1612 (1581–1644) | 1459 (1387–1531) | 1370 (1327–1392) | -242 (-15.0%) |
| graph-benchmark-ldbc-complex | memory | 2759 (2712–2819) | 2592 (2541–2690) | 2431 (2407–2465) | -328 (-11.9%) |
| graph-benchmark-ldbc-complex | lazy | 1537 (1478–1603) | 1374 (1324–1420) | 1254 (1237–1265) | -283 (-18.4%) |
| graph-benchmark | memory | 440 (437–442) | 439 (438–439) | 439 (438–440) | -1 (-0.2%) |

Readings vary by up to 150 MiB between processes of the same wheel, so read the falls as roughly 250 to 400
MiB, against a 360 MiB fall in allocated bytes.

**Build cost on SF1.** Building the custom suite's engine takes 1.20 s on the branch against 1.56 s on `main`
in memory mode, and 1.20 s against 1.57 s in lazy mode (means of three processes, which agree to 2%). Peak
resident memory of that process is lower on the branch in both modes (2,430 against 2,683 MiB, and 1,357
against 1,546 MiB), so the sort does not raise the peak.

**Physical plans.** All 53 queries of the three suites get the same plan on `main`, the control and the
branch, in both modes. The one difference is the order of two filters on `Place` in IC3, which also changes
between two runs of the same `main` wheel.

## Shapes no suite has

Four synthetic graphs, loaded as Arrow tables, three processes per wheel, every answer equal across wheels.
The first three have 4 million children with one edge each to 200,000 parents, and 4 million edges from
100,000 persons to children. [Every figure](csr-memory-79-followup.md) is listed separately. Change in
median latency of the branch:

| Graph | Query | vs `main` | vs control |
| --- | --- | ---: | ---: |
| Dense ids, tables in id order (words) | two hops through all 4M sources, uniform | -43.8% | -30.2% |
| | two hops through a 3% hot subset of them | -45.5% | -30.2% |
| | two hops from 1% of persons | -44.0% | -26.4% |
| | two hops where half the lookups find nothing | -32.1% | -19.9% |
| | the whole relationship | -68.7% | -67.2% |
| | reverse direction, 1% of parents | -3.0% | -0.9% |
| | one source | +1.6% | +2.0% |
| Dense ids, tables in random order (words) | two hops, uniform | -43.3% | -30.7% |
| | two hops, hot subset | -45.9% | -30.7% |
| | two hops from 1% of persons | -48.0% | -31.9% |
| | the whole relationship | -31.4% | -28.4% |
| | reverse direction, 1% of parents | +5.3% | +2.7% |
| | one source | -0.8% | -0.7% |
| Random 45-bit ids, tables in random order (ranges) | two hops, uniform | -19.5% | -0.3% |
| | two hops, hot subset | -20.3% | -0.0% |
| | two hops from 1% of persons | -24.5% | +0.1% |
| | the whole relationship | -4.0% | +0.9% |
| | reverse direction, 1% of parents | -8.8% | -9.2% |
| | one source | +0.7% | -0.2% |
| 2M nodes, 20M edges, many-to-many (ranges) | one hop from 1% of nodes | -11.6% | +3.4% |
| | two hops | -11.9% | +3.9% |
| | three hops | -5.3% | +2.5% |
| | the whole relationship | -12.1% | +1.4% |
| | one source | +3.2% | +0.1% |

- The hot-subset and half-missing slowdowns seen in the microbenchmarks do not appear end to end at this size.
- With random ids the rule falls back to ranges, as intended: the branch and the control agree to 1%, apart
  from one 0.9 ms query.
- The two figures above +3% against `main` are on queries of 0.6 and 0.9 ms whose three processes differ by
  9% and 6%.
- The branch is 2.5% to 3.9% slower than the control on the many-to-many hops, although the two build the
  same index for that graph. Their processes differ by 2.6% to 4.5%, so this is at the edge of noise.
- CSR build time falls by 22% to 42% on all four graphs, and peak memory is never higher than on `main`.

### Source numbering

The first version of this work numbered every direction's sources by key. On the random-id graph, a scan of
the whole relationship was then 27.7% slower than `main` (57.7 against 45.2 ms), on the control as well, so
the numbering caused it and not either layout. In that graph the node table and the edge table list the
children in the same, random order. `main` numbers sources in the order the edge table lists them, so a scan
of the node table reads the neighbours in sequence; numbered by key, the same scan jumps around the neighbour
array. A wheel with the ranges layout and `main`'s numbering confirmed it: 43.6 ms.

SF1 hid this. The four large many-to-one edge tables checked follow their node tables exactly, and the
comment and post tables are 97% and 90% in id order, so the three orders nearly coincide there.

The branch now numbers sources in table order, and that scan is 4.0% faster than `main`. Words directions
keep key order. The second graph in the table above covers the case where that could cost: dense ids whose
tables are in random order. Its whole-relationship scan is still 31% faster than `main`, though less than the
69% of the graph whose tables are in id order.

The change gave up some speed on the many-to-many graph, whose node table is in id order and whose edge table
is random: one hop from 1% of nodes was 33% faster than `main` when numbered by key and is 12% faster now.
Numbering sources by the node table's row order would suit both cases. It was not tried.

### IC14

IC14 follows every person's comments to the message each replies to and on to that message's creator. Mean
latency in memory mode, each wheel within 1.5% of itself across rounds:

| Wheel | Layout | Numbering | IC14 (ms) |
| --- | --- | --- | ---: |
| `main` | map and offsets | table order | 270 to 273 |
| First version of the control | ranges | key order | 235 |
| A second control built from it | ranges | table order | 339 |
| Control | ranges | table order | 244 |
| Branch | ranges and words | table order, key order for words | 242 |

The second control and the final control are written to build the same index: the same layout, the same
numbering and the same order of edges within a source. Their indexes were not compared byte for byte. They differ by 39% on this query and by no more than a few percent on
any other IC query except IC12 (6% to 10%). IC14 is therefore sensitive to something in a build other than
the index, and its 10% to 11% gain on the branch is not claimed.

## Known limits

- **Stored CSR files from 0.10 are not read.** A load could never choose the words layout for them, so the
  stored format number was raised and the next full build replaces them.
- **Table order is not always scan order.** Numbering follows the edge table, which helps a scan only when
  the node table is in the same order. Numbering by the node table's row order was not tried.
- **IC14** moves by up to 39% between builds for a reason not pinned down.
- **Synthetic graphs are uniform.** They have no skewed degrees and no string properties. Timestamp-style ids
  were not run end to end; the rule treats them as it treats random ids.
- **Words between 1M and 4M sources** were measured end to end only on SF1, and otherwise in the debug
  microbenchmark sweep.
- **The custom suite** (`graph-benchmark`) has no direction above 100,000 sources, so it exercises ranges and
  the new build, not words.

## Validation

On debug builds of the branch:

- `cargo test --workspace`: 717 passed, 0 failed, 11 ignored.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `make install`, `make test`: 1,722 Python tests passed. `make lint`: Ruff, formatting and Pyright passed.
- Strict Zensical build and `uvx typos@1.51.1 .`: passed.

On release wheels: all 30 custom queries, all 14 IC queries and all 9 `graph-benchmark` queries passed their
answer checks on `main`, the control and the branch, in every mode and round.

## Reproducing the ledger

```bash
CSR_MEMORY_DATA=/path/to/graph_lance cargo test -p lance-csr --lib sf1_memory_ledger -- --ignored --nocapture
```
