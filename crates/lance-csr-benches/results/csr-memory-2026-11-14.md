# CSR memory experiment for issue #79

Baseline: `cbad7b8914e4149aa0465ca522eec9250d5034c2` (`main` when the experiment started). The work is
uncommitted on `perf/csr-memory-79`.

In short: the index's allocated size on SF1 falls from 776.9 MiB to 433.3 MiB, resident memory after engine
start falls by 227 to 394 MiB on the two SF1 suites, one query that scans whole many-to-one relationships
runs in half the time, and no other query moves beyond run-to-run noise because of the new layout.

## Where the memory is

A diagnostic built each of the 23 SF1 relationship types in both directions from the local Lance datasets,
reading `src`, `dst` and `_rowaddr`: 17,256,038 edges and 11,361,156 source entries across 46 adjacencies.

| Storage, both directions | `main` (MiB) | Branch (MiB) |
| --- | ---: | ---: |
| Source index (map, plus offsets on `main`) | 444.7 | 92.6 |
| Neighbours | 263.3 | 263.3 |
| Packed edge rows | 68.9 | 77.4 |
| Total | 776.9 | 433.3 |

The total is 44.2% smaller. Map figures are allocator-requested bytes worked out from each map's capacity
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

The words layout needs sources numbered in key order, so a full build now sorts edges by source key instead of
numbering sources as they are first seen. That is why packed edge rows grew by 8.5 MiB: where a table was
already in source order, rows used to pack to 8 bits per edge and now take 11 to 12.

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
per suite and mode, wheels alternated, load average between 2.7 and 7.5. Three wheels were built: `main` at
`cbad7b8`, the branch, and a control that is the branch with the words layout switched off by one constant.
Branch against control isolates the words layout; control against `main` shows the ranges layout and the new
build order, plus whatever two builds differ by anyway. Every run passed its answer checks.
[Every query](csr-memory-79-suites.md) is listed separately.

| Suite | Mode | Queries | Branch vs `main` | Branch vs control | Control vs `main` |
| --- | --- | ---: | ---: | ---: | ---: |
| graph-benchmark-ldbc | memory | 30 | -1.1% | -1.7% | +0.7% |
| graph-benchmark-ldbc | lazy | 30 | -2.6% | -2.1% | -0.5% |
| graph-benchmark-ldbc-complex | memory | 14 | -1.8% | -0.4% | -1.3% |
| graph-benchmark-ldbc-complex | lazy | 14 | -2.4% | -2.5% | +0.1% |
| graph-benchmark | memory | 9 | -2.0% | +0.2% | -2.2% |

These are geometric means of the per-query change in mean latency. What stands behind them:

- **Custom query 30 runs in half the time:** 41.3 ms on `main`, 40.5 ms on the control and 19.3 ms on the
  branch in memory mode, and the same in lazy mode, in every round. It follows three many-to-one
  relationships (a comment's creator, the post it replies to, that post's creator) for every comment, so it
  does little but look up sources in the three largest directions the words layout applies to. It is this
  layout's best case and accounts for the whole of the first two rows' gain: without it, the 29 other custom
  queries average +1.5% against `main` in memory mode and -0.2% in lazy mode.
- **IC14 is 13% to 15% faster than `main`, but not because of the words layout:** 276 ms on `main`, 241 ms
  on the control, 236 ms on the branch. The control shares the gain, so it comes from the ranges layout, the
  build order, or build-to-build variance, which has reached 15% to 30% on single queries here before. It
  needs a further control to be credited to anything.
- **The sub-millisecond custom queries are 1% to 3% slower than `main` in memory mode,** at most 0.02 ms each
  (query 26: 0.73 to 0.75 ms). `main`'s own two rounds differ by up to 5.8% on the same queries, and lazy mode
  shows no such shift, so this is within noise.
- **IC12 in lazy mode (-9.9%) is noise:** `main`'s own two rounds differ by 13.6% on it.
- **The custom suite (`graph-benchmark`)** has no direction above 100,000 sources, so the branch and the
  control use the same layouts there and agree to 0.2%. Its queries 8 and 9 are 5% faster than `main` on both.

Resident memory of a process after it builds the engine, three processes per wheel (mean, with the range):

| Suite | Mode | `main` (MiB) | Control (MiB) | Branch (MiB) | Branch vs `main` |
| --- | --- | ---: | ---: | ---: | ---: |
| graph-benchmark-ldbc | memory | 2234 (2208–2253) | 2151 (2143–2161) | 1840 (1835–1844) | -394 (-17.6%) |
| graph-benchmark-ldbc | lazy | 1578 (1550–1610) | 1468 (1408–1511) | 1351 (1256–1436) | -227 (-14.4%) |
| graph-benchmark-ldbc-complex | memory | 2773 (2686–2851) | 2658 (2620–2732) | 2460 (2442–2470) | -313 (-11.3%) |
| graph-benchmark-ldbc-complex | lazy | 1569 (1539–1623) | 1435 (1382–1467) | 1340 (1285–1390) | -229 (-14.6%) |
| graph-benchmark | memory | 442 (441–442) | 437 (436–437) | 437 (436–438) | -4 (-1.0%) |

The lazy-mode readings vary by up to 180 MiB between processes of the same wheel, so read them as a fall of
roughly 200 to 250 MiB, less than the 344 MiB fall in allocated bytes. Peak memory during the build was not
measured separately.

## Known limits

- **Stored CSR files from 0.10 are not read.** Their source keys aren't in ascending order, so the stored
  format number was raised and the next full build replaces them.
- **Hot subsets.** The slowdown above is not removed by any threshold. Queries that fan in to popular nodes
  are where an engine regression would show.
- **Half-missing lookups.** At parity in the debug microbenchmarks on SF1 keys; an earlier optimised run of an
  earlier variant was 1.37 times slower. No suite query isolates this case.
- **Build cost and peak memory.** Not measured. The build no longer does a hash insert per edge, but sorts by key.
- **Large many-to-many directions** on other graphs get ranges only.
- **The custom suite** (`graph-benchmark`) has no direction above 100,000 sources, so it exercises ranges and
  the new build order, not words.

## Validation

On debug builds of the branch:

- `cargo test --workspace`: 716 passed, 0 failed, 11 ignored.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `make install`, `make test`: 1,722 Python tests passed. `make lint`: Ruff, formatting and Pyright passed.
- Strict Zensical build and `uvx typos@1.51.1 .`: passed.

The earlier cross-version persistence check no longer applies: the stored format number changed, so `main` and
the branch ignore each other's files.

On release wheels: all 30 custom queries, all 14 IC queries and all 9 `graph-benchmark` queries passed their
answer checks on `main`, the control and the branch, in every mode and round.

Physical plans were not compared between wheels. The planner's only input from the index, the number of
sources per direction, is the same in all three.

## Reproducing the ledger

```bash
CSR_MEMORY_DATA=/path/to/graph_lance cargo test -p lance-csr --lib sf1_memory_ledger -- --ignored --nocapture
```
