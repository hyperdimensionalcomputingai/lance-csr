# Multi-source BFS experiment (#76)

Base: `c4ab747a62c79291820c181c5f5043983e232dcd` (latest main when fetched on November 13, 2026).
This is an experiment behind the `experimental-msbfs` feature. Normal builds are unchanged, and no planner or
query API selects the prototype; only the `cypher` run switches it on, through a hidden process-wide toggle.

## Reproduce

```bash
mbx build -p lance-csr-benches --example multi_source_bfs --features experimental-msbfs --release --locked
./target/release/examples/multi_source_bfs check
python3 crates/lance-csr-benches/experiments/multi_source_bfs/run.py ./target/release/examples/multi_source_bfs /tmp/msbfs-results
./target/release/examples/multi_source_bfs cypher > /tmp/msbfs-results/cypher.csv
```

The runner uses the Python standard library and macOS `/usr/bin/time -l`. Keep the machine otherwise idle.
The executable can run on other platforms; the RSS wrapper is macOS-specific.

## What is compared

- `current`: the actual existing search functions: bidirectional for one target, forward for several targets. This models execution with reverse adjacency available.
- `forward`: the actual existing forward search, to isolate batching from bidirectional search selection; it also models the single-target fallback when reverse adjacency is unavailable.
- `batched`: up to 64 independent sources sharing neighbour reads, with sparse raw-key bit masks. Each source
  retains its own depth map and every shortest-path predecessor, including parallel-edge multiplicity.
- `masked`: a control for `batched`. Traversal keeps one lane mask per vertex and one `(predecessor, lane mask)`
  record per discovering edge, shared by all sources; per-source maps are built afterwards, only for vertices on a
  shortest path to a requested target.
- `parallel`: the existing selection with up to eight independent source tasks on an eight-worker Tokio runtime.

All paths use the same real `Adjacency`/`Neighbors` implementation and production path reconstruction routine.
Normal builds compile out the experimental module and profiling hook. The opt-in hook checks whether counting
is enabled once per expanded vertex; this small overhead is present in baseline and batched measurements.
There is no production eligibility rule, memory reservation or new user-facing knob.

The potential integration point is `shortest_path.rs::search_batch`, after its existing grouping of targets
by source. The current operator already searches repeated source bindings once per input batch; this
experiment asks whether different sources should share expansion work. Normal selection remains in that
function, with bidirectional search only when reverse adjacency is available and a source has one target.

## End-to-end run

`multi_source_bfs cypher` runs the same workloads as Cypher through `CypherEngine` with a CSR, in memory. A `WANTS`
relationship binds each (source, target) request, and the query returns both endpoints, `length(p)` and `nodes(p)`,
so parsing, planning, the join, path reconstruction and list output are inside the timing. Three selections
alternate within one process (one warm-up and seven timed executions each) and must return the same rows:

- `current`: the existing selection.
- `masked_multi`: `masked` for the sources the forward search would take (several targets, or no reverse
  adjacency); bidirectional search for the others.
- `masked_all`: `masked` for every source.

Each case runs with `shortestPath` and `allShortestPaths`, and with DataFusion's default partitioning and one
target partition. Layered graphs skip `allShortestPaths`: each pair has tens of thousands of shortest paths, which
measures output, not search. The CSV also reports how many `search_batch` calls ran and how many sources went to
`masked`.

## Inputs and checks

Deterministic 4,096-vertex graphs: a shared hub, a uniform directed graph, directed chains, an undirected tree,
disconnected components, and a layered directed graph. The tree has separate distant (beyond the hop limit) and nearby reachable targets. The main matrix uses 64 sources and either one or three
targets each; a hub sweep also uses 1, 8, 65 and 256 sources. Hub, uniform and layered cases also run at 32,768 vertices. Maximum depth is 16. These synthetic inputs are
controls, not a representative sample of customer graphs. `MSBFS_VERTICES` selects the graph size (default 4,096); the runner sets it explicitly.

The self-check compares complete depth/predecessor maps against forward BFS and all reconstructed shortest
paths against forward/bidirectional BFS. It covers directed/undirected graphs, duplicates, ties, self-loops,
cycles, missing vertices, unreachable targets, source equal to target, depth bounds, deletes/appends, repeated
source bindings, a 64-source batch boundary, and sparse negative/large raw keys. There are 160 combinations of
small generated graphs, direction, delta and depth, plus separate early-stop/empty-target and raw-key checks.

Every workload/mode also runs separately with `MSBFS_VERIFY=1`, checking each requested target's depth against
`current`. This extra reference search is omitted from the measured RSS processes. Timed processes validate
that returned paths start/end correctly and follow real edges; the runner checks matching aggregate lengths
and output sizes across modes and repetitions.

## Measurements and limits

Each process performs one warm-up and seven timed executions; CSV reports the median search time and median
reconstruction time separately. Two fresh process repetitions reverse mode order. Graph construction is
outside timing. Results are retained during reconstruction and dropped between repetitions.

See [RESULTS.md](RESULTS.md) and [raw measurements](results/raw.csv).

A separate untimed pass reports neighbour-list scans, traversed edge occurrences, retained depth entries and
predecessor entries in the returned search state (not transient bidirectional reverse maps). Work counters are not gathered for parallel tasks (CSV zeros mean unavailable). RSS is
whole-process peak, including graph construction, runtime and allocator retention; it is not operator-only
memory or a memory bound. Validation of target distances occurs in separate processes. Reporting separate
search/reconstruction medians does not make their sum a directly measured end-to-end median.

The kernel modes measure no parsing, planning, Arrow input/output batching or Cypher execution; the end-to-end
run below does. Neither measures Lance I/O or production memory-pool integration. Weighted paths and source-specific filters are outside the experiment.
All-shortest-path correctness is checked on small graphs; large workloads materialize one path per target.
Cancellation points are retained in the prototype, but cancellation and execution deadlines need dedicated
integration tests before any adoption.

These measurements cannot establish a main-versus-branch engine speedup. Before integrating a selected path,
run both SF1 suites in both modes with release wheels and the unchanged `CONTRIBUTING.md` harness, as well as
end-to-end shortest-path workloads and memory-budget/fallback checks.
