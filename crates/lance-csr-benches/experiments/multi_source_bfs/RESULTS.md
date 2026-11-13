# Multi-source BFS feasibility results — November 13, 2026

Base: `c4ab747a62c79291820c181c5f5043983e232dcd` (main, v0.10.0). Branch: `codex/multi-source-bfs-experiment`. This is an opt-in search-kernel experiment; normal query execution is unchanged.

## Search measurements

Times are milliseconds, reported as the median of two process medians. Each process used one warm-up and seven timed searches. Lower is better. `Current` uses bidirectional search for one target when reverse adjacency is available; otherwise it uses forward search. `Parallel` uses up to eight independent current searches on eight Tokio workers, so it has a larger CPU budget.

| Graph | Vertices | Sources | Targets/source | Current | Forward | Batched | Parallel | Batched/current |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hub | 4096 | 64 | 1 | 0.026 | 11.042 | 14.370 | 0.130 | 557.17× |
| hub | 4096 | 64 | 3 | 11.048 | 11.065 | 14.769 | 2.761 | 1.34× |
| uniform | 4096 | 64 | 1 | 0.790 | 13.844 | 13.677 | 0.556 | 17.31× |
| uniform | 4096 | 64 | 3 | 25.563 | 25.379 | 25.307 | 7.299 | 0.99× |
| chain | 4096 | 64 | 1 | 0.051 | 0.047 | 0.057 | 0.188 | 1.13× |
| chain | 4096 | 64 | 3 | 0.057 | 0.058 | 0.067 | 0.196 | 1.17× |
| tree | 4096 | 64 | 1 | 0.364 | 2.497 | 1.954 | 0.378 | 5.36× |
| tree | 4096 | 64 | 3 | 2.500 | 2.495 | 1.911 | 0.958 | 0.76× |
| tree_near | 4096 | 64 | 1 | 0.296 | 1.299 | 1.010 | 0.328 | 3.41× |
| tree_near | 4096 | 64 | 3 | 2.480 | 2.474 | 1.970 | 0.964 | 0.79× |
| disconnected | 4096 | 64 | 1 | 0.395 | 0.401 | 0.605 | 0.431 | 1.53× |
| disconnected | 4096 | 64 | 3 | 0.409 | 0.403 | 0.616 | 0.534 | 1.51× |
| layered | 4096 | 64 | 1 | 1.020 | 1.929 | 1.744 | 0.599 | 1.71× |
| layered | 4096 | 64 | 3 | 1.926 | 1.873 | 1.748 | 0.832 | 0.91× |
| hub | 4096 | 1 | 3 | 0.173 | 0.175 | 0.300 | 0.198 | 1.74× |
| hub | 4096 | 8 | 3 | 1.380 | 1.388 | 1.533 | 0.365 | 1.11× |
| hub | 4096 | 65 | 3 | 11.316 | 11.233 | 15.049 | 3.013 | 1.33× |
| hub | 4096 | 256 | 3 | 44.232 | 44.342 | 58.850 | 10.974 | 1.33× |
| hub | 32768 | 64 | 1 | 0.025 | 88.822 | 159.806 | 0.132 | 6365.77× |
| hub | 32768 | 64 | 3 | 89.465 | 89.138 | 162.107 | 19.617 | 1.81× |
| uniform | 32768 | 64 | 1 | 2.137 | 125.530 | 197.931 | 0.956 | 92.63× |
| uniform | 32768 | 64 | 3 | 161.048 | 161.294 | 249.758 | 48.543 | 1.55× |
| layered | 32768 | 64 | 1 | 1.019 | 1.935 | 1.739 | 0.591 | 1.71× |
| layered | 32768 | 64 | 3 | 1.931 | 1.846 | 1.780 | 0.848 | 0.92× |

`tree` searches distant leaves with no path inside the 16-hop bound; `tree_near` searches reachable leaves. Disconnected targets are unreachable. All other scenarios return paths. The larger layered graph increases total graph size, but the selected sources still visit similarly sized local regions; it is not a larger explored-frontier test.

## What we learn

**Do not integrate this batched implementation as a default or broadly selected replacement.** It demonstrates correct sharing of neighbour reads across independent searches, but it does not establish a general performance win.

- **Preserve bidirectional search.** Every single-target case is slower with batching than with current selection. The 32K hub is an extreme example: current search touches 256 edge occurrences across all 64 requests, while batched forward search builds source-specific state across the wide layer. This is a different search strategy, not evidence that sharing reads itself costs thousands of times more.
- **The benefit is case-dependent even with several targets.** The two 4K tree cases reduce search time by about 21–24%; layered cases improve by roughly 8–9%. The 4K uniform result is essentially a tie. Chains and disconnected components get slower. Larger hub and uniform graphs make the prototype 1.81× and 1.55× slower respectively.
- **Read sharing does not remove source-specific state.** On the 4K three-target hub, edge visits drop from 262,336 to 4,351, but both forward methods retain 262,144 predecessor entries. The batched method still updates per-source hash maps and predecessor vectors; shared scans alone do not guarantee lower elapsed time. On the 32K hub, whole-process peak RSS rises from 268.5 MiB to 392.8 MiB.
- **Bounded source parallelism is the stronger next experiment.** At 64 sources and three targets, it is about 2–4.6× faster than current selection for the tree, layered, hub and uniform cases, including the larger graphs. It preserves the bidirectional/forward choice. It loses on tiny chain/disconnected searches and uses more CPU; these are throughput/latency trade-offs, not equal-resource algorithmic speedups.
- **Single-path reconstruction can matter for small searches.** In the three-target chain it costs more than the search. Kernel-only improvements therefore do not translate directly into the same query-level improvement. Large all-shortest-path output is not benchmarked.

## Repeat variability

The ranges below span the two process medians, not confidence intervals. Small differences should not be treated as established improvements. Different mode orders reduce one source of bias, but this is a local exploratory measurement, not a controlled statistical study.

| Graph | Vertices | Sources | Targets/source | Current range (ms) | Batched range (ms) | Parallel range (ms) |
|---|---:|---:|---:|---:|---:|---:|
| hub | 4096 | 64 | 1 | 0.026–0.026 | 14.297–14.443 | 0.127–0.132 |
| hub | 4096 | 64 | 3 | 11.042–11.055 | 14.768–14.769 | 2.741–2.781 |
| uniform | 4096 | 64 | 1 | 0.780–0.801 | 13.607–13.747 | 0.545–0.566 |
| uniform | 4096 | 64 | 3 | 25.518–25.608 | 25.169–25.444 | 7.281–7.318 |
| chain | 4096 | 64 | 1 | 0.050–0.051 | 0.056–0.058 | 0.185–0.190 |
| chain | 4096 | 64 | 3 | 0.057–0.058 | 0.067–0.068 | 0.189–0.203 |
| tree | 4096 | 64 | 1 | 0.360–0.368 | 1.942–1.965 | 0.369–0.386 |
| tree | 4096 | 64 | 3 | 2.482–2.517 | 1.910–1.911 | 0.953–0.962 |
| tree_near | 4096 | 64 | 1 | 0.292–0.301 | 1.007–1.013 | 0.321–0.336 |
| tree_near | 4096 | 64 | 3 | 2.473–2.487 | 1.965–1.974 | 0.944–0.985 |
| disconnected | 4096 | 64 | 1 | 0.383–0.408 | 0.602–0.608 | 0.430–0.433 |
| disconnected | 4096 | 64 | 3 | 0.403–0.415 | 0.614–0.618 | 0.533–0.535 |
| layered | 4096 | 64 | 1 | 1.015–1.024 | 1.740–1.749 | 0.591–0.608 |
| layered | 4096 | 64 | 3 | 1.892–1.960 | 1.748–1.748 | 0.826–0.837 |
| hub | 4096 | 1 | 3 | 0.172–0.173 | 0.300–0.300 | 0.194–0.203 |
| hub | 4096 | 8 | 3 | 1.377–1.383 | 1.522–1.544 | 0.362–0.367 |
| hub | 4096 | 65 | 3 | 11.260–11.372 | 15.046–15.052 | 2.992–3.034 |
| hub | 4096 | 256 | 3 | 44.187–44.276 | 58.369–59.331 | 10.909–11.039 |
| hub | 32768 | 64 | 1 | 0.025–0.025 | 159.457–160.155 | 0.127–0.137 |
| hub | 32768 | 64 | 3 | 89.408–89.523 | 162.093–162.121 | 19.416–19.818 |
| uniform | 32768 | 64 | 1 | 2.117–2.157 | 197.732–198.130 | 0.951–0.961 |
| uniform | 32768 | 64 | 3 | 160.456–161.640 | 249.426–250.091 | 48.330–48.756 |
| layered | 32768 | 64 | 1 | 0.977–1.061 | 1.715–1.762 | 0.589–0.594 |
| layered | 32768 | 64 | 3 | 1.877–1.985 | 1.756–1.804 | 0.799–0.896 |

## Reconstruction and peak memory

Reconstruction is timed separately using the production path routine, returning one path per target. Its median is not additive with the search median to produce an end-to-end median. Peak RSS is the whole process, including graph construction, runtime, profiling and allocator retention; it is not operator-only memory. The distance-verification reference search ran in separate processes.

| Graph | Vertices | Sources | Targets/source | Reconstruction ms: current / forward / batched / parallel | Peak MiB: current / forward / batched / parallel |
|---|---:|---:|---:|---|---|
| hub | 4096 | 64 | 1 | 0.0095 / 0.0295 / 0.0228 / 0.0133 | 12.5 / 43.1 / 56.2 / 12.5 |
| hub | 4096 | 64 | 3 | 0.0608 / 0.0591 / 0.0418 / 0.0668 | 43.1 / 43.1 / 56.3 / 49.5 |
| uniform | 4096 | 64 | 1 | 0.0198 / 0.0525 / 0.0434 / 0.0227 | 15.2 / 35.5 / 39.9 / 15.5 |
| uniform | 4096 | 64 | 3 | 0.1576 / 0.1613 / 0.1419 / 0.1587 | 46.3 / 46.3 / 60.3 / 55.3 |
| chain | 4096 | 64 | 1 | 0.0313 / 0.0338 / 0.0377 / 0.0365 | 11.9 / 11.9 / 11.9 / 12.1 |
| chain | 4096 | 64 | 3 | 0.0979 / 0.1008 / 0.1047 / 0.1069 | 12.0 / 12.0 / 12.0 / 12.1 |
| tree | 4096 | 64 | 1 | 0.0021 / 0.0028 / 0.0032 / 0.0028 | 12.0 / 16.5 / 21.0 / 12.3 |
| tree | 4096 | 64 | 3 | 0.0049 / 0.0047 / 0.0047 / 0.0059 | 16.4 / 16.5 / 20.9 / 17.1 |
| tree_near | 4096 | 64 | 1 | 0.0578 / 0.0636 / 0.0621 / 0.0655 | 12.2 / 14.4 / 16.4 / 12.4 |
| tree_near | 4096 | 64 | 3 | 0.1778 / 0.1771 / 0.2150 / 0.2088 | 16.6 / 16.6 / 21.2 / 17.5 |
| disconnected | 4096 | 64 | 1 | 0.0019 / 0.0021 / 0.0020 / 0.0033 | 13.3 / 13.3 / 13.7 / 13.5 |
| disconnected | 4096 | 64 | 3 | 0.0035 / 0.0031 / 0.0037 / 0.0047 | 13.3 / 13.3 / 13.7 / 13.4 |
| layered | 4096 | 64 | 1 | 0.0275 / 0.0340 / 0.0296 / 0.0372 | 14.5 / 15.5 / 16.0 / 14.9 |
| layered | 4096 | 64 | 3 | 0.0943 / 0.0904 / 0.0881 / 0.0916 | 15.5 / 15.5 / 16.0 / 15.9 |
| hub | 4096 | 1 | 3 | 0.0009 / 0.0008 / 0.0009 / 0.0012 | 12.6 / 12.6 / 12.6 / 12.6 |
| hub | 4096 | 8 | 3 | 0.0049 / 0.0054 / 0.0055 / 0.0072 | 16.0 / 16.0 / 17.7 / 18.4 |
| hub | 4096 | 65 | 3 | 0.0652 / 0.0602 / 0.0433 / 0.0663 | 47.2 / 47.1 / 56.5 / 51.1 |
| hub | 4096 | 256 | 3 | 0.3051 / 0.3015 / 0.2765 / 0.3170 | 135.5 / 135.4 / 160.9 / 144.8 |
| hub | 32768 | 64 | 1 | 0.0101 / 0.0641 / 0.0246 / 0.0120 | 23.8 / 268.5 / 392.1 / 23.9 |
| hub | 32768 | 64 | 3 | 0.1092 / 0.1096 / 0.0470 / 0.1077 | 268.5 / 268.5 / 392.8 / 302.7 |
| uniform | 32768 | 64 | 1 | 0.0259 / 0.1247 / 0.1060 / 0.0314 | 46.5 / 186.3 / 237.4 / 47.1 |
| uniform | 32768 | 64 | 3 | 0.3149 / 0.3186 / 0.2689 / 0.3182 | 225.1 / 225.1 / 276.9 / 242.1 |
| layered | 32768 | 64 | 1 | 0.0303 / 0.0289 / 0.0326 / 0.0312 | 38.0 / 39.0 / 39.5 / 38.3 |
| layered | 32768 | 64 | 3 | 0.0839 / 0.0824 / 0.0908 / 0.0998 | 39.0 / 39.0 / 39.5 / 39.3 |

## Work and retained state

These counters come from separate untimed passes. Retained state counts the returned maps, not temporary reverse-search maps. Parallel counters are intentionally unavailable rather than inferred. Full counters for every mode are in [raw.csv](results/raw.csv).

| Graph | Vertices | Sources | Targets/source | Edge occurrences: current → batched | Neighbour scans: current → batched | Predecessor entries: current → batched |
|---|---:|---:|---:|---:|---:|---:|
| hub | 4096 | 64 | 1 | 256 → 4,351 | 128 → 129 | 192 → 262,144 |
| hub | 4096 | 64 | 3 | 262,336 → 4,351 | 192 → 129 | 262,144 → 262,144 |
| uniform | 4096 | 64 | 1 | 16,408 → 83,656 | 2,051 → 10,457 | 12,878 → 323,010 |
| uniform | 4096 | 64 | 3 | 1,114,928 → 84,288 | 139,366 → 10,536 | 583,953 → 583,953 |
| chain | 4096 | 64 | 1 | 512 → 512 | 512 → 512 | 512 → 512 |
| chain | 4096 | 64 | 3 | 640 → 640 | 640 → 640 | 640 → 640 |
| tree | 4096 | 64 | 1 | 9,486 → 2,904 | 3,847 → 1,396 | 2,887 → 47,040 |
| tree | 4096 | 64 | 3 | 78,720 → 2,904 | 31,744 → 1,396 | 47,040 → 47,040 |
| tree_near | 4096 | 64 | 1 | 6,930 → 2,840 | 2,835 → 1,332 | 3,213 → 24,360 |
| tree_near | 4096 | 64 | 3 | 78,116 → 2,904 | 31,500 → 1,396 | 46,680 → 46,680 |
| disconnected | 4096 | 64 | 1 | 14,848 → 15,616 | 3,712 → 3,904 | 9,216 → 9,792 |
| disconnected | 4096 | 64 | 3 | 15,616 → 15,616 | 3,904 → 3,904 | 9,792 → 9,792 |
| layered | 4096 | 64 | 1 | 35,840 → 4,760 | 4,480 → 595 | 23,744 → 78,848 |
| layered | 4096 | 64 | 3 | 78,848 → 4,760 | 9,856 → 595 | 78,848 → 78,848 |
| hub | 4096 | 1 | 3 | 4,099 → 4,099 | 3 → 3 | 4,096 → 4,096 |
| hub | 4096 | 8 | 3 | 32,792 → 4,127 | 24 → 17 | 32,768 → 32,768 |
| hub | 4096 | 65 | 3 | 266,435 → 8,450 | 195 → 132 | 266,240 → 266,240 |
| hub | 4096 | 256 | 3 | 1,049,344 → 17,404 | 768 → 516 | 1,048,576 → 1,048,576 |
| hub | 32768 | 64 | 1 | 256 → 33,023 | 128 → 129 | 192 → 2,097,152 |
| hub | 32768 | 64 | 3 | 2,097,344 → 33,023 | 192 → 129 | 2,097,152 → 2,097,152 |
| uniform | 32768 | 64 | 1 | 44,480 → 748,200 | 5,560 → 93,525 | 31,698 → 2,631,340 |
| uniform | 32768 | 64 | 3 | 5,911,896 → 750,864 | 738,987 → 93,858 | 3,314,964 → 3,314,964 |
| layered | 32768 | 64 | 1 | 35,840 → 4,760 | 4,480 → 595 | 23,744 → 78,848 |
| layered | 32768 | 64 | 3 | 78,848 → 4,760 | 9,856 → 595 | 78,848 → 78,848 |

## Correctness and validation

- Release build succeeded with the opt-in `experimental-msbfs` feature.
- Self-check passed: 160 graph/direction/delta/depth combinations with 65 requests each, comparing complete depth maps, predecessor multiplicities and all shortest paths. Additional checks cover early stopping across lanes, empty target sets and sparse negative/large raw keys.
- All 24 workloads × four modes matched the current implementation on every requested target distance in separate verification processes.
- All 192 measured process runs passed path-edge/start/end checks and matched aggregate output lengths/checksums across methods and repetitions.
- Six existing shortest-path-related Rust regression tests passed, including cancellation and bidirectional equivalence.
- `mbx fmt --all --check`, Ruff format/check for the runner, and `uvx typos@1.51.1 .` passed.
- `mbx clippy --workspace --all-targets --release --features experimental-msbfs --locked -- -D warnings` passed.

Regression command: `mbx test -p lance-csr shortest_path --lib --release --features experimental-msbfs --locked`.

## Recommendation and next decision

Keep issue #76 general-purpose and optional. This prototype preserves independent source identities and all predecessor ties; it is not nearest-source BFS and makes no social-graph-specific assumptions. It still needs shared adjacency/direction/filter semantics and unweighted shortest-path searches. Its 64-source cap bounds mask width, **not memory consumption**.

If continuing implementation work, first test bounded source-level concurrency inside the existing `search_batch`, with a small-work fallback and explicit memory/cancellation handling. Measure end-to-end Cypher input grouping, path reconstruction and output before adopting it. Concurrency must coordinate with DataFusion partition parallelism to avoid oversubscription. This report does not justify a production threshold.

Further MSBFS work would need evidence that predecessor/state handling can be made cheaper, or a workload where shared expansion clearly dominates that cost. Do not narrow lance-csr to those graph shapes, remove bidirectional search, or infer an automatic cost gate from this small matrix.

## Reproduction and limits

Host: Apple M5, Mac17,2, 10 cores, 24 GiB RAM, macOS 26.6.2 (25G83). Compiler: rustc 1.96.0 (ac68faa20 2026-05-25). Benchmarks ran sequentially, with no compilation overlapping the final measurements. No new dependencies were added.

See [README.md](README.md) for exact commands and workload construction, [run.py](run.py) for the runner, and [raw.csv](results/raw.csv) for all unrounded measurements. The normal build compiles out the experimental adapter and instrumentation. Timed experimental builds check a disabled profiling flag once per expanded vertex for both existing and batched searches; this overhead is not independently calibrated.

Not checked: full main-versus-branch SF1 suites, Python bindings, production planner integration, memory-pool enforcement, prototype cancellation/deadline integration, source-specific filters, weighted paths or large all-shortest-path materialization. No end-to-end engine speedup is claimed. Both unchanged SF1 suites in memory/lazy modes and dedicated end-to-end shortest-path tests remain prerequisites for any production performance PR, following `CONTRIBUTING.md`.
