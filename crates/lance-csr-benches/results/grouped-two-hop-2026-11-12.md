# Grouped two-hop count validation

Comparison of fresh release wheels; SF1 runners, queries, expected answers and settings are unchanged.

## Provenance

- main: `c062ac287009a249fae2549d6bcd2fb31ce33a82`
- branch: `3d94501e0ea3ffc3e26f4ed714c03f0038ff2638`
- custom: `d5ca71dde7e3f7d7f22336b9a7c83cc2205d7b93`
- complex: `a971f4e5aa8f35d59e939f6efc30c1a1e5edc91e`
- hardware: `Mac16,6; 16 CPU cores; 128 GiB RAM; macOS 26.6.2 (25G83)`
- dataset_sha256: `5d74f5b0b0570eb593b3317e14ffab3d5dd181efb7e2c947e0ed0409c377b23d`
- dataset_url: `https://datasets.ldbcouncil.org/snb-interactive-v1/social_network-sf1-CsvBasic-StringDateFormatter.tar.zst`
- initial_prototype: `fac8aa0; initial measurements in ../lance-csr-136-initial-results; superseded after a 2.5x selective outer-group regression`

## SF1 suites

Latencies are milliseconds, averaged over two process runs per build. Run gap is the absolute difference between the two means divided by their average. Ratios below one favor the branch.

### custom, memory

| Query | main ms | branch ms | Change | main run gap | branch run gap |
|---|---:|---:|---:|---:|---:|
| test_benchmark_query1 | 0.823 | 0.738 | -10.4% | 16.9% | 2.0% |
| test_benchmark_query2 | 0.679 | 0.696 | +2.6% | 3.5% | 7.7% |
| test_benchmark_query3 | 0.769 | 0.802 | +4.3% | 10.0% | 15.5% |
| test_benchmark_query4 | 0.616 | 0.615 | -0.2% | 5.2% | 10.5% |
| test_benchmark_query5 | 1.032 | 1.031 | -0.1% | 12.4% | 7.5% |
| test_benchmark_query6 | 0.533 | 0.536 | +0.5% | 15.3% | 13.6% |
| test_benchmark_query7 | 1.277 | 1.293 | +1.2% | 9.0% | 7.9% |
| test_benchmark_query8 | 0.807 | 0.905 | +12.1% | 12.1% | 8.9% |
| test_benchmark_query9 | 0.959 | 1.014 | +5.8% | 13.1% | 3.4% |
| test_benchmark_query10 | 0.926 | 0.970 | +4.7% | 11.4% | 0.8% |
| test_benchmark_query11 | 1.988 | 1.958 | -1.5% | 23.4% | 9.5% |
| test_benchmark_query12 | 1.024 | 0.973 | -5.0% | 9.8% | 2.0% |
| test_benchmark_query13 | 0.969 | 1.032 | +6.5% | 2.3% | 2.4% |
| test_benchmark_query14 | 0.699 | 0.688 | -1.5% | 3.0% | 0.3% |
| test_benchmark_query15 | 0.726 | 0.727 | +0.1% | 4.6% | 3.0% |
| test_benchmark_query16 | 0.834 | 0.847 | +1.6% | 1.4% | 1.4% |
| test_benchmark_query17 | 1.398 | 1.186 | -15.2% | 8.8% | 1.1% |
| test_benchmark_query18 | 0.905 | 0.882 | -2.6% | 17.9% | 4.7% |
| test_benchmark_query19 | 1.206 | 1.250 | +3.7% | 7.4% | 5.7% |
| test_benchmark_query20 | 1.848 | 1.799 | -2.7% | 1.4% | 5.9% |
| test_benchmark_query21 | 0.901 | 0.916 | +1.7% | 2.3% | 2.6% |
| test_benchmark_query22 | 0.890 | 0.906 | +1.8% | 2.4% | 3.2% |
| test_benchmark_query23 | 0.932 | 0.994 | +6.6% | 6.0% | 8.8% |
| test_benchmark_query24 | 0.875 | 0.895 | +2.3% | 4.7% | 0.4% |
| test_benchmark_query25 | 0.983 | 1.051 | +6.9% | 8.8% | 7.0% |
| test_benchmark_query26 | 0.880 | 0.953 | +8.4% | 16.8% | 1.9% |
| test_benchmark_query27 | 1.067 | 1.123 | +5.2% | 12.4% | 3.6% |
| test_benchmark_query28 | 1.845 | 1.827 | -1.0% | 7.3% | 1.8% |
| test_benchmark_query29 | 0.864 | 0.913 | +5.7% | 4.6% | 4.1% |
| test_benchmark_query30 | 24.965 | 25.032 | +0.3% | 4.3% | 4.1% |

Geometric mean branch/main: **1.013**.

### custom, lazy

| Query | main ms | branch ms | Change | main run gap | branch run gap |
|---|---:|---:|---:|---:|---:|
| test_benchmark_query1 | 1.391 | 1.469 | +5.6% | 6.2% | 2.9% |
| test_benchmark_query2 | 1.542 | 1.595 | +3.4% | 3.3% | 6.4% |
| test_benchmark_query3 | 0.995 | 0.957 | -3.9% | 11.7% | 1.7% |
| test_benchmark_query4 | 1.074 | 1.151 | +7.2% | 4.7% | 1.7% |
| test_benchmark_query5 | 2.162 | 2.157 | -0.2% | 0.2% | 0.2% |
| test_benchmark_query6 | 0.953 | 0.922 | -3.2% | 10.9% | 3.2% |
| test_benchmark_query7 | 1.456 | 1.435 | -1.5% | 0.2% | 2.3% |
| test_benchmark_query8 | 1.770 | 1.695 | -4.3% | 8.0% | 3.4% |
| test_benchmark_query9 | 1.550 | 1.572 | +1.4% | 3.8% | 3.1% |
| test_benchmark_query10 | 1.246 | 1.317 | +5.7% | 11.6% | 3.6% |
| test_benchmark_query11 | 2.413 | 2.499 | +3.6% | 0.6% | 1.1% |
| test_benchmark_query12 | 5.528 | 5.357 | -3.1% | 7.6% | 2.0% |
| test_benchmark_query13 | 1.468 | 1.495 | +1.8% | 0.5% | 1.9% |
| test_benchmark_query14 | 0.887 | 0.934 | +5.4% | 0.9% | 0.9% |
| test_benchmark_query15 | 1.033 | 1.062 | +2.8% | 6.7% | 1.2% |
| test_benchmark_query16 | 1.240 | 1.277 | +3.0% | 13.9% | 3.6% |
| test_benchmark_query17 | 2.011 | 2.161 | +7.5% | 4.6% | 7.1% |
| test_benchmark_query18 | 1.629 | 1.678 | +3.0% | 0.9% | 1.6% |
| test_benchmark_query19 | 1.704 | 2.063 | +21.1% | 4.9% | 16.3% |
| test_benchmark_query20 | 45.052 | 45.080 | +0.1% | 1.2% | 0.9% |
| test_benchmark_query21 | 1.108 | 1.061 | -4.3% | 14.9% | 6.0% |
| test_benchmark_query22 | 1.280 | 1.277 | -0.2% | 4.1% | 2.2% |
| test_benchmark_query23 | 1.513 | 1.493 | -1.3% | 0.6% | 1.5% |
| test_benchmark_query24 | 1.304 | 1.249 | -4.2% | 6.5% | 10.3% |
| test_benchmark_query25 | 1.539 | 1.571 | +2.1% | 12.4% | 0.8% |
| test_benchmark_query26 | 1.309 | 1.309 | +0.0% | 0.8% | 0.3% |
| test_benchmark_query27 | 1.753 | 1.968 | +12.3% | 1.0% | 9.2% |
| test_benchmark_query28 | 2.521 | 2.527 | +0.2% | 5.8% | 1.2% |
| test_benchmark_query29 | 1.457 | 1.462 | +0.3% | 7.1% | 14.7% |
| test_benchmark_query30 | 23.792 | 23.721 | -0.3% | 4.1% | 1.7% |

Geometric mean branch/main: **1.019**.

### complex, memory

| Query | main ms | branch ms | Change | main run gap | branch run gap |
|---|---:|---:|---:|---:|---:|
| test_benchmark_query1 | 25.740 | 26.161 | +1.6% | 2.1% | 6.3% |
| test_benchmark_query2 | 3.155 | 3.128 | -0.9% | 5.3% | 2.3% |
| test_benchmark_query3 | 24.799 | 25.205 | +1.6% | 2.8% | 5.4% |
| test_benchmark_query4 | 7.159 | 7.581 | +5.9% | 3.0% | 13.9% |
| test_benchmark_query5 | 53.650 | 57.827 | +7.8% | 4.0% | 4.4% |
| test_benchmark_query6 | 27.301 | 27.592 | +1.1% | 3.8% | 0.5% |
| test_benchmark_query7 | 11.370 | 11.507 | +1.2% | 1.0% | 1.1% |
| test_benchmark_query8 | 2.822 | 2.694 | -4.6% | 4.7% | 0.3% |
| test_benchmark_query9 | 45.018 | 47.258 | +5.0% | 2.9% | 11.0% |
| test_benchmark_query10 | 19.525 | 19.458 | -0.3% | 1.6% | 2.1% |
| test_benchmark_query11 | 3.611 | 3.481 | -3.6% | 4.2% | 1.7% |
| test_benchmark_query12 | 22.168 | 21.827 | -1.5% | 0.1% | 0.7% |
| test_benchmark_query13 | 1.343 | 1.367 | +1.8% | 13.1% | 2.5% |
| test_benchmark_query14 | 319.737 | 322.742 | +0.9% | 0.1% | 0.5% |

Geometric mean branch/main: **1.011**.

### complex, lazy

| Query | main ms | branch ms | Change | main run gap | branch run gap |
|---|---:|---:|---:|---:|---:|
| test_benchmark_query1 | 37.807 | 37.135 | -1.8% | 1.2% | 1.7% |
| test_benchmark_query2 | 29.623 | 29.040 | -2.0% | 0.2% | 1.4% |
| test_benchmark_query3 | 182.710 | 184.598 | +1.0% | 0.1% | 2.8% |
| test_benchmark_query4 | 15.535 | 15.185 | -2.3% | 3.6% | 0.5% |
| test_benchmark_query5 | 83.551 | 82.375 | -1.4% | 1.3% | 1.3% |
| test_benchmark_query6 | 29.744 | 29.746 | +0.0% | 2.3% | 2.2% |
| test_benchmark_query7 | 16.315 | 16.285 | -0.2% | 1.0% | 0.6% |
| test_benchmark_query8 | 4.198 | 4.056 | -3.4% | 1.6% | 3.2% |
| test_benchmark_query9 | 350.265 | 356.295 | +1.7% | 2.1% | 0.7% |
| test_benchmark_query10 | 29.800 | 29.582 | -0.7% | 0.6% | 0.3% |
| test_benchmark_query11 | 6.327 | 6.282 | -0.7% | 0.3% | 1.7% |
| test_benchmark_query12 | 24.790 | 24.268 | -2.1% | 1.1% | 1.2% |
| test_benchmark_query13 | 1.951 | 1.808 | -7.3% | 14.4% | 1.1% |
| test_benchmark_query14 | 342.725 | 334.926 | -2.3% | 2.4% | 0.2% |

Geometric mean branch/main: **0.984**.

## Focused generated workload

Each number averages two fresh-process medians of 11 cached executions. Peak RSS is the largest process peak (includes Python, Arrow, graph construction and the engine); it is not operator-only memory. Every result checksum matches across builds and repeats. Path counts are the exact enumerated-reference result cardinality. Max rows is the largest instrumented operator output_rows count (approximate, parsed from rounded metric units) from a separate EXPLAIN ANALYZE via GRAPH_TABLE over the same engine; CSR operators have no direct row metric, but their adjacent projections do. Profiling happens after the timed executions and the peak-RSS reading.

| Case | Group | Edges/type | Valid paths | main ms | branch ms | Ratio | main peak MiB | branch peak MiB | main max rows | branch max rows |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| balanced | a.id | 65536 | 4194304 | 7.732 | 2.577 | 0.333 | 119.8 | 115.8 | 4190000 | 65540 |
| balanced | b.id | 65536 | 4194304 | 2.212 | 2.368 | 1.071 | 114.4 | 113.2 | 65540 | 1020 |
| balanced | a.bucket | 65536 | 4194304 | 7.997 | 2.614 | 0.327 | 118.0 | 113.5 | 4190000 | 65540 |
| balanced | b.bucket | 65536 | 4194304 | 2.369 | 2.450 | 1.034 | 112.9 | 112.3 | 65540 | 1020 |
| skewed | a.id | 2048 | 1051648 | 11.484 | 11.519 | 1.003 | 100.5 | 100.9 | 1050000 | 1050000 |
| skewed | b.id | 2048 | 1051648 | 1.206 | 1.237 | 1.026 | 103.0 | 102.4 | 2050 | 2050 |
| skewed | a.bucket | 2048 | 1051648 | 11.948 | 11.957 | 1.001 | 100.4 | 100.5 | 1050000 | 1050000 |
| skewed | b.bucket | 2048 | 1051648 | 1.234 | 1.222 | 0.990 | 102.0 | 101.7 | 2050 | 2050 |
| same_type | a.id | 65536 | 4193280 | 8.508 | 2.838 | 0.334 | 120.3 | 116.4 | 4190000 | 65540 |
| same_type | b.id | 65536 | 4193280 | 8.596 | 2.780 | 0.323 | 120.7 | 112.9 | 4190000 | 1020 |
| same_type | a.bucket | 65536 | 4193280 | 9.014 | 2.828 | 0.314 | 120.1 | 115.7 | 4190000 | 65540 |
| same_type | b.bucket | 65536 | 4193280 | 8.993 | 3.081 | 0.343 | 119.5 | 115.6 | 4190000 | 1020 |
| same_end | a.id | 65536 | 4128768 | 8.502 | 4.581 | 0.539 | 119.3 | 136.3 | 4190000 | 65540 |
| same_end | b.id | 65536 | 4128768 | 8.762 | 3.386 | 0.386 | 121.1 | 145.3 | 4190000 | 131100 |
| same_end | a.bucket | 65536 | 4128768 | 8.840 | 4.571 | 0.517 | 119.2 | 138.3 | 4190000 | 65540 |
| same_end | b.bucket | 65536 | 4128768 | 9.026 | 3.661 | 0.406 | 120.5 | 145.9 | 4190000 | 131100 |
| low_fanout | a.id | 1024 | 1024 | 0.850 | 0.825 | 0.971 | 100.5 | 100.1 | 1020 | 1020 |
| low_fanout | b.id | 1024 | 1024 | 0.822 | 0.841 | 1.023 | 100.6 | 100.6 | 1020 | 1020 |
| low_fanout | a.bucket | 1024 | 1024 | 0.836 | 0.822 | 0.983 | 99.8 | 99.6 | 1020 | 1020 |
| low_fanout | b.bucket | 1024 | 1024 | 0.830 | 0.790 | 0.952 | 99.3 | 99.5 | 1020 | 1020 |
| selective | a.id | 65536 | 2 | 1.001 | 1.044 | 1.042 | 111.2 | 111.3 | 2 | 2 |
| selective | b.id | 65536 | 2 | 2.449 | 2.545 | 1.039 | 112.4 | 112.4 | 1020 | 1020 |
| selective | a.bucket | 65536 | 2 | 1.063 | 1.137 | 1.070 | 111.2 | 112.5 | 2 | 2 |
| selective | b.bucket | 65536 | 2 | 2.544 | 2.608 | 1.025 | 113.3 | 112.8 | 1020 | 1020 |

## Correctness and static checks

- Rust workspace: 708 passed, 10 ignored; no failures.
- Python: 1,722 passed.
- Workspace Clippy with warnings denied, Rust formatting, Ruff, Pyright, strict Zensical, spelling and documentation snippets passed.
- No public API or storage changes. No PR or merge was performed.


## Selective-query safeguard and timing investigation

The first prototype made ID-bound outer grouping about 2.5x slower by counting the opposite hop across the graph. The final planner retains the selective walk for literal, parameterized, reversed and inline outer-ID equalities. The final focused ratios are 1.042 for `a.id` and 1.070 for `a.bucket`, within the repository tolerance. Other selective predicates still use the existing mean-fan-out heuristic.

The primary custom-lazy Q19 mean was +21.1%. Its branch runs were 2.232 and 1.895 ms (main: 1.663 and 1.746 ms). A third complete custom-lazy pair produced 1.754 ms on main and 1.825 ms on the branch (+4.1%); every answer passed. Full Q19 explain plans are identical, SHA-256 `7939083ac4752436333cd65fbba80b94cf3e9f6c983cd35e03cb6b26f7cdce6b`, and do not use the new rewrite. The confirmation supports timing variability; it does not establish a Q19 performance change. The original runs above are retained.

### Additional full custom-lazy confirmation

| Query | main ms | branch ms | Change |
|---|---:|---:|---:|
| test_benchmark_query1 | 1.458 | 1.623 | +11.3% |
| test_benchmark_query2 | 1.553 | 1.700 | +9.5% |
| test_benchmark_query3 | 0.955 | 0.948 | -0.7% |
| test_benchmark_query4 | 1.027 | 1.018 | -0.9% |
| test_benchmark_query5 | 2.090 | 2.196 | +5.1% |
| test_benchmark_query6 | 0.839 | 0.863 | +2.9% |
| test_benchmark_query7 | 1.356 | 1.473 | +8.6% |
| test_benchmark_query8 | 1.856 | 1.669 | -10.1% |
| test_benchmark_query9 | 1.583 | 1.559 | -1.5% |
| test_benchmark_query10 | 1.295 | 1.277 | -1.4% |
| test_benchmark_query11 | 2.470 | 2.546 | +3.1% |
| test_benchmark_query12 | 5.436 | 5.208 | -4.2% |
| test_benchmark_query13 | 1.459 | 1.474 | +1.0% |
| test_benchmark_query14 | 0.855 | 0.879 | +2.9% |
| test_benchmark_query15 | 1.072 | 1.213 | +13.1% |
| test_benchmark_query16 | 1.292 | 1.225 | -5.2% |
| test_benchmark_query17 | 2.073 | 1.897 | -8.5% |
| test_benchmark_query18 | 1.599 | 1.690 | +5.7% |
| test_benchmark_query19 | 1.754 | 1.825 | +4.1% |
| test_benchmark_query20 | 45.520 | 44.464 | -2.3% |
| test_benchmark_query21 | 1.152 | 1.043 | -9.5% |
| test_benchmark_query22 | 1.338 | 1.272 | -5.0% |
| test_benchmark_query23 | 1.498 | 1.553 | +3.7% |
| test_benchmark_query24 | 1.323 | 1.278 | -3.4% |
| test_benchmark_query25 | 1.540 | 1.566 | +1.6% |
| test_benchmark_query26 | 1.340 | 1.285 | -4.1% |
| test_benchmark_query27 | 1.670 | 1.819 | +8.9% |
| test_benchmark_query28 | 2.465 | 2.762 | +12.1% |
| test_benchmark_query29 | 1.312 | 1.483 | +13.0% |
| test_benchmark_query30 | 23.321 | 24.817 | +6.4% |

Confirmation geometric mean branch/main: 1.017.

## Focused Criterion comparison

These are 256-node generated cases, comparing weighted queries with the path-enumerating `count(*) + 0` reference on the final branch. Warm-up and measurement times are one second each, with ten samples; all 40 cases passed their answer and plan-selection self-checks. They are separate from the 1,024-node main/branch measurements above.

| Case/group | weighted ms | enumerated ms | Ratio |
|---|---:|---:|---:|
| balanced/a.bucket | 1.706 | 4.679 | 0.365 |
| balanced/a.id | 1.753 | 4.699 | 0.373 |
| balanced/b.bucket | 1.614 | 4.676 | 0.345 |
| balanced/b.id | 1.448 | 4.703 | 0.308 |
| low_fanout/a.bucket | 0.800 | 0.770 | 1.039 |
| low_fanout/a.id | 0.773 | 0.730 | 1.059 |
| low_fanout/b.bucket | 0.772 | 0.699 | 1.103 |
| low_fanout/b.id | 0.778 | 0.805 | 0.966 |
| same_type/a.bucket | 2.009 | 5.045 | 0.398 |
| same_type/a.id | 1.827 | 5.071 | 0.360 |
| same_type/b.bucket | 2.281 | 4.925 | 0.463 |
| same_type/b.id | 1.799 | 4.885 | 0.368 |
| selective/a.bucket | 1.055 | 1.103 | 0.956 |
| selective/a.id | 1.042 | 0.949 | 1.098 |
| selective/b.bucket | 1.562 | 1.067 | 1.464 |
| selective/b.id | 1.498 | 0.965 | 1.553 |
| skewed/a.bucket | 1.688 | 1.754 | 0.963 |
| skewed/a.id | 1.700 | 1.728 | 0.984 |
| skewed/b.bucket | 1.117 | 1.677 | 0.666 |
| skewed/b.id | 1.143 | 1.677 | 0.682 |

## Interpretation and reproduction

- Broad outer grouping is about 3x faster in the focused main/branch cases. Different-type middle grouping reduces intermediate work but does not show a latency improvement on these small inputs.
- Same-end grouped counts are about 1.9–2.6x faster and add roughly 17–25 MiB of peak process RSS in these cases. They retain endpoint-count hash tables instead of streaming all paths through a final aggregate.
- Peak RSS includes the interpreter, imported libraries, graph construction and engine. Operator row metrics are rounded, are measured separately from timing, and are not a bound on simultaneously resident rows.
- The SF1 archive digest above is locally computed, not checked against an upstream published digest. Both suites use their unchanged ingestion scripts and the same downloaded CSV snapshot.
- Reproduce the SF1 comparison with the recorded commits and the release-wheel harness in the repository CONTRIBUTING.md. Both wheels use the verified mbx Cargo shim.
- Reproduce the generated profile with `uv run --frozen --with <wheel> python <checkout>/crates/lance-csr-benches/scripts/grouped_two_hop.py balanced a.id` from a prepared benchmark environment. Run each case and group against both wheels in alternating fresh processes twice. The helper independently checks answers against enumeration after capturing timing and RSS.
- Reproduce Criterion with `mbx bench -p lance-csr-benches --bench graph_execution -- grouped_two_hop --warm-up-time 1 --measurement-time 1 --sample-size 10`.
