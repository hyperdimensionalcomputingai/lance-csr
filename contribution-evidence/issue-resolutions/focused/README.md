# Focused measurements

All 126 cells passed independently checked answers. Values below are medians of three fresh processes.

| Hub rows | Cohort | First batch ms | Observed growth MB |
| --- | --- | ---: | ---: |
| 100000 | main | 52.25 | 485.9 |
| 100000 | 11 | 6.77 | 114.9 |
| 500000 | main | 245.30 | 2144.5 |
| 500000 | 11 | 7.00 | 108.3 |
| 1000000 | main | 547.27 | 4179.4 |
| 1000000 | 11 | 8.03 | 70.8 |

The graph carries fixed 4 KiB properties. The cursor emits at most 8,192 output rows per batch; RSS growth excludes resident input and is a sampled lower bound.

| Mixed-label max hops | Cohort | CSR operators | Union operators | Physical text bytes | Explain ms |
| --- | --- | ---: | ---: | ---: | ---: |
| 2 | main | 3 | 1 | 5722 | 1.29 |
| 2 | 11 | 3 | 1 | 5722 | 1.24 |
| 2 | 13 | 1 | 0 | 1054 | 0.72 |
| 5 | main | 15 | 1 | 61822 | 4.31 |
| 5 | 11 | 15 | 1 | 61822 | 4.36 |
| 5 | 13 | 1 | 0 | 1054 | 0.70 |
| 10 | main | 55 | 1 | 522502 | 21.48 |
| 10 | 11 | 55 | 1 | 522502 | 21.99 |
| 10 | 13 | 1 | 0 | 1062 | 0.72 |
| 20 | main | 210 | 1 | 5493832 | 195.74 |
| 20 | 11 | 210 | 1 | 5493832 | 195.58 |
| 20 | 13 | 1 | 0 | 1062 | 0.77 |

| IC query | Cohort | Execute and convert ms |
| --- | --- | ---: |
| 9 | main | 1014.47 |
| 9 | 13 | 1020.94 |
| 9 | 16 | 895.47 |
| 12 | main | 189.72 |
| 12 | 11 | 191.20 |
| 12 | 13 | 32.67 |
| 13 | main | 8.90 |
| 13 | 11 | 8.01 |
| 13 | 17 | 3.81 |
| 14 | main | 76.29 |
| 14 | 11 | 83.93 |
| 14 | 17 | 58.35 |

Cohorts: main=54e0230, 11=e127755, 13=1bf6d42, 16=447355d, 17=09a92e4. Full hashes and loaded-wheel identities are in the raw results.

A separate deterministic shortest-path test retains 102 depth-map nodes versus 10,101 for forward BFS on the same wide-middle graph; these are retained search states, not instrumented SF1 edge-visit counts.

Stage profiling, IC9 read counters, IC10 count specialization, partition sizing and timeout cleanup have separate receipts. Their units and limitations must be preserved.
