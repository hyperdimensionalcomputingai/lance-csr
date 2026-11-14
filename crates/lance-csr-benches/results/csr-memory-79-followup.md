# CSR memory: build cost and synthetic graphs, 2026-11-14

Every figure behind "Shapes no suite has", the build cost and the IC14 table in `csr-memory-2026-11-14.md`.
Wheels: `main` at `cbad7b8`; `control` is the branch with the words layout off; `branch` is the branch.

## Build cost on SF1 (custom suite's engine, mean of 3 processes)

| Mode | Wheel | Engine construction (s) | Summed CSR build (s) | RSS after (MiB) | Peak RSS (MiB) |
| --- | --- | ---: | ---: | ---: | ---: |
| memory | main | 1.56 (1.55–1.58) | 1.48 | 2683 | 2683 |
| memory | control | 1.34 (1.33–1.35) | 1.25 | 2490 | 2490 |
| memory | branch | 1.20 (1.20–1.21) | 1.12 | 2430 | 2430 |
| lazy | main | 1.57 (1.57–1.57) | 1.57 | 1546 | 1546 |
| lazy | control | 1.33 (1.33–1.34) | 1.33 | 1472 | 1472 |
| lazy | branch | 1.20 (1.20–1.21) | 1.20 | 1357 | 1357 |

## Shapes no suite has (median ms; median of 3 processes)

**many_to_one_dense**

| Measure | main | control | branch | branch vs main | branch vs control | branch's processes differ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| construct_ms | 1706 | 1087 | 1037 | -39.2% | -4.6% | 0.3% |
| csr_build_ms | 1617 | 994 | 943 | -41.7% | -5.1% | 0.2% |
| engine_rss_mib | 1638 | 1568 | 1390 | -15.2% | -11.4% | 1.7% |
| peak_rss_mib | 2023 | 1952 | 1774 | -12.3% | -9.1% | 1.4% |
| fan_in_uniform | 138.80 | 111.80 | 78.05 | -43.8% | -30.2% | 4.4% |
| fan_in_hot_3pct | 124.54 | 97.17 | 67.82 | -45.5% | -30.2% | 3.6% |
| fan_in_selective | 4.76 | 3.62 | 2.66 | -44.0% | -26.4% | 13.5% |
| whole_relationship | 44.83 | 42.71 | 14.02 | -68.7% | -67.2% | 3.1% |
| reverse_1pct | 0.98 | 0.96 | 0.96 | -3.0% | -0.9% | 18.3% |
| point | 0.75 | 0.75 | 0.76 | +1.6% | +2.0% | 3.2% |
| fan_in_half_missing | 96.11 | 81.56 | 65.30 | -32.1% | -19.9% | 4.3% |

**many_to_one_random**

| Measure | main | control | branch | branch vs main | branch vs control | branch's processes differ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| construct_ms | 1433 | 1145 | 1144 | -20.2% | -0.1% | 0.3% |
| csr_build_ms | 1348 | 1052 | 1052 | -22.0% | +0.0% | 0.3% |
| engine_rss_mib | 1428 | 1426 | 1385 | -3.0% | -2.9% | 2.8% |
| peak_rss_mib | 1800 | 1798 | 1757 | -2.4% | -2.3% | 2.2% |
| fan_in_uniform | 137.89 | 111.30 | 110.97 | -19.5% | -0.3% | 5.0% |
| fan_in_hot_3pct | 124.33 | 99.10 | 99.06 | -20.3% | -0.0% | 1.3% |
| fan_in_selective | 4.74 | 3.57 | 3.57 | -24.5% | +0.1% | 3.3% |
| whole_relationship | 44.92 | 42.77 | 43.14 | -4.0% | +0.9% | 2.8% |
| reverse_1pct | 0.96 | 0.97 | 0.88 | -8.8% | -9.2% | 2.7% |
| point | 0.75 | 0.75 | 0.75 | +0.7% | -0.2% | 0.7% |

**many_to_one_shuffled**

| Measure | main | control | branch | branch vs main | branch vs control | branch's processes differ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| construct_ms | 1429 | 1143 | 1031 | -27.9% | -9.8% | 0.7% |
| csr_build_ms | 1344 | 1050 | 939 | -30.2% | -10.7% | 0.5% |
| engine_rss_mib | 1432 | 1429 | 1306 | -8.9% | -8.6% | 0.9% |
| peak_rss_mib | 1782 | 1779 | 1655 | -7.1% | -7.0% | 0.7% |
| fan_in_uniform | 136.44 | 111.74 | 77.41 | -43.3% | -30.7% | 3.9% |
| fan_in_hot_3pct | 124.52 | 97.29 | 67.42 | -45.9% | -30.7% | 2.6% |
| fan_in_selective | 4.71 | 3.60 | 2.45 | -48.0% | -31.9% | 2.6% |
| whole_relationship | 45.43 | 43.53 | 31.15 | -31.4% | -28.4% | 0.4% |
| reverse_1pct | 0.92 | 0.94 | 0.96 | +5.3% | +2.7% | 5.9% |
| point | 0.75 | 0.75 | 0.74 | -0.8% | -0.7% | 3.8% |

**many_to_many**

| Measure | main | control | branch | branch vs main | branch vs control | branch's processes differ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| construct_ms | 2447 | 1760 | 1756 | -28.2% | -0.2% | 0.3% |
| csr_build_ms | 2395 | 1703 | 1699 | -29.0% | -0.2% | 0.3% |
| engine_rss_mib | 2209 | 2149 | 2149 | -2.7% | +0.0% | 0.0% |
| peak_rss_mib | 2729 | 2670 | 2670 | -2.2% | +0.0% | 0.0% |
| point | 0.60 | 0.62 | 0.62 | +3.2% | +0.1% | 8.9% |
| one_hop_1pct | 3.99 | 3.41 | 3.53 | -11.6% | +3.4% | 4.5% |
| two_hop | 9.26 | 7.85 | 8.16 | -11.9% | +3.9% | 2.6% |
| three_hop | 2.15 | 1.98 | 2.03 | -5.3% | +2.5% | 4.2% |
| whole_relationship | 170.84 | 148.03 | 150.15 | -12.1% | +1.4% | 3.5% |


# The first version, which numbered every direction by key

Measured before sources were numbered in table order. `control2` is that version's control with sources
numbered in table order.

## IC suite with a second control (mean ms over 3 rounds)

**memory**

| Query | main | control2 | control | branch | main's rounds differ | control2 vs main | control vs control2 | branch vs control |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| query1 | 23.73 | 23.71 | 23.70 | 23.49 | 1.5% | -0.1% | -0.1% | -0.9% |
| query2 | 2.39 | 2.37 | 2.41 | 2.42 | 4.9% | -0.4% | +1.6% | +0.2% |
| query3 | 25.17 | 25.10 | 25.09 | 25.11 | 1.2% | -0.3% | -0.0% | +0.1% |
| query4 | 5.61 | 5.60 | 5.67 | 5.66 | 2.3% | -0.2% | +1.2% | -0.2% |
| query5 | 75.26 | 73.06 | 74.37 | 71.22 | 2.9% | -2.9% | +1.8% | -4.2% |
| query6 | 20.37 | 19.92 | 20.49 | 20.92 | 2.5% | -2.2% | +2.9% | +2.1% |
| query7 | 9.60 | 9.66 | 9.70 | 9.70 | 1.1% | +0.7% | +0.5% | -0.0% |
| query8 | 2.25 | 2.24 | 2.26 | 2.25 | 0.5% | -0.6% | +0.8% | -0.1% |
| query9 | 36.39 | 35.63 | 36.80 | 37.07 | 6.1% | -2.1% | +3.3% | +0.7% |
| query10 | 20.14 | 20.10 | 20.17 | 20.14 | 0.6% | -0.2% | +0.4% | -0.1% |
| query11 | 2.96 | 2.96 | 2.98 | 2.98 | 1.8% | +0.1% | +0.6% | -0.0% |
| query12 | 16.33 | 17.89 | 17.09 | 17.28 | 2.8% | +9.6% | -4.5% | +1.1% |
| query13 | 1.11 | 1.10 | 1.12 | 1.13 | 3.7% | -0.2% | +1.0% | +1.1% |
| query14 | 270.39 | 338.96 | 234.64 | 230.78 | 0.6% | +25.4% | -30.8% | -1.6% |

Geometric means: control2 vs main +1.7%, control vs control2 -2.0%, branch vs control -0.1%, branch vs main -0.5%

**lazy**

| Query | main | control2 | control | branch | main's rounds differ | control2 vs main | control vs control2 | branch vs control |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| query1 | 32.43 | 31.97 | 32.52 | 32.60 | 0.5% | -1.4% | +1.7% | +0.2% |
| query2 | 25.32 | 24.28 | 24.86 | 24.83 | 1.1% | -4.1% | +2.4% | -0.1% |
| query3 | 162.51 | 162.21 | 167.16 | 167.85 | 2.5% | -0.2% | +3.1% | +0.4% |
| query4 | 12.50 | 12.47 | 12.60 | 12.55 | 0.4% | -0.2% | +1.0% | -0.4% |
| query5 | 98.09 | 100.91 | 103.29 | 97.79 | 1.7% | +2.9% | +2.4% | -5.3% |
| query6 | 22.02 | 21.63 | 22.24 | 22.19 | 2.7% | -1.8% | +2.8% | -0.2% |
| query7 | 13.34 | 13.23 | 13.39 | 13.34 | 1.5% | -0.8% | +1.2% | -0.4% |
| query8 | 3.27 | 3.24 | 3.27 | 3.27 | 1.1% | -1.2% | +0.9% | +0.1% |
| query9 | 302.42 | 299.47 | 305.52 | 301.32 | 1.1% | -1.0% | +2.0% | -1.4% |
| query10 | 28.30 | 28.63 | 28.74 | 28.41 | 1.9% | +1.2% | +0.4% | -1.2% |
| query11 | 5.32 | 5.34 | 5.34 | 5.31 | 1.9% | +0.4% | +0.0% | -0.6% |
| query12 | 18.70 | 19.80 | 20.29 | 18.13 | 0.6% | +5.9% | +2.5% | -10.6% |
| query13 | 1.45 | 1.44 | 1.44 | 1.44 | 3.5% | -0.2% | -0.2% | +0.2% |
| query14 | 279.52 | 346.01 | 244.83 | 237.93 | 0.8% | +23.8% | -29.2% | -2.8% |

Geometric means: control2 vs main +1.5%, control vs control2 -1.0%, branch vs control -1.6%, branch vs main -1.2%

## Shapes no suite has (median ms; median of 3 processes)

**many_to_one_dense**

| Measure | main | control | branch | branch vs main | branch vs control | branch's processes differ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| construct_ms | 1689 | 790 | 744 | -56.0% | -5.9% | 0.1% |
| csr_build_ms | 1604 | 698 | 652 | -59.4% | -6.7% | 0.3% |
| engine_rss_mib | 1637 | 1528 | 1386 | -15.4% | -9.3% | 3.6% |
| peak_rss_mib | 2022 | 1912 | 1770 | -12.5% | -7.4% | 2.8% |
| fan_in_uniform | 138.49 | 105.46 | 75.01 | -45.8% | -28.9% | 2.4% |
| fan_in_hot_3pct | 124.36 | 96.26 | 64.00 | -48.5% | -33.5% | 4.2% |
| fan_in_selective | 4.73 | 3.45 | 2.39 | -49.5% | -30.7% | 2.6% |
| whole_relationship | 45.47 | 42.89 | 13.70 | -69.9% | -68.1% | 2.7% |
| reverse_1pct | 0.96 | 0.81 | 0.88 | -8.2% | +8.6% | 16.4% |
| point | 0.77 | 0.75 | 0.75 | -3.0% | -0.6% | 2.1% |
| fan_in_half_missing | 97.84 | 78.08 | 62.44 | -36.2% | -20.0% | 7.1% |

**many_to_one_random**

| Measure | main | control | branch | branch vs main | branch vs control | branch's processes differ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| construct_ms | 1430 | 789 | 795 | -44.4% | +0.8% | 1.0% |
| csr_build_ms | 1346 | 698 | 704 | -47.7% | +0.8% | 1.2% |
| engine_rss_mib | 1477 | 1420 | 1420 | -3.8% | +0.0% | 2.9% |
| peak_rss_mib | 1849 | 1791 | 1792 | -3.1% | +0.0% | 2.3% |
| fan_in_uniform | 136.32 | 107.98 | 110.14 | -19.2% | +2.0% | 0.7% |
| fan_in_hot_3pct | 122.06 | 95.79 | 94.35 | -22.7% | -1.5% | 2.1% |
| fan_in_selective | 4.82 | 3.54 | 3.51 | -27.1% | -0.9% | 1.2% |
| whole_relationship | 45.17 | 57.59 | 57.68 | +27.7% | +0.2% | 0.4% |
| reverse_1pct | 0.90 | 0.87 | 0.84 | -6.8% | -3.5% | 17.3% |
| point | 0.75 | 0.74 | 0.76 | +0.2% | +2.1% | 3.7% |

**many_to_many**

| Measure | main | control | branch | branch vs main | branch vs control | branch's processes differ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| construct_ms | 2426 | 1323 | 1331 | -45.1% | +0.6% | 0.2% |
| csr_build_ms | 2373 | 1265 | 1273 | -46.4% | +0.6% | 0.2% |
| engine_rss_mib | 2208 | 2134 | 2136 | -3.3% | +0.1% | 0.1% |
| peak_rss_mib | 2728 | 2654 | 2656 | -2.6% | +0.1% | 0.1% |
| point | 0.60 | 0.61 | 0.61 | +2.5% | +1.0% | 3.6% |
| one_hop_1pct | 4.33 | 2.98 | 2.92 | -32.6% | -2.1% | 10.2% |
| two_hop | 9.89 | 8.10 | 8.00 | -19.1% | -1.3% | 5.4% |
| three_hop | 2.17 | 2.04 | 2.01 | -7.2% | -1.4% | 4.8% |
| whole_relationship | 172.50 | 126.03 | 123.34 | -28.5% | -2.1% | 5.7% |

