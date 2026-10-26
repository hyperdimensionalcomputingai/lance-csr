issue-11-clean-r2-ic-memory → issue-17-ic-memory

14 queries correct. Geometric mean speedup: 1.071× (above 1 is faster).

| Query | Before mean ms | After mean ms | After / before |
| --- | ---: | ---: | ---: |
| 1 | 28.869 | 29.046 | 1.006 |
| 2 | 3.125 | 3.484 | 1.115 |
| 3 | 70.663 | 68.130 | 0.964 |
| 4 | 7.151 | 7.406 | 1.036 |
| 5 | 66.281 | 72.415 | 1.093 |
| 6 | 27.097 | 33.846 | 1.249 |
| 7 | 23.450 | 25.706 | 1.096 |
| 8 | 2.606 | 2.686 | 1.031 |
| 9 | 44.156 | 49.233 | 1.115 |
| 10 | 91.875 | 98.403 | 1.071 |
| 11 | 6.160 | 6.471 | 1.050 |
| 12 | 178.952 | 187.363 | 1.047 |
| 13 | 5.906 | 1.418 | 0.240 |
| 14 | 79.837 | 56.181 | 0.704 |

Queries above 10% are candidates for repeat measurement, not established regressions.
Raw reports, software/build contexts, data hashes and commands are linked by the manifest.
