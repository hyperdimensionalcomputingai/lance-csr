issue-11-clean-r2-ic-memory → issue-13-cancellation-release-ic-memory

14 queries correct. Geometric mean speedup: 1.273× (above 1 is faster).

| Query | Before mean ms | After mean ms | After / before |
| --- | ---: | ---: | ---: |
| 1 | 28.869 | 32.396 | 1.122 |
| 2 | 3.125 | 3.340 | 1.069 |
| 3 | 70.663 | 51.596 | 0.730 |
| 4 | 7.151 | 6.826 | 0.955 |
| 5 | 66.281 | 54.020 | 0.815 |
| 6 | 27.097 | 21.514 | 0.794 |
| 7 | 23.450 | 23.151 | 0.987 |
| 8 | 2.606 | 2.586 | 0.993 |
| 9 | 44.156 | 36.942 | 0.837 |
| 10 | 91.875 | 91.490 | 0.996 |
| 11 | 6.160 | 4.529 | 0.735 |
| 12 | 178.952 | 21.316 | 0.119 |
| 13 | 5.906 | 5.786 | 0.980 |
| 14 | 79.837 | 71.559 | 0.896 |

Queries above 10% are candidates for repeat measurement, not established regressions.
Raw reports, software/build contexts, data hashes and commands are linked by the manifest.
