issue-11-clean-r2-ic-lazy → issue-13-cancellation-release-ic-lazy

14 queries correct. Geometric mean speedup: 1.184× (above 1 is faster).

| Query | Before mean ms | After mean ms | After / before |
| --- | ---: | ---: | ---: |
| 1 | 38.072 | 38.387 | 1.008 |
| 2 | 32.860 | 32.541 | 0.990 |
| 3 | 509.083 | 522.179 | 1.026 |
| 4 | 15.518 | 16.366 | 1.055 |
| 5 | 83.840 | 82.162 | 0.980 |
| 6 | 30.515 | 23.044 | 0.755 |
| 7 | 35.384 | 36.003 | 1.018 |
| 8 | 3.899 | 3.994 | 1.024 |
| 9 | 1082.573 | 1144.331 | 1.057 |
| 10 | 121.840 | 122.753 | 1.007 |
| 11 | 8.866 | 6.812 | 0.768 |
| 12 | 189.851 | 28.097 | 0.148 |
| 13 | 6.177 | 6.206 | 1.005 |
| 14 | 77.378 | 71.807 | 0.928 |

Queries above 10% are candidates for repeat measurement, not established regressions.
Raw reports, software/build contexts, data hashes and commands are linked by the manifest.
