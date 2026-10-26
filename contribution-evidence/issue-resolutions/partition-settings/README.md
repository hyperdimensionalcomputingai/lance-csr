# DataFusion partition evidence
#15 retains the current default. Every alternative worsened at least one complete workload; repeat checks confirm several large individual regressions. One partition improves Q30-memory by24-25% on two new paired repeats, supporting optional user tuning.

Ratios are candidate/default mean time; lower is faster. All528 matrix query checks and all60 repeat cells passed their immutable goldens.

|Partitions|Round-robin|Q30 memory|Q30 lazy|IC memory|IC lazy|
|---|---|---:|---:|---:|---:|
|default|True|1.000|1.000|1.000|1.000|
|default|False|0.850|1.033|0.979|1.115|
|1|True|0.786|1.017|1.127|1.291|
|1|False|0.791|1.014|1.138|1.278|
|2|True|0.868|1.021|1.111|1.125|
|2|False|0.809|0.972|1.150|1.232|

## Repeated regressions
|Suite|Mode|Partitions|Round-robin|Query|New pair1|New pair2|
|---|---|---|---|---|---:|---:|
|q30|lazy|default|False|test_benchmark_query30|8.000|9.156|
|ic|lazy|1|True|test_benchmark_query5|5.179|5.636|
|ic|memory|1|True|test_benchmark_query5|5.014|5.285|
|q30|lazy|1|True|test_benchmark_query30|10.193|8.703|
|ic|lazy|1|False|test_benchmark_query5|5.251|4.779|
|ic|memory|1|False|test_benchmark_query5|5.005|5.116|
|q30|lazy|1|False|test_benchmark_query30|9.163|9.445|
|ic|lazy|2|True|test_benchmark_query5|2.822|3.002|
|ic|memory|2|True|test_benchmark_query5|3.829|3.675|
|q30|lazy|2|True|test_benchmark_query30|4.914|4.694|
|ic|lazy|2|False|test_benchmark_query5|3.127|3.280|
|ic|memory|2|False|test_benchmark_query5|4.001|4.038|
|q30|lazy|2|False|test_benchmark_query30|10.988|9.054|

Complete per-query means/medians: `per-query.csv`; machine-readable settings and repeats: `summary.json`. All source, wheel, dataset, interpreter and toolchain hashes remain in the referenced immutable manifests.

Representative scan:500000 rows with4KiB properties;36 fresh processes all passed row/count/checksum/payload checks. Original1000000-row memory setup failed Arrow i32 offset validation before query. A mistyped checksum in the first500000-row harness was caught by assertions; both failed attempts are preserved and excluded. Corrected expected rows166667/checksum41666583333. See `../issue-15-large-scan-results-80aa203-r3-500k/summary.json` and original failure logs.

No default change, adaptive scheduler or public round-robin option. Other settings/query flags without paired repeats remain unverified. This experiment does not substitute for final-source main/parent submission comparisons.

Submitted by David Hughes (@stugorf), with AI Collaboration.
