# feat(graph): enforce cooperative query deadlines

Relates to #19. Adds keyword-only `timeout=None` in seconds to `CypherEngine.execute`, `execute_batches` and `CypherQuery.execute`. One monotonic deadline includes planning and pauses between reader calls. Zero expires immediately; negative, nonfinite and unrepresentable values raise `ValueError`.

The native Arrow reader is preserved. Initial execution/planning expiry raises `TimeoutError`; reading an expired stream raises `ArrowInvalid` with `Query timeout exceeded`. A watchdog releases the owned stream while the consumer is idle. Close/drop and ordinary execution errors release resources. Custom traversals yield after bounded work; foreign blocking calls and synchronous parser work remain cooperative limits.

Source `0189c99aac4de1ff7ea85a9032bd9824f965f1b8`, actual dependency base `81ef524be83db4abb626394dd4940121a45751e2`. The base and head both have verified optimized wheels. The exact head passed 487 Python tests, 15 separate binding tests and its documented source/static checks. Installed release tests and no-Rust smoke passed on Python 3.13 and 3.12; proof and supplementary runtime receipt are in `wheels/issue-19-corrected/`. A real DataFusion CAST failure with a retained Arrow buffer fails before the error-cleanup correction and passes after it.

API reference, Cypher guide, stubs, strict typing examples and timeout regressions accompany the change. Complete main/parent SF1 tables, measured cancellation/normal-query overhead and final repeat acceptance are linked from the submission evidence bundle when finalized.

The two external benchmark repositories remain read/run only. Adopting their `QUERY_TIMEOUT_SECONDS` option is a remaining acceptance item, so this submission must not automatically close #19. No push, PR or merge has occurred.

The corrected head passed 599 Rust workspace tests, all 487 Python tests, 15 separately linked binding tests and 39 executable documentation blocks, with all required static/docs checks. Timed contexts install a private physical join guard; ordinary engine contexts remain reusable. Raw adjacency candidates retain bounded cancellation even through tombstones.

The [controlled CPU proof](../correction-proofs/timeout-cpu-proof.json) catches the earlier background-work leak and passes all three corrected trials. With a 50 ms deadline, observed return delay was 0.026–0.065 ms and own-process CPU during each subsequent 250 ms idle interval was 0.038–0.080 ms after a 50 ms grace. This is a representative local stress proof, not universal hard preemption. Opt-in normal-query timing had paired geometric mean overhead of 18.9%; sample means were 0.399 ms without and 0.469 ms with a deadline. Full workload and repeat gates still hold IC6 and inherited CSR flags for correction.

## Frozen review and acceptance

Head `0189c99aac4de1ff7ea85a9032bd9824f965f1b8`; actual dependency base `81ef524be83db4abb626394dd4940121a45751e2`. The [release proof](../release-receipts/issue-19-corrected/release-proof.json) records wheel SHA-256 `69e3f9bff73f4096143d47f7b61d46e967ed3c14a22cd6a84a5b0f15d64fa28c` and installed/archive binary identity. Its outside-checkout consumer passed 487 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. The measured regression gate remains held; the [draft index](README.md) and [gate ledger](../review-readiness-original.json) identify the remaining CSR or corrected-owner checks. Passing correctness does not waive a repeated slowdown above 10%. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

External benchmark-runner adoption remains outside the read/run-only boundary; this PR must not close #19.

Submitted by David Hughes (@stugorf), with AI Collaboration.
