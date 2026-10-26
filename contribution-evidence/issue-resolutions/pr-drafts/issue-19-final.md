# feat(graph): enforce cooperative query deadlines

Relates to #19. Adds keyword-only `timeout=None` in seconds to `CypherEngine.execute`, `execute_batches` and `CypherQuery.execute`. One monotonic deadline includes planning and pauses between reads. Zero expires immediately; negative, nonfinite and unrepresentable values raise `ValueError`.

Native `pyarrow.RecordBatchReader` is preserved. Initial expiry raises `TimeoutError`; expired reads raise `ArrowInvalid` with `Query timeout exceeded`. A watchdog releases owned streams during consumer idle time. Close/drop and ordinary errors also release resources. Timed contexts install a private DataFusion join guard, including rejected nested-loop candidates; ordinary contexts remain reusable.

Raw CSR scan quanta count tombstones and suppressed edges, missing source rows and empty frontier nodes. Local counters and synchronous bounded scan helpers avoid a coroutine-state update on every edge. The [failing-before/passing-after proof](../correction-proofs/cancellation-paired-inputs-r1.json) uses byte-identical deterministic real-state tests at bug-fix commit `b84e628`. The final performance tuning changes the fixed-hop test bound to 8,192 and extends its tombstone fixture to 20,000 edges; the full final-head suite verifies those scenarios again.

Head `a641cf25cbe34db25245ce2543140af5b735d30e`, branch `codex/issue-19-scan-budget`; frozen base `81ef524be83db4abb626394dd4940121a45751e2` remains unchanged. Iterative path-specific cancellation corrections belong to #13; the combined candidate verifies both together with CALL. No extra public prepared-query or cancellation API is introduced.

## Validation and documentation

The [exact-source receipt](../source-receipts/issue-19-scan-budget-source-proof.json) records 602 Rust tests, 487 Python tests, 15 separate binding tests and 39 executable documentation blocks. Formatting, workspace/binding Clippy, Ruff, Pyright, strict Zensical and pinned spelling passed. Tests cover expiry before/during/between reads, idle ownership release, partial results, close/drop, stopped tasks, invalid values, ordinary errors and engine reuse.

The [release receipt](../release-receipts/issue-19-scan-budget-release/release-proof.json) records archive/installed binary identity, strict installed typing and all 487 Python tests on both 3.13 and 3.12 outside the checkout, with no Rust on PATH. API reference, stubs, examples and guides match the new keyword.

Fixed-hop scan quanta use 8,192 raw candidates or missing source rows, with tests for rejected output, long tombstone runs and empty sources. This matches the default output chunk and retains a fixed cancellation bound.

The [controlled CPU proof](../correction-proofs/timeout-cpu-proof.json) catches the original background-work leak and passes all three corrected representative trials. That join guard is unchanged by the later scan-quantum corrections. Its opt-in normal-query sample measured 18.9% paired geometric mean overhead (0.399 ms disabled, 0.469 ms enabled). This is a local fixture, not a universal overhead guarantee. The final owner passes both complete suites in memory and lazy modes. Its initial repeat investigation retained one parent IC6 memory flag; a prospectively fixed 32-pair study has after/before ratio 0.942 with 95% interval [0.906, 0.978], while the unchanged-parent control includes equality. The [precision report](../repeat-investigations/scan-budget-ic6-precision/summary.json) retains the initial failed gate, all runs and the method/provenance correction. Other parent and main gates pass. The accepted combined build passes correlated CALL cleanup. Integrating the larger budget at `891d8d7` caused a repeated lazy Q30 slowdown (ratios 1.127 and 1.178), so that variant is rejected. Coordinated integration retains the stronger 1,024-work bound and matching tests in qualified source `7d2ef3e`. Keep this PR as a draft until the actual target uses that explicitly tested resolution; the standalone 8,192-work tuning is not a public API requirement. See the [integration record](../INTEGRATION.md).

Timeouts are cooperative: foreign blocking calls and synchronous parser work are not hard-preemptible. The two external benchmark runners remain read/run only; their adoption is an outstanding acceptance item. This submission must keep #19 open.

Submitted by David Hughes (@stugorf), with AI Collaboration.
