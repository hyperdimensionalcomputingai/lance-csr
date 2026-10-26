# feat(graph): enforce cooperative query deadlines

Relates to #19. Adds keyword-only `timeout=None` in seconds to `CypherEngine.execute`, `execute_batches` and `CypherQuery.execute`. One monotonic deadline includes planning and pauses between reads. Zero expires immediately; negative, nonfinite and unrepresentable values raise `ValueError`.

Native `pyarrow.RecordBatchReader` is preserved. Initial expiry raises `TimeoutError`; expired reads raise `ArrowInvalid` with `Query timeout exceeded`. A watchdog releases owned streams during consumer idle time. Close/drop and ordinary errors also release resources. Timed contexts install a private DataFusion join guard, including rejected nested-loop candidates; ordinary contexts remain reusable.

Raw CSR scan quanta count tombstones and suppressed edges, missing source rows and empty frontier nodes. Local counters and synchronous bounded scan helpers avoid a coroutine-state update on every edge. The [failing-before/passing-after proof](../correction-proofs/cancellation-paired-inputs-r1.json) uses identical deterministic real-state tests.

Head `b84e62866aa94effa07bb0742a4b26c3dc83c9d5`, branch `codex/issue-19-local-cursor-quanta`; frozen base `81ef524be83db4abb626394dd4940121a45751e2` remains unchanged. Iterative path-specific cancellation corrections belong to #13; the combined candidate verifies both together with CALL. No extra public prepared-query or cancellation API is introduced.

## Validation and documentation

The [exact-source receipt](../source-receipts/issue-19-empty-work-source-proof.json) records 602 Rust tests, 487 Python tests, 15 separate binding tests and 39 executable documentation blocks. Formatting, workspace/binding Clippy, Ruff, Pyright, strict Zensical and pinned spelling passed. Tests cover expiry before/during/between reads, idle ownership release, partial results, close/drop, stopped tasks, invalid values, ordinary errors and engine reuse.

The [release receipt](../release-receipts/issue-19-empty-work-release/release-proof.json) records archive/installed binary identity, strict installed typing and all 487 Python tests on both 3.13 and 3.12 outside the checkout, with no Rust on PATH. API reference, stubs, examples and guides match the new keyword.

The [controlled CPU proof](../correction-proofs/timeout-cpu-proof.json) catches the original background-work leak and passes all three corrected representative trials. That join guard is unchanged by the later scan-quantum corrections. Its opt-in normal-query sample measured 18.9% paired geometric mean overhead (0.399 ms disabled, 0.469 ms enabled). This is a local fixture, not a universal overhead guarantee. Final full-suite/paired acceptance and combined CALL cleanup proof are running; see the [review status](README.md).

Timeouts are cooperative: foreign blocking calls and synchronous parser work are not hard-preemptible. The two external benchmark runners remain read/run only; their adoption is an outstanding acceptance item. This submission must keep #19 open.

Submitted by David Hughes (@stugorf), with AI Collaboration.
