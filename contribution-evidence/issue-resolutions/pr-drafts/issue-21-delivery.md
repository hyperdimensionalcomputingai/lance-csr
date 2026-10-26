# Fix output aliases that shadow node bindings

Fixes #21. `MATCH (p:Person) RETURN p.name AS p ORDER BY p` now sorts the projected name. The planner resolves every RETURN expression before removing colliding node bindings, so `RETURN p.name AS p, p.id AS id` still reads the original node in the same projection. Whole-node projections keep their existing behavior. The Cypher guide documents this scope rule.

Proposed head `ccfc4fece4c1a099ab5b494f6a4533dfd03acc11` on `codex/issue-21-delivery`, compared against actual string/list parent `d44596967609bce35fad34acafe68753a2913383`. This submission changes only alias binding, its regression tests and the guide.

Validation:

- Full source suite: 599 Rust tests (9 ignored) and 760 Python tests passed, together with Clippy, formatting, Ruff, Pyright, executable docs, strict Zensical and pinned spelling.
- Optimized wheel: 65 installed tests passed on both CPython 3.13.17 and 3.12.9. Installed binary bytes match the wheel; typing parity and Arrow/Lance/CSR smoke pass with Rust absent from PATH.
- Direct reproduction on the actual parent wheel: 14 expected failures and four existing passes. The fixed wheel passes all 18 cases with identical test/fixture bytes and matched Python, PyArrow and Lance versions. Cases cover scalar/aggregate aliases, optional/null rows, case variants, nested sort functions, same-projection property access, WITH and whole-node projections in CSR and join modes.

Paired evidence: `/private/tmp/lance-csr-submissions/issue-21-paired-regression-proof.json`. Release proof, build context, consumer-input hashes and wheel: `/private/tmp/lance-csr-submissions/wheels/issue-21-delivery/`. Wheel SHA-256: `b5eb6c4d9519e3c3ed0b607e7d912763023afb8268c25c997b1002257716aff5`. Full check logs use `/private/tmp/lance-csr-runtime/issue-21-delivery-*.log`.

The alias patch is identical to historical `84ab2c2`; its prerequisite engine tree differs. The previous branches and evidence remain preserved. Matched main/parent SF1 comparison and the coordinator's clean-checkout gate remain separate. No other-platform or performance claim, push or PR is made here.

## Frozen review and acceptance

Head `ccfc4fece4c1a099ab5b494f6a4533dfd03acc11`; actual dependency base `d44596967609bce35fad34acafe68753a2913383`. The [release proof](../release-receipts/issue-21-delivery/release-proof.json) records wheel SHA-256 `b5eb6c4d9519e3c3ed0b607e7d912763023afb8268c25c997b1002257716aff5` and installed/archive binary identity. Its outside-checkout consumer passed 65 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| issue-20-string-list-delivery | Q30 | [1.017×](../comparisons-original/issue-21-delivery-vs-issue-20-string-list-delivery-q30-memory.md) | [0.970×](../comparisons-original/issue-21-delivery-vs-issue-20-string-list-delivery-q30-lazy.md) |
| issue-20-string-list-delivery | IC | [1.034×](../comparisons-original/issue-21-delivery-vs-issue-20-string-list-delivery-ic-memory.md) | [0.931×](../comparisons-original/issue-21-delivery-vs-issue-20-string-list-delivery-ic-lazy.md) |
| main-cp313 | Q30 | [1.007×](../comparisons-original/issue-21-delivery-vs-main-cp313-q30-memory.md) | [0.935×](../comparisons-original/issue-21-delivery-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [1.019×](../comparisons-original/issue-21-delivery-vs-main-cp313-ic-memory.md) | [0.953×](../comparisons-original/issue-21-delivery-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
