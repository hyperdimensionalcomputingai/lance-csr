# refactor(graph): expose read-only mapping accessors

Adds read-only node/relationship mapping iterators and migrates engine/planner/binding callers to them. Rust users can inspect validated mappings without depending on direct field access. Existing public fields and serialized names remain compatible in this submission; the Rust crate README documents the migration.

Mapping normalization and lookup tests retain the existing builder invariants. Packaging smoke covers the Python API and refresh. This compatible stage does not claim that direct mutation/deserialization can no longer bypass invariants. The private-field and validated-deserialization change is implemented on a separate next-breaking-release branch and remains a closure dependency for#23.

Verified release: `96598edac33e9c8dab8bf3b0ec1324308faf589e`; wheel SHA-256 `1602445499c3c1c01766741a7c5f80af33d8364ef6b9173bba6c981e3bd0ffde`. Its clean log and build log confirm all three local crates were rebuilt from that source. The outside-checkout smoke runs without Rust on PATH; the installed extension matches the archive. 3 focused tests passed. Source/runtime/hash evidence: `wheels/issue-23-accessors/release-proof.json` and `build-context.json`.

## Frozen review and acceptance

Head `96598edac33e9c8dab8bf3b0ec1324308faf589e`; actual dependency base `54e02307a48e467a98d9da2183f5adcb767e7c99`. The [release proof](../release-receipts/issue-23-accessors/release-proof.json) records wheel SHA-256 `1602445499c3c1c01766741a7c5f80af33d8364ef6b9173bba6c981e3bd0ffde` and installed/archive binary identity. Its outside-checkout consumer passed 3 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Mapping-field privacy and validated deserialization remain held for the next breaking release; this PR must not close #23.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| main-cp313 | Q30 | [0.978×](../comparisons-original/issue-23-accessors-vs-main-cp313-q30-memory.md) | [0.938×](../comparisons-original/issue-23-accessors-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [0.952×](../comparisons-original/issue-23-accessors-vs-main-cp313-ic-memory.md) | [1.019×](../comparisons-original/issue-23-accessors-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
