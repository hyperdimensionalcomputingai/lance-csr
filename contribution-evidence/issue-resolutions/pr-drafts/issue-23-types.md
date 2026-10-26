# feat(cypher): reject known incompatible expression types

Uses catalog schemas and the existing semantic scopes to validate arithmetic and comparisons before execution. Errors identify the expression and operand types, surfacing as Python `ValueError`. Supported numeric promotion, null operands and temporal combinations remain valid; unresolved types are left to execution rather than rejected speculatively.

The Cypher guide and API reference describe the validation. Real Arrow-schema regressions cover numeric/temporal compatibility, established mismatches, alias/WITH scope propagation and nulls. This submission stacks the compatible mapping accessors; privacy and validated deserialization remain staged for the next breaking release.

Verified release: `1939b6948792ed84eecd0914ff413da792ac2875`; wheel SHA-256 `6ec09a58dbb3ce6a7568cbf5b8e36abdccbefbd419f23c6f8da8d2b860ac7419`. Its clean log and build log confirm all three local crates were rebuilt from that source. The outside-checkout smoke runs without Rust on PATH; the installed extension matches the archive. 16 focused tests passed. Source/runtime/hash evidence: `wheels/issue-23-types/release-proof.json` and `build-context.json`.

## Frozen review and acceptance

Head `1939b6948792ed84eecd0914ff413da792ac2875`; actual dependency base `96598edac33e9c8dab8bf3b0ec1324308faf589e`. The [release proof](../release-receipts/issue-23-types/release-proof.json) records wheel SHA-256 `6ec09a58dbb3ce6a7568cbf5b8e36abdccbefbd419f23c6f8da8d2b860ac7419` and installed/archive binary identity. Its outside-checkout consumer passed 16 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Mapping-field privacy and validated deserialization remain held for the next breaking release; this PR must not close #23.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| issue-23-accessors | Q30 | [0.985×](../comparisons-original/issue-23-types-vs-issue-23-accessors-q30-memory.md) | [1.006×](../comparisons-original/issue-23-types-vs-issue-23-accessors-q30-lazy.md) |
| issue-23-accessors | IC | [1.040×](../comparisons-original/issue-23-types-vs-issue-23-accessors-ic-memory.md) | [0.987×](../comparisons-original/issue-23-types-vs-issue-23-accessors-ic-lazy.md) |
| main-cp313 | Q30 | [0.964×](../comparisons-original/issue-23-types-vs-main-cp313-q30-memory.md) | [0.944×](../comparisons-original/issue-23-types-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [0.990×](../comparisons-original/issue-23-types-vs-main-cp313-ic-memory.md) | [1.005×](../comparisons-original/issue-23-types-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
