# fix(graph): prevent stale concurrent refresh publication

Concurrent `refresh()` calls could publish an older replacement after a newer snapshot. Replacements are built outside the state lock and published only when the starting `Arc` snapshot is still current; otherwise refresh retries from the new snapshot. Snapshot identity avoids incorrectly treating equal dataset version numbers as equal engine state.

Existing readers retain their original snapshot and failed refresh preserves the previous state. The Lance guide and API reference document retries. There is no public signature change.

A deterministic barrier-controlled binding test drives the actual engine state through the stale-publication race. Lance fixtures cover refresh, failed refresh, old readers and configured query results. The Python release consumer runs `test_engine_lance.py`; the private binding test is exercised separately because its crate is excluded from the Rust workspace.

Verified release: `58b96fb601eda5f02caff0dc537c1ffa0f18bd15`; wheel SHA-256 `9a382c7f8c18b0b5dd54ab23c36a17eea7aa79f7fcd1cfbed044111ec49fdb3c`. Its clean log and build log confirm all three local crates were rebuilt from that source. The outside-checkout smoke runs without Rust on PATH; the installed extension matches the archive. 22 focused tests passed. Source/runtime/hash evidence: `wheels/issue-12/release-proof.json` and `build-context.json`.

## Frozen review and acceptance

Head `58b96fb601eda5f02caff0dc537c1ffa0f18bd15`; actual dependency base `54e02307a48e467a98d9da2183f5adcb767e7c99`. The [release proof](../release-receipts/issue-12/release-proof.json) records wheel SHA-256 `9a382c7f8c18b0b5dd54ab23c36a17eea7aa79f7fcd1cfbed044111ec49fdb3c` and installed/archive binary identity. Its outside-checkout consumer passed 22 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| main-cp313 | Q30 | [0.964×](../comparisons-original/issue-12-vs-main-cp313-q30-memory.md) | [0.947×](../comparisons-original/issue-12-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [0.972×](../comparisons-original/issue-12-vs-main-cp313-ic-memory.md) | [0.984×](../comparisons-original/issue-12-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
