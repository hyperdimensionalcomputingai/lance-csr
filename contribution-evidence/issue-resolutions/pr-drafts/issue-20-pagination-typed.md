# feat(cypher): bind parameters in SKIP and LIMIT

Relates to #20. SKIP/LIMIT accept nonnegative integral parameters wherever those clauses already exist, including WITH LIMIT. Missing, null, fractional, negative and overflowing values fail explicitly. Row counts that affect plan shape bypass template reuse.

Parser, semantic, Rust execution and Python tests cover valid counts, lexical scope and rejected forms. The Cypher/API guides include executable examples. The Rust README documents the public RowCount AST migration for downstream exhaustive matches; Python method signatures are unchanged.

Frozen head `90c141a0979bea1291baba501d49677a8dabb516` compares with typed-container head `89f96a422a6a2e566b68496269f63b6f00ef908c`. The exact source passed its Rust workspace, all-target Clippy, 455 Python tests, lint/types, strict documentation, executable snippets and pinned spelling checks. Its optimized wheel passed all 455 tests outside the source checkout, binary/archive identity, typing and no-Rust smoke checks.

Release identity: `../release-receipts/issue-20-pagination-curated/release-proof.json`. Complete Q30/IC golden checks passed in memory and lazy modes; final paired-repeat acceptance and performance tables must be linked before publication. The #20 tracker remains open for its other finite feature groups.

## Frozen review and acceptance

Head `90c141a0979bea1291baba501d49677a8dabb516`; actual dependency base `89f96a422a6a2e566b68496269f63b6f00ef908c`. The [release proof](../release-receipts/issue-20-pagination-curated/release-proof.json) records wheel SHA-256 `df06959072c6d95c3366d596673fde2d5ac12d26e66cf23bcbbaa557f4589180` and installed/archive binary identity. Its outside-checkout consumer passed 455 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Keep the #20 tracker open until every approved finite feature group has passed acceptance. No full Cypher-conformance claim is made.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| issue-20-containers-curated | Q30 | [0.984×](../comparisons-original/issue-20-pagination-curated-vs-issue-20-containers-curated-q30-memory.md) | [1.004×](../comparisons-original/issue-20-pagination-curated-vs-issue-20-containers-curated-q30-lazy.md) |
| issue-20-containers-curated | IC | [1.028×](../comparisons-original/issue-20-pagination-curated-vs-issue-20-containers-curated-ic-memory.md) | [0.996×](../comparisons-original/issue-20-pagination-curated-vs-issue-20-containers-curated-ic-lazy.md) |
| main-cp313 | Q30 | [0.965×](../comparisons-original/issue-20-pagination-curated-vs-main-cp313-q30-memory.md) | [0.946×](../comparisons-original/issue-20-pagination-curated-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [1.014×](../comparisons-original/issue-20-pagination-curated-vs-main-cp313-ic-memory.md) | [1.008×](../comparisons-original/issue-20-pagination-curated-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
