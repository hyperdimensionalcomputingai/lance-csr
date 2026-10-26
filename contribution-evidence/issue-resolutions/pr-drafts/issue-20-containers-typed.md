# feat(cypher): support typed list and map parameters

Relates to #20. Python list/map parameters now convert recursively through the existing expression representation. Large integer IDs, nested containers, nulls and empty structs retain their values. Unsupported objects, non-string map keys, overflowing integers and recursive containers fail at the Python boundary.

Regression cases exercise RETURN, IN, UNWIND, map properties, comprehensions, integer vectors, streaming and `CypherQuery.parameters()`. Public stubs and strict consumer examples use recursive input/result types. The Cypher, API and vector guides document the existing homogeneous Arrow-list limitation.

Frozen head `89f96a422a6a2e566b68496269f63b6f00ef908c` compares with `4b157120db78fad823e494deea2dba5c82729f59`, the explicit integer-literal/stub prerequisite. Only container conversion, recursive typing, tests and documentation belong to this diff. The exact source passed the full 411-test Python suite and its Rust/static/documentation checks. The optimized wheel passed all 411 tests outside the checkout, installed-binary hash checks, strict consumer typing and the no-Rust Arrow/Lance/CSR smoke.

Release identity: `../release-receipts/issue-20-containers-curated/release-proof.json`. Complete Q30/IC golden checks passed in memory and lazy modes; final paired-repeat acceptance and performance tables must be linked before publication. The #20 tracker remains open for its other finite feature groups.

## Frozen review and acceptance

Head `89f96a422a6a2e566b68496269f63b6f00ef908c`; actual dependency base `4b157120db78fad823e494deea2dba5c82729f59`. The [release proof](../release-receipts/issue-20-containers-curated/release-proof.json) records wheel SHA-256 `958433fba7164a7539326bed7589044502eeb374b77d1cdcb9ba415d03cc1aac` and installed/archive binary identity. Its outside-checkout consumer passed 411 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Keep the #20 tracker open until every approved finite feature group has passed acceptance. No full Cypher-conformance claim is made.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| issue-20-parameter-prerequisites | Q30 | [0.995×](../comparisons-original/issue-20-containers-curated-vs-issue-20-parameter-prerequisites-q30-memory.md) | [1.031×](../comparisons-original/issue-20-containers-curated-vs-issue-20-parameter-prerequisites-q30-lazy.md) |
| issue-20-parameter-prerequisites | IC | [0.997×](../comparisons-original/issue-20-containers-curated-vs-issue-20-parameter-prerequisites-ic-memory.md) | [1.012×](../comparisons-original/issue-20-containers-curated-vs-issue-20-parameter-prerequisites-ic-lazy.md) |
| main-cp313 | Q30 | [0.980×](../comparisons-original/issue-20-containers-curated-vs-main-cp313-q30-memory.md) | [0.942×](../comparisons-original/issue-20-containers-curated-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [0.987×](../comparisons-original/issue-20-containers-curated-vs-main-cp313-ic-memory.md) | [1.012×](../comparisons-original/issue-20-containers-curated-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
