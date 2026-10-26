Title: fix(cypher): preserve integer-list precision

Fixes #3.

Integer-only list literals now retain Int64 values, including IDs above float32's exact range. Decimal or exponent notation still denotes a vector; vector operations convert integer lists at their boundary. A single-row logical source also lets a configured engine execute standalone RETURN expressions.

The Cypher and vector guides explain literal types and executable examples. No Python signature changes. Rust callers matching every LogicalOperator variant must handle SingleRow.

Verified locally at f71fe3c481b5c8599c24e904d163b8867eae29af:

- The precision regression suite fails against the exact main release wheel: 24 failed, 18 passed (also reproduced against the published release). The candidate passes all 42 tests, including integer types, large IDs, comprehensions, vector metrics, standalone returns and pagination.
- Full Python suite: 374 passed. Rust library: 338 passed. The full workspace, all-target Clippy, Python lint/type checks, strict docs, executable snippets and pinned spelling checks pass. Exact commands and results are recorded in issue-3-* logs.
- A release wheel passes all 42 regressions and the memory/Lance/CSR/shortest-path/refresh smoke test outside the checkout, without Rust on PATH. Installed extension bytes match the wheel. See wheels/issue-3/release-proof.json for the wheel hash and logs.

## Frozen review and acceptance

Head `f71fe3c481b5c8599c24e904d163b8867eae29af`; actual dependency base `54e02307a48e467a98d9da2183f5adcb767e7c99`. The [release proof](../release-receipts/issue-3-cp313/release-proof.json) records wheel SHA-256 `5cf6d0beb0610d9c2c852d6cd02359a2deffb6430bf048dabc85a6e48a071b37` and installed/archive binary identity. Its outside-checkout consumer passed 42 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| main-cp313 | Q30 | [1.007×](../comparisons-original/issue-3-cp313-vs-main-cp313-q30-memory.md) | [0.923×](../comparisons-original/issue-3-cp313-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [0.982×](../comparisons-original/issue-3-cp313-vs-main-cp313-ic-memory.md) | [1.014×](../comparisons-original/issue-3-cp313-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
