# Count qualifying comprehension elements without building a list

Issue18. Proposed branch `codex/issue-18-curated` at `4bc6eeae6a6f0c71bb23559b3c81387a49970836`; actual comparison base `codex/issue-18-prerequisites` at `07a0fa1246c1829f4184e7d33c5fa063a4a76f19` (pagination90 plus alias-scope21).

A direct `size([x IN values WHERE predicate])` now counts the qualifying rows without collecting an intermediate list. The existing element stream, predicate and correlated EXISTS behavior remain responsible for membership and multiplicity. Empty/null lists produce zero; duplicate elements and duplicate outer rows retain their counts. Mapped comprehensions and pattern comprehensions keep normal list behavior.

The output remains Int64 with the same column name, values and multiplicity. The count result is now correctly marked non-nullable. Rust users constructing `LogicalOperator::ListComprehension` directly must set its new `count_only` field to false for ordinary list production; the migration is documented in the crate README.

This focused source uses the existing native cardinality function. It requires no cache14, math/string feature group, or partition15 option. The original combined scalar-function commit and b118562 integration fix are absent. Its prerequisite alias copy and standalone21 head are recorded in `issue-18-curated-lineage.json`.

Validation passed on the exact clean source: 592 Rust tests (9 ignored), 485 Python tests, 11 excluded native binding tests, 38 executable documentation blocks, workspace and binding Clippy with warnings denied, both Rust formatting checks, Ruff check/format, Pyright0 errors/warnings, strict docs and typos. Raw logs and hashes are indexed in `issue-18-curated-source-proof.json`. The immediate-parent optimized Python3.13 wheel has passed installed verification. Optimized head-wheel qualification also passed on Python3.13.17: all5 packaged files byte-verified after installation outside the checkout, no-Rust memory/Lance/CSR/shortestPath/refresh smoke, and 36 selected Python tests. See `wheels/issue-18-curated/release-proof.json`.

Tests compare the specialization with an explicit mapped-list reference over actual Arrow sources in CSR and join modes, including nulls, duplicate edges, duplicate outer rows, node lookup and EXISTS. Plan checks prove the eligible path removes array_agg while ineligible forms retain it. The benchmark adds an eligible/reference pair. The exact-source SF1 golden checks passed; measured IC10 and paired-repeat acceptance remain separate performance evidence.

Reproduction: use the repository build/test commands and the accompanying `focused-native-validation.md` recipe for the excluded PyO3 test executable. Tests need only generated local fixtures and the locked toolchain/dependencies.

## Frozen review and acceptance

Head `4bc6eeae6a6f0c71bb23559b3c81387a49970836`; actual dependency base `07a0fa1246c1829f4184e7d33c5fa063a4a76f19`. The [release proof](../release-receipts/issue-18-curated/release-proof.json) records wheel SHA-256 `d1f40e1f051d36d9fbe99e60b56a2220497d68b28867839f60f908aca304b3d0` and installed/archive binary identity. Its outside-checkout consumer passed 36 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| issue-18-prerequisites | Q30 | [0.999×](../comparisons-original/issue-18-curated-vs-issue-18-prerequisites-q30-memory.md) | [0.968×](../comparisons-original/issue-18-curated-vs-issue-18-prerequisites-q30-lazy.md) |
| issue-18-prerequisites | IC | [1.005×](../comparisons-original/issue-18-curated-vs-issue-18-prerequisites-ic-memory.md) | [1.046×](../comparisons-original/issue-18-curated-vs-issue-18-prerequisites-ic-lazy.md) |
| main-cp313 | Q30 | [0.988×](../comparisons-original/issue-18-curated-vs-main-cp313-q30-memory.md) | [0.934×](../comparisons-original/issue-18-curated-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [1.003×](../comparisons-original/issue-18-curated-vs-main-cp313-ic-memory.md) | [1.040×](../comparisons-original/issue-18-curated-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
