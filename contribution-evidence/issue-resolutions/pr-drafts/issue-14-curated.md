# Reuse scalar query templates within each engine snapshot

Issue14. Proposed branch `codex/issue-14-curated` at `74dd1e5d9ad680b79da40428e2f7e2cc18eb6671`; actual comparison base `codex/issue-14-prerequisites` at `248def899cea569f57e44fa447ad9ccf5f8f5eab`.

Repeated compatible scalar queries now reuse parsing, semantic analysis, graph logical planning and an unoptimized DataFusion logical template. Each execution binds its current values before optimization and physical planning. A bounded128-entry cache belongs to the engine snapshot; successful refresh creates a new cache and active readers retain the old snapshot. Concurrent executions keep parameter bindings separate.

The key distinguishes scalar types and null. Container parameters, parameterized pagination and other shape-sensitive forms explicitly use fresh planning. This introduces no public Python prepared-query API and does not reuse optimized or physical plans. Output names for unaliased parameters stay stable.

Prerequisites include the corrected typed-container/pagination foundation90, refresh12, nonbreaking config accessors23 and schema-aware type checking23. The qualified standalone12/23 heads and their cherry-picked copies are distinguished in `issue-14-curated-lineage.json`; their separate wheels are not substitutes for this combined comparison base.

All core, catalog, binding and benchmark source is byte-identical to the previously working dbef1ea implementation. The focused diff excludes prerequisite stubs, container and pagination feature changes. The refresh-test import repair remains in14 because14 extracted the shared test fixture; original58 and prerequisite248 already import it correctly.

Validation passed on the exact clean source: 596 Rust tests (10 ignored), 490 Python tests, 13 excluded native binding tests, 38 executable documentation blocks, workspace and binding Clippy with warnings denied, both Rust formatting checks, Ruff check/format, Pyright0 errors/warnings, strict docs and typos. Raw logs and hashes are indexed in `issue-14-curated-source-proof.json`. The immediate-parent optimized Python3.13 wheel has passed installed verification. Optimized head-wheel qualification also passed on Python3.13.17: all5 packaged files byte-verified after installation outside the checkout, no-Rust memory/Lance/CSR/shortestPath/refresh smoke, and 57 selected Python tests. See `wheels/issue-14-curated/release-proof.json`.

The benchmark harness measures fresh planning, reusable stages, parameter rebinding and execution separately. Complete SF1 main/parent measurements and golden checks have finished; paired-repeat acceptance is complete. No performance gain or issue closure is claimed from unit tests.

Reproduction: use the repository build/test commands and the accompanying `focused-native-validation.md` recipe for the excluded PyO3 test executable. Tests need only generated local fixtures and the locked toolchain/dependencies.

## Frozen review and acceptance

Head `74dd1e5d9ad680b79da40428e2f7e2cc18eb6671`; actual dependency base `248def899cea569f57e44fa447ad9ccf5f8f5eab`. The [release proof](../release-receipts/issue-14-curated/release-proof.json) records wheel SHA-256 `bb54cdd8536282b6ccfaf2e7db986297c0f6b7cd810c86b4e1bc66b9e93a6914` and installed/archive binary identity. Its outside-checkout consumer passed 57 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| issue-14-prerequisites | Q30 | [1.056×](../comparisons-original/issue-14-curated-vs-issue-14-prerequisites-q30-memory.md) | [1.051×](../comparisons-original/issue-14-curated-vs-issue-14-prerequisites-q30-lazy.md) |
| issue-14-prerequisites | IC | [0.999×](../comparisons-original/issue-14-curated-vs-issue-14-prerequisites-ic-memory.md) | [1.012×](../comparisons-original/issue-14-curated-vs-issue-14-prerequisites-ic-lazy.md) |
| main-cp313 | Q30 | [1.029×](../comparisons-original/issue-14-curated-vs-main-cp313-q30-memory.md) | [0.990×](../comparisons-original/issue-14-curated-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [1.015×](../comparisons-original/issue-14-curated-vs-main-cp313-ic-memory.md) | [1.042×](../comparisons-original/issue-14-curated-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
