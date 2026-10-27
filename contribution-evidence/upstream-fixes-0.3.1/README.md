# Three upstream bug fixes: 0.3.1 publication evidence

All three fixes are committed and pushed on independent feature branches. Each is prepared for a separate PR in `hyperdimensionalcomputingai/lance-csr`, referencing the inherited upstream bugs. Nothing is merged and no release is published. The [current publication receipt](https://github.com/hyperdimensionalcomputingai/lance-csr/blob/codex/upstream-fix-evidence-031/contribution-evidence/upstream-fixes-0.3.1/PUBLISHED_PRS.md) records the resulting PR URLs and verified state.

| Upstream issue | Source branch / head | Debug and installed-wheel Python tests |
|---|---|---:|
| #123 | `codex/fix-aggregate-predicates` / `80d443888487` | 1,387 |
| #105 | `codex/fix-column-casing` / `e6df15716a05` | 1,393 |
| #101 | `codex/fix-trailing-semicolon` / `28096751241a` | 1,299 |

Tested base: `f5a986e822032a3aa8c52af55ad08e92504ee73c`. Observed main: `6effd0db0c002912cf969fd7447ea7a3ce2631df`. The later main commit changes documentation links and package URL metadata only: runtime source paths and all dependency locks are unchanged. Each PR applies cleanly to that main; the exact local integration checks and diff are inside the archive. The old 0.2.0 review package is historical and is not performance evidence for these new heads.

## What changed and what remains

- **#123:** protect empty-input distinct counts from DataFusion's unsafe statistics shortcut using a query-local frame. Keep the caller state, other optimizer rules, custom planner and deadline context. Existing aggregate AST traversal is reused from main.
- **#105:** bind graph scan fields to their physical Arrow spelling before applying normalized Cypher names, with shared schema lookup for inline filters and exact-field handling when reversing edges.
- **#101:** accept one optional final query semicolon; continue rejecting extra statements and repeated terminators.

After each fix its listed bug is resolved by that branch; after all three, no bug from this three-item scope remains. The feature requests deferred from this work remain **untyped variable-length relationships (#104), factorized intermediates (#163), optional Lance/DataFusion-only builds (#171), and JNI (#115)**. Fork PRs do not automatically close upstream issues.

Each code submission includes focused Rust/Python regressions and a Cypher guide update. Public Python signatures, stubs and dependencies are unchanged.

## Correctness and build policy

Every candidate passed debug workspace tests, Clippy with warnings denied, formatting, a rebuilt Python extension (`make install`), the complete Python suite, Ruff/Pyright, executable documentation snippets, strict Zensical and pinned typos 1.26.0. **All three debug gates passed before any final production build.** Future correctness checks follow that order: debug first across all proposed PRs, production only for final verification/measurements.

The exact new regression matrices against the verified 0.3.1 debug baseline produced 166 failures / 124 passes: #123 30/94, #105 104/26, #101 32/4. Candidate matrices pass 124, 130 and 36 cases respectively. Full installed-wheel suites pass the totals above; the independently built baseline passes 1,263 tests. Consumers run outside source checkouts with Rust absent from PATH, compare every packaged/installed payload and native digest, and reject packaged bytecode.

## Benchmark acceptance

All four release phases ran both full SF1 suites in memory and lazy modes, serially after builds and functional checks stopped. Original goldens and dataset manifests remain unchanged. Baseline results are reused across the three comparisons only within this newly verified cohort.

| Issue | Workload | Mode | First-pass geometric speedup | Acceptance |
|---|---|---|---:|---|
| #123 | q30 | memory | 0.924x | passed_performance_gate |
| #123 | q30 | lazy | 1.024x | passed_performance_gate |
| #123 | ic | memory | 0.994x | passed_performance_gate |
| #123 | ic | lazy | 0.999x | passed_performance_gate |
| #105 | q30 | memory | 0.989x | passed_performance_gate |
| #105 | q30 | lazy | 1.035x | passed_performance_gate |
| #105 | ic | memory | 0.958x | passed_performance_gate |
| #105 | ic | lazy | 0.971x | passed_performance_gate |
| #101 | q30 | memory | 1.000x | passed_performance_gate |
| #101 | q30 | lazy | 1.020x | passed_performance_gate |
| #101 | ic | memory | 1.004x | passed_performance_gate |
| #101 | ic | lazy | 1.021x | passed_performance_gate |

Above 1 is faster. These are unchanged first-pass suite means. Per-query flags and paired/precision investigations stay visible in [final-gates.json](final-gates.json); the [before/after tables](comparisons/) include every query. Unresolved repeatable regressions block readiness. No original observations are overwritten by selected-query checks.

Environment: Apple M4 Max, 16 cores, 128 GiB, macOS arm64; Rust 1.96.0 and Python 3.13.17 for all builds and consumers; identical pinned runtime dependencies and fixture manifests per suite. Workloads: 30-query `6260689` and IC `c185123`. Settings, source/wheel/native hashes, exact commands, hardware and peak process RSS are in the raw manifest and release receipts.

These are local engineering comparisons, not an audited SNB result or performance guarantee. Other operating-system/Python combinations and hosted CI are not qualified by these local runs. PR checks report hosted CI separately.

## Reproduce and inspect

Read [REPRODUCE.md](REPRODUCE.md), [STATUS.json](STATUS.json), [final-gates.json](final-gates.json) and the per-query tables. Download [evidence.tar.gz](evidence.tar.gz), check [its SHA-256](evidence.tar.gz.sha256), then verify the internal [artifact checksums](artifact-checksums.json). The archive contains raw checks, release receipts, benchmark reports/logs, exact scripts and protocols, locks, patches and the main-update check. It contains no datasets or native binaries. Original absolute paths are provenance; the instructions explain how to use independent paths.

Submitted by David Hughes (@stugorf), with AI Collaboration.
