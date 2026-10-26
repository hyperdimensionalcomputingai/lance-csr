# Add map/entity keys and node labels

Part of #20. Keep the tracker open for its remaining feature groups.

`keys` returns all map keys, including keys whose values are null, and non-null exposed property names for graph entities. Exposed identifiers and relationship endpoints remain part of that property set. `labels` returns the canonical configured node label and preserves WITH aliases and optional-match nulls. Mixed-label bindings fail explicitly instead of returning an incorrect union of labels.

The submission diff contains only map/entity function planning, validation, tests and documentation. The Cypher guide documents the existing single-label boundary and includes executable examples. Python signatures are unchanged.

Local branch `codex/issue-20-entities-delivery`, proposed source `99d11c0ea0e72ecf592d65165a47101ce281ca3a`, compared against actual parent `d44596967609bce35fad34acafe68753a2913383`. Checkout: `/private/tmp/lance-csr-issue-20-entities-delivery`. Earlier source branches and immutable artifacts remain preserved.

Full validation on this exact source: **599 Rust tests** (9 ignored, 16 targets) and **774 Python tests** passed. Workspace all-target Clippy with `-D warnings`, Rust formatting, Ruff, Pyright, executable documentation, strict Zensical and pinned typos 1.26.0 passed. Logs: `/private/tmp/lance-csr-runtime/issue-20-entities-delivery-*.log`. The debug extension was rebuilt from the qualified checkout using CPython 3.13.17/abi3-py3.12.

Focused coverage: Maps and null fields; node/relationship keys; optional entities; WITH alias preservation; configured label spelling; invalid kinds; explicit mixed-label rejection; CSR/join parity.

An optimized wheel was built from this exact source with fixed CPython 3.13.17 after cleaning all three local crates in an isolated target. Build logs confirm the source paths. Outside-checkout consumers passed **79 tests on CPython 3.13.17** and **79 passed on CPython 3.12.9**. The installed extension bytes match the archive, typing files and runtime/stub parity checks pass, and the repository wheel smoke exercises Arrow memory, Lance disk, CSR traversal, shortestPath and refresh with Rust absent from PATH.

Wheel SHA-256: `c403020283095e6480a4a4e768076ce965313951324b87d43379c04320008ffd`. Source/build/consumer proof and actual interpreter/PyO3 configuration: `/private/tmp/lance-csr-submissions/wheels/issue-20-entities-delivery/`. These are functional release checks, not performance measurements.

Reproduction: `cargo test --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo fmt --all --check`; from `python/`, `make install`, `make test` and `make lint`; from the root, `uv run --project docs --locked zensical build --strict` and `uvx typos@1.26.0 .`. Release packaging uses `maturin build --release --locked` from `python/` with an explicit interpreter and a clean isolated Cargo target.

## Frozen review and acceptance

Head `99d11c0ea0e72ecf592d65165a47101ce281ca3a`; actual dependency base `d44596967609bce35fad34acafe68753a2913383`. The [release proof](../release-receipts/issue-20-entities-delivery/release-proof.json) records wheel SHA-256 `c403020283095e6480a4a4e768076ce965313951324b87d43379c04320008ffd` and installed/archive binary identity. Its outside-checkout consumer passed 79 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Keep the #20 tracker open until every approved finite feature group has passed acceptance. No full Cypher-conformance claim is made.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| issue-20-string-list-delivery | Q30 | [1.005×](../comparisons-original/issue-20-entities-delivery-vs-issue-20-string-list-delivery-q30-memory.md) | [0.992×](../comparisons-original/issue-20-entities-delivery-vs-issue-20-string-list-delivery-q30-lazy.md) |
| issue-20-string-list-delivery | IC | [1.030×](../comparisons-original/issue-20-entities-delivery-vs-issue-20-string-list-delivery-ic-memory.md) | [1.015×](../comparisons-original/issue-20-entities-delivery-vs-issue-20-string-list-delivery-ic-lazy.md) |
| main-cp313 | Q30 | [0.995×](../comparisons-original/issue-20-entities-delivery-vs-main-cp313-q30-memory.md) | [0.955×](../comparisons-original/issue-20-entities-delivery-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [1.014×](../comparisons-original/issue-20-entities-delivery-vs-main-cp313-ic-memory.md) | [1.039×](../comparisons-original/issue-20-entities-delivery-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
