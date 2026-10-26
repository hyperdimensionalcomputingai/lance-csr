# Add numeric scalar functions with strict Cypher types

Part of #20. Keep the tracker open for its remaining feature groups.

This submission adds `abs`, `round`, `floor`, `ceil` and `sqrt`. DataFusion kernels handle matching semantics; Cypher rounding keeps ties toward positive infinity. Numeric output widths and argument kinds are explicit. Integer `abs` rejects values outside Int64; the floating-point operations accept unsigned inputs by conversion to Float64. The Cypher guide documents nulls, non-finite values, rounding and overflow.

It also defines the SQL export boundary for custom scalar implementations: runtime-only calls remaining after constant folding raise the existing `NotImplementedError`, while constant folding and faithful native functions remain exportable. Typed null constants retain their Arrow type in the Default SQL dialect through the existing native `arrow_cast` function; other dialects reject these typed-null exports explicitly. Bare `null` remains untyped. The SQL guide, native docstring and manual API reference describe this observable behavior. Existing list `size` and two-argument `range` retain their native SQL export.

Local branch `codex/issue-20-math-delivery`, proposed source `c53f47c2304f09f48019e0d3facdc7ece62c3a83`, compared against actual parent `90c141a0979bea1291baba501d49677a8dabb516`. Checkout: `/private/tmp/lance-csr-issue-20-math-delivery`. Earlier source branches and immutable artifacts remain preserved.

Full validation on tracked tree 4e3826d8c804ba07c727d56bde8164f972d9d3e3, verified identical to this head with git diff --exit-code: **594 Rust tests** (9 ignored, 16 targets) and **573 Python tests** passed. Workspace all-target Clippy with `-D warnings`, Rust formatting, Ruff, Pyright, executable documentation, strict Zensical and pinned typos 1.26.0 passed. Logs: `/private/tmp/lance-csr-runtime/issue-20-math-qualified-*.log`. The debug extension was rebuilt from the qualified checkout using CPython 3.13.17/abi3-py3.12.

Focused coverage: Literals and real nullable Arrow columns; narrow integer/Float32 inputs; unsigned values above Int64; NaN/infinity and signed zero; half-rounding boundaries; invalid arity/kinds; real Cypher versus SqlEngine values and Arrow types; typed-null default-dialect round trips; rejection in other dialects; unchanged native Unicode and list/range SQL behavior.

An optimized wheel was built from this exact source with fixed CPython 3.13.17 after cleaning all three local crates in an isolated target. Build logs confirm the source paths. Outside-checkout consumers passed **125 tests on CPython 3.13.17** and **125 passed on CPython 3.12.9**. The installed extension bytes match the archive, typing files and runtime/stub parity checks pass, and the repository wheel smoke exercises Arrow memory, Lance disk, CSR traversal, shortestPath and refresh with Rust absent from PATH.

Wheel SHA-256: `0bdcda659d4a2b223e241fc1c49ffb752504679aec0d2ec0b30dc3043d7503ef`. Source/build/consumer proof and actual interpreter/PyO3 configuration: `/private/tmp/lance-csr-submissions/wheels/issue-20-math-delivery/`. These are functional release checks, not performance measurements.

Reproduction: `cargo test --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo fmt --all --check`; from `python/`, `make install`, `make test` and `make lint`; from the root, `uv run --project docs --locked zensical build --strict` and `uvx typos@1.26.0 .`. Release packaging uses `maturin build --release --locked` from `python/` with an explicit interpreter and a clean isolated Cargo target.

## Frozen review and acceptance

Head `c53f47c2304f09f48019e0d3facdc7ece62c3a83`; actual dependency base `90c141a0979bea1291baba501d49677a8dabb516`. The [release proof](../release-receipts/issue-20-math-delivery/release-proof.json) records wheel SHA-256 `0bdcda659d4a2b223e241fc1c49ffb752504679aec0d2ec0b30dc3043d7503ef` and installed/archive binary identity. Its outside-checkout consumer passed 125 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Keep the #20 tracker open until every approved finite feature group has passed acceptance. No full Cypher-conformance claim is made.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| issue-20-pagination-curated | Q30 | [1.033×](../comparisons-original/issue-20-math-delivery-vs-issue-20-pagination-curated-q30-memory.md) | [1.026×](../comparisons-original/issue-20-math-delivery-vs-issue-20-pagination-curated-q30-lazy.md) |
| issue-20-pagination-curated | IC | [0.941×](../comparisons-original/issue-20-math-delivery-vs-issue-20-pagination-curated-ic-memory.md) | [1.036×](../comparisons-original/issue-20-math-delivery-vs-issue-20-pagination-curated-ic-lazy.md) |
| main-cp313 | Q30 | [0.996×](../comparisons-original/issue-20-math-delivery-vs-main-cp313-q30-memory.md) | [0.971×](../comparisons-original/issue-20-math-delivery-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [0.955×](../comparisons-original/issue-20-math-delivery-vs-main-cp313-ic-memory.md) | [1.045×](../comparisons-original/issue-20-math-delivery-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
