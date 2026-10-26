# Add Cypher string and list scalar functions

Part of #20. Keep the tracker open for its remaining feature groups.

This submission adds `left`, `right`, `substring`, `replace`, `split`, `trim`/`lTrim`/`rTrim`, string/list `reverse`, `last`, `tail`, outer list `size` and optional `range` steps. It handles Unicode characters, null/empty inputs and strict argument kinds. Unsigned Arrow range arguments outside Int64 raise an error instead of becoming null.

Faithful native list-size and inclusive-range lowering preserves SQL export. Runtime-only string/list implementations use the inherited explicit SQL export boundary; folded typed-null constants retain their exact Arrow types in the Default dialect. The Cypher and SQL guides contain examples and limits. Python signatures are unchanged.

Local branch `codex/issue-20-string-list-delivery`, proposed source `d44596967609bce35fad34acafe68753a2913383`, compared against actual parent `c53f47c2304f09f48019e0d3facdc7ece62c3a83`. Checkout: `/private/tmp/lance-csr-issue-20-string-list-delivery`. Earlier source branches and immutable artifacts remain preserved.

Full validation on this exact source: **598 Rust tests** (9 ignored, 16 targets) and **742 Python tests** passed. Workspace all-target Clippy with `-D warnings`, Rust formatting, Ruff, Pyright, executable documentation, strict Zensical and pinned typos 1.26.0 passed. Logs: `/private/tmp/lance-csr-runtime/issue-20-string-list-delivery-*.log`. The debug extension was rebuilt from the qualified checkout using CPython 3.13.17/abi3-py3.12.

Focused coverage: Unicode and nullable Arrow inputs; empty strings/lists; invalid lengths, arity and kinds; inclusive/descending ranges; real Arrow overflow in all three range positions; CSR/join parity; SQL equality for List/LargeList/FixedSizeList and nested outer length; eleven typed-null string/list SQL round trips.

An optimized wheel was built from this exact source with fixed CPython 3.13.17 after cleaning all three local crates in an isolated target. Build logs confirm the source paths. Outside-checkout consumers passed **295 tests on CPython 3.13.17** and **295 passed on CPython 3.12.9**. The installed extension bytes match the archive, typing files and runtime/stub parity checks pass, and the repository wheel smoke exercises Arrow memory, Lance disk, CSR traversal, shortestPath and refresh with Rust absent from PATH.

Wheel SHA-256: `cd5b5d48a924582e6482df4221e77bf34e982f1c48ec112740d578aa587251b6`. Source/build/consumer proof and actual interpreter/PyO3 configuration: `/private/tmp/lance-csr-submissions/wheels/issue-20-string-list-delivery/`. These are functional release checks, not performance measurements.

Reproduction: `cargo test --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo fmt --all --check`; from `python/`, `make install`, `make test` and `make lint`; from the root, `uv run --project docs --locked zensical build --strict` and `uvx typos@1.26.0 .`. Release packaging uses `maturin build --release --locked` from `python/` with an explicit interpreter and a clean isolated Cargo target.

## Frozen review and acceptance

Head `d44596967609bce35fad34acafe68753a2913383`; actual dependency base `c53f47c2304f09f48019e0d3facdc7ece62c3a83`. The [release proof](../release-receipts/issue-20-string-list-delivery/release-proof.json) records wheel SHA-256 `cd5b5d48a924582e6482df4221e77bf34e982f1c48ec112740d578aa587251b6` and installed/archive binary identity. Its outside-checkout consumer passed 295 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Keep the #20 tracker open until every approved finite feature group has passed acceptance. No full Cypher-conformance claim is made.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| issue-20-math-delivery | Q30 | [0.994×](../comparisons-original/issue-20-string-list-delivery-vs-issue-20-math-delivery-q30-memory.md) | [0.992×](../comparisons-original/issue-20-string-list-delivery-vs-issue-20-math-delivery-q30-lazy.md) |
| issue-20-math-delivery | IC | [1.032×](../comparisons-original/issue-20-string-list-delivery-vs-issue-20-math-delivery-ic-memory.md) | [0.980×](../comparisons-original/issue-20-string-list-delivery-vs-issue-20-math-delivery-ic-lazy.md) |
| main-cp313 | Q30 | [0.990×](../comparisons-original/issue-20-string-list-delivery-vs-main-cp313-q30-memory.md) | [0.963×](../comparisons-original/issue-20-string-list-delivery-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [0.985×](../comparisons-original/issue-20-string-list-delivery-vs-main-cp313-ic-memory.md) | [1.024×](../comparisons-original/issue-20-string-list-delivery-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
