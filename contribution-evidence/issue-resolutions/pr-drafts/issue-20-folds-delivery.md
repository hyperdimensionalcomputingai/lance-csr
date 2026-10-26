# Add ordered reduce and list predicates

Part of #20. Keep the tracker open for its remaining feature groups.

`reduce` evaluates each item in order with explicit accumulator and iterator scope. `all`, `any`, `none` and `single` implement three-valued truth rules for null lists, null items, null predicate results and empty lists. Expressions can capture outer scalar/struct values and nest scalar folds. Graph predicates reuse relational list-comprehension planning; correlation preserves input multiplicity and null equality without changing collected-list order.

The Cypher guide gives executable examples and the scalar reduce-body boundary. Graph comprehensions and aggregates are supported as list inputs; they are rejected inside a reduce body requiring accumulator-dependent graph evaluation. Official IC14 also needs the separate path-value submission. The Rust README documents public `ValueExpression` additions and migration for exhaustive matches/visitors. Python signatures are unchanged.

Local branch `codex/issue-20-folds-delivery`, proposed source `88dd97e8baa87cc76dbd968b5761252be3bf60ec`, compared against actual parent `99d11c0ea0e72ecf592d65165a47101ce281ca3a`. Checkout: `/private/tmp/lance-csr-issue-20-folds-delivery`. Earlier source branches and immutable artifacts remain preserved.

Full validation on this exact source: **601 Rust tests** (9 ignored, 16 targets) and **858 Python tests** passed. Workspace all-target Clippy with `-D warnings`, Rust formatting, Ruff, Pyright, executable documentation, strict Zensical and pinned typos 1.26.0 passed. Logs: `/private/tmp/lance-csr-runtime/issue-20-folds-delivery-*.log`. The debug extension was rebuilt from the qualified checkout using CPython 3.13.17/abi3-py3.12.

Focused coverage: Ordered arithmetic/string accumulation; lexical shadowing and nested folds; captured structs/scalars; complete null/empty truth tables; graph predicates; invalid scope/type cases; explicit unsupported-body errors.

An optimized wheel was built from this exact source with fixed CPython 3.13.17 after cleaning all three local crates in an isolated target. Build logs confirm the source paths. Outside-checkout consumers passed **131 tests on CPython 3.13.17** and **131 passed on CPython 3.12.9**. The installed extension bytes match the archive, typing files and runtime/stub parity checks pass, and the repository wheel smoke exercises Arrow memory, Lance disk, CSR traversal, shortestPath and refresh with Rust absent from PATH.

Wheel SHA-256: `272d18ce93742f46eed21fc04819f98379f91a9f44141f7940102578dc51af02`. Source/build/consumer proof and actual interpreter/PyO3 configuration: `/private/tmp/lance-csr-submissions/wheels/issue-20-folds-delivery/`. These are functional release checks, not performance measurements.

Reproduction: `cargo test --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo fmt --all --check`; from `python/`, `make install`, `make test` and `make lint`; from the root, `uv run --project docs --locked zensical build --strict` and `uvx typos@1.26.0 .`. Release packaging uses `maturin build --release --locked` from `python/` with an explicit interpreter and a clean isolated Cargo target.

## Frozen review and acceptance

Head `88dd97e8baa87cc76dbd968b5761252be3bf60ec`; actual dependency base `99d11c0ea0e72ecf592d65165a47101ce281ca3a`. The [release proof](../release-receipts/issue-20-folds-delivery/release-proof.json) records wheel SHA-256 `272d18ce93742f46eed21fc04819f98379f91a9f44141f7940102578dc51af02` and installed/archive binary identity. Its outside-checkout consumer passed 131 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Keep the #20 tracker open until every approved finite feature group has passed acceptance. No full Cypher-conformance claim is made.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| issue-20-entities-delivery | Q30 | [0.992×](../comparisons-original/issue-20-folds-delivery-vs-issue-20-entities-delivery-q30-memory.md) | [1.033×](../comparisons-original/issue-20-folds-delivery-vs-issue-20-entities-delivery-q30-lazy.md) |
| issue-20-entities-delivery | IC | [0.967×](../comparisons-original/issue-20-folds-delivery-vs-issue-20-entities-delivery-ic-memory.md) | [0.983×](../comparisons-original/issue-20-folds-delivery-vs-issue-20-entities-delivery-ic-lazy.md) |
| main-cp313 | Q30 | [0.987×](../comparisons-original/issue-20-folds-delivery-vs-main-cp313-q30-memory.md) | [0.987×](../comparisons-original/issue-20-folds-delivery-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [0.980×](../comparisons-original/issue-20-folds-delivery-vs-main-cp313-ic-memory.md) | [1.021×](../comparisons-original/issue-20-folds-delivery-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
