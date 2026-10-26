# Add scoped UNION and UNION ALL queries

Part of #20. Keep the tracker open for its remaining feature groups.

UNION branches have independent scopes and require matching output names, order and compatible Arrow types. UNION ALL preserves duplicates; UNION deduplicates complete rows. Branch-local ordering and pagination remain local. Numeric/null coercion and result nullability are determined before results are exposed, independent of batch arrival order.

The Cypher guide includes deterministic examples, scope rules and limits. The Rust README describes the public AST additions and exhaustive-match migration. Python signatures are unchanged. The finite submission diff contains UNION implementation and final output-schema collection; alias shadowing is its explicit prerequisite, and entity/fold functions remain on their independent branch.

Local branch `codex/issue-20-union-delivery`, proposed source `6e62a429da270649aa6877e81fc4a67df941bdc8`, compared against actual parent `ccfc4fece4c1a099ab5b494f6a4533dfd03acc11`. Checkout: `/private/tmp/lance-csr-issue-20-union-delivery`. Earlier source branches and immutable artifacts remain preserved.

Full validation on this exact source: **602 Rust tests** (9 ignored, 16 targets) and **797 Python tests** passed. Workspace all-target Clippy with `-D warnings`, Rust formatting, Ruff, Pyright, executable documentation, strict Zensical and pinned typos 1.26.0 passed. Logs: `/private/tmp/lance-csr-runtime/issue-20-union-delivery-*.log`. The debug extension was rebuilt from the qualified checkout using CPython 3.13.17/abi3-py3.12.

Focused coverage: Independent scopes; mixed UNION modes; duplicates and empty branches; incompatible names/counts/types; graph rows; repeated changing nullable parameters in collected and streamed results.

Reproduction: `cargo test --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo fmt --all --check`; from `python/`, `make install`, `make test` and `make lint`; from the root, `uv run --project docs --locked zensical build --strict` and `uvx typos@1.26.0 .`. Release packaging uses `maturin build --release --locked` from `python/` with an explicit interpreter and a clean isolated Cargo target.

## Frozen review and acceptance

Head `6e62a429da270649aa6877e81fc4a67df941bdc8`; actual dependency base `ccfc4fece4c1a099ab5b494f6a4533dfd03acc11`. The [release proof](../release-receipts/issue-20-union-delivery/release-proof.json) records wheel SHA-256 `b2da5cff27b6006b7b1de8068cdd937dbd62e423c5caaa1a7d4630f45b490db8` and installed/archive binary identity. Its outside-checkout consumer passed 102 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Keep the #20 tracker open until every approved finite feature group has passed acceptance. No full Cypher-conformance claim is made.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| issue-21-delivery | Q30 | [0.983×](../comparisons-original/issue-20-union-delivery-vs-issue-21-delivery-q30-memory.md) | [1.038×](../comparisons-original/issue-20-union-delivery-vs-issue-21-delivery-q30-lazy.md) |
| issue-21-delivery | IC | [0.986×](../comparisons-original/issue-20-union-delivery-vs-issue-21-delivery-ic-memory.md) | [1.084×](../comparisons-original/issue-20-union-delivery-vs-issue-21-delivery-ic-lazy.md) |
| main-cp313 | Q30 | [0.990×](../comparisons-original/issue-20-union-delivery-vs-main-cp313-q30-memory.md) | [0.970×](../comparisons-original/issue-20-union-delivery-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [1.004×](../comparisons-original/issue-20-union-delivery-vs-main-cp313-ic-memory.md) | [1.033×](../comparisons-original/issue-20-union-delivery-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
