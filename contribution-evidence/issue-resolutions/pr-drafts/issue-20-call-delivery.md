# feat(cypher): support scoped read-only CALL subqueries

Relates to #20. `CALL { ... RETURN ... }` supports a leading plain importing WITH, separate UNION branch scopes and exported values. Each input row evaluates its child query; every child result extends that row. Empty inner results, aggregates, duplicate inputs, ordering and name collisions follow the tested scope and multiplicity rules.

The operator preserves entity/path values, streams one child at a time and yields after bounded polling work, including empty batches. Drop, errors and timeout release owned child streams. Procedures, YIELD and writes remain unsupported. Guides include executable examples, cache behavior, execution strategy and migration for Rust `ReadingClause::Call` and `LogicalOperator::{Apply, CallInput}`. Python signatures are unchanged.

Frozen head `f2cc84c64898185bf8ea4826b276b61ea8b20b63` compares with `32fee35e2aad15167a7d33ec0d9eee9b3d268b5b`. That base contains the documented language, cache/type, traversal, path and deadline prerequisites. This diff owns CALL parsing, scope, planning, Apply/CallInput execution, necessary path/cache demand and regressions. The partition override and count-only comprehension specialization have separate submissions.

Exact clean-source validation passed 641 Rust workspace tests, 1,183 Python tests, 16 separately linked binding tests and all formatting, Clippy, Ruff, Pyright, executable-doc, strict Zensical and pinned spelling checks. The optimized wheel passed all 1,183 Python tests outside the checkout, archive/installed identity, strict consumer typing and no-Rust smoke. Tests cover importing-WITH restrictions, nested/UNION scopes, zero/multiple child rows, optional aggregation, output schema, correlations, aliases, cache bindings, rich paths, cancellation and child-stream ownership.

Source proof: `../source-receipts/issue-20-call-delivery-source-proof.json`. Release proof: `../release-receipts/issue-20-call-delivery-release/release-proof.json`. Complete Q30/IC golden checks passed in both modes; immediate-parent repeat gates pass; coordinated integration remains required. Keep the #20 tracker open until every finite feature group meets its acceptance checks.

## Frozen review and acceptance

Head `f2cc84c64898185bf8ea4826b276b61ea8b20b63`; actual dependency base `32fee35e2aad15167a7d33ec0d9eee9b3d268b5b`. The [release proof](../release-receipts/issue-20-call-delivery-release/release-proof.json) records wheel SHA-256 `c036ec112f6f7ecb62bb9532f50dcae900f46600af4a6b6a55ba2a5e27439136` and installed/archive binary identity. Its outside-checkout consumer passed 1183 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. The frozen head retains the recorded main-comparison prerequisite flags; immediate-parent gates pass. The qualified combined source applies the shared CSR and compact path-null corrections. This remains a coordinated draft until its actual target matches the [integration record](../INTEGRATION.md); original frozen-head flags are not relabeled as passing. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Keep the #20 tracker open until every approved finite feature group has passed acceptance. No full Cypher-conformance claim is made.

Submitted by David Hughes (@stugorf), with AI Collaboration.
