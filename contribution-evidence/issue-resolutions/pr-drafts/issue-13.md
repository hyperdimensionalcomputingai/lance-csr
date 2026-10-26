# perf(csr): traverse variable-length paths iteratively

Resolves #13. Eligible variable-length CSR paths now use one expansion operator for the complete hop range. A depth-first cursor retains one frame per hop, yields bounded output batches and cooperates with the executor while rejecting candidates. Intermediate nodes carry both label and key; missing intermediate nodes are dropped as on the join path, and duplicate edges retain their multiplicity.

Mixed-type paths such as Tag → hasType → TagClass → isSubclassOf → TagClass retain their type choices until execution, instead of unrolling every length and type sequence. Ordinary alternation and unsupported index shapes retain the existing fallback. The 20-hop limit and existing undirected immediate-backtracking rule remain; full relationship uniqueness on longer cycles remains outside scope.

Zero-hop ranges now match the source identity in both indexed and join paths. Different labels with the same integer key do not match. A regression also fixes branch label propagation for zero-hop paths inside a union.

The traversal reuses CSR projection pruning, final target filtering, target reads, snapshot ownership and error propagation. No dependencies or Python API signatures change. Rust callers constructing `LogicalOperator::VariableLengthExpand` directly must now provide its `target_label` field. This submission depends on #11 (base e127755); local commit 1bf6d42.

## Validation

- Passed: isolated `cargo test --workspace --locked`; 341 library unit tests plus workspace integration and documentation tests. Log: `/private/tmp/lance-csr-runtime/issue-13-workspace-tests.log`.
- Passed: rebuilt isolated extension; `python -m pytest -q python/tests`: 347 tests in 15.81 seconds. Log: `/private/tmp/lance-csr-runtime/issue-13-python-tests.log`.
- Passed: `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all --check`, `make lint`, and `git diff --check`.
- Passed: `uv run --project docs --locked zensical build --strict`, `uvx typos@1.26.0 .`, and `python ../docs/check_snippets.py csr.md cypher.md` (13 guide blocks).
- Regression evidence includes CSR/join parity, directed and undirected paths, duplicate edges, cycles and self-loops, null/dangling endpoints, mixed-label alternation with overlapping IDs, zero-hop identity and non-identity, cooperative yielding without matching output, and exactly one physical expansion at hop limits 2, 5 and 20.
- Pending: IC12 and both required GDC SF1 suites against exact main with identical inputs/settings, representative memory and latency measurements, dataset manifests/checksums, and clean-checkout reproduction. Operator-count and bounded-cursor assertions are not a timing or process-memory claim. The branch is not yet submission-ready while these checks remain pending.

Release qualification: clean source `1bf6d42` was rebuilt after a successful binding-manifest `cargo clean --release` of all three local crates. Its installed Python 3.12 extension matched the wheel bytes and passed Arrow/Lance/CSR/shortest-path/refresh smoke checks with Rust absent from PATH plus 155 focused tests, including typed variable-length alternation. Build interpreter: Python 3.13.17; wheel tag: `cp312-abi3-macosx_11_0_arm64`. Proof: `/private/tmp/lance-csr-submissions/wheels/issue-13/release-proof.json`; wheel SHA256 `f0128f457a9193be373148c6429f233a624ec03a506beb3bf9c819f375c34804`. Both complete SF1 workloads passed their unchanged golden checks in memory and lazy modes; paired-repeat acceptance remains open.

## Frozen review and acceptance

Head `1bf6d42141b9cc6ee1824d000f49cdc0ebd9d8fe`; actual dependency base `e127755c872949733c69435047384c9d17fb572b`. The [release proof](../release-receipts/issue-13/release-proof.json) records wheel SHA-256 `f0128f457a9193be373148c6429f233a624ec03a506beb3bf9c819f375c34804` and installed/archive binary identity. Its outside-checkout consumer passed 155 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. The measured regression gate remains held; the [draft index](README.md) and [gate ledger](../review-readiness-original.json) identify the remaining CSR or corrected-owner checks. Passing correctness does not waive a repeated slowdown above 10%. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Submitted by David Hughes (@stugorf), with AI Collaboration.
