# perf(graph): search shortest paths from both ends

Resolves #17. A source with one target in an input batch uses complete-layer bidirectional search when reverse adjacency is available. The smaller frontier expands first. The two predecessor graphs join at one depth cut, retaining every shortest alternative and the multiplicity of parallel edges.

Sources with multiple targets retain the shared forward search. Directed queries without a CSR retain the existing forward adjacency build; undirected queries can search both ends of their symmetric adjacency. No Python API or relationship-alternation semantics change. This submission depends on #11 (local base e127755).

The guide documents the selected search and shortest-path tie behavior. Rust tests compare every directed graph on three nodes, all endpoint pairs and hop bounds, in both directed and undirected mode. Separate cases cover multiple meeting nodes, duplicate relationships, self-loops, cycles, odd/even path lengths and a wide middle layer that need not be expanded. Python tests exercise outgoing, incoming and undirected all-shortest queries and shared-source queries, with and without CSR.

## Validation

- Passed: isolated `cargo test --locked -p lance-graph --lib shortest_path::tests`: 4 tests. Log: `/private/tmp/lance-csr-runtime/issue-17-rust-isolated.log`.
- Passed: `cargo fmt --all --check`; changed Python test passes Ruff lint and format checks; `git diff --check`.
- Passed: `uv run --project docs --locked zensical build --strict` and `uvx typos@1.26.0 .`.
- Passed: isolated rebuilt extension; `python -m pytest -q python/tests/test_engine_paths.py python/tests/test_engine_csr.py`: 106 tests (`issue-17-python-tests.log`).
- Passed: isolated `cargo test --workspace --locked` and `cargo clippy --workspace --all-targets -- -D warnings` (`issue-17-workspace-tests.log`, `issue-17-clippy.log`).
- Passed: `make lint`; `python ../docs/check_snippets.py csr.md cypher.md`: all 13 affected guide blocks.
- Passed: standalone full Python suite: 341 tests (`/private/tmp/lance-csr-runtime/issue-17-python-full.log`).
- Passed: binding-manifest release clean and rebuild of all three local crates at `09a92e4`, installed extension byte match, real Arrow/Lance/CSR/shortest/refresh smoke outside the source tree without Rust, and 118 installed-wheel tests. Build Python 3.13.17; consumer Python 3.12.9. Wheel SHA256 `20b5c6cb724d846ff2f54649f9773db6a5e85ee51ae5b8321aeb371a04bba9c8`; immutable proof `/private/tmp/lance-csr-submissions/wheels/issue-17/release-proof.json`. Final combined integration reproduction remains separate.
- Pending: same-input correctness and before/after timings for IC13, IC14 and both required GDC SF1 suites, with manifests/checksums and hardware/settings recorded. The wide-layer unit test establishes visited-node behavior, not a measured speed claim.

## Frozen review and acceptance

Head `09a92e424bd8a7dffc0c3ff6a82838dc088de59e`; actual dependency base `e127755c872949733c69435047384c9d17fb572b`. The [release proof](../release-receipts/issue-17/release-proof.json) records wheel SHA-256 `20b5c6cb724d846ff2f54649f9773db6a5e85ee51ae5b8321aeb371a04bba9c8` and installed/archive binary identity. Its outside-checkout consumer passed 118 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. The measured regression gate remains held; the [draft index](README.md) and [gate ledger](../review-readiness-original.json) identify the remaining CSR or corrected-owner checks. Passing correctness does not waive a repeated slowdown above 10%. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Submitted by David Hughes (@stugorf), with AI Collaboration.
