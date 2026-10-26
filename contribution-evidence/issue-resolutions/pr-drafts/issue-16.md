# perf(csr): reuse filter columns during target materialization

Resolves #16's duplicate property reads within a CSR output batch. Target columns already read for a pushed-down predicate are filtered once and reused when the projection also needs them. Only the remaining properties are fetched for surviving rows. Target IDs are reconstructed from neighbour values even when a predicate reads the ID.

Existing projection pruning and Lance's row sorting/deduplication remain responsible for their current work. This adds no persistent property cache, dependency, Python API or configuration. Reuse ends with the output batch and retains the query's immutable snapshot. The guide describes that scope, and the existing CSR benchmark gains a filter-plus-projection case.

## Evidence and validation

- Read-only exact-main release-wheel IC9 diagnostic: 1,397,943 candidate messages; 197,085 survivors of the date predicate; 20 final rows. The two message expansion nodes both filter and project `creationDate`. Final result SHA-256: `7466c490da461fc2ab2c388a674a8f2e484cee1e3201cc7ad75cf87a1c6877ee`.
- Reproduction script: `/private/tmp/lance-csr-submissions/profile_ic9.py`; baseline plan/result: `ic9-main-profile.plan.txt`, `ic9-main-profile.result.json`; raw log: `/private/tmp/lance-csr-runtime/issue-16-main-profile.log`. The count variants are separate queries. Their elapsed times are diagnostic only, not an additive phase profile or a controlled benchmark.
- Passed: isolated `cargo test --locked -p lance-graph --lib csr_expand::tests`: 11 tests. The new case performs real Arrow and Lance reads with a test-only read-cell counter, checking duplicate/out-of-order IDs, null predicates, empty results, overlapping/non-overlapping/empty projections, and unused columns. Six candidate age values plus four surviving names require ten requested cells; IDs and repeated age reads require none.
- Passed: changed Python files pass Ruff and AST checks; `make lint`; strict Zensical build and pinned typos check.
- Passed: isolated rebuilt extension; `python -m pytest -q python/tests`: 351 tests; complete `cargo test --workspace --locked` and `cargo clippy --workspace --all-targets -- -D warnings`. Logs: `issue-16-python-tests.log`, `issue-16-workspace-tests.log`, `issue-16-clippy.log` under `/private/tmp/lance-csr-runtime/`.
- Passed: `python ../docs/check_snippets.py csr.md`: five guide blocks.
- Passed: changed-build IC9 candidate count, survivor count and complete result hash exactly match main. Changed artifacts: `/private/tmp/lance-csr-submissions/ic9-issue-16-profile.plan.txt`, `ic9-issue-16-profile.result.json`; raw log: `/private/tmp/lance-csr-runtime/issue-16-changed-profile.log`. The changed build is debug and main is release, so their elapsed times must not be compared.
- Passed: `python benchmarks/csr_expand.py --data /private/tmp/lance-csr-submissions/issue-16-micro-data --edges 10000 --repeats 2`; CSR/join counts and checksums agree, including 15 filtered-name rows with checksum 58. Raw output: `/private/tmp/lance-csr-runtime/issue-16-micro-smoke.json`. This is a functional harness smoke test, not a representative timing result.
- Pending: exact-main versus final-release IC9 and both required GDC SF1 suites under identical quiescent conditions, and clean-checkout reproduction. No timing or disk-byte reduction claim is made from requested-cell counts. The branch is not yet submission-ready.

This submission depends on #11 and #13 (local base 1bf6d42); local commit 447355d.

Release qualification: clean source `447355d` was rebuilt after a successful binding-manifest `cargo clean --release` of all three local crates. Its installed Python 3.12 extension matched the wheel bytes and passed Arrow/Lance/CSR/shortest-path/refresh smoke checks with Rust absent from PATH plus 159 focused tests. Build interpreter: Python 3.13.17; wheel tag: `cp312-abi3-macosx_11_0_arm64`. Proof: `/private/tmp/lance-csr-submissions/wheels/issue-16/release-proof.json`; wheel SHA256 `ffb2c78cff997addc80e290af87219c3edfbcf94a1214b162fb6cbac329dbfea`. Both complete SF1 workloads passed their unchanged golden checks in memory and lazy modes; paired-repeat acceptance remains open.

## Frozen review and acceptance

Head `447355dbaf90993d33926afc48ab6712e421505a`; actual dependency base `1bf6d42141b9cc6ee1824d000f49cdc0ebd9d8fe`. The [release proof](../release-receipts/issue-16/release-proof.json) records wheel SHA-256 `ffb2c78cff997addc80e290af87219c3edfbcf94a1214b162fb6cbac329dbfea` and installed/archive binary identity. Its outside-checkout consumer passed 159 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. The measured regression gate remains held; the [draft index](README.md) and [gate ledger](../review-readiness-original.json) identify the remaining CSR or corrected-owner checks. Passing correctness does not waive a repeated slowdown above 10%. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Keep #16 open if the measured property-read/latency improvement is insufficient.

Submitted by David Hughes (@stugorf), with AI Collaboration.
