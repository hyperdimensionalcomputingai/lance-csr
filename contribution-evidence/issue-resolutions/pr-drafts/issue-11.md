# fix(csr): stream high-degree expansion in bounded chunks

Resolves #11. CSR expansion now yields each completed output chunk before visiting the remainder of a high-degree input row. It retains one input batch and a pair of neighbour offsets per partition instead of collecting every neighbour and every output batch first.

Deleted edge occurrences are resolved to immutable snapshot offsets during refresh, removing the per-query copy of each source's tombstone map. Duplicate edges, target filtering/lookups, closing edges and old-reader snapshots retain their existing semantics. Refreshes with deletions now scan each affected source's neighbour list; this trade-off needs the recorded refresh and workload measurements below.

Documentation explains which memory is bounded and which remains outside this operator. The existing memory benchmark gains a deterministic wide-property hub case; it checks output count/checksum and records first-batch latency, pre-query RSS, total peak RSS and peak growth as a lower bound.

## Validation

- Passed: pinned `cargo fmt --all --check`; changed Python files pass Ruff lint and formatting checks; `git diff --check`.
- Passed: `uv run --project docs --locked zensical build --strict`.
- Passed: `uvx typos@1.26.0 .`.
- Passed: isolated `cargo test --locked -p lance-graph --lib csr_expand::tests`: 10 tests (log: `/private/tmp/lance-csr-runtime/issue-11-rust-isolated.log`). Regression cases cover partial traversal of a 100,000-edge hub, duplicate deletion offsets, immutable snapshots, self-loops, null closing keys and missing target rows. Earlier shared-target logs reused another branch's binary and are excluded from evidence.
- Passed: rebuilt extension in an isolated worktree; `python -m pytest -q python/tests/test_engine_csr.py python/tests/test_engine_csr_delta.py`: 96 tests. Log: `/private/tmp/lance-csr-runtime/issue-11-python-tests.log`.
- Passed: isolated `cargo clippy --workspace --all-targets -- -D warnings`. Log: `/private/tmp/lance-csr-runtime/issue-11-clippy.log`.
- Passed: standalone `cargo test --workspace --locked`, full Python suite (333 tests), `make lint`, and all 13 affected CSR/Cypher guide blocks. Logs: `issue-11-workspace-tests.log`, `issue-11-python-full.log`, `issue-11-python-lint.log`, `issue-11-snippets.log` under `/private/tmp/lance-csr-runtime/`.
- Pending: before/after correctness and timings for both required GDC SF1 suites; high-degree memory measurements and deletion-refresh comparison on identical hardware/settings. No speed or memory-reduction claim is made before these run.
- Passed: 10,000-row wide-hub harness smoke test, 2 output batches and checksum 49,995,000. Raw output: `/private/tmp/lance-csr-runtime/issue-11-memory-smoke-unsandboxed.json`. The sandbox blocked `ps` on the first attempt, so the own-process RSS measurement was rerun with that read permitted. This validates the harness and output, not memory scaling.
- Pending: clean-checkout reproduction of the exact proposed commit, pinned dataset manifests/checksums and accessible runner instructions. The current branch is not yet submission-ready.

## Memory reproduction

After building the extension with `make install` in `python/`, use the same generated data for both revisions:

```bash
uv run --no-sync python benchmarks/bounded_memory.py generate --out /tmp/csr-hub-100k --rows 100000 --hub
uv run --no-sync python benchmarks/bounded_memory.py run --data /tmp/csr-hub-100k --query hub
```

Repeat for 10,000 and 1,000,000 rows. Every target carries 4,096 deterministic pseudorandom payload bytes. Compare fresh-process measurements; raw process RSS includes the resident indexes and Lance/runtime caches, and peak growth is explicitly a lower bound rather than an allocation accounting proof. The Rust cursor regression independently checks that the remaining hub has not been visited after the first chunk.

Release artifact qualification: clean source `e127755` was rebuilt after a successful binding-manifest `cargo clean --release` for the catalog, engine and Python binding crates. The installed Python 3.12 extension matched the wheel bytes and passed Arrow/Lance/CSR/shortest-path/refresh smoke checks with Rust absent from PATH plus 110 focused tests. Build interpreter: Python 3.13.17; wheel tag: `cp312-abi3-macosx_11_0_arm64`. Final proof: `/private/tmp/lance-csr-submissions/wheels/issue-11-clean-r2/release-proof.json`; wheel SHA256 `12f84f269da0d8234713716f5a4f25d260400390d8fb6e03fc4d48b7890acabe`. Earlier preliminary artifacts remain separate and are not the final qualification. Both complete SF1 workloads passed their unchanged golden checks in memory and lazy modes; paired-repeat acceptance remains open.

## Frozen review and acceptance

Head `e127755c872949733c69435047384c9d17fb572b`; actual dependency base `54e02307a48e467a98d9da2183f5adcb767e7c99`. The [release proof](../release-receipts/issue-11-clean-r2/release-proof.json) records wheel SHA-256 `12f84f269da0d8234713716f5a4f25d260400390d8fb6e03fc4d48b7890acabe` and installed/archive binary identity. Its outside-checkout consumer passed 110 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. The measured regression gate remains held; the [draft index](README.md) and [gate ledger](../review-readiness-original.json) identify the remaining CSR or corrected-owner checks. Passing correctness does not waive a repeated slowdown above 10%. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Submitted by David Hughes (@stugorf), with AI Collaboration.
