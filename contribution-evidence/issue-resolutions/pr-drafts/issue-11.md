# fix(csr): stream high-degree expansion in bounded chunks

Resolves #11. CSR expansion emits bounded output chunks while retaining a resumable neighbour position, rather than collecting the complete fan-out and output. Deleted occurrences use snapshot offsets; duplicates, self-loops, missing targets, filtering, errors and existing-reader snapshots preserve their semantics.

The cursor borrows adjacency slices at the saved offset. Counters stay local until a chunk returns, and the three tiny live-neighbour constructors inline at the traversal boundary. This removes the measured per-edge abstraction overhead without changing deletion semantics. Deletion refresh still scans affected sources to resolve immutable offsets.

Head `d7ad39819d04acf44907213ba7532df5e93d8869`, branch `codex/issue-11-inline-local-cursors`; original base `54e02307a48e467a98d9da2183f5adcb767e7c99` is unchanged.

## Validation and documentation

The [exact-source proof](../source-receipts/issue-11-fused-cursor-source-proof.json) records 588 Rust tests, 333 Python tests, 11 separate binding tests and 36 executable documentation blocks. Workspace/binding Clippy, formatting, Ruff, Pyright, strict Zensical and pinned spelling passed. Native regressions prove the first chunk leaves later neighbours unvisited and cover base/delta offsets, tombstones, cancellation, duplicates and snapshot isolation.

The [optimized release receipt](../release-receipts/issue-11-fused-cursor-release/release-proof.json) identifies the wheel and installed/archive binary hashes. Both Python 3.13 and 3.12 passed 110 focused installed tests and no-Rust smoke checks outside the checkout.

Both complete SF1 suites pass unchanged goldens in memory and lazy modes. Fresh paired checks clear the historical Q30/IC6 slowdown under the frozen repeat rule. Earlier slower implementations and their failed gates remain evidence, rather than being replaced. The exact final wheel passed all nine wide-hub measurements; [raw results and medians](../focused/final-hub-and-cancellation/hub-summary.json) retain output checks and the reused matched main baseline.

The CSR guide explains per-partition bounds excluding resident input/graph data. The executable wide-property benchmark validates count/checksum and records first-batch latency and sampled incremental RSS. RSS growth is a lower bound, not allocation accounting; deletion-refresh work is a separate trade-off. See [reproduction instructions](../REPRODUCE.md) for pinned inputs and commands.

Submitted by David Hughes (@stugorf), with AI Collaboration.
