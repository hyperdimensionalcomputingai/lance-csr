Title: feat(python): allow an explicit query partition target

Some short-query workloads benefit from fewer query partitions, while the controlled SF1 comparisons also
show repeatable regressions on other queries. Keep DataFusion's default and add a keyword-only
`CypherEngine(..., target_partitions=None)` override for callers who have measured their workload.

The option accepts positive Python integers, rejects booleans and invalid bounds, and survives refresh.
It uses the existing SessionConfig setting; it does not cap threads or memory. Constructor docs, API
reference, stubs, strict typing checks and a runnable refresh example are updated together.

Scope: feature `2dac56b` plus embedded-test documentation `7461a77`, compared with prerequisite `91d6b48`.
This is a local review draft; the broad prerequisite chain must be represented by its actual upstream
submission bases when preparing the final PR. Related issue: #15.

Validation: 642 Rust tests, 1209 Python tests, 17 private bridge tests, 46 executable doc blocks, the updated
example, all lint/typing/docs checks, and an installed no-Rust wheel smoke with 26 focused partition/typing
tests. The native test asserts actual partition targets and the unchanged round-robin default before and
after refresh. Arrow/Lance, CSR/join, cached parameters and active readers are covered.

Evidence and exact artifact hashes: `/private/tmp/lance-csr-issue-15-option-proof.md`. The coordinator will
attach the controlled matrix and paired-repeat report to the durable submission evidence. No universal
speedup is claimed, and no benchmark ran during active builds. Final combined release qualification remains
separate from this scoped source proof.

## Frozen review and acceptance

Head `7461a7774d6cdf74bbd6961efa7ec86b229fa945`; actual dependency base `91d6b482a625b671c1e2eef923a4105331dc7ef4`. The [release proof](../release-receipts/issue-15-option-release/release-proof.json) records wheel SHA-256 `2788a29d5f5bdb5d6f2d4b44eb42f2af882282b409f13b7aa63e1110d647af3b` and installed/archive binary identity. Its outside-checkout consumer passed 1209 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. The measured regression gate remains held; the [draft index](README.md) and [gate ledger](../review-readiness-original.json) identify the remaining CSR or corrected-owner checks. Passing correctness does not waive a repeated slowdown above 10%. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Submitted by David Hughes (@stugorf), with AI Collaboration.
