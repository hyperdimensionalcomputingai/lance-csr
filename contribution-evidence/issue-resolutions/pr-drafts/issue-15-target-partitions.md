# feat(python): allow an explicit query partition target

Relates to #15. Keep DataFusion's existing partition/default repartition settings and add keyword-only `CypherEngine(..., target_partitions=None)` for callers who measure their workload. Positive Python integers are accepted; booleans and invalid bounds are rejected. Refresh preserves the override. It controls partitions rather than threads or memory.

The controlled comparison covers partitions 1, 2 and the measured host's default 16, with round-robin repartition enabled and disabled: 24 complete workload cells and 528 unchanged golden checks, followed by 60 paired repeat cells. No alternative improves both workload geometric means without per-query regressions. The [setting report](../partition-settings/README.md) retains every query and the trade-off. Thirty-six wide-scan runs over 500,000 rows with 4 KiB properties also pass; failed setup/checksum attempts are preserved and excluded. No adaptive scheduler or universal speedup is introduced.

Head `7461a7774d6cdf74bbd6961efa7ec86b229fa945`; frozen base `91d6b482a625b671c1e2eef923a4105331dc7ef4`. Source checks passed 642 Rust tests, 1,209 Python tests, 17 separate bridge tests, 46 executable documentation blocks and required lint/typing/docs checks. Native checks assert the actual partition target and retained round-robin setting before and after refresh. Constructor guides, API reference, stubs and the executable refresh example agree.

The [optimized wheel receipt](../release-receipts/issue-15-option-release/release-proof.json) records archive/installed identity and all 1,209 installed Python tests. Both complete workloads pass in memory and lazy modes. The frozen head's main-comparison CSR/path flags remain in the original ledger; its immediate-parent gates pass. Coordinated application uses the corrected CSR and compact path-null planning in the qualified combined source. This remains a draft until that actual target matches the [integration record](../INTEGRATION.md); changing the base requires requalification.

Submitted by David Hughes (@stugorf), with AI Collaboration.
