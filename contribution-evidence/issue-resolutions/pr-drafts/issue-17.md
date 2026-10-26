# perf(graph): search shortest paths from both ends

Resolves #17. Bound endpoint pairs expand complete smaller-frontier layers bidirectionally when reverse adjacency is available. Both predecessor graphs retain every shortest alternative and parallel-edge multiplicity, joining at a correct depth cut. Shared one-to-many source searches keep their forward search. Directed fallback and relationship-alternation semantics remain unchanged.

Native tests compare independently expected results across every directed three-node graph, endpoint pairs and hop bounds. Separate cases cover odd/even distances, several meeting nodes, duplicates, cycles, self-loops, equal endpoints, no path and a wide middle layer. Python tests cover incoming/outgoing/undirected, optional and shared-source queries with and without CSR. Guides document the execution strategy and tie behavior; Python signatures do not change.

Head `09a92e424bd8a7dffc0c3ff6a82838dc088de59e`; frozen base `e127755c872949733c69435047384c9d17fb572b`. Workspace tests/Clippy, 341 Python tests, formatting, lint/typing, strict documentation, spelling and all 13 affected guide blocks pass. The [optimized wheel receipt](../release-receipts/issue-17/release-proof.json) records archive/installed identity and 118 installed focused tests outside the checkout.

Both full workloads pass unchanged goldens in memory and lazy modes. Against the actual parent, IC13 after/before ratios are 0.240 memory and 0.302 lazy; IC14 ratios are 0.704 and 0.682. These are retained first-pass measurements, with complete [per-query tables](../comparisons-original/issue-17-vs-issue-11-clean-r2-ic-lazy.json). The independent wide-middle fixture visits 102 depth-map states versus 10,101 forward states; this is a synthetic algorithm-work proof, not instrumented SF1 edge visits.

Immediate-parent gates pass. The frozen head's inherited main Q30 flag remains recorded; the qualified combined source applies the corrected CSR kernel. Keep this as a coordinated draft until its target matches the [integration record](../INTEGRATION.md), with requalification after any base/conflict-resolution change.

Submitted by David Hughes (@stugorf), with AI Collaboration.
