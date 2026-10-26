# perf(csr): traverse variable-length paths iteratively

Resolves #13. Eligible variable-length paths use one resumable depth-first CSR operator for the complete hop range. Mixed-label alternation remains typed until execution; unsupported shapes keep the join fallback. Output chunks are bounded and the 20-hop limit remains.

Zero-hop paths use source identity in both execution routes, including label identity when integer keys overlap. Duplicate paths and the existing immediate-backtracking rule are preserved. Rust callers constructing `LogicalOperator::VariableLengthExpand` provide `target_label`; Python signatures are unchanged.

Raw candidate checkpoints count deleted/suppressed edges as work. Source-row checkpoints also yield when no path can be emitted. Two deterministic real-state regressions fail against the original production head and pass unchanged against this correction; the [paired input proof](../correction-proofs/cancellation-paired-inputs-r1.json) records function hashes and failing-before logs.

Head `c6bc5c150a2e198e2346e1870509db0f4ce69887`, branch `codex/issue-13-cancellation-boundaries`; frozen base `e127755c872949733c69435047384c9d17fb572b` remains unchanged. This head includes the small adjacency corrections needed by its traversal; integration with the final #11 head is verified by the combined candidate, without silently changing this base.

## Validation and documentation

The [exact-source receipt](../source-receipts/issue-13-cancellation-source-proof.json) records 593 Rust tests, 347 Python tests, 11 separate binding tests and 36 executable documentation blocks. Formatting, workspace/binding Clippy, Ruff, Pyright, strict documentation and spelling passed. Regression coverage includes zero-hop expectations, cycles, duplicates, typed alternation, optional matches, null/dangling endpoints and bounded polling without output.

The [release receipt](../release-receipts/issue-13-cancellation-release/release-proof.json) records archive/installed binary identity and full 347-test installed suites on both Python 3.13 and 3.12, outside the checkout with Rust absent from PATH.

The [focused plan measurements](../focused/README.md) retain constant iterative operator count at hop limits 2, 5, 10 and 20, with planning time and explain size. That planning code is unchanged by the final polling correction; old execution timings are identified by their original head. The final head passes both complete suites/modes and every paired immediate-parent gate. Lazy IC3 and IC9 remain above the main-comparison threshold in both fresh pairs, so standalone main acceptance is held. The explicitly tested combined candidate clears these gates with the coordinated property-read changes; this does not relabel this frozen head as passing. See the [paired report](../repeat-investigations/final-heads/acceptance.json) and [integration record](../INTEGRATION.md). The Cypher/CSR guides document supported ranges, fallback and limits.

Submitted by David Hughes (@stugorf), with AI Collaboration.
