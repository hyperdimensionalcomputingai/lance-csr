# Local PR review index

The 23 packets below cover 14 issues. Sixteen are ready for individual review; seven must remain coordinated drafts until their actual targets match the qualified integration. No remote publication has occurred.

[Heads, bases and gates](../review-readiness-current.json) identify exact commits, wheels and dependencies. [Reproduction](../REPRODUCE.md) covers all prerequisites; the [integration decision](../INTEGRATION.md) preserves the smaller cancellation budget after rejecting the larger-budget combined variant.

| Issue | Focused submission | Review state |
| --- | --- | --- |
| #3 | [integer precision](issue-3.md) | Ready for individual review |
| #11 | [bounded expansion](issue-11.md) | Ready for individual review |
| #12 | [refresh publication](issue-12.md) | Ready for individual review |
| #13 | [iterative paths](issue-13.md) | Coordinated draft |
| #14 | [plan templates](issue-14-curated.md) | Ready for individual review |
| #15 | [partition override](issue-15-target-partitions.md) | Coordinated draft |
| #16 | [property read reuse](issue-16.md) | Coordinated draft |
| #17 | [bidirectional search](issue-17.md) | Coordinated draft |
| #18 | [comprehension counts](issue-18-curated.md) | Ready for individual review |
| #19 | [query deadlines](issue-19-final.md) | Coordinated draft |
| #20 | [container parameters](issue-20-containers-typed.md) | Ready for individual review |
| #20 | [pagination parameters](issue-20-pagination-typed.md) | Ready for individual review |
| #20 | [math](issue-20-math-delivery.md) | Ready for individual review |
| #20 | [string/list functions](issue-20-string-list-delivery.md) | Ready for individual review |
| #20 | [entity metadata](issue-20-entities-delivery.md) | Ready for individual review |
| #20 | [reduce/list predicates](issue-20-folds-delivery.md) | Ready for individual review |
| #20 | [UNION](issue-20-union-delivery.md) | Ready for individual review |
| #20 | [path values](issue-20-paths-curated.md) | Coordinated draft |
| #20 | [CALL](issue-20-call-delivery.md) | Coordinated draft |
| #21 | [alias shadowing](issue-21-delivery.md) | Ready for individual review |
| #22 | [public stubs](issue-22.md) | Ready for individual review |
| #23 | [compatible accessors](issue-23-accessors.md) | Ready for individual review |
| #23 | [expression types](issue-23-types.md) | Ready for individual review |

The selected combined source `7d2ef3e` passes 650 Rust tests, 1,241 Python tests, 17 binding tests, 46 documentation blocks, required static/docs checks and full installed suites on Python 3.13/3.12. Both complete workloads, all main/integration-parent repeat gates, canonical IC14 and correlated CALL cleanup pass. Original frozen-head flags remain visible.

The larger-budget combined `891d8d7` is rejected for repeated lazy Q30 query 30 regression; it is retained with all passing correctness and failed performance evidence. Standalone #19 uses its own qualified budget against the original parent, so its draft explicitly requires the selected integration resolution.

Publication requires owner approval. #19 runner adoption and #23 breaking-release stages remain open; the #20 tracker awaits acceptance of every finite group.

The [current-main supplement](../artifacts/current-main-supplement/README.md) verifies the new `90114f3` contribution/README commit: engine code and locks are unchanged, all five main-target merges are clean, and strict docs/spelling pass on local verification merge `5fe24cd`. The benchmark evidence remains valid; frozen source branches are preserved.

Submitted by David Hughes (@stugorf), with AI Collaboration.
