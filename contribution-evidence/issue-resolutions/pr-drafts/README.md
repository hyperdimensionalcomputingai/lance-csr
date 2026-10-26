# Local PR review status

Sixteen submission heads have completed performance acceptance. Seven remain held for the CSR performance gate or corrected-owner verification. No branch has been pushed and no PR has been opened.

The [original frozen bases](../review-branches.json) and [current draft heads](../review-heads-current.json), installed [release receipts](../release-receipts/) and per-PR before/after tables identify what was actually tested. [Reproduction instructions](../REPRODUCE.md) cover public data, locked dependencies and all required commands.

| Issue | Focused submission | Current acceptance |
| --- | --- | --- |
| #3 | integer precision | Complete for frozen head |
| #11 | bounded expansion | Complete for corrected head |
| #12 | refresh publication | Complete for frozen head |
| #13 | iterative paths | Held; correction or shared CSR gate remains |
| #14 | plan templates | Complete for frozen head |
| #15 | partition override | Held; correction or shared CSR gate remains |
| #16 | property read reuse | Held; correction or shared CSR gate remains |
| #17 | bidirectional search | Held; correction or shared CSR gate remains |
| #18 | comprehension counts | Complete for frozen head |
| #19 | query deadlines | Held; correction or shared CSR gate remains |
| #20 | container parameters | Complete for frozen head |
| #20 | pagination parameters | Complete for frozen head |
| #20 | math | Complete for frozen head |
| #20 | string/list functions | Complete for frozen head |
| #20 | entity metadata | Complete for frozen head |
| #20 | reduce/list predicates | Complete for frozen head |
| #20 | UNION | Complete for frozen head |
| #20 | path values | Held; correction or shared CSR gate remains |
| #20 | CALL | Held; correction or shared CSR gate remains |
| #21 | alias shadowing | Complete for frozen head |
| #22 | public stubs | Complete for frozen head |
| #23 | compatible accessors | Complete for frozen head |
| #23 | expression types | Complete for frozen head |

The original 128 complete-workload cells passed 2,816 unchanged golden checks. All 115 paired-repeat groups completed another 2,016 checks. Smaller flags received controlled follow-up: the integer-list and container investigations do not substantiate a 10% regression; the prospectively fixed 32-pair entity study has after/before ratio 0.998 with 95% interval [0.960, 1.036] and an unchanged-wheel control consistent with equality.

The corrected path and timeout owners passed another 264 complete-workload checks together with a CSR prototype. Path null routing has identical frozen-input proof of 16 failures before and 16 passes after. Both corrected path queries match all 58 IC14 golden paths and weights. The timeout correction passes all three local post-expiry CPU cleanup trials; opt-in overhead and cooperative limits remain documented.

The final combined source `7d2ef3e11b3858cc4571db315df4e848a37bdb82` passed 650 Rust workspace tests (10 ignored), 1,241 Python tests, 17 separate binding tests, 46 executable documentation blocks and all required static/docs checks. Its optimized wheel passed the full installed Python suite and strict consumer typing on both Python 3.13 and 3.12 outside the source checkout. Final workload correctness passed all 12 cells (264 unchanged golden checks). All 21 paired groups completed 348 checks. The combined candidate clears every main/integration-parent performance gate and passes all supporting proofs. Standalone owner regressions remain explicit; the timeout owner has a separate local scan-budget experiment in qualification.

#19 external runner adoption and #23 privacy/validated deserialization for the next breaking release remain closure dependencies. The #20 tracker remains open until every finite group is accepted. Publication still needs owner approval.


[Final combined and standalone gates](../repeat-investigations/final-heads/acceptance.json) distinguish the accepted combined result from held individual heads. The timeout scan-budget experiment remains separate until qualified.

Submitted by David Hughes (@stugorf), with AI Collaboration.
