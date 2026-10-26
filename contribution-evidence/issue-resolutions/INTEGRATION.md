# Frozen branches and shared corrections

The 23 focused submissions retain the exact dependency bases in `review-branches.json`. Current proposed heads are recorded separately in `review-heads-current.json`. A corrected dependency does not retroactively change a frozen head's benchmark result.

Four owner heads replace their original implementation heads:

| Owner | Final head | Reason |
| --- | --- | --- |
| #11 | `d7ad39819d04acf44907213ba7532df5e93d8869` | Borrow saved adjacency slices; keep cursor counters local; inline the three live-neighbour constructors. |
| #13 | `c6bc5c150a2e198e2346e1870509db0f4ce69887` | Count raw deleted/suppressed candidates and source rows that produce no paths at cancellation checkpoints. |
| #19 | `b84e62866aa94effa07bb0742a4b26c3dc83c9d5` | Guard timed DataFusion joins; scan bounded synchronous raw-candidate quanta; yield on empty input/frontier work. |
| #20 path values | `d595e377322e2105f7fbd1bb16d11f6afd776198` | Keep bare path null checks on compact shortest-path planning and bind them to the length column. |

The combined source `7d2ef3e11b3858cc4571db315df4e848a37bdb82` is the explicit integration of those corrections with the remaining feature groups. It preserves target-property reuse, typed path identity, rich relationship occurrences, CALL scope, parameter templates, alias handling, list-count specialization, partition configuration and public stubs. Its exact source and installed optimized wheel have independent receipts.

The original `bfbbb24` optimized candidate remains the qualified integration baseline. The intermediate `23bc699` snapshot is functional evidence, not a performance-accepted release; its unsuccessful partial inlining is superseded by the final combined head. Slower cursor prototypes remain in local history with their failed gates. No baseline, golden result or failed measurement is overwritten.

Review each focused diff against its recorded base. Review the final integration against `bfbbb24` to see the shared corrections together. Dependent submissions must state both their own frozen-head outcome and the actual combined outcome. A passing combined check does not relabel an old standalone regression as passing.

Before merge, use the reviewed dependency order and the demonstrated conflict resolutions in the combined source. If a proposed head, target base, conflict resolution or public interface changes, qualify that actual result again. Unchanged baseline measurements and independent checks remain reusable. This record does not authorize a push, PR creation or merge.

# Outstanding closure items

- #19 external benchmark-runner adoption remains outside the read/run boundary.
- #23 private mapping fields and validated deserialization are held for the next breaking release; the nonbreaking submissions do not close the whole issue.
- #16 addresses repeated filter/projection property reads. Broader decoding and physical disk-I/O attribution remain unverified.
- The #20 tracker stays open until every approved finite group is accepted; no full Cypher-conformance claim is made.

Submitted by David Hughes (@stugorf), with AI Collaboration.
