# Issue resolution evidence

This is the local review record for the 14-issue resolution series. Source changes live on separate `codex/issue-*` branches. No branch has been pushed and no PR has been opened. A passing check here applies only to its recorded commit and wheel.

The final combined source is `7d2ef3e11b3858cc4571db315df4e848a37bdb82` on `codex/issue-resolution-final`. It passed 650 Rust workspace tests (10 ignored), 1,241 Python tests, 17 separately linked binding tests and 46 executable documentation blocks, plus formatting, workspace/binding Clippy, Ruff, Pyright, strict docs and spelling. Its [exact-source proof](source-receipts/candidate-7d2ef3e-clean-source-proof.json) and [optimized release receipt](release-receipts/candidate-final-release/release-proof.json) identify the actual code and wheel. Full installed Python suites also pass on Python 3.13 and 3.12 outside the source checkout, including strict consumer typing. Earlier functional snapshots and failed performance prototypes remain separate.

Sixteen focused submission heads now have completed benchmark acceptance. Seven remain held for CSR performance or corrected-owner verification. The [PR review status](pr-drafts/README.md) identifies each submission; the [original gate ledger](review-readiness-original.json) preserves its frozen source and measured flags.

## Complete workload correctness and open acceptance

All 128 cells across 32 qualified release cohorts passed correctness: 2,816 unchanged golden checks. The final 196 before/after tables reuse the matched main baseline and actual dependency-parent measurements. Earlier first-pass artifacts remain intact. Every source and release packaging stage is complete, including scoped path/CALL heads and their bases.

All 115 paired/reversed repeat groups finished: 460 processes and 2,016 unchanged golden checks. The [original repeat report](repeat-investigations/original/acceptance.json) retains every flag. Controlled follow-up did not substantiate a 10% regression for the integer-list, container-parameter and entity-function changes. The fixed-size entity study excluded its pilot and used 32 fresh pairs plus an unchanged-wheel control; its 95% after/before interval is [0.960, 1.036]. Earlier failed gates remain intact.

Corrected path and timeout owners passed complete workload correctness. Path null routing has [identical-input proof](correction-proofs/path-null-paired-proof-r1.json) of 16 failures before and 16 passes after; canonical IC14 and the existing port match all 58 unchanged golden paths and weights. The timeout correction passes all three local post-expiry CPU cleanup trials. These results do not waive the remaining CSR performance gate.

The [focused measurements](focused/README.md) summarize 126 verified cells: bounded hub memory/first-batch latency, variable-length planning and IC9/12/13/14 execution. Separate receipts retain stage profiling, actual IC9 read requests, IC10 plan reduction, partition sizing and timeout overhead. Read counts are not physical disk I/O; sampled RSS growth is a lower bound; no universal hard-preemption claim is made.

## Completed measurements

The [partition comparison](partition-settings/README.md) contains all 528 query rows from the six settings, two workloads and two input modes. Every golden check passed. Sixty paired repeat cells also passed. No alternative improved both workload geometric means; the existing default is retained. Repeated in-memory Q30 improvements support an optional positive-integer partition target for users who measure their own workload.

The separate wide scan validated 36 fresh-process runs over 500,000 rows with 4 KiB properties. Its elapsed time includes output validation; setup and resident input memory are separate. An original million-row memory input exceeded Arrow's signed 32-bit offset limit before query execution. An intermediate expected-checksum typo was caught by assertions. Both failed attempts are preserved and excluded.

## Required submission gates

Each code submission needs complete Q30 and IC comparisons against `main`, and its immediate dependency base, in memory and lazy modes. Full per-query results, repeat investigations above 10%, source/wheel hashes and portable regeneration commands accompany each final draft. Original source branches, optimized wheels and baselines remain frozen. Necessary corrections have isolated branches and new receipts; the PR status table records which performance gates still remain.

The series includes parser, semantic, Rust execution, Python, typing, documentation and executable-example checks. The excluded Python binding crate is tested separately from the Rust workspace. [Reproduction instructions](REPRODUCE.md) list the public datasets, pinned benchmark revisions, build commands and environment requirements.

Two closure dependencies remain explicit: #19 adoption by external benchmark runners is outside their read/run authorization, and #23 mapping-field privacy with validated deserialization is held for the next breaking release. The #20 tracker remains open until every finite feature group meets its checks. No complete Cypher-conformance claim is made.


The [final-head comparison tables](comparisons-final-heads/index.json) and [paired acceptance report](repeat-investigations/final-heads/acceptance.json) preserve the actual remaining standalone flags. See [integration prerequisites](INTEGRATION.md) before changing a base or interpreting combined evidence.

Submitted by David Hughes (@stugorf), with AI Collaboration.
