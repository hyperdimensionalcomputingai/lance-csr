# Issue resolution evidence

This is the local review record for the 14-issue resolution series. Source changes live on separate `codex/issue-*` branches. No branch has been pushed and no PR has been opened. A passing check here applies only to its recorded commit and wheel.

The current combined candidate is `bfbbb24db2fdd2753b959ca775d5c85eaea75cae`. Independent clean-source checks passed all 643 Rust workspace tests, 1,224 Python tests, 17 separately linked binding tests and 46 executable documentation blocks, plus all formatting, Clippy, Ruff, Pyright, strict docs and spelling checks. The optimized wheel passed all 1,224 Python tests outside the checkout. The clean-source proof is recorded in `candidate-clean-source-proof.json`.

## Completed first acceptance pass

All 104 cells across 26 verified release cohorts passed correctness: 2,288 unchanged golden checks. The [156 before/after tables](first-pass-comparisons/index.json) reuse that single matched baseline and the actual dependency-parent measurements. Queries over 10% are flags for paired and reversed repeats, not confirmed regressions. Final acceptance remains pending those repeats and the remaining scoped path/CALL packaging.

## Completed measurements

The [partition comparison](partition-settings/README.md) contains all 528 query rows from the six settings, two workloads and two input modes. Every golden check passed. Sixty paired repeat cells also passed. No alternative improved both workload geometric means; the existing default is retained. Repeated in-memory Q30 improvements support an optional positive-integer partition target for users who measure their own workload.

The separate wide scan validated 36 fresh-process runs over 500,000 rows with 4 KiB properties. Its elapsed time includes output validation; setup and resident input memory are separate. An original million-row memory input exceeded Arrow's signed 32-bit offset limit before query execution. An intermediate expected-checksum typo was caught by assertions. Both failed attempts are preserved and excluded.

## Required submission gates

Each code submission needs complete Q30 and IC comparisons against `main`, and its immediate dependency base, in memory and lazy modes. Full per-query results, repeat investigations above 10%, source/wheel hashes and portable regeneration commands accompany each final draft. These comparisons are pending until the focused source branches and optimized wheels are frozen.

The series includes parser, semantic, Rust execution, Python, typing, documentation and executable-example checks. The excluded Python binding crate is tested separately from the Rust workspace. [Reproduction instructions](REPRODUCE.md) list the public datasets, pinned benchmark revisions, build commands and environment requirements.

Two closure dependencies remain explicit: #19 adoption by external benchmark runners is outside their read/run authorization, and #23 mapping-field privacy with validated deserialization is held for the next breaking release. The #20 tracker remains open until every finite feature group meets its checks. No complete Cypher-conformance claim is made.

Submitted by David Hughes (@stugorf), with AI Collaboration.
