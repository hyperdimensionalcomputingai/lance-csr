# Issue resolution review packet

This local packet covers 14 issues through 23 focused submissions. Sixteen are ready for individual review; seven require coordinated integration and should remain drafts. No branch has been pushed and no PR has been opened by this work. Start with the [PR index](pr-drafts/README.md), [current readiness ledger](review-readiness-current.json) and [frozen heads/bases](review-heads-current.json).

The selected combined source is `7d2ef3e11b3858cc4571db315df4e848a37bdb82`, branch `codex/issue-resolution-final`. Its [exact-source proof](source-receipts/candidate-7d2ef3e-clean-source-proof.json) records 650 Rust workspace tests (10 ignored), 1,241 Python tests, 17 separate binding tests and 46 executable documentation blocks. Formatting, workspace/binding Clippy, Ruff, Pyright, strict Zensical and pinned spelling passed. The [optimized wheel](release-receipts/candidate-final-release/release-proof.json), SHA-256 `ee0635e2477ee57805382b315110c386aa7de947acf42ea3960804ea2ceeaa5d`, passed all 1,241 installed Python tests and strict consumer typing on Python 3.13 and 3.12 outside the source checkout, with Rust absent from PATH.

## Selected combined performance

Both workloads pass unchanged goldens in memory and lazy modes. All selected-source main and qualified-integration-parent gates pass under the frozen paired-repeat rule. Retained first-pass geometric mean speedups against main are below; greater than one means faster.

| Workload | Memory | Lazy |
| --- | ---: | ---: |
| Q30 | 1.024 | 0.942 |
| IC | 1.393 | 1.356 |

The lazy Q30 suite mean is about 6.2% slower; the individual repeat gates still pass. [Full per-query comparisons](comparisons-final-heads/index.json) and the [paired report](repeat-investigations/final-heads/acceptance.json) retain variability and initial flags. Passing the combined source does not relabel regressions in an older standalone head.

The full audit matrix now contains 184 cells across 46 release cohorts and 4,048 unchanged golden checks. It includes prerequisites and rejected experiments, whose performance outcomes are separate. The original 128-cell/2,816-check wave and all original 115 paired-repeat groups remain immutable. Controlled follow-up for integer lists, container parameters, entity metadata and timeout IC6 retains every initial failed gate, fixed sample size, control and outcome.

## Integration decision and focused evidence

The standalone timeout head's larger work budget passes its own acceptance, including the [32-pair IC6 investigation](repeat-investigations/scan-budget-ic6-precision/summary.json). The larger-budget combined variant `891d8d7` fails lazy Q30 query 30 and is rejected. The [integration record](INTEGRATION.md) explains the required smaller-budget resolution and the seven coordinated drafts; no public API depends on a particular private work quantum.

[Focused measurements](focused/README.md) cover hub memory/first-batch latency, constant-size iterative plans, planning stages, actual IC9 requests, IC10 reduction, shortest-path work and partition settings. The exact final #11 head has [nine fresh hub measurements](focused/final-hub-and-cancellation/hub-summary.json), reusing the unchanged matched main baseline. Sampled incremental RSS is a lower bound and excludes resident graph/input. ObjectStore requests are not physical disk I/O; independent decoding attribution remains unverified.

The accepted combined release passes all three correlated CALL post-expiry CPU trials and both canonical/ported IC14 against all 58 golden paths and weights. Their [raw receipts](focused/final-hub-and-cancellation/manifest.json) identify the actual wheel. Timeouts remain cooperative. The partition report retains the existing default because no tested alternative meets both suite and per-query criteria; unchanged scan evidence remains applicable.

## Reproduction and publication

[Reproduction instructions](REPRODUCE.md) pin public SF1 input, ingestion scripts, benchmark revisions, hardware, dependency locks and commands. Source/wheel receipts, before/after regression inputs, executable examples and API/stub checks accompany each focused draft. Current source refs and raw records are packaged separately with checksums; original absolute paths in receipts are provenance, not regeneration prerequisites.

Publication awaits owner approval under the approved delivery plan. External runner adoption for #19 remains outside read/run authorization; #23 field privacy/validated deserialization waits for the next breaking release. The #20 tracker remains open until every finite group is accepted. No complete Cypher-conformance or other-OS verification is claimed.

Submitted by David Hughes (@stugorf), with AI Collaboration.
