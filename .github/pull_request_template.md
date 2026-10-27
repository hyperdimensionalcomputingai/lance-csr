## Problem and why it matters

What problem does this solve, who runs into it, and what shows it on `main` (a reproducer, a profile or a
benchmark run)? Why does the fix belong in lance-csr, and what does it cost? Link related issues.

## Change

What the change does, and any approaches you tried and rejected.

## Validation

List the checks you ran and their results. For a bug fix, include a regression test.

For a code change, include results from both benchmark suites, built with the harness in
[CONTRIBUTING.md](https://github.com/hyperdimensionalcomputingai/lance-csr/blob/main/CONTRIBUTING.md#build-the-benchmark-harness).
For each suite and mode (`memory`, `lazy`), report every query:

| Query | `main` (ms) | branch (ms) | Change |
| --- | ---: | ---: | ---: |
| | | | |

Add the geometric mean of the ratios, the gap between the two `main` runs, the commits of lance-csr and
both benchmark repositories, and the machine, OS and memory. Explain any queries that got noticeably slower.
Documentation-only changes don't need benchmarks.

## User impact

Note any API or behaviour changes and the documentation or examples updated, or write “None”.
