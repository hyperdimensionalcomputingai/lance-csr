# Contributing to lance-csr

Start with the [project goals](README.md#goals) and [contribution guidelines](README.md#contributing).
This project focuses on single-machine graph queries; distributed execution is out of scope.
For a larger change, open an issue to explain the problem and proposed approach before starting work.

## Report a bug or suggest an improvement

Search the [existing issues](https://github.com/hyperdimensionalcomputingai/lance-csr/issues) first.
For bugs, include a small reproducer, expected and actual results, your lance-csr and Python versions,
and your operating system. Use synthetic data when possible; remove credentials and private data.
For improvements, describe the use case and how the change would help users.

## Prepare a pull request

Follow the [build instructions](README.md#build-from-source) and
[Python development guide](python/DEVELOPMENT.md) to set up and run the relevant checks.
Keep changes focused, add a regression test for bug fixes, and update affected documentation for
user-visible changes. Python API changes also need an update to `docs/api-reference.md`.

Explain the problem, why the change is needed, and how you validated it. Code changes need correctness
checks and before-and-after results from both benchmark suites listed in the
[contribution guidelines](README.md#contributing), using the same hardware and settings.
Documentation-only changes do not need benchmarks. Agent-assisted contributions follow the same
requirements: review the generated code and validate it before submitting.
