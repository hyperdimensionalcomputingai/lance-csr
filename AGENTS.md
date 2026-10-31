# Repository Guidelines

## Project Structure & Module Organization
- `crates/lance-csr/` hosts the Rust Cypher engine; keep new modules under `src/` and co-locate helpers inside `query/` or feature-specific submodules.
- `crates/lance-csr-catalog/` provides the graph source catalog and `DirNamespace` (Unity Catalog and Delta Lake support were removed).
- `crates/lance-csr-benches/` contains performance benchmarks in a dedicated unpublished crate; add new benchmarks here rather than in the main crate.
- `crates/lance-csr-python/src/` contains the PyO3 bridge; `python/python/lance_csr/` holds the pure-Python facade and packaging metadata.
- `python/python/tests/` stores functional tests; mirror new features with targeted cases here and in the corresponding Rust module.
- `examples/` demonstrates Cypher usage; update or add examples when introducing new public APIs.
- `docs/` is the Zensical documentation site (configured by `zensical.toml`); follow `docs/AGENTS.md`.
- Keep the docs in step with the code. Any substantial code change (a new or changed Python method, parameter, return type, exception or Cypher feature, or a change in behaviour users can observe) updates the affected guide pages in the same pull request. The Python API reference (`docs/api-reference.md`) is generated from the docstrings and type hints of the public classes in `python/python/lance_csr/`, so a Python API change updates those docstrings, checked against the behaviour in `crates/lance-csr-python/src/`. A change to a DataFusion extension (an extension node or operator, an optimizer rule, a planner rewrite, or the CSR and node indexes) updates `docs/internals/`. Document Python behaviour there, not in Rust `///` comments on the bindings. Then run `uv run --project docs --locked zensical build --strict` and `uvx typos@1.26.0 .` (the spell check CI runs).

## Build, Test, and Development Commands
- `cargo test --workspace` (from the repository root) runs the Rust tests; building needs `protoc`.
- `cargo bench --bench graph_execution -p lance-csr-benches` measures performance-critical changes; include shortened runs with `--warm-up-time 1`.
- Never use `pip`. In `python/`, `make install` runs `uv sync --locked --no-install-project` (locked dependencies and the `dev` group) and then a debug build with `uv run --no-sync maturin develop`; re-run it after Rust edits. Change dependencies with `uv add`/`uv remove` and commit `uv.lock`.
- `make test` runs the Python tests; `make lint` runs `ruff`, formatting checks, and `pyright`.

## Coding Style & Naming Conventions
- Format Rust with `cargo fmt --all`; keep modules and functions snake_case, types PascalCase, and reuse `snafu` error patterns.
- Run `cargo clippy --workspace --all-targets -- -D warnings` to catch lint regressions, as CI does.
- Use 4-space indentation in Python; maintain snake_case modules, CamelCase classes, and type-annotated public APIs.
- Apply `ruff format python/` before committing; `ruff check` and `pyright` enforce import hygiene and typing.

## Testing Guidelines
- Add Rust unit tests alongside implementations via `#[cfg(test)]`; prefer focused scenarios over broad integration.
- Python tests belong in `python/python/tests/`; name files `test_*.py` and use markers (`gpu`, `cuda`, `integration`, `slow`) consistently.
- When touching performance-sensitive code, capture representative `cargo bench` or large-table pytest timing notes in the PR.

## Commit & Pull Request Guidelines
- Follow the existing history style (`feat(graph):`, `docs:`, `refactor(query):`), using imperative, ≤72-character subjects.
- Reference issues or discussions when relevant and include brief context in the body.
- PRs should describe scope, list test commands run, mention benchmark deltas when applicable, and highlight impacts on bindings or examples.
- To cut a release, follow `RELEASE.md`: bump the version and lockfiles, publish a GitHub release (which builds, tests and publishes to PyPI in one run, with no separate dry run), then publish the crates by hand once PyPI has the new version.
- Before a PR claims a code change is faster, or doesn't slow anything down, follow `CONTRIBUTING.md`: explain the problem and why the change is needed, and measure the branch against `main` on both benchmark suites with its harness (release wheels loaded with `uv run --with`, both `memory` and `lazy` modes). Never edit benchmark queries, expected answers or runner settings, and report every query.
