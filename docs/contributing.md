# Contributing

Contributions are welcome, but only [within the project's scope](index.md#what-it-is-not): `lance-csr` is intended to be a fast, **single-machine**
Cypher engine on Lance. Pull requests that add distributed execution are closed as out of scope.

## Set up

You need:

- Rust, through [rustup](https://rustup.rs). `rust-toolchain.toml` pins the version, and rustup installs it on
  the first build.
- `protoc`, which Lance's build needs: `brew install protobuf` on macOS, or
  `sudo apt-get install -y protobuf-compiler` on Debian and Ubuntu.
- Python 3.12 or newer, and [uv](https://docs.astral.sh/uv/). Don't use `pip`.

Build the extension into `python/.venv`:

```bash
cd python
make install   # uv sync --locked --no-install-project, then uv run --no-sync maturin develop
```

Re-run `make install` after changing Rust code; Python changes apply immediately. `python/DEVELOPMENT.md`
explains the uv flags and release builds.

## Test and lint

=== "Rust"

    From the repository root:

    ```bash
    cargo test --workspace
    cargo clippy --workspace --all-targets -- -D warnings
    cargo fmt --all --check
    ```

=== "Python"

    From `python/`:

    ```bash
    make test   # pytest python/tests
    make lint   # ruff check, ruff format --check, pyright
    ```

Add Rust unit tests next to the code with `#[cfg(test)]`, and Python tests in `python/python/tests/test_*.py`.
A CSR change should test that the CSR path returns the same rows as the join path.

## Documentation

The docs site is built with [Zensical](https://zensical.org/) from Markdown in `docs/`, configured by
`zensical.toml` at the repository root. Its tooling is a separate, locked uv project in `docs/`, so it never
needs a Rust build:

```bash
uv run --project docs --locked zensical serve          # live preview at http://localhost:8000
uv run --project docs --locked zensical build --strict # what CI runs
```

The Python examples in the pages are executable. After changing them, or any API they use, run them against
your build:

```bash
cd python
uv run --no-sync python ../docs/check_snippets.py           # every page
uv run --no-sync python ../docs/check_snippets.py csr.md    # one page
```

`docs/AGENTS.md` has the writing guidelines. In short: when a pull request changes a user-facing API or
behaviour, update the guide and the [API reference](api-reference.md) in the same pull request.

## Pull requests

- Follow the commit style in the history: `feat(graph):`, `fix(cypher):`, `docs:`, `refactor(query):`, with
  an imperative subject of at most 72 characters.
- Describe the scope, list the test commands you ran, and note any impact on the Python bindings, examples or
  docs.
- Explain the problem the change solves and why it's needed. For a code change, include before-and-after
  results from both [benchmark suites](benchmarks.md), measured against `main` with the harness described in
  [`CONTRIBUTING.md`](https://github.com/hyperdimensionalcomputingai/lance-csr/blob/main/CONTRIBUTING.md#build-the-benchmark-harness).
  Numbers from [`python/benchmarks/`](benchmarks.md#engine-micro-benchmarks) can support a claim but don't
  replace the two suites.

See [Project structure](project_structure.md) for a map of the code.
