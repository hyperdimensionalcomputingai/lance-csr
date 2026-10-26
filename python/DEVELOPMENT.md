# Development

## Requirements

- Rust, through [rustup](https://rustup.rs). `rust-toolchain.toml` at the repository root pins the version, with
  `clippy` and `rustfmt`; rustup installs it on the first build.
- `protoc` for Lance's build: `brew install protobuf` on macOS, `sudo apt-get install -y protobuf-compiler` on
  Debian and Ubuntu.
- Python 3.12 or newer, and [uv](https://docs.astral.sh/uv/).

## Building the extension

Dependencies are managed with uv and pinned in `uv.lock`. The package is built with
[maturin](https://github.com/PyO3/maturin): `pyproject.toml` points it at the bindings crate in
`crates/lance-csr-python/`, so run these commands from this `python/` directory.

```shell
cd python
uv sync --locked --no-install-project   # locked dependencies and the dev group into .venv
uv run --no-sync maturin develop        # debug build of the extension, installed in editable mode
```

`make install` runs both. The `dev` dependency group in `pyproject.toml` holds maturin, pytest, pandas, ruff and
pyright, and `uv sync` installs it by default.

- `--no-install-project` leaves the package to `maturin develop`. Without it, uv builds the package through
  maturin's build backend, in release mode.
- `--no-sync` on `uv run` uses `.venv` as it is. A plain `uv run` or `uv sync` re-syncs exactly to the lockfile and
  uninstalls the `maturin develop` build, which the lockfile doesn't list. To re-sync and keep the build, add
  `--inexact` (as `make sync` does).
- To change dependencies, use `uv add` and `uv remove` (`uv add --group dev <tool>` for development tools), and
  commit the updated `uv.lock`.

`maturin develop` installs the package in editable mode: changes to `python/lance_csr/` take effect immediately,
while Rust changes need another `maturin develop`.

Debug builds compile quickly but run queries much more slowly. To time queries, build optimized code with
`uv run --no-sync maturin develop --release`. Release builds use thin LTO and one codegen unit
(`.cargo/config.toml`), so they take several minutes. Alternating `maturin develop` and `maturin build` invalidates
each other's build cache, so pick one per environment.

## Tests

```shell
make test                                                            # every Python test
uv run --no-sync pytest python/tests/test_engine_csr.py -k undirected   # a subset
```

The Rust tests run from the repository root:

```shell
cargo test --workspace
```

## Formatting and linting

```shell
make format    # ruff format
make lint      # ruff check, ruff format --check, pyright
```

For Rust, from the repository root (CI runs the same checks):

```shell
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

## Benchmarks

`python/benchmarks/` holds scripts for the engine's own costs: catalog reuse, bounded-memory streaming, CSR expansion
and incremental CSR maintenance. Build with `uv run --no-sync maturin develop --release` before running them. The
Rust Criterion benchmarks live in the `lance-csr-benches` crate:

```shell
cargo bench -p lance-csr-benches --bench graph_execution -- --warm-up-time 1
```

The Graph Data Council (GDC), formerly known as the Linked Data Benchmark Council (LDBC), maintains the
Social Network Benchmark (SNB). For comparisons with other graph engines on GDC SNB SF1, see
[GDC SF1 benchmark repository](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc).

## Building wheels

The published wheels are built by `.github/workflows/python-publish.yml`: Linux x86-64 for glibc 2.28
(manylinux_2_28) and native Apple Silicon, plus an sdist. Each wheel is tested in a clean environment without Rust,
by `ci/check_wheel.py` and the full test suite.

To build a wheel for your own machine:

```shell
make wheel     # uv run --no-sync maturin build --release --out dist --sdist
```

To build the Linux x86-64 wheel the way CI does, on macOS or Linux, add the target and run maturin with its `zig`
extra, which brings the zig linker. Use `uv run --with` rather than `uvx`: maturin finds zig through `python3` on
`PATH`, which `uv run` sets up and `uvx` does not.

```shell
rustup target add x86_64-unknown-linux-gnu
uv run --no-project --with 'maturin[zig]>=1.9,<2' maturin build --release --locked --strip --zig \
    --target x86_64-unknown-linux-gnu \
    --compatibility manylinux_2_28 \
    --out dist
```
