# Development

## Requirements

- Rust, through [rustup](https://rustup.rs). `rust-toolchain.toml` at the repository root pins the version, with
  `clippy` and `rustfmt`; rustup installs it on the first build.
- `protoc` for Lance's build: `brew install protobuf` on macOS, `sudo apt-get install -y protobuf-compiler` on
  Debian and Ubuntu.
- Python 3.12 or newer, and [uv](https://docs.astral.sh/uv/).

## Building the extension

The package is built with [maturin](https://github.com/PyO3/maturin). `pyproject.toml` points it at the bindings
crate in `crates/lance-graph-python/`, so run maturin from this `python/` directory.

```shell
cd python
make venv       # uv venv --python 3.12 .venv
make install    # maturin develop --extras tests,dev: debug build, plus pytest, pandas, ruff and pyright
```

Or by hand, with the environment activated:

```shell
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install 'maturin[patchelf]'
maturin develop --extras tests,dev
```

`maturin develop` installs the package in editable mode: changes to `python/lance_csr/` take effect immediately,
while Rust changes need another `maturin develop`.

Debug builds compile quickly but run queries much more slowly. To time queries, build optimized code with
`maturin develop --release`. Release builds use thin LTO and one codegen unit (`.cargo/config.toml`), so they take
several minutes. Alternating `maturin develop` and `maturin build` in the same environment invalidates each other's
build cache, so pick one per environment.

If another virtual environment is active, run `deactivate` first so the commands use `.venv`.

## Tests

```shell
make test                                              # every Python test
pytest python/tests/test_engine_csr.py -k undirected   # a subset
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
and incremental CSR maintenance. Build with `maturin develop --release` before running them. The Rust Criterion
benchmarks live in the `lance-graph-benches` crate:

```shell
cargo bench -p lance-graph-benches --bench graph_execution -- --warm-up-time 1
```

For comparisons with other graph engines on LDBC SNB SF1, see
[graph-benchmark-ldbc](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc).

## Building wheels

The published wheels are built by `.github/workflows/python-publish.yml`: Linux x86-64 for glibc 2.28
(manylinux_2_28) and native Apple Silicon, plus an sdist. Each wheel is tested in a clean environment without Rust,
by `ci/check_wheel.py` and the full test suite.

To build a wheel for your own machine:

```shell
make wheel     # maturin build --release --out dist --sdist
```

To build the Linux x86-64 wheel the way CI does, on macOS or Linux, install [zig](https://ziglang.org) (for example
`pip install ziglang`) and the target, then:

```shell
rustup target add x86_64-unknown-linux-gnu
maturin build --release --locked --strip --zig \
    --target x86_64-unknown-linux-gnu \
    --compatibility manylinux_2_28 \
    --out dist
```
