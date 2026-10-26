# Reproduce a proposed submission

Use the exact source revision and wheel digest from that submission's evidence manifest. Run from a fresh worktree. Generated datasets, virtual environments and Rust build caches are not required inputs from the author's workstation.

## Prerequisites

Rust 1.96.0, Cargo, `protoc`, uv, a C/C++ toolchain, Python 3.12 for the development commands, and Python 3.13.17 for the matched benchmark cohort. Dataset extraction additionally needs `curl`, `tar` and `zstd` or `unzstd`. Measurements recorded here use an Apple M4 Max with 16 CPU cores and 128 GiB RAM on macOS 26.6.2 arm64. Other operating systems have not been validated locally.

From the engine checkout:

```bash
cargo test --workspace --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy --manifest-path crates/lance-graph-python/Cargo.toml --all-targets --locked -- -D warnings
```

From `python/`:

```bash
make install PYTHON=3.12.9
make test
make lint
uv run --no-sync python ../docs/check_snippets.py
```

From the engine root:

```bash
uv run --project docs --locked zensical build --strict
uvx typos@1.26.0 .
```

The binding crate is excluded from the workspace. Its standalone Rust test executable also needs the real Python library and standard library. Follow `python/DEVELOPMENT.md` at the exact candidate revision; set `PYTHONHOME` only for the embedded test process, to the selected interpreter's `sys.base_prefix`. Record the executable returned by Cargo JSON rather than selecting an old binary by filename glob.

Build the optimized wheel from `python/`, with one fixed build interpreter for every compared revision:

```bash
uv run --no-sync maturin build --release --locked --interpreter .venv/bin/python --out dist
```

The measured cohort's build interpreter is Python 3.13.17. The `cp312-abi3` tag denotes the minimum ABI, not the actual build interpreter. When reusing a build cache, give each worktree its own writable target and clean the three local crates through the binding manifest before building. Preserve dependency locks. Verify the installed extension digest against the wheel archive before every benchmark cell.

## Public SF1 data

Both workloads use the public SNB Interactive v1 initial SF1 `CsvBasic` / `StringDateFormatter` archive:

- Source: <https://datasets.ldbcouncil.org/snb-interactive-v1/social_network-sf1-CsvBasic-StringDateFormatter.tar.zst>
- Observed archive size: 241,007,155 bytes.
- Observed SHA-256: `5d74f5b0b0570eb593b3317e14ffab3d5dd181efb7e2c947e0ed0409c377b23d`.
- Extracted CSV size is approximately 1.1 GB. Regenerate Lance tables with each pinned repository's ingestion scripts. The archive digest records this run's input; it is not an upstream published integrity attestation.

Use fresh checkout directories, retaining existing benchmark outputs and golden answers:

```bash
git clone https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc.git q30
git -C q30 checkout 6260689aa1fba4eeb4e99958430386b2fcd13f71
git clone https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc-complex.git ic
git -C ic checkout c185123edd58b37b417bc4deb2fec643fdbd5e61
```

Run the pinned scripts from the parent of the two new checkouts:

```bash
uv run --no-project --python 3.13.17 python q30/download_dataset.py
uv run --no-project --python 3.13.17 python ic/download_dataset.py
shasum -a 256 q30/social_network-sf1-CsvBasic-StringDateFormatter.tar.zst
shasum -a 256 ic/social_network-sf1-CsvBasic-StringDateFormatter.tar.zst
uv run --no-project --python 3.13.17 --with pylance==13.0.0 --with pyarrow==26.0.0 python q30/lance_graph/build_graph.py
uv run --no-project --python 3.13.17 --with pylance==13.0.0 --with pyarrow==26.0.0 python ic/lance_csr/build_graph.py
```

Require both archive digests to match before ingestion. The Q30 README's older Parquet description is stale at this revision: its actual pinned script reads the extracted CSV files. Q30's `lance_csr` runner reads `lance_graph/graph_lance`; IC's runner reads `lance_csr/graph_lance`. The scripts intentionally differ in empty-string/null handling. Preserve both pipelines. Neither golden answers nor benchmark source files are changed by this series.

## Run a benchmark cell

Use the absolute path of the intended immutable wheel. Run from the selected benchmark repository's `lance_csr/` directory, with its locked dependencies installed. Do not use an absolute virtual-environment Python child that drops uv's wheel overlay. Explicitly load the wheel in every child command.

```bash
unset WRITE_GOLDEN
LANCE_CSR_MODE=memory uv run --offline --no-sync --with "$CANDIDATE_WHEEL" python -m pytest benchmark_query.py -q --benchmark-min-rounds=5 --benchmark-min-time=0.000005 --benchmark-max-time=1.0 --benchmark-timer=time.perf_counter --benchmark-calibration-precision=10 --benchmark-warmup=off --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname --benchmark-save-data --benchmark-json="$RESULT_JSON"
```

Repeat with `LANCE_CSR_MODE=lazy` and both complete suites. Record the actual imported extension digest, interpreter/dependency versions, dataset manifest digests, source commit and wheel hash. Stop builds, other benchmarks and profiling before collecting timings; run cells serially on the same host. Repeat flagged regressions in paired and reversed order. Preserve raw failures, partial outputs and the selected protocol rather than relabeling them as passing measurements.

The results are local engineering comparisons, not an official audited SNB benchmark result. Dataset distribution conditions remain those of its source; the evidence bundle contains no SF1 records.

Regenerated Lance file UUIDs and manifest digests can differ; the archive digest and pinned ingestion code define the input, while each comparison freezes and verifies its own resulting dataset manifests. Golden result checks remain unchanged.

Raw evidence retains its original CSV line endings and reference-query whitespace. The evidence branch scopes Git whitespace handling to those raw files and spelling exceptions to two exact verbatim receipt fields; source and prose checks remain strict.
