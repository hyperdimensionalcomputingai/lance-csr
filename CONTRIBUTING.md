# Contributing to lance-csr

lance-csr aims to be a fast, correct, **single-machine** Cypher engine on Lance (see the
[project goals](README.md#goals)). Distributed execution is out of scope. Contributions within those goals
are welcome, from people and from their coding agents.

What matters in a pull request:

- **A clear reason:** the problem it solves, or the queries it makes faster.
- **Correct results:** every test and benchmark answer still checks out.
- **Evidence:** a regression test for a bug fix; before-and-after numbers from both benchmark suites for a
  code change.
- **No surprises:** no unexplained slowdowns elsewhere.

The sections below cover each of these, including a step-by-step benchmark harness you can hand to a coding
agent.

## Start with the why

Before writing code, be able to answer these questions. Your pull request description should answer them
too.

1. **What is the problem?** A query that returns the wrong rows, fails, or runs slowly. Show it.
2. **Who runs into it?** A workload, a query shape, a data size. "It could be faster" is not a problem yet.
3. **What is the evidence?** A reproducer, a profile, or a benchmark run that shows the problem on `main`.
4. **Why does the fix belong in lance-csr?** Rather than in DataFusion, Lance, or the user's query.
5. **What does it cost?** New code paths, memory, build time, API surface.
6. **What else did you try?** Approaches you rejected, and why.

For a larger change, open an [issue](https://github.com/hyperdimensionalcomputingai/lance-csr/issues) with
these answers before you start, so that we can agree on the approach before you spend time on it.

## What evidence each change needs

| Change | Evidence |
| --- | --- |
| Bug fix | A regression test that fails on `main` and passes on your branch. |
| Performance improvement | Before-and-after results from both benchmark suites, built with the [harness below](#build-the-benchmark-harness). |
| New feature or Cypher support | Tests, documentation, and benchmark results showing that existing queries didn't get slower. |
| Refactor | Tests pass, and benchmark results if it touches planning, execution or the CSR. |
| Documentation only | None. |

Every code change also needs the usual checks to pass; see [Set up, test and lint](#set-up-test-and-lint).

## The benchmark suites

We guide performance work with two suites over the GDC Social Network Benchmark SF1 dataset (3.18M nodes,
17.3M relationships). Each lives in its own repository, next to the runners for the other engines, and every
query checks its result against an expected answer:

- [graph-benchmark-ldbc](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc): 30 custom
  Cypher queries. lance-csr's runner is in `lance_csr/`, a uv project of its own.
- [graph-benchmark-ldbc-complex](https://github.com/hyperdimensionalcomputingai/graph-benchmark-ldbc-complex):
  the 14 Interactive complex read queries (IC1–IC14). lance-csr's runner is in `lance_csr/` and uses the
  repository's root uv project.

Both run lance-csr in two modes, selected by `LANCE_CSR_MODE`: `memory` holds every table as Arrow in memory,
and `lazy` reads the Lance datasets from disk as queries need them. A change can speed up one mode and slow
down the other, so measure both.

The micro-benchmarks in [`python/benchmarks/`](python/benchmarks/) and
[`crates/lance-csr-benches/`](crates/lance-csr-benches/README.md) are useful for isolating one operator,
and you're welcome to include their numbers. They support a claim; they don't replace the two suites.

## Build the benchmark harness

The runners pin `lance-csr` to a release from PyPI. Running `./run_benchmarks.sh` as it is measures that
release, not your branch. The harness below builds two wheels, one from `main` and one from your branch, and
runs the same runner against each with `uv run --with`, which loads the wheel you name in place of the pinned
release and leaves the runner's files untouched.

### 1. Set up the suites once

Clone both benchmark repositories next to your lance-csr checkout, and follow their READMEs to download the
SF1 dataset and build the Lance datasets: `lance_graph/` in graph-benchmark-ldbc (its `lance_csr/` runner
reads `../lance_graph/graph_lance`), and `lance_csr/build_graph.py` in graph-benchmark-ldbc-complex. This is
the slow part, and you only do it once.

```text
code/
├── lance-csr/
├── graph-benchmark-ldbc/
└── graph-benchmark-ldbc-complex/
```

### 2. Build a wheel from `main` and one from your branch

From `lance-csr/python/`, with the development environment set up (`make install`, see
[`python/DEVELOPMENT.md`](python/DEVELOPMENT.md)):

```bash
export BENCH=~/lance-csr-bench
git switch main
rm -rf "$BENCH/wheels/main" && uv run --no-sync maturin build --release --out "$BENCH/wheels/main"
git switch my-branch
rm -rf "$BENCH/wheels/branch" && uv run --no-sync maturin build --release --out "$BENCH/wheels/branch"
```

Always benchmark release builds; debug builds run queries many times slower. Each release build takes
several minutes. Both wheels report the same version, so write down the commit each one was built from.
Rebuild the branch wheel after every change you want to measure.

### 3. Run each suite against each wheel

From a benchmark repository's root, this runs one suite, in one mode, against one wheel, with the settings
from that repository's `run_benchmarks.sh`, and saves the results under `$BENCH/runs`:

```bash
bench() {  # usage: bench <main|branch> <memory|lazy>
  local wheel=$(ls "$BENCH/wheels/$1"/lance_csr-*.whl 2>/dev/null)
  [ -f "$wheel" ] || { echo "no single wheel in $BENCH/wheels/$1" >&2; return 1; }
  local suite=$(basename "$PWD")
  (cd lance_csr && LANCE_CSR_MODE=$2 uv run --frozen --with "$wheel" \
    pytest benchmark_query.py -p no:faulthandler \
    --benchmark-min-rounds=5 --benchmark-min-time=0.000005 --benchmark-max-time=1.0 \
    --benchmark-timer=time.perf_counter --benchmark-calibration-precision=10 \
    --benchmark-warmup=off --benchmark-warmup-iterations=5 --benchmark-disable-gc \
    --benchmark-sort=fullname \
    --benchmark-storage="file://$BENCH/runs/$suite" --benchmark-save="$2-$1")
}
```

Before timing anything, check that the runner loads the wheel and not the pinned release. The path printed
should be in uv's cache, not in the runner's `.venv`:

```bash
(cd lance_csr && uv run --frozen --with "$BENCH"/wheels/branch/lance_csr-*.whl \
  python -c "import lance_csr; print(lance_csr.__file__)")
```

Then alternate the builds, so that a change in machine load affects both equally, and run each build twice:

```bash
for mode in memory lazy; do
  bench main $mode; bench branch $mode; bench main $mode; bench branch $mode
done
```

Run this in both repositories. Keep the machine otherwise idle and plugged in, and use the same machine for
every run. Each run's tests check results against the expected answers: a failing test means the change is
wrong, however fast it is.

### 4. Compare

`pytest-benchmark compare` lists the saved runs side by side. From a benchmark repository's root:

```bash
suite=$(basename "$PWD")
(cd lance_csr && uv run --frozen pytest-benchmark compare "$BENCH/runs/$suite/*/*memory-*.json" \
  --group-by=name --columns=mean,stddev --time-unit=ms)
```

For each suite and mode, report the mean latency per query on `main` and on your branch, the change, and
the geometric mean of the per-query ratios. The difference between the two `main` runs tells you how much
the numbers move on your machine without any code change.

### How to read the results

Queries in these suites take from under a millisecond to about a second, and a query's time can move by
15–20% between runs on the same machine, in either direction. Treat changes within that range as noise:
don't claim them as improvements, and we won't hold them against you as regressions.

Changes beyond that range are worth looking at. A real improvement shows up in both runs of your branch, by
more than the gap between the two `main` runs. If several queries get slower by more than that range,
investigate before you open the pull request: profile them, find out why, and explain it in the description.
A regression with a good explanation, traded for a larger gain, can still be the right change; we'll work it
out with you in review.

## A brief for your coding agent

To have an agent produce the evidence, give it this guide and a brief like this one:

> Measure the performance effect of the branch `my-branch` in the lance-csr checkout at `<path>`, following
> "Build the benchmark harness" in its `CONTRIBUTING.md`. Build release wheels from `main` and from the
> branch. Before timing, confirm the runner imports each wheel rather than the pinned release. Run both
> suites, in both `memory` and `lazy` modes, alternating `main` and branch runs twice each.
> Don't edit the benchmark queries, expected answers or runner settings. Report every query, not only those
> that improved: for each suite and mode, a table of mean latency on `main` and on the branch with the
> change, the geometric mean of the ratios, and the gap between the two `main` runs. List any failing
> tests. Record the commits of lance-csr and both benchmark repositories, and the machine, OS and memory.

Read what the agent reports before you post it. You're the author of the pull request, and its numbers have to
hold up when someone else reproduces them.

## Why it's worth the effort

- **Pull requests with reproducible numbers get reviewed first.** A table we can reproduce answers most of
  the questions we would otherwise ask in review.
- **Without numbers, we'll ask for them.** A performance pull request without results waits until it has
  them.
- **Finding a problem is a contribution.** If the harness shows a query where lance-csr is slow or wrong,
  an issue with the query and your numbers is valuable on its own, even without a fix. So is a new query
  added to one of the benchmark repositories that covers a workload they miss.

## Set up, test and lint

[Contributing](docs/contributing.md) in the docs covers the setup (Rust, `protoc`, Python, uv), the test and
lint commands, and how to build the documentation site; [`python/DEVELOPMENT.md`](python/DEVELOPMENT.md) has
the details of the Python build. In short, never use `pip`, and run these before you push:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
cd python && make test && make lint
```

When you change user-facing behaviour or the Python API, update the affected guide pages and
`docs/api-reference.md` in the same pull request, then run
`uv run --project docs --locked zensical build --strict` and `uvx typos@1.26.0 .`.

## Report a bug or suggest an improvement

Search the [existing issues](https://github.com/hyperdimensionalcomputingai/lance-csr/issues) first. For a
bug, include a small reproducer, the expected and actual results, your lance-csr and Python versions, and your
operating system. Use synthetic data when you can, and remove credentials and private data. For an
improvement, describe the use case and how the change would help users.

## Open a pull request

- Keep it focused on one change, and follow the commit style in the history (`feat(graph):`, `fix(cypher):`,
  `docs:`, `refactor(query):`), with an imperative subject of at most 72 characters.
- Fill in the [pull request template](.github/pull_request_template.md): the problem and why the change is
  needed, the checks you ran, the benchmark tables, and the effect on users.
- Agent-assisted pull requests are held to the same standard. Review the code and the numbers yourself
  before you submit.
