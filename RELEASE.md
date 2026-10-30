# Releasing lance-csr

A release publishes the Python package to [PyPI](https://pypi.org/project/lance-csr/) and the
`lance-csr-catalog` and `lance-csr` crates to [crates.io](https://crates.io/crates/lance-csr). PyPI publishing is
automated from a GitHub release. crates.io publishing is done by hand, afterwards.

The whole release takes about 40 minutes, almost all of it the macOS wheel build. Don't add steps that build the
release wheels a second time.

## Before you start

- Everything going into the release is merged to `main`, and `main`'s CI is green.
- Performance claims in the merged PRs already have the benchmark evidence `CONTRIBUTING.md` asks for. A release
  doesn't rerun the benchmark suites.
- Decide the version. Until 1.0, a change that breaks the Python or Rust API (a `!` in the commit type, such as
  `refactor(config)!:`) bumps the minor version (0.4.0 → 0.5.0). Anything else bumps the patch version.

## 1. Bump the version

From a branch off the latest `main`:

```bash
uvx bump-my-version bump minor   # or: patch
cargo update --workspace
cargo update --workspace --manifest-path crates/lance-csr-python/Cargo.toml
(cd python && uv lock)
```

`bump-my-version` updates `.bumpversion.toml`, the four crate `Cargo.toml` files and `python/pyproject.toml`. It
doesn't update the three lockfiles that also carry the version, which is what the other three commands are for.
The diff should be 9 files and 15 lines each way, touching only version lines. If `uv lock` or `cargo update`
moves a dependency, undo that and update only the workspace packages. Add `--allow-dirty` to `bump-my-version` if
untracked files in your checkout stop it.

Check that no docs or READMEs pin the old version, for example with `git grep -n '0\.4'`.

Open a PR titled `chore: bump version X.Y.Z → A.B.C`. The commit body says why it's a minor or patch bump.
Squash-merge it once CI passes.

## 2. Write the release notes

Follow the earlier releases on the [Releases page](https://github.com/hyperdimensionalcomputingai/lance-csr/releases):

- **Breaking:** each API break, which languages it affects, and the replacement.
- **Query results that change**, if any: queries that now return different rows.
- **New features and performance:** grouped by area, with the benchmark numbers that matter, measured against
  the previous release.
- **Known trade-offs:** any query that got slower, by how much, and the issue that tracks it.
- **Other:** tests, CI and docs PRs, in one line.
- **PRs:** one line per PR (`* title by @author in <url>`), then the full changelog link
  (`compare/vX.Y.Z...vA.B.C`).

Have the maintainer approve the notes before you publish. The release is public as soon as it exists.

## 3. Publish the GitHub release

Create the release on the bump's squash-merge commit, with a tag that matches the version exactly:

```bash
gh release create vA.B.C --target <merge commit> --title vA.B.C --notes-file <notes.md> --latest
```

Publishing it starts `.github/workflows/python-publish.yml`:

| Job | Time | What it does |
| --- | ---: | --- |
| `build-linux` | about 22 min | manylinux_2_28 x86-64 wheel and the sdist |
| `build-macos` | about 35 min | macOS arm64 wheel, built natively on `macos-15` |
| `test` | under 1 min each | each wheel on Python 3.12, 3.13 and 3.14 |
| `publish` | seconds | checks the tag matches `python/pyproject.toml`, then uploads through PyPI trusted publishing |

The build jobs run in parallel. Nothing reaches PyPI unless every build and test job passes. The `pypi`
environment only accepts `v*` tags, and the PyPI trusted publisher is registered for workflow
`python-publish.yml` and environment `pypi`, so don't rename either.

**Don't run a separate `mode=dry_run` before a release.** The release run builds and tests the same wheels
before it publishes, so a dry run only doubles the wait. Changes to how wheels are built (this workflow,
`ci/check_wheel.py`, `python/.cargo/`) already build and test the release wheels in their own PR.

### If the release run fails

Nothing has been published, so nothing needs undoing on PyPI.

- **A flaky test or a runner problem** (for example a GitHub runner image outage): rerun the failed jobs with
  `gh run rerun <run id> --failed`.
- **A real bug:** fix it on `main` through a normal PR, then release the next patch version. Don't move or reuse
  a tag that has already been released.

## 4. Publish the crates

Only after PyPI shows the new version (`https://pypi.org/pypi/lance-csr/json`), so the two registries never
disagree. A maintainer with a crates.io token runs this from the merged `main`:

```bash
cargo publish -p lance-csr-catalog -p lance-csr
```

Cargo publishes `lance-csr-catalog` first, since `lance-csr` depends on it. `lance-csr-python` and
`lance-csr-benches` are `publish = false`.

## 5. Check the release

```bash
uv run --no-project --with lance-csr==A.B.C python -c "import lance_csr, importlib.metadata as m; print(m.version('lance-csr'))"
```

Then close any milestone or tracker issues the release completes.
