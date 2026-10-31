# Releasing lance-csr

A release publishes the Python package to [PyPI](https://pypi.org/project/lance-csr/) and the
`lance-csr-catalog` and `lance-csr` crates to [crates.io](https://crates.io/crates/lance-csr). PyPI publishing is
automated from a GitHub release, and the crates follow once PyPI has the new version.

The whole release takes about 50 minutes: the macOS wheel build, then the crates. Don't add steps that build the
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
| `publish-crates` | about 10 to 15 min | after `publish`: packages and verifies `lance-csr-catalog` and `lance-csr`, then uploads them through crates.io trusted publishing |

The build jobs run in parallel. Nothing reaches PyPI unless every build and test job passes, and nothing
reaches crates.io unless PyPI publishing succeeds. The `pypi` and `crates-io` environments only accept `v*`
tags. The PyPI trusted publisher is registered for workflow `python-publish.yml` and environment `pypi`, and
the crates.io trusted publishers for both crates for the same workflow and environment `crates-io`, so don't
rename any of them.

**Don't run a separate `mode=dry_run` before a release.** The release run builds and tests the same wheels
before it publishes, so a dry run only doubles the wait. Changes to how wheels are built (this workflow,
`ci/check_wheel.py`, `python/.cargo/`) already build and test the release wheels in their own PR.

### If the release run fails

If a build or test job failed, nothing has been published, so nothing needs undoing on PyPI.

- **A flaky test or a runner problem** (for example a GitHub runner image outage): rerun the failed jobs with
  `gh run rerun <run id> --failed`.
- **A real bug:** fix it on `main` through a normal PR, then release the next patch version. Don't move or reuse
  a tag that has already been released.

### If only `publish-crates` fails

PyPI already has the release, so don't release again. Rerun just that job with `gh run rerun <run id> --failed`;
the wheels aren't rebuilt.

Cargo publishes `lance-csr-catalog` first, since `lance-csr` depends on it. If it published the catalog and
then failed on `lance-csr`, a rerun fails because the catalog version already exists. Publish the rest by hand
from the release tag: a crate owner runs `cargo publish -p lance-csr`. If **Require trusted publishing** is on
for the crate, crates.io rejects API tokens, so turn it off for the manual publish and back on afterwards.
`lance-csr-python` and `lance-csr-benches` are `publish = false`.

## 4. Check the release

```bash
uv run --no-project --with lance-csr==A.B.C python -c "import lance_csr, importlib.metadata as m; print(m.version('lance-csr'))"
```

Then close any milestone or tracker issues the release completes.
