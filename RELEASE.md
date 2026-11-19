# Releasing lance-csr

A release publishes the Python package to [PyPI](https://pypi.org/project/lance-csr/), the
`lance-csr` crate to [crates.io](https://crates.io/crates/lance-csr) and the Node.js packages to
[npm](https://www.npmjs.com/package/lance-csr). PyPI and npm publishing are automated from a GitHub
release, and the crates follow once PyPI has the new version.

The whole release takes about 65 minutes: the Windows wheel build, then the crates. Don't add steps that build the
release wheels a second time.

## Before you start

- Everything going into the release is merged to `main`, and `main`'s CI is green.
- Performance claims in the merged PRs already have the benchmark evidence `CONTRIBUTING.md` asks for. A release
  doesn't rerun the benchmark suites.
- `lance-csr` is on the newest lance release, and its arrow, datafusion and object_store requirements are the
  ones that lance release's workspace `Cargo.toml` uses. A Rust project that depends on both lance and `lance-csr`
  otherwise builds two copies of lance (or arrow, or datafusion), and their types can't be passed between them.
  0.11.0 shipped on lance 13, almost three weeks after lance 14.0.0 came out. Compare:

  ```bash
  cargo search lance --limit 1
  gh api 'repos/lance-format/lance/contents/Cargo.toml?ref=vX.Y.Z' -q .content | base64 -d \
    | grep -E '^(arrow|datafusion|object_store)[a-z-]* ='
  git grep -n -E '^(lance|arrow|datafusion|object_store)[a-z-]* =' -- 'crates/*/Cargo.toml'
  ```

  If lance has a newer release, upgrade to it in its own PR and merge that before the version bump. Copy lance's
  requirement strings as they are (`"58.0.0"`, a caret range, not `"=58.0.0"`). A new lance major is a breaking
  change for Rust users, so it bumps our minor version.
- Decide the version. Until 1.0, a change that breaks the Python or Rust API (a `!` in the commit type, such as
  `refactor(config)!:`) bumps the minor version (0.4.0 → 0.5.0). Anything else bumps the patch version.

## 1. Bump the version

From a branch off the latest `main`:

```bash
uvx bump-my-version bump minor   # or: patch
cargo update --workspace
cargo update --workspace --manifest-path crates/lance-csr-python/Cargo.toml
cargo update --workspace --manifest-path crates/lance-csr-node/Cargo.toml
(cd python && uv lock)
(cd nodejs && npm install --package-lock-only)
```

`bump-my-version` updates `.bumpversion.toml`, the four crate `Cargo.toml` files, `python/pyproject.toml`,
`nodejs/package.json`, the three platform packages' `nodejs/npm/*/package.json` and the generated napi loader
`nodejs/native.js`. It doesn't update the five lockfiles that also carry the version, which is what the other
five commands are for. The diff should be 16 files and 76 lines each way, touching only version lines. If
`uv lock`, `npm install` or `cargo update` moves a dependency, undo that and update only the workspace packages.
Add `--allow-dirty` to `bump-my-version` if untracked files in your checkout stop it.

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

Publishing it starts `.github/workflows/python-publish.yml` and `.github/workflows/nodejs-publish.yml`, which run
side by side. `python-publish.yml`:

| Job | Time | What it does |
| --- | ---: | --- |
| `build-linux` | about 22 min (x86-64), TBD (aarch64) | manylinux_2_28 x86-64 and aarch64 wheels, each built natively (`ubuntu-24.04`, `ubuntu-24.04-arm`), and the sdist |
| `build-macos` | about 35 min | macOS arm64 wheel, built natively on `macos-15` |
| `build-windows` | about 52 min | Windows x86-64 wheel, built natively on `windows-2022` |
| `test` | under 1 min each | each wheel on Python 3.12, 3.13 and 3.14 |
| `publish` | seconds | checks the tag matches `python/pyproject.toml`, then uploads through PyPI trusted publishing |
| `publish-crates` | about 10 to 15 min | after `publish`: packages and verifies `lance-csr`, then uploads it through crates.io trusted publishing |

`nodejs-publish.yml`:

| Job | Time | What it does |
| --- | ---: | --- |
| `build-linux`, `build-macos` | TBD | the native binary for each platform, built as the wheels are (zig for glibc 2.28 on Linux); there is no Windows package |
| `pack` | a few minutes | `lance-csr` and the three platform packages, as the tarballs the later jobs install and publish |
| `test` | a few minutes each | the whole Node.js test suite against each platform's release binary, then the installed tarballs, on Node.js 22, 24 and 26 |
| `publish` | seconds | checks the tag matches `nodejs/package.json`, then publishes the platform packages and `lance-csr` through npm trusted publishing |

The build jobs run in parallel and restore no caches (no sccache, no uv cache), so nothing a pull request's run left
behind can end up in a release wheel. Nothing reaches PyPI unless every build and test job passes, and nothing
reaches crates.io unless PyPI publishing succeeds. The `pypi`, `crates-io` and `npm` environments only accept `v*`
tags. The PyPI trusted publisher is registered for workflow `python-publish.yml` and environment `pypi`, and
the crates.io trusted publisher for `lance-csr` for the same workflow and environment `crates-io`, and the npm
trusted publisher for each of the four npm packages for workflow `nodejs-publish.yml` and environment `npm`, so
don't rename any of them.

npm can only register a trusted publisher for a package that already exists, so each of the four npm packages
was first published by hand as an empty `0.0.0` placeholder. Each has a trusted publisher (this repository,
`nodejs-publish.yml`, environment `npm`) that may run `npm publish`, and requires two-factor authentication with
no bypass tokens, so the workflow publishes every release, the first included. A new platform package needs the
same: publish a placeholder, then add its trusted publisher, before the release that ships it.

**Don't run a separate `mode=dry_run` before a release.** The release run builds and tests the same wheels
before it publishes, so a dry run only doubles the wait. Changes to how wheels are built (this workflow,
`ci/check_wheel.py`, `python/.cargo/`) already build and test the release wheels in their own PR, and changes to
how the npm packages are built (`nodejs-publish.yml`, `ci/check_npm_package.mjs`, `nodejs/.cargo/`,
`nodejs/npm/`) do the same for the npm packages.

### If the release run fails

If a build or test job failed, that workflow has published nothing, so nothing needs undoing on PyPI or npm.

- **A flaky test or a runner problem** (for example a GitHub runner image outage): rerun the failed jobs with
  `gh run rerun <run id> --failed`.
- **A real bug:** fix it on `main` through a normal PR, then release the next patch version. Don't move or reuse
  a tag that has already been released.

### If only the npm `publish` job fails

The workflow skips any package version npm already has, so rerun the failed job with
`gh run rerun <run id> --failed`; the binaries aren't rebuilt.

### If only `publish-crates` fails

PyPI already has the release, so don't release again. Rerun just that job with `gh run rerun <run id> --failed`;
the wheels aren't rebuilt.

`lance-csr-python` and `lance-csr-benches` are `publish = false`. Up to 0.7.1, the catalog types shipped
in a separate `lance-csr-catalog` crate; it is no longer published.

## 4. Check the release

```bash
uv run --no-project --with lance-csr==A.B.C python -c "import lance_csr, importlib.metadata as m; print(m.version('lance-csr'))"
npm view lance-csr@A.B.C version
```

Then close any milestone or tracker issues the release completes.
