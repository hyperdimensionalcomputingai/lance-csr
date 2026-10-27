# Three submitted pull requests

Verified 2026-10-27T00:08:29.587895+00:00. All are open, ready for review, authored by `stugorf`, and target `main`. Nothing is merged; auto-merge is disabled.

| PR | Upstream bug | Exact tested head |
|---|---|---|
| [#65 — fix(query): preserve empty-input distinct counts](https://github.com/hyperdimensionalcomputingai/lance-csr/pull/65) | #123 | `80d4438884872ce9d18c528caa1519441a9460ce` |
| [#66 — fix(planner): preserve physical column casing in graph scans](https://github.com/hyperdimensionalcomputingai/lance-csr/pull/66) | #105 | `e6df15716a052af380e8a5d2da340cc506ecd188` |
| [#67 — fix(parser): accept one trailing Cypher semicolon](https://github.com/hyperdimensionalcomputingai/lance-csr/pull/67) | #101 | `28096751241a8707042091e5012f99ce1f32f594` |

Every candidate passed the debug checks before any final production build. Each final installed wheel passed its full Python suite, and both complete SF1 suites passed in memory and lazy modes. Paired checks resolved the initial >10% timing flags; original observations remain unchanged.

The [frozen validation evidence](https://github.com/hyperdimensionalcomputingai/lance-csr/tree/4e6349469c4e5a436136e74a29e153e68e5b6f79/contribution-evidence/upstream-fixes-0.3.1) is published and linked in every PR. Its remote archive blob and size were verified against the local artifact. Source branches and evidence are public; native wheels and datasets remain local.

Hosted checks had no failures at this initial snapshot. Rust, Clippy and Python jobs were still running; use each PR’s checks for current status. See [PUBLISHED_PRS.json](PUBLISHED_PRS.json) for exact snapshots and body hashes.

Deferred from this work: untyped variable-length relationships (#104), factorized intermediates (#163), optional Lance/DataFusion-only builds (#171), and JNI (#115).

Submitted by David Hughes (@stugorf), with AI Collaboration.
