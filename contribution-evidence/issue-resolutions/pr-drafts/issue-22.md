# feat(python): ship public API type stubs

Resolves #22. Publish PEP 561 typing metadata for the compiled `lance_csr._internal.graph` module and the public facade, including all enums, graph/query classes, and `DirNamespace`. Signatures match the PyO3 definitions; dataset dictionaries, scalar parameters, execution options, and statistics returns receive concrete types.

A strict consumer exercises the public surface, including expected type errors. Runtime tests compare exported classes, members, argument kinds, and defaults against the installed stubs. The wheel smoke check now requires the marker and both stub files. The handwritten API reference and Python development guide explain usage and local validation. `pyarrow-stubs` is a development dependency only.

## Validation

- `make lint`: Ruff, formatting, and Pyright pass with zero errors or warnings.
- `uv run --project docs --locked zensical build --strict` and `uvx typos@1.26.0 .`: pass.
- `maturin sdist --out /private/tmp/lance-csr-issue-22-dist` and `maturin build --out /private/tmp/lance-csr-issue-22-dist`, both from `python/`: pass. Built from commit `6175122` in an isolated worktree and Cargo target with local crates cleaned before rebuilding.
- Archive inspection: both wheel and sdist include `lance_csr/py.typed`, `_internal/__init__.pyi`, and `_internal/graph.pyi`.
- Clean installed-wheel verification outside the checkout, Python 3.12.9, macOS arm64: `ci/check_wheel.py` passes Arrow-memory, Lance-disk, CSR, shortest-path, and refresh cases without Rust on PATH.
- Installed-wheel runtime/stub parity: 3 tests pass, including deliberately removed-member and changed-default detection.
- Copied strict consumer checked against the installed wheel: Pyright reports zero errors, warnings, or informational diagnostics.
- Verification log: `/private/tmp/lance-csr-runtime/issue-22-wheel-verification.log`.

Wheel SHA-256: `062c5f8731d22023456e1c373b0ac5320c45c9d2b987097921d8a9566719f7bc`.
Source distribution SHA-256: `23a1ec6cd1c58d0647034b12755f03c9e6aa09d247183f5cbdffb242465bf89e`.

The debug wheel and source-distribution artifacts above remain retained. A separate release wheel built with `maturin build --release --locked` from `python/` now passes the same outside-checkout smoke, all 3 runtime/stub parity tests, and the strict Pyright consumer on Python 3.12.9 / macOS 26.6.2 arm64. The installed extension's bytes match the wheel archive exactly; Cargo logs show all three local crates recompiled from this branch after release-profile cleanup.

Release wheel: `/private/tmp/lance-csr-submissions/wheels/issue-22/lance_csr-0.2.0-cp312-abi3-macosx_11_0_arm64.whl`.
Release SHA-256: `a8559f2adc008c13385ed017de5bb83d8cbf88f11d00a7afa8d4243a5de6e2ad`.
Release proof: `/private/tmp/lance-csr-submissions/wheels/issue-22/release-proof.json`.

Other operating systems and the remaining release-wheel CI matrix are unrun locally. No external datasets or SF1 timings were used for wheel verification; this patch changes no query implementation.

## Frozen review and acceptance

Head `6175122bd24cc8fe95ddd4cf82d7492975ea6e7d`; actual dependency base `54e02307a48e467a98d9da2183f5adcb767e7c99`. The [release proof](../release-receipts/issue-22-cp313/release-proof.json) records wheel SHA-256 `34c933d1b215fec095e24324532f69d447e5f0af38eec214279c1ef32c90af27` and installed/archive binary identity. Its outside-checkout consumer passed 3 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. Paired/reversed repeats are complete. No regression remains above the agreed 10% gate after the recorded follow-up investigations. The [original flags and repeats](../repeat-investigations/original/acceptance.json), [smaller-flag pilot](../repeat-investigations/small-flags-pilot/summary.json) and [fixed-size entity study](../repeat-investigations/entity-precision/summary.json) remain separate; earlier measurements are preserved. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

## Workload results

Geometric mean speedup from the original complete-suite cells; values above 1 are faster. Repeat investigations determine the regression gate.

| Compared with | Workload | Memory | Lazy |
| --- | --- | ---: | ---: |
| main-cp313 | Q30 | [0.984×](../comparisons-original/issue-22-cp313-vs-main-cp313-q30-memory.md) | [0.908×](../comparisons-original/issue-22-cp313-vs-main-cp313-q30-lazy.md) |
| main-cp313 | IC | [0.987×](../comparisons-original/issue-22-cp313-vs-main-cp313-ic-memory.md) | [0.993×](../comparisons-original/issue-22-cp313-vs-main-cp313-ic-lazy.md) |

Build, dataset and wheel hashes are recorded in the linked release proof; [reproduction instructions](../REPRODUCE.md) cover clean source checks, both upstream runners and the serial timing protocol.

Publication is held for owner approval. Other operating systems have not been verified locally.

Submitted by David Hughes (@stugorf), with AI Collaboration.
