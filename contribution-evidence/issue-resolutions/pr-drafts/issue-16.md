# perf(csr): reuse filter columns during target materialization

Addresses duplicate property reads in #16. Columns fetched for a pushed target predicate are filtered once and reused when projection also needs them. Remaining properties are fetched only for survivors. Reuse stays within one bounded output batch and its pinned snapshot; this adds no full-column cache, Python API or dependency. Existing projection pruning and Lance row sorting/deduplication retain their roles.

Real Arrow/Lance tests cover duplicate and out-of-order positions, null predicates, empty results, overlapping/disjoint/empty projections, reconstructed IDs and unused columns. A read-cell counter verifies six candidate ages plus four surviving names require ten cells, without repeated age or ID reads. Complete workspace tests/Clippy, 351 Python tests, lint/typing, strict docs, spelling and executable guides pass. The CSR guide and existing benchmark describe and exercise filter/projection reuse.

Head `447355dbaf90993d33926afc48ab6712e421505a`; frozen base `1bf6d42141b9cc6ee1824d000f49cdc0ebd9d8fe`. The [release receipt](../release-receipts/issue-16/release-proof.json) records installed/archive identity and 159 installed focused tests outside the source checkout.

Both complete suites pass unchanged goldens in both modes. Recorded after/before ratios against the actual parent are:

| Query | Memory | Lazy |
| --- | ---: | ---: |
| IC2 | 1.037 | 0.851 |
| IC3 | 1.028 | 0.906 |
| IC9 | 1.053 | 0.865 |
| Q12 | 1.020 | 0.943 |
| Q20 | 0.988 | 1.003 |

These are the retained controlled first-pass measurements; lower is faster. [Raw comparisons](../comparisons-original/issue-16-vs-issue-13-ic-lazy.json) and the [focused read profile](../focused/README.md) retain all rows. Broader main-to-head requested-cell/ObjectStore differences also include prerequisite #11/#13 changes and cannot be attributed solely to this diff. ObjectStore requests are not physical disk I/O; separate decoding attribution remains unverified.

Immediate-parent repeat gates pass. The frozen head retains inherited main Q30 flags; the qualified combined source applies the coordinated CSR correction. Keep this as a coordinated draft under the [integration record](../INTEGRATION.md), and keep #16 open if reviewers find the measured improvement insufficient.

Submitted by David Hughes (@stugorf), with AI Collaboration.
