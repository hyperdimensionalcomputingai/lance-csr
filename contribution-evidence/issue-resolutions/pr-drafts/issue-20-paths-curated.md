# Preserve named path and relationship values

Part of #20. Keep the tracker open for the other feature groups.

Named paths now retain ordered node and actual relationship occurrences through RETURN, collect and
UNWIND. Parallel relationships remain distinguishable even with identical endpoints and properties.
`relationships`, `startNode` and `endNode` expose the stored records. Public `nodes(path)` remains a list
of integer keys; property comprehensions use typed node records to preserve identity across labels.
Shortest paths that cross labels retain that identity even when only length or keys are requested.
Homogeneous length/key consumers keep their compact CSR route through WITH aliases.

The inventory reads each participating source once per query and shares it between path operators.
Its edge identities are query-local occurrence ordinals, not persistent IDs. Inventory memory grows
with participating tables; bounded traversal/output does not make a rich-path query constant-memory.
Duplicate non-null node IDs within one label and incompatible mixed-label Arrow property types fail
explicitly. Ordinary variable-length paths retain the 20-hop limit; shortest paths retain their
existing uncapped default. Guides describe the Arrow record contract, limits and executable examples.
The Rust README covers PathPattern.variable and LogicalOperator::PathMatch migration.

Proposed branch: `codex/issue-20-path-null-recheck`, source `d595e377322e2105f7fbd1bb16d11f6afd776198`.
**Compare against `84fcac65be2d4f5104061da1af16294656875b1a`**, before any rich-path code.
That parent is current language/folds `88dd97e8` plus the independently reviewed #11/#13/#16/#17
changes.
Fold implementation is inherited, not included in this submission diff. CALL and UNION are separate.

This slice exposes the existing bidirectional BFS internally and applies cooperative raw-candidate
checkpoints needed by inventory traversal. Those private helper changes overlap the coordinated #19
cancellation work; no #19 Python timeout parameter or bridge is included here. Future plan caching
must bypass rich PathMatch operators until private property predicates are safely bindable. CALL/UNION
integration must carry the same GraphConfig and shared query source inventory through nested scopes.

Exact clean-source checks passed 617 Rust workspace tests, 961 Python tests, 43 executable documentation blocks, workspace all-target Clippy, formatting, Ruff, Pyright, strict Zensical and pinned spelling. The final curated optimized wheel also passed the unmodified canonical IC14 query and existing port against all 58 immutable golden paths and weights, with result SHA-256 `7a2c586b7a491e4341fd8b638e759c6a3ba42c048a3df4ce709bb0b386dbc7c9`. This canonical run establishes correctness; its elapsed time was collected during builds and is excluded from controlled performance claims.

Path null checks, including Boolean/CASE/list predicates and WITH aliases, now retain the compact homogeneous shortest-path route. Null checks bind to its length column; rich path demand remains separate. The [same frozen regression inputs](../correction-proofs/path-null-paired-proof-r1.json) fail 16 times before and pass 16 times after. Corrected-owner Q30/IC comparisons pass all golden checks and all immediate-parent repeat gates; inherited main-comparison flags remain held for the shared CSR correction.

## Frozen review and acceptance

Head `d595e377322e2105f7fbd1bb16d11f6afd776198`; actual dependency base `84fcac65be2d4f5104061da1af16294656875b1a`. The [release proof](../release-receipts/issue-20-path-null-corrected/release-proof.json) records wheel SHA-256 `90351c7e49d7b9099e6baed212c49b625547ea544818e059a589e3aa4c93ab87` and installed/archive binary identity. Its outside-checkout consumer passed 961 tests; smoke and typing details are recorded in that exact receipt.

The complete Q30 and IC suites passed unchanged golden checks against main and the actual parent, using both memory and lazy input modes. First-pass tables are retained without recalculation. The frozen head retains the recorded main-comparison prerequisite flags; immediate-parent gates pass. The qualified combined source applies the shared CSR and compact path-null corrections. This remains a coordinated draft until its actual target matches the [integration record](../INTEGRATION.md); original frozen-head flags are not relabeled as passing. Reproduction commands, dataset checksums, hardware, dependencies and all per-query results accompany the evidence bundle.

Keep the #20 tracker open until every approved finite feature group has passed acceptance. No full Cypher-conformance claim is made.

Submitted by David Hughes (@stugorf), with AI Collaboration.
