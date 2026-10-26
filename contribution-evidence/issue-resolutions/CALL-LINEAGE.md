# Focused correlated CALL submission: dependency and patch ownership

Submitted by David Hughes (@stugorf), with AI Collaboration.

Status: assembly completed and exact source/release qualification passed. Frozen CALL head `f2cc84c64898185bf8ea4826b276b61ea8b20b63` compares with actual prerequisite `32fee35e2aad15167a7d33ec0d9eee9b3d268b5b`. Full source checks passed 641 Rust workspace tests, 1,183 Python tests, 16 separately linked binding tests and the required static/documentation checks. The optimized parent/head wheels passed all 1,110/1,183 Python tests outside source respectively. All complete SF1 golden checks passed; paired-repeat acceptance is still open. See `issue-20-call-delivery-source-proof.json`, both release receipts and `frozen-submission-manifest-r2.json`.

The dependency and ownership analysis below records the assembly rationale. Historical proposal wording does not describe unfinished source or packaging.

## Actual prerequisite sources

The shared foundation is pagination90c141a0979bea1291baba501d49677a8dabb516, descended from containers89f96a4 and prerequisite4b15712 (integer precisionf71 plus stubs6175122). These are real ancestors, not declared comparison labels.

| Submission | Actual source | Necessary behavior |
|---|---|---|
| Cache14 | focused74dd1e5, prerequisite248def8 (refresh12 + accessors/types23) | Snapshot-local template cache with explicit CALL/path bypass and catalog-aware typing |
| Math20 | c53f47c2304f09f48019e0d3facdc7ece62c3a83 | Existing math kernels, strict casts, typed-null SQL behavior |
| String/list20 | d44596967609bce35fad34acafe68753a2913383 | Functions used by complete language behavior and tests |
| Entities20 | 99d11c0ea0e72ecf592d65165a47101ce281ca3a | Node/relationship values retained across projections and child outputs |
| Folds20 | 88dd97e8baa87cc76dbd968b5761252be3bf60ec | Local-variable, reduce and predicate AST forms and lexical scope |
| Alias21 | ccfc4fece4c1a099ab5b494f6a4533dfd03acc11, branch from String/list | Correct child sort/top-k aliases |
| UNION20 | 6e62a429da270649aa6877e81fc4a67df941bdc8, after alias21 | Branch output coercion, streaming and collection schema behavior |
| Paths20 | 92cfb4ac051429cbe96a3a1f6184636a194e87ce, actual comparison base84fcac65be2d4f5104061da1af16294656875b1a | Typed path values and query-local inventory with SinglePartition input contract |
| Runtime19 | Original63a4928 plus prerequisite integration | Shared cooperative scheduling and deadline/reader cleanup contract |

The parent selected path prerequisites folds88dd +32dd935 (11) +da3632b (13) +a769a0c (16) +84fcac6 (17). The narrower hard imports of the path modules are adjacency/cursor11, bidirectional shortest17 and shared cooperative-yield helpers;13 and16 are explicitly selected stable integration prerequisites, not inherent requirements of Apply. No15 partition option or18 count-only optimization is required for CALL.

The comparison base must contain these non-CALL features first, including their interaction repairs. Then the CALL branch adds only its behavior. The combined prerequisite source needs its own optimized release wheel and exact-source proof; previously qualified individual wheels are not its immediate-parent baseline.

## Ownership of original mixed integration changes

| Original patch | Curated owner and action |
|---|---|
| dbef77d | CALL: private Apply/CallInput operators, parser and importing-WITH semantics, per-row child streams, entity output retention. Adopt the already-shared cooperative-yield helper instead of adding a second helper. |
| 123c8d3 | CALL owns independent import/catalog seeding in UNION branches, child entity preservation and nested CALL cache bypass. Generic UNION/collection schema repairs already belong to UNION6e62a42; do not duplicate them. |
| b05ed30 semantic.rs/types.rs | Types23 + folds integration: bind lambda local expression types and restore outer type scope, recognize LocalVariable/ListPredicate, keep reduce result unknown after its lexical scope ends. This belongs in the comparison base, with its own tests. |
| b05ed30 query.rs | Paths + cache integration: rich-path inventories cannot be cached; ordinary substitution must rebind extension-owned path predicates. This belongs in the comparison base. |
| b05ed30 scheduling | CALL adopts the single existing CSR cooperative helper. Preserve errors and cancellation. |
| 99655be | Types23 + math/string integration: known scalar/list return-type inference across WITH/UNWIND. This belongs in the comparison base, not the CALL diff. |
| 33da59d +5f4d139 | CALL: poll budget at the top of every Apply loop iteration, including empty input/child batches; real MemTable regression disables inherited budget before proving its own yield. |
| 32edb0c | CALL owns conservative path demand for imported aliases and CALL alias regression. UNION-only path consumers belong to path/UNION integration in the base. |
| 3465806 +91d6b48 | Core typed shortest routing is path5f48. Config-aware recursive demand through CALL is CALL. Recursive UNION demand, direct path/cache refresh isolation and fixture config are path/cache/UNION integration in the base. |
| bfbbb24 migration guidance | CALL owns ReadingClause::Call, LogicalOperator::Apply/CallInput. Paths own PathPattern.variable, PathMatch and path-source context. Folds own Reduce/ListPredicate/LocalVariable. Count-only18 owns its own field migration separately. |

## Required final checks

1. Full workspace and excluded binding tests from the actual focused source, full Python suite, Clippy, format, Ruff, Pyright, strict docs, executable snippets and typos.
2. CALL behavior: independent imported lexical scopes for each UNION branch; duplicate outer rows, empty outer/child relations, per-row aggregates/top-k, exported collisions, node/path values, scalar rebinding and null/type coercion.
3. Cross-feature boundaries: fresh rich inventories and snapshot refresh isolation, shared path_sources in child/branch contexts, cache bypass at every query scope, lambda shadow/restore and known scalar type errors.
4. Cancellation: true all-ready empty MemTable batches must reach Apply's checkpoint; real child errors and dropped/timed-out readers release resources.
5. Build an optimized matched-Python3.13 wheel for both actual CALL parent and head. Byte-verify installed payloads outside source, run no-Rust consumer smoke and targeted tests. SF1 timing remains a separate centrally scheduled gate.

No performance gain, remote CI status, or completed PR is claimed by this plan.

## Historical assembly proposal

Start a new CALL-prerequisite worktree at path92cfb4a (which already contains pagination90 and the full math/string/entity/fold chain), then apply only UNION's aliasccfc +eefd2eb +6e62a42 changes, focused14's prerequisite refresh/accessors/types and cache patches, and19's deadline/cleanup changes. Keep common stubs and typed foundation from90. Integrate23's established scalar-return inference99655be and the fold-local type-scope hunks ofb05ed30 before freezing the comparison base. Add non-CALL UNION/path traversal and path/template eligibility fixes to this base. Avoid cherry-picking the historical combined math/string commits or the count-only18 branch.

Then apply CALLdbef77d and the CALL-only scope/path/cancellation patches above. This will require ordinary additive conflicts in AST visitors, query cache eligibility, graph execution deadlines and semantic type scopes. Freeze and qualify the resulting prerequisite source separately before comparing it with the CALL head. The existing broad final source is the reference for behavior and regression coverage, not the chosen PR base.

Existing regression placement must follow ownership too: `test_lambda_types_shadow_and_restore_outer_bindings` currently lives in the historical CALL test file but executes no CALL. Move its numeric/null rebinding, lambda shadow/restore and mismatch cases into the prerequisite type/fold test suite. Likewise direct rich-path scalar rebinding and refresh-reader isolation belong to the prerequisite path/cache tests; keep their CALL-import and correlated-UNION counterparts in the CALL tests. This avoids making a prerequisite correctness fix depend on a later parser feature merely because of the old file location.
