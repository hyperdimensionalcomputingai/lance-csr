Title: refactor(config)!: make graph mappings immutable and validate input

Future breaking-release stage of #23. Do not include this branch in the current compatible submission series or close #23 until the breaking release ships.

GraphConfig mapping fields become private. Reads use the previously added lookup/iterator accessors; writes use the existing builder. Serialized field names remain unchanged. Deserialization rejects duplicate/non-normalized map keys, label/type mismatches, empty ID/endpoint fields and unconfigured endpoint labels instead of constructing invalid mappings.

The Rust crate README explains migration from direct mutation and struct literals. Existing builder normalization and replacement behavior are retained. Python signatures are unchanged.

Branch: codex/issue-23-private-mappings-next-breaking, based on 1939b69; final local commit 98dc6b41e3bbf8f4d27aa4f1aeab3f931277ac56. Public deserialization tests use the same Arrow-independent configuration API on both revisions: the old implementation passes round-trip preservation but fails both malformed-input tests; the candidate passes all three. Compile-fail examples verify that neither map can be mutated directly. The full Rust workspace, Clippy, strict docs and spelling pass; an explicit compile check of the excluded binding crate passes.

Remaining release gates: validate Python packaging/runtime on that commit, compare both complete workloads in memory/lazy modes and reproduce from a clean checkout when this stage is scheduled for release. No push or PR has been made.

Submitted by David Hughes (@stugorf), with AI Collaboration.
