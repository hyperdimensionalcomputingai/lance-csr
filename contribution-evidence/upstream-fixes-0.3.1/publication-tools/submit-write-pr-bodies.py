"""Write focused PR descriptions using verified final acceptance evidence."""
import json
from pathlib import Path
import sys
commit=sys.argv[1]
assert len(commit)==40 and all(c in '0123456789abcdef' for c in commit)
root=Path('/private/tmp/lance-csr-submissions')
publication=Path('/private/tmp/lance-csr-upstream-publication/contribution-evidence/upstream-fixes-0.3.1')
status=json.loads((publication/'STATUS.json').read_text())
gates=json.loads((publication/'final-gates.json').read_text());assert gates['all_passed']
evidence=f'https://github.com/hyperdimensionalcomputingai/lance-csr/blob/{commit}/contribution-evidence/upstream-fixes-0.3.1'
items={123:{'title':'fix(query): preserve empty-input distinct counts','before':'30 failed / 94 passed','focused':124,'problem':"`MATCH (p:Person) WHERE p.id = 99 RETURN count(DISTINCT p.id) > 0 AS present` can return `true` when no row matches, because DataFusion's statistics shortcut turns the empty distinct count into 1.",'change':"Construct a query-local frame that omits `aggregate_statistics` only when the logical plan contains a distinct count. Preserve the caller session, the other optimizer rules, its query planner and the deadline context. Execution, streaming and explain share the guard. Current main already supports aggregates inside comparison/CASE expressions; this PR does not duplicate that work.",'coverage':'Empty and nonempty counts, booleans, CASE, WITH, duplicates, nulls, parameter reuse, output types, caller-session preservation, legacy/join/CSR execution and streaming over Arrow/Lance.','impact':'Correct empty-input distinct counts and derived predicates. The Cypher guide documents the count semantics and scoped optimizer guard.'},105:{'title':'fix(planner): preserve physical column casing in graph scans','before':'104 failed / 26 passed','focused':130,'problem':'Queries over fields such as `isMarried` can fail with a missing lowercase-column error, including queries that do not select that property, because scans project the complete schema.','change':'Bind physical fields with exact Arrow schema names, then apply existing normalized Cypher aliases. Resolve inline filters against the schema spelling through one shared helper, including source nodes, target nodes, relationships and reversed undirected edges.','coverage':'Mixed-case node IDs, edge endpoints and properties; inline/WHERE filters, target joins, undirected/variable-length traversal, optional nulls, aliases, legacy/join/CSR APIs and native streaming over Arrow/Lance.','impact':'Mixed-case Arrow/Lance fields remain unchanged in the source and work through case-insensitive Cypher names. The existing case-sensitivity guide section is updated.'},101:{'title':'fix(parser): accept one trailing Cypher semicolon','before':'32 failed / 4 passed','focused':36,'problem':'A complete valid query ending in `;` is rejected as unexpected trailing input.','change':'Consume one optional terminator at the shared parser entry point. String literals still parse normally; repeated terminators, empty statements and appended statements remain errors.','coverage':'Standalone scalar/null RETURN, UNION ALL, MATCH/WITH, aggregation, pagination, surrounding whitespace, semicolons in strings, collected/streaming execution, equivalent ASTs and multi-statement rejection.','impact':'Calls accept one optional trailing semicolon. The Cypher guide documents the one-query-per-call rule.'}}
output=root/'submit-pr-bodies';output.mkdir(exist_ok=False)
for issue,item in items.items():
 record=next(r for r in status['issues'] if r['upstream_issue']==issue)
 comparisons=[c for c in gates['comparisons'] if c['after']==f'submit-{issue}']
 assert len(comparisons)==4
 table=['| Workload | Mode | Correct queries | First-pass geometric speedup |','|---|---|---:|---:|']
 for c in comparisons:
  table.append(f"| {'30-query SF1' if c['suite']=='q30' else 'IC SF1'} | {c['mode']} | {30 if c['suite']=='q30' else 14} | {c['geometric_mean_speedup']:.3f}x |")
 body=f'''## Problem and change

{item['problem']}

{item['change']}

Refs https://github.com/lance-format/lance-graph/issues/{issue}. This is a lance-csr fix; it does not close the upstream issue.

## Validation

- Failing before on the verified 0.3.1 debug baseline: **{item['before']}**. Passing after: **{item['focused']} focused cases**, **{record['python_tests_passed']:,} complete Python tests** in both debug and isolated production-wheel verification.
- Passed: `cargo fmt --all --check`, `cargo test --workspace --locked`, workspace Clippy with warnings denied, `make install`, `make test`, `make lint`, executable documentation snippets, strict Zensical build and `typos@1.26.0`.
- All three candidate debug gates passed before any final production build. Fresh locked consumers outside the checkouts verified wheel/package payloads and native hashes, then ran each complete suite with Rust absent from PATH.

{item['coverage']}

{chr(10).join(table)}

Above 1 is faster. These original suite means are retained unchanged; initial >10% per-query flags and their paired investigations are recorded in the [final gate record]({evidence}/final-gates.json). Both complete workloads preserve the original golden answers. These are local engineering measurements, not an audited SNB result or performance guarantee.

Measured serially after builds stopped, on Apple M4 Max (16 cores, 128 GiB), macOS arm64, Rust 1.96.0 and Python 3.13.17. The suites are pinned at `6260689` (30 queries) and `c185123` (IC); settings, dependencies, SF1 manifests, timings and peak process RSS are recorded. [Per-query before/after tables]({evidence.replace('/blob/', '/tree/')}/comparisons), [reproduction instructions]({evidence}/REPRODUCE.md), and [hashed raw evidence]({evidence}/evidence.tar.gz) provide the commands and receipts.

## User impact

{item['impact']} No Python signatures, stubs or dependencies change.

Independently based on `{status['tested_base']}`, head `{record['head']}`. Latest main `{status['observed_main']}` adds documentation links/package URLs only; runtime sources and locks are unchanged, and this PR applies cleanly. Other platforms and hosted CI are not included in the local verification; check the PR checks for their current status.

Submitted by David Hughes (@stugorf), with AI Collaboration.
'''
 (output/f'PR-{issue}.md').write_text(body)
 (output/f'PR-{issue}.json').write_text(json.dumps({'issue':issue,'title':item['title'],'body_path':str(output/f'PR-{issue}.md'),'branch':record['branch'],'head':record['head'],'base':'main'},indent=2)+'\n')
print(output)
