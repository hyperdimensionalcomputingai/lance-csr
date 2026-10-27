"""Record verified PR publication without altering frozen acceptance evidence."""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import shutil

out=Path('/private/tmp/lance-csr-upstream-publication/contribution-evidence/upstream-fixes-0.3.1')
logs=Path('/private/tmp/lance-csr-runtime')
root=Path('/private/tmp/lance-csr-submissions')
proofs=json.loads((out/'STATUS.json').read_text())
expected={65:123,66:105,67:101}
items=[]
for number,issue in expected.items():
 raw=json.loads((logs/f'submit-pr-{number}.json').read_text())
 source=next(r for r in proofs['issues'] if r['upstream_issue']==issue)
 assert raw['head']['sha']==source['head'] and raw['head']['ref']==source['branch']
 assert raw['state']=='open' and not raw['draft'] and not raw['merged'] and raw['auto_merge'] is None
 assert raw['base']['ref']=='main' and raw['user']['login']=='stugorf'
 body=(root/f'submit-pr-bodies/PR-{issue}.md').read_text()
 assert raw['body'].replace('\r\n','\n')==body
 checks=json.loads((logs/f'submit-pr-{number}-checks.json').read_text())['statusCheckRollup']
 failed=[c for c in checks if c.get('conclusion') in ('FAILURE','CANCELLED','TIMED_OUT','ACTION_REQUIRED')]
 assert not failed,failed
 item={'upstream_issue':issue,'number':number,'url':raw['html_url'],'title':raw['title'],'head_branch':raw['head']['ref'],'head_commit':raw['head']['sha'],'base_branch':raw['base']['ref'],'base_commit':raw['base']['sha'],'author':raw['user']['login'],'state':raw['state'],'draft':raw['draft'],'merged':raw['merged'],'auto_merge':raw['auto_merge'],'mergeable_at_snapshot':raw['mergeable'],'created_at':raw['created_at'],'body_sha256':hashlib.sha256(body.encode()).hexdigest(),'initial_ci_checks':checks}
 items.append(item)
 shutil.copy2(root/f'submit-pr-bodies/PR-{issue}.md',out/f'PR-{issue}.md')
publication={'verified_utc':datetime.now(timezone.utc).isoformat(),'evidence_commit':'4e6349469c4e5a436136e74a29e153e68e5b6f79','evidence_archive_sha256':hashlib.sha256((out/'evidence.tar.gz').read_bytes()).hexdigest(),'remote_archive_blob_sha':'f7bce952a8da9b8a7a76b69f47f7e4e7e453e75c','remote_archive_bytes':2731667,'creation_method':'Authenticated GitHub CLI as stugorf after GitHub connector returned 403 Resource not accessible by integration.','attribution':'Submitted by David Hughes (@stugorf), with AI Collaboration.','pull_requests':items,'limits':['No merge or auto-merge enabled.','Hosted Rust, Clippy and Python jobs were still running at this initial snapshot; use PR checks for current status.','Deferred feature requests remain outside these three bug fixes.']}
(out/'PUBLISHED_PRS.json').write_text(json.dumps(publication,indent=2)+'\n')
lines=['# Three submitted pull requests','',f"Verified {publication['verified_utc']}. All are open, ready for review, authored by `stugorf`, and target `main`. Nothing is merged; auto-merge is disabled.",'','| PR | Upstream bug | Exact tested head |','|---|---|---|']
for r in items:lines.append(f"| [#{r['number']} — {r['title']}]({r['url']}) | #{r['upstream_issue']} | `{r['head_commit']}` |")
lines+=['','Every candidate passed the debug checks before any final production build. Each final installed wheel passed its full Python suite, and both complete SF1 suites passed in memory and lazy modes. Paired checks resolved the initial >10% timing flags; original observations remain unchanged.','', 'The [frozen validation evidence](https://github.com/hyperdimensionalcomputingai/lance-csr/tree/4e6349469c4e5a436136e74a29e153e68e5b6f79/contribution-evidence/upstream-fixes-0.3.1) is published and linked in every PR. Its remote archive blob and size were verified against the local artifact. Source branches and evidence are public; native wheels and datasets remain local.','', 'Hosted checks had no failures at this initial snapshot. Rust, Clippy and Python jobs were still running; use each PR’s checks for current status. See [PUBLISHED_PRS.json](PUBLISHED_PRS.json) for exact snapshots and body hashes.','', 'Deferred from this work: untyped variable-length relationships (#104), factorized intermediates (#163), optional Lance/DataFusion-only builds (#171), and JNI (#115).','', 'Submitted by David Hughes (@stugorf), with AI Collaboration.']
(out/'PUBLISHED_PRS.md').write_text('\n'.join(lines)+'\n')
tools=out/'publication-tools';tools.mkdir()
for name in ('submit-write-pr-bodies.py','submit-record-publication.py'):
 shutil.copy2(root/name,tools/name)
print(json.dumps({'prs':[r['url'] for r in items],'receipt':str(out/'PUBLISHED_PRS.md')},indent=2))
