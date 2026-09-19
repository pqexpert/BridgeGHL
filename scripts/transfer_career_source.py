"""Transfer the pinned private Drive export over the existing protected SSH path."""
import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
from urllib.request import Request, urlopen

REF = '2e68154e352523e2e0b09f39ad452e63a9d24f7e'
root = Path('private-career-source')
root.mkdir(mode=0o700, exist_ok=True)
try:
    def fetch(name):
        url = 'https://api.github.com/repos/pqexpert/income-accelerator/contents/ingestion-staging/' + name + '?ref=' + REF
        req = Request(url, headers={'Authorization':'Bearer '+os.environ['SOURCE_TOKEN'],
            'Accept':'application/vnd.github.raw+json','User-Agent':'BridgeGHL/0.4.0'})
        with urlopen(req,timeout=30) as r: return r.read(2000000)
    raw = gzip.decompress(base64.b64decode(fetch('career-2026-09-19.json.gz.b64'),validate=False))
    control = fetch('control.json')
    config = json.loads(control)
    if len(raw)>5000000 or hashlib.sha256(raw).hexdigest()!=config['sha256']:
        raise ValueError('Pinned source checksum mismatch')
    for name,data in [('source.json',raw),('control.json',control)]:
        p=root/name;p.write_bytes(data);p.chmod(0o600)
    dest=os.environ['REMOTE_USER']+'@'+os.environ['REMOTE_HOST']
    key=str(Path.home()/'.ssh/bridgeghl')
    ssh=['ssh','-i',key,dest]
    remote='/tmp/bridgeghl-career-'+os.environ['GITHUB_RUN_ID']
    subprocess.run(ssh+['umask 077; mkdir -p '+remote],check=True)
    subprocess.run(['scp','-i',key,str(root/'source.json'),str(root/'control.json'),
        'scripts/bootstrap_career.py','scripts/run_career_import.sh',dest+':'+remote+'/'],check=True)
    subprocess.run(ssh+['bash '+remote+'/run_career_import.sh '+remote],check=True)
finally:
    for p in root.glob('*'):p.unlink()
    root.rmdir()
