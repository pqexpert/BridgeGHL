"""Deploy a pinned private package through the existing owner VPS transport."""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import urllib.error
import urllib.request

SOURCE_SHA = "6e26499749acd7ad54931e3466fc29eb4a1a6196"
PATHS = ('runtime/service.py', 'runtime/install.py', 'runtime/ziji-distiller.service',
         'gpt/INSTRUCTIONS.md', 'V1-OPERATING-CONTRACT.md', 'SKILL.md')


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def main():
    token = os.environ['SOURCE_TOKEN'].strip()
    model_key = os.environ['MODEL_KEY'].strip()
    user, host = os.environ['REMOTE_USER'], os.environ['REMOTE_HOST']
    if not token or not model_key or not re.fullmatch('[a-f0-9]{40}', SOURCE_SHA):
        raise SystemExit('Required existing binding or source pin missing; values withheld')
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]*', user) or not re.fullmatch(r'[A-Za-z0-9.:-]+', host):
        raise SystemExit('SSH target format invalid; values withheld')
    def fetch(path):
        request = urllib.request.Request('https://api.github.com/repos/pqexpert/fabric-distiller/contents/' + path + '?ref=' + SOURCE_SHA,
            headers={'Authorization':'Bearer ' + token, 'Accept':'application/vnd.github+json', 'X-GitHub-Api-Version':'2022-11-28'})
        try:
            with urllib.request.build_opener(NoRedirect).open(request, timeout=30) as response:
                data = json.loads(response.read(1000000))
            if data['encoding'] != 'base64':
                raise ValueError('unexpected_encoding')
            return base64.b64decode(data['content']).decode()
        except urllib.error.HTTPError as error:
            raise SystemExit('Private pinned source fetch HTTP ' + str(error.code) + '; no unchanged retry') from None
        except Exception:
            raise SystemExit('Private pinned source fetch failed; details withheld') from None
    manifest = json.loads(fetch('IMPORT-MANIFEST.json'))
    files = {path:fetch(path) for path in PATHS}
    for path, content in files.items():
        if hashlib.sha256(content.encode()).hexdigest() != manifest['files'][path]:
            raise SystemExit('Pinned private source hash mismatch')
    payload = {'source_sha':SOURCE_SHA, 'files':files, 'hashes':{path:manifest['files'][path] for path in PATHS},
               'model_key':model_key, 'model':'gpt-5.6'}
    remote = "sudo -n python3 -c 'import json,sys; p=json.load(sys.stdin); s={}; exec(compile(p[\"files\"][\"runtime/install.py\"], \"installer\", \"exec\"), s); s[\"install\"](p)'"
    command = ['ssh', '-i', str(Path.home()/'.ssh/fabric-preflight'), '-o','BatchMode=yes', '-o','ConnectTimeout=15',
               '-o','IdentitiesOnly=yes', '-o','StrictHostKeyChecking=yes',
               '-o','UserKnownHostsFile='+str(Path.home()/'.ssh/fabric-known-hosts'), user+'@'+host, remote]
    try:
        result = subprocess.run(command, input=json.dumps(payload), text=True, capture_output=True, timeout=250)
        reports = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{"stage": "DISTILLER_VPS_DEPLOYMENT"')]
        if len(reports) != 1:
            raise ValueError('missing_report')
        report = reports[0]
        # Never forward private source, model output, credentials, or raw SSH errors.
        safe = {'stage':'DISTILLER_VPS_DEPLOYMENT', 'source_sha':SOURCE_SHA, 'fabric_live':False}
        for key in ('outcome','phase','error','inference','restart_readback','bridgeghl','result_digest'):
            if key in report:
                value = report[key]
                if not isinstance(value,str) or not re.fullmatch('[A-Za-z0-9_]{1,80}',value):
                    raise ValueError('unsafe_report')
                safe[key]=value
        safe['calendar_writer']=False
        print(json.dumps(safe, sort_keys=True))
        if result.returncode or report.get('outcome') != 'PASS':
            raise SystemExit(1)
    except (OSError, ValueError, subprocess.TimeoutExpired):
        raise SystemExit('Deployment did not return a valid sanitized result; inspect before another attempt') from None


if __name__ == '__main__':
    main()
