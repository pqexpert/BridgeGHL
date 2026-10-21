"""Encrypted off-host recovery using the existing GitHub and SSH bindings.

Only ciphertext is uploaded; recovery keys live in protected GitHub secrets.
No provider mutation, service restart, schedule cutover, or plaintext logging.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone

REPOSITORIES = ('BridgeGHL', 'fabric-dispatch', 'fabric-distiller', 'income-accelerator',
                'rsc-operations-console', 'Dzokden-Store-Manager-Ziji', 'FungibleRD-MVP')
DESTINATION = 'pqexpert/fabric-dispatch'
SECRET = 'FABRIC_BACKUP_KEY_V1'
SECRET_BASE = '/repos/pqexpert/BridgeGHL/environments/zijifabric/secrets/'
MAGIC = b'ZIJI-BACKUP-AES256GCM-1\n'

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('unexpected_redirect')


def api(path, data=None, method='GET', raw=False):
    url = path if path.startswith('https://') else 'https://api.github.com' + path
    if not url.startswith(('https://api.github.com/', 'https://uploads.github.com/')):
        raise ValueError('unapproved_github_host')
    body = data if raw else (json.dumps(data).encode() if data is not None else None)
    req = urllib.request.Request(url, data=body, method=method, headers={
        'Authorization': 'Bearer ' + os.environ['SOURCE_TOKEN'],
        'Accept': 'application/vnd.github+json',
        'Content-Type': 'application/octet-stream' if raw else 'application/json',
        'X-GitHub-Api-Version': '2022-11-28'})
    with urllib.request.build_opener(NoRedirect).open(req, timeout=120) as response:
        content = response.read()
        return json.loads(content) if content else {}


def recovery_key():
    value = os.environ.get('BACKUP_KEY', '').strip()
    if value:
        key = base64.b64decode(value, validate=True)
        if len(key) != 32:
            raise ValueError('invalid_backup_key')
        return key
    # Refuse rotation if secret exists but was not bound to this run.
    try:
        api(SECRET_BASE + SECRET)
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise
    else:
        raise ValueError('existing_backup_key_not_bound')
    from nacl.public import PublicKey, SealedBox
    public = api(SECRET_BASE + 'public-key')
    key = os.urandom(32)
    sealed = SealedBox(PublicKey(base64.b64decode(public['key']))).encrypt(base64.b64encode(key))
    api(SECRET_BASE + SECRET, {'key_id': public['key_id'],
        'encrypted_value': base64.b64encode(sealed).decode()}, method='PUT')
    if api(SECRET_BASE + SECRET).get('name') != SECRET:
        raise ValueError('recovery_secret_metadata_readback_failed')
    return key


def run(command, **kwargs):
    result = subprocess.run(command, capture_output=True, timeout=600, **kwargs)
    if result.returncode:
        raise ValueError('subprocess_failed_' + Path(command[0]).name)
    return result.stdout


def encrypt(source, target, key):
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    nonce = os.urandom(12)
    context = Cipher(algorithms.AES(key), modes.GCM(nonce)).encryptor()
    context.authenticate_additional_data(MAGIC)
    with source.open('rb') as inp, target.open('wb') as out:
        out.write(MAGIC + nonce)
        while data := inp.read(1024 * 1024):
            out.write(context.update(data))
        out.write(context.finalize())
        out.write(context.tag)


def decrypt(source, target, key):
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    with source.open('rb') as inp:
        if inp.read(len(MAGIC)) != MAGIC:
            raise ValueError('invalid_backup_header')
        nonce = inp.read(12)
        inp.seek(-16, 2)
        tag = inp.read(16)
        remaining = inp.tell() - len(MAGIC) - 12 - 16
        inp.seek(len(MAGIC) + 12)
        context = Cipher(algorithms.AES(key), modes.GCM(nonce, tag)).decryptor()
        context.authenticate_additional_data(MAGIC)
        with target.open('wb') as out:
            while remaining:
                data = inp.read(min(remaining, 1024 * 1024))
                if not data:
                    raise ValueError('truncated_backup')
                remaining -= len(data)
                out.write(context.update(data))
            out.write(context.finalize())


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(data)
    return h.hexdigest()


def download_asset(asset_id, target):
    req = urllib.request.Request('https://api.github.com/repos/' + DESTINATION + '/releases/assets/' + str(asset_id), headers={
        'Authorization': 'Bearer ' + os.environ['SOURCE_TOKEN'], 'Accept': 'application/octet-stream'})
    # Strip Authorization before following GitHub's signed download redirect.
    try:
        response = urllib.request.build_opener(NoRedirect).open(req, timeout=120)
    except ValueError:
        class CaptureRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, request, fp, code, msg, headers, url):
                self.location = url
                return None
        handler = CaptureRedirect()
        try:
            urllib.request.build_opener(handler).open(req, timeout=120)
        except urllib.error.HTTPError as exc:
            if exc.code not in (301, 302, 303, 307, 308):
                raise
        url = handler.location
        from urllib.parse import urlparse
        host = urlparse(url).hostname or ''
        if not url.startswith('https://') or not host.endswith(('.githubusercontent.com', '.github.com')):
            raise ValueError('unexpected_asset_host')
        response = urllib.request.build_opener(NoRedirect).open(url, timeout=120)
    with response, target.open('wb') as out:
        while data := response.read(1024 * 1024):
            out.write(data)


def main():
    os.umask(0o077)
    repo = api('/repos/' + DESTINATION)
    if repo.get('private') is not True:
        raise ValueError('backup_destination_must_be_private')
    key = recovery_key()
    user, host = os.environ['REMOTE_USER'], os.environ['REMOTE_HOST']
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]*', user) or not re.fullmatch(r'[A-Za-z0-9.:-]+', host):
        raise ValueError('invalid_existing_ssh_target')
    ssh = ['ssh', '-i', str(Path.home()/'.ssh/fabric-preflight'), '-o', 'BatchMode=yes',
           '-o', 'ConnectTimeout=15', '-o', 'StrictHostKeyChecking=yes',
           '-o', 'UserKnownHostsFile=' + str(Path.home()/'.ssh/fabric-known-hosts'), user+'@'+host]
    run_id = os.environ['GITHUB_RUN_ID'] + '-' + os.environ['GITHUB_RUN_ATTEMPT']
    if not re.fullmatch(r'[0-9]+-[0-9]+', run_id):
        raise ValueError('invalid_run_id')
    remote_dir = '/var/tmp/ziji-backup-' + run_id
    remote_path = remote_dir + '/server.tar.gz'
    with tempfile.TemporaryDirectory(prefix='fabric-backup-') as tmp:
        root = Path(tmp)
        try:
            source = Path('scripts/fabric_snapshot.py').read_text()
            # Uploaded helper runs as root, with archive restricted to owner read/write.
            run(ssh + ['sudo -n install -d -m 700 ' + shlex.quote(remote_dir)])
            command = 'sudo -n python3 - --output ' + shlex.quote(remote_path)
            report = json.loads(run(ssh + [command], input=source.encode()))
            server_archive = root/'server.tar.gz'
            # Stream through SSH rather than weakening archive file permissions for scp.
            with server_archive.open('wb') as out:
                p = subprocess.run(ssh + ['sudo -n cat ' + shlex.quote(remote_path)], stdout=out,
                                   stderr=subprocess.PIPE, timeout=600)
                if p.returncode:
                    raise ValueError('snapshot_transfer_failed')
            bundles = root/'repositories'
            bundles.mkdir()
            heads = {}
            git_env = dict(os.environ, GIT_CONFIG_COUNT='1',
                GIT_CONFIG_KEY_0='http.https://github.com/.extraheader',
                GIT_CONFIG_VALUE_0='AUTHORIZATION: basic ' + base64.b64encode(
                    ('x-access-token:' + os.environ['SOURCE_TOKEN']).encode()).decode())
            for name in REPOSITORIES:
                mirror = root/(name+'.git')
                run(['git', 'clone', '--quiet', '--mirror', 'https://github.com/pqexpert/'+name+'.git', str(mirror)], env=git_env)
                heads[name] = run(['git','--git-dir='+str(mirror),'rev-parse','HEAD']).decode().strip()
                bundle = bundles/(name+'.bundle')
                run(['git','--git-dir='+str(mirror),'bundle','create',str(bundle),'--all'])
                # A fresh repository checks the bundle's complete prerequisites.
                verify = root/('verify-'+name)
                run(['git','init','--quiet',str(verify)])
                run(['git','-C',str(verify),'bundle','verify',str(bundle)])
            manifest = {'schema':'ziji-recovery/1', 'created_at':datetime.now(timezone.utc).isoformat(),
                'run_id':run_id, 'workflow_sha':os.environ['GITHUB_SHA'], 'source_heads':heads,
                'server_snapshot':report, 'server_archive_sha256':digest(server_archive),
                'bundles':{p.name:digest(p) for p in bundles.iterdir()},
                'coverage':'Existing VPS Fabric/BridgeGHL files plus seven Git repository histories. Native SaaS datasets and ChatGPT connector credentials are not exported.',
                'key_secret':SECRET, 'key_id':hashlib.sha256(key).hexdigest()[:16]}
            (root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
            plain = root/'recovery.tar'
            with tarfile.open(plain,'w') as archive:
                for name in ('server.tar.gz','repositories','manifest.json'):
                    archive.add(root/name,arcname=name)
            encrypted = root/'fabric-recovery.aesgcm'
            encrypt(plain,encrypted,key)
            tag = 'server-backup-'+run_id
            release = api('/repos/'+DESTINATION+'/releases', {'tag_name':tag,
                'name':'Encrypted Fabric recovery '+run_id, 'draft':False, 'prerelease':True,
                'body':'Encrypted VPS snapshot and Git history. Recovery key: BridgeGHL / zijifabric / '+SECRET+'. Includes coverage manifest. Do not publish plaintext.'},method='POST')
            upload = release['upload_url'].split('{',1)[0]+'?name=fabric-recovery.aesgcm'
            if encrypted.stat().st_size > 1500*1024*1024:
                raise ValueError('archive_exceeds_bounded_upload_size')
            asset = api(upload,encrypted.read_bytes(),method='POST',raw=True)
            downloaded = root/'downloaded.aesgcm'
            download_asset(asset['id'],downloaded)
            if digest(downloaded) != digest(encrypted):
                raise ValueError('github_download_digest_mismatch')
            restored = root/'restored.tar'
            decrypt(downloaded,restored,key)
            if digest(restored) != digest(plain):
                raise ValueError('restored_archive_digest_mismatch')
            # Verify downloaded member hashes, not only the locally created archive.
            with tarfile.open(restored,'r') as archive:
                recovered_manifest=json.load(archive.extractfile('manifest.json'))
                for member, expected in [('server.tar.gz',recovered_manifest['server_archive_sha256'])]+[
                    ('repositories/'+name,value) for name,value in recovered_manifest['bundles'].items()]:
                    h=hashlib.sha256()
                    f=archive.extractfile(member)
                    for chunk in iter(lambda:f.read(1024*1024),b''): h.update(chunk)
                    if h.hexdigest()!=expected: raise ValueError('restored_member_digest_mismatch')
            receipt = {'stage':'FABRIC_GITHUB_BACKUP','outcome':'PASS','release_url':release['html_url'],
                'ciphertext_sha256':digest(encrypted),'encrypted_bytes':encrypted.stat().st_size,
                'key_id':manifest['key_id'],'repository_count':len(heads),'download_decrypt_verified':True,
                'server_snapshot':report,'native_saas_exported':False,'services_restarted':False}
            print(json.dumps(receipt,sort_keys=True))
            Path(os.environ.get('GITHUB_STEP_SUMMARY','backup-summary.md')).write_text(
                'Encrypted recovery backup verified: '+release['html_url']+'\n\nSHA-256: '+receipt['ciphertext_sha256']+
                '\n\nCoverage: existing VPS files and seven repository histories; native SaaS datasets remain at their providers.\n')
        finally:
            # Remove only this run's private temporary archive; never delete live state.
            try:
                run(ssh+['sudo -n rm -f -- '+shlex.quote(remote_path)])
                run(ssh+['sudo -n rmdir -- '+shlex.quote(remote_dir)])
            except Exception:
                print(json.dumps({'stage':'FABRIC_BACKUP_CLEANUP','outcome':'NEEDS_CLEANUP', 'private_temporary_archive_retained':True}))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'stage':'FABRIC_GITHUB_BACKUP','outcome':'FAILED',
            'error_type':type(exc).__name__,'http_status':getattr(exc,'code',None),
            'reason':str(exc) if isinstance(exc,ValueError) and re.fullmatch(r'[a-z0-9_]{1,100}',str(exc)) else 'details_withheld'}))
        raise SystemExit(1)
