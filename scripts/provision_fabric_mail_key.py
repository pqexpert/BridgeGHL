"""Use GitHub's native sealed-secret API; never receive or print plaintext."""
import base64
import json
import os
from pathlib import Path
import urllib.request
import urllib.error
BASE = 'https://api.github.com/repos/pqexpert/BridgeGHL/environments/zijisecrets/secrets'
NAME = 'AGENTMAIL_API_KEY'
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('redirect_refused')
def request(path, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(BASE + path, data=body, method='GET' if body is None else 'PUT', headers={
        'Authorization': 'Bearer ' + os.environ['SOURCE_TOKEN'],
        'Accept': 'application/vnd.github+json', 'Content-Type': 'application/json'})
    with urllib.request.build_opener(NoRedirect).open(req, timeout=20) as response:
        data = response.read(65536)
        return response.status, json.loads(data) if data else {}
def main():
    _, public = request('/public-key')
    artifact = Path('ops/fabric-mail-key.sealed.json')
    if not artifact.exists():
        print(json.dumps({'environment': 'zijisecrets', 'public_key_id': public['key_id'], 'public_key': public['key']}))
        return
    envelope = json.loads(artifact.read_text())
    if set(envelope) != {'key_id', 'encrypted_value'} or envelope['key_id'] != public['key_id']:
        raise ValueError('key_id_mismatch')
    sealed = base64.b64decode(envelope['encrypted_value'], validate=True)
    if not 49 <= len(sealed) <= 4096:
        raise ValueError('invalid_envelope')
    # Never overwrite a pre-existing credential without a separate rotation decision.
    try:
        request('/' + NAME)
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise
    else:
        raise ValueError('secret_already_present')
    status, _ = request('/' + NAME, envelope)
    _, readback = request('/' + NAME)
    if status != 201 or readback.get('name') != NAME:
        raise ValueError('secret_readback_failed')
    print(json.dumps({'secret_created': True, 'name': NAME, 'environment': 'zijisecrets', 'plaintext_exposed': False}))
if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'outcome': 'HOLD', 'error_type': type(exc).__name__, 'http_status': getattr(exc, 'code', None)}))
        raise SystemExit(1)
