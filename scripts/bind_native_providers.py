"""Verify native provider access on IONOS; persist isolated, root-only bindings.

No business mutations, messages, schedules, services, or Calendar writer changes.
Only sanitized receipts cross back from the server. Credentials travel on SSH stdin.
"""
import base64
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request

JIRA_ROOT = 'https://api.atlassian.com/ex/jira/20f21ae4-e364-40df-b5a1-b2512fd862d6'
KEYS = ('JIRA_API_TOKEN', 'JIRA_EMAIL', 'SHOPIFY_CLIENT_ID', 'SHOPIFY_CLIENT_SECRET', 'SHOPIFY_SHOP_DOMAIN',
        'GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET', 'GOOGLE_REFRESH_TOKEN')

class ProbeError(Exception):
    pass

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None

def request(url, headers=None, body=None):
    data = json.dumps(body).encode() if body is not None else None
    h = {'Accept': 'application/json', **(headers or {})}
    if data is not None:
        h['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, headers=h, data=data)
    try:
        with urllib.request.build_opener(NoRedirect).open(req, timeout=25) as r:
            return json.loads(r.read(2000000))
    except urllib.error.HTTPError as e:
        raise ProbeError('http_' + str(e.code)) from None
    except Exception:
        raise ProbeError('network_or_response_error') from None

def required(config, names):
    if not all(isinstance(config.get(k), str) and config[k].strip() for k in names):
        raise ProbeError('binding_missing')
    if any('\n' in config[k] or '\r' in config[k] for k in names):
        raise ProbeError('binding_format_invalid')

def jira(config, call=request):
    required(config, ('JIRA_EMAIL', 'JIRA_API_TOKEN'))
    auth = base64.b64encode((config['JIRA_EMAIL'] + ':' + config['JIRA_API_TOKEN']).encode()).decode()
    headers = {'Authorization': 'Basic ' + auth}
    identity = call(JIRA_ROOT + '/rest/api/3/myself', headers)
    if not identity.get('active') or identity.get('accountId') != '712020:957ef1d2-89f5-461a-90e3-ffcfb77330bd':
        raise ProbeError('jira_identity_mismatch')
    issue = call(JIRA_ROOT + '/rest/api/3/issue/RBO-97?fields=project', headers)
    if issue.get('key') != 'RBO-97' or issue.get('fields', {}).get('project', {}).get('key') != 'RBO':
        raise ProbeError('jira_project_mismatch')
    names = ['BROWSE_PROJECTS', 'CREATE_ISSUES', 'EDIT_ISSUES', 'ADD_COMMENTS', 'TRANSITION_ISSUES']
    permissions = call(JIRA_ROOT + '/rest/api/3/mypermissions?' + urllib.parse.urlencode(
        {'projectKey':'RBO', 'permissions':','.join(names)}), headers).get('permissions', {})
    granted = {k: permissions.get(k, {}).get('havePermission') is True for k in names}
    return {'authentication':'verified', 'issue_read':'verified', 'project_permissions':granted,
            'write_execution':'not_attempted'}, {'base_url':JIRA_ROOT, 'email':config['JIRA_EMAIL'],
            'api_token':config['JIRA_API_TOKEN'], 'project_allowlist':['RBO']}

def shopify(config, call=request):
    required(config, ('SHOPIFY_SHOP_DOMAIN', 'SHOPIFY_CLIENT_ID', 'SHOPIFY_CLIENT_SECRET'))
    domain = config['SHOPIFY_SHOP_DOMAIN'].strip().lower()
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]*\.myshopify\.com', domain):
        raise ProbeError('shop_domain_format_invalid')
    root = 'https://' + domain
    token = call(root + '/admin/oauth/access_token', body={'grant_type':'client_credentials',
        'client_id':config['SHOPIFY_CLIENT_ID'], 'client_secret':config['SHOPIFY_CLIENT_SECRET']})
    if not isinstance(token.get('access_token'), str) or not token['access_token']:
        raise ProbeError('shop_token_missing')
    headers = {'X-Shopify-Access-Token':token['access_token']}
    response = call(root + '/admin/api/2026-10/graphql.json', headers, {'query':
        '{ shop { name email currencyCode primaryDomain { host } } currentAppInstallation { accessScopes { handle } } }'})
    if response.get('errors'):
        raise ProbeError('shop_graphql_error')
    shop = response.get('data', {}).get('shop', {})
    if (shop.get('name') != 'Tantric Treasures' or shop.get('email') != 'store@dzokden.org'
        or shop.get('currencyCode') != 'USD' or shop.get('primaryDomain', {}).get('host') not in
        ('www.tantrictreasures.com', 'tantrictreasures.com')):
        raise ProbeError('shop_identity_mismatch')
    scopes = response.get('data', {}).get('currentAppInstallation', {}).get('accessScopes', [])
    handles = sorted({x['handle'] for x in scopes if isinstance(x, dict) and
        isinstance(x.get('handle'), str) and re.fullmatch('[a-z_]{1,100}', x['handle'])})
    return {'authentication':'verified', 'store_identity':'verified', 'granted_scopes':handles,
            'write_execution':'not_attempted'}, {'shop_domain':domain, 'client_id':config['SHOPIFY_CLIENT_ID'],
            'client_secret':config['SHOPIFY_CLIENT_SECRET'], 'api_version':'2026-10',
            'expected_public_domain':'www.tantrictreasures.com'}

def persist(base, lane, config):
    for p in (base.parent, base, base / lane):
        if p.is_symlink():
            raise ProbeError('binding_path_symlink')
        p.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = base / lane / 'credentials.json'
    if target.is_symlink():
        raise ProbeError('binding_path_symlink')
    content = json.dumps(config, sort_keys=True) + '\n'
    if target.exists():
        if target.read_text() != content:
            raise ProbeError('existing_binding_conflict_preserved')
        if target.stat().st_mode & 0o077 or target.stat().st_uid != os.geteuid():
            raise ProbeError('existing_binding_permissions_invalid')
        return 'already_present_verified'
    fd, name = tempfile.mkstemp(dir=target.parent, prefix='.staging-')
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(name, 0o600)
        os.link(name, target)  # Exclusive install; never replace a concurrent binding.
        if target.read_text() != content:
            raise ProbeError('binding_readback_failed')
    finally:
        os.unlink(name)
    return 'installed_verified'

def google_inventory():
    # Only known runtime config homes; inspect key names, never print values.
    found = set()
    paths = []
    for root in (Path('/etc/ziji-fabric'), Path('/etc/bridgeghl')):
        if root.exists() and not root.is_symlink():
            paths.extend(root.rglob('*.env'))
            paths.extend(root.rglob('*.json'))
    for path in paths[:80]:
        if path.is_symlink() or path.stat().st_size > 100000:
            continue
        try:
            raw = path.read_text()
            if path.suffix == '.json':
                value = json.loads(raw)
                names = list(value) if isinstance(value, dict) else []
            else:
                names = re.findall(r'^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=', raw, re.M)
            found.update(k for k in names if re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,100}', k)
                         and re.search('google|gcal|oauth|refresh|client_secret|client_id', k, re.I))
        except (ValueError, OSError):
            continue
    return {'existing_runtime_candidate_names':sorted(found), 'calendar_writer_enabled':False}

def run_server(config, base=Path('/etc/ziji-fabric/providers'), call=request):
    results = {}
    for lane, probe in (('jira', jira), ('shopify', shopify)):
        try:
            receipt, binding = probe(config, call)
            receipt['binding'] = persist(base, lane, binding)
            results[lane] = receipt
        except ProbeError as e:
            results[lane] = {'status':str(e), 'binding':'not_changed'}
        except Exception:
            results[lane] = {'status':'local_binding_error', 'binding':'inspect_before_retry'}
    results['google'] = google_inventory()
    results['google']['github_oauth_bindings_present'] = {k:bool(config.get(k)) for k in KEYS if k.startswith('GOOGLE_')}
    return {'stage':'NATIVE_PROVIDER_BINDINGS', 'results':results, 'runtime_activation':False}

def deploy():
    user, host = os.environ['REMOTE_USER'], os.environ['REMOTE_HOST']
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]*', user) or not re.fullmatch(r'[A-Za-z0-9.:-]+', host):
        raise ProbeError('ssh_target_invalid')
    payload = {'source':Path(__file__).read_text(), 'config':{k:os.environ.get(k, '').strip() for k in KEYS}}
    remote = "sudo -n python3 -c 'import json,sys; p=json.load(sys.stdin); s={\"__name__\":\"binding_remote\"}; exec(compile(p[\"source\"],\"binding\",\"exec\"),s); print(json.dumps(s[\"run_server\"](p[\"config\"]),sort_keys=True))'"
    command = ['ssh', '-i', str(Path.home()/'.ssh/fabric-preflight'), '-o','BatchMode=yes', '-o','ConnectTimeout=15',
               '-o','IdentitiesOnly=yes', '-o','StrictHostKeyChecking=yes',
               '-o','UserKnownHostsFile='+str(Path.home()/'.ssh/fabric-known-hosts'), user+'@'+host, remote]
    result = subprocess.run(command, input=json.dumps(payload), text=True, capture_output=True, timeout=240)
    if result.returncode:
        raise ProbeError('remote_binding_failed_details_withheld')
    receipt = json.loads(result.stdout)
    if receipt.get('stage') != 'NATIVE_PROVIDER_BINDINGS':
        raise ProbeError('receipt_invalid')
    print(json.dumps(receipt, sort_keys=True))

if __name__ == '__main__':
    try:
        deploy()
    except Exception:
        print('{"stage":"NATIVE_PROVIDER_BINDINGS","status":"transport_failed_details_withheld"}')
        raise SystemExit(1)
