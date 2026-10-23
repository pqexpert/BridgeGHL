"""Verify native provider access on IONOS; persist isolated, root-only bindings.

No business mutations, messages, schedules, services, or Calendar writer changes.
Only sanitized receipts cross back from the server. Credentials travel on SSH stdin.
"""
import base64
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request

JIRA_ROOT = 'https://api.atlassian.com/ex/jira/20f21ae4-e364-40df-b5a1-b2512fd862d6'
KEYS = ('JIRA_API_TOKEN', 'JIRA_EMAIL', 'SHOPIFY_CLIENT_ID', 'SHOPIFY_CLIENT_SECRET', 'SHOPIFY_SHOP_DOMAIN')

class ProbeError(Exception):
    pass

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None

def request(url, headers=None, body=None):
    form = url.endswith('/admin/oauth/access_token') or url == 'https://oauth2.googleapis.com/token'
    data = (urllib.parse.urlencode(body).encode() if form else json.dumps(body).encode()) if body is not None else None
    h = {'Accept': 'application/json', **(headers or {})}
    if data is not None:
        h['Content-Type'] = 'application/x-www-form-urlencoded' if form else 'application/json'
    req = urllib.request.Request(url, headers=h, data=data)
    try:
        with urllib.request.build_opener(NoRedirect).open(req, timeout=25) as r:
            return json.loads(r.read(2000000))
    except urllib.error.HTTPError as e:
        code = 'http_' + str(e.code)
        try:
            raw = e.read(16000).decode('utf-8', errors='replace').lower()
            for error in ('shop_not_permitted', 'invalid_client_secret', 'invalid_api_key',
                          'invalid_client', 'invalid_request', 'invalid_grant', 'unauthorized_client',
                          'unsupported_grant_type', 'invalid_scope', 'app_not_installed', 'access_denied'):
                if re.search(r'(?<![a-z_])' + error + r'(?![a-z_])', raw):
                    code += '_' + error
                    break
        except Exception:
            pass
        raise ProbeError(code) from None
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
    if (not identity.get('active') or not identity.get('accountId') or
        (not isinstance(identity.get('emailAddress'), str) or identity['emailAddress'].lower() != config['JIRA_EMAIL'].lower())):
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

def google(config, call=request):
    required(config, ('GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET', 'GOOGLE_REFRESH_TOKEN'))
    token = call('https://oauth2.googleapis.com/token', body={
        'grant_type':'refresh_token', 'client_id':config['GOOGLE_CLIENT_ID'],
        'client_secret':config['GOOGLE_CLIENT_SECRET'], 'refresh_token':config['GOOGLE_REFRESH_TOKEN']})
    if not isinstance(token.get('access_token'), str) or not token['access_token']:
        raise ProbeError('google_access_token_missing')
    headers = {'Authorization':'Bearer ' + token['access_token']}
    owner = 'restorationconsultantsoutreach@gmail.com'
    about = call('https://www.googleapis.com/drive/v3/about?fields=user(emailAddress)', headers)
    if about.get('user', {}).get('emailAddress', '').lower() != owner:
        raise ProbeError('google_identity_mismatch')
    calendars, page, seen = [], None, set()
    for _ in range(20):
        params = {'maxResults':250}
        if page:
            params['pageToken'] = page
        result = call('https://www.googleapis.com/calendar/v3/users/me/calendarList?' + urllib.parse.urlencode(params), headers)
        calendars.extend(result.get('items', []))
        page = result.get('nextPageToken')
        if not page:
            break
        if page in seen:
            raise ProbeError('google_calendar_pagination_repeated')
        seen.add(page)
    else:
        raise ProbeError('google_calendar_pagination_incomplete')
    woo = [c for c in calendars if c.get('summary') == 'WooCalendar' and c.get('accessRole') == 'owner']
    if len(woo) != 1:
        raise ProbeError('woo_calendar_identity_ambiguous')
    receipt = {'authentication':'verified', 'account_identity':'verified', 'drive_read':'verified',
               'calendar_list_read':'verified', 'calendar_count':len(calendars),
               'woo_owned_matches':len(woo), 'calendar_writer_enabled':False, 'write_execution':'not_attempted'}
    binding = {'client_id':config['GOOGLE_CLIENT_ID'], 'client_secret':config['GOOGLE_CLIENT_SECRET'],
               'refresh_token':config['GOOGLE_REFRESH_TOKEN'], 'expected_email':owner}
    if len(woo) == 1:
        binding['calendar_id'] = woo[0]['id']
        result = call('https://www.googleapis.com/calendar/v3/calendars/' + urllib.parse.quote(woo[0]['id'], safe='') + '/events?maxResults=1&fields=items(id)', headers)
        receipt['calendar_event_read'] = 'verified'
    return receipt, binding

def existing_google(base, call=request):
    """Revalidate shared Calendar identity using only its admitted private binding."""
    target=base/'google'/'credentials.json'
    if not target.exists() and not target.is_symlink():raise ProbeError('binding_missing')
    for path in (base,base/'google',target):
        st=path.lstat()
        if path.is_symlink() or st.st_uid!=os.geteuid() or st.st_mode & 0o077:
            raise ProbeError('existing_binding_permissions_invalid')
    if not stat.S_ISREG(target.stat().st_mode) or target.stat().st_size>65536:
        raise ProbeError('existing_binding_invalid')
    before=target.read_bytes()
    binding=json.loads(before)
    if binding.get('expected_email')!='restorationconsultantsoutreach@gmail.com' or not binding.get('calendar_id'):
        raise ProbeError('existing_google_identity_invalid')
    config={k:binding.get(v,'') for k,v in (
        ('GOOGLE_CLIENT_ID','client_id'),('GOOGLE_CLIENT_SECRET','client_secret'),('GOOGLE_REFRESH_TOKEN','refresh_token'))}
    receipt,verified=google(config,call)
    if verified!=binding or target.is_symlink() or target.read_bytes()!=before:
        raise ProbeError('existing_binding_conflict_preserved')
    receipt.update(binding='already_present_verified',credential_source='existing_shared_google',binding_unchanged=True,
                   income_google_binding='separate_not_modified')
    return receipt

def host_inventory():
    import shutil
    services = {}
    for name in ('bridgeghl', 'bridgeghl-mcp', 'ziji-distiller', 'ollama'):
        r = subprocess.run(['systemctl','is-active',name], capture_output=True, text=True, timeout=10)
        services[name] = r.stdout.strip() if r.stdout.strip() in ('active','inactive','failed','unknown') else 'unknown'
    return {'services':services, 'cpu_count':os.cpu_count(),
            'available_memory_kib':next((int(x.split()[1]) for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemAvailable:')), None),
            'ollama_installed':bool(shutil.which('ollama')), 'gpu_tool_present':bool(shutil.which('nvidia-smi'))}

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
    for lane, probe in (('jira', jira), ('google', google)):
        try:
            if lane == 'google':
                receipt = existing_google(base, call)
            else:
                receipt, binding = probe(config, call)
                receipt['binding'] = persist(base, lane, binding)
            results[lane] = receipt
        except ProbeError as e:
            results[lane] = {'status':str(e), 'binding':'not_changed'}
        except Exception:
            results[lane] = {'status':'local_binding_error', 'binding':'inspect_before_retry'}
    results['shopify'] = {'status':'hosted_route_retained_native_deferred', 'binding':'not_changed'}
    results['google']['github_drive_candidate_used'] = False
    try:
        host = host_inventory()
    except Exception:
        host = {'status':'inventory_unavailable'}
    return {'stage':'NATIVE_PROVIDER_BINDINGS', 'results':results, 'host':host, 'runtime_activation':False}

def collect_remote(payload, receipt):
    try:
        scope = {'__name__':'collector_remote'}
        exec(compile(payload['collector'], 'collector', 'exec'), scope)
        receipt['collection'] = scope['install'](payload['collector'], payload['source'])
    except Exception:
        receipt['collection'] = {'status':'installation_failed_inspect_before_retry'}
    return receipt


def deployment_succeeded(receipt):
    results = receipt.get('results', {})
    collection = receipt.get('collection', {})
    return (all(results.get(lane, {}).get('binding') in
                ('installed_verified', 'already_present_verified') for lane in ('jira', 'google'))
            and collection.get('cron_installed') is True
            and all(collection.get(lane, {}).get('status') == 'retrieved_and_persisted'
                    for lane in ('jira', 'calendar')))

def deploy():
    user, host = os.environ['REMOTE_USER'], os.environ['REMOTE_HOST']
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]*', user) or not re.fullmatch(r'[A-Za-z0-9.:-]+', host):
        raise ProbeError('ssh_target_invalid')
    payload = {'source':Path(__file__).read_text(), 'collector':Path(__file__).with_name('collect_native_sources.py').read_text(),
               'config':{k:os.environ.get(k, '').strip() for k in KEYS}}
    remote = "sudo -n python3 -c 'import json,sys,types; p=json.load(sys.stdin); m=types.ModuleType(\"bind_native_providers\"); sys.modules[m.__name__]=m; exec(compile(p[\"source\"],\"binding\",\"exec\"),m.__dict__); r=m.run_server(p[\"config\"]); r=m.collect_remote(p,r); print(json.dumps(r,sort_keys=True)); sys.exit(0 if m.deployment_succeeded(r) else 1)'"
    command = ['ssh', '-i', str(Path.home()/'.ssh/fabric-preflight'), '-o','BatchMode=yes', '-o','ConnectTimeout=15',
               '-o','IdentitiesOnly=yes', '-o','StrictHostKeyChecking=yes',
               '-o','UserKnownHostsFile='+str(Path.home()/'.ssh/fabric-known-hosts'), user+'@'+host, remote]
    result = subprocess.run(command, input=json.dumps(payload), text=True, capture_output=True, timeout=240)
    receipt = json.loads(result.stdout)
    if receipt.get('stage') != 'NATIVE_PROVIDER_BINDINGS':
        raise ProbeError('receipt_invalid')
    print(json.dumps(receipt, sort_keys=True))
    if result.returncode or not deployment_succeeded(receipt):
        raise ProbeError('native_deployment_incomplete')

if __name__ == '__main__':
    try:
        deploy()
    except Exception:
        print('{"stage":"NATIVE_PROVIDER_BINDINGS","status":"deployment_failed_details_withheld"}')
        raise SystemExit(1)
