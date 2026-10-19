"""Transfer the existing protected AgentMail key using one-use RSA-OAEP wrapping."""
import base64
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

BASE=Path('/etc/ziji-fabric')

def command(args, **kwargs):
    return subprocess.run(args,check=True,capture_output=True,**kwargs)

def remote(payload):
    user,host=os.environ['REMOTE_USER'],os.environ['REMOTE_HOST']
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]*',user) or not re.fullmatch(r'[A-Za-z0-9.:-]+',host): raise ValueError('invalid_target')
    payload['source']=Path(__file__).read_text()
    code="import json,sys; p=json.load(sys.stdin); s={'__name__':'remote_install'}; exec(compile(p.pop('source'),'installer','exec'),s); s['on_host'](p)"
    import shlex
    r=command(['ssh','-i',str(Path.home()/'.ssh/fabric-preflight'),'-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+str(Path.home()/'.ssh/fabric-known-hosts'),user+'@'+host,'sudo -n python3 -c '+shlex.quote(code)],input=json.dumps(payload).encode(),timeout=120)
    return json.loads(r.stdout)

def on_host(p):
    if os.geteuid()!=0 or not re.fullmatch(r'[0-9]+-[0-9]+',p['run']): raise ValueError('invalid_context')
    BASE.mkdir(mode=0o750,exist_ok=True)
    transport=BASE/('alert-transfer-'+p['run'])
    if transport.is_symlink(): raise ValueError('symlink')
    transport.mkdir(mode=0o700,exist_ok=True)
    private=transport/'private.pem'
    if p['mode']=='prepare':
        if private.exists(): raise ValueError('transfer_already_exists')
        command(['openssl','genpkey','-algorithm','RSA','-pkeyopt','rsa_keygen_bits:4096','-out',str(private)])
        private.chmod(0o600)
        public=command(['openssl','pkey','-in',str(private),'-pubout']).stdout
        print(json.dumps({'public_key':base64.b64encode(public).decode()})); return
    if p['mode']=='cleanup':
        private.unlink(missing_ok=True); transport.rmdir(); print(json.dumps({'cleanup':True})); return
    clear=command(['openssl','pkeyutl','-decrypt','-inkey',str(private),'-pkeyopt','rsa_padding_mode:oaep','-pkeyopt','rsa_oaep_md:sha256'],input=base64.b64decode(p['sealed'],validate=True)).stdout
    config=json.loads(clear)
    scope={'__name__':'installed_mailer'}
    exec(compile(p['mailer'],'mailer','exec'),scope)
    if not scope['owner'](config.get('recipient')) or not config.get('key'): raise ValueError('config_mismatch')
    target=Path('/opt/ziji-fabric/fabric_alert_mailer.py')
    if target.is_symlink(): raise ValueError('symlink')
    target.write_text(p['mailer']);target.chmod(0o700)
    scope['atomic'](BASE/'alerts.json',config)
    private.unlink();transport.rmdir()
    # One persistent canary identity prevents duplicate sends on workflow retries.
    import contextlib, io
    captured=io.StringIO()
    with contextlib.redirect_stdout(captured): scope['main'](True)
    result=json.loads(captured.getvalue())
    canary=json.loads((scope['ROOT']/'canary.json').read_text())
    if canary.get('phase')!='accepted': raise ValueError('canary_not_accepted')
    cron=Path('/etc/cron.d/ziji-fabric-alerts')
    desired='# Owner-only local host alerts, zero model calls.\n19 * * * * root /usr/bin/python3 /opt/ziji-fabric/fabric_alert_mailer.py >/dev/null 2>&1\n'
    if cron.is_symlink() or (cron.exists() and cron.read_text()!=desired): raise ValueError('cron_conflict')
    if subprocess.run(['systemctl','is-active','--quiet','crond']).returncode: raise ValueError('cron_inactive')
    cron.write_text(desired);cron.chmod(0o644)
    # Do not send a second test or initial healthy message.
    print(json.dumps(dict(result, cron_installed=True, model_calls=0, scope='host_only', message_id=canary['message_id'])))


def main():
    mode=sys.argv[1]
    run=os.environ['GITHUB_RUN_ID']+'-'+os.environ['GITHUB_RUN_ATTEMPT']
    if mode=='wrap':
        from fabric_alert_mailer import api,owner
        key=os.environ.get('AGENTMAIL_API_KEY','').strip()
        if not key: raise ValueError('agentmail_key_missing_in_zijisecrets')
        inbox=api(key)
        recipient=inbox.get('metadata',{}).get('alertRecipient')
        if not owner(recipient): raise ValueError('recipient_mismatch')
        with tempfile.TemporaryDirectory() as d:
            pub=Path(d)/'public.pem';pub.write_bytes(base64.b64decode(os.environ['TRANSFER_PUBLIC'],validate=True))
            sealed=command(['openssl','pkeyutl','-encrypt','-pubin','-inkey',str(pub),'-pkeyopt','rsa_padding_mode:oaep','-pkeyopt','rsa_oaep_md:sha256'],input=json.dumps({'key':key,'recipient':recipient}).encode()).stdout
        with open(os.environ['GITHUB_OUTPUT'],'a') as out: out.write('sealed='+base64.b64encode(sealed).decode()+'\n')
        print(json.dumps({'agentmail_binding_verified':True,'recipient_verified':True,'credential_values_returned':False,
            'jira_binding_present':bool(os.environ.get('JIRA_API_TOKEN') or os.environ.get('ATLASSIAN_API_TOKEN'))}))
    elif mode=='prepare':
        result=remote({'mode':mode,'run':run})
        with open(os.environ['GITHUB_OUTPUT'],'a') as out: out.write('public_key='+result['public_key']+'\n')
        print('One-use public wrapping key ready; private key stays on VPS')
    elif mode=='cleanup':
        print(json.dumps(remote({'mode':mode,'run':run})))
    else:
        result=remote({'mode':'install','run':run,'sealed':os.environ['TRANSFER_SEALED'],'mailer':Path('scripts/fabric_alert_mailer.py').read_text()})
        print(json.dumps(result,sort_keys=True))

if __name__=='__main__':
    try: main()
    except Exception as exc:
        print(json.dumps({'outcome':'HOLD','error_type':type(exc).__name__,'values_withheld':True}))
        raise SystemExit(1)
