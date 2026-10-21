"""Deterministic owner-only host alerts. No model, CRM or Calendar operations."""
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import urllib.request
import urllib.parse

INBOX = 'ziji-fabric-alerts@agentmail.to'
OWNER_HASH = '4c4455cd78f66a1a8d5e72ac0f9eb46321b6c9c3b7cb567cdb6bad495afe1fd7'
ROOT = Path('/var/lib/ziji-fabric/alerts')
CONFIG = Path('/etc/ziji-fabric/alerts.json')
SNAPSHOT = Path('/var/lib/ziji-fabric/dispatch-shadow/latest.json')

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None

def api(key, suffix='', body=None):
    url = 'https://api.agentmail.to/v0/inboxes/' + urllib.parse.quote(INBOX, safe='') + suffix
    request = urllib.request.Request(url, headers={'Authorization':'Bearer '+key, 'Content-Type':'application/json'},
        data=json.dumps(body).encode() if body is not None else None)
    with urllib.request.build_opener(NoRedirect).open(request, timeout=25) as response:
        return json.loads(response.read(262144))

def atomic(path, value):
    if path.is_symlink():
        raise ValueError('symlink_rejected')
    fd, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream); stream.flush(); os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)

def owner(recipient):
    return isinstance(recipient, str) and hashlib.sha256(recipient.encode()).hexdigest() == OWNER_HASH

def classify(snapshot, now):
    try:
        seen = dt.datetime.fromisoformat(snapshot['observed_at_utc'].replace('Z','+00:00'))
        if seen.tzinfo is None or not -300 <= (now-seen).total_seconds() <= 5400:
            return ['P1:host:watchdog_receipt_stale']
        alerts = snapshot['alerts']
        if not isinstance(alerts,list) or any(not isinstance(a,str) or not re.fullmatch(r'P1:host:[A-Za-z0-9_.:]{1,100}',a) for a in alerts):
            return ['P1:host:watchdog_receipt_invalid']
        return sorted(set(alerts))
    except (KeyError, ValueError, TypeError):
        return ['P1:host:watchdog_receipt_invalid']

def deliver(config, state_path, alerts, now, canary=False, evidence=None):
    if not owner(config.get('recipient')): raise ValueError('recipient_mismatch')
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    if state.get('phase') in ('intent','uncertain'):
        return {'outcome':'HOLD_UNCERTAIN_SEND'}
    digest = hashlib.sha256(json.dumps(alerts).encode()).hexdigest()
    if state.get('digest') == digest or (not alerts and not state):
        return {'outcome':'NO_CHANGE', 'messages_sent':0}
    sent_at = state.get('sent_at')
    if sent_at and (now-dt.datetime.fromisoformat(sent_at)).total_seconds() < 3600 and not any(a.startswith('P0:') and a not in state.get('alerts',[]) for a in alerts):
        return {'outcome':'RATE_LIMITED', 'messages_sent':0}
    # Pre-read binding before any send. Never follow redirects with credentials.
    binding = api(config['key'])
    if binding.get('inbox_id') != INBOX or binding.get('status') == 'paused' or binding.get('metadata',{}).get('alertRecipient') != config['recipient']:
        raise ValueError('provider_binding_mismatch')
    subject = 'Ziji Fabric — IONOS email route test' if canary else ('Ziji Fabric — material condition' if alerts else 'Ziji Fabric — verified recovery')
    text = ('This is the single authorized server-to-email acceptance test. No action is required. '
            'It verifies email transport only; full Fabric receipt monitoring remains staged.' if canary else
            'Deterministic Fabric monitor observed: '+(', '.join(alerts) if alerts else 'verified recovery from the previously reported condition')+'. Review RBO-97 for the owning recovery action.')
    if evidence and not canary:
        details = []
        for key in alerts:
            item = evidence.get(key,{})
            if item:
                details.append(key+"; evidence="+str(item.get('reference'))+
                    "; observed="+str(item.get('observed_at'))+
                    "; safe default="+item.get('safe_default','Preserve last verified state')+
                    "; attempted recovery="+item.get('attempted_recovery','Read-only reconciliation')+
                    "; next action="+item.get('next_action','Inspect owning receipt'))
        text += "\n" + "\n".join(details)
    intent = {'phase':'intent','digest':digest,'observed_at':now.isoformat(),'alerts':alerts}
    atomic(state_path,intent)
    try:
        result = api(config['key'],'/messages/send',{'to':[config['recipient']],'subject':subject,'text':text,'labels':['fabric-server-alert'],'track_opens':False})
        if not result.get('message_id') or not result.get('thread_id'): raise ValueError('missing_receipt')
        final = dict(intent, phase='accepted',sent_at=now.isoformat(),message_id=result['message_id'],thread_id=result['thread_id'])
        atomic(state_path,final)
        return {'outcome':'PROVIDER_ACCEPTED','message_id':result['message_id'],'thread_id':result['thread_id'],'messages_sent':1,'mailbox_delivery_verified':False}
    except Exception:
        atomic(state_path,dict(intent,phase='uncertain'))
        raise

def main(canary=False):
    if CONFIG.is_symlink() or CONFIG.stat().st_mode & 0o077: raise ValueError('unsafe_config')
    config=json.loads(CONFIG.read_text())
    ROOT.mkdir(mode=0o700,parents=True,exist_ok=True)
    with (ROOT/'run.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        now=dt.datetime.now(dt.timezone.utc)
        if canary: alerts=['P1:host:authorized_transport_test']
        else:
            try:
                if SNAPSHOT.is_symlink() or SNAPSHOT.stat().st_size>65536: raise ValueError('invalid_snapshot')
                alerts=classify(json.loads(SNAPSHOT.read_text()),now)
            except (OSError,ValueError): alerts=['P1:host:watchdog_receipt_missing']
        evidence = {}
        if not canary and Path('/etc/ziji-fabric/failure-alarm.json').exists():
            from fabric_failure_alarm import collect
            report = collect(now)
            atomic(ROOT/'fabric-latest.json',report)
            alerts = sorted(set(alerts + report['alerts']))
            evidence = report['evidence']
            # Missing source coverage cannot establish a recovery.
            previous = json.loads((ROOT/'delivery.json').read_text()) if (ROOT/'delivery.json').exists() else {}
            uncertain = set()
            if report['checks']['jira'] != 'VERIFIED' or report['checks']['unparsed_hosted_run_records']:
                uncertain.update(('income','rsc','store','evidence'))
            if report['checks']['calendar_writer'] == 'UNAVAILABLE':
                uncertain.add('distiller')
            alerts = sorted(set(alerts + [a for a in previous.get('alerts',[]) if a.split(':')[1] in uncertain]))
        result=deliver(config, ROOT/('canary.json' if canary else 'delivery.json'),alerts,now,canary,evidence)
        atomic(ROOT/'latest.json',dict(result,observed_at=now.isoformat(),model_calls=0))
        print(json.dumps(result,sort_keys=True))

if __name__=='__main__':
    import sys
    try: main('--canary' in sys.argv)
    except Exception as exc:
        print(json.dumps({'outcome':'HOLD','error_type':type(exc).__name__,'values_withheld':True}))
        raise SystemExit(1)
