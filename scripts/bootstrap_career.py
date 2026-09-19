"""One-time authorized career destination setup inside the protected bridge runtime."""
import json
import os
from pathlib import Path
import re
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app as bridge
from ingestion import journal

ROOT = Path('/var/lib/bridgeghl/career-import')

def call(method, path, **kwargs):
    status, data = bridge.highlevel_request(method, bridge.HIGHLEVEL_BASE_URL.rstrip('/') + path, **kwargs)
    if not 200 <= status < 300:
        raise RuntimeError('provider_status=' + str(status))
    return data

def main():
    control = json.loads((ROOT / 'control.json').read_text())
    if bridge.HIGHLEVEL_LOCATION_ID != control['expected_location_id']:
        raise RuntimeError('Location does not match the authenticated account')
    if not bridge.API_KEY or bridge.bridge_health_snapshot().state != 'HEALTHY':
        raise RuntimeError('Bridge health gate failed')
    pipeline_name = 'Income Accelerator - Career Pursuits'
    context_name = 'Income Accelerator Source Context'
    plan = {'pipeline': pipeline_name, 'stages': ['Source intake', 'Preparing', 'Applied', 'Interviewing', 'Offer'],
            'context_contact': context_name, 'messaging': False, 'source_sha256': control['sha256']}
    (ROOT / 'bootstrap-dry-run.json').write_text(json.dumps(plan, indent=2))
    os.chmod(ROOT / 'bootstrap-dry-run.json', 0o600)
    bridge.append_audit_log({'action': 'career_bootstrap', 'result': 'intent', **plan})
    with journal(str(ROOT / 'bootstrap.sqlite3')) as db:
        def effect(key, find, create):
            found = find()
            if found: return found
            if db.execute('SELECT key FROM effects WHERE key=?', (key,)).fetchone():
                raise RuntimeError('Ambiguous prior bootstrap effect; native reconciliation required')
            db.execute('INSERT INTO effects VALUES (?,?,?)', (key, 'pending', None)); db.commit()
            created = create()
            native = created.get('contact') or created.get('pipeline') or created
            native_id = native.get('id')
            db.execute('UPDATE effects SET native_id=? WHERE key=?',(native_id,key));db.commit()
            for attempt in range(4):
                found = find()
                if found: break
                time.sleep(2)
            if not found: raise RuntimeError('Bootstrap native readback failed: '+key)
            db.execute('UPDATE effects SET state=?,native_id=? WHERE key=?', ('verified',found['id'],key));db.commit()
            return found
        def find_pipeline():
            matches = [p for p in call('GET','/opportunities/pipelines',params={'locationId':bridge.HIGHLEVEL_LOCATION_ID}).get('pipelines',[]) if p.get('name') == pipeline_name]
            if len(matches)>1: raise RuntimeError('Ambiguous pipeline')
            return matches[0] if matches else None
        pipeline = effect('income-pipeline',find_pipeline,lambda:call('POST','/opportunities/pipelines',body={
            'locationId':bridge.HIGHLEVEL_LOCATION_ID,'name':pipeline_name,'showInFunnel':False,'showInPieChart':False,
            'stages':[{'name':name,'position':i,'showInFunnel':False} for i,name in enumerate(plan['stages'])]}))
        def find_contact():
            data=call('GET','/contacts/',params={'locationId':bridge.HIGHLEVEL_LOCATION_ID,'query':context_name,'limit':100})
            matches=[c for c in data.get('contacts',[]) if (c.get('contactName') or c.get('name') or ' '.join(filter(None,[c.get('firstName'),c.get('lastName')]))).lower()==context_name.lower()]
            if len(matches)>1: raise RuntimeError('Ambiguous context contact')
            return matches[0] if matches else None
        contact=effect('income-context',find_contact,lambda:call('POST','/contacts/',body={'locationId':bridge.HIGHLEVEL_LOCATION_ID,
            'name':context_name,'dnd':True,'source':'BridgeGHL source context; not a recruiter or outreach recipient'}))
        actual=call('GET','/contacts/'+contact['id']).get('contact',{})
        if actual.get('locationId')!=bridge.HIGHLEVEL_LOCATION_ID: raise RuntimeError('Context location mismatch')
        stage=next(s for s in pipeline['stages'] if s['name']=='Source intake')
        bindings={'HIGHLEVEL_INGEST_INCOME_PIPELINE_ID':pipeline['id'],'HIGHLEVEL_INGEST_INCOME_STAGE_ID':stage['id'],
            'HIGHLEVEL_INGEST_INCOME_DEFAULT_CONTACT_ID':contact['id'],'LIVE_WRITE_ENABLED':'true','HIGHLEVEL_INGEST_ENABLED':'true'}
        if any(not re.fullmatch(r'[A-Za-z0-9_-]+',v) for v in bindings.values()): raise RuntimeError('Invalid configuration value')
        env=Path('/etc/bridgeghl/bridgeghl.env')
        lines=[line for line in env.read_text().splitlines() if line.split('=',1)[0] not in bindings]
        temp=env.with_suffix('.career-tmp')
        fd=os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
        with os.fdopen(fd,'w') as f: f.write('\n'.join(lines+[k+'='+v for k,v in bindings.items()])+'\n')
        stat=env.stat();os.chown(temp,stat.st_uid,stat.st_gid);os.chmod(temp,stat.st_mode & 0o777);os.replace(temp,env)
        (ROOT/'bindings.json').write_text(json.dumps(bindings));os.chmod(ROOT/'bindings.json',0o600)
        bridge.append_audit_log({'action':'career_bootstrap','result':'verified','pipeline_id':pipeline['id'],'contact_id':contact['id']})
        print(json.dumps({'career_destination':'verified','live_write_enabled':True}))

if __name__=='__main__':
    try: main()
    except Exception as exc:
        print(json.dumps({'state':'BLOCKED','phase':'bootstrap','error_type':type(exc).__name__,
                          'detail':str(exc)[:160] if isinstance(exc,RuntimeError) else 'See private runtime receipt'}))
        sys.exit(1)
