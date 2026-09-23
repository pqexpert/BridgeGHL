"""Bind existing operational CRM fields; no contact/opportunity mutation."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from urllib.request import Request, urlopen

sys.path.insert(0, '/home/bridgeadmin/apps/BridgeGHL')
import app as bridge

NAMES = ['Ecosystem Next Action', 'Ecosystem Next Follow-Up Date',
         'Ecosystem Evidence Status', 'Ecosystem Data Quality',
         'Ecosystem Next Technical Proof Needed', 'Ecosystem Blocker or Risk Note']
TAGS = ['mode:test', 'mode:draft', 'quality:needs-enrichment', 'outreach:review-required']
ROOT = Path('/var/lib/bridgeghl/ecosystem')
ENV = Path('/etc/bridgeghl/bridgeghl.env')

def get(path, **params):
    status, result = bridge.highlevel_request('GET', bridge.HIGHLEVEL_BASE_URL.rstrip('/') + path, params=params)
    if status != 200:
        raise RuntimeError('provider_read_failed')
    return result

def atomic(path, data, mode=0o600):
    fd, tmp = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)

def render_env(original, updates):
    lines = original.splitlines()
    for key, value in updates.items():
        indices = [i for i, line in enumerate(lines) if line.startswith(key + '=')]
        if len(indices) > 1: raise RuntimeError('ambiguous_environment_binding')
        if indices: lines[indices[0]] = key + '=' + value
        else: lines.append(key + '=' + value)
    return '\n'.join(lines) + '\n'

def main():
    assert bridge.HIGHLEVEL_LOCATION_ID == 'p8CNFbnxiaIi44O6mZq2'
    loc = '/locations/' + bridge.HIGHLEVEL_LOCATION_ID
    fields = get(loc + '/customFields', model='opportunity')['customFields']
    mapping = {}
    for name in NAMES:
        matches = [f for f in fields if f['name'] == name and f.get('model') == 'opportunity' and f.get('dataType') == 'TEXT']
        if len(matches) != 1: raise RuntimeError('field_identity_not_unique')
        mapping[name] = matches[0]['id']
    tags = get(loc + '/tags')['tags']
    if not all(sum(t['name'] == name for t in tags) == 1 for name in TAGS):
        raise RuntimeError('tag_identity_not_unique')
    workflows = get('/workflows/', locationId=bridge.HIGHLEVEL_LOCATION_ID)['workflows']
    if any(w.get('status') != 'draft' for w in workflows):
        raise RuntimeError('workflow_trigger_review_required')
    receipt = json.loads((ROOT / 'receipt.json').read_text())
    cid = receipt['canary']['contact_id']
    contact = get('/contacts/' + cid)['contact']
    assert contact.get('dnd') is True and not contact.get('email') and not contact.get('phone')
    assert 'mode:test' in contact.get('tags', [])
    pipeline = receipt['pipelines']['TEST ONLY - BridgeGHL Validation']
    oid = receipt['canary']['opportunity_ids'][0]
    opp = get('/opportunities/' + oid)['opportunity']
    assert opp.get('pipelineId') == pipeline['id'] and opp.get('contactId') == cid
    assert opp['name'].startswith('TEST ONLY')
    existing_fields = set(bridge.APPROVED_CUSTOM_FIELD_IDS)
    existing_tags = set(bridge.APPROVED_CONTACT_TAGS)
    if not existing_fields.issubset(mapping.values()) or not existing_tags.issubset(TAGS):
        raise RuntimeError('unreviewed_existing_allowlist')
    plan = dict(fields=mapping, tags=TAGS, canary_contact_id=cid, canary_opportunity_id=oid,
                prior_stage=opp.get('pipelineStageId'), prior_status=opp.get('status'),
                test_stages=pipeline['stages'], workflows_draft=True)
    ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic(ROOT / 'agent-field-plan.json', json.dumps(plan).encode())
    print('native_mapping_verified=true; operational_fields=6; bounded_tags=4; canary_isolated=true', flush=True)
    before = ENV.read_bytes()
    updates = {'HIGHLEVEL_APPROVED_CUSTOM_FIELD_IDS': ','.join(sorted(mapping.values())),
               'HIGHLEVEL_APPROVED_CONTACT_TAGS': ','.join(sorted(TAGS))}
    after = render_env(before.decode(), updates).encode()
    backup = ROOT / 'agent-field-env-before.bin'
    if before != after:
        atomic(backup, before)
        atomic(ENV, after, ENV.stat().st_mode & 0o777)
        try:
            subprocess.run(['systemctl', 'restart', 'bridgeghl.service'], check=True, capture_output=True)
            for attempt in range(15):
                try:
                    request = Request('http://127.0.0.1:8000/health', headers={'x-api-key': os.environ['BRIDGE_API_KEY']})
                    with urlopen(request, timeout=10) as response: health = json.load(response)
                    assert health['state'] == 'HEALTHY' and health['approved_custom_field_count'] == 6 and health['approved_contact_tag_count'] == 4
                    break
                except Exception:
                    if attempt == 14: raise
                    time.sleep(1)
        except Exception:
            atomic(ENV, before, ENV.stat().st_mode & 0o777)
            subprocess.run(['systemctl', 'restart', 'bridgeghl.service'], check=True, capture_output=True)
            raise RuntimeError('binding_failed_prior_environment_restored')
    print('operational_binding_verified=true; crm_records_mutated=0; messages_sent=0', flush=True)
    # Only isolated test identifiers are emitted, never real contact data.
    print(json.dumps({'test_contact_id': cid, 'test_opportunity_id': oid, 'prior_stage': opp.get('pipelineStageId'),
                      'test_stage_ids': [s['id'] for s in pipeline['stages']]}), flush=True)

if __name__ == '__main__':
    try: main()
    except Exception as error:
        print('binding_failed=true; error_class=' + type(error).__name__, flush=True)
        sys.exit(1)
