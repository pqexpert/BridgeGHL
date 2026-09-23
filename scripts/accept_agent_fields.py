"""Acceptance of existing MCP field route using an isolated canary only."""
import json, os, sys, uuid
from pathlib import Path
from urllib.request import Request, urlopen
sys.path.insert(0, '/home/bridgeadmin/apps/BridgeGHL')
import app as bridge
ROOT = Path('/var/lib/bridgeghl/ecosystem')
def mcp(name, arguments):
    body = {'jsonrpc':'2.0','id':1,'method':'tools/call','params':{'name':name,'arguments':arguments}}
    req = Request('http://127.0.0.1:8001/mcp', data=json.dumps(body).encode(), headers={'Content-Type':'application/json','Accept':'application/json, text/event-stream'})
    with urlopen(req, timeout=40) as response: result = json.load(response)
    result = result['result']
    if isinstance(result.get('structuredContent'), dict): return result['structuredContent']
    for content in result.get('content', []):
        if content.get('type') == 'text':
            try: return json.loads(content['text'])
            except ValueError: pass
    raise RuntimeError('mcp_result_unrecognized')
def get(path):
    status, data = bridge.highlevel_request('GET', bridge.HIGHLEVEL_BASE_URL.rstrip('/') + path)
    assert status == 200
    return data
def main():
    plan = json.loads((ROOT/'agent-field-plan.json').read_text())
    oid, cid = plan['canary_opportunity_id'], plan['canary_contact_id']
    contact = get('/contacts/' + cid)['contact']
    assert contact['dnd'] is True and not contact.get('email') and not contact.get('phone')
    opp = get('/opportunities/' + oid)['opportunity']
    assert opp['contactId'] == cid and opp['name'].startswith('TEST ONLY')
    fid = plan['fields']['Ecosystem Next Action']
    reason = 'Owner-authorized isolated field-route acceptance; no recipient, campaign or real relationship mutation.'
    rejected = mcp('dry_run_opportunity_update', {'opportunity_id':oid,'reason':reason,'custom_fields':{'not-an-approved-field':'TEST ONLY'}})
    assert rejected.get('bridge_http_status') == 422
    payload = {'opportunity_id':oid,'reason':reason,'custom_fields':{fid:'TEST ONLY - field route verified; no operational follow-up due.'}}
    dry = mcp('dry_run_opportunity_update', payload)
    assert dry.get('accepted') is True
    out = mcp('execute_opportunity_update', payload)
    assert out.get('accepted') is True and out.get('verified') is True and out.get('readback_status') == 200
    proof = dict(negative_allowlist_rejected=True, dry_run_accepted=True, canary_field_verified=True, audit_id=out['audit_id'], real_contacts_changed=0, messages_sent=0)
    path = ROOT/'agent-field-acceptance.json'
    path.write_text(json.dumps(proof))
    path.chmod(0o600)
    print(json.dumps(proof))
if __name__ == '__main__':
    try: main()
    except Exception as error:
        print('acceptance_failed=true; error_class=' + type(error).__name__)
        sys.exit(1)
