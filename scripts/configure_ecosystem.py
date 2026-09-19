"""Bounded, resumable RSC configuration using the existing protected bridge.

Default is inventory/dry-run. --execute admits only this reviewed manifest.
No outbound messages, workflow activation, real contact edits or credential writes.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app as bridge
from ingestion import journal

VERSION = 'rsc-ecosystem/1'
LOCATION = 'p8CNFbnxiaIi44O6mZq2'
ROOT = Path('/var/lib/bridgeghl/ecosystem')
COMMON_FIELDS = ['Mission Domain', 'Source Record ID', 'Source URL', 'Drive Asset URL',
                 'Evidence Status', 'Data Quality', 'Next Action', 'Next Follow-Up Date',
                 'Source Observed At', 'Record Mode']
CONTACT_FIELDS = ['Relationship Type', 'Relationship Stage', 'Organization Role',
                  'Outreach Permission', 'Last Meaningful Interaction']
OPPORTUNITY_FIELDS = ['Original Source Status', 'Priority', 'Primary Offer Type',
                      'Environment Stage', 'Technical Readiness Summary',
                      'Commercial Language Allowed', 'Next Technical Proof Needed',
                      'Blocker or Risk Note', 'Buyer Sector', 'Buyer Role',
                      'Source Surface and Attribution', 'Owning Mission']
PIPELINES = {
    'RSC - Commercial Opportunities': ['Source intake', 'Qualified', 'Discovery',
                                     'Solution and proof', 'Proposal', 'Decision'],
    'Ecosystem - Relationship Stewardship': ['Source intake', 'Identity verified',
                                           'Context prepared', 'Engaged', 'Next action agreed', 'Maintaining'],
    'TEST ONLY - BridgeGHL Validation': ['Draft test', 'Verified test'],
}
TAGS = ['bridgeghl:managed', 'mode:test', 'mode:draft', 'domain:income', 'domain:rsc',
        'context:syracuse', 'context:ziji', 'context:fungible', 'source:google-drive',
        'quality:needs-enrichment', 'outreach:review-required', 'record:source-context']


def fields():
    return [{'name': 'Ecosystem ' + name, 'model': model, 'dataType': 'TEXT'}
            for model, extra in [('contact', CONTACT_FIELDS), ('opportunity', OPPORTUNITY_FIELDS)]
            for name in COMMON_FIELDS + extra]


def call(method, path, **kw):
    status, data = bridge.highlevel_request(method, bridge.HIGHLEVEL_BASE_URL.rstrip('/') + path, **kw)
    if not 200 <= status < 300:
        # Provider bodies can contain private data. Keep failures bounded.
        raise RuntimeError('provider_status=' + str(status) + ' method=' + method + ' resource=' + path.split('/')[1])
    return data


def unique(items, predicate):
    matches = [x for x in items if predicate(x)]
    if len(matches) > 1:
        raise RuntimeError('Ambiguous native identity; reconcile before mutation')
    return matches[0] if matches else None


def persist(name, value):
    ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(ROOT, 0o700)
    path = ROOT / name
    tmp = path.with_suffix('.tmp')
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(value, stream, indent=2)
    os.replace(tmp, path)


def ensure(db, key, find, create, verify, read_id=None):
    """Reconcile native state first; never replay a write with uncertain effects."""
    found = find()
    previous = db.execute('SELECT state,native_id FROM effects WHERE key=?', (key,)).fetchone()
    if not found and previous and previous[1] and read_id:
        found = read_id(previous[1])
    if found:
        verify(found)
        db.execute('INSERT OR REPLACE INTO effects VALUES (?,?,?)', (key, 'verified', found['id']))
        db.commit()
        return found
    if previous:
        raise RuntimeError('Prior effect not visible: ' + key + '; no write retry')
    db.execute('INSERT INTO effects VALUES (?,?,?)', (key, 'pending', None))
    db.commit()
    bridge.append_audit_log({'action': 'ecosystem_config', 'version': VERSION, 'effect': key, 'result': 'intent'})
    created = create()
    if not created.get('id'):
        raise RuntimeError('Missing native ID: ' + key)
    db.execute('UPDATE effects SET native_id=? WHERE key=?', (created['id'], key))
    db.commit()
    for attempt in range(3):
        found = read_id(created['id']) if read_id else find()
        if found:
            break
        time.sleep(1)
    if not found or found['id'] != created['id']:
        raise RuntimeError('Native readback differs: ' + key)
    verify(found)
    db.execute('UPDATE effects SET state=? WHERE key=?', ('verified', key))
    db.commit()
    bridge.append_audit_log({'action': 'ecosystem_config', 'version': VERSION, 'effect': key,
                             'result': 'verified', 'native_id': found['id']})
    return found


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    require(bridge.HIGHLEVEL_LOCATION_ID == LOCATION, 'Location mismatch')
    require(bridge.bridge_health_snapshot().state == 'HEALTHY', 'Bridge health gate failed')
    loc = '/locations/' + LOCATION
    existing_fields = call('GET', loc + '/customFields', params={'model': 'all'}).get('customFields', [])
    existing_pipelines = call('GET', '/opportunities/pipelines', params={'locationId': LOCATION}).get('pipelines', [])
    existing_tags = call('GET', loc + '/tags').get('tags', [])
    workflows = call('GET', '/workflows/', params={'locationId': LOCATION}).get('workflows', [])
    plan = {'schema_version': VERSION, 'location_id': LOCATION, 'fields': fields(),
            'pipelines': PIPELINES, 'tags': TAGS, 'messages': False,
            'existing_field_count': len(existing_fields),
            'existing_pipeline_names': [p['name'] for p in existing_pipelines],
            'workflow_states': [{'id': w['id'], 'status': w.get('status')} for w in workflows],
            'canary': {'name': 'TEST ONLY - BridgeGHL Ecosystem Canary', 'dnd': True,
                       'email': None, 'phone': None, 'opportunities': 2, 'completed_tasks': 1}}
    digest = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
    persist('plan.json', plan)
    print(json.dumps({'phase': 'dry_run', 'manifest': VERSION, 'sha256': digest,
                      'field_count': len(fields()), 'pipeline_count': len(PIPELINES),
                      'workflow_states': [w.get('status') for w in workflows]}), flush=True)
    if not args.execute:
        return
    # A published workflow can react to CRM creation even without an explicit
    # enrolment. Do not claim DND alone suppresses every workflow side effect.
    require(all(w.get('status') == 'draft' for w in workflows),
            'Active or unknown workflow requires trigger review before canary creation')
    receipt = {'schema_version': VERSION, 'location_id': LOCATION, 'manifest_sha256': digest,
               'fields': {}, 'pipelines': {}, 'tags': {}, 'canary': {}, 'messages_sent': 0}
    with journal(str(ROOT / 'effects.sqlite3')) as db:
        for f in fields():
            def find(f=f):
                return unique(call('GET', loc + '/customFields', params={'model': f['model']}).get('customFields', []),
                              lambda x: x['name'] == f['name'])
            def verify(x, f=f):
                require(x.get('model') == f['model'] and x.get('dataType') == f['dataType'], 'Field type mismatch')
            item = ensure(db, 'field:' + f['model'] + ':' + f['name'], find,
                          lambda f=f: call('POST', loc + '/customFields', body=f)['customField'], verify)
            receipt['fields'][f['model'] + ':' + f['name']] = item['id']
        for name in TAGS:
            item = ensure(db, 'tag:' + name,
                          lambda name=name: unique(call('GET', loc + '/tags').get('tags', []), lambda x: x['name'] == name),
                          lambda name=name: call('POST', loc + '/tags', body={'name': name})['tag'], lambda x: None)
            receipt['tags'][name] = item['id']
        for name, stages in PIPELINES.items():
            def find(name=name):
                return unique(call('GET', '/opportunities/pipelines', params={'locationId': LOCATION}).get('pipelines', []), lambda x: x['name'] == name)
            def verify(x, stages=stages):
                require([s['name'] for s in sorted(x['stages'], key=lambda s: s.get('position', 0))] == stages, 'Pipeline stages differ')
            item = ensure(db, 'pipeline:' + name, find,
                          lambda name=name, stages=stages: call('POST', '/opportunities/pipelines', body={
                              'locationId': LOCATION, 'name': name, 'showInFunnel': False, 'showInPieChart': False,
                              'stages': [{'name': s, 'position': i, 'showInFunnel': False} for i, s in enumerate(stages)]}).get('pipeline'), verify)
            receipt['pipelines'][name] = item
        persist('receipt.json', receipt)
        canary_name = plan['canary']['name']
        def find_canary():
            return unique(call('POST', '/contacts/search', body={'locationId': LOCATION, 'query': canary_name,
                          'page': 1, 'pageLimit': 100}).get('contacts', []),
                          lambda x: (x.get('contactName') or x.get('name') or ' '.join(filter(None, [x.get('firstName'), x.get('lastName')]))).lower() == canary_name.lower())
        def verify_canary(x):
            c = call('GET', '/contacts/' + x['id'])['contact']
            require(c.get('locationId') == LOCATION and c.get('dnd') is True and not c.get('email') and not c.get('phone'), 'Canary isolation mismatch')
            require((c.get('name') or c.get('contactName') or ' '.join(filter(None, [c.get('firstName'), c.get('lastName')]))).lower() == canary_name.lower(), 'Canary name mismatch')
        contact = ensure(db, 'canary:contact', find_canary, lambda: call('POST', '/contacts/', body={
            'locationId': LOCATION, 'firstName': 'TEST ONLY - BridgeGHL', 'lastName': 'Ecosystem Canary',
            'name': canary_name, 'source': 'BridgeGHL test - no outreach', 'dnd': True,
            'tags': ['mode:test', 'bridgeghl:managed'],
            'customFields': [{'id': receipt['fields']['contact:Ecosystem Record Mode'], 'field_value': 'test'}]})['contact'], verify_canary,
            lambda cid: call('GET', '/contacts/' + cid)['contact'])
        cid = contact['id']
        receipt['canary']['contact_id'] = cid
        pipeline = receipt['pipelines']['TEST ONLY - BridgeGHL Validation']
        opportunity_ids = []
        for suffix in ('A', 'B'):
            title = 'TEST ONLY - Shared contact opportunity ' + suffix
            def find_opp(title=title):
                return unique(call('GET', '/opportunities/search', params={'location_id': LOCATION,
                              'pipeline_id': pipeline['id'], 'contact_id': cid, 'limit': 100}).get('opportunities', []), lambda x: x['name'] == title)
            def verify_opp(x):
                o = call('GET', '/opportunities/' + x['id'])['opportunity']
                require(o.get('contactId') == cid and o.get('pipelineId') == pipeline['id'], 'Canary opportunity mismatch')
            op = ensure(db, 'canary:opportunity:' + suffix, find_opp, lambda title=title: call('POST', '/opportunities/', body={
                'locationId': LOCATION, 'pipelineId': pipeline['id'], 'pipelineStageId': pipeline['stages'][0]['id'],
                'contactId': cid, 'name': title, 'status': 'open', 'monetaryValue': 0,
                'customFields': [{'id': receipt['fields']['opportunity:Ecosystem Record Mode'], 'field_value': 'test'}]})['opportunity'], verify_opp,
                lambda oid: call('GET', '/opportunities/' + oid)['opportunity'])
            opportunity_ids.append(op['id'])
        require(len(set(opportunity_ids)) == 2, 'Shared-parent canary did not produce distinct opportunities')
        receipt['canary']['opportunity_ids'] = opportunity_ids
        task_title = 'TEST ONLY - Completed follow-up capability check'
        def find_task():
            return unique(call('GET', '/contacts/' + cid + '/tasks').get('tasks', []), lambda x: x['title'] == task_title)
        task = ensure(db, 'canary:task', find_task, lambda: call('POST', '/contacts/' + cid + '/tasks', body={
            'title': task_title, 'body': 'Synthetic completed task. Do not contact anyone. Validates the native task route.',
            'dueDate': '2026-09-19T23:59:00Z', 'completed': True})['task'],
            lambda x: require(x.get('completed') is True, 'Canary task completion mismatch'))
        receipt['canary']['task_id'] = task['id']
        receipt['verified'] = True
        persist('receipt.json', receipt)
        print(json.dumps({'phase': 'verified', 'fields': len(receipt['fields']), 'tags': len(receipt['tags']),
                          'pipelines': len(receipt['pipelines']), 'canary': receipt['canary'], 'messages_sent': 0}), flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'state': 'BLOCKED', 'error_type': type(exc).__name__,
                          'detail': str(exc)[:180] if isinstance(exc, RuntimeError) else 'Inspect private runtime evidence'}), flush=True)
        sys.exit(1)
