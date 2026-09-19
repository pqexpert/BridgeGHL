import json
from types import SimpleNamespace
import pytest
from fastapi import HTTPException
import ingestion
from scripts.ingest_drive import source_records


def record(**kw):
    return ingestion.IngestRecord(domain='income', source_id='source-1', source_url='https://app.notion.com/p/source-1',
                                  title='Role', kind='context', **kw)


def test_full_source_content_survives_chunking():
    r = record(content='é' * 30000, properties={'Next Action': 'Research', 'Resume': 'https://drive.google.com/x'})
    bodies = ingestion.note_bodies(r)
    recovered = json.loads(''.join(b.split('\n', 1)[1] for b in bodies))
    assert recovered['content'] == r.content
    assert recovered['properties'] == r.properties
    assert len(bodies) > 1


def test_export_includes_pursuit_pages_and_asset_links():
    sid, report = 'a' * 32, 'b' * 32
    bundle = {'schema_version':'career-drive-export/1',
        'collections':{'assets':{'complete':True,'rows':[{'url':'https://app.notion.com/p/'+sid,'Asset Name':'Resume'}]}},
        'current_pursuit_pages':[{'url':'https://app.notion.com/p/'+sid+'?pvs=204','text':'Full source'},
                                 {'url':'https://app.notion.com/p/'+report,'title':'Pursuit report','text':'Both career routes'}],
        'current_drive_assets':[{'id':'private-asset','title':'Resume'}]}
    records=list(source_records(bundle,['assets']))
    assert len(records)==2
    assert records[0]['content']=='Full source'
    assert records[1]['content']=='Both career routes'
    assert records[1]['properties']['drive_assets']==bundle['current_drive_assets']


def test_dzokden_domain_rejected():
    with pytest.raises(ValueError):
        ingestion.IngestRecord(domain='dzokden', source_id='s', source_url='x', title='x', kind='contact')


def test_missing_contact_not_invented(monkeypatch):
    routes = {}
    bridge = SimpleNamespace(app=SimpleNamespace(post=lambda p: lambda f: routes.setdefault(p, f)), require_api_key=lambda k: None)
    ingestion.register_routes(bridge)
    monkeypatch.delenv('HIGHLEVEL_INGEST_INCOME_DEFAULT_CONTACT_ID', raising=False)
    result = routes['/dry-run/ingest/source-record'](record(), 'key')
    assert not result['accepted']
    assert result['missing_configuration'] == ['default_contact_id']


def test_disabled_ingestion_never_calls_provider(monkeypatch):
    routes = {}
    bridge = SimpleNamespace(app=SimpleNamespace(post=lambda p: lambda f: routes.setdefault(p, f)), require_api_key=lambda k: None)
    ingestion.register_routes(bridge)
    monkeypatch.delenv('HIGHLEVEL_INGEST_ENABLED', raising=False)
    with pytest.raises(HTTPException) as exc:
        routes['/execute/ingest/source-record'](record(), 'key')
    assert exc.value.status_code == 409


@pytest.mark.parametrize('reject_first', [False, True])
def test_replay_reads_native_note_and_does_not_create_twice(monkeypatch, tmp_path, reject_first):
    routes, notes, audits = {}, {}, []
    attempts = []
    monkeypatch.setenv('HIGHLEVEL_INGEST_ENABLED', 'true')
    monkeypatch.setenv('HIGHLEVEL_INGEST_INCOME_DEFAULT_CONTACT_ID', 'contact1')
    def provider(method, url, **kw):
        path = url.removeprefix('https://provider')
        if path == '/contacts/contact1': return 200, {'contact': {'id': 'contact1', 'locationId': 'loc'}}
        if path == '/contacts/contact1/notes' and method == 'GET': return 200, {'notes': list(notes.values())}
        if path == '/contacts/contact1/notes' and method == 'POST':
            attempts.append(1)
            if reject_first and len(attempts)==1: return 400, {'message':'Invalid request'}
            nid = str(len(notes) + 1)
            notes[nid] = {'id': nid, **kw['body']}
            return 201, {'note': notes[nid]}
        if path.startswith('/contacts/contact1/notes/'): return 200, {'note': notes[path.rsplit('/', 1)[-1]]}
        raise AssertionError((method, path))
    bridge = SimpleNamespace(app=SimpleNamespace(post=lambda p: lambda f: routes.setdefault(p, f)), require_api_key=lambda k: None,
        highlevel_request=provider, HIGHLEVEL_BASE_URL='https://provider', HIGHLEVEL_LOCATION_ID='loc',
        AUDIT_LOG_PATH=str(tmp_path / 'audit.jsonl'), append_audit_log=audits.append,
        bridge_health_snapshot=lambda: SimpleNamespace(state='HEALTHY'))
    ingestion.register_routes(bridge)
    execute = routes['/execute/ingest/source-record']
    if reject_first:
        with pytest.raises(HTTPException): execute(record(), 'key')
    assert execute(record(), 'key')['verified']
    assert execute(record(), 'key')['verified']
    assert len(notes) == 1
    # Lost native state must not silently cause another POST.
    notes.clear()
    with pytest.raises(HTTPException) as exc: execute(record(), 'key')
    assert exc.value.status_code == 409
    assert not notes


def test_cross_location_contact_stops_before_notes(monkeypatch, tmp_path):
    routes = {}
    monkeypatch.setenv('HIGHLEVEL_INGEST_ENABLED', 'true')
    monkeypatch.setenv('HIGHLEVEL_INGEST_INCOME_DEFAULT_CONTACT_ID', 'contact1')
    calls = []
    def provider(method, url, **kw):
        calls.append(method)
        return 200, {'contact': {'id': 'contact1', 'locationId': 'other'}}
    bridge = SimpleNamespace(app=SimpleNamespace(post=lambda p: lambda f: routes.setdefault(p, f)), require_api_key=lambda k: None,
        highlevel_request=provider, HIGHLEVEL_BASE_URL='https://provider', HIGHLEVEL_LOCATION_ID='loc',
        AUDIT_LOG_PATH=str(tmp_path / 'audit.jsonl'), append_audit_log=lambda e: None,
        bridge_health_snapshot=lambda: SimpleNamespace(state='HEALTHY'))
    ingestion.register_routes(bridge)
    with pytest.raises(HTTPException): routes['/execute/ingest/source-record'](record(), 'key')
    assert calls == ['GET']


def test_export_preserves_status_and_links_without_inventing_email():
    row = {'url': 'https://app.notion.com/p/' + 'a'*32, 'Company': 'Employer', 'Role': 'Analyst', 'Status': 'Applied', 'Tailored Resume Link': 'https://drive.google.com/file'}
    bundle = {'schema_version': 'career-drive-export/1', 'collections': {'job_applications': {'complete': True, 'rows': [row]}}}
    r = list(source_records(bundle, ['job_applications']))[0]
    assert r['properties'] == row
    assert r['contact_email'] is None
