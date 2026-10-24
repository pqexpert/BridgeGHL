import pytest
from scripts import income_workbench as workbench


def fixture(monkeypatch, *, native_changes=None):
    sources = {
        'a': {'title': 'Onsite role', 'source_url': 'https://source.invalid/a',
              'properties': {'Work Mode': 'Onsite', 'Type': 'IT Support'}},
        'b': {'title': 'Remote contract', 'source_url': 'https://source.invalid/b',
              'properties': {'Work Mode': 'Remote', 'Type': 'Contract',
                             'Contact Email': 'PRIVATE', 'Next Action': 'stale source'}},
    }
    bindings = [(sid, {}, {'contact_id': 'context', 'opportunity_id': sid}) for sid in sources]
    monkeypatch.setattr(workbench, 'verified_notes', lambda *a: {})
    monkeypatch.setattr(workbench, 'assemble', lambda notes, key, *a: {
        'd'*64: {'source': next(s for sid, s in sources.items() if workbench.source_key(sid) == key)}})
    monkeypatch.setattr(workbench, 'read_income_source', lambda ref, *a: {
        'source': sources[ref['source_id']], 'opportunity_updated': 'version'})
    calls = []
    def call(method, path):
        calls.append((method, path))
        if path == '/contacts/context':
            return {'contact': {'id': 'context', 'locationId': 'location'}}
        if path.endswith('/notes'):
            return {'notes': []}
        sid = path.split('/')[-1]
        return {'opportunity': {'id': sid, 'contactId': 'context', 'locationId': 'location',
            'pipelineId': 'career', 'name': 'role [src:'+workbench.source_key(sid)+']',
            'status': 'open', 'updatedAt': 'version',
            'customFields': [{'id': 'next', 'fieldValue': 'Current native action'}],
            **(native_changes or {})}}
    def build(limit=1):
        return workbench.build(limit, location_id='location', pipeline_id='career',
            default_contact_id='context', journal_path='unused',
            fields={'opportunity:Ecosystem Next Action': 'next'}, call=call, bindings=bindings)
    return build, calls


def test_prefers_explicit_remote_contract_and_preserves_current_native_action(monkeypatch):
    build, calls = fixture(monkeypatch)
    report = build()
    entry = report['entries'][0]
    assert entry['source_id'] == 'b' and entry['preference_band'] == 'remote_contract'
    assert entry['next_action'] == 'Current native action'
    assert entry['gaps'] == ['owner', 'follow_up']
    assert entry['employer_posting_revalidated'] is False and entry['execution_admitted'] is False
    assert 'PRIVATE' not in str(report) and 'Contact Email' not in str(report)
    assert all(method == 'GET' for method, _ in calls)


@pytest.mark.parametrize('changes', [
    {'locationId': 'other'}, {'contactId': 'other'}, {'pipelineId': 'other'},
    {'name': 'unrelated'}, {'updatedAt': 'changed'}, {'id': 'wrong'},
])
def test_identity_or_current_native_change_stops_report(monkeypatch, changes):
    build, _ = fixture(monkeypatch, native_changes=changes)
    with pytest.raises(ValueError, match='workbench_native_changed'):
        build()


def test_closed_native_is_excluded_instead_of_reopened(monkeypatch):
    build, _ = fixture(monkeypatch, native_changes={'status': 'won'})
    result = build()
    assert result['entries'] == [] and result['excluded']['native_not_open'] == 1


@pytest.mark.parametrize('value', ['not remote', 'Hybrid', '', None])
def test_ambiguous_or_negative_work_mode_is_never_remote(value):
    assert workbench.preference({'Work Mode': value}) == 3


@pytest.mark.parametrize('limit', [0, 13, True])
def test_bounded_sample(monkeypatch, limit):
    build, calls = fixture(monkeypatch)
    with pytest.raises(ValueError, match='workbench_limit'):
        build(limit)
    assert calls == []
