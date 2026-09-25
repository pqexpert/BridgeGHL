from types import SimpleNamespace
import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from journey_evidence import EvidenceQuery, read_evidence

CONTACT = {'contact': {'id': 'person', 'locationId': 'location'}}
def bridge_for(*responses):
    replies = iter(responses)
    calls = []
    def request(method, url, **kwargs):
        assert method == 'GET'
        assert kwargs['retry_reads'] is False
        calls.append((url, kwargs))
        return next(replies)
    return SimpleNamespace(HIGHLEVEL_LOCATION_ID='location', HIGHLEVEL_PIT='server-only', HIGHLEVEL_BASE_URL='https://services.leadconnectorhq.com', highlevel_request=request, calls=calls)

def read(bridge, resource='tasks', **kwargs):
    return read_evidence(bridge, EvidenceQuery(contact_id='person', resource=resource, **kwargs))

def test_wrong_resource_not_empty_success():
    bridge = bridge_for((200, CONTACT), (200, {'contacts': []}))
    with pytest.raises(HTTPException): read(bridge)

def test_location_fails_before_child_access():
    bridge = bridge_for((200, {'contact': {'id': 'person', 'locationId': 'elsewhere'}}))
    with pytest.raises(HTTPException) as exc: read(bridge)
    assert exc.value.status_code == 403
    assert len(bridge.calls) == 1

@pytest.mark.parametrize('status', [401, 403])
def test_auth_failure_not_no_records(status):
    with pytest.raises(HTTPException) as exc: read(bridge_for((status, {'sensitive': 'do not return'})))
    assert exc.value.status_code == 424
    assert 'sensitive' not in str(exc.value.detail)

def test_task_projection_and_truncation():
    rows = [{'id': str(i), 'contactId': 'person', 'body': 'secret', 'title': 'private', 'completed': False} for i in range(3)]
    result = read(bridge_for((200, CONTACT), (200, {'tasks': rows})), limit=2)
    assert len(result['records']) == 2
    assert result['pagination']['complete'] is False
    assert all(set(r) == {'id', 'contactId', 'completed'} for r in result['records'])

def test_cross_contact_child_denied():
    with pytest.raises(HTTPException):
        read(bridge_for((200, CONTACT), (200, {'tasks': [{'id': 'task', 'contactId': 'another'}]})))

def test_conversation_type_and_partial_page():
    row = {'id': 'conv', 'contactId': 'person', 'locationId': 'location', 'lastMessageBody': 'secret', 'lastMessageDate': 123}
    result = read(bridge_for((200, CONTACT), (200, {'conversations': [row], 'total': 2})), resource='conversations', limit=1)
    assert result['pagination']['complete'] is False
    assert 'lastMessageBody' not in result['records'][0]

@pytest.mark.parametrize('next_page', [True, False])
def test_messages_native_status_pagination_and_privacy(next_page):
    conv = {'id': 'conv', 'contactId': 'person', 'locationId': 'location'}
    row = dict(conv, id='msg', conversationId='conv', status='delivered', body='secret', direction='outbound', attachments=['private'])
    b = bridge_for((200, CONTACT), (200, conv), (200, {'messages': {'messages': [row], 'nextPage': next_page, 'lastMessageId': 'msg'}}))
    result = read(b, resource='messages', conversation_id='conv')
    assert result['pagination']['complete'] is not next_page
    assert result['records'][0]['status'] == 'delivered'
    assert 'body' not in result['records'][0] and 'attachments' not in result['records'][0]

def test_conversation_parent_wrong_contact():
    b = bridge_for((200, CONTACT), (200, {'id': 'conv', 'contactId': 'other', 'locationId': 'location'}))
    with pytest.raises(HTTPException): read(b, resource='messages', conversation_id='conv')
    assert len(b.calls) == 2

def test_submissions_filter_window_and_partial():
    data = {'submissions': [{'id': 'submission', 'contactId': 'person', 'formId': 'form', 'others': {'private': 'secret'}}], 'meta': {'currentPage': 1, 'nextPage': 2}}
    b = bridge_for((200, CONTACT), (200, data))
    result = read(b, resource='submissions', start_date='2026-09-01', end_date='2026-09-25')
    assert result['pagination']['complete'] is False
    assert b.calls[-1][1]['params']['q'] == 'person'
    assert 'others' not in result['records'][0]

@pytest.mark.parametrize('args', [{'contact_id': '../escape', 'resource': 'tasks'}, {'contact_id': 'person', 'resource': 'messages'}, {'contact_id': 'person', 'resource': 'tasks', 'limit': 51}, {'contact_id': 'person', 'resource': 'submissions', 'start_date': '2026-01-01', 'end_date': '2026-09-25'}])
def test_bounds(args):
    with pytest.raises(ValidationError): EvidenceQuery(**args)

def test_route_auth_before_provider(monkeypatch):
    import app
    monkeypatch.setattr(app, 'API_KEY', 'real-test-key')
    monkeypatch.setattr(app, 'highlevel_request', lambda *a, **k: pytest.fail('must not contact provider'))
    endpoint = next(r.endpoint for r in app.app.routes if getattr(r, 'path', '') == '/read/journey-evidence')
    with pytest.raises(HTTPException) as exc:
        endpoint(EvidenceQuery(contact_id='person', resource='tasks'), x_api_key='wrong')
    assert exc.value.status_code == 401
