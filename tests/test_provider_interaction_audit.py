import json
from types import SimpleNamespace

import pytest
import requests

import app
import provider_budget


@pytest.fixture
def transport(monkeypatch, tmp_path):
    path = tmp_path/'audit.jsonl'
    monkeypatch.setattr(app, 'HIGHLEVEL_PIT', 'SECRET_TOKEN')
    monkeypatch.setattr(app, 'HIGHLEVEL_LOCATION_ID', 'location')
    monkeypatch.setattr(app, 'AUDIT_LOG_PATH', str(path))
    monkeypatch.setattr(provider_budget, 'reserve', lambda: False)
    return path


def test_metadata_only_records_actual_provider_rejection(transport, monkeypatch):
    monkeypatch.setattr(app.requests, 'request', lambda *a, **kw: SimpleNamespace(
        status_code=403, json=lambda: {'private': 'RESPONSE_SECRET'}))
    result = app._highlevel_request('POST', 'https://provider.invalid/contacts/PRIVATE_ID/notes?email=PRIVATE_EMAIL',
        body={'body': 'MESSAGE_SECRET'}, params={'token': 'QUERY_SECRET'}, retry_reads=False)
    assert result[0] == 403
    text = transport.read_text()
    assert not any(s in text for s in ('SECRET', 'PRIVATE', 'provider.invalid', 'email='))
    intent, response = map(json.loads, text.splitlines())
    assert intent['request_id'] == response['request_id']
    assert intent['result'] == 'intent' and response['result'] == 'response'
    assert response['provider_status'] == 403 and response['resource'] == 'contacts'
    assert response['method'] == 'POST' and response['observed_at']


def test_transport_failure_is_unknown_and_never_replayed(transport, monkeypatch):
    calls = []
    def fail(*a, **kw):
        calls.append(1)
        raise requests.Timeout('PRIVATE_URL?secret=SECRET')
    monkeypatch.setattr(app.requests, 'request', fail)
    with pytest.raises(requests.Timeout):
        app._highlevel_request('PUT', 'https://provider.invalid/opportunities/id')
    events = [json.loads(s) for s in transport.read_text().splitlines()]
    assert len(calls) == 1 and events[-1]['result'] == 'transport_unknown'
    assert 'SECRET' not in transport.read_text()


def test_missing_pre_request_audit_prevents_effect(transport, monkeypatch):
    calls = []
    monkeypatch.setattr(app.requests, 'request', lambda *a, **kw: calls.append(1))
    def fail(_):
        raise OSError('audit unavailable')
    monkeypatch.setattr(app, 'append_audit_log', fail)
    with pytest.raises(OSError):
        app._highlevel_request('POST', 'https://provider.invalid/contacts/id/notes')
    assert calls == []


def test_post_response_audit_failure_returns_correlated_unresolved(transport, monkeypatch):
    calls = []
    original_append = app.append_audit_log

    def fail_response(entry):
        if entry.get('result') == 'response':
            raise OSError('audit unavailable after provider response')
        original_append(entry)

    monkeypatch.setattr(app, 'append_audit_log', fail_response)
    monkeypatch.setattr(app.requests, 'request', lambda *a, **kw: (
        calls.append(1) or SimpleNamespace(status_code=200, json=lambda: {'id': 'created'})
    ))

    with pytest.raises(app.HTTPException) as excinfo:
        app._highlevel_request(
            'POST',
            'https://provider.invalid/contacts/id/notes',
            body={'body': 'MESSAGE_SECRET'},
            retry_reads=False,
        )

    assert calls == [1]
    assert excinfo.value.status_code == 503
    assert excinfo.value.detail['error'] == 'provider_mutation_outcome_unknown'
    assert excinfo.value.detail['audit_id']
    assert excinfo.value.detail['provider_status'] == 200
    assert excinfo.value.detail['audit_persistence'] == 'failed_after_provider_response'
    events = [json.loads(s) for s in transport.read_text().splitlines()]
    assert [event['result'] for event in events] == ['intent']
    assert events[0]['request_id'] == excinfo.value.detail['audit_id']


def test_rate_limit_observation_survives_response_audit_failure(transport, monkeypatch):
    observed = []
    original_append = app.append_audit_log

    def fail_response(entry):
        if entry.get('result') == 'response':
            raise OSError('audit unavailable after provider response')
        original_append(entry)

    monkeypatch.setattr(provider_budget, 'reserve', lambda: True)
    monkeypatch.setattr(
        provider_budget, 'observe',
        lambda status, headers: observed.append((status, dict(headers))),
    )
    monkeypatch.setattr(app, 'append_audit_log', fail_response)
    monkeypatch.setattr(app.requests, 'request', lambda *a, **kw: SimpleNamespace(
        status_code=429,
        headers={'Retry-After': '60', 'X-RateLimit-Daily-Remaining': '0'},
        json=lambda: {},
    ))

    with pytest.raises(app.HTTPException):
        app._highlevel_request(
            'POST',
            'https://provider.invalid/contacts/id/notes',
            body={'body': 'MESSAGE_SECRET'},
            retry_reads=False,
        )

    assert observed == [(429, {
        'Retry-After': '60',
        'X-RateLimit-Daily-Remaining': '0',
    })]


def test_existing_unscoped_read_retry_has_separate_attempt_evidence(transport, monkeypatch):
    responses = iter((503, 200))
    monkeypatch.setattr(app.time, 'sleep', lambda _: None)
    monkeypatch.setattr(app.requests, 'request', lambda *a, **kw: SimpleNamespace(
        status_code=next(responses), json=lambda: {}))
    app._highlevel_request('GET', 'https://provider.invalid/contacts/id')
    events = [json.loads(s) for s in transport.read_text().splitlines()]
    assert [(e['attempt'], e['result']) for e in events] == [
        (1, 'intent'), (1, 'response'), (2, 'intent'), (2, 'response')]
    assert len({e['request_id'] for e in events}) == 1
