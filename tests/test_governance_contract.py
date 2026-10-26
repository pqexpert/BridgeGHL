import importlib
import json
import os

import pytest
import requests
from fastapi import HTTPException
from starlette.requests import Request

os.environ.setdefault("BRIDGE_API_KEY", "test-key")
os.environ.setdefault("HIGHLEVEL_LOCATION_ID", "test-location")
os.environ.setdefault("HIGHLEVEL_APPROVED_CUSTOM_FIELD_IDS", "cf-approved")
os.environ.setdefault("HIGHLEVEL_APPROVED_CONTACT_TAGS", "approved-tag")

import app as bridge


def test_contact_upsert_is_disabled():
    with pytest.raises(HTTPException) as exc:
        bridge.contact_upsert_disabled()
    assert exc.value.status_code == 410
    assert exc.value.detail["error"] == "contact_upsert_disabled_by_governance"


def test_opportunity_body_is_update_only_shape():
    changes = bridge.OpportunityChanges(
        pipeline_stage_id="stage-1",
        status="open",
        assigned_to="owner-1",
        custom_fields={"cf-approved": "value"},
    )
    body = bridge.build_opportunity_update_body(changes)
    assert body == {
        "pipelineStageId": "stage-1",
        "status": "open",
        "assignedTo": "owner-1",
        "customFields": [{"id": "cf-approved", "fieldValue": "value"}],
    }


def test_unapproved_custom_field_fails_closed(monkeypatch):
    monkeypatch.setattr(bridge, "APPROVED_CUSTOM_FIELD_IDS", {"cf-approved"})
    payload = bridge.OpportunityUpdateRequest(
        opportunity_id="opp-1",
        changes=bridge.OpportunityChanges(custom_fields={"cf-blocked": "x"}),
        reason="test",
    )
    result = bridge.validate_opportunity_request(payload)
    assert result["valid"] is False
    assert result["blocked_custom_field_ids"] == ["cf-blocked"]


def test_unapproved_contact_tag_fails_closed(monkeypatch):
    monkeypatch.setattr(bridge, "APPROVED_CONTACT_TAGS", {"approved-tag"})
    payload = bridge.ContactTagMutationRequest(
        contact_id="contact-1",
        tags_add=["blocked-tag"],
        reason="test",
    )
    result = bridge.validate_contact_tag_request(payload)
    assert result["valid"] is False
    assert result["blocked_tags"] == ["blocked-tag"]


def test_contact_tag_overlap_is_rejected(monkeypatch):
    monkeypatch.setattr(bridge, "APPROVED_CONTACT_TAGS", {"approved-tag"})
    payload = bridge.ContactTagMutationRequest(
        contact_id="contact-1",
        tags_add=["approved-tag"],
        tags_remove=["approved-tag"],
        reason="test",
    )
    result = bridge.validate_contact_tag_request(payload)
    assert result["valid"] is False
    assert any("same tag" in error for error in result["errors"])


def test_verification_requires_requested_state():
    projected = {
        "pipelineStageId": "stage-1",
        "status": "open",
        "assignedTo": "owner-1",
        "customFields": {"cf-approved": "value"},
    }
    changes = bridge.OpportunityChanges(
        pipeline_stage_id="stage-1",
        status="open",
        assigned_to="owner-1",
        custom_fields={"cf-approved": "value"},
    )
    assert bridge.verify_opportunity(projected, changes)["ok"] is True


def test_v3_header_is_explicit():
    assert bridge.highlevel_headers(redacted=True)["Version"] == "v3"


def test_task_action_key_is_exact_and_bounded():
    payload = bridge.TaskCreateRequest(
        contact_id="contact-1",
        action_key="launch17-next-action:v1:location1:form1:submission1:07",
        due_date="2026-10-15T12:00:00Z",
        reason="synthetic acceptance",
    )
    assert bridge.validate_task_create_request(payload)["valid"] is True

    payload.action_key = "launch17-next-action:v1:location1:form1:submission1:18"
    result = bridge.validate_task_create_request(payload)
    assert result["valid"] is False
    assert any("door 01-17" in error for error in result["errors"])


def test_task_payload_is_derived_and_has_no_send_surface():
    payload = bridge.TaskCreateRequest(
        contact_id="contact-1",
        action_key="launch17-next-action:v1:location1:form1:submission1:13",
        due_date="2026-10-15T08:00:00-04:00",
        assigned_to="owner-1",
        reason="synthetic acceptance",
    )
    body = bridge.build_task_create_body(payload)
    assert body["title"] == (
        "Launch17 next action | "
        "launch17-next-action:v1:location1:form1:submission1:13"
    )
    assert body["dueDate"] == "2026-10-15T12:00:00Z"
    assert body["completed"] is False
    assert body["assignedTo"] == "owner-1"
    assert set(body) == {"title", "body", "dueDate", "completed", "assignedTo"}


def test_task_verification_requires_exact_native_state():
    payload = bridge.TaskCreateRequest(
        contact_id="contact-1",
        action_key="launch17-next-action:v1:location1:form1:submission1:17",
        due_date="2026-10-15T12:00:00Z",
        reason="synthetic acceptance",
    )
    projected = {
        "id": "task-1",
        "contactId": "contact-1",
        "assignedTo": None,
        "dueDate": "2026-10-15T12:00:00.000Z",
        "completed": False,
        "title": bridge.task_title(payload.action_key),
    }
    assert bridge.verify_task(projected, payload)["ok"] is True
    projected["completed"] = True
    assert bridge.verify_task(projected, payload)["ok"] is False


def test_task_delete_requires_exact_identity():
    payload = bridge.TaskDeleteRequest(
        contact_id="contact-1",
        task_id="task-1",
        action_key="launch17-next-action:v1:location1:form1:submission1:16",
        reason="cleanup",
    )
    projected = {
        "id": "task-1",
        "contactId": "contact-1",
        "title": bridge.task_title(payload.action_key),
    }
    assert bridge.verify_task_identity(projected, payload)["ok"] is True
    projected["title"] = "Unrelated task"
    assert bridge.verify_task_identity(projected, payload)["ok"] is False


def test_task_actions_require_explicit_opt_in(monkeypatch):
    monkeypatch.setattr(bridge, "TASK_WRITES_ENABLED", False)
    assert {"create_task", "delete_task"}.isdisjoint(bridge.allowed_actions())

    monkeypatch.setattr(bridge, "TASK_WRITES_ENABLED", True)
    assert {"create_task", "delete_task"}.issubset(bridge.allowed_actions())
    assert "send_message" not in bridge.allowed_actions()
    assert "create_contact" not in bridge.allowed_actions()


def task_request():
    return bridge.TaskCreateRequest(
        contact_id="contact-1",
        action_key="launch17-next-action:v1:location1:form1:submission1:17",
        due_date="2026-10-20T12:00:00Z",
        reason="isolated acceptance",
    )


def http_request():
    return Request({"type": "http", "client": ("127.0.0.1", 1234), "headers": []})


def test_task_mutation_timeout_is_audited_as_unresolved(monkeypatch, tmp_path):
    calls = iter([
        (200, {"contact": {"id": "contact-1", "locationId": "test-location"}}),
        (200, {"tasks": []}),
    ])

    def provider(method, url, **kwargs):
        if method == "POST":
            raise requests.Timeout("provider timeout")
        return next(calls)

    audit = tmp_path / "audit.jsonl"
    monkeypatch.setattr(bridge, "API_KEY", "test-key")
    monkeypatch.setattr(bridge, "HIGHLEVEL_LOCATION_ID", "test-location")
    monkeypatch.setattr(bridge, "AUDIT_LOG_PATH", str(audit))
    monkeypatch.setattr(bridge, "execute_guard", lambda action: None)
    monkeypatch.setattr(bridge, "highlevel_request", provider)

    with pytest.raises(HTTPException) as exc:
        bridge.execute_task_create(task_request(), http_request(), x_api_key="test-key")

    assert exc.value.status_code == 503
    assert exc.value.detail["error"] == "provider_mutation_outcome_unknown"
    entries = [json.loads(line) for line in audit.read_text().splitlines()]
    assert [entry["phase"] for entry in entries] == ["intent", "final"]
    assert entries[0]["audit_id"] == entries[1]["audit_id"]
    assert entries[1]["effect"] == "UNRESOLVED"
    assert entries[1]["verified"] is False



def test_task_create_preserves_task_audit_identity_for_structured_unresolved(monkeypatch, tmp_path):
    calls = iter([
        (200, {"contact": {"id": "contact-1", "locationId": "test-location"}}),
        (200, {"tasks": []}),
    ])

    def provider(method, url, **kwargs):
        if method == "POST":
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "provider_mutation_outcome_unknown",
                    "audit_id": "provider-interaction-1",
                    "provider_status": 201,
                    "audit_persistence": "failed_after_provider_response",
                },
            )
        return next(calls)

    audit = tmp_path / "audit.jsonl"
    monkeypatch.setattr(bridge, "API_KEY", "test-key")
    monkeypatch.setattr(bridge, "HIGHLEVEL_LOCATION_ID", "test-location")
    monkeypatch.setattr(bridge, "AUDIT_LOG_PATH", str(audit))
    monkeypatch.setattr(bridge, "execute_guard", lambda action: None)
    monkeypatch.setattr(bridge, "highlevel_request", provider)

    with pytest.raises(HTTPException) as exc:
        bridge.execute_task_create(task_request(), http_request(), x_api_key="test-key")

    entries = [json.loads(line) for line in audit.read_text().splitlines()]
    assert exc.value.status_code == 503
    assert exc.value.detail["audit_id"] == entries[0]["audit_id"] == entries[1]["audit_id"]
    assert exc.value.detail["provider_interaction_audit_id"] == "provider-interaction-1"
    assert exc.value.detail["provider_status"] == 201
    assert entries[1]["effect"] == "UNRESOLVED"
    assert entries[1]["provider_interaction"]["audit_id"] == "provider-interaction-1"


def test_task_delete_preserves_task_audit_identity_for_structured_unresolved(monkeypatch, tmp_path):
    payload = bridge.TaskDeleteRequest(
        contact_id="contact-1",
        task_id="task-1",
        action_key="launch17-next-action:v1:location1:form1:submission1:17",
        reason="isolated cleanup",
    )
    calls = iter([
        (200, {"contact": {"id": "contact-1", "locationId": "test-location"}}),
        (200, {"task": {
            "id": "task-1",
            "contactId": "contact-1",
            "title": bridge.task_title(payload.action_key),
        }}),
    ])

    def provider(method, url, **kwargs):
        if method == "DELETE":
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "provider_mutation_outcome_unknown",
                    "audit_id": "provider-interaction-2",
                    "provider_status": 204,
                    "audit_persistence": "failed_after_provider_response",
                },
            )
        return next(calls)

    audit = tmp_path / "audit.jsonl"
    monkeypatch.setattr(bridge, "API_KEY", "test-key")
    monkeypatch.setattr(bridge, "HIGHLEVEL_LOCATION_ID", "test-location")
    monkeypatch.setattr(bridge, "AUDIT_LOG_PATH", str(audit))
    monkeypatch.setattr(bridge, "execute_guard", lambda action: None)
    monkeypatch.setattr(bridge, "highlevel_request", provider)

    with pytest.raises(HTTPException) as exc:
        bridge.execute_task_delete(payload, http_request(), x_api_key="test-key")

    entries = [json.loads(line) for line in audit.read_text().splitlines()]
    assert exc.value.status_code == 503
    assert exc.value.detail["audit_id"] == entries[0]["audit_id"] == entries[1]["audit_id"]
    assert exc.value.detail["provider_interaction_audit_id"] == "provider-interaction-2"
    assert exc.value.detail["provider_status"] == 204
    assert entries[1]["effect"] == "UNRESOLVED"
    assert entries[1]["provider_interaction"]["audit_id"] == "provider-interaction-2"


def test_task_success_audit_persists_native_readback(monkeypatch, tmp_path):
    payload = task_request()
    calls = iter([
        (200, {"contact": {"id": "contact-1", "locationId": "test-location"}}),
        (200, {"tasks": []}),
        (200, {"task": {"id": "task-1"}}),
        (200, {"task": {
            "id": "task-1",
            "contactId": "contact-1",
            "assignedTo": None,
            "dueDate": payload.due_date,
            "completed": False,
            "title": bridge.task_title(payload.action_key),
        }}),
    ])
    audit = tmp_path / "audit.jsonl"
    monkeypatch.setattr(bridge, "API_KEY", "test-key")
    monkeypatch.setattr(bridge, "HIGHLEVEL_LOCATION_ID", "test-location")
    monkeypatch.setattr(bridge, "AUDIT_LOG_PATH", str(audit))
    monkeypatch.setattr(bridge, "execute_guard", lambda action: None)
    monkeypatch.setattr(bridge, "highlevel_request", lambda *args, **kwargs: next(calls))

    result = bridge.execute_task_create(payload, http_request(), x_api_key="test-key")

    assert result.effect == "CREATED"
    assert result.verified is True
    entries = [json.loads(line) for line in audit.read_text().splitlines()]
    final = entries[-1]
    assert final["readback"]["id"] == "task-1"
    assert final["verification"]["ok"] is True
