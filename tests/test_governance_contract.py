import importlib
import os

import pytest
from fastapi import HTTPException

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
