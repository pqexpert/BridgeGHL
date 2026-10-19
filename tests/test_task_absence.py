import pytest
import app as bridge
from starlette.requests import Request

KEY = "launch17-next-action:v1:location1:form1:event1:17"


def test_complete_empty_list_proves_absence():
    assert bridge.task_absent_from_complete_list({"tasks": []}, "contact1", "task1", KEY)


@pytest.mark.parametrize("data", [
    {}, {"error": "bad request"}, {"tasks": None}, {"tasks": [None]},
    {"tasks": [], "nextPage": 2}, {"tasks": [], "meta": {"hasMore": True}},
    {"tasks": [{"id": "other", "contactId": "foreign"}]},
    {"tasks": [{"contactId": "contact1"}]},
    {"tasks": [{"id": "task1", "contactId": "contact1"}]},
    {"tasks": [{"id": "other", "contactId": "contact1", "title": bridge.task_title(KEY)}]},
])
def test_ambiguous_or_present_task_is_not_absence(data):
    assert not bridge.task_absent_from_complete_list(data, "contact1", "task1", KEY)


def test_other_same_contact_tasks_do_not_block_verified_absence():
    assert bridge.task_absent_from_complete_list(
        {"tasks": [{"id": "unrelated", "contactId": "contact1", "title": "Other"}]},
        "contact1", "task1", KEY
    )


@pytest.mark.parametrize("list_response,expected", [
    ((200, {"tasks": []}), True),
    ((500, {"tasks": []}), False),
    ((200, {"tasks": [{"id": "task1", "contactId": "contact1"}]}), False),
])
def test_delete_400_requires_successful_native_absence_readback(monkeypatch, list_response, expected):
    responses = iter([
        (200, {"contact": {"id": "contact1", "locationId": "location1"}}),
        (200, {"task": {"id": "task1", "contactId": "contact1", "title": bridge.task_title(KEY)}}),
        (200, {}), (400, {"message": "provider error"}), list_response,
    ])
    methods, audit = [], []
    def provider(method, *args, **kwargs):
        methods.append(method)
        return next(responses)
    monkeypatch.setattr(bridge, "API_KEY", "test-key")
    monkeypatch.setattr(bridge, "HIGHLEVEL_LOCATION_ID", "location1")
    monkeypatch.setattr(bridge, "execute_guard", lambda action: None)
    monkeypatch.setattr(bridge, "highlevel_request", provider)
    monkeypatch.setattr(bridge, "append_audit_log", audit.append)
    result = bridge.execute_task_delete(
        bridge.TaskDeleteRequest(contact_id="contact1", task_id="task1", action_key=KEY, reason="Verify isolated canary cleanup"),
        Request({"type": "http", "headers": [], "client": ("127.0.0.1", 1234)}), x_api_key="test-key",
    )
    assert result.verified is expected
    assert methods == ["GET", "GET", "DELETE", "GET", "GET"]
    assert [entry["phase"] for entry in audit] == ["intent", "final"]
    assert audit[-1]["verified"] is expected
    assert audit[-1]["effect"] == ("DELETED" if expected else "UNRESOLVED")
