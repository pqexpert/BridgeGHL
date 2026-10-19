import io
import json
from urllib.error import HTTPError

import mcp_server


def test_task_error_preserves_safe_reconciliation_fields(monkeypatch):
    body = json.dumps({
        "detail": {
            "error": "provider_mutation_outcome_unknown",
            "audit_id": "audit-1",
            "task_id": "task-1",
        }
    }).encode()

    def fail(*args, **kwargs):
        raise HTTPError("http://bridge", 503, "unavailable", {}, io.BytesIO(body))

    monkeypatch.setenv("BRIDGE_API_KEY", "test-key")
    monkeypatch.setattr(mcp_server, "urlopen", fail)
    result = mcp_server._call("/execute/task/create", {})

    assert result == {
        "ok": False,
        "bridge_http_status": 503,
        "error": "provider_mutation_outcome_unknown",
        "audit_id": "audit-1",
        "task_id": "task-1",
    }


def test_execute_task_uses_operation_level_timeout(monkeypatch):
    captured = {}

    def call(path, payload, timeout=25):
        captured.update(path=path, payload=payload, timeout=timeout)
        return {"ok": True}

    monkeypatch.setattr(mcp_server, "_call", call)
    result = mcp_server.execute_task_create(
        contact_id="contact-1",
        action_key="launch17-next-action:v1:location1:form1:submission1:17",
        due_date="2026-10-20T12:00:00Z",
        reason="acceptance",
    )

    assert result == {"ok": True}
    assert captured["path"] == "/execute/task/create"
    assert captured["timeout"] == 150


def test_health_projects_plugin_contract(monkeypatch):
    monkeypatch.setattr(mcp_server, "_call", lambda path: {"ok": True, "state": "HEALTHY"})
    result = mcp_server.bridge_health()

    assert result["plugin_contract"]["version"] == "rsc-command-center-plugin/v1"
    assert result["plugin_contract"]["actor"] == "RSC Command Center"
    assert set(result["plugin_contract"]["task_tools_advertised"]) == mcp_server.TASK_TOOL_NAMES
