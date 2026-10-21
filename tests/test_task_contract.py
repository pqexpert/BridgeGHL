import asyncio
import copy

import app
import mcp_server
from task_contract import TASK_TOOL_ROUTES, verify_task_contract


def _tools_document():
    tools = asyncio.run(mcp_server.SERVER.list_tools())
    return {"tools": [tool.model_dump(by_alias=True) for tool in tools]}


def _tool(document, name):
    return next(item for item in document["tools"] if item["name"] == name)


def test_task_tools_match_backend_routes_and_schemas():
    assert verify_task_contract(_tools_document(), app.app.openapi()) == []


def test_route_drift_is_reported():
    document = copy.deepcopy(app.app.openapi())
    document["paths"].pop(TASK_TOOL_ROUTES["execute_task_create"])

    assert verify_task_contract(_tools_document(), document) == [
        "missing backend route: POST /execute/task/create"
    ]


def test_schema_drift_is_reported():
    document = _tools_document()
    _tool(document, "dry_run_task_delete")["inputSchema"]["properties"].pop("task_id")

    assert verify_task_contract(document, app.app.openapi()) == [
        "dry_run_task_delete missing fields: task_id"
    ]


def test_nullability_drift_is_reported():
    document = _tools_document()
    _tool(document, "dry_run_task_create")["inputSchema"]["properties"][
        "assigned_to"
    ] = {"type": "string"}

    assert "dry_run_task_create.assigned_to schema differs" in verify_task_contract(
        document, app.app.openapi()
    )


def test_validation_constraint_drift_is_reported():
    document = _tools_document()
    _tool(document, "execute_task_delete")["inputSchema"]["properties"]["reason"][
        "pattern"
    ] = "^unsafe-drift$"

    assert "execute_task_delete.reason schema differs" in verify_task_contract(
        document, app.app.openapi()
    )


def test_request_level_constraint_drift_is_reported():
    document = _tools_document()
    _tool(document, "dry_run_task_delete")["inputSchema"][
        "additionalProperties"
    ] = False

    assert "dry_run_task_delete request schema differs" in verify_task_contract(
        document, app.app.openapi()
    )


def test_ref_sibling_constraint_drift_is_reported():
    document = copy.deepcopy(app.app.openapi())
    route = TASK_TOOL_ROUTES["execute_task_create"]
    schema = document["paths"][route]["post"]["requestBody"]["content"][
        "application/json"
    ]["schema"]
    assert "$ref" in schema
    schema["maxProperties"] = 1

    assert "execute_task_create request schema differs" in verify_task_contract(
        _tools_document(), document
    )


def test_task_annotations_expose_safe_replay_and_delete_semantics():
    tools = {tool["name"]: tool for tool in _tools_document()["tools"]}
    assert tools["dry_run_task_create"]["annotations"]["idempotentHint"] is True
    assert tools["dry_run_task_delete"]["annotations"]["idempotentHint"] is True
    assert tools["execute_task_delete"]["annotations"]["idempotentHint"] is True
    assert (
        tools["execute_task_create"]["annotations"].get("idempotentHint") is not True
    )
    assert tools["execute_task_create"]["annotations"]["destructiveHint"] is False
    assert tools["execute_task_delete"]["annotations"]["destructiveHint"] is True
