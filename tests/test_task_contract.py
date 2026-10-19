import asyncio
import copy

import app
import mcp_server
from task_contract import TASK_TOOL_ROUTES, verify_task_contract


def _tools_document():
    tools = asyncio.run(mcp_server.SERVER.list_tools())
    return {"tools": [tool.model_dump(by_alias=True) for tool in tools]}


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
    tool = next(item for item in document["tools"] if item["name"] == "dry_run_task_delete")
    tool["inputSchema"]["properties"].pop("task_id")

    assert verify_task_contract(document, app.app.openapi()) == [
        "dry_run_task_delete missing fields: task_id"
    ]


def test_task_annotations_expose_replay_and_delete_semantics():
    tools = {tool["name"]: tool for tool in _tools_document()["tools"]}
    for name in TASK_TOOL_ROUTES:
        assert tools[name]["annotations"]["idempotentHint"] is True
    assert tools["execute_task_create"]["annotations"]["destructiveHint"] is False
    assert tools["execute_task_delete"]["annotations"]["destructiveHint"] is True
