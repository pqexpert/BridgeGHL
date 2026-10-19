"""Verify that MCP task tools expose the governed backend task contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


TASK_TOOL_ROUTES = {
    "dry_run_task_create": "/dry-run/task/create",
    "execute_task_create": "/execute/task/create",
    "dry_run_task_delete": "/dry-run/task/delete",
    "execute_task_delete": "/execute/task/delete",
}


def _resolve(document: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
    while "$ref" in schema:
        target: Any = document
        for part in schema["$ref"].removeprefix("#/").split("/"):
            target = target[part.replace("~1", "/").replace("~0", "~")]
        schema = target
    return schema


def _request_schema(document: dict[str, Any], route: str) -> dict[str, Any]:
    operation = document.get("paths", {}).get(route, {}).get("post", {})
    schema = operation.get("requestBody", {}).get("content", {}).get(
        "application/json", {}
    ).get("schema", {})
    return _resolve(document, schema) if schema else {}


def _json_type(document: dict[str, Any], schema: dict[str, Any]) -> str | None:
    schema = _resolve(document, schema)
    if "anyOf" in schema:
        choices = [
            _resolve(document, item)
            for item in schema["anyOf"]
            if _resolve(document, item).get("type") != "null"
        ]
        return choices[0].get("type") if len(choices) == 1 else None
    return schema.get("type")


def verify_task_contract(
    tools_document: dict[str, Any], openapi_document: dict[str, Any]
) -> list[str]:
    """Return deterministic contract errors; an empty result is a match."""
    tools = tools_document.get("result", {}).get("tools", tools_document.get("tools", []))
    advertised = {tool.get("name"): tool for tool in tools if isinstance(tool, dict)}
    errors: list[str] = []

    for name, route in TASK_TOOL_ROUTES.items():
        tool = advertised.get(name)
        if tool is None:
            errors.append(f"missing MCP tool: {name}")
            continue
        backend = _request_schema(openapi_document, route)
        if not backend:
            errors.append(f"missing backend route: POST {route}")
            continue

        mcp = tool.get("inputSchema", {})
        mcp_properties = mcp.get("properties", {})
        backend_properties = backend.get("properties", {})
        missing = sorted(set(backend_properties) - set(mcp_properties))
        extra = sorted(set(mcp_properties) - set(backend_properties))
        if missing:
            errors.append(f"{name} missing fields: {', '.join(missing)}")
        if extra:
            errors.append(f"{name} undocumented fields: {', '.join(extra)}")
        if set(mcp.get("required", [])) != set(backend.get("required", [])):
            errors.append(f"{name} required fields differ")
        for field in sorted(set(mcp_properties) & set(backend_properties)):
            if _json_type(openapi_document, backend_properties[field]) != _json_type(
                {}, mcp_properties[field]
            ):
                errors.append(f"{name}.{field} type differs")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("tools", type=Path)
    parser.add_argument("openapi", type=Path)
    args = parser.parse_args()
    errors = verify_task_contract(
        json.loads(args.tools.read_text()), json.loads(args.openapi.read_text())
    )
    if errors:
        raise SystemExit("task contract mismatch:\n- " + "\n- ".join(errors))
    print("task_contract_verified=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
