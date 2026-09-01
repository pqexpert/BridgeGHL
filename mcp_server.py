"""Tool-only MCP adapter for the governed BridgeGHL write boundary.

This process never holds a HighLevel credential. It authenticates to the local
BridgeGHL service with BRIDGE_API_KEY and must be exposed only behind the
organization's OAuth 2.1-capable MCP edge.
"""
from __future__ import annotations

import os
from typing import Any, Literal

import requests
from mcp.server.fastmcp import FastMCP

BRIDGE_BASE_URL = os.getenv("BRIDGE_BASE_URL", "http://bridgeghl:3000").rstrip("/")
BRIDGE_API_KEY = os.getenv("BRIDGE_API_KEY", "")
BRIDGE_TIMEOUT_SECONDS = float(os.getenv("BRIDGE_TIMEOUT_SECONDS", "30"))

mcp = FastMCP(
    "rsc-highlevel-write-bridge",
    instructions=(
        "Use bridge_health before live work. Every write must first use its matching "
        "dry-run tool. Execute only the existing-opportunity and allowlisted contact-tag "
        "envelope. Treat verified=false or a non-HEALTHY bridge as unresolved."
    ),
)


def _bridge(method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    if not BRIDGE_API_KEY:
        raise RuntimeError("Bridge caller credential is not configured")
    response = requests.request(
        method,
        f"{BRIDGE_BASE_URL}{path}",
        headers={"X-API-Key": BRIDGE_API_KEY, "Accept": "application/json"},
        json=payload,
        timeout=BRIDGE_TIMEOUT_SECONDS,
    )
    try:
        data = response.json()
    except ValueError:
        data = {"error": "Bridge returned a non-JSON response"}
    if not response.ok:
        detail = data.get("detail", data) if isinstance(data, dict) else {"error": "Bridge request failed"}
        raise RuntimeError(f"Bridge rejected request (HTTP {response.status_code}): {detail}")
    return data


@mcp.tool(
    annotations={"readOnlyHint": True, "destructiveHint": False, "openWorldHint": True}
)
def bridge_health() -> dict[str, Any]:
    """Use this before CRM work to verify BridgeGHL configuration, readback, and allowed actions."""
    return _bridge("GET", "/health")


@mcp.tool(
    annotations={"readOnlyHint": True, "destructiveHint": False, "openWorldHint": True}
)
def dry_run_opportunity_update(
    opportunity_id: str,
    reason: str,
    pipeline_stage_id: str | None = None,
    status: Literal["open", "won", "lost", "abandoned"] | None = None,
    assigned_to: str | None = None,
    custom_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Use this to validate an existing-opportunity mutation without changing HighLevel."""
    return _bridge(
        "POST",
        "/dry-run/opportunity/update",
        {
            "opportunity_id": opportunity_id,
            "reason": reason,
            "changes": {
                "pipeline_stage_id": pipeline_stage_id,
                "status": status,
                "assigned_to": assigned_to,
                "custom_fields": custom_fields or {},
            },
        },
    )


@mcp.tool(
    annotations={"readOnlyHint": False, "destructiveHint": False, "openWorldHint": True}
)
def execute_opportunity_update(
    opportunity_id: str,
    reason: str,
    pipeline_stage_id: str | None = None,
    status: Literal["open", "won", "lost", "abandoned"] | None = None,
    assigned_to: str | None = None,
    custom_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Use this after a matching dry run to execute and verify a bounded existing-opportunity update."""
    return _bridge(
        "POST",
        "/execute/opportunity/update",
        {
            "opportunity_id": opportunity_id,
            "reason": reason,
            "changes": {
                "pipeline_stage_id": pipeline_stage_id,
                "status": status,
                "assigned_to": assigned_to,
                "custom_fields": custom_fields or {},
            },
        },
    )


@mcp.tool(
    annotations={"readOnlyHint": True, "destructiveHint": False, "openWorldHint": True}
)
def dry_run_contact_tags(
    contact_id: str,
    reason: str,
    tags_add: list[str] | None = None,
    tags_remove: list[str] | None = None,
) -> dict[str, Any]:
    """Use this to validate allowlisted contact-tag normalization without changing HighLevel."""
    return _bridge(
        "POST",
        "/dry-run/contact/tags",
        {
            "contact_id": contact_id,
            "reason": reason,
            "tags_add": tags_add or [],
            "tags_remove": tags_remove or [],
        },
    )


@mcp.tool(
    annotations={"readOnlyHint": False, "destructiveHint": False, "openWorldHint": True}
)
def execute_contact_tags(
    contact_id: str,
    reason: str,
    tags_add: list[str] | None = None,
    tags_remove: list[str] | None = None,
) -> dict[str, Any]:
    """Use this after a matching dry run to normalize allowlisted contact tags and verify readback."""
    return _bridge(
        "POST",
        "/execute/contact/tags",
        {
            "contact_id": contact_id,
            "reason": reason,
            "tags_add": tags_add or [],
            "tags_remove": tags_remove or [],
        },
    )


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
