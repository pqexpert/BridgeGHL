"""Loopback-only MCP adapter to the existing governed BridgeGHL service.

This process does not hold a second HighLevel token or implement CRM writes.
The BridgeGHL service remains the sole writer and audit boundary.
"""

import json
import os
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from mcp.server import MCPServer
from mcp.types import ToolAnnotations


SERVER = MCPServer("BridgeGHL governed CRM")
BRIDGE = "http://127.0.0.1:8000"


def _call(path: str, payload: dict | None = None) -> dict:
    key = os.environ.get("BRIDGE_API_KEY", "")
    if not key:
        return {"ok": False, "error": "bridge_caller_credential_missing"}
    request = Request(
        BRIDGE + path,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"x-api-key": key, "Content-Type": "application/json"},
        method="POST" if payload is not None else "GET",
    )
    try:
        with urlopen(request, timeout=25) as response:
            return json.load(response)
    except HTTPError as error:
        # Do not forward provider error bodies, which may contain private data.
        return {"ok": False, "bridge_http_status": error.code}
    except (URLError, TimeoutError):
        return {"ok": False, "error": "bridge_unavailable"}


@SERVER.tool(annotations=ToolAnnotations(read_only_hint=True, destructive_hint=False, open_world_hint=True))
def bridge_health() -> dict:
    """Read the live BridgeGHL health and HighLevel readback state."""
    return _call("/health")


@SERVER.tool(annotations=ToolAnnotations(read_only_hint=True, destructive_hint=False, open_world_hint=True))
def dry_run_opportunity_update(
    opportunity_id: str,
    reason: str,
    pipeline_stage_id: str | None = None,
    status: str | None = None,
    assigned_to: str | None = None,
) -> dict:
    """Validate a bounded opportunity change without mutating HighLevel."""
    return _call("/dry-run/opportunity/update", {
        "opportunity_id": opportunity_id,
        "reason": reason,
        "changes": {"pipeline_stage_id": pipeline_stage_id, "status": status, "assigned_to": assigned_to},
    })


@SERVER.tool()
def execute_opportunity_update(
    opportunity_id: str,
    reason: str,
    pipeline_stage_id: str | None = None,
    status: str | None = None,
    assigned_to: str | None = None,
) -> dict:
    """Execute an authorized opportunity change with bridge audit and readback."""
    return _call("/execute/opportunity/update", {
        "opportunity_id": opportunity_id,
        "reason": reason,
        "changes": {"pipeline_stage_id": pipeline_stage_id, "status": status, "assigned_to": assigned_to},
    })


@SERVER.tool(annotations=ToolAnnotations(read_only_hint=True, destructive_hint=False, open_world_hint=True))
def dry_run_contact_tags(contact_id: str, reason: str, tags_add: list[str], tags_remove: list[str]) -> dict:
    """Validate allowlisted contact tag changes without mutation."""
    return _call("/dry-run/contact/tags", {
        "contact_id": contact_id, "reason": reason, "tags_add": tags_add, "tags_remove": tags_remove,
    })


@SERVER.tool()
def execute_contact_tags(contact_id: str, reason: str, tags_add: list[str], tags_remove: list[str]) -> dict:
    """Execute authorized tag normalization with bridge audit and readback."""
    return _call("/execute/contact/tags", {
        "contact_id": contact_id, "reason": reason, "tags_add": tags_add, "tags_remove": tags_remove,
    })


if __name__ == "__main__":
    # Only the VPS loopback can reach this service; connect externally through
    # an authenticated tunnel, after validating its access controls.
    SERVER.run(transport="streamable-http", host="127.0.0.1", port=8001,
               stateless_http=True, json_response=True)
