"""Loopback-only MCP adapter to the existing governed BridgeGHL service.

This process does not hold a second HighLevel token or implement CRM writes.
The BridgeGHL service remains the sole writer and audit boundary.
"""

import json
import os
from typing import Annotated, Literal
from pydantic import Field
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
        # Only stable bridge-owned evidence errors are safe to surface.
        result = {"ok": False, "bridge_http_status": error.code}
        if path == "/read/journey-evidence":
            try:
                code = json.load(error).get("detail", {}).get("error")
                if code in {"provider_binding_missing", "provider_unavailable", "provider_scope_or_auth_rejected",
                            "provider_read_failed", "provider_resource_type_mismatch", "contact_identity_mismatch",
                            "location_mismatch", "provider_pagination_mismatch", "conversation_identity_mismatch",
                            "resource_contact_mismatch", "resource_location_mismatch", "resource_conversation_mismatch"}:
                    result["error"] = code
            except (ValueError, AttributeError, TypeError):
                pass
        return result
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
    custom_fields: dict[str, str] | None = None,
) -> dict:
    """Validate a bounded opportunity change without mutating HighLevel."""
    return _call("/dry-run/opportunity/update", {
        "opportunity_id": opportunity_id,
        "reason": reason,
        "changes": {"pipeline_stage_id": pipeline_stage_id, "status": status, "assigned_to": assigned_to, "custom_fields": custom_fields or {}},
    })


@SERVER.tool()
def execute_opportunity_update(
    opportunity_id: str,
    reason: str,
    pipeline_stage_id: str | None = None,
    status: str | None = None,
    assigned_to: str | None = None,
    custom_fields: dict[str, str] | None = None,
) -> dict:
    """Execute an authorized opportunity change with bridge audit and readback."""
    return _call("/execute/opportunity/update", {
        "opportunity_id": opportunity_id,
        "reason": reason,
        "changes": {"pipeline_stage_id": pipeline_stage_id, "status": status, "assigned_to": assigned_to, "custom_fields": custom_fields or {}},
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


@SERVER.tool(annotations=ToolAnnotations(read_only_hint=True, destructive_hint=False, open_world_hint=True))
def read_journey_evidence(contact_id: str, resource: Literal["conversations", "messages", "tasks", "submissions", "email"],
                          conversation_id: str | None = None,
                          limit: Annotated[int, Field(ge=1, le=50)] = 20,
                          cursor: str | None = None, page: Annotated[int, Field(ge=1, le=100)] = 1,
                          start_date: str | None = None, end_date: str | None = None,
                          email_id: str | None = None) -> dict:
    """Read contact-scoped conversations, messages, tasks, submissions or one email.

    Uses the existing server-side identity. No bodies, subjects or form answers.
    Check pagination completeness; a failed/mismatched read is not zero results.
    Limit must be 1–50; page 1–100. Messages/email require conversation_id.
    Email requires email_id (individual email ID, not its thread ID).
    Submissions end_date is exclusive; default tomorrow UTC includes today,
    with at most a 31-day window. Email status is native provider status.
    Discover individual email IDs from messages.email_message_ids when present.
    This read cannot prove workflow enrollment, inbox receipt or a complete journey.
    """
    return _call("/read/journey-evidence", {
        "contact_id": contact_id, "resource": resource, "conversation_id": conversation_id,
        "limit": limit, "cursor": cursor, "page": page,
        "start_date": start_date, "end_date": end_date, "email_id": email_id,
    })


if __name__ == "__main__":
    # Only the VPS loopback can reach this service; connect externally through
    # an authenticated tunnel, after validating its access controls.
    SERVER.run(transport="streamable-http", host="127.0.0.1", port=8001,
               stateless_http=True, json_response=True)
