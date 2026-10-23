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

from task_contract import TASK_TOOL_ROUTES


SERVER = MCPServer("BridgeGHL governed CRM")
BRIDGE = "http://127.0.0.1:8000"
MCP_CONTRACT_VERSION = "rsc-command-center-plugin/v1"
TASK_TOOL_NAMES = set(TASK_TOOL_ROUTES)
SAFE_TASK_ERRORS = {
    "action_not_allowed",
    "bridge_not_ready_for_live_execute",
    "contact_location_validation_failed",
    "pre_write_task_read_failed",
    "duplicate_action_key_conflict",
    "exact_replay_missing_task_id",
    "existing_action_key_state_conflict",
    "task_identity_validation_failed",
    "provider_mutation_outcome_unknown",
}


def _call(path: str, payload: dict | None = None, timeout: float = 25) -> dict:
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
        with urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except HTTPError as error:
        # Only stable bridge-owned evidence errors are safe to surface.
        result = {"ok": False, "bridge_http_status": error.code}
        try:
            detail = json.load(error).get("detail", {})
            if path == "/read/journey-evidence":
                code = detail.get("error") if isinstance(detail, dict) else None
                if code in {"provider_binding_missing", "provider_unavailable", "provider_scope_or_auth_rejected",
                            "provider_read_failed", "provider_resource_type_mismatch", "contact_identity_mismatch",
                            "location_mismatch", "provider_pagination_mismatch", "conversation_identity_mismatch",
                            "resource_contact_mismatch", "resource_location_mismatch", "resource_conversation_mismatch"}:
                    result["error"] = code
            elif path.startswith(("/dry-run/task/", "/execute/task/")) and isinstance(detail, dict):
                code = detail.get("error")
                if code in SAFE_TASK_ERRORS:
                    result["error"] = code
                    for name in ("audit_id", "task_id", "status", "match_count", "bridge_state"):
                        value = detail.get(name)
                        if isinstance(value, (str, int, bool)):
                            result[name] = value
        except (ValueError, AttributeError, TypeError):
            pass
        return result
    except (URLError, TimeoutError):
        return {"ok": False, "error": "bridge_unavailable"}


@SERVER.tool(annotations=ToolAnnotations(read_only_hint=True, destructive_hint=False, open_world_hint=True))
def bridge_health() -> dict:
    """Read the live BridgeGHL health and HighLevel readback state."""
    result = _call("/health")
    if isinstance(result, dict):
        result["plugin_contract"] = {
            "version": MCP_CONTRACT_VERSION,
            "actor": "RSC Command Center",
            "interface_model": "GPTs are bounded plugins/interfaces, not Actors or authority sources",
            "task_tools_advertised": sorted(TASK_TOOL_NAMES),
            "completion_signal": "native readback plus audit receipt",
        }
    return result


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


@SERVER.tool(annotations=ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=True))
def dry_run_task_create(
    contact_id: str,
    action_key: str,
    due_date: str,
    reason: str,
    assigned_to: str | None = None,
) -> dict:
    """Validate one exact Launch17 task create without mutation."""
    return _call(TASK_TOOL_ROUTES["dry_run_task_create"], {
        "contact_id": contact_id,
        "action_key": action_key,
        "due_date": due_date,
        "assigned_to": assigned_to,
        "reason": reason,
    })


@SERVER.tool(annotations=ToolAnnotations(read_only_hint=False, destructive_hint=False, open_world_hint=True))
def execute_task_create(
    contact_id: str,
    action_key: str,
    due_date: str,
    reason: str,
    assigned_to: str | None = None,
) -> dict:
    """Create one exact Launch17 task with replay suppression and native readback."""
    return _call(TASK_TOOL_ROUTES["execute_task_create"], {
        "contact_id": contact_id,
        "action_key": action_key,
        "due_date": due_date,
        "assigned_to": assigned_to,
        "reason": reason,
    }, timeout=150)


@SERVER.tool(annotations=ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=True))
def dry_run_task_delete(contact_id: str, task_id: str, action_key: str, reason: str) -> dict:
    """Validate exact cleanup of a governed Launch17 task without mutation."""
    return _call(TASK_TOOL_ROUTES["dry_run_task_delete"], {
        "contact_id": contact_id,
        "task_id": task_id,
        "action_key": action_key,
        "reason": reason,
    })


@SERVER.tool(annotations=ToolAnnotations(read_only_hint=False, destructive_hint=True, idempotent_hint=True, open_world_hint=True))
def execute_task_delete(contact_id: str, task_id: str, action_key: str, reason: str) -> dict:
    """Delete only the task matching the exact contact, task ID and Launch17 action key."""
    return _call(TASK_TOOL_ROUTES["execute_task_delete"], {
        "contact_id": contact_id,
        "task_id": task_id,
        "action_key": action_key,
        "reason": reason,
    }, timeout=150)


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


@SERVER.tool(annotations=ToolAnnotations(read_only_hint=True, destructive_hint=False, open_world_hint=True))
def dry_run_source_record(
    domain: Literal['income', 'rsc'],
    source_id: Annotated[str, Field(min_length=1, max_length=200)],
    source_url: Annotated[str, Field(min_length=1, max_length=2000)],
    kind: Literal['contact', 'opportunity', 'context'],
    title: Annotated[str, Field(min_length=1, max_length=500)],
    properties: dict | None = None,
    content: Annotated[str, Field(max_length=200000)] = '',
    contact_email: str | None = None,
    contact_name: str | None = None,
    company: str | None = None,
) -> dict:
    """Preview existing source-keyed CRM ingestion without provider mutation.

    RSC/Income only. Use the unchanged stable source ID and true source facts.
    No inferred contact details or Dzokden customer data. Returns missing
    configuration, note-part count and intended contact/opportunity effects.
    This preview does not itself grant execution authority.
    """
    return _call('/dry-run/ingest/source-record', {
        'domain':domain, 'source_id':source_id, 'source_url':source_url, 'kind':kind,
        'title':title, 'properties':properties or {}, 'content':content,
        'contact_email':contact_email, 'contact_name':contact_name, 'company':company,
    })


@SERVER.tool(annotations=ToolAnnotations(read_only_hint=False, destructive_hint=False, idempotent_hint=True, open_world_hint=True))
def execute_source_record(
    domain: Literal['income', 'rsc'],
    source_id: Annotated[str, Field(min_length=1, max_length=200)],
    source_url: Annotated[str, Field(min_length=1, max_length=2000)],
    kind: Literal['contact', 'opportunity', 'context'],
    title: Annotated[str, Field(min_length=1, max_length=500)],
    properties: dict | None = None,
    content: Annotated[str, Field(max_length=200000)] = '',
    contact_email: str | None = None,
    contact_name: str | None = None,
    company: str | None = None,
) -> dict:
    """Execute an already authorized, previewed source record through the sole bridge writer.

    Existing ingestion gate, location checks, pending-intent journal, source-key
    dedupe and exact native readback apply. Preserve original source identity.
    Changed source content creates a new versioned note; native opportunity
    stage and existing contact fields remain unchanged. No messaging or contact
    enrichment. Timeout is unresolved: reconcile the same source identity, never
    invent another ID to bypass a pending effect. Only verified native IDs prove completion.
    """
    return _call('/execute/ingest/source-record', {
        'domain':domain, 'source_id':source_id, 'source_url':source_url, 'kind':kind,
        'title':title, 'properties':properties or {}, 'content':content,
        'contact_email':contact_email, 'contact_name':contact_name, 'company':company,
    }, timeout=150)


if __name__ == "__main__":
    # Only the VPS loopback can reach this service; connect externally through
    # an authenticated tunnel, after validating its access controls.
    SERVER.run(transport="streamable-http", host="127.0.0.1", port=8001,
               stateless_http=True, json_response=True)
