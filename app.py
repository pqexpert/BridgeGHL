from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Any, Optional, Literal
from datetime import datetime, timezone
import json
import os
import uuid

import requests

app = FastAPI(title="BridgeGHL", version="0.4.0")

API_KEY = os.getenv("BRIDGE_API_KEY", "")
HIGHLEVEL_PIT = os.getenv("HIGHLEVEL_PIT", "")
HIGHLEVEL_LOCATION_ID = os.getenv("HIGHLEVEL_LOCATION_ID", "")
AUDIT_LOG_PATH = os.getenv("AUDIT_LOG_PATH", "/var/log/bridgeghl/audit.log")
HIGHLEVEL_BASE_URL = os.getenv("HIGHLEVEL_BASE_URL", "https://services.leadconnectorhq.com")
LIVE_WRITE_ENABLED = os.getenv("LIVE_WRITE_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
HEALTHCHECK_TIMEOUT_SECONDS = float(os.getenv("HEALTHCHECK_TIMEOUT_SECONDS", "5"))


def parse_csv_env(name: str) -> set[str]:
    return {
        value.strip()
        for value in os.getenv(name, "").split(",")
        if value and value.strip()
    }


APPROVED_CUSTOM_FIELD_IDS = parse_csv_env("HIGHLEVEL_APPROVED_CUSTOM_FIELD_IDS")
APPROVED_CONTACT_TAGS = parse_csv_env("HIGHLEVEL_APPROVED_CONTACT_TAGS")

ALLOWED_ACTIONS = {
    "update_opportunity",
    "normalize_contact_tags",
}

ALLOWED_OPPORTUNITY_STATUSES = {"open", "won", "lost", "abandoned"}


class OpportunityChanges(BaseModel):
    pipeline_stage_id: Optional[str] = None
    status: Optional[Literal["open", "won", "lost", "abandoned"]] = None
    assigned_to: Optional[str] = None
    custom_fields: dict[str, Any] = Field(default_factory=dict)


class OpportunityUpdateRequest(BaseModel):
    opportunity_id: str
    changes: OpportunityChanges
    reason: str


class ContactTagMutationRequest(BaseModel):
    contact_id: str
    tags_add: list[str] = Field(default_factory=list)
    tags_remove: list[str] = Field(default_factory=list)
    reason: str


class DryRunResponse(BaseModel):
    accepted: bool
    mode: Literal["dry_run"]
    action: str
    validation: dict
    outbound_request: dict


class ExecuteResponse(BaseModel):
    accepted: bool
    mode: Literal["execute"]
    action: str
    audit_id: str
    highlevel_statuses: list[dict]
    readback_status: Optional[int] = None
    verified: bool
    verification: dict


class BridgeHealthResponse(BaseModel):
    ok: bool
    state: Literal["HEALTHY", "DEGRADED", "UNCONFIGURED", "ERROR"]
    service: str
    live_write_enabled: bool
    configured: dict
    allowed_actions: list[str]
    approved_custom_field_count: int
    approved_contact_tag_count: int
    last_checked_at: str
    checks: list[dict]


def require_api_key(x_api_key: Optional[str]):
    if not API_KEY:
        raise HTTPException(status_code=500, detail="BRIDGE_API_KEY is not configured")
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


def normalize_string(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = value.strip()
    return normalized or None


def normalize_opportunity_changes(changes: OpportunityChanges) -> OpportunityChanges:
    fields = {
        str(field_id).strip(): value
        for field_id, value in changes.custom_fields.items()
        if str(field_id).strip()
    }
    return OpportunityChanges(
        pipeline_stage_id=normalize_string(changes.pipeline_stage_id),
        status=changes.status,
        assigned_to=normalize_string(changes.assigned_to),
        custom_fields=fields,
    )


def normalize_tags(tags: list[str]) -> list[str]:
    return sorted({tag.strip() for tag in tags if tag and tag.strip()})


def validate_opportunity_request(payload: OpportunityUpdateRequest) -> dict:
    errors: list[str] = []
    blocked_field_ids = sorted(
        field_id
        for field_id in payload.changes.custom_fields
        if field_id not in APPROVED_CUSTOM_FIELD_IDS
    )

    if not payload.opportunity_id.strip():
        errors.append("opportunity_id is required")
    if not payload.reason.strip():
        errors.append("reason is required")
    if payload.changes.status and payload.changes.status not in ALLOWED_OPPORTUNITY_STATUSES:
        errors.append("unsupported opportunity status")
    if blocked_field_ids:
        errors.append("one or more custom field IDs are not allowlisted")
    if not any(
        [
            payload.changes.pipeline_stage_id,
            payload.changes.status,
            payload.changes.assigned_to,
            payload.changes.custom_fields,
        ]
    ):
        errors.append("at least one allowed opportunity change is required")

    return {
        "valid": not errors,
        "errors": errors,
        "blocked_custom_field_ids": blocked_field_ids,
        "custom_field_allowlist_configured": bool(APPROVED_CUSTOM_FIELD_IDS),
    }


def validate_contact_tag_request(payload: ContactTagMutationRequest) -> dict:
    errors: list[str] = []
    overlap = sorted(set(payload.tags_add).intersection(payload.tags_remove))
    requested_tags = sorted(set(payload.tags_add).union(payload.tags_remove))
    blocked_tags = sorted(tag for tag in requested_tags if tag not in APPROVED_CONTACT_TAGS)

    if not payload.contact_id.strip():
        errors.append("contact_id is required")
    if not payload.reason.strip():
        errors.append("reason is required")
    if not requested_tags:
        errors.append("at least one tag add/remove is required")
    if overlap:
        errors.append("the same tag cannot be added and removed in one request")
    if blocked_tags:
        errors.append("one or more contact tags are not allowlisted")

    return {
        "valid": not errors,
        "errors": errors,
        "blocked_tags": blocked_tags,
        "contact_tag_allowlist_configured": bool(APPROVED_CONTACT_TAGS),
    }


def highlevel_headers(redacted: bool = False) -> dict:
    return {
        "Authorization": "Bearer ***redacted***" if redacted else f"Bearer {HIGHLEVEL_PIT}",
        "Version": "v3",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def highlevel_request(
    method: str,
    url: str,
    *,
    body: Optional[dict] = None,
    params: Optional[dict] = None,
    timeout: float = 30,
) -> tuple[int, dict]:
    if not HIGHLEVEL_PIT:
        raise HTTPException(status_code=500, detail="HIGHLEVEL_PIT is not configured")
    if not HIGHLEVEL_LOCATION_ID:
        raise HTTPException(status_code=500, detail="HIGHLEVEL_LOCATION_ID is not configured")

    response = requests.request(
        method,
        url,
        headers=highlevel_headers(),
        json=body,
        params=params,
        timeout=timeout,
    )
    try:
        data = response.json()
    except ValueError:
        data = {"raw_text": response.text}
    return response.status_code, data


def opportunity_endpoint(opportunity_id: str) -> str:
    return f"{HIGHLEVEL_BASE_URL.rstrip('/')}/opportunities/{opportunity_id}"


def opportunity_search_endpoint() -> str:
    return f"{HIGHLEVEL_BASE_URL.rstrip('/')}/opportunities/search"


def contact_endpoint(contact_id: str) -> str:
    return f"{HIGHLEVEL_BASE_URL.rstrip('/')}/contacts/{contact_id}"


def contact_tags_endpoint(contact_id: str) -> str:
    return f"{HIGHLEVEL_BASE_URL.rstrip('/')}/contacts/{contact_id}/tags"


def build_opportunity_update_body(changes: OpportunityChanges) -> dict:
    body: dict[str, Any] = {}
    if changes.pipeline_stage_id:
        body["pipelineStageId"] = changes.pipeline_stage_id
    if changes.status:
        body["status"] = changes.status
    if changes.assigned_to:
        body["assignedTo"] = changes.assigned_to
    if changes.custom_fields:
        body["customFields"] = [
            {"id": field_id, "fieldValue": value}
            for field_id, value in changes.custom_fields.items()
        ]
    return body


def ensure_audit_dir() -> str:
    audit_dir = os.path.dirname(AUDIT_LOG_PATH) or "."
    os.makedirs(audit_dir, exist_ok=True)
    return audit_dir


def audit_log_writable() -> bool:
    try:
        audit_dir = ensure_audit_dir()
        return os.access(audit_dir, os.W_OK)
    except OSError:
        return False


def append_audit_log(entry: dict):
    ensure_audit_dir()
    with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, separators=(",", ":")) + "\n")


def caller_ip(request: Request) -> Optional[str]:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return None


def unwrap_record(data: dict, key: str) -> dict:
    if not isinstance(data, dict):
        return {}
    nested = data.get(key)
    return nested if isinstance(nested, dict) else data


def custom_field_map(record: dict) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in record.get("customFields") or []:
        if not isinstance(item, dict):
            continue
        field_id = item.get("id") or item.get("fieldId")
        if not field_id:
            continue
        value = item.get("fieldValue")
        if "fieldValue" not in item:
            value = item.get("value")
        result[str(field_id)] = value
    return result


def project_opportunity(data: dict, requested_field_ids: set[str]) -> dict:
    record = unwrap_record(data, "opportunity")
    fields = custom_field_map(record)
    return {
        "pipelineStageId": record.get("pipelineStageId"),
        "status": record.get("status"),
        "assignedTo": record.get("assignedTo"),
        "customFields": {
            field_id: fields.get(field_id)
            for field_id in sorted(requested_field_ids)
        },
    }


def project_contact_tags(data: dict) -> dict:
    record = unwrap_record(data, "contact")
    return {"tags": sorted(record.get("tags") or [])}


def verify_opportunity(projected: dict, changes: OpportunityChanges) -> dict:
    checks: list[dict] = []
    if changes.pipeline_stage_id:
        checks.append(
            {
                "field": "pipelineStageId",
                "ok": projected.get("pipelineStageId") == changes.pipeline_stage_id,
            }
        )
    if changes.status:
        checks.append({"field": "status", "ok": projected.get("status") == changes.status})
    if changes.assigned_to:
        checks.append(
            {"field": "assignedTo", "ok": projected.get("assignedTo") == changes.assigned_to}
        )
    for field_id, expected in changes.custom_fields.items():
        checks.append(
            {
                "field": f"customFields.{field_id}",
                "ok": projected.get("customFields", {}).get(field_id) == expected,
            }
        )
    return {"ok": bool(checks) and all(check["ok"] for check in checks), "checks": checks}


def verify_contact_tags(projected: dict, tags_add: list[str], tags_remove: list[str]) -> dict:
    actual = set(projected.get("tags") or [])
    checks = [
        *[{"tag": tag, "operation": "add", "ok": tag in actual} for tag in tags_add],
        *[{"tag": tag, "operation": "remove", "ok": tag not in actual} for tag in tags_remove],
    ]
    return {"ok": bool(checks) and all(check["ok"] for check in checks), "checks": checks}


def bridge_health_snapshot() -> BridgeHealthResponse:
    checks: list[dict] = []
    configured = {
        "bridge_api_key": bool(API_KEY),
        "highlevel_pit": bool(HIGHLEVEL_PIT),
        "highlevel_location_id": bool(HIGHLEVEL_LOCATION_ID),
        "highlevel_base_url": bool(HIGHLEVEL_BASE_URL),
        "audit_log_path": bool(AUDIT_LOG_PATH),
    }

    for name, ok in configured.items():
        checks.append({"name": name, "ok": ok, "detail": "configured" if ok else "missing"})

    audit_ok = audit_log_writable()
    checks.append({"name": "audit_log_writable", "ok": audit_ok, "detail": "writable" if audit_ok else "not writable"})

    reachability_ok = False
    if LIVE_WRITE_ENABLED and HIGHLEVEL_PIT and HIGHLEVEL_LOCATION_ID:
        try:
            status, _ = highlevel_request(
                "GET",
                opportunity_search_endpoint(),
                params={"locationId": HIGHLEVEL_LOCATION_ID, "limit": 1},
                timeout=HEALTHCHECK_TIMEOUT_SECONDS,
            )
            reachability_ok = 200 <= status < 300
            checks.append(
                {
                    "name": "highlevel_readback",
                    "ok": reachability_ok,
                    "detail": f"status={status}",
                }
            )
        except Exception as exc:
            checks.append({"name": "highlevel_readback", "ok": False, "detail": type(exc).__name__})
    else:
        checks.append(
            {
                "name": "highlevel_readback",
                "ok": False,
                "detail": "skipped until live writes and protected runtime config are enabled",
            }
        )

    required_config_ok = all(configured.values()) and audit_ok
    if not required_config_ok:
        state = "UNCONFIGURED"
        ok = False
    elif not LIVE_WRITE_ENABLED:
        state = "DEGRADED"
        ok = False
    elif not reachability_ok:
        state = "ERROR"
        ok = False
    else:
        state = "HEALTHY"
        ok = True

    return BridgeHealthResponse(
        ok=ok,
        state=state,
        service="BridgeGHL",
        live_write_enabled=LIVE_WRITE_ENABLED,
        configured=configured,
        allowed_actions=sorted(ALLOWED_ACTIONS),
        approved_custom_field_count=len(APPROVED_CUSTOM_FIELD_IDS),
        approved_contact_tag_count=len(APPROVED_CONTACT_TAGS),
        last_checked_at=datetime.now(timezone.utc).isoformat(),
        checks=checks,
    )


def execute_guard(action: str):
    if action not in ALLOWED_ACTIONS:
        raise HTTPException(status_code=400, detail={"error": "action_not_allowed", "action": action})
    health = bridge_health_snapshot()
    if health.state != "HEALTHY":
        raise HTTPException(
            status_code=409,
            detail={
                "error": "bridge_not_ready_for_live_execute",
                "bridge_state": health.state,
                "live_write_enabled": health.live_write_enabled,
            },
        )


@app.get("/health", response_model=BridgeHealthResponse)
def health():
    return bridge_health_snapshot()


@app.post("/contacts/upsert")
@app.post("/dry-run/contact/upsert")
@app.post("/execute/contact/upsert")
def contact_upsert_disabled():
    raise HTTPException(
        status_code=410,
        detail={
            "error": "contact_upsert_disabled_by_governance",
            "use": ["/dry-run/contact/tags", "/execute/contact/tags"],
        },
    )


@app.post("/dry-run/opportunity/update", response_model=DryRunResponse)
def dry_run_opportunity_update(
    payload: OpportunityUpdateRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
):
    require_api_key(x_api_key)
    normalized = OpportunityUpdateRequest(
        opportunity_id=payload.opportunity_id.strip(),
        changes=normalize_opportunity_changes(payload.changes),
        reason=payload.reason.strip(),
    )
    validation = validate_opportunity_request(normalized)
    if not validation["valid"]:
        raise HTTPException(status_code=422, detail=validation)

    return DryRunResponse(
        accepted=True,
        mode="dry_run",
        action="update_opportunity",
        validation=validation,
        outbound_request={
            "method": "PUT",
            "url": opportunity_endpoint(normalized.opportunity_id),
            "headers": highlevel_headers(redacted=True),
            "json": build_opportunity_update_body(normalized.changes),
            "caller_ip": caller_ip(request),
            "bridge_state": bridge_health_snapshot().state,
            "rollback_notes": [
                "execute captures the relevant pre-state before mutation",
                "post-write readback must verify the requested fields",
                "if verification fails, treat the action as unresolved and escalate with the audit ID",
            ],
        },
    )


@app.post("/execute/opportunity/update", response_model=ExecuteResponse)
def execute_opportunity_update(
    payload: OpportunityUpdateRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
):
    require_api_key(x_api_key)
    execute_guard("update_opportunity")
    normalized = OpportunityUpdateRequest(
        opportunity_id=payload.opportunity_id.strip(),
        changes=normalize_opportunity_changes(payload.changes),
        reason=payload.reason.strip(),
    )
    validation = validate_opportunity_request(normalized)
    if not validation["valid"]:
        raise HTTPException(status_code=422, detail=validation)

    requested_field_ids = set(normalized.changes.custom_fields)
    before_status, before_data = highlevel_request(
        "GET", opportunity_endpoint(normalized.opportunity_id)
    )
    if not 200 <= before_status < 300:
        raise HTTPException(
            status_code=409,
            detail={"error": "pre_write_readback_failed", "status": before_status},
        )

    before = project_opportunity(before_data, requested_field_ids)
    write_status, _ = highlevel_request(
        "PUT",
        opportunity_endpoint(normalized.opportunity_id),
        body=build_opportunity_update_body(normalized.changes),
    )
    after_status, after_data = highlevel_request(
        "GET", opportunity_endpoint(normalized.opportunity_id)
    )
    after = project_opportunity(after_data, requested_field_ids) if 200 <= after_status < 300 else {}
    verification = verify_opportunity(after, normalized.changes) if after else {"ok": False, "checks": []}

    audit_id = str(uuid.uuid4())
    audit_entry = {
        "audit_id": audit_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": "update_opportunity",
        "mode": "execute",
        "caller_ip": caller_ip(request),
        "target_id": normalized.opportunity_id,
        "reason": normalized.reason,
        "before": before,
        "requested": build_opportunity_update_body(normalized.changes),
        "after": after,
        "write_status": write_status,
        "readback_status": after_status,
        "verified": verification["ok"],
        "result": "verified" if verification["ok"] else "needs_runtime_verification",
    }
    append_audit_log(audit_entry)

    return ExecuteResponse(
        accepted=200 <= write_status < 300,
        mode="execute",
        action="update_opportunity",
        audit_id=audit_id,
        highlevel_statuses=[{"operation": "update_opportunity", "status": write_status}],
        readback_status=after_status,
        verified=verification["ok"],
        verification=verification,
    )


@app.post("/dry-run/contact/tags", response_model=DryRunResponse)
def dry_run_contact_tags(
    payload: ContactTagMutationRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
):
    require_api_key(x_api_key)
    normalized = ContactTagMutationRequest(
        contact_id=payload.contact_id.strip(),
        tags_add=normalize_tags(payload.tags_add),
        tags_remove=normalize_tags(payload.tags_remove),
        reason=payload.reason.strip(),
    )
    validation = validate_contact_tag_request(normalized)
    if not validation["valid"]:
        raise HTTPException(status_code=422, detail=validation)

    return DryRunResponse(
        accepted=True,
        mode="dry_run",
        action="normalize_contact_tags",
        validation=validation,
        outbound_request={
            "operations": [
                *(
                    [
                        {
                            "method": "POST",
                            "url": contact_tags_endpoint(normalized.contact_id),
                            "json": {"tags": normalized.tags_add},
                        }
                    ]
                    if normalized.tags_add
                    else []
                ),
                *(
                    [
                        {
                            "method": "DELETE",
                            "url": contact_tags_endpoint(normalized.contact_id),
                            "json": {"tags": normalized.tags_remove},
                        }
                    ]
                    if normalized.tags_remove
                    else []
                ),
            ],
            "headers": highlevel_headers(redacted=True),
            "caller_ip": caller_ip(request),
            "bridge_state": bridge_health_snapshot().state,
            "rollback_notes": [
                "execute reads current tags before mutation",
                "a failed remove after a successful add triggers compensation for newly added tags",
                "post-write readback must verify additions and removals",
            ],
        },
    )


@app.post("/execute/contact/tags", response_model=ExecuteResponse)
def execute_contact_tags(
    payload: ContactTagMutationRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
):
    require_api_key(x_api_key)
    execute_guard("normalize_contact_tags")
    normalized = ContactTagMutationRequest(
        contact_id=payload.contact_id.strip(),
        tags_add=normalize_tags(payload.tags_add),
        tags_remove=normalize_tags(payload.tags_remove),
        reason=payload.reason.strip(),
    )
    validation = validate_contact_tag_request(normalized)
    if not validation["valid"]:
        raise HTTPException(status_code=422, detail=validation)

    before_status, before_data = highlevel_request("GET", contact_endpoint(normalized.contact_id))
    if not 200 <= before_status < 300:
        raise HTTPException(
            status_code=409,
            detail={"error": "pre_write_readback_failed", "status": before_status},
        )

    before = project_contact_tags(before_data)
    before_tags = set(before["tags"])
    effective_add = sorted(set(normalized.tags_add) - before_tags)
    effective_remove = sorted(set(normalized.tags_remove).intersection(before_tags))

    statuses: list[dict] = []
    compensation: list[dict] = []
    write_ok = True

    if effective_add:
        add_status, _ = highlevel_request(
            "POST",
            contact_tags_endpoint(normalized.contact_id),
            body={"tags": effective_add},
        )
        statuses.append({"operation": "add_tags", "status": add_status})
        write_ok = 200 <= add_status < 300

    if write_ok and effective_remove:
        remove_status, _ = highlevel_request(
            "DELETE",
            contact_tags_endpoint(normalized.contact_id),
            body={"tags": effective_remove},
        )
        statuses.append({"operation": "remove_tags", "status": remove_status})
        write_ok = 200 <= remove_status < 300

        if not write_ok and effective_add:
            compensate_status, _ = highlevel_request(
                "DELETE",
                contact_tags_endpoint(normalized.contact_id),
                body={"tags": effective_add},
            )
            compensation.append(
                {"operation": "remove_added_tags", "status": compensate_status}
            )

    after_status, after_data = highlevel_request("GET", contact_endpoint(normalized.contact_id))
    after = project_contact_tags(after_data) if 200 <= after_status < 300 else {}
    verification = (
        verify_contact_tags(after, normalized.tags_add, normalized.tags_remove)
        if after and write_ok
        else {"ok": False, "checks": []}
    )

    audit_id = str(uuid.uuid4())
    audit_entry = {
        "audit_id": audit_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": "normalize_contact_tags",
        "mode": "execute",
        "caller_ip": caller_ip(request),
        "target_id": normalized.contact_id,
        "reason": normalized.reason,
        "before": before,
        "requested": {"tags_add": normalized.tags_add, "tags_remove": normalized.tags_remove},
        "effective": {"tags_add": effective_add, "tags_remove": effective_remove},
        "statuses": statuses,
        "compensation": compensation,
        "after": after,
        "readback_status": after_status,
        "verified": verification["ok"],
        "result": "verified" if verification["ok"] else "needs_runtime_verification",
    }
    append_audit_log(audit_entry)

    return ExecuteResponse(
        accepted=write_ok,
        mode="execute",
        action="normalize_contact_tags",
        audit_id=audit_id,
        highlevel_statuses=statuses + compensation,
        readback_status=after_status,
        verified=verification["ok"],
        verification=verification,
    )
