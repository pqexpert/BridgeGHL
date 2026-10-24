from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Any, Optional, Literal
from datetime import datetime, timezone
import json
import os
import re
import uuid
import hashlib
from urllib.parse import urlsplit

import requests
import time

app = FastAPI(title="BridgeGHL", version="0.6.0")

API_KEY = os.getenv("BRIDGE_API_KEY", "")
HIGHLEVEL_PIT = os.getenv("HIGHLEVEL_PIT", "")
HIGHLEVEL_LOCATION_ID = os.getenv("HIGHLEVEL_LOCATION_ID", "")
AUDIT_LOG_PATH = os.getenv("AUDIT_LOG_PATH", "/var/log/bridgeghl/audit.log")
HIGHLEVEL_BASE_URL = os.getenv("HIGHLEVEL_BASE_URL", "https://services.leadconnectorhq.com")
LIVE_WRITE_ENABLED = os.getenv("LIVE_WRITE_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
TASK_WRITES_ENABLED = os.getenv("HIGHLEVEL_TASK_WRITES_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
HEALTHCHECK_TIMEOUT_SECONDS = float(os.getenv("HEALTHCHECK_TIMEOUT_SECONDS", "5"))


def parse_csv_env(name: str) -> set[str]:
    return {
        value.strip()
        for value in os.getenv(name, "").split(",")
        if value and value.strip()
    }


APPROVED_CUSTOM_FIELD_IDS = parse_csv_env("HIGHLEVEL_APPROVED_CUSTOM_FIELD_IDS")
APPROVED_CONTACT_TAGS = parse_csv_env("HIGHLEVEL_APPROVED_CONTACT_TAGS")

BASE_ALLOWED_ACTIONS = {
    "update_opportunity",
    "normalize_contact_tags",
}
TASK_ACTIONS = {"create_task", "delete_task"}


def allowed_actions() -> set[str]:
    """Return the active mutation envelope; task authority defaults closed."""
    return BASE_ALLOWED_ACTIONS | (TASK_ACTIONS if TASK_WRITES_ENABLED else set())

TASK_ACTION_PREFIX = "launch17-next-action:v1:"
TASK_ACTION_KEY_RE = re.compile(
    r"^launch17-next-action:v1:[A-Za-z0-9_-]+:[A-Za-z0-9_-]+:[A-Za-z0-9_-]+:(?:0[1-9]|1[0-7])$"
)
SAFE_PROVIDER_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")

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


class TaskCreateRequest(BaseModel):
    contact_id: str
    action_key: str
    due_date: str
    assigned_to: Optional[str] = None
    reason: str


class TaskDeleteRequest(BaseModel):
    contact_id: str
    task_id: str
    action_key: str
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


class TaskExecuteResponse(BaseModel):
    accepted: bool
    mode: Literal["execute"]
    action: Literal["create_task", "delete_task"]
    effect: Literal["CREATED", "NO_CHANGE_EXACT_REPLAY", "DELETED", "UNRESOLVED"]
    audit_id: str
    task_id: Optional[str] = None
    highlevel_statuses: list[dict]
    readback_status: Optional[int] = None
    verified: bool
    verification: dict


class BridgeHealthResponse(BaseModel):
    ok: bool
    state: Literal["HEALTHY", "DEGRADED", "UNCONFIGURED", "ERROR"]
    service: str
    live_write_enabled: bool
    task_writes_enabled: bool
    configured: dict
    allowed_actions: list[str]
    approved_custom_field_count: int
    approved_contact_tag_count: int
    last_checked_at: str
    checks: list[dict]
    actor_operational_projection: dict = Field(default_factory=dict)
    highlevel_requests: dict = Field(default_factory=dict)


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


def normalize_due_date(value: str) -> str:
    raw = value.strip()
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("due_date must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def task_title(action_key: str) -> str:
    return f"Launch17 next action | {action_key}"


def build_task_create_body(payload: TaskCreateRequest) -> dict:
    body: dict[str, Any] = {
        "title": task_title(payload.action_key),
        "body": f"Governed exact-event action. No outreach or Calendar authority. {payload.action_key}",
        "dueDate": normalize_due_date(payload.due_date),
        "completed": False,
    }
    if payload.assigned_to:
        body["assignedTo"] = payload.assigned_to
    return body


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


def validate_task_create_request(payload: TaskCreateRequest) -> dict:
    errors: list[str] = []
    if not payload.contact_id or not SAFE_PROVIDER_ID_RE.fullmatch(payload.contact_id):
        errors.append("contact_id must be a non-empty provider ID")
    if not payload.reason:
        errors.append("reason is required")
    if not TASK_ACTION_KEY_RE.fullmatch(payload.action_key):
        errors.append("action_key must be an exact Launch17 event key for door 01-17")
    if payload.assigned_to and not SAFE_PROVIDER_ID_RE.fullmatch(payload.assigned_to):
        errors.append("assigned_to must be a provider ID")
    try:
        normalize_due_date(payload.due_date)
    except (TypeError, ValueError):
        errors.append("due_date must be an ISO-8601 timestamp with timezone")
    return {"valid": not errors, "errors": errors}


def validate_task_delete_request(payload: TaskDeleteRequest) -> dict:
    errors: list[str] = []
    if not payload.contact_id or not SAFE_PROVIDER_ID_RE.fullmatch(payload.contact_id):
        errors.append("contact_id must be a non-empty provider ID")
    if not payload.task_id or not SAFE_PROVIDER_ID_RE.fullmatch(payload.task_id):
        errors.append("task_id must be a non-empty provider ID")
    if not payload.reason:
        errors.append("reason is required")
    if not TASK_ACTION_KEY_RE.fullmatch(payload.action_key):
        errors.append("action_key must be an exact Launch17 event key for door 01-17")
    return {"valid": not errors, "errors": errors}


def highlevel_headers(redacted: bool = False) -> dict:
    return {
        "Authorization": "Bearer ***redacted***" if redacted else f"Bearer {HIGHLEVEL_PIT}",
        "Version": "v3",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "BridgeGHL/0.6.0",
    }


def _highlevel_request(
    method: str,
    url: str,
    *,
    body: Optional[dict] = None,
    params: Optional[dict] = None,
    timeout: float = 30,
    retry_reads: bool = True,
) -> tuple[int, dict]:
    if not HIGHLEVEL_PIT:
        raise HTTPException(status_code=500, detail="HIGHLEVEL_PIT is not configured")
    if not HIGHLEVEL_LOCATION_ID:
        raise HTTPException(status_code=500, detail="HIGHLEVEL_LOCATION_ID is not configured")

    from provider_budget import reserve, observe, BudgetDeferred
    try:
        counted = reserve()
    except BudgetDeferred as exc:
        if str(exc) == 'request_burst_budget' and exc.retry_after is not None and exc.retry_after <= 10:
            time.sleep(exc.retry_after + 0.01)  # No network retry; reservation precedes the first attempt.
            try:
                counted = reserve()
            except BudgetDeferred as deferred:
                raise HTTPException(429, str(deferred)) from None
        else:
            raise HTTPException(429, str(exc)) from None
    from provider_budget import CURRENT
    scope = CURRENT.get()
    request_id = str(uuid.uuid4())
    # No URL/query, target ID, headers, body, response content or exception text.
    resource = urlsplit(url).path.strip('/').split('/')[0]
    resource = resource if resource in {'contacts', 'opportunities', 'conversations',
        'locations', 'forms', 'workflows', 'calendars', 'users'} else 'other'
    metadata = {'action': 'provider_interaction', 'request_id': request_id,
                'actor': scope[0] if scope else 'unscoped',
                'method': method.upper() if method.upper() in {'GET', 'POST', 'PUT', 'PATCH', 'DELETE'} else 'OTHER',
                'resource': resource,
                'target_sha256': hashlib.sha256(urlsplit(url).path.encode()).hexdigest()}

    def attempt(number):
        started = time.monotonic()
        def record(result, **extra):
            append_audit_log({**metadata, 'attempt': number, 'result': result,
                'observed_at': datetime.now(timezone.utc).isoformat(), **extra})
        # Audit failure here prevents the provider request. An intent without an
        # outcome remains unresolved; it must never be read as provider success.
        record('intent')
        try:
            response = requests.request(method, url, headers=highlevel_headers(),
                                        json=body, params=params, timeout=timeout)
        except Exception:
            record('transport_unknown', elapsed_ms=round((time.monotonic()-started)*1000))
            raise
        record('response', provider_status=response.status_code,
               elapsed_ms=round((time.monotonic()-started)*1000))
        return response

    response = attempt(1)
    # A transient provider read rejection must not replay a mutation.
    if retry_reads and not counted and method.upper() == 'GET' and response.status_code in (401, 429, 502, 503, 504):
        time.sleep(2)
        response = attempt(2)
    if counted:
        observe(response.status_code, response.headers)
    try:
        data = response.json()
    except ValueError:
        data = {"raw_text": response.text}
    return response.status_code, data


def highlevel_request(method: str, url: str, *, body=None, params=None, timeout=30, retry_reads=True):
    # Legacy bridge routes are RSC-scoped; typed Income routes and the private
    # root projector supply narrower trusted contexts. Clients cannot set this.
    from provider_budget import CURRENT, actor_scope
    if CURRENT.get() is not None:
        return _highlevel_request(method, url, body=body, params=params, timeout=timeout, retry_reads=False)
    from pathlib import Path
    with actor_scope('rsc', Path(AUDIT_LOG_PATH).parent / 'ingestion.sqlite3'):
        return _highlevel_request(method, url, body=body, params=params, timeout=timeout, retry_reads=False)


def opportunity_endpoint(opportunity_id: str) -> str:
    return f"{HIGHLEVEL_BASE_URL.rstrip('/')}/opportunities/{opportunity_id}"


def opportunity_search_endpoint() -> str:
    return f"{HIGHLEVEL_BASE_URL.rstrip('/')}/opportunities/search"


def contact_endpoint(contact_id: str) -> str:
    return f"{HIGHLEVEL_BASE_URL.rstrip('/')}/contacts/{contact_id}"


def contact_tags_endpoint(contact_id: str) -> str:
    return f"{HIGHLEVEL_BASE_URL.rstrip('/')}/contacts/{contact_id}/tags"


def contact_tasks_endpoint(contact_id: str) -> str:
    return f"{HIGHLEVEL_BASE_URL.rstrip('/')}/contacts/{contact_id}/tasks"


def contact_task_endpoint(contact_id: str, task_id: str) -> str:
    return f"{contact_tasks_endpoint(contact_id)}/{task_id}"


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
        f.flush()
        os.fsync(f.fileno())


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


def unwrap_task_list(data: dict) -> list[dict]:
    if not isinstance(data, dict):
        return []
    tasks = data.get("tasks")
    return [task for task in tasks if isinstance(task, dict)] if isinstance(tasks, list) else []


def task_absent_from_complete_list(data: dict, contact_id: str, task_id: str, action_key: str) -> bool:
    """Fail closed on malformed, foreign or paginated task-list responses.

    The provider's Get all Tasks contract is an unpaginated tasks array.
    A 400 from a deleted task's direct GET is NOT absence evidence by itself.
    """
    if not isinstance(data, dict) or not set(data) <= {"tasks", "traceId"}:
        return False
    tasks = data.get("tasks")
    if not isinstance(tasks, list):
        return False
    for task in tasks:
        if not isinstance(task, dict) or not task.get("id") or task.get("contactId") != contact_id:
            return False
        if task["id"] == task_id or task.get("title") == task_title(action_key):
            return False
    return True


def contact_belongs_to_location(data: dict, contact_id: str) -> bool:
    record = unwrap_record(data, "contact")
    return (
        str(record.get("id") or "") == contact_id
        and str(record.get("locationId") or "") == HIGHLEVEL_LOCATION_ID
    )


def project_task(data: dict) -> dict:
    record = unwrap_record(data, "task")
    return {
        "id": record.get("id"),
        "contactId": record.get("contactId"),
        "assignedTo": record.get("assignedTo"),
        "dueDate": record.get("dueDate"),
        "completed": record.get("completed"),
        "title": record.get("title"),
    }


def verify_task(projected: dict, payload: TaskCreateRequest) -> dict:
    checks = [
        {"field": "title", "ok": projected.get("title") == task_title(payload.action_key)},
        {"field": "contactId", "ok": projected.get("contactId") == payload.contact_id},
        {"field": "completed", "ok": projected.get("completed") is False},
    ]
    try:
        actual_due = normalize_due_date(str(projected.get("dueDate") or ""))
    except (TypeError, ValueError):
        actual_due = None
    checks.append({"field": "dueDate", "ok": actual_due == normalize_due_date(payload.due_date)})
    if payload.assigned_to:
        checks.append({"field": "assignedTo", "ok": projected.get("assignedTo") == payload.assigned_to})
    return {"ok": all(check["ok"] for check in checks), "checks": checks}


def verify_task_identity(projected: dict, payload: TaskDeleteRequest) -> dict:
    checks = [
        {"field": "id", "ok": projected.get("id") == payload.task_id},
        {"field": "contactId", "ok": projected.get("contactId") == payload.contact_id},
        {"field": "title", "ok": projected.get("title") == task_title(payload.action_key)},
    ]
    return {"ok": all(check["ok"] for check in checks), "checks": checks}


def highlevel_request_status():
    from pathlib import Path
    path = Path(AUDIT_LOG_PATH).parent / 'highlevel-request-status.json'
    try:
        if path.is_symlink() or path.stat().st_size > 1048576:
            raise ValueError('status_bounds')
        value = json.loads(path.read_text())
        fields = ('request_id','lane','operation','request_comment_id','request_sha256',
                  'status','native_readback_at','cached')
        rows = []
        for row in value.get('requests',[])[:100]:
            item = {k:row[k] for k in fields if k in row}
            receipt = row.get('receipt') or {}
            if isinstance(receipt,dict):
                item['receipt'] = {k:receipt[k] for k in ('status','comment_id','native_readback') if k in receipt}
            rows.append(item)
        return {'source':'cached_request_delivery','observed_at':value.get('observed_at'),
                'current_provider_read':False,'requests':rows}
    except (OSError, ValueError, TypeError, KeyError):
        return {'source':'cached_request_delivery','status':'UNAVAILABLE','current_provider_read':False}


def operational_projection_status():
    # Cached delivery metadata only. Never implies current provider/mission truth.
    from pathlib import Path
    path = Path(AUDIT_LOG_PATH).parent / 'actor-projection-status.json'
    try:
        if path.is_symlink() or path.stat().st_size > 65536:
            raise ValueError('status_bounds')
        value = json.loads(path.read_text())
        keys = ('status','source_receipt_id','source_body_sha256','source_updated_at',
                'source_reported_outcome','source_kind','native_readback_at','delivery')
        actors = {actor:{k:row[k] for k in keys if k in row}
                  for actor,row in value.get('actors',{}).items()
                  if actor in {'income','rsc','store','distiller'} and isinstance(row,dict)}
        from provider_budget import status
        return {'source':'cached_operational_delivery', 'observed_at':value.get('observed_at'),
                'current_provider_read':False, 'actors':actors, 'budget':status(path.parent/'ingestion.sqlite3')}
    except (OSError, ValueError, TypeError, KeyError):
        return {'source':'cached_operational_delivery', 'status':'UNAVAILABLE', 'current_provider_read':False}


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
        task_writes_enabled=TASK_WRITES_ENABLED,
        configured=configured,
        allowed_actions=sorted(allowed_actions()),
        approved_custom_field_count=len(APPROVED_CUSTOM_FIELD_IDS),
        approved_contact_tag_count=len(APPROVED_CONTACT_TAGS),
        last_checked_at=datetime.now(timezone.utc).isoformat(),
        checks=checks,
        actor_operational_projection=operational_projection_status(),
        highlevel_requests=highlevel_request_status(),
    )


def execute_guard(action: str):
    if action not in allowed_actions():
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

@app.post("/dry-run/task/create", response_model=DryRunResponse)
def dry_run_task_create(
    payload: TaskCreateRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
):
    require_api_key(x_api_key)
    try:
        due_date = normalize_due_date(payload.due_date)
    except (TypeError, ValueError):
        due_date = payload.due_date.strip()
    normalized = TaskCreateRequest(
        contact_id=payload.contact_id.strip(),
        action_key=payload.action_key.strip(),
        due_date=due_date,
        assigned_to=normalize_string(payload.assigned_to),
        reason=payload.reason.strip(),
    )
    validation = validate_task_create_request(normalized)
    if not validation["valid"]:
        raise HTTPException(status_code=422, detail=validation)
    return DryRunResponse(
        accepted=True,
        mode="dry_run",
        action="create_task",
        validation=validation,
        outbound_request={
            "method": "POST",
            "url": contact_tasks_endpoint(normalized.contact_id),
            "headers": highlevel_headers(redacted=True),
            "json": build_task_create_body(normalized),
            "dedupe": {"match": "exact derived title", "effect": "NO_CHANGE_EXACT_REPLAY"},
            "caller_ip": caller_ip(request),
            "bridge_state": bridge_health_snapshot().state,
            "rollback_notes": [
                "execute validates the contact belongs to the configured location",
                "execute reads all contact tasks and suppresses one exact replay",
                "cleanup is permitted only through the exact task ID and action key",
            ],
        },
    )


@app.post("/execute/task/create", response_model=TaskExecuteResponse)
def execute_task_create(
    payload: TaskCreateRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
):
    require_api_key(x_api_key)
    execute_guard("create_task")
    try:
        due_date = normalize_due_date(payload.due_date)
    except (TypeError, ValueError):
        due_date = payload.due_date.strip()
    normalized = TaskCreateRequest(
        contact_id=payload.contact_id.strip(),
        action_key=payload.action_key.strip(),
        due_date=due_date,
        assigned_to=normalize_string(payload.assigned_to),
        reason=payload.reason.strip(),
    )
    validation = validate_task_create_request(normalized)
    if not validation["valid"]:
        raise HTTPException(status_code=422, detail=validation)

    contact_status, contact_data = highlevel_request(
        "GET", contact_endpoint(normalized.contact_id), timeout=10
    )
    if not 200 <= contact_status < 300 or not contact_belongs_to_location(contact_data, normalized.contact_id):
        raise HTTPException(
            status_code=409,
            detail={"error": "contact_location_validation_failed", "status": contact_status},
        )

    list_status, list_data = highlevel_request(
        "GET", contact_tasks_endpoint(normalized.contact_id), timeout=10
    )
    if not 200 <= list_status < 300:
        raise HTTPException(
            status_code=409,
            detail={"error": "pre_write_task_read_failed", "status": list_status},
        )
    matches = [
        task for task in unwrap_task_list(list_data)
        if task.get("title") == task_title(normalized.action_key)
    ]
    if len(matches) > 1:
        raise HTTPException(
            status_code=409,
            detail={"error": "duplicate_action_key_conflict", "match_count": len(matches)},
        )

    audit_id = str(uuid.uuid4())
    statuses: list[dict] = [
        {"operation": "read_contact", "status": contact_status},
        {"operation": "list_tasks", "status": list_status},
    ]

    if len(matches) == 1:
        task_id = str(matches[0].get("id") or "")
        if not task_id:
            raise HTTPException(status_code=409, detail={"error": "exact_replay_missing_task_id"})
        readback_status, readback_data = highlevel_request(
            "GET", contact_task_endpoint(normalized.contact_id, task_id), timeout=15
        )
        projected = project_task(readback_data) if 200 <= readback_status < 300 else {}
        verification = verify_task(projected, normalized) if projected else {"ok": False, "checks": []}
        if not verification["ok"]:
            raise HTTPException(
                status_code=409,
                detail={"error": "existing_action_key_state_conflict", "task_id": task_id},
            )
        statuses.append({"operation": "read_task", "status": readback_status})
        append_audit_log({
            "audit_id": audit_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "action": "create_task",
            "mode": "execute",
            "caller_ip": caller_ip(request),
            "target_id": normalized.contact_id,
            "action_key": normalized.action_key,
            "reason": normalized.reason,
            "effect": "NO_CHANGE_EXACT_REPLAY",
            "task_id": task_id,
            "statuses": statuses,
            "readback": projected,
            "verification": verification,
            "verified": True,
            "result": "verified_exact_replay",
        })
        return TaskExecuteResponse(
            accepted=True,
            mode="execute",
            action="create_task",
            effect="NO_CHANGE_EXACT_REPLAY",
            audit_id=audit_id,
            task_id=task_id,
            highlevel_statuses=statuses,
            readback_status=readback_status,
            verified=True,
            verification=verification,
        )

    requested = build_task_create_body(normalized)
    append_audit_log({
        "audit_id": audit_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": "create_task",
        "mode": "execute",
        "phase": "intent",
        "caller_ip": caller_ip(request),
        "target_id": normalized.contact_id,
        "action_key": normalized.action_key,
        "reason": normalized.reason,
        "requested": requested,
        "statuses": statuses,
        "result": "intent_persisted",
    })
    task_id = ""
    readback_status: Optional[int] = None
    projected: dict = {}
    try:
        write_status, write_data = highlevel_request(
            "POST",
            contact_tasks_endpoint(normalized.contact_id),
            body=requested,
            timeout=15,
            retry_reads=False,
        )
        statuses.append({"operation": "create_task", "status": write_status})
        created = unwrap_record(write_data, "task") if 200 <= write_status < 300 else {}
        task_id = str(created.get("id") or "")
        if task_id:
            readback_status, readback_data = highlevel_request(
                "GET", contact_task_endpoint(normalized.contact_id, task_id), timeout=15
            )
            projected = project_task(readback_data) if 200 <= readback_status < 300 else {}
            statuses.append({"operation": "read_task", "status": readback_status})
    except requests.RequestException as exc:
        append_audit_log({
            "audit_id": audit_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "action": "create_task",
            "mode": "execute",
            "phase": "final",
            "target_id": normalized.contact_id,
            "action_key": normalized.action_key,
            "effect": "UNRESOLVED",
            "statuses": statuses,
            "verified": False,
            "result": "provider_mutation_outcome_unknown",
            "error_type": type(exc).__name__,
        })
        raise HTTPException(
            status_code=503,
            detail={"error": "provider_mutation_outcome_unknown", "audit_id": audit_id},
        ) from exc
    verification = verify_task(projected, normalized) if projected else {"ok": False, "checks": []}
    verified = bool(200 <= write_status < 300 and verification["ok"])
    effect = "CREATED" if verified else "UNRESOLVED"
    append_audit_log({
        "audit_id": audit_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": "create_task",
        "mode": "execute",
        "caller_ip": caller_ip(request),
        "target_id": normalized.contact_id,
        "action_key": normalized.action_key,
        "reason": normalized.reason,
        "phase": "final",
        "requested": requested,
        "effect": effect,
        "task_id": task_id or None,
        "statuses": statuses,
        "readback_status": readback_status,
        "readback": projected,
        "verification": verification,
        "verified": verified,
        "result": "verified" if verified else "needs_runtime_verification",
    })
    return TaskExecuteResponse(
        accepted=200 <= write_status < 300,
        mode="execute",
        action="create_task",
        effect=effect,
        audit_id=audit_id,
        task_id=task_id or None,
        highlevel_statuses=statuses,
        readback_status=readback_status,
        verified=verified,
        verification=verification,
    )


@app.post("/dry-run/task/delete", response_model=DryRunResponse)
def dry_run_task_delete(
    payload: TaskDeleteRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
):
    require_api_key(x_api_key)
    normalized = TaskDeleteRequest(
        contact_id=payload.contact_id.strip(),
        task_id=payload.task_id.strip(),
        action_key=payload.action_key.strip(),
        reason=payload.reason.strip(),
    )
    validation = validate_task_delete_request(normalized)
    if not validation["valid"]:
        raise HTTPException(status_code=422, detail=validation)
    return DryRunResponse(
        accepted=True,
        mode="dry_run",
        action="delete_task",
        validation=validation,
        outbound_request={
            "method": "DELETE",
            "url": contact_task_endpoint(normalized.contact_id, normalized.task_id),
            "headers": highlevel_headers(redacted=True),
            "precondition": "exact task ID, contact ID and derived action-key title must match",
            "caller_ip": caller_ip(request),
            "bridge_state": bridge_health_snapshot().state,
            "rollback_notes": ["deletion is limited to an exact governed Launch17 task"],
        },
    )


@app.post("/execute/task/delete", response_model=TaskExecuteResponse)
def execute_task_delete(
    payload: TaskDeleteRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
):
    require_api_key(x_api_key)
    execute_guard("delete_task")
    normalized = TaskDeleteRequest(
        contact_id=payload.contact_id.strip(),
        task_id=payload.task_id.strip(),
        action_key=payload.action_key.strip(),
        reason=payload.reason.strip(),
    )
    validation = validate_task_delete_request(normalized)
    if not validation["valid"]:
        raise HTTPException(status_code=422, detail=validation)

    contact_status, contact_data = highlevel_request(
        "GET", contact_endpoint(normalized.contact_id), timeout=10
    )
    if not 200 <= contact_status < 300 or not contact_belongs_to_location(contact_data, normalized.contact_id):
        raise HTTPException(
            status_code=409,
            detail={"error": "contact_location_validation_failed", "status": contact_status},
        )
    before_status, before_data = highlevel_request(
        "GET", contact_task_endpoint(normalized.contact_id, normalized.task_id), timeout=15
    )
    before = project_task(before_data) if 200 <= before_status < 300 else {}
    identity = verify_task_identity(before, normalized) if before else {"ok": False, "checks": []}
    if not identity["ok"]:
        raise HTTPException(
            status_code=409,
            detail={"error": "task_identity_validation_failed", "status": before_status},
        )

    audit_id = str(uuid.uuid4())
    statuses = [
        {"operation": "read_contact", "status": contact_status},
        {"operation": "read_task", "status": before_status},
    ]
    append_audit_log({
        "audit_id": audit_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": "delete_task",
        "mode": "execute",
        "phase": "intent",
        "caller_ip": caller_ip(request),
        "target_id": normalized.contact_id,
        "action_key": normalized.action_key,
        "reason": normalized.reason,
        "task_id": normalized.task_id,
        "readback_before": before,
        "identity_verification": identity,
        "statuses": statuses,
        "result": "intent_persisted",
    })
    try:
        delete_status, _ = highlevel_request(
            "DELETE",
            contact_task_endpoint(normalized.contact_id, normalized.task_id),
            timeout=15,
            retry_reads=False,
        )
        statuses.append({"operation": "delete_task", "status": delete_status})
        readback_status, _ = highlevel_request(
            "GET", contact_task_endpoint(normalized.contact_id, normalized.task_id), timeout=15
        )
        statuses.append({"operation": "read_deleted_task", "status": readback_status})
        list_absence = False
        if 200 <= delete_status < 300 and readback_status == 400:
            list_status, list_data = highlevel_request(
                "GET", contact_tasks_endpoint(normalized.contact_id), timeout=15
            )
            statuses.append({"operation": "read_all_tasks_after_delete", "status": list_status})
            list_absence = list_status == 200 and task_absent_from_complete_list(
                list_data, normalized.contact_id, normalized.task_id, normalized.action_key
            )
    except requests.RequestException as exc:
        append_audit_log({
            "audit_id": audit_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "action": "delete_task",
            "mode": "execute",
            "phase": "final",
            "target_id": normalized.contact_id,
            "action_key": normalized.action_key,
            "task_id": normalized.task_id,
            "readback_before": before,
            "identity_verification": identity,
            "effect": "UNRESOLVED",
            "statuses": statuses,
            "verified": False,
            "result": "provider_mutation_outcome_unknown",
            "error_type": type(exc).__name__,
        })
        raise HTTPException(
            status_code=503,
            detail={"error": "provider_mutation_outcome_unknown", "audit_id": audit_id,
                    "task_id": normalized.task_id},
        ) from exc
    absence_verified = readback_status == 404 or list_absence
    verified = 200 <= delete_status < 300 and absence_verified
    verification = {
        "ok": verified,
        "checks": [
            {"field": "identity", "ok": identity["ok"]},
            {"field": "provider_absence_readback", "ok": absence_verified},
        ],
        "absence_method": "complete_task_list" if list_absence else "direct_get_404" if readback_status == 404 else "unverified",
    }
    append_audit_log({
        "audit_id": audit_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": "delete_task",
        "mode": "execute",
        "caller_ip": caller_ip(request),
        "target_id": normalized.contact_id,
        "action_key": normalized.action_key,
        "reason": normalized.reason,
        "phase": "final",
        "effect": "DELETED" if verified else "UNRESOLVED",
        "task_id": normalized.task_id,
        "statuses": statuses,
        "readback_before": before,
        "identity_verification": identity,
        "verification": verification,
        "verified": verified,
        "result": "verified" if verified else "needs_runtime_verification",
    })
    return TaskExecuteResponse(
        accepted=200 <= delete_status < 300,
        mode="execute",
        action="delete_task",
        effect="DELETED" if verified else "UNRESOLVED",
        audit_id=audit_id,
        task_id=normalized.task_id,
        highlevel_statuses=statuses,
        readback_status=readback_status,
        verified=verified,
        verification=verification,
    )


# Source imports are separately enabled and remain inside this bridge's boundary.
import sys
from ingestion import register_routes
ingestion_operations = register_routes(sys.modules[__name__])

# Existing bridge credential and location boundary, read-only evidence surface.
from journey_evidence import register_routes as register_evidence_routes
register_evidence_routes(sys.modules[__name__])
