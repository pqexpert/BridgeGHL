from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, EmailStr, Field
from typing import Any, Optional, Literal
from datetime import datetime, timezone
import json
import os
import re
import uuid

import requests

app = FastAPI(title="BridgeGHL", version="0.3.0")

API_KEY = os.getenv("BRIDGE_API_KEY", "")
HIGHLEVEL_PIT = os.getenv("HIGHLEVEL_PIT", "")
HIGHLEVEL_LOCATION_ID = os.getenv("HIGHLEVEL_LOCATION_ID", "")
AUDIT_LOG_PATH = os.getenv("AUDIT_LOG_PATH", "/var/log/bridgeghl/audit.log")
HIGHLEVEL_BASE_URL = os.getenv("HIGHLEVEL_BASE_URL", "https://services.leadconnectorhq.com")
LIVE_WRITE_ENABLED = os.getenv("LIVE_WRITE_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
HEALTHCHECK_TIMEOUT_SECONDS = float(os.getenv("HEALTHCHECK_TIMEOUT_SECONDS", "5"))

PHONE_CLEAN_RE = re.compile(r"[^0-9+]")
ALLOWED_OPPORTUNITY_FIELDS = {
    "Environment Stage",
    "Evidence Tier Available",
    "Commercial Language Allowed",
    "Next Technical Proof Needed",
    "Blocker / Risk Note",
    "Next Follow-Up Date",
    "Opportunity Owner",
    "Primary Offer Type",
    "Technical Readiness Summary",
    "Telephony Lane",
}
ALLOWED_ACTIONS = {
    "contact_upsert",
    "update_opportunity",
    "set_stage",
    "set_custom_fields",
    "normalize_tags",
    "assign_owner",
    "set_next_follow_up_date",
}


class ContactUpsertRequest(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[EmailStr] = None
    phone: Optional[str] = None
    notes: Optional[str] = None
    source: Optional[str] = "bridgeghl"
    tags: list[str] = Field(default_factory=list)


class OpportunityChanges(BaseModel):
    stage_id: Optional[str] = None
    fields: dict[str, Any] = Field(default_factory=dict)
    tags_add: list[str] = Field(default_factory=list)
    tags_remove: list[str] = Field(default_factory=list)
    owner_id: Optional[str] = None
    next_follow_up_date: Optional[str] = None


class OpportunityUpdateRequest(BaseModel):
    opportunity_id: str
    changes: OpportunityChanges
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
    highlevel_status: int
    highlevel_response: dict


class BridgeHealthResponse(BaseModel):
    ok: bool
    state: Literal["HEALTHY", "DEGRADED", "UNCONFIGURED", "ERROR"]
    service: str
    live_write_enabled: bool
    configured: dict
    allowed_actions: list[str]
    last_checked_at: str
    checks: list[dict]


def require_api_key(x_api_key: Optional[str]):
    if not API_KEY:
        raise HTTPException(status_code=500, detail="BRIDGE_API_KEY is not configured")
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


def normalize_phone(phone: Optional[str]) -> Optional[str]:
    if not phone:
        return None
    cleaned = PHONE_CLEAN_RE.sub("", phone.strip())
    return cleaned or None


def normalize_payload(payload: ContactUpsertRequest) -> ContactUpsertRequest:
    tags = sorted({tag.strip() for tag in payload.tags if tag and tag.strip()})
    return ContactUpsertRequest(
        first_name=payload.first_name.strip() if payload.first_name else None,
        last_name=payload.last_name.strip() if payload.last_name else None,
        email=payload.email,
        phone=normalize_phone(payload.phone),
        notes=payload.notes.strip() if payload.notes else None,
        source=payload.source.strip() if payload.source else "bridgeghl",
        tags=tags,
    )


def normalize_opportunity_changes(changes: OpportunityChanges) -> OpportunityChanges:
    normalized_fields = {}
    for key, value in changes.fields.items():
        if isinstance(key, str) and key.strip():
            normalized_fields[key.strip()] = value
    return OpportunityChanges(
        stage_id=changes.stage_id.strip() if changes.stage_id else None,
        fields=normalized_fields,
        tags_add=sorted({t.strip() for t in changes.tags_add if t and t.strip()}),
        tags_remove=sorted({t.strip() for t in changes.tags_remove if t and t.strip()}),
        owner_id=changes.owner_id.strip() if changes.owner_id else None,
        next_follow_up_date=changes.next_follow_up_date.strip() if changes.next_follow_up_date else None,
    )


def validate_contact_payload(payload: ContactUpsertRequest) -> dict:
    errors = []
    if not payload.email and not payload.phone:
        errors.append("At least one identifier is required: email or phone")
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "has_email": payload.email is not None,
        "has_phone": payload.phone is not None,
    }


def validate_opportunity_request(payload: OpportunityUpdateRequest) -> dict:
    errors = []
    warnings = []
    blocked_fields = [name for name in payload.changes.fields if name not in ALLOWED_OPPORTUNITY_FIELDS]
    if not payload.opportunity_id.strip():
        errors.append("opportunity_id is required")
    if not payload.reason.strip():
        errors.append("reason is required")
    if blocked_fields:
        errors.append(f"field not allowed: {', '.join(blocked_fields)}")
    if not any([
        payload.changes.stage_id,
        payload.changes.fields,
        payload.changes.tags_add,
        payload.changes.tags_remove,
        payload.changes.owner_id,
        payload.changes.next_follow_up_date,
    ]):
        errors.append("at least one allowed change is required")
    if payload.changes.tags_add and payload.changes.tags_remove:
        overlap = sorted(set(payload.changes.tags_add).intersection(payload.changes.tags_remove))
        if overlap:
            warnings.append(f"same tag both added and removed: {', '.join(overlap)}")
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "blocked_fields": blocked_fields,
        "allowed_actions": sorted(ALLOWED_ACTIONS),
    }


def build_contact_upsert_request(payload: ContactUpsertRequest) -> dict:
    body = {
        "locationId": HIGHLEVEL_LOCATION_ID,
        "firstName": payload.first_name,
        "lastName": payload.last_name,
        "email": str(payload.email) if payload.email else None,
        "phone": payload.phone,
        "tags": payload.tags,
        "source": payload.source,
        "customFields": [],
    }
    if payload.notes:
        body["customFields"].append({"key": "notes", "field_value": payload.notes})
    return body


def build_opportunity_update_request(payload: OpportunityUpdateRequest) -> dict:
    return {
        "locationId": HIGHLEVEL_LOCATION_ID,
        "opportunityId": payload.opportunity_id,
        "stageId": payload.changes.stage_id,
        "customFields": [
            {"key": key, "field_value": value}
            for key, value in payload.changes.fields.items()
        ],
        "tagsAdd": payload.changes.tags_add,
        "tagsRemove": payload.changes.tags_remove,
        "ownerId": payload.changes.owner_id,
        "nextFollowUpDate": payload.changes.next_follow_up_date,
        "reason": payload.reason,
    }


def contact_upsert_endpoint() -> str:
    return f"{HIGHLEVEL_BASE_URL.rstrip('/')}/contacts/upsert"


def opportunity_update_endpoint() -> str:
    return f"{HIGHLEVEL_BASE_URL.rstrip('/')}/opportunities/upsert"


def redacted_headers() -> dict:
    return {
        "Authorization": "Bearer ***redacted***",
        "Version": "2021-07-28",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def send_highlevel_request(url: str, body: dict) -> tuple[int, dict]:
    if not HIGHLEVEL_PIT:
        raise HTTPException(status_code=500, detail="HIGHLEVEL_PIT is not configured")
    if not HIGHLEVEL_LOCATION_ID:
        raise HTTPException(status_code=500, detail="HIGHLEVEL_LOCATION_ID is not configured")

    headers = {
        "Authorization": f"Bearer {HIGHLEVEL_PIT}",
        "Version": "2021-07-28",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    response = requests.post(url, headers=headers, json=body, timeout=30)
    try:
        data = response.json()
    except ValueError:
        data = {"raw_text": response.text}
    return response.status_code, data


def ensure_audit_dir():
    audit_dir = os.path.dirname(AUDIT_LOG_PATH)
    if audit_dir:
        os.makedirs(audit_dir, exist_ok=True)


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


def bridge_health_snapshot() -> BridgeHealthResponse:
    checks = []
    configured = {
        "bridge_api_key": bool(API_KEY),
        "highlevel_pit": bool(HIGHLEVEL_PIT),
        "highlevel_location_id": bool(HIGHLEVEL_LOCATION_ID),
        "highlevel_base_url": bool(HIGHLEVEL_BASE_URL),
        "audit_log_path": bool(AUDIT_LOG_PATH),
    }

    if not API_KEY:
        checks.append({"name": "bridge_api_key", "ok": False, "detail": "BRIDGE_API_KEY missing"})
    else:
        checks.append({"name": "bridge_api_key", "ok": True, "detail": "configured"})

    if not HIGHLEVEL_PIT:
        checks.append({"name": "highlevel_pit", "ok": False, "detail": "HIGHLEVEL_PIT missing"})
    else:
        checks.append({"name": "highlevel_pit", "ok": True, "detail": "configured"})

    if not HIGHLEVEL_LOCATION_ID:
        checks.append({"name": "highlevel_location_id", "ok": False, "detail": "HIGHLEVEL_LOCATION_ID missing"})
    else:
        checks.append({"name": "highlevel_location_id", "ok": True, "detail": HIGHLEVEL_LOCATION_ID})

    if LIVE_WRITE_ENABLED and HIGHLEVEL_PIT and HIGHLEVEL_LOCATION_ID:
        try:
            response = requests.get(
                f"{HIGHLEVEL_BASE_URL.rstrip('/')}/locations/{HIGHLEVEL_LOCATION_ID}",
                headers={
                    "Authorization": f"Bearer {HIGHLEVEL_PIT}",
                    "Version": "2021-07-28",
                    "Accept": "application/json",
                },
                timeout=HEALTHCHECK_TIMEOUT_SECONDS,
            )
            checks.append({
                "name": "highlevel_reachability",
                "ok": 200 <= response.status_code < 300,
                "detail": f"status={response.status_code}",
            })
        except requests.RequestException as exc:
            checks.append({"name": "highlevel_reachability", "ok": False, "detail": str(exc)})
    else:
        checks.append({
            "name": "highlevel_reachability",
            "ok": False,
            "detail": "skipped until live write is enabled and config is complete",
        })

    if not configured["bridge_api_key"] or not configured["highlevel_base_url"]:
        state = "UNCONFIGURED"
        ok = False
    elif not configured["highlevel_pit"] or not configured["highlevel_location_id"]:
        state = "UNCONFIGURED"
        ok = False
    else:
        reachability_check = next((check for check in checks if check["name"] == "highlevel_reachability"), None)
        if LIVE_WRITE_ENABLED and reachability_check and not reachability_check["ok"]:
            state = "ERROR"
            ok = False
        elif LIVE_WRITE_ENABLED:
            state = "HEALTHY"
            ok = True
        else:
            state = "DEGRADED"
            ok = False

    return BridgeHealthResponse(
        ok=ok,
        state=state,
        service="BridgeGHL",
        live_write_enabled=LIVE_WRITE_ENABLED,
        configured=configured,
        allowed_actions=sorted(ALLOWED_ACTIONS),
        last_checked_at=datetime.now(timezone.utc).isoformat(),
        checks=checks,
    )


def execute_guard(action: str):
    health = bridge_health_snapshot()
    if action not in ALLOWED_ACTIONS:
        raise HTTPException(status_code=400, detail={"error": "action_not_allowed", "action": action})
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
def contacts_upsert_compat():
    return {
        "deprecated": True,
        "use": ["/dry-run/contact/upsert", "/execute/contact/upsert"],
    }


@app.post("/dry-run/contact/upsert", response_model=DryRunResponse)
def dry_run_contact_upsert(
    payload: ContactUpsertRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
):
    require_api_key(x_api_key)
    normalized = normalize_payload(payload)
    validation = validate_contact_payload(normalized)
    if not validation["valid"]:
        raise HTTPException(status_code=422, detail=validation)

    outbound_body = build_contact_upsert_request(normalized)
    return DryRunResponse(
        accepted=True,
        mode="dry_run",
        action="contact_upsert",
        validation=validation,
        outbound_request={
            "method": "POST",
            "url": contact_upsert_endpoint(),
            "headers": redacted_headers(),
            "json": outbound_body,
            "caller_ip": caller_ip(request),
            "bridge_state": bridge_health_snapshot().state,
        },
    )


@app.post("/execute/contact/upsert", response_model=ExecuteResponse)
def execute_contact_upsert(
    payload: ContactUpsertRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
):
    require_api_key(x_api_key)
    execute_guard("contact_upsert")
    normalized = normalize_payload(payload)
    validation = validate_contact_payload(normalized)
    if not validation["valid"]:
        raise HTTPException(status_code=422, detail=validation)

    audit_id = str(uuid.uuid4())
    outbound_body = build_contact_upsert_request(normalized)
    status_code, response_json = send_highlevel_request(contact_upsert_endpoint(), outbound_body)

    audit_entry = {
        "audit_id": audit_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": "contact_upsert",
        "mode": "execute",
        "caller_ip": caller_ip(request),
        "request_summary": {
            "email": str(normalized.email) if normalized.email else None,
            "phone": normalized.phone,
        },
        "target": contact_upsert_endpoint(),
        "http_status": status_code,
        "result": "success" if 200 <= status_code < 300 else "failure",
    }
    append_audit_log(audit_entry)

    return ExecuteResponse(
        accepted=200 <= status_code < 300,
        mode="execute",
        action="contact_upsert",
        audit_id=audit_id,
        highlevel_status=status_code,
        highlevel_response=response_json,
    )


@app.post("/dry-run/opportunity/update", response_model=DryRunResponse)
def dry_run_opportunity_update(
    payload: OpportunityUpdateRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
):
    require_api_key(x_api_key)
    normalized_changes = normalize_opportunity_changes(payload.changes)
    normalized_payload = OpportunityUpdateRequest(
        opportunity_id=payload.opportunity_id.strip(),
        changes=normalized_changes,
        reason=payload.reason.strip(),
    )
    validation = validate_opportunity_request(normalized_payload)
    if not validation["valid"]:
        raise HTTPException(status_code=422, detail=validation)

    outbound_body = build_opportunity_update_request(normalized_payload)
    return DryRunResponse(
        accepted=True,
        mode="dry_run",
        action="update_opportunity",
        validation=validation,
        outbound_request={
            "method": "POST",
            "url": opportunity_update_endpoint(),
            "headers": redacted_headers(),
            "json": outbound_body,
            "caller_ip": caller_ip(request),
            "bridge_state": bridge_health_snapshot().state,
            "rollback_notes": [
                "capture current stage before execution",
                "capture current tags before execution",
                "capture affected custom field values before execution",
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
    normalized_changes = normalize_opportunity_changes(payload.changes)
    normalized_payload = OpportunityUpdateRequest(
        opportunity_id=payload.opportunity_id.strip(),
        changes=normalized_changes,
        reason=payload.reason.strip(),
    )
    validation = validate_opportunity_request(normalized_payload)
    if not validation["valid"]:
        raise HTTPException(status_code=422, detail=validation)

    audit_id = str(uuid.uuid4())
    outbound_body = build_opportunity_update_request(normalized_payload)
    status_code, response_json = send_highlevel_request(opportunity_update_endpoint(), outbound_body)

    audit_entry = {
        "audit_id": audit_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": "update_opportunity",
        "mode": "execute",
        "caller_ip": caller_ip(request),
        "request_summary": {
            "opportunity_id": normalized_payload.opportunity_id,
            "stage_id": normalized_payload.changes.stage_id,
            "owner_id": normalized_payload.changes.owner_id,
            "fields": list(normalized_payload.changes.fields.keys()),
            "tags_add": normalized_payload.changes.tags_add,
            "tags_remove": normalized_payload.changes.tags_remove,
            "next_follow_up_date": normalized_payload.changes.next_follow_up_date,
        },
        "target": opportunity_update_endpoint(),
        "http_status": status_code,
        "result": "success" if 200 <= status_code < 300 else "failure",
    }
    append_audit_log(audit_entry)

    return ExecuteResponse(
        accepted=200 <= status_code < 300,
        mode="execute",
        action="update_opportunity",
        audit_id=audit_id,
        highlevel_status=status_code,
        highlevel_response=response_json,
    )
