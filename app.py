from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, EmailStr, Field
from typing import Optional, Literal
from datetime import datetime, timezone
import json
import os
import re
import uuid

import requests

app = FastAPI(title="BridgeGHL", version="0.2.0")

API_KEY = os.getenv("BRIDGE_API_KEY", "")
HIGHLEVEL_PIT = os.getenv("HIGHLEVEL_PIT", "")
HIGHLEVEL_LOCATION_ID = os.getenv("HIGHLEVEL_LOCATION_ID", "")
AUDIT_LOG_PATH = os.getenv("AUDIT_LOG_PATH", "/var/log/bridgeghl/audit.log")
HIGHLEVEL_BASE_URL = os.getenv("HIGHLEVEL_BASE_URL", "https://services.leadconnectorhq.com")

PHONE_CLEAN_RE = re.compile(r"[^0-9+]" )


class ContactUpsertRequest(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[EmailStr] = None
    phone: Optional[str] = None
    notes: Optional[str] = None
    source: Optional[str] = "bridgeghl"
    tags: list[str] = Field(default_factory=list)


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


def highlevel_endpoint() -> str:
    return f"{HIGHLEVEL_BASE_URL.rstrip('/')}/contacts/upsert"


def redacted_headers() -> dict:
    return {
        "Authorization": "Bearer ***redacted***",
        "Version": "2021-07-28",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def send_highlevel_contact_upsert(body: dict) -> tuple[int, dict]:
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

    response = requests.post(highlevel_endpoint(), headers=headers, json=body, timeout=30)
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


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "BridgeGHL",
        "write_surface": [
            "/dry-run/contact/upsert",
            "/execute/contact/upsert",
        ],
    }


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
            "url": highlevel_endpoint(),
            "headers": redacted_headers(),
            "json": outbound_body,
            "caller_ip": caller_ip(request),
        },
    )


@app.post("/execute/contact/upsert", response_model=ExecuteResponse)
def execute_contact_upsert(
    payload: ContactUpsertRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
):
    require_api_key(x_api_key)
    normalized = normalize_payload(payload)
    validation = validate_contact_payload(normalized)
    if not validation["valid"]:
        raise HTTPException(status_code=422, detail=validation)

    audit_id = str(uuid.uuid4())
    outbound_body = build_contact_upsert_request(normalized)
    status_code, response_json = send_highlevel_contact_upsert(outbound_body)

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
        "target": highlevel_endpoint(),
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
