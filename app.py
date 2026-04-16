from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import os

app = FastAPI(title="BridgeGHL", version="0.1.0")

API_KEY = os.getenv("BRIDGE_API_KEY", "")

class ContactUpsertRequest(BaseModel):
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    phone: str | None = None
    notes: str | None = None


def require_api_key(x_api_key: str | None):
    if not API_KEY:
        raise HTTPException(status_code=500, detail="BRIDGE_API_KEY is not configured")
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "BridgeGHL",
        "write_surface": ["/contacts/upsert"],
    }


@app.post("/contacts/upsert")
def contacts_upsert(payload: ContactUpsertRequest, x_api_key: str | None = Header(default=None)):
    require_api_key(x_api_key)
    # Phase 1 stub only. Replace with governed HighLevel API write logic.
    return {
        "accepted": True,
        "mode": "stub",
        "message": "Validated and accepted for controlled write-path testing.",
        "payload": payload.model_dump(),
    }
