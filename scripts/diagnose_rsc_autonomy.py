"""Read-only reconciliation of the isolated canary; public output has no IDs/PII."""
import json
from collections import deque
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app as bridge
from scripts.accept_rsc_autonomy import ACTION_KEY, DUE_DATE, SETUP_RECEIPT


def inspect():
    result = {"mode": "read_only", "mutations": 0}
    setup = json.loads(SETUP_RECEIPT.read_text())
    result["setup_verified"] = setup.get("verified") is True
    result["location_matches"] = setup.get("location_id") == bridge.HIGHLEVEL_LOCATION_ID
    if not result["setup_verified"] or not result["location_matches"]:
        return result
    cid = setup["canary"]["contact_id"]
    status, data = bridge.highlevel_request("GET", bridge.contact_endpoint(cid), timeout=10)
    result["contact_status"] = status
    result["identity_matches"] = status == 200 and bridge.contact_belongs_to_location(data, cid)
    if not result["identity_matches"]:
        return result
    contact = bridge.unwrap_record(data, "contact")
    result["isolated"] = contact.get("dnd") is True and not contact.get("email") and not contact.get("phone")
    if not result["isolated"]:
        return result
    status, data = bridge.highlevel_request("GET", bridge.contact_tasks_endpoint(cid), timeout=10)
    result["task_list_status"] = status
    if status != 200:
        return result
    matches = [t for t in bridge.unwrap_task_list(data) if t.get("title") == bridge.task_title(ACTION_KEY)]
    result["exact_task_count"] = len(matches)
    if len(matches) == 1 and matches[0].get("id"):
        status, data = bridge.highlevel_request("GET", bridge.contact_task_endpoint(cid, matches[0]["id"]), timeout=10)
        result["task_read_status"] = status
        payload = bridge.TaskCreateRequest(contact_id=cid, action_key=ACTION_KEY, due_date=DUE_DATE, reason="Read-only reconciliation")
        if status == 200:
            result["verification"] = bridge.verify_task(bridge.project_task(data), payload)
    # Only fixed bridge-owned check fields and booleans cross into public CI.
    audit = Path(bridge.AUDIT_LOG_PATH)
    if audit.is_file():
        with audit.open() as stream:
            tail = deque(stream, maxlen=5000)
        matching = []
        for line in tail:
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get("action_key") == ACTION_KEY:
                matching.append(row)
        result["audit_tail_matches"] = len(matching)
        result["audit_intents"] = sum(r.get("phase") == "intent" for r in matching)
        result["audit_finals"] = sum(r.get("phase") == "final" for r in matching)
        if matching:
            row = matching[-1]
            result["last_audit_verified"] = row.get("verified") is True
            result["last_audit_is_create"] = row.get("action") == "create_task"
            result["last_audit_is_delete"] = row.get("action") == "delete_task"
            result["last_audit_unresolved"] = row.get("effect") == "UNRESOLVED"
    return result


if __name__ == "__main__":
    try:
        print(json.dumps(inspect(), sort_keys=True))
    except Exception:
        print('{"mode":"read_only","diagnostic_complete":false,"mutations":0}')
        raise SystemExit(1)
