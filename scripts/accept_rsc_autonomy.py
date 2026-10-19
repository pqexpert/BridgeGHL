"""Exercise the isolated HighLevel task envelope and persist a private receipt.

This acceptance uses only the existing DND/no-recipient ecosystem canary. It
creates one derived internal task, proves exact replay suppression, and removes
that same task through the governed bridge. It never sends or enrolls anything.
"""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app as bridge


ROOT = Path("/var/lib/bridgeghl/ecosystem")
SETUP_RECEIPT = ROOT / "receipt.json"
ACCEPTANCE_RECEIPT = ROOT / "rsc-autonomy-acceptance.json"
ACTION_KEY = (
    "launch17-next-action:v1:"
    "p8CNFbnxiaIi44O6mZq2:synthetic:autonomycanary:17"
)
DUE_DATE = "2099-01-01T12:00:00Z"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def bridge_post(path, payload):
    response = requests.post(
        "http://127.0.0.1:8000" + path,
        headers={"x-api-key": os.environ["BRIDGE_API_KEY"]},
        json=payload,
        timeout=170,
    )
    try:
        result = response.json()
    except ValueError as exc:
        raise RuntimeError("bridge returned a non-JSON response") from exc
    if response.status_code != 200:
        detail = result.get("detail", {}) if isinstance(result, dict) else {}
        code = detail.get("error") if isinstance(detail, dict) else None
        raise RuntimeError("bridge rejected acceptance: " + str(code or response.status_code))
    return result


def write_private(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def main():
    require(bridge.bridge_health_snapshot().state == "HEALTHY", "bridge is not healthy")
    require(bridge.TASK_WRITES_ENABLED, "task writes are not explicitly enabled")
    setup = json.loads(SETUP_RECEIPT.read_text())
    require(setup.get("verified") is True, "ecosystem setup receipt is not verified")
    require(setup.get("location_id") == bridge.HIGHLEVEL_LOCATION_ID, "location mismatch")
    contact_id = setup["canary"]["contact_id"]

    status, data = bridge.highlevel_request("GET", bridge.contact_endpoint(contact_id), timeout=10)
    contact = bridge.unwrap_record(data, "contact")
    require(status == 200, "canary contact read failed")
    require(bridge.contact_belongs_to_location(data, contact_id), "canary contact identity mismatch")
    require(contact.get("dnd") is True, "canary contact is not DND")
    require(not contact.get("email") and not contact.get("phone"), "canary has recipient data")

    payload = {
        "contact_id": contact_id,
        "action_key": ACTION_KEY,
        "due_date": DUE_DATE,
        "assigned_to": None,
        "reason": "Owner-authorized isolated RSC autonomy acceptance; no communication.",
    }
    dry_create = bridge_post("/dry-run/task/create", payload)
    require(dry_create.get("accepted") is True, "create dry-run rejected")
    first = bridge_post("/execute/task/create", payload)
    require(first.get("verified") is True, "first create not verified")
    require(first.get("effect") in {"CREATED", "NO_CHANGE_EXACT_REPLAY"}, "unexpected create effect")
    task_id = first.get("task_id")
    require(isinstance(task_id, str) and task_id, "verified create missing task ID")

    replay = bridge_post("/execute/task/create", payload)
    require(replay.get("verified") is True, "replay not verified")
    require(replay.get("effect") == "NO_CHANGE_EXACT_REPLAY", "replay created a duplicate")
    require(replay.get("task_id") == task_id, "replay resolved to a different task")

    cleanup = {
        "contact_id": contact_id,
        "task_id": task_id,
        "action_key": ACTION_KEY,
        "reason": "Clean up the exact isolated RSC autonomy canary task.",
    }
    dry_delete = bridge_post("/dry-run/task/delete", cleanup)
    require(dry_delete.get("accepted") is True, "delete dry-run rejected")
    deleted = bridge_post("/execute/task/delete", cleanup)
    require(deleted.get("verified") is True and deleted.get("effect") == "DELETED",
            "cleanup not verified")
    readback_status, _ = bridge.highlevel_request(
        "GET", bridge.contact_task_endpoint(contact_id, task_id), timeout=10
    )
    absence_verified = readback_status == 404
    if readback_status == 400:
        list_status, list_data = bridge.highlevel_request(
            "GET", bridge.contact_tasks_endpoint(contact_id), timeout=10
        )
        absence_verified = list_status == 200 and bridge.task_absent_from_complete_list(
            list_data, contact_id, task_id, ACTION_KEY
        )
    require(absence_verified, "deleted task absence not verified")

    receipt = {
        "contract": "rsc-command-center-plugin/v1",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "location_id": bridge.HIGHLEVEL_LOCATION_ID,
        "contact_id": contact_id,
        "action_key": ACTION_KEY,
        "task_id": task_id,
        "create_effect": first["effect"],
        "create_audit_id": first["audit_id"],
        "replay_effect": replay["effect"],
        "replay_audit_id": replay["audit_id"],
        "delete_effect": deleted["effect"],
        "delete_audit_id": deleted["audit_id"],
        "native_delete_readback_status": readback_status,
        "native_absence_verified": absence_verified,
        "messages_sent": 0,
        "verified": True,
    }
    write_private(ACCEPTANCE_RECEIPT, receipt)
    print(json.dumps({
        "rsc_autonomy_canary": "VERIFIED",
        "create_verified": True,
        "replay_suppressed": True,
        "cleanup_verified": True,
        "native_delete_readback": readback_status,
        "messages_sent": 0,
    }, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (KeyError, OSError, RuntimeError, requests.RequestException) as exc:
        print(json.dumps({"rsc_autonomy_canary": "BLOCKED", "error_type": type(exc).__name__}))
        raise SystemExit(1)
