"""Install reviewed continuation on the existing collector cadence; never execute a job here."""
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path


def install(payload):
    if os.geteuid() != 0: raise ValueError("root_required")
    from collect_native_sources import atomic
    bundle = payload["continuation"]
    for name in ("controller", "collector", "runtime"):
        if hashlib.sha256(bundle[name].encode()).hexdigest() != bundle["hashes"][name]:
            raise ValueError("continuation_hash_mismatch")
    cron = Path("/etc/cron.d/ziji-fabric-native-sources")
    if not cron.is_file() or "11 * * * * root /usr/bin/python3 /opt/ziji-fabric/native/collect_native_sources.py --trigger cron" not in cron.read_text():
        raise ValueError("existing_hourly_cadence_required")
    root = Path("/opt/ziji-fabric/dispatch/continuation")
    state = Path("/var/lib/ziji-fabric/dispatch/continuation")
    cfg = Path("/etc/ziji-fabric/dispatch")
    for path in (root, state, cfg):
        if path.is_symlink(): raise ValueError("unsafe_continuation_directory")
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.chmod(0o700)
    scope = {"__name__":"continuation_validation"}
    exec(compile(bundle["controller"], "income_continuation.py", "exec"), scope)
    queue = payload["queue"]
    if queue.get("version") != 1 or not isinstance(queue.get("items"), list) or not 1 <= len(queue["items"]) <= 32:
        raise ValueError("invalid_queue")
    now = dt.datetime.now(dt.timezone.utc)
    for item in queue["items"]:
        if not scope["admitted"](item, now): raise ValueError("queue_admission_not_current")
        intake = item["intake"]
        if intake.get("drive_folder") != "1j_pa8A2sXBSA9vy87B6nyKo-myZ3HDaE": raise ValueError("career_folder_scope")
        if not intake.get("tracker_url", "").startswith("https://app.notion.com/p/"): raise ValueError("canonical_tracker_scope")
    destination = cfg / "income-queue.json"
    collector = Path("/opt/ziji-fabric/native/collect_native_sources.py")
    if collector.is_symlink() or hashlib.sha256(collector.read_bytes()).hexdigest() not in (bundle["collector_previous_sha256"],bundle["hashes"]["collector"]):
        raise ValueError("collector_changed_preserved")
    # Serialize source/queue installation against both current and legacy execution.
    with (state / "controller.lock").open("a") as control:
        control_path = state / "controller.lock"; control_path.chmod(0o600)
        fcntl.flock(control, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with Path("/var/lib/ziji-fabric/income/unit.lock").open("a") as native:
            fcntl.flock(native, fcntl.LOCK_EX | fcntl.LOCK_NB)
            previous = scope["read"](destination) if destination.exists() else {"version":1,"items":[]}
            items = {scope["identity"](e["intake"]):e for e in previous["items"]}
            items.update({scope["identity"](e["intake"]):e for e in queue["items"]})
            if len(items) > 32: raise ValueError("queue_capacity")
            sources = {root / "income_continuation.py":bundle["controller"],
                       Path("/opt/ziji-fabric/income/career_unit.py"):bundle["runtime"],
                       collector:bundle["collector"]}
            for path, source in sources.items():
                if path.is_symlink(): raise ValueError("source_symlink")
                atomic(path, source)
                if path.read_text() != source: raise ValueError("continuation_source_readback")
            atomic(destination, json.dumps({"version":1,"items":list(items.values())},sort_keys=True))
            verified = scope["read"](destination)
            if len(verified["items"]) != len(items): raise ValueError("queue_readback")
            receipt = {"stage":"INCOME_CONTINUATION_INSTALL","outcome":"STAGED",
                       "source_readback":True,"queue_items":len(items),"source_pins":bundle["pins"],
                       "installed_at":now.isoformat(),"existing_cadence":"hourly_at_minute_11",
                       "new_scheduler":False,"model_calls":0,"jobs_executed":0,
                       "paid_fallback":False,"daily_model_limit":1,
                       "pause_preserved":(state / "pause").exists()}
            atomic(state / "installation.json",json.dumps(receipt,sort_keys=True))
            return receipt


def observe(wake=False):
    """Pure local status; wake explicitly runs one admitted useful controller pass."""
    if os.geteuid() != 0: raise ValueError("root_required")
    from collect_native_sources import continuation
    state = Path("/var/lib/ziji-fabric/dispatch/continuation")
    def load(path):
        if not path.exists(): return {}
        if path.is_symlink() or path.stat().st_uid != 0 or path.stat().st_mode & 0o077 or path.stat().st_size > 1048576:
            raise ValueError("private_status_file_required")
        value = json.loads(path.read_text())
        if not isinstance(value, dict): raise ValueError("status_object_required")
        return value
    if wake:
        if not (state / "installation.json").is_file(): raise ValueError("continuation_not_installed")
        continuation(timeout=240)
    installed = load(state / "installation.json")
    ledger = load(state / "state.json")
    latest = load(state / "latest.json")
    caller = load(state / "caller-latest.json")
    queue = load(Path("/etc/ziji-fabric/dispatch/income-queue.json"))
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    units = ledger.get("units", {})
    receipt = latest.get("receipt", {})
    files = {"controller":"/opt/ziji-fabric/dispatch/continuation/income_continuation.py",
             "runtime":"/opt/ziji-fabric/income/career_unit.py",
             "collector":"/opt/ziji-fabric/native/collect_native_sources.py"}
    hashes = {}
    for name, filename in files.items():
        path = Path(filename)
        if path.exists():
            if path.is_symlink() or path.stat().st_uid != 0 or path.stat().st_mode & 0o022:
                raise ValueError("installed_source_permissions")
            hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {"stage":"INCOME_CONTINUATION_STATUS", "read_only":not wake,
            "wake_requested":wake, "observed_at":dt.datetime.now(dt.timezone.utc).isoformat(),
            "paused":(state / "pause").exists(), "installed":bool(installed),
            "source_pins":installed.get("source_pins"), "installed_sha256":hashes,
            "queue_items":len(queue.get("items", [])),
            "completed_units":sum(x.get("status") == "VERIFIED" for x in units.values()),
            "completed_receipts":[{"unit":key,"artifact_sha256":unit.get("result",{}).get("artifact_sha256"),
                "jira_comment_id":unit.get("result",{}).get("receipt",{}).get("comment_id"),
                "native_readback":unit.get("result",{}).get("receipt",{}).get("native_readback",False),
                "canonical_writeback":unit.get("result",{}).get("canonical_writeback")}
                for key,unit in units.items() if unit.get("status")=="VERIFIED"],
            "unfinished_units":sum(x.get("status") != "VERIFIED" for x in units.values()),
            "daily_model_budget_reserved":today in ledger.get("model_days", {}),
            "latest":{k:latest[k] for k in ("status","reason","phase","observed_at",
                       "artifact_sha256","canonical_writeback") if k in latest},
            "caller":{k:caller[k] for k in ("status","exit_code","observed_at") if k in caller},
            "jira_comment_id":receipt.get("comment_id"),
            "native_receipt_readback":receipt.get("native_readback", False),
            "paid_fallback":False, "submitted":0, "contacted":0}
