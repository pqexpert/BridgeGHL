"""Install reviewed continuation on the existing collector cadence; never execute a job here."""
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
from pathlib import Path


def validate_tracker(item, scope):
    intake = item["intake"]
    if intake.get("tracker_url", "").startswith("https://app.notion.com/p/"):
        return  # Preserve existing immutable historical identities and completed caches.
    ref = scope["highlevel_reference"](item.get("canonical",{}).get("highlevel"))
    if (intake.get("tracker_url") != scope["highlevel_identity"](ref)
            or intake.get("tracker_version") != ref["current_source_sha256"]):
        raise ValueError("canonical_tracker_scope")


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
    old_comment = "# Native read-only evidence; no model or provider writes."
    new_comment = "# Read-only source collection; admitted Income continuation may use local inference and write Drive/Jira evidence."
    cron_text = cron.read_text()
    expected_line = "11 * * * * root /usr/bin/python3 /opt/ziji-fabric/native/collect_native_sources.py --trigger cron >/dev/null 2>&1\n"
    if cron_text not in (old_comment+"\n"+expected_line, new_comment+"\n"+expected_line):
        raise ValueError("existing_cron_conflict_preserved")
    root = Path("/opt/ziji-fabric/dispatch/continuation")
    state = Path("/var/lib/ziji-fabric/dispatch/continuation")
    cfg = Path("/etc/ziji-fabric/dispatch")
    for path in (root, state, cfg):
        if path.is_symlink(): raise ValueError("unsafe_continuation_directory")
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.chmod(0o700)
    scope = {"__name__":"continuation_validation"}
    exec(compile(bundle["controller"], "income_continuation.py", "exec"), scope)
    refresh = payload.get("mode") == "continuation-refresh"
    queue = {"version":1,"items":[]} if refresh else payload["queue"]
    if queue.get("version") != 1 or not isinstance(queue.get("items"), list) or not (0 if refresh else 1) <= len(queue["items"]) <= 32:
        raise ValueError("invalid_queue")
    now = dt.datetime.now(dt.timezone.utc)
    for item in queue["items"]:
        if not scope["admitted"](item, now): raise ValueError("queue_admission_not_current")
        intake = item["intake"]
        if intake.get("drive_folder") != "1j_pa8A2sXBSA9vy87B6nyKo-myZ3HDaE": raise ValueError("career_folder_scope")
        validate_tracker(item, scope)
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
            if refresh and not destination.is_file():raise ValueError("existing_queue_required")
            previous = scope["read"](destination) if destination.exists() else {"version":1,"items":[]}
            items = {scope["identity"](e["intake"]):e for e in previous["items"]}
            items.update({scope["identity"](e["intake"]):e for e in queue["items"]})
            if len(items) > 32: raise ValueError("queue_capacity")
            if cron.read_text() != cron_text:
                raise ValueError("existing_cron_conflict_preserved")
            if collector.is_symlink() or hashlib.sha256(collector.read_bytes()).hexdigest() not in (bundle["collector_previous_sha256"],bundle["hashes"]["collector"]):
                raise ValueError("collector_changed_preserved")
            admission_config = payload.get("admission_config")
            config_state = "UNCHANGED"
            admission_path = cfg / "income-admission.json"
            if admission_config is not None:
                expected_fields = {"enabled","issuer_account_id","authority_ref","authority_comment_updated","drive_folder"}
                if (not isinstance(admission_config,dict) or set(admission_config) != expected_fields
                        or admission_config["enabled"] is not True
                        or not isinstance(admission_config["issuer_account_id"],str)
                        or not 1 <= len(admission_config["issuer_account_id"]) <= 200
                        or admission_config["authority_ref"] != "RBO-97:12086"
                        or admission_config["drive_folder"] != "1j_pa8A2sXBSA9vy87B6nyKo-myZ3HDaE"):
                    raise ValueError("admission_config_scope")
                try:
                    # Jira's exact version string is retained; only parsing adapts to Python 3.9.
                    parse_stamp = admission_config["authority_comment_updated"].replace("Z","+00:00")
                    parse_stamp = re.sub(r"([+-]\d{2})(\d{2})$",r"\1:\2",parse_stamp)
                    stamp = dt.datetime.fromisoformat(parse_stamp)
                    if stamp.tzinfo is None:raise ValueError("aware_timestamp_required")
                except (ValueError,TypeError,AttributeError):
                    raise ValueError("admission_config_authority_version") from None
                scope["validate_policy"](admission_config)
                if admission_path.is_symlink():raise ValueError("admission_config_conflict_preserved")
                if admission_path.exists():
                    if admission_path.stat().st_uid != 0 or scope["read"](admission_path) != admission_config:
                        raise ValueError("admission_config_conflict_preserved")
                    config_state = "IDENTICAL"
                else:
                    atomic(admission_path,json.dumps(admission_config,sort_keys=True))
                    if scope["read"](admission_path) != admission_config:raise ValueError("admission_config_readback")
                    config_state = "INITIALIZED"
            sources = {root / "income_continuation.py":bundle["controller"],
                       Path("/opt/ziji-fabric/income/career_unit.py"):bundle["runtime"],
                       collector:bundle["collector"]}
            for path, source in sources.items():
                if path.is_symlink(): raise ValueError("source_symlink")
                atomic(path, source)
                if path.read_text() != source: raise ValueError("continuation_source_readback")
            atomic(cron, new_comment+"\n"+expected_line, 0o644)
            if not refresh:
                atomic(destination, json.dumps({"version":1,"items":list(items.values())},sort_keys=True))
            verified = scope["read"](destination)
            if len(verified["items"]) != len(items): raise ValueError("queue_readback")
            receipt = {"stage":"INCOME_CONTINUATION_INSTALL","outcome":"STAGED",
                       "source_readback":True,"admission_config":config_state,"queue_unchanged":refresh,"queue_items":len(items),"source_pins":bundle["pins"],
                       "installed_at":now.isoformat(),"existing_cadence":"hourly_at_minute_11",
                       "new_scheduler":False,"model_calls":0,"jobs_executed":0,
                       "paid_fallback":False,"daily_model_limit":1,
                       "pause_preserved":(state / "pause").exists()}
            atomic(state / "installation.json",json.dumps(receipt,sort_keys=True))
            return receipt


def observe(bundle, wake=False):
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
    installed = load(state / "installation.json")
    files = {"controller":"/opt/ziji-fabric/dispatch/continuation/income_continuation.py",
             "runtime":"/opt/ziji-fabric/income/career_unit.py",
             "collector":"/opt/ziji-fabric/native/collect_native_sources.py"}
    hashes = {}
    for name, filename in files.items():
        path = Path(filename)
        if path.is_file() and not path.is_symlink() and path.stat().st_uid == 0 and not path.stat().st_mode & 0o022:
            hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    validated = bool(installed) and all(hashes.get(name) == bundle["hashes"][name] for name in files)
    caller = load(state / "caller-latest.json")
    if wake:
        if validated:
            caller = continuation(timeout=240) or {"status":"WORKER_MISSING"}
        else:
            caller = {"status":"INSTALLATION_NOT_VALIDATED"}
    ledger = load(state / "state.json")
    latest = load(state / "latest.json")
    queue = load(Path("/etc/ziji-fabric/dispatch/income-queue.json"))
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    units = ledger.get("units", {})
    queue_ids = {hashlib.sha256(json.dumps([entry["intake"][k] for k in
                 ("tracker_url","packet_id","job_id")],separators=(",",":")).encode()).hexdigest()
                 for entry in queue.get("items", [])}
    current = False
    if caller.get("status") == "COMPLETED" and caller.get("exit_code") == 0:
        try:
            stamps = [dt.datetime.fromisoformat(value.replace("Z","+00:00")) for value in
                      (caller["started_at"],latest["observed_at"],caller["observed_at"])]
            current = (all(stamp.tzinfo is not None for stamp in stamps) and stamps[0] <= stamps[1] <= stamps[2]
                       and caller.get("result_sha256") == hashlib.sha256(json.dumps(latest,sort_keys=True).encode()).hexdigest())
        except (KeyError,ValueError,TypeError):
            pass
    if not current:
        latest = {"status":caller.get("status","UNKNOWN"),"reason":"current_pass_evidence_unavailable"}
        if caller.get("status") == "COMPLETED":latest["status"] = "STALE_EVIDENCE"
    receipt = latest.get("receipt", {})
    return {"stage":"INCOME_CONTINUATION_STATUS", "read_only":not wake,
            "wake_requested":wake, "observed_at":dt.datetime.now(dt.timezone.utc).isoformat(),
            "paused":(state / "pause").exists(), "configured":bool(installed), "installed":validated,
            "current_pass_verified":current, "wake_ok":current if wake else None,
            "source_pins":installed.get("source_pins"), "installed_sha256":hashes,
            "cron_description":Path("/etc/cron.d/ziji-fabric-native-sources").read_text().splitlines()[0],
            "queue_items":len(queue.get("items", [])),
            "completed_units":sum(x.get("status") == "VERIFIED" for x in units.values()),
            "completed_receipts":[{"unit":key,"artifact_sha256":unit.get("result",{}).get("artifact_sha256"),
                "jira_comment_id":unit.get("result",{}).get("receipt",{}).get("comment_id"),
                "native_readback":unit.get("result",{}).get("receipt",{}).get("native_readback",False),
                "canonical_writeback":unit.get("result",{}).get("canonical_writeback")}
                for key,unit in units.items() if unit.get("status")=="VERIFIED"],
            "unfinished_units":sum(units.get(key,{}).get("status") != "VERIFIED" for key in queue_ids),
            "daily_model_budget_reserved":today in ledger.get("model_days", {}),
            "latest":{k:latest[k] for k in ("status","reason","phase","observed_at",
                       "artifact_sha256","canonical_writeback") if k in latest},
            "caller":{k:caller[k] for k in ("status","exit_code","started_at","observed_at") if k in caller},
            "jira_comment_id":receipt.get("comment_id"),
            "native_receipt_readback":receipt.get("native_readback", False),
            "paid_fallback":False, "submitted":0, "contacted":0}
