"""Source identity observation from existing backup clones; no provider/model calls.

Daily observation is not source admission, deployed-pin freshness or actor wakeup.
Only fixed-path metadata is sent to the VPS, never source text or credentials.
"""
import base64
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

WATCHED = {
    "FungibleRD-MVP": (
        "AGENTS.md", "docs/llmlms/ZIJI-CONSTITUTIONAL-SOURCE-REGISTRY.json",
        "docs/llmlms/FABRIC-SOCIAL-CONTRACT.md", "docs/llmlms/SEMANTIC-GLOSSARY.md",
        "docs/llmlms/CONTROL-POINT-REGISTRY.md", "docs/llmlms/sops/OPERATIONS-GUIDE.md",
        "docs/llmlms/sops/SOP-001-FORWARD-RESOLUTION-AND-CONTEXT-INVALIDATION.md",
        "skills/wise-action-governor/SKILL.md"),
    "BridgeGHL": ("AGENTS.md", "PQEXPERT-ESTATE-OPERATING-POINTER.md",
                 "scripts/collect_native_sources.py", "scripts/watch_native_sources.py"),
    "rsc-operations-console": ("AGENTS.md", "server/native_rsc.py",
                              "web/highlevel-journeys/CURRENT-STATE.json"),
    "fabric-dispatch": ("AGENTS.md",),
    "fabric-distiller": ("AGENTS.md",),
    "income-accelerator": ("AGENTS.md",),
    "Dzokden-Store-Manager-Ziji": ("AGENTS.md",),
}
STATE = Path("/var/lib/ziji-fabric/native-sources/source-watch")
SHA = re.compile(r"[0-9a-f]{40}")
class SourceError(Exception):
    pass

def validate(observations):
    if not isinstance(observations, dict) or set(observations) != set(WATCHED):
        raise SourceError("source_repository_set_invalid")
    for name, item in observations.items():
        if (not isinstance(item, dict) or set(item) != {"repository", "observed_head", "files"} or
            item["repository"] != "pqexpert/" + name or
            not isinstance(item["observed_head"], str) or not SHA.fullmatch(item["observed_head"]) or
            not isinstance(item["files"], dict) or set(item["files"]) != set(WATCHED[name])):
            raise SourceError("source_identity_invalid")
        for blob in item["files"].values():
            if blob is not None and (not isinstance(blob, str) or not SHA.fullmatch(blob)):
                raise SourceError("source_blob_invalid")
    return observations

def gather(root, heads, run):
    """Read existing exact-head mirrors locally. Missing/deleted watched files are explicit."""
    observations = {}
    for name, paths in WATCHED.items():
        head = heads[name]
        if not isinstance(head, str) or not SHA.fullmatch(head):
            raise SourceError("source_head_invalid")
        raw = run(["git", "--git-dir=" + str(root / (name + ".git")),
                   "ls-tree", "-rz", head, "--", *paths])
        files = dict.fromkeys(paths)
        for entry in raw.decode().split("\0"):
            if not entry:
                continue
            meta, path = entry.split("\t", 1)
            mode, kind, sha = meta.split()
            if path not in files or kind != "blob" or mode not in ("100644", "100755"):
                raise SourceError("source_tree_entry_invalid")
            files[path] = sha
        observations[name] = {"repository": "pqexpert/" + name,
                              "observed_head": head, "files": files}
    return validate(observations)

def persist(observations, atomic, state=STATE):
    validate(observations)
    if state.is_symlink():
        raise SourceError("source_state_symlink")
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    state.chmod(0o700)
    current = state / "cursor.json"
    if current.is_symlink():
        raise SourceError("source_cursor_symlink")
    with (state / "run.lock").open("a") as lock:
        os.chmod(lock.name, 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX)
        previous = json.loads(current.read_text()) if current.exists() else None
        if previous is not None:
            validate(previous)
        pending_path = state / "pending.json"
        if pending_path.is_symlink():
            raise SourceError("source_pending_symlink")
        pending = json.loads(pending_path.read_text()) if pending_path.exists() else []
        if (not isinstance(pending, list) or any(not isinstance(key, str) or
                not re.fullmatch(r"[0-9a-f]{64}", key) for key in pending)):
            raise SourceError("source_pending_invalid")
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        changed, refs = [], []
        for name, item in observations.items():
            old = previous[name] if previous else None
            delta = [{"path": path, "previous_blob": old["files"][path], "observed_blob": blob}
                     for path, blob in item["files"].items() if old and old["files"][path] != blob]
            if not delta:
                continue
            identity = {"repository": item["repository"], "previous_observed_head": old["observed_head"],
                        "observed_upstream_head": item["observed_head"], "changed_paths": delta}
            key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
            target = state / (key + ".json")
            receipt = dict(identity, observed_at=now, disposition="PENDING_REVIEW_NOT_ADMITTED",
                           route="owning_next_useful_cycle_SOP_001", deployed_pins_changed=False,
                           policy_applied=False, actor_invocations=0, model_calls=0, jira_writes=0)
            # Receipt precedes cursor, so response loss reuses the stable transition.
            if target.is_symlink():
                raise SourceError("source_receipt_symlink")
            if target.exists():
                prior = json.loads(target.read_text())
                if any(prior.get(k) != v for k, v in identity.items()):
                    raise SourceError("source_receipt_identity_mismatch")
            else:
                atomic(target, json.dumps(receipt, sort_keys=True))
            if not target.exists() or any(json.loads(target.read_text()).get(k) != v for k, v in identity.items()):
                raise SourceError("source_receipt_readback_failed")
            if key not in pending:
                pending.append(key)
            refs.append(str(target))
            changed.append(item["repository"])
        atomic(pending_path, json.dumps(pending))
        atomic(current, json.dumps(observations, sort_keys=True))
        if json.loads(current.read_text()) != observations:
            raise SourceError("source_cursor_readback_failed")
        missing = {item["repository"]: [p for p, blob in item["files"].items() if blob is None]
                   for item in observations.values() if any(v is None for v in item["files"].values())}
        result = {"stage": "SOURCE_IDENTITY_OBSERVATION",
                  "status": "BASELINE_OBSERVED_NOT_ADMITTED" if previous is None else
                            ("CHANGES_PENDING_REVIEW" if changed else "NO_CHANGE"),
                  "observed_at": now, "cadence": "existing_daily_backup_07_43_UTC",
                  "repository_count": len(observations), "changed_repositories": changed,
                  "coverage": "UNKNOWN_MISSING_REQUIRED_SOURCES" if missing else "SELECTED_PATHS_COMPLETE",
                  "missing_paths": missing,
                  "receipt_paths": refs, "pending_review_count": len(pending), "pending_index": str(pending_path), "source_heads": {k:v["observed_head"] for k,v in observations.items()},
                  "deployed_pins_changed": False, "policy_applied": False, "actor_invocations": 0,
                  "canva_live_refresh": False, "model_calls": 0, "github_extra_calls": 0,
                  "jira_writes": 0, "new_observer_state_in_this_archive": False}
        atomic(state / "latest.json", json.dumps(result, sort_keys=True))
        if json.loads((state / "latest.json").read_text()) != result:
            raise SourceError("source_summary_readback_failed")
        return result

def send(root, heads, ssh, run):
    """One existing pinned SSH transport; no GitHub credential enters the payload."""
    observations = gather(root, heads, run)
    source = base64.b64encode(Path(__file__).read_bytes()).decode()
    runner = ("import base64,json,sys; sys.path.insert(0,'/opt/ziji-fabric/native'); "
              "from collect_native_sources import atomic; s={'__name__':'source_observer'}; "
              "exec(compile(base64.b64decode(" + repr(source) + "),'source_observer','exec'),s); "
              "print(json.dumps(s['persist'](json.load(sys.stdin),atomic),sort_keys=True))")
    result = subprocess.run(ssh + ["sudo -n python3 -c " + shlex.quote(runner)],
                            input=json.dumps(observations), text=True, capture_output=True, timeout=60)
    if result.returncode:
        raise SourceError("source_transport_failed")
    receipt = json.loads(result.stdout)
    if (receipt.get("stage") != "SOURCE_IDENTITY_OBSERVATION" or receipt.get("repository_count") != len(WATCHED) or
        receipt.get("source_heads") != {k: v["observed_head"] for k, v in observations.items()}):
        raise SourceError("source_transport_readback_failed")
    return receipt

def observe_after_backup(root, heads, ssh, run):
    """Nonfatal by design: observation never changes verified backup acceptance."""
    try:
        return send(root, heads, ssh, run)
    except Exception as exc:
        return {"stage": "SOURCE_IDENTITY_OBSERVATION", "status": "UNKNOWN",
                "reason": str(exc) if isinstance(exc, SourceError) else "source_observation_failed_details_withheld",
                "model_calls": 0, "github_extra_calls": 0, "jira_writes": 0,
                "backup_acceptance_unchanged": True}
