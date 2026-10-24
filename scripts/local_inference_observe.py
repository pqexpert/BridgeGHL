#!/usr/bin/env python3
"""Metadata-only, on-demand local inference observations. Never invokes a model."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import time
import urllib.error
import urllib.request

HOME = Path.home()
STATE = HOME / ".local/share/ziji-local/observability"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def command(argv):
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=8)
        return p.returncode, p.stdout
    except (OSError, subprocess.TimeoutExpired):
        return -1, ""


def service(name, user=False):
    args = ["systemctl"] + (["--user"] if user else [])
    rc, output = command(args + ["show", name, "--no-pager",
        "--property=ActiveState,SubState,NRestarts,MainPID"])
    return dict(line.split("=", 1) for line in output.splitlines() if "=" in line) if rc == 0 else {"ActiveState": "unknown"}


def health(port, folder=None):
    try:
        headers = {}
        if folder:
            p = HOME / ".local/share" / folder / "api-key"
            s = p.lstat()
            if not stat.S_ISREG(s.st_mode) or s.st_uid != os.geteuid() or s.st_mode & 0o077 or s.st_size > 10000:
                return "PRIVATE_KEY_INVALID"
            headers["Authorization"] = "Bearer " + p.read_text().strip()
        path = "/health" if folder else "/api/health"
        req = urllib.request.Request("http://127.0.0.1:%d%s" % (port, path), headers=headers)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        with opener.open(req, timeout=3) as r:
            raw = r.read(8193)
            if len(raw) > 8192:
                return "INVALID_RESPONSE"
            return "READY" if r.status == 200 and json.loads(raw).get("status") == "ok" else "NOT_READY"
    except Exception:
        return "UNAVAILABLE"


def gateway_access():
    """Check transport exposure and anonymous rejection without performing inference."""
    rc, output = command(["ss", "-H", "-ltn"])
    listeners = []
    if rc == 0:
        for line in output.splitlines():
            fields = line.split()
            if len(fields) >= 4 and fields[3].endswith(":20128"):
                listeners.append(fields[3])
    status = "UNKNOWN"
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        with opener.open("http://127.0.0.1:20128/v1/models", timeout=3) as response:
            status = response.status
    except urllib.error.HTTPError as exc:
        status = exc.code
    except Exception:
        pass
    return {"anonymous_models_http_status": status, "listeners": listeners,
        "loopback_only": bool(listeners) and all(a in ("127.0.0.1:20128", "[::1]:20128") for a in listeners)}


def omniroute():
    # Node's modern SQLite can read this schema; the system Python SQLite cannot.
    nodes = sorted((HOME / ".nvm/versions/node").glob("v*/bin/node"))
    candidates = list(reversed(nodes)) + ([Path(shutil.which("node"))] if shutil.which("node") else [])
    script = r'''
const {DatabaseSync}=require('node:sqlite');
const db=new DatabaseSync(process.argv[1],{readOnly:true});
const connections=db.prepare('SELECT id,provider,auth_type,is_active FROM provider_connections').all();
const providers=connections.map(({id,...p})=>p);
const expected={'duck-proposal':'http://127.0.0.1:18181/v1','duck-coder':'http://127.0.0.1:18081/v1'};
const nodes=db.prepare('SELECT type,prefix,base_url FROM provider_nodes').all().map(n=>{
  let loopback=false;try{const u=new URL(n.base_url);loopback=u.protocol==='http:'&&['127.0.0.1','localhost','[::1]'].includes(u.hostname)}catch{}
  return {type:n.type,loopback,reviewed_route:expected[n.prefix]===n.base_url&&n.type==='openai-compatible'};
});
const row=db.prepare("SELECT value FROM key_value WHERE namespace='settings' AND key='call_log_pipeline_enabled'").get();
const key=db.prepare("SELECT model_access_mode,allowed_models,allowed_connections,allowed_combos,allow_auto_combos,no_log,scopes,is_active FROM api_keys WHERE name='Duck local-only inference'").get();
if(key){try{const allowed=JSON.parse(key.allowed_connections);const expectedIds=connections.filter(c=>c.is_active&&['openai-compatible-chat-duck-proposal','openai-compatible-chat-duck-coder'].includes(c.provider)).map(c=>c.id);key.connections_verified=Array.isArray(allowed)&&allowed.length===2&&new Set(allowed).size===2&&expectedIds.length===2&&allowed.every(id=>expectedIds.includes(id));}catch{key.connections_verified=false}}
console.log(JSON.stringify({status:'READ_ONLY_SNAPSHOT',providers,nodes,call_log_pipeline_enabled:row?['1','true'].includes(row.value):null,
local_only_key_policy:key||null,
recorded_usage_rows:db.prepare('SELECT count(*) AS n FROM usage_history').get().n,
recorded_call_rows:db.prepare('SELECT count(*) AS n FROM call_logs').get().n}));db.close();
'''
    db = HOME / ".omniroute/storage.sqlite"
    try:
        s = db.lstat()
        if not stat.S_ISREG(s.st_mode) or s.st_uid != os.geteuid() or s.st_mode & 0o077:
            return {"status": "PRIVATE_DATABASE_REQUIRED"}
    except OSError:
        return {"status": "UNAVAILABLE"}
    for node in candidates:
        rc, out = command([str(node), "--disable-warning=ExperimentalWarning", "-e", script, str(db)])
        if rc == 0:
            try:
                return json.loads(out)
            except ValueError:
                pass
    return {"status": "UNKNOWN_SQLITE_CLIENT_UNAVAILABLE"}


def observe():
    report = {"schema": "ziji-local-observation/1", "event_type": "local_inference_observation",
        "observed_at": datetime.now(timezone.utc).isoformat(), "model_calls": 0,
        "external_api_calls": 0, "anomalies": []}
    anomalies = report["anomalies"]
    report["models"] = {"proposal": health(18181, "ziji-local"), "coder": health(18081, "zijifabric-local")}
    for name, state in report["models"].items():
        if state != "READY":
            anomalies.append("LOCAL_MODEL_" + name.upper() + "_UNAVAILABLE")
    report["services"] = {n: service(n, True) for n in ("ziji-local-model.service", "ziji-local-llm.service", "ziji-local-tunnel.service", "ziji-omniroute.service")}
    report["wazuh_services"] = {n: service(n) for n in ("wazuh-manager", "wazuh-indexer", "wazuh-dashboard")}
    report["wazuh_alerts"] = "NOT_INSPECTED_REQUIRES_AUTHORIZED_ADMIN_ACCESS"
    for name, value in {**report["services"], **report["wazuh_services"]}.items():
        if value.get("ActiveState") != "active":
            anomalies.append("SERVICE_NOT_ACTIVE:" + name)
    memory = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in ("MemAvailable", "MemTotal", "SwapFree", "SwapTotal"):
            memory[key + "_KiB"] = int(value.split()[0])
    report["memory"] = memory
    if memory.get("MemAvailable_KiB", 0) < 2 * 1024 * 1024:
        anomalies.append("LOW_AVAILABLE_RAM")
    if memory.get("SwapTotal_KiB", 0) and memory.get("SwapFree_KiB", 0) < 1024 * 1024:
        anomalies.append("LOW_FREE_SWAP_ALLOCATION_IS_NOT_PAGING_RATE")
    report["omniroute_health"] = health(20128)
    report["gateway_access"] = gateway_access()
    if report["gateway_access"]["anonymous_models_http_status"] not in (401, 403):
        anomalies.append("OMNIROUTE_ANONYMOUS_REJECTION_UNVERIFIED")
    if not report["gateway_access"]["loopback_only"]:
        anomalies.append("OMNIROUTE_LOOPBACK_LISTENER_UNVERIFIED")
    report["omniroute"] = omniroute()
    omni = report["omniroute"]
    if omni.get("status") != "READ_ONLY_SNAPSHOT":
        anomalies.append("OMNIROUTE_ROUTING_UNKNOWN")
    else:
        if any(p.get("is_active") and p.get("provider") == "codex" for p in omni["providers"]):
            anomalies.append("OMNIROUTE_CLOUD_CODEX_ENABLED")
        if any(p.get("is_active") and p.get("provider") not in (
                "openai-compatible-chat-duck-proposal", "openai-compatible-chat-duck-coder") for p in omni["providers"]):
            anomalies.append("OMNIROUTE_ACTIVE_PROVIDER_OUTSIDE_REVIEWED_LOCAL_SET")
        if not omni["nodes"]:
            anomalies.append("OMNIROUTE_HAS_NO_CUSTOM_LOCAL_NODES")
        if any(not n.get("loopback") for n in omni["nodes"]):
            anomalies.append("OMNIROUTE_CUSTOM_NODE_OUTSIDE_LOOPBACK")
        if any(not n.get("reviewed_route") for n in omni["nodes"]):
            anomalies.append("OMNIROUTE_CUSTOM_ROUTE_CHANGED")
        if not local_key_restricted(omni.get("local_only_key_policy")):
            anomalies.append("OMNIROUTE_LOCAL_KEY_POLICY_UNVERIFIED")
        report["request_content_logging"] = "DISABLED; metadata observer provides visibility without raw prompts" if not omni.get("call_log_pipeline_enabled") else "ENABLED_IN_OMNIROUTE_REVIEW_PRIVACY"
    if report["omniroute_health"] != "READY":
        anomalies.append("OMNIROUTE_UNAVAILABLE")
    # Never print journal messages: they may contain prompts, paths or credentials.
    rc, output = command(["journalctl", "_SYSTEMD_USER_UNIT=ziji-local-model.service", "_SYSTEMD_USER_UNIT=ziji-local-llm.service",
        "--since", "1 hour ago", "--no-pager", "-o", "json", "-n", "200"])
    entries = []
    for line in output.splitlines():
        try:
            value = json.loads(line)
            if isinstance(value, dict) and "MESSAGE" in value:
                entries.append(value)
        except ValueError:
            pass
    warnings = sum(str(e.get("PRIORITY", "6")) in ("0", "1", "2", "3", "4") for e in entries)
    report["journal"] = {"status": "READABLE" if rc == 0 and entries else "NO_VISIBLE_ENTRIES",
        "sample_limit": 200, "visible_entries": len(entries), "warning_or_higher": warnings,
        "coverage": "Accessible model service entries in the last hour; absence is not proof of healthy logs"}
    if not entries:
        anomalies.append("MODEL_JOURNAL_NO_VISIBLE_ENTRIES")
    if warnings:
        anomalies.append("MODEL_JOURNAL_WARNINGS_PRESENT")
    report["anomaly_count"] = len(anomalies)
    report["billing"] = "NOT_CHECKED; local gateway address does not imply local model execution"
    return report


def local_key_restricted(policy):
    """Deny unknown or broadened policy; do not treat a friendly key name as evidence."""
    try:
        return (policy["model_access_mode"] == "restricted"
            and policy["connections_verified"] is True
            and policy["is_active"] == 1 and policy["allow_auto_combos"] == 0
            and policy["no_log"] == 1 and json.loads(policy["scopes"]) == []
            and json.loads(policy["allowed_combos"]) == []
            and set(json.loads(policy["allowed_models"])) == {
                "duck-proposal/ziji-local", "duck-coder/ziji-local-coder"}
            and len(set(json.loads(policy["allowed_connections"]))) == 2)
    except (KeyError, TypeError, ValueError):
        return False


def record(report):
    STATE.mkdir(mode=0o700, parents=True, exist_ok=True)
    s = STATE.lstat()
    if not stat.S_ISDIR(s.st_mode) or s.st_uid != os.geteuid() or s.st_mode & 0o077:
        raise ValueError("Private observability directory required")
    target = STATE / "events.jsonl"
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW
    fd = os.open(str(target), flags, 0o600)
    with os.fdopen(fd, "a") as stream:
        s = os.fstat(stream.fileno())
        if not stat.S_ISREG(s.st_mode) or s.st_uid != os.geteuid() or s.st_mode & 0o077 or s.st_nlink != 1:
            raise ValueError("Private regular event file required")
        # Bounded metadata stream; no backups containing raw inference content.
        if s.st_size > 1024 * 1024:
            stream.seek(0)
            stream.truncate()
        stream.write(json.dumps(report, separators=(",", ":")) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watch", action="store_true", help="Refresh every 30 seconds until Ctrl-C")
    parser.add_argument("--record", action="store_true", help="Write bounded private metadata JSONL for optional Wazuh ingestion")
    args = parser.parse_args()
    while True:
        report = observe()
        if args.record:
            record(report)
        print(json.dumps(report, indent=2), flush=True)
        if not args.watch:
            break
        time.sleep(30)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
