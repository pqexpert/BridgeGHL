"""On-demand local status; no inference, external API, SSH, or service changes."""
from datetime import datetime,timezone
import json
import os
import re
from pathlib import Path
import stat
import subprocess
import urllib.request

STATE=Path.home()/".local/share/ziji-local"
UNITS=("ziji-local-model.service","ziji-local-tunnel.service","ziji-local-recovery.timer")
FIELDS=("ActiveState","SubState","NRestarts","ActiveEnterTimestamp","LastTriggerUSec","NextElapseUSecRealtime")

def stamp(path):
    return datetime.fromtimestamp(path.stat().st_mtime,timezone.utc).isoformat()

def private_json(path):
    if path.is_symlink() or path.stat().st_size>100000 or not path.is_file():raise ValueError()
    return json.loads(path.read_text())

def unit(name):
    try:
        result=subprocess.run(["systemctl","--user","show",name,"--no-pager","--property="+",".join(FIELDS)],capture_output=True,text=True,timeout=5)
        if result.returncode:return {"status":"UNKNOWN"}
        return {k:v for k,sep,v in (line.partition("=") for line in result.stdout.splitlines()) if sep and k in FIELDS}
    except (OSError,subprocess.TimeoutExpired):return {"status":"UNKNOWN"}

def health():
    try:
        path=STATE/"api-key";st=path.lstat()
        if not stat.S_ISREG(st.st_mode) or st.st_uid!=os.geteuid() or st.st_mode & 0o077 or st.st_size>10000:
            return "UNKNOWN_PRIVATE_KEY_REQUIRED"
        key=path.read_text().strip()
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self,*args,**kwargs):return None
        req=urllib.request.Request("http://127.0.0.1:18181/health",headers={"Authorization":"Bearer "+key})
        with urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect).open(req,timeout=3) as response:
            value=json.loads(response.read(4096))
            return "READY" if response.status==200 and value.get("status")=="ok" else "NOT_READY"
    except Exception:return "UNAVAILABLE"

def receipts():
    result={"coverage":"Local CLI proposal receipts only; excludes VPS-side job ledger","count":0}
    try:
        paths=[p for p in (STATE/"receipts").glob("*.json") if p.is_file() and not p.is_symlink()]
        result["count"]=len(paths)
        if paths:
            p=max(paths,key=lambda x:x.stat().st_mtime);value=private_json(p)
            result["latest_modified_utc"]=stamp(p)
            result["latest_mode"]="proposal_only" if value.get("mode")=="proposal_only" else "UNKNOWN"
            lane=value.get("lane")
            result["latest_lane"]=lane if lane in ("engineering","income","rsc","store","distiller") else "UNKNOWN"
    except (OSError,ValueError,TypeError):result["status"]="UNKNOWN"
    return result

def vps_receipts():
    result={"status":"UNAVAILABLE", "coverage":"Cached deployment receipts only; not live VPS state", "receipts":[]}
    try:
        path=STATE/"vps-worker-receipts.json";st=path.lstat()
        if st.st_uid!=os.geteuid() or st.st_mode & 0o077:raise ValueError()
        value=private_json(path)
        if value.get("schema")!="ziji-cached-vps-receipts/1" or not isinstance(value.get("receipts"),list):raise ValueError()
        for row in value["receipts"][:8]:
            if not isinstance(row,dict) or row.get("lane") not in ("income","rsc","store","distiller","dispatch"):continue
            observed=datetime.fromisoformat(row["observed_at"].replace("Z","+00:00"))
            if observed.tzinfo is None:raise ValueError()
            clean={"lane":row["lane"],"observed_at":observed.isoformat(),"evidence_state":"CACHED_VPS_RECEIPT"}
            for key,pattern in (("outcome",r"[A-Z_]{1,48}"),("action_class",r"[a-z0-9_]{1,80}"),
                                ("run_id",r"[0-9]{1,24}"),("source_sha",r"[0-9a-f]{7,64}"),
                                ("artifact_sha256",r"[0-9a-f]{64}")):
                if isinstance(row.get(key),str) and re.fullmatch(pattern,row[key]):clean[key]=row[key]
            for key in ("native_readback","whole_actor_replaced","paid_fallback"):
                if isinstance(row.get(key),bool):clean[key]=row[key]
            if type(row.get("model_calls")) is int and 0<=row["model_calls"]<=1000:clean["model_calls"]=row["model_calls"]
            result["receipts"].append(clean)
        result.update(status="CACHED_VPS_RECEIPT",cache_modified_utc=stamp(path))
    except (OSError,ValueError,TypeError,KeyError,AttributeError):
        result["status"]="UNAVAILABLE_OR_INVALID"
        result["receipts"]=[]
    return result

def main():
    recovery={"status":"UNKNOWN"}
    try:
        path=STATE/"recovery-upload.json";value=private_json(path)
        recovery={"status":"VERIFIED_ON_VPS" if value.get("verified") is True else "UNKNOWN", "receipt_modified_utc":stamp(path),
                  "github_backup":"Separate off-host receipt; not proved by this local file"}
    except (OSError,ValueError,TypeError):pass
    report={"observed_at":datetime.now(timezone.utc).isoformat(),"services":{u:unit(u) for u in UNITS},
            "local_health":health(),"proposal_receipts":receipts(),"recovery":recovery,"vps_receipts":vps_receipts(),
            "operator_pause":{"requested":(STATE/"transport/operator-pause.json").exists(),
                              "scope":"Local inference only; does not cancel remote provider effects"},
            "vps_live_check":"NOT_PERFORMED; active tunnel alone does not prove remote jobs or connectivity",
            "remote_evidence":"docs/fabric-cutover-execution.md (dated verified run links)",
            "work_state":"READY is availability, not a running job or autonomous mission",
            "this_refresh":{"model_calls":0,"external_api_calls":0,"ssh_calls":0,"service_changes":0}}
    print(json.dumps(report,indent=2))
if __name__=="__main__":main()
