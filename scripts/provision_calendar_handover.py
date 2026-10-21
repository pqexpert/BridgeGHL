"""Deploy the pinned read-only Calendar adapter using the existing protected SSH route."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

PIN="7e22ab5447ec7d285e969edb8d52d46c679f0e4a"

def server(payload):
    import sys
    from datetime import datetime, timezone
    if os.geteuid()!=0:
        raise ValueError("root_required")
    sys.path.insert(0,"/opt/ziji-fabric/native")
    import collect_native_sources as native
    adapter=payload["adapter"]
    if hashlib.sha256(adapter.encode()).hexdigest()!=payload["sha256"]:
        raise ValueError("adapter_digest_mismatch")
    scope={"__name__":"calendar_handover"}
    exec(compile(adapter,"calendar_handover.py","exec"),scope)
    root=Path("/opt/ziji-fabric/native")
    state=Path("/var/lib/ziji-fabric/native-sources")
    if root.is_symlink() or state.is_symlink():
        raise ValueError("unsafe_directory")
    native.atomic(root/"calendar_handover.py",adapter)
    cfg=native.config("google")
    calendar_id="7bf3f127fbe4bc97b1cf9d7ae76a8570e679df6c4914a69b12b3bb093d656f78@group.calendar.google.com"
    if cfg.get("calendar_id")!=calendar_id or cfg.get("expected_email")!="restorationconsultantsoutreach@gmail.com":
        raise ValueError("calendar_binding_mismatch")
    binding={"calendar_id":calendar_id,"expected_email":cfg["expected_email"],"writable_series":{
        "daily":"7vuan7f09u4t7lviduqkq64osc","income1":"8ap9mopddmperjvhdbl4bloevc",
        "income2":"sibhg1ho7sli8j3khcos4ipjus","secondary":"63nkn19ao4d8opjnhcr5sb98fs"}}
    native.atomic(Path("/etc/ziji-fabric/providers/google/calendar-handover.json"),json.dumps(binding))
    # Reuse a fresh snapshot. A stale cache triggers only the Calendar collector.
    current=scope["private_read"](state/"calendar.json")
    now=datetime.now(timezone.utc)
    fresh=(now-scope["stamp"](current["observed_at"])).total_seconds() <= 7200
    if not fresh:
        current=native.calendar_snapshot(cfg,now)
        current["observed_at"]=now.isoformat()
        native.atomic(state/"calendar-previous.json",(state/"calendar.json").read_text())
        native.atomic(state/"calendar.json",json.dumps(current,sort_keys=True))
    import fcntl
    with (state/"calendar-feedback-ledger.json.lock").open("a") as lock:
        os.chmod(lock.name,0o600)
        fcntl.flock(lock,fcntl.LOCK_EX)
        previous=scope["private_read"](state/"calendar-previous.json") if (state/"calendar-previous.json").exists() else None
        ledgerpath=state/"calendar-feedback-ledger.json"
        ledger=scope["private_read"](ledgerpath) if ledgerpath.exists() else {}
        report,ledger=scope["compare"](current,previous,binding,ledger,now)
        report.update({"adapter_source_commit":PIN,"adapter_sha256":payload["sha256"],"snapshot_reused":fresh})
        scope["private_write"](ledgerpath,ledger)
        scope["private_write"](state/"calendar-handover-report.json",report)
    public={k:v for k,v in report.items() if k not in {"blocked_instances","protected_changes"}}
    public["blocked_instances_count"]=len(report["blocked_instances"])
    public["protected_changes_count"]=len(report["protected_changes"])
    from collections import Counter
    public["blocked_reason_counts"]=dict(Counter(x["reason"] for x in report["blocked_instances"]))
    shapes=[]
    for event in current.get("events",[]):
        if event.get("recurringEventId") not in binding["writable_series"].values():
            continue
        desc=event.get("description","")
        shapes.append({"start_token_count":desc.count("FABRIC CURRENT NOTE"),
            "end_token_count":desc.count("END FABRIC CURRENT NOTE"),
            "human_marker_count":desc.count("PRESERVED CONTEXT AND RESPONSE"),
            "starts_with_start":desc.startswith("FABRIC CURRENT NOTE"),
            "exact_marker_lines":[line.strip() for line in desc.splitlines() if line.strip() in {"FABRIC CURRENT NOTE","END FABRIC CURRENT NOTE","PRESERVED CONTEXT AND RESPONSE"}]})
    public["marker_shapes"]=shapes
    public["stage"]="CALENDAR_HANDOVER_READ_ONLY"
    public["adapter_readback_verified"]=hashlib.sha256((root/"calendar_handover.py").read_bytes()).hexdigest()==payload["sha256"]
    return public

def deploy():
    user,host=os.environ["REMOTE_USER"],os.environ["REMOTE_HOST"]
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*",user) or not re.fullmatch(r"[A-Za-z0-9.:-]+",host):
        raise ValueError("ssh_target_invalid")
    source=Path(__file__)
    adapter=source.with_name("calendar_handover_pinned.py").read_text()
    manifest=json.loads(source.with_name("calendar_handover_pin.json").read_text())
    sha=hashlib.sha256(adapter.encode()).hexdigest()
    if manifest!={"source_commit":PIN,"sha256":sha}:
        raise ValueError("pinned_source_mismatch")
    payload={"source":source.read_text(),"adapter":adapter,"sha256":sha}
    remote="sudo -n python3 -c 'import json,sys; p=json.load(sys.stdin); s={\"__name__\":\"remote_handover\"}; exec(compile(p[\"source\"],\"handover_deploy\",\"exec\"),s); print(json.dumps(s[\"server\"](p),sort_keys=True))'"
    cmd=["ssh","-i",str(Path.home()/".ssh/fabric-preflight"),"-o","BatchMode=yes","-o","ConnectTimeout=15",
         "-o","IdentitiesOnly=yes","-o","StrictHostKeyChecking=yes","-o","UserKnownHostsFile="+str(Path.home()/".ssh/fabric-known-hosts"),user+"@"+host,remote]
    run=subprocess.run(cmd,input=json.dumps(payload),text=True,capture_output=True,timeout=180)
    if run.returncode:
        raise ValueError("remote_handover_failed")
    report=json.loads(run.stdout)
    if report.get("stage")!="CALENDAR_HANDOVER_READ_ONLY" or report.get("calendar_writes")!=0:
        raise ValueError("invalid_receipt")
    print(json.dumps(report,sort_keys=True))

if __name__=="__main__":
    try:
        deploy()
    except Exception:
        print('{"stage":"CALENDAR_HANDOVER_READ_ONLY","status":"failed_details_withheld","calendar_writer_enabled":false}')
        raise SystemExit(1)
