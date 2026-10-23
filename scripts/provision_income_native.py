"""Stage a pinned Income unit via existing protected transport; sanitize public logs."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

PIN = "9c85cb323c5b9e420f0bbd20bff0273f56ed8d9e"

def _server(payload):
    import sys
    import fcntl
    if os.geteuid()!=0:raise ValueError("root_required")
    if payload.get("mode") == "backlog-inspect":
        scope={"__name__":"backlog_inspection"}
        exec(compile(payload["backlog_reader"],"income_backlog.py","exec"),scope)
        return scope["inspect_manifest"]()
    if payload.get("mode") == "inspect":
        scope={"__name__":"income_inspection"}
        exec(compile(payload["inspector"],"income_inspection.py","exec"),scope)
        return scope["inspect"]()
    if payload.get("mode") in {"continuation", "continuation-refresh", "continuation-status", "continuation-wake"}:
        if hashlib.sha256(payload["installer"].encode()).hexdigest() != payload["continuation"]["hashes"]["installer"]:
            raise ValueError("installer_hash_mismatch")
        sys.path.insert(0,"/opt/ziji-fabric/native")
        scope={"__name__":"continuation_install"}
        exec(compile(payload["installer"],"install_income_continuation.py","exec"),scope)
        if payload["mode"] in {"continuation", "continuation-refresh"}: return scope["install"](payload)
        return scope["observe"](payload["continuation"], wake=payload["mode"] == "continuation-wake")
    source=payload["runtime"]
    if hashlib.sha256(source.encode()).hexdigest()!=payload["sha256"]:raise ValueError("runtime_hash")
    sys.path.insert(0,"/opt/ziji-fabric/native")
    import collect_native_sources as native
    root=Path("/opt/ziji-fabric/income")
    state=Path("/var/lib/ziji-fabric/income")
    cfgdir=Path("/etc/ziji-fabric/income")
    for path in (root,state,cfgdir):
        if path.is_symlink():raise ValueError("directory_symlink")
        path.mkdir(parents=True,exist_ok=True,mode=0o700)
        path.chmod(0o700)
    native.atomic(root/"career_unit.py",source)
    resume=payload.get("mode")=="resume"
    shared=Path("/etc/ziji-fabric/providers/google/credentials.json")
    shared_digest=hashlib.sha256(shared.read_bytes()).hexdigest() if resume else None
    if resume:
        inspector={"__name__":"income_candidate"}
        exec(compile(payload["inspector"],"income_inspection.py","exec"),inspector)
        candidate=inspector["candidate"](payload["credentials"])
        if candidate.get("outcome")!="READY_FOR_REVIEW":
            return {"stage":"INCOME_NATIVE_UNIT","outcome":"BLOCKED","candidate":candidate,"shared_binding_unchanged":True}
        from bind_native_providers import persist
        c=payload["credentials"]
        binding={"client_id":c["GOOGLE_CLIENT_ID"],"client_secret":c["GOOGLE_CLIENT_SECRET"],
                 "refresh_token":c["GOOGLE_REFRESH_TOKEN"],"expected_email":"restorationconsultantsoutreach@gmail.com"}
        installed=persist(Path("/etc/ziji-fabric/providers"),"income-google",binding)
        isolated=Path("/etc/ziji-fabric/providers/income-google/credentials.json")
        if json.loads(isolated.read_text())!=binding:raise ValueError("income_binding_readback")
        if hashlib.sha256(shared.read_bytes()).hexdigest()!=shared_digest:raise ValueError("shared_binding_changed")
        cfgpath=cfgdir/"intake.json"
        if cfgpath.is_symlink() or cfgpath.stat().st_mode & 0o077:raise ValueError("intake_private_required")
        cfg=json.loads(cfgpath.read_text())
    else:
        cfg=payload["intake"]
        native.atomic(cfgdir/"intake.json",json.dumps(cfg,sort_keys=True))
    scope={"__name__":"native_income"}
    exec(compile(source,str(root/"career_unit.py"),"exec"),scope)
    lockfd=os.open(state/"unit.lock",os.O_CREAT|os.O_RDWR,0o600)
    with os.fdopen(lockfd,"w") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        if resume:
            report=scope["resume_cached"](cfg,payload["canonical"])
        else:
            report=scope["execute"](cfg)
        scope["atomic"](state/"last-report.json",report)
    if resume and hashlib.sha256(shared.read_bytes()).hexdigest()!=shared_digest:raise ValueError("shared_binding_changed")
    receipt=report.get("receipt",{})
    return {"stage":"INCOME_NATIVE_UNIT","source_commit":PIN,"runtime_sha256":payload["sha256"],
        "intake_staged":True,"outcome":report.get("outcome"),"phase":report.get("phase"),
        "artifact_sha256":report.get("artifact_sha256"),
        "native_receipt_readback":receipt.get("native_readback",False),"jira_comment_id":receipt.get("comment_id"),
        "canonical_writeback":report.get("canonical_writeback","PENDING_HOSTED_BRIDGE"),
        "submitted":0,"contacted":0,"schedule_changed":False,
        "cached_resume":resume,"model_calls":0 if resume else None,
        "shared_binding_unchanged":True if resume else None,"income_binding":installed if resume else None}

def server(payload):
    try:
        return _server(payload)
    except Exception as exc:
        code = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
        if not re.fullmatch(r"[A-Za-z0-9_]{1,100}", code):
            code = type(exc).__name__
        return {"stage":"INCOME_NATIVE_UNIT", "outcome":"FAIL", "error_code":code,
                "private_details_withheld":True, "submitted":0, "contacted":0}

def deploy():
    user,host=os.environ["REMOTE_USER"],os.environ["REMOTE_HOST"]
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*",user) or not re.fullmatch(r"[A-Za-z0-9.:-]+",host):raise ValueError("ssh_target")
    here=Path(__file__)
    runtime=here.with_name("income_career_pinned.py").read_text()
    manifest=json.loads(here.with_name("income_career_pin.json").read_text())
    sha=hashlib.sha256(runtime.encode()).hexdigest()
    if manifest!={"source_commit":PIN,"sha256":sha}:raise ValueError("runtime_pin")
    mode=os.environ.get("INCOME_MODE","inspect")
    if mode not in {"backlog-inspect","inspect","execute","resume","continuation","continuation-refresh","continuation-status","continuation-wake"}:raise ValueError("invalid_mode")
    payload={"source":here.read_text(),"runtime":runtime,"sha256":sha,"mode":mode,
             "inspector":here.with_name("inspect_income_native.py").read_text()}
    if mode == "backlog-inspect": payload["backlog_reader"]=here.with_name("income_backlog.py").read_text()
    if mode.startswith("continuation"):
        payload["installer"]=here.with_name("install_income_continuation.py").read_text()
        payload["continuation"]=json.loads(here.with_name("income_continuation_bundle.json").read_text())
        if mode=="continuation": payload["queue"]=json.loads(os.environ["INCOME_CONTINUATION_QUEUE_JSON"])
        if mode in {"continuation","continuation-refresh"} and os.environ.get("INCOME_ADMISSION_CONFIG_JSON"):
            payload["admission_config"]=json.loads(os.environ["INCOME_ADMISSION_CONFIG_JSON"])
    if mode=="execute":payload["intake"]=json.loads(os.environ["INCOME_NATIVE_INTAKE_JSON"])
    if mode=="resume":
        payload["credentials"]={k:os.environ[k] for k in ("GOOGLE_CLIENT_ID","GOOGLE_CLIENT_SECRET","GOOGLE_REFRESH_TOKEN")}
        payload["canonical"]=json.loads(os.environ["INCOME_CANONICAL_REVALIDATION_JSON"])
    remote="sudo -n python3 -c 'import json,sys; p=json.load(sys.stdin); s={\"__name__\":\"income_deploy\"}; exec(compile(p[\"source\"],\"income_deploy\",\"exec\"),s); print(json.dumps(s[\"server\"](p),sort_keys=True))'"
    cmd=["ssh","-i",str(Path.home()/".ssh/fabric-preflight"),"-o","BatchMode=yes","-o","ConnectTimeout=15",
         "-o","IdentitiesOnly=yes","-o","StrictHostKeyChecking=yes","-o","UserKnownHostsFile="+str(Path.home()/".ssh/fabric-known-hosts"),user+"@"+host,remote]
    result=subprocess.run(cmd,input=json.dumps(payload),text=True,capture_output=True,timeout=300)
    if result.returncode:raise ValueError("remote_income_failure_details_withheld")
    report=json.loads(result.stdout)
    print(json.dumps(report,sort_keys=True))
    if mode in {"continuation-status", "continuation-wake"}:
        if report.get("stage") != "INCOME_CONTINUATION_STATUS": raise ValueError("continuation_status_failed")
        if mode == "continuation-wake" and report.get("wake_ok") is not True: raise ValueError("continuation_wake_not_verified")
        return
    if mode in {"continuation","continuation-refresh"}:
        if report.get("outcome")!="STAGED" or report.get("source_readback") is not True: raise ValueError("continuation_install_failed")
        return
    if mode in {"inspect","backlog-inspect"}:
        if report.get("read_only") is not True:raise ValueError("inspection_failed")
        return
    if report.get("outcome")!="SUCCESS" or not report.get("native_receipt_readback"):
        raise ValueError("income_effect_not_verified")
if __name__=="__main__":
    try:deploy()
    except Exception as exc:
        print(json.dumps({"stage":"INCOME_NATIVE_UNIT","outcome":"FAIL","error":str(exc) if isinstance(exc,ValueError) else type(exc).__name__,"private_details_withheld":True}))
        raise SystemExit(1)
