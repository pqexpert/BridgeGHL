"""Stage a pinned Income unit via existing protected transport; sanitize public logs."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

PIN = "d3efba1815d1a3f67cfba2e986d9afbfa5532758"

def _server(payload):
    import sys
    import fcntl
    if os.geteuid()!=0:raise ValueError("root_required")
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
    cfg=payload["intake"]
    native.atomic(cfgdir/"intake.json",json.dumps(cfg,sort_keys=True))
    scope={"__name__":"native_income"}
    exec(compile(source,str(root/"career_unit.py"),"exec"),scope)
    lockfd=os.open(state/"unit.lock",os.O_CREAT|os.O_RDWR,0o600)
    with os.fdopen(lockfd,"w") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        report=scope["execute"](cfg)
        scope["atomic"](state/"last-report.json",report)
    receipt=report.get("receipt",{})
    return {"stage":"INCOME_NATIVE_UNIT","source_commit":PIN,"runtime_sha256":payload["sha256"],
        "intake_staged":True,"outcome":report.get("outcome"),"phase":report.get("phase"),
        "artifact_sha256":report.get("artifact_sha256"),
        "native_receipt_readback":receipt.get("native_readback",False),"jira_comment_id":receipt.get("comment_id"),
        "canonical_writeback":report.get("canonical_writeback","PENDING_HOSTED_BRIDGE"),
        "submitted":0,"contacted":0,"schedule_changed":False}

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
    payload={"source":here.read_text(),"runtime":runtime,"sha256":sha,"intake":json.loads(os.environ["INCOME_NATIVE_INTAKE_JSON"])}
    remote="sudo -n python3 -c 'import json,sys; p=json.load(sys.stdin); s={\"__name__\":\"income_deploy\"}; exec(compile(p[\"source\"],\"income_deploy\",\"exec\"),s); print(json.dumps(s[\"server\"](p),sort_keys=True))'"
    cmd=["ssh","-i",str(Path.home()/".ssh/fabric-preflight"),"-o","BatchMode=yes","-o","ConnectTimeout=15",
         "-o","IdentitiesOnly=yes","-o","StrictHostKeyChecking=yes","-o","UserKnownHostsFile="+str(Path.home()/".ssh/fabric-known-hosts"),user+"@"+host,remote]
    result=subprocess.run(cmd,input=json.dumps(payload),text=True,capture_output=True,timeout=300)
    if result.returncode:raise ValueError("remote_income_failure_details_withheld")
    report=json.loads(result.stdout)
    print(json.dumps(report,sort_keys=True))
    if report.get("outcome")!="SUCCESS" or not report.get("native_receipt_readback"):
        raise ValueError("income_effect_not_verified")
if __name__=="__main__":
    try:deploy()
    except Exception as exc:
        print(json.dumps({"stage":"INCOME_NATIVE_UNIT","outcome":"FAIL","error":str(exc) if isinstance(exc,ValueError) else type(exc).__name__,"private_details_withheld":True}))
        raise SystemExit(1)
