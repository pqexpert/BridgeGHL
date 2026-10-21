"""Stage and execute one pinned RSC unit; does not replace hosted cadence."""
import hashlib,json,os,re,subprocess
from pathlib import Path

def remote(payload):
    if os.geteuid()!=0:raise ValueError("root_required")
    import sys
    sys.path.insert(0,"/opt/ziji-fabric/native")
    from collect_native_sources import atomic
    root=Path("/opt/ziji-fabric/actors/rsc")
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    if root.is_symlink():raise ValueError("unsafe_root")
    root.chmod(0o700)
    bundle=payload["bundle"]
    for name,value in bundle["files"].items():
        if name not in {"native_rsc.py","CURRENT-STATE.json","instructions.txt","pin.json","native_jira_receipt.py"}:raise ValueError("file_not_admitted")
        if hashlib.sha256(value.encode()).hexdigest()!=bundle["hashes"][name]:raise ValueError("pin_mismatch")
        destination=Path("/opt/ziji-fabric/native/native_jira_receipt.py") if name=="native_jira_receipt.py" else root/name
        atomic(destination,value)
        if destination.read_text()!=value:raise ValueError("source_readback")
    run=subprocess.run(["python3",str(root/"native_rsc.py")],capture_output=True,text=True,timeout=240)
    if run.returncode:raise ValueError("rsc_execution_failed_"+run.stdout[-200:])
    result=json.loads(run.stdout)
    result["stage"]="RSC_NATIVE_UNIT"
    result["cron_created"]=False
    # Candidate is public-safe source-derived CURRENT-STATE only, no model/feedback.
    private=json.loads(Path("/var/lib/ziji-fabric/actors/rsc/latest.json").read_text())
    result["native_readback"]=private["native_readback"]
    result["candidate"]=json.loads(Path(private["candidate_path"]).read_text()) if private.get("candidate_changed") else None
    return result

def main():
    user,host=os.environ["REMOTE_USER"],os.environ["REMOTE_HOST"]
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*",user) or not re.fullmatch(r"[A-Za-z0-9.:-]+",host):raise ValueError("ssh_target")
    payload={"source":Path(__file__).read_text(),"bundle":json.loads(Path(__file__).with_name("native_rsc_bundle.json").read_text())}
    command="sudo -n python3 -c 'import json,sys; p=json.load(sys.stdin); s={\"__name__\":\"deploy_rsc\"}; exec(compile(p[\"source\"],\"deploy_rsc\",\"exec\"),s); print(json.dumps(s[\"remote\"](p),sort_keys=True))'"
    args=["ssh","-i",str(Path.home()/".ssh/fabric-preflight"),"-o","BatchMode=yes","-o","ConnectTimeout=15","-o","IdentitiesOnly=yes","-o","StrictHostKeyChecking=yes","-o","UserKnownHostsFile="+str(Path.home()/".ssh/fabric-known-hosts"),user+"@"+host,command]
    run=subprocess.run(args,input=json.dumps(payload),text=True,capture_output=True,timeout=300)
    if run.returncode:raise ValueError("remote_native_rsc_failed")
    print(run.stdout.strip())
if __name__=="__main__":
    try:main()
    except Exception as exc:
        print(json.dumps({"stage":"RSC_NATIVE_UNIT","outcome":"FAIL","error":type(exc).__name__,"details_withheld":True}))
        raise SystemExit(1)
