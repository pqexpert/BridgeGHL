"""Protected staging/activation for the existing Distiller writer."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
PIN="45525d014eede8ae3d2b446b278bd635d768815e"

def server(payload):
    if os.geteuid()!=0:raise ValueError("root_required")
    root=Path("/opt/ziji-fabric/calendar-writer")
    cron=Path("/etc/cron.d/ziji-fabric-calendar-writer")
    if root.is_symlink() or cron.is_symlink():raise ValueError("unsafe_install_path")
    root.mkdir(mode=0o700,parents=True,exist_ok=True);os.chmod(root,0o700)
    sys.path.insert(0,"/opt/ziji-fabric/native")
    from collect_native_sources import atomic
    for name,body in payload["files"].items():
        if name not in ("calendar_writer.py","calendar_handover.py","source_actions.py"):raise ValueError("invalid_package_file")
        if hashlib.sha256(body.encode()).hexdigest()!=payload["hashes"][name]:raise ValueError("package_digest")
        atomic(root/name,body)
    sys.path.insert(0,str(root))
    import calendar_writer as writer
    if payload["action"]=="deactivate":
        if cron.exists():cron.unlink()
        writer.save(writer.ACTIVATION,{"enabled":False,"hosted_writer_disabled":True})
        return {"stage":"CALENDAR_WRITER","status":"DEACTIVATED","calendar_writes":0}
    if payload["action"]=="activate":
        evidence=payload.get("hosted_disable_evidence",{})
        if evidence.get("id")!="6a92e1ad92888191a7a8d6ce4fcc1acc" or evidence.get("is_enabled") is not False:
            raise ValueError("hosted_disable_evidence_required")
        at=writer.handover.stamp(evidence["observed_at"])
        if abs((writer.utcnow()-at).total_seconds())>600:raise ValueError("hosted_disable_evidence_stale")
        writer.save(writer.ACTIVATION,{"enabled":True,"hosted_writer_disabled":True,"hosted_evidence":evidence,"source_commit":PIN})
        report=writer.run("FULL",write=True,accept_handover=True)
        if report.get("status") not in ("VERIFIED","NO_CHANGE"):raise ValueError("first_run_not_verified")
        expected="# Existing Distiller six ET slots; off-slot local checks make no API calls.\n0,15,45 * * * * root /usr/bin/python3 /opt/ziji-fabric/calendar-writer/calendar_writer.py --scheduled --write >/dev/null 2>&1\n"
        if cron.exists() and cron.read_text()!=expected:raise ValueError("cron_conflict_preserved")
        atomic(cron,expected,0o644)
    else:
        report=writer.run("FULL",write=False)
    # Full private report persists on host; only non-sensitive proof crosses back.
    return {"stage":"CALENDAR_WRITER","source_commit":PIN,"action":payload["action"],
        "status":report["status"],"mode":report["mode"],"observed_at":report["observed_at"],
        "coverage":report["coverage"],"calendar_writes":report["calendar_writes"],
        "eligible_instances":report.get("eligible_instances",0),"feedback_added":report["feedback_added"],
        "feedback_consumed":report["feedback_consumed"],"pending_feedback":report["pending_feedback"],
        "ambiguous_event_count":report["ambiguous_event_count"],"checkpoint":report.get("checkpoint"),
        "source_interpretation":report.get("source_interpretation",{}),
        "handover_baselines_adopted":report.get("handover_baselines_adopted",0),
        "model_status":report["model_status"],"last_good_full":report.get("last_good_full"),
        "cron_enabled":cron.exists(),"package_readback":all(hashlib.sha256((root/n).read_bytes()).hexdigest()==d for n,d in payload["hashes"].items()),
        "calendar_results":report["calendar_results"]}

def deploy():
    root=Path(__file__).parents[1]/"config/calendar-writer"
    manifest=json.loads((root/"pin.json").read_text())
    if manifest["source_commit"]!=PIN:raise ValueError("pin_mismatch")
    files={n:(root/n).read_text() for n in manifest["files"]}
    for name,body in files.items():
        if hashlib.sha256(body.encode()).hexdigest()!=manifest["files"][name]:raise ValueError("source_digest_mismatch")
    action=os.environ.get("ACTION","stage")
    if action not in ("stage","activate","deactivate"):raise ValueError("action_invalid")
    user,host=os.environ["REMOTE_USER"],os.environ["REMOTE_HOST"]
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*",user) or not re.fullmatch(r"[A-Za-z0-9.:-]+",host):raise ValueError("ssh_target_invalid")
    evidence=json.loads(os.environ.get("HOSTED_DISABLE_EVIDENCE") or "{}")
    payload={"source":Path(__file__).read_text(),"files":files,"hashes":manifest["files"],"action":action,"hosted_disable_evidence":evidence}
    remote="sudo -n python3 -c 'import json,sys; p=json.load(sys.stdin); s={\"__name__\":\"writer_deploy\"}; exec(compile(p[\"source\"],\"writer_deploy\",\"exec\"),s); print(json.dumps(s[\"server\"](p),sort_keys=True))'"
    cmd=["ssh","-i",str(Path.home()/".ssh/fabric-preflight"),"-o","BatchMode=yes","-o","ConnectTimeout=15",
         "-o","IdentitiesOnly=yes","-o","StrictHostKeyChecking=yes","-o","UserKnownHostsFile="+str(Path.home()/".ssh/fabric-known-hosts"),user+"@"+host,remote]
    result=subprocess.run(cmd,input=json.dumps(payload),text=True,capture_output=True,timeout=480)
    if result.returncode:
        # Return only a known local error code, never provider bodies or credentials.
        errors=re.findall(r"ValueError: ([A-Za-z0-9_]+)",result.stderr)
        raise ValueError(errors[-1] if errors else "remote_writer_failed")
    report=json.loads(result.stdout)
    if report.get("stage")!="CALENDAR_WRITER":raise ValueError("receipt_invalid")
    print(json.dumps(report,sort_keys=True))

if __name__=="__main__":
    try:deploy()
    except Exception as exc:
        print(json.dumps({"stage":"CALENDAR_WRITER","status":"FAIL","reason":str(exc) if isinstance(exc,ValueError) else type(exc).__name__}))
        raise SystemExit(1)
