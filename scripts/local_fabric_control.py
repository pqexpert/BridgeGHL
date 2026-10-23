"""Local inference pause/resume only; no remote authority or provider effects."""
import argparse, datetime, fcntl, json, os
from pathlib import Path
import subprocess
import local_fabric_status as status

STATE=Path.home()/".local/share/ziji-local/transport"
MARKER=STATE/"operator-pause.json"
UNITS=("ziji-local-tunnel.service","ziji-local-model.service")
DROPINS=Path.home()/".config/systemd/user"
CONTENT="[Unit]\nConditionPathExists=!%h/.local/share/ziji-local/transport/operator-pause.json\n"

def command(*args):
    p=subprocess.run(["systemctl","--user",*args],capture_output=True,text=True,timeout=30)
    if p.returncode:raise RuntimeError("systemctl_"+args[0]+"_failed")
    return p.stdout.strip()

def write_private(path,value):
    if path.is_symlink():raise RuntimeError("path_conflict")
    temporary=path.with_name(path.name+".tmp-"+os.urandom(8).hex())
    fd=os.open(str(temporary),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    try:
        with os.fdopen(fd,"w") as f:json.dump(value,f,sort_keys=True);f.flush();os.fsync(f.fileno())
        os.replace(temporary,path)
    finally:
        if temporary.exists():temporary.unlink()

def install_conditions():
    paths=[]
    for unit in UNITS:
        directory=DROPINS/(unit+".d")
        path=directory/"90-ziji-operator-pause.conf"
        if directory.is_symlink() or path.is_symlink():raise RuntimeError("dropin_conflict")
        if path.exists() and path.read_text()!=CONTENT:raise RuntimeError("dropin_conflict")
        paths.append(path)
    for path in paths:
        path.parent.mkdir(parents=True,exist_ok=True)
        if not path.exists():
            fd=os.open(str(path),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            with os.fdopen(fd,"w") as f:f.write(CONTENT)
    command("daemon-reload")

def read_pause():
    if MARKER.is_symlink():raise RuntimeError("pause_marker_conflict")
    if not MARKER.exists():return None
    if MARKER.stat().st_size>4096 or MARKER.stat().st_uid!=os.geteuid() or MARKER.stat().st_mode & 0o077:
        raise RuntimeError("pause_marker_permissions")
    value=json.loads(MARKER.read_text())
    if value.get("schema")!="ziji-local-pause/1" or not isinstance(value.get("resume_units"),list) or any(x not in UNITS for x in value["resume_units"]):raise RuntimeError("pause_marker_invalid")
    return value

def operate(action):
    STATE.mkdir(mode=0o700,parents=True,exist_ok=True)
    fd=os.open(str(STATE/"operator.lock"),os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,"w") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        paused=read_pause()
        if action=="pause":
            install_conditions() # Conflict fails before any unit or marker mutation.
            if paused is None:
                active=[u for u in UNITS if command("show",u,"--property=ActiveState","--value") in ("active","activating")]
                paused={"schema":"ziji-local-pause/1","paused_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),"resume_units":active}
                write_private(MARKER,paused)
            errors=[]
            for u in UNITS:
                try:command("stop",u)
                except RuntimeError:errors.append(u)
            if errors:raise RuntimeError("pause_incomplete")
            return {"state":"PAUSED","scope":"local_inference_only","recovery_preserved":True}
        if paused is None:return {"state":"NOT_PAUSED","service_changes":0}
        install_conditions()
        MARKER.unlink()
        started=[]
        try:
            for u in reversed(UNITS):
                if u in paused["resume_units"]:
                    command("start",u);started.append(u)
                    if command("show",u,"--property=ActiveState","--value")!="active":raise RuntimeError("resume_not_active")
        except RuntimeError:
            write_private(MARKER,paused)
            for u in reversed(started):
                try:command("stop",u)
                except RuntimeError:pass
            raise
        return {"state":"RESUMED","started_units":started,"enablement_changed":False}

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("action",choices=("pause","resume","status"));a=p.parse_args()
    if a.action=="status":status.main();return
    try:print(json.dumps(operate(a.action),sort_keys=True))
    except Exception as exc:
        print(json.dumps({"state":"CONTROL_FAILED","reason":str(exc) if isinstance(exc,RuntimeError) else type(exc).__name__,"pause_marker_present":MARKER.exists()}))
        raise SystemExit(1)
if __name__=="__main__":main()
