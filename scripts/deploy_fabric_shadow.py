"""Install the existing pinned read-only watchdog via the existing VPS transport.

No model/provider credentials are installed. No hosted task is disabled.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import urllib.request

SOURCE_SHA = "10aaf603179fa6a826def9e1f79590f4be9db847"
PINS = {"server/cron_watch.py": "857ae94d1182d12e8c639047d1e95f571a28376f",
        "server/test_cron_watch.py": "22363b9cc312882cb63da320cdbf5cbc3a9d839d"}
BASE = Path("/opt/ziji-fabric/dispatch")
STATE = Path("/var/lib/ziji-fabric/dispatch-shadow")
CRON = Path("/etc/cron.d/ziji-fabric-shadow")


def verify(files):
    if set(files) != set(PINS):
        raise ValueError("source_set_mismatch")
    for name, content in files.items():
        raw = content.encode()
        digest = hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
        if digest != PINS[name]:
            raise ValueError("source_hash_mismatch")


def atomic(path, content, mode):
    if path.is_symlink():
        raise ValueError("unexpected_symlink")
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".staging-")
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def sample(trigger="immediate_validation"):
    """Local-only observation with bounded storage and no send/write capability."""
    import fcntl
    with (STATE / "run.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"overlap_suppressed": True}
        command = [sys.executable, str(BASE / "releases" / SOURCE_SHA / "server/cron_watch.py"), "--shadow"]
        run = subprocess.run(command, capture_output=True, text=True, timeout=30, check=True)
        value = json.loads(run.stdout)
        if value.get("mode") != "shadow_read_only" or any(value.get(k) != 0 for k in
                ("model_calls", "calendar_writes", "external_messages", "crm_writes")):
            raise ValueError("shadow_boundary_failed")
        value["trigger"] = trigger
        latest = STATE / "latest.json"
        if latest.exists():
            atomic(STATE / "previous.json", latest.read_text(), 0o600)
        atomic(latest, json.dumps(value, sort_keys=True) + "\n", 0o600)
        return value


def install(files):
    verify(files)
    if os.geteuid() != 0:
        raise ValueError("root_required_for_existing_cron")
    if subprocess.run(["systemctl", "is-active", "crond"], capture_output=True).returncode:
        raise ValueError("existing_cron_not_active")
    # Refuse unexpected symlinks instead of overwriting an unknown deployment.
    for path in (BASE, STATE, CRON):
        if path.is_symlink():
            raise ValueError("unexpected_symlink")
    release = BASE / "releases" / SOURCE_SHA
    for path in (release / "server", STATE):
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.chmod(0o700)
    for name, content in files.items():
        atomic(release / name, content, 0o600)
    subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", str(release / "server"), "-p", "test_*.py"],
                   check=True, capture_output=True, timeout=30)
    helper = BASE / "shadow_runner.py"
    atomic(helper, Path(__file__).read_text(), 0o700)
    old = CRON.read_text() if CRON.exists() else None
    old_mode = CRON.stat().st_mode & 0o777 if CRON.exists() else None
    cron_text = "# Governed read-only Dispatch shadow; no provider/model calls or sends.\n17 * * * * root /usr/bin/python3 /opt/ziji-fabric/dispatch/shadow_runner.py --sample >/dev/null 2>&1\n"
    if old is not None and old != cron_text:
        raise ValueError("existing_cron_conflict")
    prior_scheduled = False
    if (STATE / "latest.json").exists():
        prior = json.loads((STATE / "latest.json").read_text())
        prior_scheduled = prior.get("trigger") == "cron"
    try:
        first = sample()
        second = sample()
        atomic(CRON, cron_text, 0o644)
        if CRON.read_text() != cron_text:
            raise ValueError("cron_readback_failed")
        atomic(STATE / "installation.json", json.dumps({"source_sha": SOURCE_SHA,
               "cron_installed": True, "hosted_tasks_disabled": False,
               "installed_at": second["observed_at_utc"]}), 0o600)
    except Exception:
        if old is None:
            CRON.unlink(missing_ok=True)
        else:
            atomic(CRON, old, old_mode)
        raise
    return {"stage": "FABRIC_SHADOW_INSTALL", "source_sha": SOURCE_SHA,
            "cron_installed": True, "two_immediate_readbacks": True,
            "scheduled_cycle_verified": prior_scheduled, "hosted_tasks_disabled": False,
            "services": second["checks"], "alert_count": len(second["alerts"]),
            "model_calls": 0, "external_messages": 0, "calendar_writes": 0, "crm_writes": 0}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def deploy():
    token = os.environ["SOURCE_TOKEN"].strip()
    user, host = os.environ["REMOTE_USER"], os.environ["REMOTE_HOST"]
    if not token or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", user) or not re.fullmatch(r"[A-Za-z0-9.:-]+", host):
        raise ValueError("existing_binding_missing")
    files = {}
    for name in PINS:
        request = urllib.request.Request("https://api.github.com/repos/pqexpert/fabric-dispatch/contents/" + name + "?ref=" + SOURCE_SHA,
            headers={"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json"})
        with urllib.request.build_opener(NoRedirect).open(request, timeout=30) as response:
            item = json.load(response)
        files[name] = base64.b64decode(item["content"]).decode()
    verify(files)
    payload = {"files": files, "installer": Path(__file__).read_text()}
    # Existing protected SSH transport; private source travels only on stdin.
    remote = "sudo -n python3 -c 'import json,sys,tempfile,runpy,os; p=json.load(sys.stdin); f=tempfile.NamedTemporaryFile(mode=\"w\",suffix=\".py\",delete=False); f.write(p[\"installer\"]); f.close(); s=runpy.run_path(f.name); print(json.dumps(s[\"install\"](p[\"files\"]),sort_keys=True)); os.unlink(f.name)'"
    command = ["ssh", "-i", str(Path.home()/".ssh/fabric-preflight"), "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
               "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes",
               "-o", "UserKnownHostsFile="+str(Path.home()/".ssh/fabric-known-hosts"), user+"@"+host, remote]
    run = subprocess.run(command, input=json.dumps(payload), text=True, capture_output=True, timeout=150)
    if run.returncode:
        # No raw stderr: private file paths, sources or secrets must not leak.
        raise ValueError("shadow_install_failed_inspect_before_retry")
    report = json.loads(run.stdout)
    if report.get("stage") != "FABRIC_SHADOW_INSTALL" or report.get("source_sha") != SOURCE_SHA:
        raise ValueError("invalid_install_receipt")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    try:
        if sys.argv[1:] == ["--sample"]:
            sample("cron")
        else:
            deploy()
    except Exception:
        print('{"stage":"FABRIC_SHADOW_INSTALL","ok":false,"details_withheld":true}')
        raise SystemExit(1)
