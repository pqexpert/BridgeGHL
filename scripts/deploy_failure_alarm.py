"""Install comparator on the existing owner-only mail cron; no new scheduler."""
import json, os, re, shlex, subprocess
from pathlib import Path

def on_host(p):
    if os.geteuid()!=0: raise ValueError("root_required")
    root=Path("/opt/ziji-fabric")
    # Existing accepted mail route/cron must remain exactly the same.
    cron=Path("/etc/cron.d/ziji-fabric-alerts")
    if not cron.exists() or "19 * * * * root /usr/bin/python3 /opt/ziji-fabric/fabric_alert_mailer.py" not in cron.read_text():
        raise ValueError("existing_alert_cron_missing")
    if not Path("/etc/ziji-fabric/alerts.json").exists():
        raise ValueError("existing_owner_mail_binding_missing")
    import sys
    sys.path.insert(0,str(root))
    for name,source in p["files"].items():
        if name not in ("fabric_failure_alarm.py","fabric_alert_mailer.py","collect_native_sources.py"): raise ValueError("source_not_allowlisted")
        path=(root/"native"/name) if name=="collect_native_sources.py" else root/name
        if path.is_symlink():raise ValueError("symlink")
        path.write_text(source);path.chmod(0o700)
    import fabric_failure_alarm as alarm
    import fabric_alert_mailer as mail
    report=alarm.collect()
    mail.ROOT.mkdir(mode=0o700,parents=True,exist_ok=True)
    mail.atomic(mail.ROOT/"fabric-latest.json",report)
    if p["mode"]=="activate":
        if report["comparable_coverage"] is not True:raise ValueError("comparable_coverage_not_ready")
        mail.atomic(Path("/etc/ziji-fabric/failure-alarm.json"),{"enabled":True,"activated_at":report["observed_at"],"source_sha":p["sha"]})
    print(json.dumps({"stage":"FABRIC_FAILURE_ALARM","mode":p["mode"],"comparable_coverage":report["comparable_coverage"],
      "checks":report["checks"],"alerts":report["alerts"],"model_calls":0,"provider_reads":0,
      "messages_sent":0,"cron_changed":False,"enabled":Path("/etc/ziji-fabric/failure-alarm.json").exists()}))

def main():
    user,host=os.environ["REMOTE_USER"],os.environ["REMOTE_HOST"]
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*",user) or not re.fullmatch(r"[A-Za-z0-9.:-]+",host):raise ValueError("target_invalid")
    files={name:Path("scripts",name).read_text() for name in ("fabric_failure_alarm.py","fabric_alert_mailer.py","collect_native_sources.py")}
    p={"files":files,"mode":os.environ.get("ALARM_MODE","inspect"),"sha":os.environ["GITHUB_SHA"],"source":Path(__file__).read_text()}
    command="import json,sys;p=json.load(sys.stdin);s={'__name__':'remote'};exec(compile(p.pop('source'),'installer','exec'),s);s['on_host'](p)"
    args=["ssh","-i",str(Path.home()/".ssh/fabric-preflight"),"-o","BatchMode=yes","-o","StrictHostKeyChecking=yes","-o","UserKnownHostsFile="+str(Path.home()/".ssh/fabric-known-hosts"),user+"@"+host,"sudo -n python3 -c "+shlex.quote(command)]
    result=subprocess.run(args,input=json.dumps(p).encode(),capture_output=True,timeout=60)
    if result.returncode:raise ValueError("alarm_install_failed")
    print(result.stdout.decode().strip())

if __name__=="__main__":
    try:main()
    except Exception as e:
        print(json.dumps({"outcome":"HOLD","error_type":type(e).__name__,"details_withheld":True}))
        raise SystemExit(1)
