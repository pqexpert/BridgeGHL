"""Read-only readiness inventory. Never return credential values or send mail."""
import json
from pathlib import Path
import shutil
import subprocess


GROUPS = {
    "smtp": ("SMTP_HOST", "SMTP_USER", "SMTP_PASSWORD", "SMTP_FROM"),
    "gmail_oauth": ("GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET", "GOOGLE_REFRESH_TOKEN"),
    "mailgun": ("MAILGUN_API_KEY", "MAILGUN_DOMAIN", "MAILGUN_FROM"),
    "jira": ("JIRA_BASE_URL", "JIRA_EMAIL", "JIRA_API_TOKEN"),
}


def inventory(path):
    result = {"exists": path.is_file(), "groups": {}}
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
        result["readable_regular_file"] = False
        return result
    allowed = set(sum(GROUPS.values(), ())) | {"FABRIC_ALERT_TO"}
    present = set()
    for line in path.read_text().splitlines():
        key, separator, value = line.partition("=")
        key = key.strip().removeprefix("export ")
        if separator and key in allowed and value.strip().strip("\"'"):
            present.add(key)
    result["readable_regular_file"] = True
    result["recipient_configured"] = "FABRIC_ALERT_TO" in present
    for name, required in GROUPS.items():
        result["groups"][name] = {"complete": all(k in present for k in required),
                                   "present_fields": sorted(set(required) & present)}
    return result


def main():
    configs = {name: inventory(Path(path)) for name, path in {
        "bridge_runtime": "/etc/bridgeghl/bridgeghl.env",
        "fabric_alert_runtime": "/etc/ziji-fabric/alerts.env",
    }.items()}
    relay = {"sendmail_installed": bool(shutil.which("sendmail")),
             "postfix_installed": bool(shutil.which("postconf")),
             "relay_configured": False, "active": False}
    if relay["postfix_installed"]:
        r = subprocess.run(["postconf", "-h", "relayhost"], capture_output=True, text=True, timeout=5)
        relay["relay_configured"] = r.returncode == 0 and bool(r.stdout.strip())
        r = subprocess.run(["systemctl", "is-active", "postfix"], capture_output=True, timeout=5)
        relay["active"] = r.returncode == 0
    print(json.dumps({"stage": "FABRIC_ALERT_ROUTE_PREFLIGHT", "configs": configs,
                      "local_relay": relay, "external_messages": 0, "model_calls": 0,
                      "delivery_verified": False}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.TimeoutExpired):
        print(json.dumps({"stage": "FABRIC_ALERT_ROUTE_PREFLIGHT", "error": "read_failed_values_withheld"}))
        raise SystemExit(1)
