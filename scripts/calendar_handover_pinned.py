"""Read-only Calendar handover comparison; no network or Calendar mutation."""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import stat
import tempfile

START = "FABRIC CURRENT NOTE"
END = "END FABRIC CURRENT NOTE"
HUMAN = "PRESERVED CONTEXT AND RESPONSE"
VOLATILE = {"description", "updated", "etag", "sequence"}

def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def stamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)

def split_description(value):
    if not isinstance(value, str):
        raise ValueError("description_missing")
    # Calendar descriptions may wrap markers in HTML or Markdown. Match unique
    # literal tokens without stripping or rewriting any source characters.
    if value.count(START) != 2 or value.count(END) != 1 or value.count(HUMAN) != 1:
        raise ValueError("ambiguous_description_markers")
    first, end, human = value.index(START), value.index(END), value.index(HUMAN)
    if not first < end < human:
        raise ValueError("ambiguous_description_boundaries")
    boundary = end + len(END)
    return value[:boundary], value[boundary:]

class VisibleResponse(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts=[]
    def handle_data(self,data):
        self.parts.append(data)

def response_has_text(value):
    parser=VisibleResponse()
    parser.feed(value)
    parser.close()
    return bool("".join(parser.parts).strip())

def protected(event):
    return {k: v for k, v in event.items() if k not in VOLATILE}

def eligible(event, series, now):
    if event.get("recurringEventId") not in series:
        return False
    if event.get("visibility") != "private" or event.get("attendees"):
        return False
    if event.get("eventType", "default") != "default" or event.get("status") == "cancelled":
        return False
    try:
        when = stamp(event["start"]["dateTime"])
        return now <= when <= now + timedelta(hours=36)
    except (KeyError, ValueError, TypeError):
        return False

def compare(current, previous, binding, ledger, now):
    if current.get("source_verified") is not True:
        raise ValueError("snapshot_not_source_verified")
    if not binding.get("calendar_id") or not binding.get("expected_email"):
        raise ValueError("binding_missing")
    series = binding.get("writable_series", {})
    if len(series) != 4 or len(set(series.values())) != 4:
        raise ValueError("four_unique_series_required")
    observed = stamp(current["observed_at"])
    if observed > now + timedelta(minutes=5) or now-observed > timedelta(hours=2):
        raise ValueError("snapshot_stale")
    coverage = current["coverage"]
    lo, hi = stamp(coverage["timeMin"]), stamp(coverage["timeMax"])
    if lo > observed-timedelta(hours=48)+timedelta(minutes=5) or hi < observed+timedelta(hours=36)-timedelta(minutes=5):
        raise ValueError("snapshot_coverage_incomplete")
    old = {e["id"]: e for e in (previous or {}).get("events", [])}
    receipts = dict(ledger.get("receipts", {}))
    report = {"mode":"READ_ONLY", "calendar_writes":0, "model_calls":0,
              "observed_at":current["observed_at"], "eligible_instances":0,
              "feedback_revisions_added":0, "blocked_instances":[],
              "protected_changes":[], "full_feedback_baseline":lo <= observed-timedelta(days=14)+timedelta(minutes=5) and current.get("feedback_scope_complete") is True,
              "writer_activation_ready":False,
              "remaining":["native_identity_and_calendar_owner_readback",
                           "feedback_receipts_consumed_by_owning_lanes",
                           "single_writer_disable_enable_transition"]}
    seen = set()
    for event in current.get("events", []):
        eid = event["id"]
        if eid in seen:
            raise ValueError("duplicate_instance")
        seen.add(eid)
        if event.get("recurringEventId") not in series.values():
            continue
        if event.get("visibility") != "private" or event.get("attendees") or event.get("eventType", "default") != "default":
            report["blocked_instances"].append({"event_id":eid,"reason":"not_private_solo_default"})
            continue
        try:
            _, suffix = split_description(event.get("description"))
        except ValueError as exc:
            report["blocked_instances"].append({"event_id":eid,"reason":str(exc)})
            continue
        prior = old.get(eid)
        previous_digest = None
        if prior:
            if protected(event) != protected(prior):
                report["protected_changes"].append(eid)
            try:
                previous_digest = digest(split_description(prior.get("description"))[1].strip())
            except ValueError:
                pass
        normalized_digest = digest(suffix.strip())
        # Blank response marker alone is not owner feedback.
        response = suffix.split(HUMAN, 1)[1]
        if response_has_text(response):
            key = "calendar-feedback:" + binding["calendar_id"] + ":" + eid + ":" + normalized_digest
            if key not in receipts:
                receipts[key] = {"event_id":eid,"event_time":event.get("start"),
                    "observed_at":current["observed_at"],"previous_digest":previous_digest,
                    "current_digest":normalized_digest,"raw_suffix_sha256":digest(suffix),
                    "private_suffix":suffix,"classification":"OWNER_REPORTED",
                    "owning_lane":next(k for k,v in series.items() if v == event["recurringEventId"]),
                    "next_verification":"Owning lane must interpret and verify against its authoritative provider.",
                    "consumed":False}
                report["feedback_revisions_added"] += 1
        if eligible(event, set(series.values()), now):
            report["eligible_instances"] += 1
    report["pending_feedback_receipts"] = sum(not x.get("consumed") for x in receipts.values())
    if not report["full_feedback_baseline"]:
        report["remaining"].append("rolling_14_day_feedback_baseline")
    return report, {"version":1,"receipts":receipts}

def private_read(path):
    p = Path(path)
    info = p.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
        raise ValueError("private_regular_file_required")
    return json.loads(p.read_text())

def private_write(path, data):
    p = Path(path)
    if p.exists() and (p.is_symlink() or p.stat().st_mode & 0o077):
        raise ValueError("private_output_required")
    fd, temp = tempfile.mkstemp(prefix=".calendar-", dir=p.parent)
    try:
        with os.fdopen(fd, "w") as out:
            json.dump(data, out, indent=2)
            out.write("\n")
            out.flush()
            os.fsync(out.fileno())
        os.replace(temp,p)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current",required=True)
    parser.add_argument("--previous")
    parser.add_argument("--binding",required=True,help="Private calendar_id, expected_email and writable_series mapping; no OAuth secrets needed")
    parser.add_argument("--ledger",required=True)
    parser.add_argument("--report",required=True)
    args=parser.parse_args()
    import fcntl
    lockfd=os.open(args.ledger+".lock",os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(lockfd,"w") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        ledger=private_read(args.ledger) if Path(args.ledger).exists() else {}
        report, ledger=compare(private_read(args.current),
            private_read(args.previous) if args.previous else None,
            private_read(args.binding),ledger,datetime.now(timezone.utc))
        private_write(args.ledger,ledger)
        private_write(args.report,report)
    print(json.dumps({k:v for k,v in report.items() if k not in {"blocked_instances","protected_changes"}}))

if __name__=="__main__":
    main()
