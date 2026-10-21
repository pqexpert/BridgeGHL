"""Read existing private receipt evidence; never poll providers or call a model."""
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
UTC = dt.timezone.utc
BASE = Path("/var/lib/ziji-fabric")
DUE = {"income": (8, 0), "rsc": (8, 30), "store": (9, 0)}
SLOTS = ((5,15,"DELTA"),(7,0,"DELTA"),(11,45,"FULL"),(15,0,"DELTA"),(17,0,"DELTA"),(20,45,"DELTA"))
P0 = {"unauthorized_external_send", "privacy_violation", "stop_dnd_violation",
      "security_breach", "irreversible_customer_harm", "destructive_error"}
P1 = {"readback_mismatch", "urgent_verified_deadline", "authoritative_state_contradiction",
      "owner_response_overwritten", "digest_regression", "calendar_readback_mismatch",
      "suffix_readback_mismatch", "uncertain_write_changed_native_state_inspect"}

def stamp(value):
    result = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timestamp_not_aware")
    return result.astimezone(UTC)

def load(path, default=None):
    try:
        if path.is_symlink() or path.stat().st_size > 8*1024*1024 or path.stat().st_uid != 0:
            return default
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default

def native_records(snapshot, ledger, now):
    records = []
    # A published internal receipt is not itself a verified domain effect.
    for entry in ledger:
        if entry.get("status") in ("VERIFIED", "EXISTING") and entry.get("native_readback") is True:
            row = dict(entry.get("receipt", {}))
            row["reference"] = "jira:" + str(entry.get("issue")) + ":" + str(entry.get("comment_id"))
            row["replay_id"] = entry.get("replay_id", "")
            row.setdefault("observed_at",entry.get("observed_at",""))
            records.append(row)
    unparsed = 0
    if snapshot.get("source_verified") is True:
        for entry in snapshot.get("receipt_candidates", []):
            body = entry.get("body", "")
            decoder = json.JSONDecoder()
            found = False
            for match in re.finditer(r"\{", body):
                try:
                    row, _ = decoder.raw_decode(body[match.start():])
                except ValueError:
                    continue
                if isinstance(row, dict) and row.get("lane") in DUE and row.get("outcome") in ("SUCCESS","NO_CHANGE","BLOCKED","FAIL"):
                    row = dict(row, reference=entry.get("reference"), observed_at=entry.get("created"))
                    key = re.search(r"(?:income|rsc|store):\d{4}-\d{2}-\d{2}:daily", body)
                    if key:
                        row.setdefault("replay_id", key.group())
                    records.append(row)
                    found = True
                    break
            if "FABRIC_RUN" in body and not found:
                try:
                    if now-stamp(entry["created"]) < dt.timedelta(hours=48): unparsed += 1
                except (ValueError,KeyError,TypeError): unparsed += 1
    records.sort(key=lambda r: str(r.get("observed_at",r.get("local_date",""))))
    return records, unparsed

def evaluate(now, snapshot, ledger, calendar, runs, state, failure=None):
    now = now.astimezone(UTC)
    local = now.astimezone(ET)
    records, unparsed = native_records(snapshot or {}, ledger, now)
    alerts, evidence, checks = [], {}, {}
    def alarm(severity, lane, condition, reference):
        key = severity + ":" + lane + ":" + condition
        alerts.append(key)
        evidence[key] = {"reference": reference, "observed_at": now.isoformat(),
                         "safe_default": "Preserve last verified state; no blind effect replay.",
                         "attempted_recovery": "Read-only native evidence reconciliation.",
                         "next_action": "Inspect the owning native receipt and repair only that condition."}
    fresh = False
    try:
        fresh = snapshot.get("source_verified") is True and -300 <= (now-stamp(snapshot["observed_at"])).total_seconds() <= 5400
    except (TypeError, KeyError, ValueError):
        pass
    checks["jira"] = "VERIFIED" if fresh else "UNAVAILABLE_OR_STALE"
    if not fresh:
        alarm("P1", "evidence", "jira_snapshot_stale_or_unavailable", "vps:native-sources/jira.json")
    checks["unparsed_hosted_run_records"] = unparsed
    # Daily completeness requires an explicit whole-run terminal, never a
    # source capability reconciliation or local model proposal.
    for lane, (hour, minute) in DUE.items():
        dates = []
        for days in (1, 0):
            date = local.date() - dt.timedelta(days=days)
            due = dt.datetime.combine(date, dt.time(hour,minute), ET)
            if local >= due + dt.timedelta(minutes=90):
                dates.append(date.isoformat())
        complete, bad = [], []
        for date in dates:
            candidates = [r for r in records if r.get("lane") == lane
                and r.get("whole_actor_replaced") is not False
                and (r.get("replay_id") == lane+":"+date+":daily" or
                     (r.get("scope") == "daily" and r.get("local_date") == date))]
            if not candidates:
                bad.append(date)
                continue
            row = candidates[-1]
            outcome = row.get("outcome")
            if outcome == "FAIL":
                bad.append(date)
            elif outcome in ("SUCCESS", "NO_CHANGE", "BLOCKED"):
                complete.append(date)
                # BLOCKED is truthful terminal state, not automatic actor failure.
                if outcome != "BLOCKED" and row.get("native_readback") is not True:
                    alarm("P1",lane,"terminal_readback_unverified",row.get("reference"))
        checks[lane] = {"terminal_dates": complete, "missing_or_failed_dates": bad,
                        "whole_actor_health": "UNKNOWN" if not complete else "TERMINAL_EVIDENCE"}
        # Missing evidence is not a whole-actor outage. Report a receipt gap only
        # when the collection is fresh and two distinct expected runs lack evidence.
        if fresh and len(bad) >= 2 and unparsed == 0:
            alarm("P1",lane,"repeated_due_receipt_gap", "jira:RBO-97")
    signals = {}
    for row in records:
        for signal in row.get("material_conditions", []):
            if not isinstance(signal, dict) or signal.get("verified") is not True:
                continue
            lane = row.get("lane") if row.get("lane") in DUE else "evidence"
            signals[(lane,signal.get("code"))] = (signal,row.get("reference"))
    for (lane,code),(signal,reference) in signals.items():
        if signal.get("active") is not True:
            continue
        if code in P0 | P1:
            alarm("P0" if code in P0 else "P1",lane,code,reference)
        if code == "sustained_efficiency_deterioration" and signal.get("native_charge_data") is True:
            alarm("P2",lane,code,reference)
    valid_runs = []
    for row in runs:
        try:
            seen = stamp(row["observed_at"])
            if seen <= now + dt.timedelta(minutes=5):
                valid_runs.append(row)
        except (KeyError, ValueError, TypeError):
            continue
    good = [r for r in valid_runs if r.get("status") in ("VERIFIED","NO_CHANGE")]
    full = [r for r in good if r.get("mode") == "FULL" and r.get("checkpoint",{}).get("verified") is True]
    full.sort(key=lambda r:r["observed_at"])
    last_full = full[-1] if full else None
    checks["calendar_writer"] = "VERIFIED_RECEIPTS" if good else "UNAVAILABLE"
    checks["calendar_state"] = "AVAILABLE" if isinstance(state.get("feedback"),dict) else "UNAVAILABLE"
    checks["last_good_full"] = last_full.get("observed_at") if last_full else None
    if last_full and now-stamp(last_full["observed_at"]) > dt.timedelta(hours=36):
        alarm("P1","distiller","full_readback_older_than_36h","vps:calendar-writer/runs")
    elif not last_full:
        checks["full_age"] = "UNKNOWN_no_native_full"
    if good:
        first = min(stamp(r["observed_at"]) for r in good)
        due_slots = []
        for days in (1,0):
            date = local.date()-dt.timedelta(days=days)
            for hour,minute,mode in SLOTS:
                due = dt.datetime.combine(date,dt.time(hour,minute),ET)
                if due >= first and local >= due+dt.timedelta(minutes=90):
                    due_slots.append(due.strftime("%Y-%m-%d_%H-%M")+"_"+mode)
        present = {r.get("run_id") for r in good}
        if len(due_slots)>=2 and all(key not in present for key in due_slots[-2:]):
            alarm("P1","distiller","repeated_due_receipt_gap","vps:calendar-writer/runs")
    full_recent = sorted([r for r in valid_runs if r.get("mode")=="FULL"],key=lambda r:r["observed_at"])[-2:]
    omitted = [r for r in full_recent if not r.get("coverage") or not r.get("checkpoint",{}).get("verified")]
    if len(omitted) == 2:
        alarm("P1","distiller","two_full_feedback_receipts_missing","vps:calendar-writer/runs")
    for row in sorted(valid_runs,key=lambda r:r["observed_at"])[-1:]:
        for item in row.get("coverage", []):
            if item.get("complete") is False:
                alarm("P1","distiller","calendar_pagination_incomplete","vps:calendar-writer/runs")
        for result in row.get("calendar_results", []):
            if result.get("status") == "VERIFIED" and result.get("protected_fields_verified") is not True:
                alarm("P1","distiller","protected_fields_unverified","vps:calendar-writer/runs")
    if failure:
        try:
            if not good or stamp(failure["observed_at"]) > max(stamp(r["observed_at"]) for r in good):
                reason = failure.get("reason")
                alarm("P1","distiller",reason if reason in P1 else "native_run_failed","vps:calendar-writer/failure.json")
        except (KeyError, ValueError, TypeError):
            checks["failure"] = "UNPARSEABLE"
    for key, item in state.get("feedback", {}).items():
        if item.get("status") == "CONSUMED" or item.get("classification") != "OWNER_REPORTED":
            continue
        try:
            seen = stamp(item["observed_at"])
            if last_full and stamp(last_full["observed_at"]) >= seen and now-seen > dt.timedelta(hours=24):
                # Stable aggregate key; no private owner text or event details in mail.
                alarm("P1","distiller","verified_owner_feedback_unconsumed_24h","vps:calendar-writer/state.json")
        except (KeyError, ValueError, TypeError):
            checks["feedback"] = "UNPARSEABLE"
    checks["provider_cost"] = "UNKNOWN_without_native_charge_data"
    checks["limits"] = ["No erased-between-polls detection","No whole-platform outage claim",
        "Unstructured legacy conditions remain unparsed; missing subset is not whole-actor failure"]
    scan_verified = bool(last_full) and bool(last_full.get("coverage")) and all(w.get("complete") is True for w in last_full["coverage"])
    comparable = fresh and unparsed == 0 and scan_verified and checks["calendar_state"] == "AVAILABLE" and all(checks[x]["terminal_dates"] for x in DUE)
    return {"schema":"fabric-failure-alarm/1","observed_at":now.isoformat(),
            "alerts":sorted(set(alerts)),"evidence":evidence,"checks":checks,
            "comparable_coverage":comparable,"model_calls":0,"provider_reads":0,
            "calendar_writes":0,"domain_writes":0}

def collect(now=None, base=BASE):
    now = now or dt.datetime.now(UTC)
    ledger = [load(p,{}) for p in sorted((base/"receipts").glob("*.json")) if re.fullmatch("[0-9a-f]{64}.json",p.name)]
    runs = [load(p,{}) for p in sorted((base/"calendar-writer/runs").glob("*_write.json"))[-100:]]
    return evaluate(now,load(base/"native-sources/jira.json",{}),ledger,
                    load(base/"calendar-writer/latest.json",{}),runs,
                    load(base/"calendar-writer/state.json",{}),
                    load(base/"calendar-writer/failure.json",{}))
