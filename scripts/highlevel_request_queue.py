"""Bounded Jira requests to existing HighLevel routes; no model or actor scheduler."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
MARKER = "FABRIC_HIGHLEVEL_REQUEST"
AUTHORITY = "RBO-97:12120"
AUTHORITY_URL = "https://restoration-mt.atlassian.net/browse/RBO-97?focusedCommentId=12120"
OPS = {"income.read_source": "income", "income.write_context": "income", "rsc.write_context": "rsc"}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def timeof(value):
    value = re.sub(r"([+-]\d\d)(\d\d)$", r"\1:\2", value)
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp_timezone")
    return parsed


def require(value, reason):
    if not value:
        raise ValueError(reason)


def parse(row, policy, now):
    body = row.get("body", "").strip()
    require(isinstance(body, str) and len(body) <= 12000 and body.startswith(MARKER+"\n"), "request_marker")
    obj = json.loads(body[len(MARKER)+1:])
    require(isinstance(obj, dict) and set(obj) == {
        "schema_version", "request_id", "lane", "operation", "authority_ref",
        "observed_at", "expires_at", "payload"}, "request_fields")
    require(obj["schema_version"] == 1 and type(obj["schema_version"]) is int, "request_schema")
    require(isinstance(obj["request_id"], str) and re.fullmatch(r"[a-z0-9][a-z0-9_.:-]{1,100}", obj["request_id"]), "request_id")
    lane = OPS.get(obj["operation"])
    require(lane is not None and lane == obj["lane"] and obj["authority_ref"] == AUTHORITY, "request_scope")
    author = row.get("author_account_id")
    require(author == policy["authority"]["issuer_account_id"]
            and author in policy["issuers"] and lane in policy["issuers"][author]
            and row.get("update_author_account_id") == author, "request_issuer")
    require(re.fullmatch(r"\d+", str(row.get("id", ""))), "request_comment_id")
    observed, expiry = timeof(obj["observed_at"]), timeof(obj["expires_at"])
    require(observed <= now < expiry and 0 < (expiry-observed).total_seconds() <= 36*3600, "request_expired")
    require(timeof(row["created"]) <= now and timeof(row["updated"]) <= now, "request_future")
    payload = obj["payload"]
    require(isinstance(payload, dict), "request_payload")
    if obj["operation"] == "income.read_source":
        require(set(payload) <= {"source_id", "contact_id", "opportunity_id", "current_source_sha256"}
                and {"source_id", "current_source_sha256"} <= set(payload), "read_fields")
        require(isinstance(payload["current_source_sha256"], str) and
                re.fullmatch("[a-f0-9]{64}", payload["current_source_sha256"]), "read_digest")
        for key in ("contact_id", "opportunity_id"):
            if key in payload:
                require(isinstance(payload[key], str) and re.fullmatch("[A-Za-z0-9_-]{1,128}", payload[key]), "read_native_id")
    else:
        require(set(payload) == {"source_id", "source_url", "title", "content"}, "write_fields")
        require(payload["source_url"] == AUTHORITY_URL, "write_source_authority")
        require(isinstance(payload["source_id"], str) and payload["source_id"].startswith("fabric-"), "write_source_namespace")
        require(isinstance(payload["title"], str) and 0 < len(payload["title"]) <= 200, "write_title")
        require(isinstance(payload["content"], str) and 0 < len(payload["content"]) <= 4000, "write_content")
        # Context-only operational text: no contacts/opportunities, dynamic
        # properties, Income admission, Store data or arbitrary CRM routes.
        require(not any(x in json.dumps(payload).lower() for x in
                        ("fabric_income_admission", "-----begin ", "api_key", "access_token", "refresh_token")),
                "write_sensitive_or_admission")
    require(isinstance(payload["source_id"], str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", payload["source_id"]), "source_id")
    return obj


def exact_matches(row, fresh):
    return (all(fresh.get(k) == row.get(k) for k in
               ("id", "created", "updated", "author_account_id", "update_author_account_id"))
            and fresh.get("body", "").strip() == row.get("body", "").strip())


def authority_matches(policy, fresh):
    authority = policy["authority"]
    require(str(fresh.get("id")) == authority["comment_id"]
            and fresh.get("updated") == authority["updated"]
            and fresh.get("author_account_id") == authority["issuer_account_id"]
            and fresh.get("update_author_account_id") == authority["issuer_account_id"],
            "authority_changed")


class Journal:
    """Request ledger inside the existing ingestion database."""
    def __init__(self, path):
        self.path = Path(path)
        require(not self.path.is_symlink(), "journal_symlink")
        with self.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS highlevel_requests (request_id TEXT PRIMARY KEY, request_hash TEXT NOT NULL, state TEXT NOT NULL, result TEXT)")
        os.chmod(self.path, 0o600)

    def connect(self):
        return sqlite3.connect(str(self.path), timeout=15)

    def get(self, key):
        with self.connect() as db:
            row = db.execute("SELECT request_hash,state,result FROM highlevel_requests WHERE request_id=?", (key,)).fetchone()
        return None if row is None else {"hash":row[0], "state":row[1], "result":json.loads(row[2]) if row[2] else None}

    def claim(self, key, fingerprint, attempt, old=None):
        with self.connect() as db:
            if old is None:
                return db.execute("INSERT OR IGNORE INTO highlevel_requests VALUES (?,?,?,?)",
                                  (key, fingerprint, "PENDING", json.dumps(attempt, sort_keys=True))).rowcount == 1
            return db.execute("UPDATE highlevel_requests SET state=?,result=? WHERE request_id=? AND request_hash=? AND state=? AND result=?",
                              ("PENDING", json.dumps(attempt, sort_keys=True), key, fingerprint,
                               old["state"], json.dumps(old["result"], sort_keys=True))).rowcount == 1

    def save(self, key, fingerprint, state, result):
        with self.connect() as db:
            changed = db.execute("UPDATE highlevel_requests SET state=?,result=? WHERE request_id=? AND request_hash=?",
                                 (state, json.dumps(result, sort_keys=True), key, fingerprint)).rowcount
        require(changed == 1, "journal_compare_failed")


def safe_result(operation, result):
    if operation == "income.read_source":
        require(result.get("domain") == "income" and result.get("native_readback") is True, "readback_missing")
        source = result.get("source", {})
        # Never echo arbitrary source content/properties, applicant data or
        # historical records into Jira. The native digest still binds full body.
        output = {k:result.get(k) for k in ("source_id", "source_key", "source_sha256",
                  "location_id", "contact_id", "opportunity_id", "note_ids")}
        title, url = source.get("title"), source.get("source_url")
        require(isinstance(title, str) and len(title) <= 500 and isinstance(url, str) and len(url) <= 2000, "readback_source_shape")
        projection = result.get("admission_projection")
        if projection is not None:
            fields = {"provider", "board", "job_id", "title", "packet_id", "do_not_submit", "duplicate_clear", "artifact_absent"}
            require(isinstance(projection, dict) and set(projection) == fields, "readback_admission_shape")
            for key in ("provider", "board", "job_id", "title", "packet_id"):
                require(isinstance(projection[key], str) and 0 < len(projection[key]) <= 500, "readback_admission_field")
            for key in ("do_not_submit", "duplicate_clear", "artifact_absent"):
                require(type(projection[key]) is bool, "readback_admission_flag")
        aliases = result.get("legacy_provenance_urls", [])
        require(isinstance(aliases, list) and len(aliases) <= 20 and all(
            isinstance(value, str) and len(value) <= 2000 for value in aliases), "readback_aliases")
        output.update(source={"title":title, "source_url":url}, native_readback=True,
                      admission_projection=projection, legacy_provenance_urls=aliases,
                      source_body_returned=False, execution_admitted=False)
        return output
    require(result.get("verified") is True and result.get("opportunity_id") is None
            and isinstance(result.get("note_ids"), list) and 0 < len(result["note_ids"]) <= 1, "write_readback_missing")
    return {**{k:result.get(k) for k in ("source_key", "source_sha256", "contact_id", "note_ids")},
            "native_readback":True}


def process(snapshot, policy, journal, exact_comment, invoke, publish, now, execute=False):
    """Callbacks keep credentials/routes in the existing protected runtime."""
    candidates = []
    for row in snapshot.get("highlevel_request_candidates", []):
        try:
            obj = parse(row, policy, now)
            candidates.append((row, obj))
        except (ValueError, TypeError, KeyError):
            continue
    counts = {}
    for _, obj in candidates:
        counts[obj["request_id"]] = counts.get(obj["request_id"], 0) + 1
    results, used = [], set()
    for row, obj in sorted(candidates, key=lambda pair: (pair[0]["created"], int(pair[0]["id"]))):
        key, lane = obj["request_id"], obj["lane"]
        fingerprint = digest(obj)
        old = journal.get(key)
        base = {"request_id":key, "lane":lane, "operation":obj["operation"],
                "request_comment_id":str(row["id"]), "request_sha256":fingerprint}
        if counts[key] != 1 or (old and old["hash"] != fingerprint):
            results.append(dict(base, status="CONFLICT"))
            continue
        if old and old["state"] == "VERIFIED":
            results.append(dict(base, status="VERIFIED", receipt=old["result"].get("receipt"),
                                native_readback_at=old["result"].get("native_readback_at"), cached=True))
            continue
        if lane in used:
            continue
        if not execute:
            used.add(lane)
            results.append(dict(base, status="PREVIEW", existing_state=old["state"] if old else None,
                                kind="context" if obj["operation"].endswith("write_context") else "source_read",
                                source_id=obj["payload"]["source_id"], model_calls=0))
            continue
        prior_attempt = old["result"] if old and old["state"] != "BRIDGE_VERIFIED" else None
        if old and old["state"] != "BRIDGE_VERIFIED":
            if not prior_attempt or prior_attempt.get("attempts", 3) >= 3:
                results.append(dict(base, status="UNKNOWN", reason="bounded_reconciliation_exhausted"))
                continue
            if (now-timeof(prior_attempt["attempted_at"])).total_seconds() < 6*3600:
                results.append(dict(base, status="DEFERRED", reason="reconciliation_backoff"))
                continue
        # Unknown/backed-off old requests do not starve later same-lane work.
        used.add(lane)
        try:
            authority_matches(policy, exact_comment("12120"))
            require(exact_matches(row, exact_comment(str(row["id"]))), "request_changed")
            parse(row, policy, now)
        except Exception:
            results.append(dict(base, status="DEFERRED", reason="authority_or_request_changed"))
            continue
        if old and old["state"] == "BRIDGE_VERIFIED":
            result = old["result"]
        else:
            attempt = {"attempted_at":now.isoformat(), "attempts":(prior_attempt or {}).get("attempts", 0)+1}
            if not journal.claim(key, fingerprint, attempt, old):
                results.append(dict(base, status="DEFERRED", reason="concurrent_claim"))
                continue
            try:
                # Same request and payload only. Writes pass through the existing
                # ingestion effect journal: native lookup precedes creation and
                # unresolved prior creation refuses another POST. Reads may retry.
                native = safe_result(obj["operation"], invoke(obj))
                result = dict(base, outcome="SUCCESS", action_class="bounded_highlevel_request",
                              authority_ref=AUTHORITY, native_result=native,
                              native_readback_at=now.isoformat(), model_calls=0,
                              whole_actor_replaced=False)
                journal.save(key, fingerprint, "BRIDGE_VERIFIED", result)
            except Exception:
                results.append(dict(base, status="DEFERRED", reason="provider_reconciliation_backoff"))
                continue
        try:
            receipt = publish("RBO-97", "highlevel:"+key, result)
            result = dict(result, receipt=receipt)
            if receipt.get("native_readback") is True and receipt.get("status") in {"VERIFIED", "EXISTING"}:
                journal.save(key, fingerprint, "VERIFIED", result)
                results.append(dict(base, status="VERIFIED", receipt=receipt,
                                    native_readback_at=result["native_readback_at"], cached=False))
            else:
                results.append(dict(base, status="RECEIPT_PENDING"))
        except Exception:
            results.append(dict(base, status="RECEIPT_PENDING"))
    return {"stage":"HIGHLEVEL_REQUEST_QUEUE", "observed_at":now.isoformat(), "requests":results,
            "model_calls":0, "mission_executions_claimed":0}


def run(execute=False):
    require(os.geteuid() == 0, "root_required")
    from project_actor_receipts import CONFIG, SOURCES, current_snapshot, validate_policy, private_read, atomic
    from provider_budget import actor_scope
    sys.path.insert(0, "/opt/ziji-fabric/native")
    from native_jira_receipt import headers, publish
    from bind_native_providers import request, JIRA_ROOT
    from collect_native_sources import text_content
    import app as bridge
    from ingestion import IngestRecord, IncomeSourceRead, configuration, note_bodies
    now = dt.datetime.now(dt.timezone.utc)
    policy = validate_policy(json.loads(private_read(CONFIG)))
    snapshot = current_snapshot(SOURCES, now)
    db = Path(bridge.AUDIT_LOG_PATH).parent/"ingestion.sqlite3"
    journal = Journal(db)

    def exact_comment(cid):
        raw = request(JIRA_ROOT+"/rest/api/3/issue/RBO-97/comment/"+cid, headers())
        author = raw.get("author", {}).get("accountId")
        updater = raw.get("updateAuthor", {}).get("accountId")
        if "updateAuthor" not in raw and raw.get("created") == raw.get("updated"):
            updater = author
        return {"id":str(raw["id"]), "created":raw["created"], "updated":raw["updated"],
                "author_account_id":author, "update_author_account_id":updater,
                "body":text_content(raw.get("body", {})).strip()}

    def invoke(obj):
        lane, operation = obj["lane"], obj["operation"]
        with actor_scope(lane, db):
            if operation == "income.read_source":
                payload = dict(obj["payload"])
                payload.setdefault("contact_id", configuration("income")["default_contact_id"])
                model = IncomeSourceRead(**payload)
                endpoint = next(r.endpoint for r in bridge.app.routes if getattr(r, "path", None) == "/read/ingest/income-source")
                result = endpoint(model, bridge.API_KEY)
                require(result.get("source_sha256") == payload["current_source_sha256"]
                        and result.get("source_id") == payload["source_id"], "readback_digest")
                return result
            record = IngestRecord(domain=lane, kind="context", properties={}, **obj["payload"])
            preview = bridge.ingestion_operations["dry_run"](record, bridge.API_KEY)
            require(preview.get("accepted") is True and preview.get("note_parts") == 1
                    and preview.get("contact_effect") == "existing_default_contact"
                    and preview.get("opportunity_effect") == "none", "preview_not_accepted")
            bridge.append_audit_log({"action":"highlevel_request_preview", "request_id":obj["request_id"],
                                    "request_sha256":digest(obj), "preview":preview, "authority_ref":AUTHORITY})
            result = bridge.ingestion_operations["execute"](record, bridge.API_KEY)
            result["source_sha256"] = note_bodies(record)[0].split(":", 2)[2].split(":", 1)[0]
            return result

    report = process(snapshot, policy, journal, exact_comment, invoke, publish, now, execute)
    if execute:
        atomic(db.parent/"highlevel-request-status.json", report, 0o644)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    try:
        print(json.dumps(run(args.execute), sort_keys=True))
    except Exception:
        print(json.dumps({"stage":"HIGHLEVEL_REQUEST_QUEUE", "status":"DEFERRED",
                          "reason":"bounded_failure_details_withheld", "model_calls":0}))
        sys.exit(1)
