"""Bounded public employer intake before the existing Income continuation.

Runs only in the existing protected operational runner. No inference, contact
creation, messages, applications, extra scheduler, or paid fallback.
"""
import argparse
import contextlib
import datetime as dt
import email.utils
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
AUTHORITY = {"id":"12221", "updated":"2026-10-24T09:08:39.303-0500",
             "issuer":"712020:957ef1d2-89f5-461a-90e3-ffcfb77330bd"}
BOARDS = (("weloglobal", "Welo Global"), ("includedhealth", "Included Health"))
MAX_READS, MAX_RECORDS, PAGE = 24, 6, 25
UUID = r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"


def require(value, reason):
    if not value:
        raise ValueError(reason)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def stamp(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def validate_authority(row):
    require(str(row.get("id")) == AUTHORITY["id"] and row.get("updated") == AUTHORITY["updated"]
            and all(row.get(k) == AUTHORITY["issuer"] for k in
                    ("author_account_id", "update_author_account_id")), "discovery_authority_changed")


def eligible(job, board):
    """Unknown geography/commitment stays unknown; never infer applicant fit."""
    categories = job.get("categories", {})
    title = job.get("text", "")
    jid = job.get("id", "")
    return (board in dict(BOARDS) and isinstance(jid, str) and re.fullmatch(UUID, jid) is not None
            and job.get("country") == "US" and job.get("workplaceType") == "remote"
            and isinstance(title, str) and 0 < len(title) <= 300
            and re.search(r"support|help.?desk|rater|evaluat|annotat|ai.train", title, re.I) is not None
            and str(categories.get("commitment", "")).lower() in
                {"contractor", "contract", "freelance", "freelance-remote", "part-time", "part time"}
            and job.get("hostedUrl") == "https://jobs.lever.co/"+board+"/"+jid)


def record_for(job, board):
    require(eligible(job, board), "posting_not_admitted")
    jid = job["id"]
    return {"domain":"income", "kind":"opportunity", "source_id":"lever-"+board+"-"+jid,
            "source_url":job["hostedUrl"], "title":dict(BOARDS)[board]+" — "+job["text"],
            "properties":{"Role":job["text"], "Link":job["hostedUrl"], "Company":dict(BOARDS)[board],
                "Status":"Research", "Work Mode":"Remote", "Type":"Contract",
                "Employer Commitment":job["categories"]["commitment"], "Country":"US",
                "Do Not Auto Submit":True, "Packet ID":"INCOME-LEVER-"+jid,
                "Next Action":"Verify candidate evidence, geography, hours and employer requirements before any application."},
            "content":"Public employer posting. Research lead only; candidate eligibility, fit, hours and work portability are unknown. No application, outreach or candidate qualification asserted."}


def retry_at(headers, now):
    value = headers.get("Retry-After", "")
    try:
        if str(value).isdigit():
            delay = dt.timedelta(seconds=int(value))
            return (now+max(delay, dt.timedelta(hours=1))).isoformat()
        parsed = email.utils.parsedate_to_datetime(value)
        require(parsed.tzinfo is not None, "retry_timezone")
        return max(now+dt.timedelta(hours=1), parsed).isoformat()
    except (ValueError, TypeError, OverflowError):
        return (now+dt.timedelta(hours=6)).isoformat()


class PublicFailure(Exception):
    def __init__(self, status=0, headers=None):
        self.status, self.headers = status, headers or {}


def public_get(board, jid=None, cursor=0):
    require(board in dict(BOARDS) and (jid is None or re.fullmatch(UUID, jid)), "public_route_scope")
    url = "https://api.lever.co/v0/postings/"+board
    if jid:
        url += "/"+jid
    else:
        # Provider filters are exact case. Client eligibility remains decisive.
        terms = [("mode", "json"), ("skip", str(cursor)), ("limit", str(PAGE))]
        terms += [("commitment", c) for c in ("Freelance", "Freelance-Remote", "Part-Time", "Contractor", "Contract")]
        url += "?"+urllib.parse.urlencode(terms)
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None
    try:
        with urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect()).open(
                urllib.request.Request(url, headers={"Accept":"application/json"}), timeout=20) as response:
            raw = response.read(2097153)
            require(len(raw) <= 2097152, "public_response_limit")
            return json.loads(raw)
    except urllib.error.HTTPError as exc:
        raise PublicFailure(exc.code, dict(exc.headers)) from None
    except (OSError, ValueError):
        raise PublicFailure() from None


def process(state, queue, ledger, now, persist, check, fetch, duplicate, ingest, enqueue):
    """Callbacks isolate side effects; each reservation is durable before dispatch."""
    result = {"stage":"INCOME_SOURCE_DISCOVERY", "model_calls":0, "paid_fallback":False,
              "authority_ref":"RBO-97:12221", "observed_at":now.isoformat()}
    def done(status, **fields):
        state["last_result"] = dict(result, status=status, **fields)
        persist()
        return state["last_result"]
    if state.get("next_poll_at") and now < stamp(state["next_poll_at"]):
        return dict(result, status="COOLDOWN", next_poll_at=state["next_poll_at"])
    check()
    # Persist global interval before network access, including failures/crashes.
    state["next_poll_at"] = (now+dt.timedelta(hours=1)).isoformat()
    day = now.astimezone(dt.timezone.utc).date().isoformat()
    days = state.setdefault("days", {})
    for old in list(days):
        if old < (now.date()-dt.timedelta(days=7)).isoformat():
            del days[old]
    budget = days.setdefault(day, {"reads":0, "records":0})
    persist()
    def get(board, jid=None, cursor=0):
        require(budget["reads"] < MAX_READS, "daily_public_read_budget")
        budget["reads"] += 1
        persist()
        try:
            return fetch(board, jid, cursor)
        except PublicFailure as exc:
            source = state.setdefault("boards", {}).setdefault(board, {})
            if exc.status in (301,302,303,307,308,401,403,404):
                source["blocked"] = "source_identity_or_authorization_"+str(exc.status)
            source["next_poll_at"] = retry_at(exc.headers, now) if exc.status == 429 else (now+dt.timedelta(hours=6)).isoformat()
            persist()
            raise
    pending = state.get("pending")
    if pending:
        job_key = "lever:"+pending["board"]+":"+pending["job_id"]
        if any(u.get("job_identity") == job_key and u.get("status") == "VERIFIED"
               for u in ledger.get("units", {}).values()):
            state.pop("pending")
            return done("RECONCILED_COMPLETED")
        if pending.get("attempts", 0) >= 3:
            return done("RECONCILIATION_REQUIRED")
        if now < stamp(pending["retry_at"]):
            return done("PENDING_BACKOFF")
        board = pending["board"]
        source = state.get("boards", {}).get(board, {})
        if source.get("blocked") or (source.get("next_poll_at") and now < stamp(source["next_poll_at"])):
            return done("SOURCE_DEFERRED")
        job = get(board, pending["job_id"])
        require(digest(record_for(job, board)) == pending["record_sha256"], "pending_public_source_changed")
    else:
        if budget["records"] >= MAX_RECORDS:
            return done("DAILY_INTAKE_BUDGET")
        slots = state.get("board_cursor", 0)
        board = None
        for offset in range(len(BOARDS)):
            index = (slots+offset) % len(BOARDS)
            candidate = BOARDS[index][0]
            source = state.setdefault("boards", {}).setdefault(candidate, {})
            if not source.get("blocked") and (not source.get("next_poll_at") or now >= stamp(source["next_poll_at"])):
                board = candidate
                state["board_cursor"] = (index+1) % len(BOARDS)
                break
        if board is None:
            return done("SOURCES_BACKED_OFF")
        source = state["boards"][board]
        ids = source.setdefault("ids", [])
        if not ids:
            page = get(board, cursor=source.get("cursor", 0))
            require(isinstance(page, list) and len(page) <= PAGE, "public_page_bounds")
            source["cursor"] = source.get("cursor", 0)+PAGE if len(page) == PAGE else 0
            source["next_poll_at"] = (now+dt.timedelta(hours=1 if len(page) == PAGE else 6)).isoformat()
            # Persist only public role IDs; current content is fetched before intake.
            ids.extend(j["id"] for j in page if isinstance(j, dict) and eligible(j, board))
            persist()
        completed = {u.get("job_identity") for u in ledger.get("units", {}).values() if u.get("status") == "VERIFIED"}
        queued = {":".join(e["intake"].get(k, "") for k in ("provider","board","job_id")) for e in queue["items"]}
        jid = None
        while ids:
            candidate = ids.pop(0)
            key = "lever:"+board+":"+candidate
            if key not in completed and key not in queued and not duplicate(board, candidate):
                jid = candidate
                break
        persist()
        if jid is None:
            return done("NO_NEW_ELIGIBLE_POSTING", board=board)
        job = get(board, jid)
        if not eligible(job, board):
            return done("POSTING_CHANGED", board=board)
        record = record_for(job, board)
        # Frozen effect plan survives timeouts; reservations include unknown writes.
        pending = {"board":board, "job_id":jid, "record":record,
                   "record_sha256":digest(record), "attempts":0, "retry_at":now.isoformat()}
        budget["records"] += 1
        state["pending"] = pending
        persist()
    check()  # Fresh authority CAS immediately before admitted provider mutation.
    pending["attempts"] += 1
    pending["retry_at"] = (now+dt.timedelta(hours=6)).isoformat()
    persist()
    native = ingest(pending["record"])
    check()
    entry = enqueue(pending, native)
    # Queue first; a crash before ledger save reconciles the same native effect.
    if not any(all(e["intake"].get(k) == entry["intake"].get(k) for k in
                   ("tracker_url", "packet_id", "job_id")) for e in queue["items"]):
        queue["items"].append(entry)
    state.pop("pending")
    persist()
    return done("ENQUEUED", board=pending["board"], source_id=pending["record"]["source_id"], native_readback=True)


def run(execute=False):
    if not execute:
        return {"stage":"INCOME_SOURCE_DISCOVERY", "status":"PREVIEW", "boards":[b for b,_ in BOARDS],
                "max_public_reads_per_day":MAX_READS, "max_records_per_day":MAX_RECORDS,
                "interval_seconds":3600, "model_calls":0, "provider_calls":0}
    require(os.geteuid() == 0, "root_required")
    sys.path.insert(0, "/opt/ziji-fabric/dispatch/continuation")
    import income_continuation as controller
    import native_income_backlog as native
    from project_actor_receipts import exact_comment, bind_operational_target_alias
    bind_operational_target_alias()
    import app as bridge
    from ingestion import IngestRecord, IncomeSourceRead, configuration, note_bodies
    from provider_budget import actor_scope
    from scripts.income_backlog import manifest
    now = dt.datetime.now(dt.timezone.utc)
    with contextlib.ExitStack() as stack:
        for path in (controller.ROOT/"controller.lock", Path("/var/lib/ziji-fabric/income/unit.lock")):
            fd = os.open(str(path), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            lock = stack.enter_context(os.fdopen(fd, "w"))
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return {"stage":"INCOME_SOURCE_DISCOVERY", "status":"BUSY", "model_calls":0}
        require(not (controller.ROOT/"pause").exists() and controller.healthy(), "local_model_offline_or_paused")
        queue = controller.read(controller.QUEUE)
        ledger_path = controller.ROOT/"state.json"
        ledger = controller.read(ledger_path)
        require(queue.get("version") == 1 and len(queue["items"]) < 32, "queue_bounds")
        # Finish admitted work first. Backed-off work does not block discovery.
        for entry in queue["items"]:
            unit = ledger.get("units", {}).get(controller.identity(entry["intake"]), {})
            if controller.admitted(entry, now) and unit.get("status") != "VERIFIED" and (
                    not unit.get("next_attempt_at") or now >= controller.timeof(unit["next_attempt_at"])):
                return {"stage":"INCOME_SOURCE_DISCOVERY", "status":"EXISTING_WORK", "model_calls":0}
        state = ledger.setdefault("source_discovery", {})
        def persist():
            controller.save(controller.QUEUE, queue)
            controller.save(ledger_path, ledger)
        def check():
            validate_authority(exact_comment(AUTHORITY["id"]))
            native.authority()
        def audit(action, **fields):
            bridge.append_audit_log({"action":"income_source_discovery_"+action,
                                     "ts":dt.datetime.now(dt.timezone.utc).isoformat(), **fields})
        def fetch(board, jid, cursor):
            audit("public_intent", board=board, role_id=jid, cursor=cursor)
            try:
                value = public_get(board, jid, cursor)
            except PublicFailure as exc:
                audit("public_result", board=board, status=exc.status or "unknown")
                raise
            audit("public_result", board=board, status=200)
            return value
        db = Path(bridge.AUDIT_LOG_PATH).parent/"ingestion.sqlite3"
        config = configuration("income")
        cid = config["default_contact_id"]
        require(re.fullmatch(r"[A-Za-z0-9_-]{1,128}", cid or ""), "internal_context_missing")
        historical = {role for role in manifest()[1]}
        notes_cache = []
        def duplicate(board, jid):
            if ("lever", board, jid) in historical:
                return True
            if not notes_cache:
                with actor_scope("income", db):
                    def get(path):
                        status, data = bridge.highlevel_request("GET", bridge.HIGHLEVEL_BASE_URL.rstrip("/")+path)
                        require(200 <= status < 300, "native_dedup_read_failed")
                        return data
                    contact = get("/contacts/"+cid).get("contact", {})
                    require(contact.get("id") == cid and contact.get("locationId") == bridge.HIGHLEVEL_LOCATION_ID,
                            "internal_context_identity")
                    notes = get("/contacts/"+cid+"/notes").get("notes")
                    require(isinstance(notes, list) and len(notes) <= 10000, "native_dedup_bounds")
                    notes_cache.append(notes)
            return any(jid in str(n.get("body", "")) for n in notes_cache[0])
        def ingest(value):
            record = IngestRecord(**value)
            preview = bridge.ingestion_operations["dry_run"](record, bridge.API_KEY)
            require(preview.get("accepted") is True and preview.get("contact_effect") == "existing_default_contact"
                    and preview.get("opportunity_effect") == "find_or_create_by_source_key"
                    and preview.get("note_parts") == 1, "intake_preview_denied")
            audit("preview", source_sha256=digest(value), preview=preview)
            check()
            result = bridge.ingestion_operations["execute"](record, bridge.API_KEY)
            require(result.get("verified") is True and result.get("contact_id") == cid
                    and result.get("opportunity_id"), "intake_native_unverified")
            source_digest = note_bodies(record)[0].split(":", 3)[2]
            endpoint = next(r.endpoint for r in bridge.app.routes if getattr(r,"path",None) == "/read/ingest/income-source")
            readback = endpoint(IncomeSourceRead(source_id=record.source_id, contact_id=cid,
                opportunity_id=result["opportunity_id"], current_source_sha256=source_digest), bridge.API_KEY)
            require(readback.get("native_readback") is True and readback.get("source_sha256") == source_digest
                    and readback.get("opportunity_status") == "open" and readback.get("source_version_count") == 1,
                    "intake_readback_changed")
            return readback
        def enqueue(pending, value):
            from scripts.income_backlog import facts
            f, reason = facts(value["source"]["properties"], pending["record"]["source_id"])
            require(not reason and f is not None, "intake_facts_unavailable")
            ref = {k:value[k] for k in ("source_id","location_id","contact_id","opportunity_id")}
            ref["current_source_sha256"] = value["source_sha256"]
            entry = native.entry_from({"facts":f,"reference":ref,"tracker_url":controller.highlevel_identity(ref),
                "native_readback":True,"native_opportunity_updated":value["opportunity_updated"]}, now)
            from native_jira_receipt import publish
            receipt = publish("RBO-97", "income:source-intake:"+pending["record"]["source_id"],
                {"lane":"income","outcome":"SUCCESS","source_id":ref["source_id"],
                 "source_sha256":value["source_sha256"],"native_readback":True,
                 "authority_ref":"RBO-97:12221","scope":"public_research_intake_only",
                 "submitted":0,"contacted":0})
            require(receipt.get("native_readback") is True, "intake_jira_receipt_pending")
            return entry
        try:
            return process(state, queue, ledger, now, persist, check, fetch, duplicate, ingest, enqueue)
        except Exception:
            persist()
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    try:
        print(json.dumps(run(args.execute), sort_keys=True))
    except Exception:
        print(json.dumps({"stage":"INCOME_SOURCE_DISCOVERY", "status":"DEFERRED",
                          "reason":"bounded_failure_details_withheld", "model_calls":0, "paid_fallback":False}))
        sys.exit(1)
