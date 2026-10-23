"""Root-only projection of attributable Jira evidence; never actor mission execution."""
import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from provider_budget import ACTORS, actor_scope, status as budget_status

CONFIG = Path("/etc/ziji-fabric/highlevel-projection.json")
SOURCES = Path("/var/lib/ziji-fabric/native-sources")
STATE = Path("/var/lib/ziji-fabric/highlevel-projection")
TASKS = {"income":"6a96ff3083b8819184b810aa8f640f3d", "rsc":"6a96ff3596ec8191901165a75230bf41",
         "store":"6a96f30dc25081918153e77e5e45f9e0", "distiller":"6a92e1ad92888191a7a8d6ce4fcc1acc"}
OUTCOMES = {"SUCCESS", "NO_CHANGE", "BLOCKED", "FAIL", "FAILED", "PARTIAL", "NOOP", "WAITING", "UNKNOWN"}


def timeof(value):
    value = re.sub(r"([+-]\d\d)(\d\d)$", r"\1:\2", value)
    result = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timestamp_timezone")
    return result


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def private_read(path, limit=1048576):
    st = path.lstat()
    if path.is_symlink() or st.st_uid != 0 or st.st_mode & 0o077 or st.st_size > limit:
        raise ValueError("private_source_permissions")
    return path.read_bytes()


def atomic(path, value, mode=0o600):
    if path.is_symlink():
        raise ValueError("state_symlink")
    fd, name = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(name, mode)
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def validate_policy(policy):
    if set(policy) != {"enabled", "authority", "issuers"} or policy["enabled"] is not True:
        raise ValueError("projection_disabled_or_invalid")
    authority = policy["authority"]
    if set(authority) != {"comment_id", "updated", "issuer_account_id"} or authority["comment_id"] != "12120":
        raise ValueError("projection_authority")
    timeof(authority["updated"])
    if not isinstance(authority["issuer_account_id"], str) or not authority["issuer_account_id"]:
        raise ValueError("projection_authority_issuer")
    if not isinstance(policy["issuers"], dict) or not policy["issuers"]:
        raise ValueError("projection_issuers")
    for issuer, lanes in policy["issuers"].items():
        if not issuer or not isinstance(lanes, list) or not lanes or not set(lanes) <= ACTORS:
            raise ValueError("projection_issuer_scope")
    return policy


def select_receipts(snapshot, policy):
    expected = policy["authority"]
    approval = [x for x in snapshot.get("operational_authority_candidates", []) if str(x.get("id")) == expected["comment_id"]]
    if len(approval) != 1 or any(approval[0].get(k) != v for k, v in {
        "updated":expected["updated"], "author_account_id":expected["issuer_account_id"],
        "update_author_account_id":expected["issuer_account_id"]}.items()):
        raise ValueError("projection_approval_changed_or_missing")
    selected = {}
    for row in snapshot.get("receipt_candidates", []):
        issuer = row.get("author_account_id")
        if issuer not in policy["issuers"] or row.get("update_author_account_id") != issuer:
            continue
        body = row.get("body", "").strip()
        first = body.splitlines()[0] if body else ""
        actor, outcome, kind = None, None, None
        hosted = re.fullmatch(r"FABRIC_RUN (income|rsc|store|distiller):\d{4}-\d{2}-\d{2}:daily\s*[—-]\s*([A-Z_]+)\s*", first)
        if hosted:
            actor, outcome = hosted.groups()
            if not re.search(r"execution[_ ]identity\s*:\s*hosted_task:" + TASKS[actor] + r"(?![a-zA-Z0-9])", body, re.I):
                continue
            kind = "hosted_daily_receipt"
        elif first.startswith(("FABRIC_RUN ", "FABRIC_FINDING ")):
            try:
                obj = json.loads(body.split("\n", 1)[1].strip())
                actor, outcome = obj.get("lane"), obj.get("outcome")
                kind = "native_daily_receipt" if first.startswith("FABRIC_RUN ") else "native_finding_receipt"
            except (ValueError, IndexError, AttributeError):
                continue
        elif first.startswith("FABRIC_CHECKPOINT hosted-distiller:"):
            actor, kind = "distiller", "hosted_distiller_checkpoint"
            # Existing checkpoint format has structured JSON after its header.
            tail = body.split("\n", 1)[1] if "\n" in body else ""
            start = tail.find("{")
            try:
                obj, _ = json.JSONDecoder().raw_decode(tail[start:])
                outcome = obj.get("status", obj.get("outcome"))
            except (ValueError, AttributeError):
                continue
        if actor not in ACTORS or actor not in policy["issuers"][issuer] or outcome not in OUTCOMES:
            continue
        if not re.fullmatch(r"\d+", str(row.get("id", ""))):
            continue
        try:
            created, updated = timeof(row["created"]), timeof(row["updated"])
        except (ValueError, TypeError, KeyError):
            continue
        item = {"actor":actor, "source_receipt_id":str(row["id"]),
                "source_created_at":created.isoformat(), "source_updated_at":updated.isoformat(),
                "source_body_sha256":hashlib.sha256(body.encode()).hexdigest(),
                "source_reported_outcome":outcome, "source_kind":kind}
        previous = selected.get(actor)
        if previous is None or (created, updated, int(row["id"])) > (
            timeof(previous["source_created_at"]), timeof(previous["source_updated_at"]), int(previous["source_receipt_id"])):
            selected[actor] = item
    return selected


def current_snapshot(sources, now):
    report = json.loads(private_read(sources/"latest.json"))
    raw = private_read(sources/"jira.json", 32*1024*1024)
    snapshot = json.loads(raw)
    observed = timeof(report["observed_at"])
    lane = report.get("jira", {})
    if (not 0 <= (now-observed).total_seconds() <= 5400 or
        lane.get("status") != "retrieved_and_persisted" or
        lane.get("snapshot_sha256") != hashlib.sha256(raw).hexdigest() or
        snapshot.get("observed_at") != report["observed_at"] or
        snapshot.get("source_verified") is not True or snapshot.get("coverage") != "all_issue_comments"):
        raise ValueError("current_jira_snapshot_unverified")
    return snapshot


def exact_comment(comment_id):
    # Existing root-only Jira binding. Never copy credentials into this policy.
    sys.path.insert(0, '/opt/ziji-fabric/native')
    from native_jira_receipt import headers
    from bind_native_providers import request, JIRA_ROOT
    from collect_native_sources import text_content
    row = request(JIRA_ROOT+'/rest/api/3/issue/RBO-97/comment/'+str(comment_id), headers())
    author = row.get('author', {}).get('accountId')
    editor = row.get('updateAuthor', {}).get('accountId')
    if 'updateAuthor' not in row and row.get('created') == row.get('updated'):
        editor = author
    return {'id':str(row.get('id')), 'updated':row.get('updated'),
            'author_account_id':author, 'update_author_account_id':editor,
            'body':text_content(row.get('body',{})).strip()}


def verify_current(item, policy, read, approval_verified):
    expected = policy['authority']
    if not approval_verified:
        row = read(expected['comment_id'])
        if (row.get('id') != expected['comment_id'] or row.get('updated') != expected['updated'] or
            row.get('author_account_id') != expected['issuer_account_id'] or
            row.get('update_author_account_id') != expected['issuer_account_id']):
            raise ValueError('projection_approval_cas_changed')
    row = read(item['source_receipt_id'])
    issuer = row.get('author_account_id')
    if (row.get('id') != item['source_receipt_id'] or
        timeof(row.get('updated')).isoformat() != item['source_updated_at'] or
        item['actor'] not in policy['issuers'].get(issuer,[]) or row.get('update_author_account_id') != issuer or
        hashlib.sha256(row.get('body','').encode()).hexdigest() != item['source_body_sha256']):
        raise ValueError('projection_receipt_cas_changed')
    return True


def bind_operational_target_alias():
    """Root script process only: never persist or widen service/customer routes."""
    if os.geteuid() != 0:
        raise ValueError('root_required')
    rsc = 'HIGHLEVEL_INGEST_RSC_DEFAULT_CONTACT_ID'
    income = 'HIGHLEVEL_INGEST_INCOME_DEFAULT_CONTACT_ID'
    if not os.environ.get(rsc) and os.environ.get(income):
        os.environ[rsc] = os.environ[income]
        return {'rsc':'shared_existing_internal_context', 'scope':'this_root_operational_process_only'}
    return {'rsc':'configured_owning_context', 'scope':'this_root_operational_process_only'}


def record(item):
    from ingestion import IngestRecord
    actor = item["actor"]
    # Existing operational context contacts, never a Store customer/contact.
    domain = "rsc" if actor == "rsc" else "income"
    return IngestRecord(domain=domain, source_id="fabric-actor-evidence:"+actor,
        source_url="jira:RBO-97:"+item["source_receipt_id"], kind="context",
        title="Fabric "+actor+" evidence checkpoint",
        properties={"schema":"fabric-actor-evidence/1", **item,
                    "business_authority":False, "mission_execution_proved_by_projection":False},
        content="Operational receipt projection only. Source-reported outcome is not independent mission verification.")


def run(execute=False, sources=SOURCES, state=STATE, config=CONFIG, read=exact_comment):
    if os.geteuid() != 0:
        raise ValueError("root_required")
    now = dt.datetime.now(dt.timezone.utc)
    policy = validate_policy(json.loads(private_read(config)))
    selected = select_receipts(current_snapshot(sources, now), policy)
    target_alias = bind_operational_target_alias()
    import app as bridge
    journal = Path(bridge.AUDIT_LOG_PATH).parent/"ingestion.sqlite3"
    # Validate exact prospective records through existing no-network dry-run.
    manifest = []
    for actor in sorted(selected):
        preview = bridge.ingestion_operations["dry_run"](record(selected[actor]), bridge.API_KEY)
        manifest.append({"actor":actor, **selected[actor], "accepted":preview["accepted"],
                         "source_key":preview["source_key"], "note_parts":preview["note_parts"],
                         "missing_configuration":preview["missing_configuration"],
                         "contact_effect":preview["contact_effect"], "opportunity_effect":preview["opportunity_effect"]})
    if not execute:
        return {"stage":"ACTOR_EVIDENCE_PREVIEW", "manifest":manifest,
                "missing_actors":sorted(ACTORS-set(selected)), "target_alias":target_alias,
                "provider_requests":0, "model_calls":0}
    if any(not row['accepted'] or row['contact_effect'] != 'existing_default_contact'
           or row['opportunity_effect'] != 'none' or row['note_parts'] != 1 for row in manifest):
        raise ValueError('projection_manifest_not_accepted')
    bridge.append_audit_log({'action':'actor_projection_preview', 'authority_ref':'RBO-97:12120',
                             'manifest':manifest, 'observed_at':now.isoformat()})
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    if state.is_symlink():
        raise ValueError("projection_state_symlink")
    state.chmod(0o700)
    with (state/"projection.lock").open("a") as lock:
        os.chmod(state/"projection.lock", 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        prior = json.loads(private_read(state/"state.json")) if (state/"state.json").exists() else {}
        results = {}
        approval_verified = False
        for actor in sorted(selected):
            item = selected[actor]
            fingerprint = digest(item)
            old = prior.get(actor, {})
            if old.get("fingerprint") == fingerprint and old.get("status") == "VERIFIED":
                results[actor] = dict(old, current_provider_read=False, delivery="UNCHANGED_PRIOR_VERIFIED")
                continue
            # Persist a bounded attempt window before any network call. An
            # ambiguous ingestion mutation is reconciled by its existing journal.
            if old.get("fingerprint") == fingerprint and old.get("attempted_at") and (
                now-timeof(old["attempted_at"])).total_seconds() < 3600:
                results[actor] = dict(old, current_provider_read=False, delivery="DEFERRED_BACKOFF")
                continue
            attempt = {**item, "fingerprint":fingerprint, "status":"PENDING",
                       "attempted_at":now.isoformat(), "current_provider_read":False}
            prior[actor] = attempt
            atomic(state/"state.json", prior)
            try:
                approval_verified = verify_current(item, policy, read, approval_verified)
                with actor_scope(actor, journal):
                    result = bridge.ingestion_operations["execute"](record(item), bridge.API_KEY)
                if result.get("verified") is not True or result.get("opportunity_id") or not result.get("note_ids"):
                    raise ValueError("projection_native_readback")
                attempt.update(native_ids={k:result.get(k) for k in ("contact_id","note_ids","opportunity_id")},
                               status="VERIFIED", native_readback_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                               current_provider_read=True, delivery="WRITTEN_OR_RECONCILED")
            except Exception:
                attempt.update(status="DEFERRED", delivery="RECONCILE_OR_BUDGET_PROVIDER")
            prior[actor] = attempt
            atomic(state/"state.json", prior)
            results[actor] = attempt
        results = {actor:{k:v for k,v in row.items() if k != "native_ids"} for actor,row in results.items()}
        report = {"stage":"ACTOR_EVIDENCE_PROJECTION", "observed_at":now.isoformat(), "actors":results,
                  "missing_actors":sorted(ACTORS-set(selected)), "model_calls":0,
                  "mission_executions":0, "target_alias":target_alias, "budget":budget_status(journal)}
        atomic(state/"latest.json", report)
        # Deliberately public-to-local-service sanitized metadata: no source body,
        # issuer IDs, contact IDs, credentials, customer or Calendar information.
        atomic(journal.parent/"actor-projection-status.json", report, 0o644)
        return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--install-policy", action="store_true")
    parser.add_argument("--install-collector", action="store_true")
    args = parser.parse_args()
    try:
        if args.install_policy:
            if os.geteuid() != 0:
                raise ValueError('root_required')
            policy_source = Path(__file__).resolve().parents[1]/'config/highlevel-actor-policy.json'
            policy = validate_policy(json.loads(policy_source.read_text()))
            CONFIG.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if CONFIG.exists() and json.loads(private_read(CONFIG)) != policy:
                raise ValueError('existing_projection_policy_preserved')
            atomic(CONFIG, policy)
        installed = None
        if args.install_collector:
            if os.geteuid() != 0:
                raise ValueError('root_required')
            root = Path(__file__).resolve().parents[1]
            bundle = json.loads((root/'scripts/income_continuation_bundle.json').read_text())
            if hashlib.sha256((root/'scripts/install_income_continuation.py').read_bytes()).hexdigest() != bundle['hashes']['installer']:
                raise ValueError('installer_source_hash')
            sys.path.insert(0, '/opt/ziji-fabric/native')
            from install_income_continuation import install
            installed = install({'mode':'continuation-refresh','continuation':bundle})
            import importlib, collect_native_sources
            collector = importlib.reload(collect_native_sources)
            collection = collector.collect('installation_validation')
            if collection.get('jira',{}).get('status') != 'retrieved_and_persisted':
                raise ValueError('current_jira_collection_failed')
        result = run(args.execute)
        if installed is not None:
            result['collector_install'] = installed
        print(json.dumps(result, sort_keys=True))
    except Exception as exc:
        # Do not print raw provider/config errors or private input.
        print(json.dumps({"stage":"ACTOR_EVIDENCE_PROJECTION", "status":"DEFERRED",
                          "reason":str(exc) if type(exc) is ValueError and re.fullmatch("[a-z_]+", str(exc)) else "bounded_failure_details_withheld",
                          "model_calls":0}, sort_keys=True))
        sys.exit(1)
