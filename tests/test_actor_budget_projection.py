import concurrent.futures
import datetime as dt
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest
import provider_budget as budget
from scripts import project_actor_receipts as projection


def policy():
    return {"enabled":True, "authority":{"comment_id":"12120", "updated":"2026-10-23T07:28:43.970-0500",
            "issuer_account_id":"publisher"}, "issuers":{"publisher":sorted(budget.ACTORS)}}


def snapshot():
    value = {"source_verified":True, "coverage":"all_issue_comments",
        "operational_authority_candidates":[{"id":"12120", "updated":policy()["authority"]["updated"],
            "author_account_id":"publisher", "update_author_account_id":"publisher"}],
        "receipt_candidates":[]}
    for n, actor in enumerate(sorted(budget.ACTORS)):
        body = ("FABRIC_CHECKPOINT hosted-distiller:2026-10-23_00-04_DELTA\n" + json.dumps({"status":"PARTIAL"})
                if actor == "distiller" else
                "FABRIC_RUN "+actor+":2026-10-23:daily — NO_CHANGE\nexecution_identity:hosted_task:"+projection.TASKS[actor])
        value["receipt_candidates"].append({"id":str(13000+n), "created":"2026-10-23T12:00:00Z",
            "updated":"2026-10-23T12:00:00Z", "author_account_id":"publisher",
            "update_author_account_id":"publisher", "body":body+"\nprivate customer text"})
    return value


def test_budget_reservation_survives_restart_and_concurrent_workers(tmp_path, monkeypatch):
    monkeypatch.setenv("HIGHLEVEL_ACTOR_REQUESTS_PER_UTC_DAY", "3")
    path = tmp_path/"ingestion.sqlite3"
    def reserve_one(_):
        try:
            with budget.actor_scope("income", path):
                budget.reserve(now=1800000000)
            return True
        except budget.BudgetDeferred:
            return False
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        assert sum(pool.map(reserve_one, range(10))) == 3
    # A fresh context/connection cannot reset the durable reservation.
    assert not reserve_one(0)
    assert budget.status(path, now=1800000000)["shared_reserved"] == 3
    with budget.actor_scope("income", path):
        assert budget.reserve(now=1800000000+86400)


def test_shared_cap_and_provider_cooldown_persist(tmp_path, monkeypatch):
    monkeypatch.setenv("HIGHLEVEL_SHARED_REQUESTS_PER_UTC_DAY", "2")
    path=tmp_path/"ingestion.sqlite3"
    for actor in ("income", "store"):
        with budget.actor_scope(actor,path): budget.reserve(now=1800000000)
    with budget.actor_scope("rsc",path), pytest.raises(budget.BudgetDeferred,match="utc_daily"):
        budget.reserve(now=1800000000)
    with budget.actor_scope("income",path):
        budget.observe(429, {"Retry-After":"120"}, now=1800000000)
    with budget.actor_scope("distiller",path), pytest.raises(budget.BudgetDeferred,match="cooldown"):
        budget.reserve(now=1800000060)
    assert budget.status(path,now=1800000060)["shared_reserved"]==2


def test_observed_lower_provider_capacity_and_day_probe(tmp_path):
    path=tmp_path/"ingestion.sqlite3"
    with budget.actor_scope("income",path):
        budget.observe(200, {"X-RateLimit-Daily-Remaining":"0"}, now=1800000000)
        with pytest.raises(budget.BudgetDeferred,match="daily_capacity"):
            budget.reserve(now=1800000100)
        assert budget.reserve(now=1800000000+86400)


def test_private_projection_four_real_formats_no_private_body():
    rows=snapshot()
    picked=projection.select_receipts(rows,projection.validate_policy(policy()))
    assert set(picked)==budget.ACTORS
    assert picked["distiller"]["source_reported_outcome"]=="PARTIAL"
    for item in picked.values():
        record=projection.record(item)
        assert record.kind=="context" and record.contact_email is None
        assert "private customer text" not in record.model_dump_json()
        assert record.properties["mission_execution_proved_by_projection"] is False
    rows["receipt_candidates"][0]["update_author_account_id"]="unknown"
    assert len(projection.select_receipts(rows,policy()))==3
    rows["operational_authority_candidates"][0]["updated"]="2026-10-23T13:00:00Z"
    with pytest.raises(ValueError,match="approval_changed"):
        projection.select_receipts(rows,policy())


def test_generic_delta_is_not_distiller_authority():
    rows=snapshot()
    rows["receipt_candidates"]=[dict(rows["receipt_candidates"][0],
        body='FABRIC_DELTA_CHECKPOINT — 2026-10-23 08:04 EDT\n{"status":"SUCCESS"}')]
    assert projection.select_receipts(rows,policy())=={}


def test_stale_or_failed_current_collection_cannot_reuse_prior_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(projection,"private_read",lambda p,limit=1048576:p.read_bytes())
    now=dt.datetime(2026,10,23,12,0,tzinfo=dt.timezone.utc)
    value=snapshot();value["observed_at"]=now.isoformat()
    raw=json.dumps(value).encode();(tmp_path/"jira.json").write_bytes(raw)
    report={"observed_at":now.isoformat(),"jira":{"status":"retrieved_and_persisted",
        "snapshot_sha256":hashlib.sha256(raw).hexdigest()}}
    (tmp_path/"latest.json").write_text(json.dumps(report))
    assert projection.current_snapshot(tmp_path,now)==value
    report["jira"]["status"]="failed"
    (tmp_path/"latest.json").write_text(json.dumps(report))
    with pytest.raises(ValueError,match="unverified"):projection.current_snapshot(tmp_path,now)
    report["jira"]["status"]="retrieved_and_persisted";report["jira"]["snapshot_sha256"]="0"*64
    (tmp_path/"latest.json").write_text(json.dumps(report))
    with pytest.raises(ValueError,match="unverified"):projection.current_snapshot(tmp_path,now)


def test_replay_after_restart_makes_no_provider_calls(tmp_path, monkeypatch):
    monkeypatch.setattr(projection.os,"geteuid",lambda:0)
    monkeypatch.setattr(projection,"private_read",lambda p,limit=1048576:p.read_bytes())
    monkeypatch.setattr(projection,"current_snapshot",lambda sources,now:snapshot())
    cfg=tmp_path/"policy.json";cfg.write_text(json.dumps(policy()))
    calls=[]
    def execute(record, key):
        calls.append(budget.CURRENT.get()[0])
        return {"verified":True,"note_ids":["note"],"opportunity_id":None}
    bridge=SimpleNamespace(AUDIT_LOG_PATH=str(tmp_path/"audit.jsonl"),API_KEY="test",append_audit_log=lambda e:None,
        ingestion_operations={"execute":execute,"dry_run":lambda r,k:{
            "accepted":True,"source_key":"key","note_parts":1,"missing_configuration":[],
            "contact_effect":"existing_default_contact","opportunity_effect":"none"}})
    monkeypatch.setitem(sys.modules,"app",bridge)
    fresh = {row['id']:dict(row,body=row['body'].strip()) for row in snapshot()['receipt_candidates']}
    fresh['12120'] = snapshot()['operational_authority_candidates'][0]
    cas=[]
    def read(cid):
        cas.append(cid)
        return fresh[cid]
    first=projection.run(True,state=tmp_path/"state",config=cfg,read=read)
    assert set(calls)==budget.ACTORS and len(calls)==4
    calls.clear()
    cas.clear()
    second=projection.run(True,state=tmp_path/"state",config=cfg,read=read)
    assert cas==[]
    assert calls==[]
    assert all(x["delivery"]=="UNCHANGED_PRIOR_VERIFIED" and not x["current_provider_read"]
               for x in second["actors"].values())


def test_transport_reserves_failed_attempt_once_without_implicit_retry(tmp_path,monkeypatch):
    import app
    monkeypatch.setattr(app,"HIGHLEVEL_PIT","test-only")
    monkeypatch.setattr(app,"HIGHLEVEL_LOCATION_ID","loc")
    monkeypatch.setattr(app,"AUDIT_LOG_PATH",str(tmp_path/"audit.jsonl"))
    calls=[]
    def request(*a,**kw):
        calls.append(1)
        return SimpleNamespace(status_code=429,headers={"Retry-After":"60"},json=lambda:{"error":"limited"})
    monkeypatch.setattr(app.requests,"request",request)
    assert app.highlevel_request("GET","https://example.invalid")[0]==429
    assert len(calls)==1
    assert budget.status(tmp_path/"ingestion.sqlite3")["requests_reserved"]=={"rsc":1}
    with pytest.raises(app.HTTPException) as exc:
        app.highlevel_request("GET","https://example.invalid")
    assert exc.value.status_code==429 and len(calls)==1


def test_existing_ingestion_journal_and_budget_transactions_do_not_deadlock(tmp_path, monkeypatch):
    import app
    from ingestion import IngestRecord
    monkeypatch.setattr(app, "HIGHLEVEL_PIT", "test-only")
    monkeypatch.setattr(app, "HIGHLEVEL_LOCATION_ID", "loc")
    monkeypatch.setattr(app, "HIGHLEVEL_BASE_URL", "https://example.invalid")
    monkeypatch.setattr(app, "AUDIT_LOG_PATH", str(tmp_path/"audit.jsonl"))
    monkeypatch.setattr(app, "API_KEY", "test-key")
    monkeypatch.setattr(app, "bridge_health_snapshot", lambda:SimpleNamespace(state="HEALTHY"))
    monkeypatch.setattr(app, "append_audit_log", lambda e:None)
    monkeypatch.setenv("HIGHLEVEL_INGEST_ENABLED", "true")
    monkeypatch.setenv("HIGHLEVEL_INGEST_INCOME_DEFAULT_CONTACT_ID", "internal")
    notes={}
    def request(method,url,**kw):
        path=url.removeprefix("https://example.invalid")
        if path=="/contacts/internal":
            data={"contact":{"id":"internal","locationId":"loc"}}
        elif path=="/contacts/internal/notes" and method=="GET":
            data={"notes":list(notes.values())}
        elif path=="/contacts/internal/notes":
            notes["n"]={"id":"n",**kw["json"]};data={"note":notes["n"]}
        else:
            data={"note":notes["n"]}
        return SimpleNamespace(status_code=200,headers={},json=lambda:data)
    monkeypatch.setattr(app.requests,"request",request)
    record=IngestRecord(domain="income",kind="context",source_id="fabric-proof",
                        source_url="jira:RBO-97:12120",title="Operational context")
    with budget.actor_scope("store",tmp_path/"ingestion.sqlite3"):
        result=app.ingestion_operations["execute"](record,"test-key")
    assert result["verified"] and len(notes)==1
    assert budget.status(tmp_path/"ingestion.sqlite3")["requests_reserved"]=={"store":4}


def test_changed_native_receipt_stops_projection_cas():
    rows=snapshot();item=projection.select_receipts(rows,policy())["store"]
    authority=rows["operational_authority_candidates"][0]
    source=next(x for x in rows["receipt_candidates"] if x["id"]==item["source_receipt_id"])
    source=dict(source,body=source["body"]+" edited")
    with pytest.raises(ValueError,match="receipt_cas_changed"):
        projection.verify_current(item,policy(),lambda cid:authority if cid=="12120" else source,False)


def test_out_of_order_provider_capacity_cannot_increase_remaining(tmp_path):
    path=tmp_path/"ingestion.sqlite3"
    with budget.actor_scope("income",path):
        budget.observe(200,{"X-RateLimit-Daily-Remaining":"8"},now=1800000000)
        budget.observe(200,{"X-RateLimit-Daily-Remaining":"10"},now=1800000001)
    assert budget.status(path,now=1800000001)["provider_daily_remaining"]==8


def test_rsc_operational_alias_preserves_existing_binding_and_source_domain(monkeypatch):
    monkeypatch.setattr(projection.os, 'geteuid', lambda:0)
    monkeypatch.delenv('HIGHLEVEL_INGEST_RSC_DEFAULT_CONTACT_ID', raising=False)
    monkeypatch.setenv('HIGHLEVEL_INGEST_INCOME_DEFAULT_CONTACT_ID', 'verified-internal-context')
    assert projection.bind_operational_target_alias()['rsc'] == 'shared_existing_internal_context'
    assert projection.os.environ['HIGHLEVEL_INGEST_RSC_DEFAULT_CONTACT_ID'] == 'verified-internal-context'
    item = projection.select_receipts(snapshot(), policy())['rsc']
    assert projection.record(item).domain == 'rsc'
    monkeypatch.setenv('HIGHLEVEL_INGEST_RSC_DEFAULT_CONTACT_ID', 'existing-rsc-context')
    projection.bind_operational_target_alias()
    assert projection.os.environ['HIGHLEVEL_INGEST_RSC_DEFAULT_CONTACT_ID'] == 'existing-rsc-context'
