"""Explicitly invoked, fixed non-PII operating checkpoint through existing bridge routes."""
import argparse
import json
import os
from pathlib import Path
import re
import sys
import tempfile

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

def record(location_id, contact_id):
    for value in (location_id, contact_id):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value or ""):
            raise ValueError("income_context_binding_invalid")
    return {"domain":"income","kind":"context",
        "source_id":"fabric-highlevel-operating-checkpoint-2026-10-23",
        "source_url":"highlevel://location/"+location_id+"/contact/"+contact_id,
        "title":"HighLevel operational cutover checkpoint — 2026-10-23",
        "properties":{
            "schema_version":"highlevel-operating-checkpoint/1",
            "record_scope":"operating_context_only","operational_system":"highlevel",
            "job_admission":False,"claimed_full_actor_autonomy":False,
            "source_ingestion":{"pull_request":101,"verified_deployment_run":39899035886},
            "source_reader":{"pull_request":103,"verified_deployment_run":39900004164},
            "operational_task_prompts":"Updated to HighLevel-first by owning operator; hosted uncovered scope retained.",
            "rsc_funnel":{"id":"2VdakruoOGdefWNci8pW",
                "published_url":"https://go.restorationsecurityoutreach.com/rsc-security-consultation-page",
                "form_id":"z3HWJ4Zx58oSRGaBNGe8","form":"PQExpert Professional Inquiry"},
            "rsc_community":{"slug":"rsc-security-resilience","created":True,
                "discovery_enabled":False,"owner_first_password_pending":True,"handover_complete":False},
            "identity_policy":"Preserve stable source IDs, historical URL aliases, completed work and original cached artifacts.",
            "execution_limits":"No synthetic job; no inference, outreach, membership invitation or paid spend from this checkpoint."
        },
        "content":"HighLevel is the operational destination. This note records real deployment and operator work, not a career opportunity or execution grant. Historical Notion references remain provenance only. Native source-reader availability does not itself prove controller admission-source handover."}

def run():
    import app as bridge
    from ingestion import IngestRecord, note_bodies
    if os.geteuid() != 0: raise ValueError("root_required")
    location = bridge.HIGHLEVEL_LOCATION_ID
    cid = os.environ.get("HIGHLEVEL_INGEST_INCOME_DEFAULT_CONTACT_ID","")
    body = record(location,cid)
    expected = note_bodies(IngestRecord(**body))
    digest = expected[0].split("\n",1)[0].split(":")[2]
    session = requests.Session()
    session.trust_env = False
    def call(path, data):
        response = session.post("http://127.0.0.1:8000"+path,json=data,
            headers={"X-API-Key":bridge.API_KEY},timeout=150,allow_redirects=False)
        if response.status_code != 200: raise ValueError("checkpoint_bridge_http_"+str(response.status_code))
        return response.json()
    preview = call("/dry-run/ingest/source-record",body)
    if preview.get("accepted") is not True: raise ValueError("checkpoint_preview_not_admitted")
    written = call("/execute/ingest/source-record",body)
    if (written.get("verified") is not True or written.get("contact_id") != cid
        or written.get("opportunity_id") is not None or not written.get("note_ids")):
        raise ValueError("checkpoint_write_unverified")
    observed = call("/read/ingest/income-source",{"source_id":body["source_id"],
        "contact_id":cid,"current_source_sha256":digest})
    expected_source = {"title":body["title"],"source_url":body["source_url"],
                       "properties":body["properties"],"content":body["content"]}
    if (observed.get("native_readback") is not True or observed.get("source_sha256") != digest
        or observed.get("source") != expected_source or observed.get("admission_projection") is not None
        or observed.get("execution_admitted") is not False
        or set(observed.get("note_ids",[])) != set(written["note_ids"])):
        raise ValueError("checkpoint_native_readback_mismatch")
    receipt = {"stage":"HIGHLEVEL_OPERATING_CHECKPOINT","verified":True,
        "source_id":body["source_id"],"source_key":written["source_key"],
        "source_sha256":digest,"contact_id":cid,"note_ids":written["note_ids"],
        "operational_system":"highlevel","model_calls":0,"job_admissions":0,"messages_sent":0}
    destination = Path("/var/lib/bridgeghl/career-import/highlevel-cutover-receipt.json")
    destination.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    if destination.is_symlink(): raise ValueError("receipt_path_unsafe")
    fd, temporary = tempfile.mkstemp(dir=destination.parent)
    try:
        with os.fdopen(fd,"w") as out:
            json.dump(receipt,out,sort_keys=True);out.flush();os.fsync(out.fileno())
        os.chmod(temporary,0o600);os.replace(temporary,destination)
    finally:
        if os.path.exists(temporary):os.unlink(temporary)
    print(json.dumps({k:v for k,v in receipt.items() if k not in ("contact_id","note_ids")},sort_keys=True))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute",action="store_true",required=True)
    parser.parse_args()
    try:run()
    except Exception as exc:
        code = str(exc) if isinstance(exc,ValueError) and re.fullmatch(r"[a-z0-9_]+",str(exc)) else "checkpoint_unverified"
        print(json.dumps({"stage":"HIGHLEVEL_OPERATING_CHECKPOINT","verified":False,
                          "reason":code,"private_details_withheld":True}))
        raise SystemExit(1)
