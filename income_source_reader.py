"""Read existing Income source notes through the governed bridge; never admit or mutate."""
import hashlib
import json
import re
import sqlite3
from pathlib import Path
from urllib.parse import quote

class SourceReadError(ValueError):
    pass

def fail(condition, reason):
    if not condition: raise SourceReadError(reason)

def source_key(source_id):
    return hashlib.sha256(("income:"+source_id).encode()).hexdigest()[:32]

def verified_notes(journal_path, contact_id):
    path = Path(journal_path)
    fail(path.is_file() and not path.is_symlink(), "verified_journal_unavailable")
    # No table creation, locks, journal updates or write-capable connection.
    with sqlite3.connect("file:"+quote(str(path.resolve()),safe="/")+"?mode=ro", uri=True) as db:
        return {key:native_id for key,state,native_id in db.execute(
            "SELECT key,state,native_id FROM effects WHERE key LIKE ?", ("note:"+contact_id+":%",))
            if state == "verified" and native_id}

def assemble(notes, key, contact_id, verified):
    groups = {}
    for note in notes:
        body = note.get("body", "")
        if not isinstance(body, str) or len(body) > 13000: continue
        header, sep, part = body.partition("\n")
        match = re.fullmatch(r"bridge-source:"+re.escape(key)+r":([a-f0-9]{64}):([0-9]+)/([0-9]+)",header)
        if not sep or not match: continue
        digest, index, total = match.group(1), int(match.group(2)), int(match.group(3))
        journal_key = "note:"+contact_id+":"+hashlib.sha256(body.encode()).hexdigest()
        if verified.get(journal_key) != note.get("id"): continue
        fail(1 <= index <= total <= 21, "source_parts_invalid")
        group = groups.setdefault(digest, {"total":total,"parts":{}})
        fail(group["total"] == total and index not in group["parts"], "source_parts_ambiguous")
        group["parts"][index] = (part, note["id"], body)
    complete = {}
    for digest, group in groups.items():
        if set(group["parts"]) != set(range(1,group["total"]+1)): continue
        raw = "".join(group["parts"][i][0] for i in range(1,group["total"]+1))
        fail(hashlib.sha256(raw.encode()).hexdigest() == digest, "source_digest_mismatch")
        data = json.loads(raw)
        fail(isinstance(data,dict) and isinstance(data.get("properties"),dict), "source_shape_invalid")
        complete[digest] = {"source":data,"parts":group["parts"]}
    return complete

def read_income_source(binding, location_id, pipeline_id, default_contact_id, journal_path, call):
    """GET-only exact source read. Explicit digest or a single verified version is required."""
    source_id, cid = binding["source_id"], binding["contact_id"]
    oid = binding.get("opportunity_id")
    for value in (cid, oid or cid):
        fail(isinstance(value,str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}",value), "native_identity_invalid")
    fail(isinstance(source_id,str) and 0 < len(source_id) <= 200, "source_identity_invalid")
    key = source_key(source_id)
    contact = call("GET","/contacts/"+cid).get("contact",{})
    fail(contact.get("id")==cid and contact.get("locationId")==location_id, "contact_scope_mismatch")
    opportunity = None
    if oid:
        opportunity = call("GET","/opportunities/"+oid).get("opportunity",{})
        fail(opportunity.get("id")==oid and opportunity.get("locationId")==location_id
             and opportunity.get("pipelineId")==pipeline_id and opportunity.get("contactId")==cid
             and opportunity.get("name","").endswith("[src:"+key+"]"), "opportunity_scope_mismatch")
    else:
        fail(cid == default_contact_id, "context_contact_scope_mismatch")
    verified = verified_notes(journal_path,cid)
    notes = call("GET","/contacts/"+cid+"/notes").get("notes")
    fail(isinstance(notes,list) and len(notes)<=10000, "source_notes_unavailable_or_unbounded")
    versions = assemble(notes,key,cid,verified)
    digest = binding.get("current_source_sha256")
    if digest is None:
        fail(len(versions)==1, "explicit_current_source_digest_required")
        digest = next(iter(versions))
    fail(isinstance(digest,str) and re.fullmatch(r"[a-f0-9]{64}",digest), "source_digest_invalid")
    fail(digest in versions, "selected_source_not_verified_complete")
    selected = versions[digest]
    # Native readback of each selected exact note, not list-response inference.
    for _,nid,body in selected["parts"].values():
        actual = call("GET","/contacts/"+cid+"/notes/"+nid).get("note",{})
        fail(actual.get("id")==nid and actual.get("body")==body, "source_note_changed")
    if oid:
        current = call("GET","/opportunities/"+oid).get("opportunity",{})
        fail(all(current.get(k)==opportunity.get(k) for k in
                 ("id","locationId","pipelineId","contactId","name","updatedAt","status","pipelineStageId")),
             "opportunity_changed")
    source = selected["source"]
    explicit = source["properties"].get("fabric_income_admission")
    fail(explicit is None or isinstance(explicit,dict), "admission_projection_invalid")
    if explicit is not None:
        fail(bool(oid), "opportunity_required_for_research_projection")
    return {"domain":"income","operational_system":"highlevel","source_id":source_id,
            "source_key":key,"source_sha256":digest,"location_id":location_id,"contact_id":cid,
            "opportunity_id":oid,"note_ids":[x[1] for x in selected["parts"].values()],
            "native_readback":True,"source":source,"admission_projection":explicit,
            "execution_admitted":False,"provider_mutations":0,"model_calls":0,
            "legacy_provenance_urls":sorted({v["source"].get("source_url","") for v in versions.values()
                if isinstance(v["source"].get("source_url"),str)}),
            "identity_rule":"Preserve original source ID and verified completion/cache aliases; operational system is HighLevel."}
