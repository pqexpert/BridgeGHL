import hashlib
import json
import sqlite3
from pathlib import Path

import pytest
from income_source_reader import read_income_source, source_key, SourceReadError

def setup_source(tmp_path, versions=1):
    sid, cid, oid = "legacy-source-id", "context", "opportunity"
    key = source_key(sid)
    notes = []
    dbfile = tmp_path / "ingestion.sqlite3"
    with sqlite3.connect(dbfile) as db:
        db.execute("CREATE TABLE effects(key TEXT PRIMARY KEY,state TEXT,native_id TEXT)")
        for n in range(versions):
            raw = json.dumps({"title":"Existing record","source_url":"https://app.notion.com/p/legacy",
                              "properties":{"version":n},"content":"Recorded context"},sort_keys=True)
            digest = hashlib.sha256(raw.encode()).hexdigest()
            body = "bridge-source:"+key+":"+digest+":1/1\n"+raw
            note = {"id":"note"+str(n),"body":body}
            notes.append(note)
            db.execute("INSERT INTO effects VALUES(?,?,?)",
                       ("note:"+cid+":"+hashlib.sha256(body.encode()).hexdigest(),"verified",note["id"]))
    opportunity = {"id":oid,"locationId":"location","pipelineId":"income","contactId":cid,
                   "name":"Existing record [src:"+key+"]","updatedAt":"version"}
    def call(method,path):
        assert method == "GET"
        if path == "/contacts/context": return {"contact":{"id":cid,"locationId":"location"}}
        if path == "/opportunities/opportunity": return {"opportunity":dict(opportunity)}
        if path == "/contacts/context/notes": return {"notes":notes}
        return {"note":next(n for n in notes if path.endswith("/"+n["id"]))}
    binding = {"source_id":sid,"contact_id":cid,"opportunity_id":oid}
    return binding, dbfile, call, notes, opportunity, digest

def test_get_only_native_readback_preserves_legacy_identity_without_admitting(tmp_path):
    binding,db,call,notes,opportunity,digest=setup_source(tmp_path)
    before = db.read_bytes()
    result=read_income_source(binding,"location","income","context",db,call)
    assert result["source_sha256"] == digest
    assert result["operational_system"] == "highlevel"
    assert result["legacy_provenance_urls"] == ["https://app.notion.com/p/legacy"]
    assert result["execution_admitted"] is False and result["admission_projection"] is None
    assert result["native_readback"] is True
    assert db.read_bytes() == before

def test_multiple_versions_need_explicit_digest(tmp_path):
    binding,db,call,notes,opportunity,digest=setup_source(tmp_path,2)
    with pytest.raises(SourceReadError,match="explicit_current_source_digest_required"):
        read_income_source(binding,"location","income","context",db,call)
    result=read_income_source({**binding,"current_source_sha256":digest},"location","income","context",db,call)
    assert result["source"]["properties"]["version"] == 1

def test_unjournaled_notes_and_wrong_pipeline_do_not_establish_truth(tmp_path):
    binding,db,call,notes,opportunity,digest=setup_source(tmp_path)
    opportunity["pipelineId"]="other"
    with pytest.raises(SourceReadError,match="opportunity_scope_mismatch"):
        read_income_source(binding,"location","income","context",db,call)
    opportunity["pipelineId"]="income"
    with sqlite3.connect(db) as connection: connection.execute("UPDATE effects SET state='pending'")
    with pytest.raises(SourceReadError,match="explicit_current_source_digest_required"):
        read_income_source(binding,"location","income","context",db,call)

def test_exact_note_readback_and_complete_parts_required(tmp_path):
    binding,db,call,notes,opportunity,digest=setup_source(tmp_path)
    def changed(method,path):
        result=call(method,path)
        if "/notes/note" in path: return {"note":{"id":"note0","body":"changed"}}
        return result
    with pytest.raises(SourceReadError,match="source_note_changed"):
        read_income_source(binding,"location","income","context",db,changed)
