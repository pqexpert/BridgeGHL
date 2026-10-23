from scripts.income_backlog import facts,role_url

def row():
    return {"Role":"Help Desk","Status":"Harvested","Do Not Auto Submit":"YES",
            "Job URL":"https://jobs.lever.co/example/12345678-1234-1234-1234-123456789abc"}

def test_existing_research_requires_no_submit_and_no_artifact():
    value,reason=facts(row(),"a"*32)
    assert reason is None and value["provider"]=="lever"
    assert value["packet_id"]=="NATIVE-"+("A"*32)
    assert facts({**row(),"Do Not Auto Submit":"__YES__"},"a"*32)[0]["do_not_submit"] is True
    assert facts({**row(),"Do Not Auto Submit":"__NO__"},"a"*32)[1]=="explicit_no_submit_missing"
    for change,expected in [({"Do Not Auto Submit":False},"explicit_no_submit_missing"),
        ({"Asset Link":"https://drive.google.com/file/known"},"artifact_already_recorded"),
        ({"Status":"Applied"},"not_unfinished_research")]:
        assert facts({**row(),**change},"a"*32)[1]==expected

def test_unsupported_or_conflicting_source_is_not_admitted():
    assert role_url({"Job URL":"https://evil.example/jobs/123"}) is None
    assert role_url({**row(),"Other":"https://jobs.ashbyhq.com/other/12345678-1234-1234-1234-123456789abc"}) is None

def test_historical_missing_flag_does_not_hide_current_native_eligibility():
    from unittest.mock import patch
    from scripts import income_backlog as b
    sid="a"*32
    binding={"contact_id":"context","opportunity_id":"opportunity"}
    current={"properties":row()}
    readback={"source":current,"source_sha256":"b"*64,
              "legacy_provenance_urls":["https://app.notion.com/p/"+sid]}
    calls=[]
    def call(method,path,**kwargs):
        calls.append(path)
        if path.endswith("/notes"):return {"notes":[]}
        return {"opportunity":{"status":"open","pipelineStageId":"intake","updatedAt":"current"}}
    with patch.object(b,"discovery_manifest",return_value=([(sid,{"Do Not Auto Submit":None},binding)],{})), \
         patch("ingestion.verified_notes",return_value={}), \
         patch("ingestion.assemble",return_value={"b"*64:{"source":current}}), \
         patch("ingestion.read_income_source",return_value=readback):
        result=b.read_backlog(0,8,location_id="location",pipeline_id="pipeline",
                             stage_id="intake",default_contact_id="context",
                             journal_path="unused",call=call)
    assert len(result["items"])==1
    assert result["items"][0]["facts"]["do_not_submit"] is True
    assert calls.count("/contacts/context/notes")==1
