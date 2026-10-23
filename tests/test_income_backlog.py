from scripts.income_backlog import facts,role_url

def row():
    return {"Role":"Help Desk","Status":"Harvested","Do Not Auto Submit":"YES",
            "Job URL":"https://jobs.lever.co/example/12345678-1234-1234-1234-123456789abc"}

def test_existing_research_requires_no_submit_and_no_artifact():
    value,reason=facts(row(),"a"*32)
    assert reason is None and value["provider"]=="lever"
    assert value["packet_id"]=="NATIVE-"+("A"*32)
    for change,expected in [({"Do Not Auto Submit":False},"explicit_no_submit_missing"),
        ({"Asset Link":"https://drive.google.com/file/known"},"artifact_already_recorded"),
        ({"Status":"Applied"},"not_unfinished_research")]:
        assert facts({**row(),**change},"a"*32)[1]==expected

def test_unsupported_or_conflicting_source_is_not_admitted():
    assert role_url({"Job URL":"https://evil.example/jobs/123"}) is None
    assert role_url({**row(),"Other":"https://jobs.ashbyhq.com/other/12345678-1234-1234-1234-123456789abc"}) is None
