import json
from pathlib import Path

import pytest

from scripts.install_income_continuation import validate_tracker
from scripts.record_highlevel_cutover import record

def controller():
    bundle=json.loads((Path(__file__).resolve().parents[1]/"scripts/income_continuation_bundle.json").read_text())
    scope={"__name__":"test_controller"}
    exec(compile(bundle["controller"],"pinned_controller","exec"),scope)
    return scope

def test_typed_highlevel_identity_and_digest_preserve_legacy():
    scope=controller()
    ref={"source_id":"stable","location_id":"location","contact_id":"contact",
         "opportunity_id":"opportunity","current_source_sha256":"a"*64}
    item={"intake":{"tracker_url":scope["highlevel_identity"](ref),"tracker_version":"a"*64},
          "canonical":{"highlevel":ref}}
    validate_tracker(item,scope)
    item["intake"]["tracker_version"]="b"*64
    with pytest.raises(ValueError,match="canonical_tracker_scope"):validate_tracker(item,scope)
    historical={"intake":{"tracker_url":"https://app.notion.com/p/original","tracker_version":"old"}}
    validate_tracker(historical,scope)
    assert historical["intake"]["tracker_version"]=="old"

def test_operating_checkpoint_has_no_job_admission_or_contact_effect():
    value=record("location","contact")
    assert value["kind"]=="context" and value["domain"]=="income"
    assert value["source_url"]=="highlevel://location/location/contact/contact"
    assert "fabric_income_admission" not in value["properties"]
    assert value["properties"]["job_admission"] is False
    assert "contact_email" not in value and "contact_name" not in value
    assert value==record("location","contact")
