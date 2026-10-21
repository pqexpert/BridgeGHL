import datetime as dt
import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location("alarm",Path(__file__).parents[1]/"scripts/fabric_failure_alarm.py")
a=importlib.util.module_from_spec(spec);spec.loader.exec_module(a)
NOW=dt.datetime(2026,10,21,18,tzinfo=dt.timezone.utc)
def fixture():
 snapshot={"source_verified":True,"observed_at":NOW.isoformat(),"receipt_candidates":[]}
 ledger=[]
 for lane in a.DUE:
  for date in ("2026-10-20","2026-10-21"):
   ledger.append({"status":"VERIFIED","native_readback":True,"issue":"RBO-97","comment_id":"1",
    "replay_id":lane+":"+date+":daily","receipt":{"lane":lane,"outcome":"NO_CHANGE","native_readback":True,"local_date":date}})
 full={"mode":"FULL","run_id":"2026-10-21_11-45_FULL","observed_at":NOW.isoformat(),
       "status":"NO_CHANGE","checkpoint":{"verified":True},"coverage":[{"kind":"baseline","complete":True}]}
 return snapshot,ledger,full
def evaluate(snapshot,ledger,full,state=None):
 return a.evaluate(NOW,snapshot,ledger,full,[full] if full else [],state or {"feedback":{}})
class AlarmTests(unittest.TestCase):
 def test_healthy_empty_feedback_and_replay_silent(self):
  s,l,f=fixture();r=evaluate(s,l,f)
  self.assertEqual(r["alerts"],[]);self.assertTrue(r["comparable_coverage"])
  self.assertEqual(evaluate(s,l,f)["alerts"],r["alerts"])
 def test_subset_not_whole_actor_terminal(self):
  s,l,f=fixture()
  l=[x for x in l if x["receipt"]["lane"]!="rsc"]
  l.append({"status":"VERIFIED","native_readback":True,"receipt":{"lane":"rsc","outcome":"SUCCESS","whole_actor_replaced":False}})
  r=evaluate(s,l,f)
  self.assertFalse(r["comparable_coverage"])
  self.assertEqual(r["checks"]["rsc"]["whole_actor_health"],"UNKNOWN")
  self.assertIn("P1:rsc:repeated_due_receipt_gap",r["alerts"])
 def test_unparsed_hosted_evidence_prevents_false_missing_alarm(self):
  s,l,f=fixture();s["receipt_candidates"]=[{"body":"FABRIC_RUN legacy text","created":NOW.isoformat()}]
  r=evaluate(s,[],f)
  self.assertFalse(r["comparable_coverage"])
  self.assertFalse(any("receipt_gap" in x for x in r["alerts"]))
 def test_actual_legacy_header_is_terminal_but_not_invented_effect_proof(self):
  s,l,f=fixture();l=[x for x in l if x["receipt"]["lane"]!="store"]
  s["receipt_candidates"]=[{"body":"FABRIC_RUN store:2026-10-21:daily — SUCCESS\nobserved_utc:2026-10-21T13:03:05Z | execution_identity:hosted_task:fixture\nEffect/readback: narrative",
      "created":NOW.isoformat(),"reference":"jira:fixture"}]
  r=evaluate(s,l,f)
  self.assertIn("2026-10-21",r["checks"]["store"]["terminal_dates"])
  self.assertIn("2026-10-21",r["checks"]["store"]["unverified_effect_dates"])
  self.assertFalse(r["comparable_coverage"])
  self.assertFalse(any("store:repeated_due_receipt_gap" in x for x in r["alerts"]))
 def test_new_verified_p0_and_explicit_recovery(self):
  s,l,f=fixture();record=l[-1]["receipt"]
  record["material_conditions"]=[{"code":"stop_dnd_violation","verified":True,"active":True}]
  self.assertIn("P0:store:stop_dnd_violation",evaluate(s,l,f)["alerts"])
  record["material_conditions"][0]["active"]=False
  self.assertNotIn("P0:store:stop_dnd_violation",evaluate(s,l,f)["alerts"])
 def test_known_feedback_24h_after_full_only(self):
  s,l,f=fixture()
  state={"feedback":{"id":{"classification":"OWNER_REPORTED","status":"PENDING_VERIFICATION","observed_at":"2026-10-20T12:00:00Z"}}}
  self.assertIn("P1:distiller:verified_owner_feedback_unconsumed_24h",evaluate(s,l,f,state)["alerts"])
  state["feedback"]["id"]["classification"]="AMBIGUOUS"
  self.assertNotIn("P1:distiller:verified_owner_feedback_unconsumed_24h",evaluate(s,l,f,state)["alerts"])
 def test_full_stale_and_unknown_cost(self):
  s,l,f=fixture();f["observed_at"]="2026-10-19T12:00:00Z"
  r=evaluate(s,l,f)
  self.assertIn("P1:distiller:full_readback_older_than_36h",r["alerts"])
  self.assertTrue(r["checks"]["provider_cost"].startswith("UNKNOWN"))
if __name__=="__main__":unittest.main()
