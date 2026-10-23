import copy
import datetime as dt
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("request_queue", Path(__file__).parents[1]/"scripts/highlevel_request_queue.py")
queue = importlib.util.module_from_spec(spec)
spec.loader.exec_module(queue)
NOW = dt.datetime(2026, 10, 23, 12, tzinfo=dt.timezone.utc)
POLICY = {"enabled":True, "authority":{"comment_id":"12120", "updated":"2026-10-23T10:00:00.000-0500",
          "issuer_account_id":"owner"}, "issuers":{"owner":["income", "rsc"]}}


def row(operation="income.write_context"):
    import json
    obj = {"schema_version":1, "request_id":"actual-plan-2026-10-23", "lane":"income",
           "operation":operation, "authority_ref":"RBO-97:12120", "observed_at":"2026-10-23T11:00:00Z",
           "expires_at":"2026-10-24T11:00:00Z", "payload":{
               "source_id":"fabric-highlevel-automatic-access-plan-2026-10-23",
               "source_url":queue.AUTHORITY_URL, "title":"Approved operating plan",
               "content":"Use current bounded HighLevel routes and preserve native evidence."}}
    if operation == "income.read_source":
        obj["payload"] = {"source_id":"fabric-highlevel-operating-checkpoint-2026-10-23",
                          "current_source_sha256":"a"*64}
    return {"id":"12345", "created":"2026-10-23T11:01:00Z", "updated":"2026-10-23T11:01:00Z",
            "author_account_id":"owner", "update_author_account_id":"owner",
            "body":queue.MARKER+"\n"+json.dumps(obj)}


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = queue.Journal(Path(self.tmp.name)/"ingestion.sqlite3")
        self.item = row()
        self.effects = []
        self.receipts = []
        self.authority = {"id":"12120", "updated":POLICY["authority"]["updated"],
                          "author_account_id":"owner", "update_author_account_id":"owner"}

    def exact(self, cid):
        return self.authority if cid == "12120" else self.item

    def invoke(self, obj):
        self.effects.append(obj)
        return {"verified":True, "source_key":"key", "contact_id":"existing", "opportunity_id":None, "note_ids":["note"]}

    def publish(self, issue, key, result):
        self.receipts.append(copy.deepcopy(result))
        return {"status":"VERIFIED", "native_readback":True, "comment_id":"12346"}

    def run_queue(self, exact=None, invoke=None, publish=None, items=None):
        return queue.process({"highlevel_request_candidates":items or [self.item]}, POLICY,
            self.journal, exact or self.exact, invoke or self.invoke, publish or self.publish, NOW, True)

    def test_write_dedupes_and_preserves_exact_receipt(self):
        first = self.run_queue()
        second = self.run_queue()
        self.assertEqual(first["requests"][0]["status"], "VERIFIED")
        self.assertTrue(second["requests"][0]["cached"])
        self.assertEqual(len(self.effects), 1)
        self.assertEqual(len(self.receipts), 1)
        self.assertEqual(self.effects[0]["payload"]["source_url"], queue.AUTHORITY_URL)

    def test_editor_or_authority_change_blocks_before_effect(self):
        edited = copy.deepcopy(self.item)
        edited["update_author_account_id"] = "other"
        self.assertEqual(self.run_queue(items=[edited])["requests"], [])
        self.authority["updated"] = "2026-10-23T12:00:00Z"
        self.assertEqual(self.run_queue()["requests"][0]["status"], "DEFERRED")
        self.assertFalse(self.effects)

    def test_exact_request_cas_and_conflicting_key(self):
        changed = dict(self.item, updated="2026-10-23T11:02:00Z")
        self.assertEqual(self.run_queue(exact=lambda cid:self.authority if cid=="12120" else changed)["requests"][0]["status"], "DEFERRED")
        self.run_queue()
        self.item["body"] = self.item["body"].replace("Approved operating plan", "Changed plan")
        self.assertEqual(self.run_queue()["requests"][0]["status"], "CONFLICT")
        self.assertEqual(len(self.effects), 1)

    def test_uncertain_effect_not_repeated_and_receipt_recovery_no_reexecution(self):
        def failure(obj):
            self.effects.append(obj)
            raise TimeoutError()
        self.run_queue(invoke=failure)
        self.assertEqual(self.run_queue()["requests"][0]["status"], "UNKNOWN")
        self.assertEqual(len(self.effects), 1)
        self.journal = queue.Journal(Path(self.tmp.name)/"second.sqlite3")
        self.effects.clear()
        self.run_queue(publish=lambda *args:{"status":"UNKNOWN", "native_readback":False})
        self.assertEqual(self.run_queue()["requests"][0]["status"], "VERIFIED")
        self.assertEqual(len(self.effects), 1)

    def test_read_requires_exact_digest_and_returns_no_arbitrary_body(self):
        self.item = row("income.read_source")
        def read(obj):
            return {"domain":"income", "native_readback":True, "source_id":obj["payload"]["source_id"],
                    "source_sha256":"a"*64, "source":{"title":"Checkpoint", "source_url":queue.AUTHORITY_URL,
                    "content":"private content omitted", "properties":{"secret":"omitted"}}, "note_ids":["n"]}
        self.run_queue(invoke=read)
        response = self.receipts[0]["native_result"]
        self.assertEqual(response["source"], {"title":"Checkpoint", "source_url":queue.AUTHORITY_URL})
        self.assertFalse(response["source_body_returned"])

    def test_narrow_operations_expiry_and_admission_rejected(self):
        import json
        for field, value in [("operation","store.write_context"),("expires_at","2026-10-26T11:00:00Z")]:
            item = row()
            obj = json.loads(item["body"].split("\n",1)[1]);obj[field]=value
            item["body"] = queue.MARKER+"\n"+json.dumps(obj)
            with self.assertRaises(ValueError):queue.parse(item,POLICY,NOW)
        item = row();item["body"] = item["body"].replace("Use current", "fabric_income_admission Use current")
        with self.assertRaises(ValueError):queue.parse(item,POLICY,NOW)

if __name__ == "__main__":
    unittest.main()
