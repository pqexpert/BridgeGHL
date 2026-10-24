import pathlib,sys,tempfile,unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/"scripts"))
import native_jira_receipt as r
class Receipts(unittest.TestCase):
    def test_timeout_reconciles_without_second_post(self):
        with tempfile.TemporaryDirectory() as d,patch.object(r,"headers",return_value={}):
            r.STATE=pathlib.Path(d);rows=[];posts=[]
            def call(url,headers=None,body=None):
                if body:
                    posts.append(body);rows.append({"id":"12","body":body["body"]})
                    raise TimeoutError()
                if "/comment?" in url:return {"startAt":0,"total":len(rows),"comments":rows}
                return rows[0]
            data={"lane":"income","outcome":"SUCCESS","native_readback":True}
            self.assertEqual(r.publish("RBO-97","income:packet:source:interviewprep",data,call)["status"],"UNKNOWN")
            self.assertEqual(r.publish("RBO-97","income:packet:source:interviewprep",data,call)["status"],"EXISTING")
            self.assertEqual(len(posts),1)
    def test_mismatched_content_refused(self):
        with tempfile.TemporaryDirectory() as d,patch.object(r,"headers",return_value={}):
            r.STATE=pathlib.Path(d)
            row={"id":"9","body":{"type":"doc","content":[{"type":"text","text":"FABRIC_FINDING rsc:scope:reconcile wrong evidence"}]}}
            def call(url,headers=None,body=None):
                self.assertIsNone(body)
                return {"startAt":0,"total":1,"comments":[row]} if "/comment?" in url else row
            with self.assertRaisesRegex(r.ProbeError,"jira_receipt_content_mismatch"):
                r.publish("RBO-34","rsc:scope:reconcile",{"lane":"rsc","outcome":"SUCCESS"},call)
    def test_cached_acknowledgement_without_readback_recovers_existing_receipt(self):
        with tempfile.TemporaryDirectory() as d,patch.object(r,"headers",return_value={}):
            r.STATE=pathlib.Path(d)
            key="rsc:scope:reconcile";data={"lane":"rsc","outcome":"SUCCESS"}
            path=r.STATE/(r.digest({"issue":"RBO-34","key":key})+".json")
            path.write_text(r.json.dumps({"status":"VERIFIED","native_readback":False,"request_hash":r.digest(data)}))
            row={"id":"9","body":{"type":"doc","content":[{"type":"text","text":"FABRIC_FINDING "+key+"\n"+r.json.dumps(data,sort_keys=True)}]}}
            def call(url,headers=None,body=None):
                self.assertIsNone(body)
                return {"startAt":0,"total":1,"comments":[row]} if "/comment?" in url else row
            result=r.publish("RBO-34",key,data,call)
            self.assertEqual(result["status"],"EXISTING")
            self.assertTrue(result["native_readback"])
    def test_unknown_absence_never_reposts(self):
        with tempfile.TemporaryDirectory() as d,patch.object(r,"headers",return_value={}):
            r.STATE=pathlib.Path(d);posts=[]
            def call(url,headers=None,body=None):
                if body:posts.append(body);raise TimeoutError()
                return {"startAt":0,"total":0,"comments":[]}
            data={"lane":"rsc","outcome":"BLOCKED"}
            for _ in range(2):self.assertEqual(r.publish("RBO-34","rsc:scope:reconcile",data,call)["status"],"UNKNOWN")
            self.assertEqual(len(posts),1)
            with self.assertRaisesRegex(r.ProbeError,"receipt_id_conflict"):
                r.publish("RBO-34","rsc:scope:reconcile",dict(data,outcome="SUCCESS"),call)
if __name__=="__main__":unittest.main()
