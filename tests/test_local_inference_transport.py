import importlib.util, json, pathlib, tempfile, unittest, urllib.error
from unittest.mock import patch
spec=importlib.util.spec_from_file_location("client",pathlib.Path(__file__).resolve().parents[1]/"scripts/local_inference_transport.py")
client=importlib.util.module_from_spec(spec);spec.loader.exec_module(client)
class TransportTests(unittest.TestCase):
    def test_offline_integrity_and_idempotency(self):
        with tempfile.TemporaryDirectory() as d:
            client.CONFIG=pathlib.Path(d)/"config";client.CONFIG.mkdir()
            client.STATE=pathlib.Path(d)/"state"
            root=pathlib.Path(__file__).resolve().parents[1]
            hashes={}
            for name in ("model-lock.json","system.txt","sources.json"):
                b=(root/"config/local-inference"/name).read_bytes()
                (client.CONFIG/name).write_bytes(b);hashes[name]=client.sha(b)
            b=b"{}";(client.CONFIG/"memory.json").write_bytes(b);hashes["memory.json"]=client.sha(b)
            (client.CONFIG/"input-lock.json").write_text(json.dumps(hashes))
            (client.CONFIG/"api-key").write_text("test-only")
            request={"request_id":"bounded-proof","lane":"engineering","prompt":"proposal"}
            with patch.object(client.urllib.request.OpenerDirector,"open",side_effect=urllib.error.URLError("offline")) as call:
                result=client.run(request)
                self.assertEqual(result["status"],"QUEUED_LOCAL_UNAVAILABLE")
                self.assertFalse(result["paid_fallback"]);self.assertEqual(call.call_count,1)
                with self.assertRaisesRegex(ValueError,"request_id_conflict"):
                    client.run(dict(request,prompt="changed"))
                self.assertEqual(call.call_count,1)
            import fcntl
            with (client.STATE/"locks/global-inference.lock").open("w") as lock:
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                with patch.object(client.urllib.request.OpenerDirector,"open") as call:
                    busy=client.run(dict(request,request_id="other-lane",lane="income"))
                    self.assertEqual(busy["status"],"QUEUED_LOCAL_BUSY")
                    call.assert_not_called()
            body,fingerprint,manifest=client.prepare(request)
            client.atomic(client.STATE/"results"/"bounded-proof.json",{"request_hash":fingerprint,"status":"SUCCESS"})
            with patch.object(client.urllib.request.OpenerDirector,"open") as call:
                self.assertEqual(client.run(request)["status"],"SUCCESS");call.assert_not_called()
            (client.CONFIG/"system.txt").write_text("changed")
            with self.assertRaisesRegex(ValueError,"input_integrity"):client.run(request)
if __name__=="__main__":unittest.main()
