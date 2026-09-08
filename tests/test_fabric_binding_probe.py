import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location("probe", pathlib.Path(__file__).parents[1] / "scripts/fabric_binding_probe.py")
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


class BindingProbeTests(unittest.TestCase):
    def test_matches_exact_channel_from_url_and_rejects_conflict(self):
        url = "https://api.chatgpt.com/v1/workspace_agents/agtch_test/trigger"
        self.assertEqual(p.trigger_id("agt_ui", url), "agtch_test")
        with self.assertRaisesRegex(p.ProbeError, "mismatch"):
            p.trigger_id("agtch_other", url)

    def test_rejects_ui_only_and_foreign_or_credential_bearing_urls(self):
        for ident, url in [("agt_ui", ""), ("", "https://chatgpt.com/agents/a/agt_ui"),
                           ("", "https://api.chatgpt.com.evil.test/v1/workspace_agents/agtch_test/trigger"),
                           ("", "https://token@api.chatgpt.com/v1/workspace_agents/agtch_test/trigger"),
                           ("", "https://api.chatgpt.com/v1/workspace_agents/agtch_test/trigger?token=x")]:
            with self.assertRaises(p.ProbeError):
                p.trigger_id(ident, url)

    def test_invalid_lanes_never_send_workspace_token(self):
        calls = []
        def call(url, token, *args):
            calls.append(url)
            return 200, {"data": []}
        result = p.probe({"model_key": "synthetic", "workspace_key": "synthetic"}, call=call)
        self.assertEqual(calls, ["https://api.openai.com/v1/models"])
        self.assertEqual(result["income"], "published_trigger_missing")

    def test_remote_denial_is_not_retried_or_counted_as_success(self):
        count = {"post": 0}
        def call(url, token, body=None, extra_headers=None):
            if body is None:
                return 200, {"data": []}
            count["post"] += 1
            raise p.ProbeError("http_403")
        result = p.probe({"model_key": "synthetic", "workspace_key": "synthetic", "agents": {
            "income": {"id": "agtch_test"}}}, call=call)
        self.assertEqual(count["post"], 1)
        self.assertEqual(result["income"], "http_403")

    def test_completed_status_requires_correlated_identity(self):
        def call(url, token, body=None, extra_headers=None):
            if "openai.com" in url:
                return 200, {"data": []}
            if body:
                return 202, {"agent_trigger_run_id": "apirun_test"}
            return 200, {"id": "apirun_wrong", "api_trigger_id": "agtch_test", "status": "completed"}
        result = p.probe({"model_key": "synthetic", "workspace_key": "synthetic", "agents": {
            "income": {"id": "agtch_test"}}}, call=call)
        self.assertEqual(result["income"], "run_identity_mismatch")

    def test_refuses_credential_redirect(self):
        self.assertIsNone(p.NoRedirect().redirect_request(None, None, 302, "", {}, "https://elsewhere.test"))


if __name__ == "__main__":
    unittest.main()
