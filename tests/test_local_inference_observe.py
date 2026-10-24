"""Check privacy boundaries and fail-closed observations without inference."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("local_observe", Path(__file__).resolve().parents[1] / "scripts/local_inference_observe.py")
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)


class ObserverTests(unittest.TestCase):
    def policy(self):
        return {"model_access_mode": "restricted", "connections_verified": True,
            "is_active": 1, "allow_auto_combos": 0, "no_log": 1,
            "scopes": "[]", "allowed_combos": "[]",
            "allowed_models": '["duck-proposal/ziji-local","duck-coder/ziji-local-coder"]',
            "allowed_connections": '["local-a","local-b"]'}

    def test_changed_or_unknown_key_policy_is_not_claimed_local_only(self):
        self.assertTrue(observer.local_key_restricted(self.policy()))
        for field, value in (("model_access_mode", "all"), ("connections_verified", False),
                ("is_active", 0), ("allow_auto_combos", 1), ("no_log", 0),
                ("scopes", '["manage"]'), ("allowed_combos", '["combo/*"]'),
                ("allowed_models", '["codex/gpt-5.4"]'), ("allowed_models", "broken")):
            with self.subTest(field=field, value=value):
                policy = self.policy()
                policy[field] = value
                self.assertFalse(observer.local_key_restricted(policy))
        self.assertFalse(observer.local_key_restricted(None))

    def test_journal_messages_do_not_enter_report_and_remote_routes_are_flagged(self):
        omni = {"status": "READ_ONLY_SNAPSHOT", "providers": [{"provider": "codex", "is_active": 1}],
            "nodes": [{"loopback": False, "reviewed_route": False}], "call_log_pipeline_enabled": False,
            "local_only_key_policy": self.policy()}
        message = json.dumps({"MESSAGE": "PRIVATE_PROMPT_AND_TOKEN_DO_NOT_EXPORT", "PRIORITY": "3"})
        with patch.object(observer, "health", return_value="READY"), \
                patch.object(observer, "service", return_value={"ActiveState": "active"}), \
                patch.object(observer, "omniroute", return_value=omni), \
                patch.object(observer, "command", return_value=(0, message)):
            result = observer.observe()
        self.assertNotIn("PRIVATE_PROMPT_AND_TOKEN", json.dumps(result))
        self.assertEqual(result["model_calls"], 0)
        self.assertEqual(result["journal"]["warning_or_higher"], 1)
        self.assertIn("OMNIROUTE_CUSTOM_NODE_OUTSIDE_LOOPBACK", result["anomalies"])
        self.assertIn("OMNIROUTE_CLOUD_CODEX_ENABLED", result["anomalies"])

    def test_private_metadata_record_rejects_symlink_and_bounds_file(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(observer, "STATE", Path(tmp) / "observations"):
            observer.record({"event_type": "test"})
            path = observer.STATE / "events.jsonl"
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            path.write_text("x" * (1024 * 1024 + 1))
            observer.record({"event_type": "bounded"})
            self.assertLess(path.stat().st_size, 100)
            self.assertEqual(json.loads(path.read_text())["event_type"], "bounded")
            path.unlink()
            target = Path(tmp) / "do-not-change"
            target.write_text("untouched")
            path.symlink_to(target)
            with self.assertRaises(OSError):
                observer.record({"event_type": "rejected"})
            self.assertEqual(target.read_text(), "untouched")


if __name__ == "__main__":
    unittest.main()
