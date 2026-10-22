import copy
import datetime as dt
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
import watch_native_sources as m
import collect_native_sources as collector

def observations():
    return {name: {"repository": "pqexpert/" + name, "observed_head": "a" * 40,
                   "files": dict.fromkeys(paths, "b" * 40)} for name, paths in m.WATCHED.items()}

class SourceObservation(unittest.TestCase):
    def test_exact_identity_rejects_wrong_repo_and_extra_source_before_write(self):
        for field, value in (("repository", "someone/BridgeGHL"), ("observed_head", "main")):
            data = observations()
            data["BridgeGHL"][field] = value
            with tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "new"
                with self.assertRaises(m.SourceError):
                    m.persist(data, collector.atomic, path)
                self.assertFalse(path.exists())
        data = observations()
        data["BridgeGHL"]["files"]["private.env"] = "b" * 40
        with self.assertRaises(m.SourceError):
            m.validate(data)

    def test_real_git_tree_selects_only_requested_metadata(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(m.WATCHED, {"sample": ("AGENTS.md", "missing.md")}, clear=True):
            root = Path(tmp); repo = root / "sample.git"
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            (repo / "AGENTS.md").write_text("instruction")
            (repo / "private.env").write_text("never propagate")
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            subprocess.run(["git", "-C", str(repo), "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                            "commit", "-qm", "source"], check=True)
            # gather expects a bare mirror; use a local clone without network.
            bare = root / "mirror"
            subprocess.run(["git", "clone", "-q", "--bare", str(repo), str(bare / "sample.git")], check=True)
            head = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"]).decode().strip()
            data = m.gather(bare, {"sample": head}, subprocess.check_output)
            self.assertEqual(set(data["sample"]["files"]), {"AGENTS.md", "missing.md"})
            self.assertIsNone(data["sample"]["files"]["missing.md"])
            result = m.persist(data, collector.atomic, root / "state")
            self.assertEqual(result["coverage"], "UNKNOWN_MISSING_REQUIRED_SOURCES")
            self.assertEqual(result["missing_paths"], {"pqexpert/sample": ["missing.md"]})

    def test_changed_head_only_no_receipt_deleted_source_and_replay_dedup(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp); data = observations()
            self.assertEqual(m.persist(data, collector.atomic, state)["status"], "BASELINE_OBSERVED_NOT_ADMITTED")
            data["BridgeGHL"]["observed_head"] = "c" * 40
            self.assertEqual(m.persist(data, collector.atomic, state)["status"], "NO_CHANGE")
            old_cursor = (state / "cursor.json").read_text()
            data["BridgeGHL"]["observed_head"] = "d" * 40
            data["BridgeGHL"]["files"]["AGENTS.md"] = None
            first = m.persist(data, collector.atomic, state)
            self.assertEqual(first["status"], "CHANGES_PENDING_REVIEW")
            self.assertEqual(first["pending_review_count"], 1)
            receipt = Path(first["receipt_paths"][0])
            saved = receipt.read_text()
            # Simulate response/cursor loss after receipt persistence.
            (state / "cursor.json").write_text(old_cursor)
            second = m.persist(data, collector.atomic, state)
            self.assertEqual(first["receipt_paths"], second["receipt_paths"])
            self.assertEqual(receipt.read_text(), saved)
            third = m.persist(data, collector.atomic, state)
            self.assertEqual(third["status"], "NO_CHANGE")
            self.assertEqual(third["pending_review_count"], 1)
            self.assertFalse(third["deployed_pins_changed"])
            self.assertEqual(third["jira_writes"], 0)

    def test_summary_symlink_is_not_followed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); state = root / "state"; state.mkdir()
            victim = root / "victim"; victim.write_text("unchanged")
            (state / "latest.json").symlink_to(victim)
            with self.assertRaises(collector.ProbeError):
                m.persist(observations(), collector.atomic, state)
            self.assertEqual(victim.read_text(), "unchanged")

    def test_transport_failure_is_nonfatal_and_collector_staleness_is_local(self):
        with patch.object(m, "send", side_effect=RuntimeError("private detail")):
            result = m.observe_after_backup(None, None, None, None)
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertTrue(result["backup_acceptance_unchanged"])
        self.assertNotIn("private detail", json.dumps(result))
        now = dt.datetime.now(dt.timezone.utc)
        with tempfile.TemporaryDirectory() as tmp, patch.object(collector, "STATE", Path(tmp)):
            state = Path(tmp) / "source-watch"
            result = m.persist(observations(), collector.atomic, state)
            self.assertEqual(collector.source_watch_status(now)["status"], result["status"])
            self.assertEqual(collector.source_watch_status(now + dt.timedelta(hours=37))["status"], "UNKNOWN_OR_STALE")

if __name__ == "__main__":
    unittest.main()
