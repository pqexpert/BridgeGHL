import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("shadow", Path(__file__).parents[1] / "scripts/deploy_fabric_shadow.py")
shadow = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(shadow)


class ShadowTests(unittest.TestCase):
    def test_source_mismatch_rejected(self):
        with self.assertRaises(ValueError):
            shadow.verify({name: "changed" for name in shadow.PINS})
        with self.assertRaises(ValueError):
            shadow.verify({})

    def test_atomic_rejects_symlink_and_preserves_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "target"
            target.write_text("keep")
            link = Path(tmp) / "link"
            link.symlink_to(target)
            with self.assertRaises(ValueError):
                shadow.atomic(link, "overwrite", 0o600)
            self.assertEqual(target.read_text(), "keep")

    def test_sample_bounded_storage_no_effects(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(shadow, "STATE", Path(tmp)):
            result = {"mode": "shadow_read_only", "model_calls": 0, "calendar_writes": 0,
                      "external_messages": 0, "crm_writes": 0, "observed_at_utc": "first"}
            with patch.object(subprocess, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps(result))):
                shadow.sample()
                shadow.sample()
            self.assertEqual(json.loads((Path(tmp)/"previous.json").read_text()), {**result, "trigger": "immediate_validation"})
            self.assertEqual((Path(tmp)/"latest.json").stat().st_mode & 0o777, 0o600)
            self.assertEqual(len(list(Path(tmp).glob("*.json"))), 2)

    def test_effectful_result_not_accepted(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(shadow, "STATE", Path(tmp)):
            result = {"mode": "shadow_read_only", "model_calls": 1, "calendar_writes": 0,
                      "external_messages": 0, "crm_writes": 0}
            with patch.object(subprocess, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps(result))):
                with self.assertRaises(ValueError):
                    shadow.sample()
            self.assertFalse((Path(tmp)/"latest.json").exists())


if __name__ == "__main__":
    unittest.main()
