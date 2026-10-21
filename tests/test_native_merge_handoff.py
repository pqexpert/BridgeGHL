import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
spec=importlib.util.spec_from_file_location("handoff",Path(__file__).parents[1]/"scripts/native_merge_handoff.py")
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Handoff(unittest.TestCase):
    def test_route_is_explicit_and_scope_bounded(self):
        p={"body":"https://restoration-mt.atlassian.net/browse/RBO-34","head":{"ref":"change"}}
        self.assertEqual(m.target(p)[:2],("RBO-34","EXPLICIT"))
        p["body"]="https://attacker.invalid/browse/RBO-34"
        self.assertEqual(m.target(p)[:2],("RBO-97","UNROUTED"))
        p["body"]="https://restoration-mt.atlassian.net/browse/RBO-34 https://restoration-mt.atlassian.net/browse/RBO-97"
        self.assertEqual(m.target(p)[:2],("RBO-97","UNROUTED"))
    def test_unmerged_event_cannot_reach_effect(self):
        with patch.object(m,"api",side_effect=[{"default_branch":"main"},{"merged":False}]) as call:
            result=m.gather("pqexpert/BridgeGHL",75)
        self.assertEqual(result["status"],"NO_OP");self.assertEqual(call.call_count,2)
        with self.assertRaisesRegex(ValueError,"scope"):m.gather("other/repo",1)
