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

    def completed(self, **changes):
        run = {"id":123, "run_attempt":1, "repository":{"full_name":"pqexpert/BridgeGHL"},
               "head_repository":{"full_name":"pqexpert/BridgeGHL"},
               "path":".github/workflows/provision-income-native.yml",
               "name":"Provision bounded native Income unit", "event":"workflow_dispatch",
               "head_branch":"main", "status":"completed", "head_sha":"a"*40,
               "conclusion":"success", "html_url":"https://github.com/pqexpert/BridgeGHL/actions/runs/123"}
        run.update(changes)
        pr = {"number":93, "merged":True, "merge_commit_sha":"a"*40, "merged_at":"2026-10-23T01:31:29Z",
              "base":{"ref":"main","repo":{"full_name":"pqexpert/BridgeGHL"}},
              "html_url":"https://github.com/pqexpert/BridgeGHL/pull/93"}
        return run, pr

    def test_terminal_rejects_untrusted_and_wrong_identity_before_routing(self):
        for changes in ({"event":"pull_request"}, {"head_branch":"topic"}, {"run_attempt":2},
                        {"path":".github/workflows/other.yml"},
                        {"name":"Misleading name"}, {"head_repository":{"full_name":"fork/BridgeGHL"}}):
            run, _ = self.completed(**changes)
            with self.subTest(changes=changes), patch.object(m,"api",side_effect=[{"default_branch":"main"},run]) as call:
                self.assertEqual(m.gather_completion("pqexpert/BridgeGHL",123,1)["status"],"NO_OP")
                self.assertEqual(call.call_count,2)
        with self.assertRaisesRegex(ValueError,"workflow_scope"):
            m.gather_completion("pqexpert/rsc-operations-console",123,1)

    def test_terminal_requires_exact_merged_source_and_final_revalidation(self):
        run, pr = self.completed()
        other = dict(pr, merge_commit_sha="b"*40)
        with patch.object(m,"api",side_effect=[{"default_branch":"main"},run,[other]]):
            self.assertEqual(m.gather_completion("pqexpert/BridgeGHL",123,1)["status"],"NO_OP")
        with patch.object(m,"api",side_effect=[{"default_branch":"main"},run,[pr],other]):
            with self.assertRaisesRegex(ValueError,"pre_effect_pr_changed"):
                m.gather_completion("pqexpert/BridgeGHL",123,1)

    def test_terminal_conclusion_is_not_native_acceptance_and_attempt_is_identity(self):
        keys = []
        for attempt, conclusion, outcome in ((1,"failure","FAIL"), (2,"success","SUCCESS")):
            run, pr = self.completed(run_attempt=attempt, conclusion=conclusion)
            with patch.object(m,"api",side_effect=[{"default_branch":"main"},run,[pr],pr]) as call:
                result=m.gather_completion("pqexpert/BridgeGHL",123,attempt)
                self.assertIn("/attempts/"+str(attempt),call.call_args_list[1].args[0])
            self.assertEqual(result["issue"],"RBO-97")
            self.assertEqual(result["receipt"]["outcome"],outcome)
            self.assertEqual(result["receipt"]["native_acceptance_state"],"PENDING_VERIFICATION")
            self.assertEqual(result["receipt"]["actor_invocations"],0)
            keys.append(result["replay_id"])
        self.assertNotEqual(*keys)
        run,pr=self.completed(run_attempt=2)
        with patch.object(m,"api",side_effect=[{"default_branch":"main"},run,[pr],pr]):
            self.assertEqual(m.gather_completion("pqexpert/BridgeGHL",123,2)["replay_id"],keys[1])
