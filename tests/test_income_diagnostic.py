import importlib.util
from pathlib import Path
import unittest

scope={}
for name in ('fabric_binding_probe.py','diagnose_income.py'):
    exec(compile((Path(__file__).parents[1]/'scripts'/name).read_text(),name,'exec'),scope)

class DiagnosticTests(unittest.TestCase):
    def config(self):
        return {'workspace_key':'synthetic','agents':{'income':{'id':scope['EXPECTED_AGENT'],'url':'https://api.chatgpt.com/v1/workspace_agents/agtch_test/trigger'}}}
    def test_trigger_error_has_stage(self):
        def call(*args): raise scope['ProbeError']('http_404')
        result=scope['diagnose'](self.config(),call)
        self.assertEqual((result['stage'],result['outcome']),('trigger','http_404'))
    def test_status_error_has_stage_and_original_key(self):
        def call(url,token,body=None,headers=None):
            if body:
                self.assertEqual(headers['Idempotency-Key'],'fabric-binding-v1-20260908-income')
                return 202,{'agent_trigger_run_id':'apirun_test'}
            raise scope['ProbeError']('http_404')
        self.assertEqual(scope['diagnose'](self.config(),call)['stage'],'run_status')
    def test_wrong_config_never_calls_provider(self):
        config=self.config();config['agents']['income']['id']='agt_wrong'
        self.assertEqual(scope['diagnose'](config,lambda *a:self.fail('provider called'))['outcome'],'configured_agent_mismatch')
