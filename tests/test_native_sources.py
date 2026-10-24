import datetime as dt
import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import tempfile
import json

sys.path.insert(0,str(Path(__file__).parents[1]/'scripts'))
import collect_native_sources as m

class NativeSources(unittest.TestCase):
    def test_compiled_install_can_import_its_installed_owning_source_helper(self):
        import importlib
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);source=root/'source';source.mkdir()
            name='fabric_install_resolution_regression'
            (source/(name+'.py')).write_text('verified = True\n')
            self.addCleanup(lambda:sys.modules.pop(name,None))
            def collect(trigger):
                self.assertTrue(importlib.import_module(name).verified)
                return {'jira':{'status':'retrieved_and_persisted'},'calendar':{'status':'retrieved_and_persisted'}}
            with patch.object(m,'ROOT',source),patch.object(m,'CRON',root/'cron'),patch.object(m.os,'geteuid',return_value=0),patch.object(m,'collect',side_effect=collect),patch.object(sys,'path',list(sys.path)):
                self.assertNotIn(str(source),sys.path)
                self.assertTrue(m.install('source','bindings')['cron_installed'])

    def test_jira_complete_pagination_and_no_execution_inference(self):
        cfg={'email':'owner@example.com','api_token':'secret','base_url':m.JIRA_ROOT,'project_allowlist':['RBO']}
        calls=[]
        def call(url,*a,**kw):
            calls.append(url)
            if '/myself' in url:
                return {'active':True,'accountId':'id','emailAddress':cfg['email']}
            self.assertIn('/comment?',url)
            start=1 if 'startAt=1' in url else 0
            return {'startAt':start,'total':2,'comments':[{'id':str(start),'created':'date','updated':'date',
              'body':{'content':[{'text':'FABRIC_FINDING evidence'}]}}]}
        result=m.jira_snapshot(cfg,call)
        self.assertEqual(result['comments_scanned'],2)
        self.assertEqual(len(result['receipt_candidates']),2)
        self.assertFalse(result['actor_execution_verified'])
        self.assertEqual(len(calls),3)  # identity plus two comment pages

    def test_dedicated_admissions_preserve_issuer_and_reject_incidental_prose(self):
        cfg={'email':'owner@example.com','api_token':'secret','base_url':m.JIRA_ROOT,'project_allowlist':['RBO']}
        rows=[{'id':str(n),'created':'same','updated':'same','author':{'accountId':'issuer'},
               'body':{'text':body}} for n,body in enumerate(('FABRIC_INCOME_ADMISSION\n{"status":"REVOKED"}', 'Mention FABRIC_INCOME_ADMISSION in prose', 'FABRIC_INCOME_ADMISSION\nmalformed'))]
        rows[2]['updated']='later'
        def call(url,*args,**kwargs):
            if '/myself' in url:return {'active':True,'accountId':'id','emailAddress':cfg['email']}
            return {'startAt':0,'total':3,'comments':rows}
        result=m.jira_snapshot(cfg,call)
        candidates=result['income_admission_candidates']
        self.assertEqual([x['id'] for x in candidates],['0','2'])
        self.assertEqual(candidates[0]['update_author_account_id'],'issuer')
        self.assertIsNone(candidates[1]['update_author_account_id'])
        self.assertEqual(result['receipt_candidates'],[])

    def test_jira_scope_and_identity_fail_before_comments(self):
        cfg={'email':'owner@example.com','api_token':'secret','base_url':m.JIRA_ROOT,'project_allowlist':['RBO']}
        from unittest.mock import Mock
        for change in ({'base_url':'https://example.invalid'}, {'project_allowlist':['OTHER']}):
            call=Mock()
            with self.assertRaisesRegex(m.ProbeError,'jira_binding_scope_mismatch'):
                m.jira_snapshot(dict(cfg,**change),call)
            call.assert_not_called()
        call=Mock(return_value={'active':True,'accountId':'id','emailAddress':'other@example.com'})
        with self.assertRaisesRegex(m.ProbeError,'jira_identity_mismatch'):
            m.jira_snapshot(cfg,call)
        call.assert_called_once()


    def test_calendar_preserves_description_and_excludes_shared_events(self):
        description='FABRIC CURRENT NOTE\nold note\nEND FABRIC CURRENT NOTE\n Human response:  keep spaces  '
        def call(url,*a,**kw):
            if 'oauth2' in url:
                return {'access_token':'secret'}
            if '/about?' in url:
                return {'user':{'emailAddress':'owner@example.com'}}
            return {'items':[{'id':'a','visibility':'private','description':description},
                {'id':'b','visibility':'private','description':description,'attendees':[{'email':'other'}]},
                {'id':'c','visibility':'public','description':description}]}
        cfg={'client_id':'id','client_secret':'secret','refresh_token':'refresh','expected_email':'owner@example.com','calendar_id':'calendar'}
        r=m.calendar_snapshot(cfg,dt.datetime.now(dt.timezone.utc),call)
        self.assertEqual([x['id'] for x in r['events']],['a'])
        self.assertEqual(r['events'][0]['description'],description)
        self.assertFalse(r['writer_allowlist_established'])
        self.assertEqual(r['calendar_writes'],0)

    def test_invalid_prior_metadata_does_not_block_either_lane(self):
        for previous in ('{broken', '[]', 'null'):
            with self.subTest(previous=previous), tempfile.TemporaryDirectory() as d:
                root=Path(d)
                (root/'latest.json').write_text(previous)
                with patch.object(m,'STATE',root), patch.object(m,'config',return_value={}), patch.object(m,'jira_snapshot',return_value={'receipt_candidates':[],'comments_scanned':0}) as jira, patch.object(m,'calendar_snapshot',return_value={'events':[],'events_scanned':0}) as calendar:
                    result=m.collect()
                self.assertEqual(result['trigger'],'manual')
                self.assertEqual(result['previous_collection']['status'],'unreadable_prior_metadata')
                self.assertEqual(result['jira']['status'],'retrieved_and_persisted')
                self.assertEqual(result['calendar']['status'],'retrieved_and_persisted')
                self.assertEqual(json.loads((root/'latest.json').read_text()),result)
                jira.assert_called_once()
                calendar.assert_called_once()

    def test_install_migrates_only_known_cron_and_labels_validation(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            cron=root/'cron'
            cron.write_text('# Native read-only evidence; no model or provider writes.\n11 * * * * root /usr/bin/python3 /opt/ziji-fabric/native/collect_native_sources.py >/dev/null 2>&1\n')
            ready={'jira':{'status':'retrieved_and_persisted'},'calendar':{'status':'retrieved_and_persisted'}}
            with patch.object(m,'ROOT',root/'source'), patch.object(m,'CRON',cron), patch.object(m.os,'geteuid',return_value=0), patch.object(m,'collect',return_value=ready.copy()) as collect:
                self.assertTrue(m.install('source','bindings')['cron_installed'])
                collect.assert_called_once_with('installation_validation')
                self.assertIn('--trigger cron',cron.read_text())
                cron.write_text('unrelated existing schedule')
                with self.assertRaises(m.ProbeError):
                    m.install('source','bindings')
                self.assertEqual(cron.read_text(),'unrelated existing schedule')

    def test_failed_collection_cannot_pass_deployment(self):
        import bind_native_providers as binding
        receipt={'results':{lane:{'binding':'already_present_verified'} for lane in ('jira','google')}}
        failed=binding.collect_remote({'collector':'raise ValueError()', 'source':''},receipt)
        self.assertIn('results',failed)
        self.assertFalse(binding.deployment_succeeded(failed))
        failed['collection']={'cron_installed':True,'jira':{'status':'retrieved_and_persisted'},'calendar':{'status':'retrieved_and_persisted'}}
        self.assertTrue(binding.deployment_succeeded(failed))
        failed['collection']['calendar']['status']='collection_failed_details_withheld'
        self.assertFalse(binding.deployment_succeeded(failed))

    def test_deploy_preserves_receipt_but_raises_on_remote_failure(self):
        import bind_native_providers as binding
        import contextlib, io
        from types import SimpleNamespace
        receipt={'stage':'NATIVE_PROVIDER_BINDINGS','results':{'jira':{'binding':'already_present_verified'}},'collection':{'status':'installation_failed_inspect_before_retry'}}
        output=io.StringIO()
        with patch.dict(binding.os.environ,{'REMOTE_USER':'deploy','REMOTE_HOST':'example.invalid'}), patch.object(binding.subprocess,'run',return_value=SimpleNamespace(returncode=1,stdout=json.dumps(receipt))), contextlib.redirect_stdout(output):
            with self.assertRaises(binding.ProbeError):
                binding.deploy()
        self.assertEqual(json.loads(output.getvalue()),receipt)

if __name__=='__main__':
    unittest.main()
