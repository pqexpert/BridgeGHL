import hashlib
import json
import os
from pathlib import Path, PosixPath
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[1]/'scripts'))
import install_income_continuation as m
import collect_native_sources as collector

class RootPath(PosixPath):
    def stat(self,*args,**kwargs):
        fields=list(super().stat(*args,**kwargs));fields[4]=0
        return os.stat_result(fields)

class ContinuationStatus(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.path=lambda name:RootPath(self.root/str(name).lstrip('/'))
        self.addCleanup(patch.stopall)
        patch.object(m,'Path',self.path).start();patch.object(m.os,'geteuid',return_value=0).start()
        self.state='/var/lib/ziji-fabric/dispatch/continuation/'
        self.bundle={'hashes':{}}
        self.files={'controller':'/opt/ziji-fabric/dispatch/continuation/income_continuation.py','runtime':'/opt/ziji-fabric/income/career_unit.py','collector':'/opt/ziji-fabric/native/collect_native_sources.py','native_selector':'/opt/ziji-fabric/dispatch/continuation/native_income_backlog.py','transport':'/usr/local/lib/ziji-local-link/local_inference_transport.py'}
        for name,path in self.files.items():
            self.write(path,'source-'+name,raw=True)
            self.bundle['hashes'][name]=hashlib.sha256(('source-'+name).encode()).hexdigest()
        self.write(self.state+'installation.json',{'source_pins':{'dispatch':'pin'}})
        self.write('/etc/cron.d/ziji-fabric-native-sources','# actual description\n',raw=True)
        self.entry={'intake':{'tracker_url':'tracker','packet_id':'packet','job_id':'job'}}
        self.key=hashlib.sha256(json.dumps(['tracker','packet','job'],separators=(',',':')).encode()).hexdigest()
        self.write('/etc/ziji-fabric/dispatch/income-queue.json',{'items':[self.entry]})
        self.write(self.state+'state.json',{'units':{'removed':{'status':'DEFERRED'},'done':{'status':'VERIFIED','result':{'receipt':{'comment_id':'123','native_readback':True}}}}})
        self.write(self.state+'latest.json',{'status':'VERIFIED','observed_at':'2026-10-23T10:00:00+00:00','receipt':{'comment_id':'123','native_readback':True}})
    def write(self,name,value,raw=False):
        p=self.path(name);p.parent.mkdir(parents=True,exist_ok=True);p.write_text(value if raw else json.dumps(value));p.chmod(0o600)
    def test_hash_validation_and_current_queue_join(self):
        result=m.observe(self.bundle)
        self.assertTrue(result['installed']);self.assertEqual(result['unfinished_units'],1)
        self.assertEqual(result['completed_units'],1)
        self.write(self.files['runtime'],'tampered',raw=True)
        self.assertFalse(m.observe(self.bundle)['installed'])
        self.path(self.files['runtime']).unlink()
        with patch.object(collector,'continuation') as invoke:
            result=m.observe(self.bundle,wake=True)
        invoke.assert_not_called();self.assertTrue(result['configured']);self.assertFalse(result['wake_ok'])
    def test_failed_or_stale_wake_preserves_history_without_current_success(self):
        for caller in ({'status':'TIMEOUT_RECONCILE_NATIVE_STATE'},{'status':'FAILED','exit_code':1},{'status':'COMPLETED','exit_code':0,'started_at':'2026-10-23T11:00:00+00:00','observed_at':'2026-10-23T11:01:00+00:00'}):
            with self.subTest(caller=caller),patch.object(collector,'continuation',return_value=caller):
                result=m.observe(self.bundle,wake=True)
                self.assertFalse(result['wake_ok']);self.assertFalse(result['native_receipt_readback'])
                self.assertIsNone(result['jira_comment_id']);self.assertEqual(result['completed_receipts'][0]['jira_comment_id'],'123')
        with patch.object(collector,'continuation',return_value={'status':'COMPLETED','exit_code':0,'started_at':'2026-10-23T09:59:00+00:00','observed_at':'2026-10-23T10:01:00+00:00','result_sha256':hashlib.sha256(json.dumps(json.loads(self.path(self.state+'latest.json').read_text()),sort_keys=True).encode()).hexdigest()}):
            self.assertTrue(m.observe(self.bundle,wake=True)['wake_ok'])
    def test_supervised_wake_uses_bounded_launcher_and_requires_current_evidence(self):
        with patch.object(collector,'continuation',return_value={'status':'TIMEOUT_RECONCILE_NATIVE_STATE'}) as invoke:
            result=m.observe(self.bundle,wake=True,supervised=True)
        invoke.assert_called_once_with(timeout=420,supervised=True)
        self.assertFalse(result['wake_ok'])
        self.assertFalse(result['native_receipt_readback'])
    def test_refresh_preserves_expired_queue_bytes(self):
        bundle=json.loads((Path(__file__).parents[1]/'scripts/income_continuation_bundle.json').read_text())
        self.write(self.files['collector'],bundle['collector'],raw=True)
        self.write(self.files['transport'],bundle['transport'],raw=True)
        cron='# Native read-only evidence; no model or provider writes.\n11 * * * * root /usr/bin/python3 /opt/ziji-fabric/native/collect_native_sources.py --trigger cron >/dev/null 2>&1\n'
        self.write('/etc/cron.d/ziji-fabric-native-sources',cron,raw=True)
        self.write('/var/lib/ziji-fabric/income/unit.lock','',raw=True)
        queue=self.path('/etc/ziji-fabric/dispatch/income-queue.json');before=queue.read_bytes()
        with patch.object(collector,'atomic',side_effect=lambda p,v,*args:self.write(str(p.relative_to(self.root)),v,raw=True)):
            result=m.install({'mode':'continuation-refresh','continuation':bundle})
        self.assertEqual(queue.read_bytes(),before);self.assertTrue(result['queue_unchanged'])
        self.assertIn('admitted Income',self.path('/etc/cron.d/ziji-fabric-native-sources').read_text())
        cfg={'enabled':True,'issuer_account_id':'fixture-issuer','authority_ref':'RBO-97:12086','authority_comment_updated':'2026-10-22T22:05:00.461-0500','drive_folder':'1j_pa8A2sXBSA9vy87B6nyKo-myZ3HDaE'}
        payload={'mode':'continuation-refresh','continuation':bundle,'admission_config':cfg}
        with patch.object(collector,'atomic',side_effect=lambda p,v,*args:self.write(str(p.relative_to(self.root)),v,raw=True)):
            self.assertEqual(m.install(payload)['admission_config'],'INITIALIZED')
            self.assertEqual(json.loads(self.path('/etc/ziji-fabric/dispatch/income-admission.json').read_text())['authority_comment_updated'],cfg['authority_comment_updated'])
            self.assertEqual(m.install(payload)['admission_config'],'IDENTICAL')
            with self.assertRaisesRegex(ValueError,'admission_config_conflict_preserved'):
                m.install({**payload,'admission_config':{**cfg,'issuer_account_id':'different'}})
        self.assertEqual(queue.read_bytes(),before)

if __name__=='__main__':unittest.main()
