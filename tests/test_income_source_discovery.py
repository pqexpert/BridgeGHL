import datetime as dt
import unittest
from unittest.mock import Mock
from scripts import income_source_discovery as m

NOW = dt.datetime(2026,10,24,14,tzinfo=dt.timezone.utc)
JID = '12345678-1234-1234-1234-123456789abc'

def job(**fields):
    return dict(id=JID,text='AI Data Rater',country='US',workplaceType='remote',
                categories={'commitment':'Freelance'},hostedUrl='https://jobs.lever.co/weloglobal/'+JID,**fields)

class DiscoveryTests(unittest.TestCase):
    def setup_run(self, **overrides):
        self.state={};self.queue={'version':1,'items':[]};self.ledger={'units':{}}
        self.persist=Mock();self.check=Mock();self.fetch=Mock(side_effect=[[job()],job()])
        self.duplicate=Mock(return_value=False);self.ingest=Mock(return_value={'native':True})
        self.enqueue=Mock(return_value={'intake':{'tracker_url':'native','packet_id':'p','job_id':JID}})
        self.args=dict(state=self.state,queue=self.queue,ledger=self.ledger,now=NOW,persist=self.persist,
                       check=self.check,fetch=self.fetch,duplicate=self.duplicate,ingest=self.ingest,enqueue=self.enqueue)
        self.args.update(overrides)
    def test_preview_has_no_provider_or_model_work(self):
        value=m.run(False)
        self.assertEqual(value['provider_calls'],0);self.assertEqual(value['model_calls'],0)
    def test_explicit_geography_remote_and_safe_source_required(self):
        self.assertTrue(m.eligible(job(),'weloglobal'))
        for field,value in [('country',None),('country','IN'),('workplaceType','hybrid'),
                            ('hostedUrl','https://attacker.invalid/'+JID),('categories',{'commitment':'Full-Time'})]:
            self.assertFalse(m.eligible({**job(),field:value},'weloglobal'))
        record=m.record_for(job(),'weloglobal')
        self.assertNotIn('contact_email',record);self.assertNotIn('contact_name',record)
        self.assertTrue(record['properties']['Do Not Auto Submit'])
    def test_one_intake_then_cooldown_does_no_extra_work(self):
        self.setup_run();result=m.process(**self.args)
        self.assertEqual(result['status'],'ENQUEUED');self.assertEqual(self.fetch.call_count,2)
        self.assertEqual(self.ingest.call_count,1);self.assertEqual(len(self.queue['items']),1)
        counts=(self.fetch.call_count,self.check.call_count,self.ingest.call_count,self.persist.call_count)
        again=m.process(**self.args)
        self.assertEqual(again['status'],'COOLDOWN')
        self.assertEqual(counts,(self.fetch.call_count,self.check.call_count,self.ingest.call_count,self.persist.call_count))
    def test_reservation_precedes_public_failure_and_quarantines_identity(self):
        self.setup_run();self.fetch.side_effect=m.PublicFailure(403)
        with self.assertRaises(m.PublicFailure):m.process(**self.args)
        self.assertEqual(self.state['days']['2026-10-24']['reads'],1)
        self.assertIn('blocked',self.state['boards']['weloglobal']);self.ingest.assert_not_called()
        self.assertGreaterEqual(self.persist.call_count,3)
    def test_rate_limit_preserves_retry_after(self):
        self.setup_run();self.fetch.side_effect=m.PublicFailure(429,{'Retry-After':'86400'})
        with self.assertRaises(m.PublicFailure):m.process(**self.args)
        self.assertEqual(self.state['boards']['weloglobal']['next_poll_at'],(NOW+dt.timedelta(days=1)).isoformat())
        self.assertEqual(m.retry_at({'Retry-After':'Sun, 25 Oct 2026 14:00:00 GMT'},NOW),(NOW+dt.timedelta(days=1)).isoformat())
    def test_duplicate_skips_source_detail_and_intake(self):
        self.setup_run();self.duplicate.return_value=True
        self.assertEqual(m.process(**self.args)['status'],'NO_NEW_ELIGIBLE_POSTING')
        self.assertEqual(self.fetch.call_count,1);self.ingest.assert_not_called()
    def test_unknown_write_retains_frozen_plan_and_reservation(self):
        self.setup_run();self.ingest.side_effect=TimeoutError()
        with self.assertRaises(TimeoutError):m.process(**self.args)
        pending=dict(self.state['pending'])
        self.assertEqual(self.state['days']['2026-10-24']['records'],1)
        self.args['now']=NOW+dt.timedelta(hours=2)
        self.assertEqual(m.process(**self.args)['status'],'PENDING_BACKOFF')
        self.assertEqual(self.state['pending'],pending)
        self.args['now']=NOW+dt.timedelta(hours=7);self.fetch.side_effect=None;self.fetch.return_value=job()
        self.ingest.side_effect=None;self.ingest.return_value={'native':True}
        self.assertEqual(m.process(**self.args)['status'],'ENQUEUED')
        self.assertEqual(self.state['days']['2026-10-24']['records'],1)
        self.assertEqual(self.ingest.call_args_list[0],self.ingest.call_args_list[1])
    def test_daily_caps_and_oversized_page_stop_writes(self):
        self.setup_run();self.state['days']={'2026-10-24':{'reads':24,'records':0}}
        with self.assertRaisesRegex(ValueError,'daily_public'):m.process(**self.args)
        self.fetch.assert_not_called();self.ingest.assert_not_called()
        self.setup_run();self.state['days']={'2026-10-24':{'reads':0,'records':6}}
        self.assertEqual(m.process(**self.args)['status'],'DAILY_INTAKE_BUDGET');self.fetch.assert_not_called()
        self.setup_run();self.fetch.side_effect=None;self.fetch.return_value=[job()]*26
        with self.assertRaisesRegex(ValueError,'page_bounds'):m.process(**self.args)
        self.ingest.assert_not_called()
    def test_source_change_and_authority_revocation_stop_pending_effect(self):
        self.setup_run();self.check.side_effect=ValueError('revoked')
        with self.assertRaisesRegex(ValueError,'revoked'):m.process(**self.args)
        self.fetch.assert_not_called();self.ingest.assert_not_called()
        row={'id':m.AUTHORITY['id'],'updated':m.AUTHORITY['updated'],
             'author_account_id':m.AUTHORITY['issuer'],'update_author_account_id':m.AUTHORITY['issuer']}
        m.validate_authority(row)
        with self.assertRaises(ValueError):m.validate_authority({**row,'updated':'changed'})
    def test_buffered_public_ids_and_pagination_are_bounded(self):
        self.setup_run();self.fetch.side_effect=[[job()]*25,job()]
        m.process(**self.args)
        self.assertEqual(self.state['boards']['weloglobal']['cursor'],25)
        self.assertLessEqual(len(self.state['boards']['weloglobal']['ids']),25)
    def test_completed_uncertain_handoff_reconciles_without_another_write(self):
        self.setup_run();self.ingest.side_effect=TimeoutError()
        with self.assertRaises(TimeoutError):m.process(**self.args)
        self.ledger['units']['done']={'status':'VERIFIED','job_identity':'lever:weloglobal:'+JID}
        self.args['now']=NOW+dt.timedelta(hours=7)
        self.assertEqual(m.process(**self.args)['status'],'RECONCILED_COMPLETED')
        self.assertNotIn('pending',self.state);self.assertEqual(self.ingest.call_count,1)

    def test_revocation_before_write_retains_reserved_plan(self):
        self.setup_run();self.check.side_effect=[None,ValueError('revoked')]
        with self.assertRaisesRegex(ValueError,'revoked'):m.process(**self.args)
        self.ingest.assert_not_called()
        self.assertEqual(self.state['days']['2026-10-24']['records'],1)
        self.assertIn('pending',self.state)

    def test_source_change_during_uncertain_recovery_never_rewrites(self):
        self.setup_run();self.ingest.side_effect=TimeoutError()
        with self.assertRaises(TimeoutError):m.process(**self.args)
        self.args['now']=NOW+dt.timedelta(hours=7)
        self.fetch.side_effect=None;self.fetch.return_value={**job(),'text':'Support Rater Changed'}
        with self.assertRaisesRegex(ValueError,'pending_public_source_changed'):m.process(**self.args)
        self.assertEqual(self.ingest.call_count,1)
