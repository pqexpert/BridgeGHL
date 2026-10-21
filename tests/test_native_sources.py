import datetime as dt
import importlib.util
from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).parents[1]/'scripts'))
import collect_native_sources as m

class NativeSources(unittest.TestCase):
    def test_jira_complete_pagination_and_no_execution_inference(self):
        cfg={'email':'owner@example.com','api_token':'secret','base_url':'https://example.invalid'}
        def call(url,*a,**kw):
            if '/myself' in url:
                return {'active':True,'accountId':'id','emailAddress':cfg['email']}
            if '/mypermissions' in url:
                return {'permissions':{}}
            if '/comment?' not in url:
                return {'key':'RBO-97','fields':{'project':{'key':'RBO'}}}
            start=1 if 'startAt=1' in url else 0
            return {'startAt':start,'total':2,'comments':[{'id':str(start),'created':'date','updated':'date',
              'body':{'content':[{'text':'FABRIC_FINDING evidence'}]}}]}
        result=m.jira_snapshot(cfg,call)
        self.assertEqual(result['comments_scanned'],2)
        self.assertEqual(len(result['receipt_candidates']),2)
        self.assertFalse(result['actor_execution_verified'])

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

if __name__=='__main__':
    unittest.main()
