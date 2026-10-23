import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('bindings', Path(__file__).parents[1]/'scripts/bind_native_providers.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class BindingsTest(unittest.TestCase):
    def test_jira_uses_authenticated_account_not_issue_assignee(self):
        cfg={'JIRA_EMAIL':'owner@example.com','JIRA_API_TOKEN':'secret'}
        def call(url, *a, **k):
            if '/myself' in url:
                return {'active':True,'accountId':'credential-owner','emailAddress':'owner@example.com'}
            if '/issue/' in url:
                return {'key':'RBO-97','fields':{'project':{'key':'RBO'}}}
            return {'permissions':{'BROWSE_PROJECTS':{'havePermission':True}}}
        receipt, binding=m.jira(cfg,call)
        self.assertEqual(receipt['issue_read'],'verified')
        self.assertFalse(receipt['project_permissions']['EDIT_ISSUES'])
        cfg['JIRA_EMAIL']='someone-else@example.com'
        with self.assertRaises(m.ProbeError):
            m.jira(cfg,call)
        with self.assertRaises(m.ProbeError):
            m.jira(cfg, lambda *a, **k: {'active':True, 'accountId':'unknown-owner'})

    def test_arbitrary_shop_destination_rejected_before_request(self):
        for domain in ['evil.example', 'store.myshopify.com@evil.example', 'store.myshopify.com/path']:
            with self.assertRaises(m.ProbeError):
                m.shopify({'SHOPIFY_SHOP_DOMAIN':domain,'SHOPIFY_CLIENT_ID':'id','SHOPIFY_CLIENT_SECRET':'secret'},
                          lambda *a, **k: self.fail('request escaped domain check'))

    def test_wrong_store_never_persisted_and_jira_failure_is_independent(self):
        cfg={'JIRA_EMAIL':'test@example.com','JIRA_API_TOKEN':'secret','SHOPIFY_SHOP_DOMAIN':'test.myshopify.com',
             'SHOPIFY_CLIENT_ID':'id','SHOPIFY_CLIENT_SECRET':'secret'}
        def call(url, *a, **k):
            if 'atlassian.com' in url:
                raise m.ProbeError('http_401')
            if url.endswith('/access_token'):
                return {'access_token':'never-log-this'}
            return {'data':{'shop':{'name':'Wrong store'}}}
        with tempfile.TemporaryDirectory() as td, patch.object(m, 'host_inventory', return_value={}):
            result=m.run_server(cfg, Path(td)/'bindings', call)
            self.assertEqual(result['results']['jira']['status'],'http_401')
            self.assertEqual(result['results']['shopify']['status'],'hosted_route_retained_native_deferred')
            self.assertFalse((Path(td)/'bindings').exists())
            self.assertNotIn('never-log-this',json.dumps(result))

    def test_google_identity_and_pagination(self):
        cfg={'GOOGLE_CLIENT_ID':'id','GOOGLE_CLIENT_SECRET':'secret','GOOGLE_REFRESH_TOKEN':'refresh'}
        def call(url, *a, **kw):
            if 'oauth2' in url:
                return {'access_token':'never-log-this'}
            if '/about?' in url:
                return {'user':{'emailAddress':'restorationconsultantsoutreach@gmail.com'}}
            if '/events?' in url:
                return {'items':[]}
            if 'pageToken' in url:
                return {'items':[{'summary':'WooCalendar','accessRole':'owner','id':'private-id'}]}
            return {'items':[], 'nextPageToken':'second'}
        receipt, binding=m.google(cfg,call)
        self.assertEqual(receipt['calendar_event_read'],'verified')
        self.assertEqual(binding['calendar_id'],'private-id')
        self.assertNotIn('never-log-this',json.dumps(receipt))
        def wrong(url, *a, **kw):
            if 'oauth2' in url:
                return {'access_token':'never-log-this'}
            return {'user':{'emailAddress':'wrong@example.com'}}
        with self.assertRaisesRegex(m.ProbeError, 'google_identity_mismatch'):
            m.google(cfg,wrong)
        def ambiguous(url, *a, **kw):
            if 'calendarList' in url:
                return {'items':[]}
            return call(url,*a,**kw)
        with self.assertRaisesRegex(m.ProbeError, 'woo_calendar_identity_ambiguous'):
            m.google(cfg,ambiguous)

    def test_optional_inventory_does_not_hide_binding_receipts(self):
        with tempfile.TemporaryDirectory() as td, patch.object(m,'host_inventory',side_effect=OSError):
            r=m.run_server({},Path(td)/'bindings')
            self.assertEqual(r['results']['jira']['status'],'binding_missing')
            self.assertEqual(r['host']['status'],'inventory_unavailable')
        r=m.collect_remote({'collector':'raise RuntimeError()'}, {'stage':'retained'})
        self.assertEqual(r['stage'],'retained')

    def test_existing_binding_preserved_and_permissions_private(self):
        with tempfile.TemporaryDirectory() as td:
            base=Path(td)/'providers'
            self.assertEqual(m.persist(base,'jira',{'token':'one'}),'installed_verified')
            self.assertEqual((base/'jira/credentials.json').stat().st_mode & 0o777,0o600)
            self.assertEqual(m.persist(base,'jira',{'token':'one'}),'already_present_verified')
            with self.assertRaises(m.ProbeError):
                m.persist(base,'jira',{'token':'two'})
            self.assertEqual(json.loads((base/'jira/credentials.json').read_text()),{'token':'one'})

    def test_successful_shop_independent_of_jira_and_no_token_in_receipt(self):
        cfg={'SHOPIFY_SHOP_DOMAIN':'test.myshopify.com','SHOPIFY_CLIENT_ID':'id','SHOPIFY_CLIENT_SECRET':'secret'}
        def call(url, *a, **k):
            if url.endswith('/access_token'):
                return {'access_token':'never-log-this'}
            return {'data':{'shop':{'name':'Tantric Treasures','email':'store@dzokden.org','currencyCode':'USD',
                    'primaryDomain':{'host':'www.tantrictreasures.com'}},
                    'currentAppInstallation':{'accessScopes':[{'handle':'read_products'}]}}}
        with tempfile.TemporaryDirectory() as td, patch.object(m,'host_inventory',return_value={}):
            r, binding=m.shopify(cfg,call)
            self.assertEqual(r['authentication'],'verified')
            self.assertNotIn('never-log-this',json.dumps(r))
            self.assertNotIn('secret',json.dumps(r))

    def test_existing_calendar_binding_ignores_new_drive_candidate(self):
        binding={'client_id':'calendar-client','client_secret':'calendar-secret','refresh_token':'calendar-refresh',
                 'expected_email':'restorationconsultantsoutreach@gmail.com','calendar_id':'woo-id'}
        candidate={'GOOGLE_CLIENT_ID':'drive-client','GOOGLE_CLIENT_SECRET':'drive-secret','GOOGLE_REFRESH_TOKEN':'drive-refresh'}
        def call(url,*args,**kw):
            if 'oauth2' in url:
                self.assertEqual(kw['body']['client_id'],'calendar-client')
                self.assertEqual(kw['body']['refresh_token'],'calendar-refresh')
                return {'access_token':'private-access'}
            if '/about?' in url:return {'user':{'emailAddress':binding['expected_email']}}
            if '/events?' in url:return {'items':[]}
            return {'items':[{'summary':'WooCalendar','accessRole':'owner','id':'woo-id'}]}
        with tempfile.TemporaryDirectory() as td,patch.object(m,'host_inventory',return_value={}):
            base=Path(td)/'providers';m.persist(base,'google',binding);m.persist(base,'income-google',{'unchanged':'income'})
            shared=(base/'google/credentials.json').read_bytes();income=(base/'income-google/credentials.json').read_bytes()
            result=m.run_server(candidate,base,call)
            self.assertTrue(result['results']['google']['binding_unchanged'])
            self.assertFalse(result['results']['google']['github_drive_candidate_used'])
            self.assertEqual((base/'google/credentials.json').read_bytes(),shared)
            self.assertEqual((base/'income-google/credentials.json').read_bytes(),income)
            self.assertNotIn('calendar-secret',json.dumps(result));self.assertNotIn('private-access',json.dumps(result))
            (base/'google/credentials.json').chmod(0o644)
            denied=m.run_server(candidate,base,lambda *a,**kw:self.fail('invalid binding must not fall back'))
            self.assertEqual(denied['results']['google']['status'],'existing_binding_permissions_invalid')

if __name__=='__main__':
    unittest.main()
