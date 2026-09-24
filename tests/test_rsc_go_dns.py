import copy
import pathlib
import sys
import unittest
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / 'scripts'))
from rsc_go_dns import apply, WANTED

BASE = [dict(id='root', name='restorationsecurityoutreach.com', type='A', content='192.0.2.1', ttl=300),
        dict(id='mail', name='mg.restorationsecurityoutreach.com', type='MX', content='mail.example.com', ttl=3600)]

class Api:
    def __init__(self, rows=None):
        self.rows = copy.deepcopy(BASE if rows is None else rows)
        self.calls = []
        self.fail_after_post = False
        self.drift_after_post = False
    def current(self):
        return 'zone', copy.deepcopy(self.rows)
    def request(self, method, path, data=None):
        self.calls.append((method, path))
        if method == 'POST':
            row = dict(id='new', **data[0]); self.rows.append(row)
            if self.fail_after_post: raise RuntimeError('uncertain_response')
            if self.drift_after_post: self.rows[1]['ttl'] = 7200
            return [copy.deepcopy(row)]
        if method == 'DELETE': self.rows = [r for r in self.rows if r['id'] != 'new']

class Tests(unittest.TestCase):
    def test_create_preserves_root_and_mail(self):
        api = Api(); saved = {}
        receipt = apply(api, lambda k,v: saved.update({k:v}))
        self.assertEqual(receipt['result'], 'created')
        self.assertEqual(api.rows[:2], BASE)
        self.assertIn('go-dns-before.json', saved)
    def test_existing_correct_record_noop(self):
        api = Api(BASE + [dict(id='existing', **dict(WANTED,ttl=3600))])
        self.assertEqual(apply(api, lambda *_:None)['result'], 'noop')
        self.assertEqual(api.calls, [])
    def test_conflict_does_not_write(self):
        api = Api(BASE + [dict(id='bad', **dict(WANTED,content='different.example'))])
        with self.assertRaisesRegex(RuntimeError, 'conflict'): apply(api, lambda *_:None)
        self.assertEqual(api.calls, [])
    def test_failed_backup_does_not_write(self):
        api = Api()
        def fail(*_): raise RuntimeError('backup_failed')
        with self.assertRaisesRegex(RuntimeError, 'backup_failed'): apply(api, fail)
        self.assertEqual(api.calls, [])
    def test_ambiguous_post_does_not_blindly_delete_or_retry(self):
        api = Api(); api.fail_after_post = True
        with self.assertRaisesRegex(RuntimeError, 'uncertain'): apply(api, lambda *_:None)
        self.assertEqual([c[0] for c in api.calls], ['POST'])
    def test_unrelated_drift_rolls_back_only_own_record(self):
        api = Api(); api.drift_after_post = True
        with self.assertRaisesRegex(RuntimeError, 'protected_records'): apply(api, lambda *_:None)
        self.assertEqual([c[0] for c in api.calls], ['POST','DELETE'])
        self.assertEqual(len(api.rows), 2)
        self.assertEqual(api.rows[1]['ttl'], 7200)
    def test_final_compare_detects_concurrent_change(self):
        api = Api()
        def persist(k,v):
            if k == 'go-dns-create-intent.json': api.rows[1]['ttl'] = 999
        with self.assertRaisesRegex(RuntimeError, 'final_compare'): apply(api, persist)
        self.assertEqual(api.calls, [])

if __name__ == '__main__': unittest.main()
