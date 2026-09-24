import copy
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from rsc_outreach_dns import Change, DOMAIN, ORIGINAL, make_snapshot, shape
from rsc_outreach_ingress import protected


def records():
    result = [dict(id=str(i), name=name, type=kind, content=ip, ttl=3600, prio=0, disabled=False)
              for i, ((name, kind), ip) in enumerate(ORIGINAL.items())]
    result.append(dict(id='mail', name=DOMAIN, type='MX', content='mail.example.test', ttl=3600, prio=10, disabled=False))
    return result


class Provider:
    def __init__(self):
        self.records = records()
        self.calls = []
        self.uncertain = False

    def current(self):
        return 'zone', copy.deepcopy(self.records)

    def update(self, zone, identity, wanted):
        self.calls.append(identity)
        for record in self.records:
            if record['id'] == identity:
                record.update(copy.deepcopy(wanted))
        if self.uncertain:
            self.uncertain = False
            raise TimeoutError('response lost after provider committed')


class Recovery(unittest.TestCase):
    def setUp(self):
        self.api = Provider()
        self.before = make_snapshot(*self.api.current(), '192.0.2.10')
        self.saved = []
        self.change = Change(self.api, self.before, lambda n, d: self.saved.append(copy.deepcopy(d)))

    def test_apply_then_rollback_preserves_all_record_ids_and_mail(self):
        self.change.apply()
        self.assertEqual({r['id'] for r in self.api.records}, {r['id'] for r in self.before['records']})
        self.assertEqual(self.api.records[-1], self.before['records'][-1])
        self.change.rollback()
        self.assertEqual([shape(r) for r in self.api.records], [shape(r) for r in self.before['records']])

    def test_unknown_effect_is_reconciled_before_restoration(self):
        self.api.uncertain = True
        with self.assertRaises(TimeoutError): self.change.apply()
        self.change.rollback()
        self.assertEqual(self.api.records, self.before['records'])

    def test_failed_durable_custody_prevents_effect(self):
        def broken(*args): raise OSError('custody unavailable')
        self.change.persist = broken
        with self.assertRaises(OSError): self.change.apply()
        self.assertEqual(self.api.calls, [])

    def test_changed_mail_prevents_first_effect(self):
        self.api.records[-1]['content'] = 'new.example.test'
        with self.assertRaises(RuntimeError): self.change.apply()
        self.assertEqual(self.api.calls, [])

    def test_concurrent_web_change_is_never_overwritten_by_rollback(self):
        self.change.apply()
        self.api.records[0]['content'] = '192.0.2.99'
        count = len(self.api.calls)
        with self.assertRaisesRegex(RuntimeError, 'rollback_target_conflict'): self.change.rollback()
        self.assertEqual(len(self.api.calls), count)
        self.assertEqual(self.api.records[0]['content'], '192.0.2.99')

    def test_empty_dns_is_not_accepted_as_original_baseline(self):
        with self.assertRaises(RuntimeError): make_snapshot('zone', [], '192.0.2.10')

    def test_firewall_comparison_excludes_only_approved_ports_in_selected_zone(self):
        before = 'public (active)\n  services: ssh\n  ports:\nprivate\n  ports: 8080/tcp'
        after = 'public (active)\n  services: ssh\n  ports: 443/tcp 80/tcp\nprivate\n  ports: 8080/tcp'
        self.assertEqual(protected(before, 'public'), protected(after, 'public'))
        self.assertNotEqual(protected(before, 'public'), protected(after.replace('8080', '8001'), 'public'))


if __name__ == '__main__': unittest.main()
