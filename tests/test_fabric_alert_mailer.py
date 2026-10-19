import datetime as dt
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('mailer',Path(__file__).parents[1]/'scripts/fabric_alert_mailer.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
NOW=dt.datetime(2026,10,19,19,tzinfo=dt.timezone.utc)
class MailerTests(unittest.TestCase):
 def test_stale_and_invalid_receipts(self):
  self.assertIn('stale',m.classify({'observed_at_utc':'2020-01-01T00:00:00Z','alerts':[]},NOW)[0])
  self.assertIn('invalid',m.classify({'observed_at_utc':NOW.isoformat(),'alerts':['private free text']},NOW)[0])
 def test_unknown_not_healthy(self):
  self.assertIn('invalid',m.classify({},NOW)[0])
 def test_owner_pin(self):
  self.assertFalse(m.owner('other@example.org'))
 def run_case(self,initial,alerts):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'state.json'
   if initial: p.write_text(json.dumps(initial))
   with patch.object(m,'owner',return_value=True),patch.object(m,'api') as api:
    result=m.deliver({'recipient':'fixture'},p,alerts,NOW)
    api.assert_not_called()
    return result
 def test_healthy_silent(self): self.assertEqual(self.run_case({},[])['outcome'],'NO_CHANGE')
 def test_uncertain_not_retried(self): self.assertEqual(self.run_case({'phase':'intent'},['P1:host:x'])['outcome'],'HOLD_UNCERTAIN_SEND')
 def test_changed_incident_rate_limit(self):
  self.assertEqual(self.run_case({'phase':'accepted','sent_at':NOW.isoformat()},['P1:host:x'])['outcome'],'RATE_LIMITED')
 def test_timeout_persists_uncertain_and_prevents_retry(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'state.json'
   binding={'inbox_id':m.INBOX,'metadata':{'alertRecipient':'fixture'}}
   with patch.object(m,'owner',return_value=True),patch.object(m,'api',side_effect=[binding,TimeoutError()]):
    with self.assertRaises(TimeoutError):m.deliver({'recipient':'fixture','key':'fixture'},p,['P1:host:x'],NOW)
   self.assertEqual(json.loads(p.read_text())['phase'],'uncertain')
 def test_exact_replay_suppressed(self):
  import hashlib
  alerts=['P1:host:x']
  digest=hashlib.sha256(json.dumps(alerts).encode()).hexdigest()
  self.assertEqual(self.run_case({'phase':'accepted','digest':digest},alerts)['outcome'],'NO_CHANGE')
if __name__=='__main__': unittest.main()
