import importlib.util
from pathlib import Path
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('backup',Path(__file__).parents[1]/'scripts/fabric_github_backup.py')
m=importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class BackupTests(unittest.TestCase):
    def test_roundtrip_and_tamper_rejected(self):
        from cryptography.exceptions import InvalidTag
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            source=root/'source'; encrypted=root/'encrypted'; restored=root/'restored'
            source.write_bytes(b'private payload\x00'*100000)
            key=b'k'*32
            m.encrypt(source,encrypted,key)
            self.assertNotIn(b'private payload',encrypted.read_bytes())
            m.decrypt(encrypted,restored,key)
            self.assertEqual(source.read_bytes(),restored.read_bytes())
            content=bytearray(encrypted.read_bytes()); content[-20]^=1
            encrypted.write_bytes(content)
            with self.assertRaises(InvalidTag): m.decrypt(encrypted,restored,key)

if __name__=='__main__': unittest.main()
