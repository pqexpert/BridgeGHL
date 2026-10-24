import hashlib
import os
from pathlib import Path,PosixPath
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"scripts"))
import deploy_native_rsc as m

class RootPath(PosixPath):
    def stat(self,*args,**kwargs):
        values=list(super().stat(*args,**kwargs));values[4]=0
        return os.stat_result(values)

class ReceiptInstall(unittest.TestCase):
    def test_unknown_source_is_preserved_and_known_source_is_read_back(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=RootPath(tmp)/"receipt.py";path.write_text("other");path.chmod(0o600)
            payload={"receipt_source":"new","receipt_sha256":hashlib.sha256(b"new").hexdigest(),
                     "receipt_previous_sha256":hashlib.sha256(b"old").hexdigest()}
            def atomic(p,value):p.write_text(value)
            with patch.object(m,"Path",return_value=path):
                with self.assertRaisesRegex(ValueError,"changed_preserved"):m.install_receipt(payload,atomic)
                self.assertEqual(path.read_text(),"other")
                path.write_text("old");m.install_receipt(payload,atomic)
                self.assertEqual(path.read_text(),"new")
                m.install_receipt(payload,atomic)
                payload["receipt_source"]="tampered"
                with self.assertRaisesRegex(ValueError,"source_hash"):m.install_receipt(payload,atomic)

if __name__=="__main__":unittest.main()
