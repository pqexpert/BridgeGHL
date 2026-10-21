import base64
import importlib.util
import io
from pathlib import Path
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("recovery", Path(__file__).parents[1] / "scripts/local_recovery_snapshot.py")
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)

class RecoveryTests(unittest.TestCase):
    def fixture(self, root):
        paths = {}
        for i, name in enumerate(sorted(r.REQUIRED)):
            path = root / str(i)
            path.write_bytes(("fixture:" + name).encode())
            paths[name] = path
        return r.capture(paths)

    def test_roundtrip_and_failed_upload_preserves_latest(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            raw, manifest = self.fixture(root)
            dest = root / "received"
            capsule = r.seal(raw)
            report = r.receive(capsule, dest)
            restored, files = r.read_latest(dest)
            self.assertEqual(restored["fingerprint"], manifest["fingerprint"])
            self.assertEqual(files["state/api-key"], b"fixture:state/api-key")
            self.assertTrue(report["verified"])
            before = (dest / "latest.json").read_bytes()
            broken = dict(capsule)
            encrypted = bytearray(base64.b64decode(broken["ciphertext"]))
            encrypted[-1] ^= 1
            broken["ciphertext"] = base64.b64encode(encrypted).decode()
            with self.assertRaises(Exception):
                r.receive(broken, dest)
            self.assertEqual((dest / "latest.json").read_bytes(), before)
            self.assertEqual(dest.stat().st_mode & 0o777, 0o700)
            for path in dest.iterdir():
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_traversal_rejected_without_extraction(self):
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode="w:gz") as tar:
            member = tarfile.TarInfo("../../escape")
            member.size = 1
            tar.addfile(member, io.BytesIO(b"x"))
        with self.assertRaises(ValueError):
            r.unseal(r.seal(raw.getvalue()))

if __name__ == "__main__":
    unittest.main()
