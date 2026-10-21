import json
from pathlib import Path
import sqlite3
import tarfile
import tempfile
import unittest
from scripts.fabric_snapshot import capture, verify


class SnapshotTest(unittest.TestCase):
    def test_online_wal_backup_manifest_and_exclusions(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root = base / 'runtime'
            root.mkdir()
            out = base / 'private'
            out.mkdir(mode=0o700)
            (root / 'state.txt').write_text('private data')
            (root / 'state.txt').chmod(0o640)
            (root / 'current').symlink_to('state.txt')
            (root / '.venv').mkdir()
            (root / '.venv' / 'excluded').write_text('dependency')
            (root / 'runner').mkdir()
            (root / 'runner' / 'pyvenv.cfg').write_text('home = /usr/bin')
            (root / 'runner' / 'excluded-package').write_text('dependency')
            db = sqlite3.connect(str(root / 'state.sqlite'))
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('CREATE TABLE state (value TEXT)')
            db.execute("INSERT INTO state VALUES ('committed WAL value')")
            db.commit()
            archive = out / 'snapshot.tar.gz'
            report = capture(archive, [root])
            self.assertEqual(report['sqlite_databases'], 1)
            self.assertEqual(report['dependency_environments'], 1)
            self.assertTrue(verify(archive)['archive_verified'])
            with tarfile.open(archive) as tar:
                names = tar.getnames()
                self.assertFalse(any('.venv' in p or p.endswith('-wal') for p in names))
                self.assertTrue(any(p.endswith('runner/pyvenv.cfg') for p in names))
                self.assertFalse(any(p.endswith('excluded-package') for p in names))
                manifest = json.load(tar.extractfile('FABRIC-SNAPSHOT-MANIFEST.json'))
                self.assertTrue(any(e['kind'] == 'symlink' for e in manifest['entries']))
                recovered = out / 'recovered.sqlite'
                recovered.write_bytes(tar.extractfile(str(root / 'state.sqlite').lstrip('/')).read())
                with sqlite3.connect(str(recovered)) as connection:
                    self.assertEqual(connection.execute('SELECT value FROM state').fetchone()[0], 'committed WAL value')
            db.close()
            self.assertEqual(archive.stat().st_mode & 0o777, 0o600)

    def test_refuses_existing_or_inside_source_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(ValueError):
                capture(root / 'snapshot.tar.gz', [root])
            existing = root / 'existing'
            existing.touch()
            with self.assertRaises(ValueError):
                capture(existing, [])


if __name__ == '__main__':
    unittest.main()
