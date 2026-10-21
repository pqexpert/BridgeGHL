"""Capture the existing Fabric runtime into a private archive for encryption.

No provider calls, service stops, credential output or uploads. The caller MUST
 encrypt this archive before off-host persistence and remove plaintext afterwards.
SQLite files use their online backup API; other files are copied individually.
This is a file-level recovery snapshot, not a cross-service atomic checkpoint.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import tarfile
import tempfile
import time
from datetime import datetime, timezone

ROOTS = (
    '/opt/ziji-fabric', '/etc/ziji-fabric', '/var/lib/ziji-fabric',
    '/etc/bridgeghl', '/var/lib/bridgeghl',
    '/home/bridgeadmin/apps/BridgeGHL', '/etc/nginx',
)
PATTERNS = (
    '/etc/systemd/system/*bridgeghl*', '/etc/systemd/system/*ziji*',
    '/etc/cron.d/*ziji*',
)
EXCLUDED = {'.git', '.venv', 'venv', 'env', 'node_modules', '__pycache__',
            '.pytest_cache', '.mypy_cache', '.cache', 'backups', 'backup'}
SERVICES = ('bridgeghl', 'bridgeghl-mcp', 'ziji-distiller', 'crond', 'nginx')


def sources():
    import glob
    paths = [Path(p) for p in ROOTS]
    for pattern in PATTERNS:
        paths.extend(Path(p) for p in glob.glob(pattern))
    return sorted(set(paths))


def is_sqlite(path):
    try:
        with path.open('rb') as stream:
            return stream.read(16) == b'SQLite format 3\x00'
    except OSError:
        return False


def sqlite_copy(source, destination):
    # mode=ro preserves WAL visibility (immutable=1 would omit uncheckpointed data).
    with sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True,
                         timeout=30) as reader:
        with sqlite3.connect(str(destination)) as writer:
            deadline = time.monotonic() + 60
            def progress(status, remaining, total):
                if time.monotonic() > deadline:
                    raise TimeoutError('sqlite_backup_deadline')
            reader.backup(writer, pages=256, progress=progress, sleep=0.05)
            if writer.execute('PRAGMA quick_check').fetchone() != ('ok',):
                raise ValueError('sqlite_snapshot_invalid')


def metadata(info):
    return {'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid,
            'gid': info.st_gid, 'mtime_ns': info.st_mtime_ns}


def capture(output, roots=None):
    output = Path(output)
    if not output.is_absolute() or output.exists() or output.is_symlink():
        raise ValueError('output_must_be_new_absolute_path')
    if stat.S_IMODE(output.parent.stat().st_mode) & 0o077:
        raise ValueError('output_directory_must_be_private')
    roots = list(sources() if roots is None else roots)
    output_resolved = output.resolve()
    for root in roots:
        if output_resolved == root.resolve() or root.resolve() in output_resolved.parents:
            raise ValueError('output_inside_source')
    manifest = {'schema': 'fabric-runtime-snapshot/1',
                'started_at': datetime.now(timezone.utc).isoformat(),
                'consistency': 'sqlite_online_backup_and_individual_file_copies',
                'roots': [str(p) for p in roots], 'missing': [], 'entries': [],
                'excluded_directory_names': sorted(EXCLUDED),
                'excluded': [], 'unstable_files': [], 'dependency_environments': [],
                'dependency_restore_note': 'Reinstall omitted environments from versioned requirements; pyvenv.cfg records interpreter provenance, not a complete installed-package lock.'}
    seen = set()
    fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, 'wb') as raw, tarfile.open(fileobj=raw, mode='w:gz', dereference=False) as archive:
            with tempfile.TemporaryDirectory(prefix='fabric-copy-', dir=output.parent) as temporary:
                def add(path):
                    name = str(path).lstrip('/')
                    if name in seen:
                        return
                    seen.add(name)
                    info = path.lstat()
                    entry = {'path': str(path), **metadata(info)}
                    member = archive.gettarinfo(str(path), arcname=name)
                    if stat.S_ISLNK(info.st_mode):
                        archive.addfile(member)
                        entry.update(kind='symlink', target=os.readlink(path))
                    elif stat.S_ISDIR(info.st_mode):
                        archive.addfile(member)
                        entry['kind'] = 'directory'
                        manifest['entries'].append(entry)
                        if (path / 'pyvenv.cfg').is_file():
                            manifest['dependency_environments'].append(str(path))
                            # Keep the interpreter marker, omit binaries/packages regardless of directory name.
                            add(path / 'pyvenv.cfg')
                            manifest['excluded'].extend(str(child) for child in sorted(path.iterdir())
                                                        if child.name != 'pyvenv.cfg')
                            return
                        for child in sorted(path.iterdir()):
                            if (child.name in EXCLUDED and child.is_dir() and not child.is_symlink()
                                    and not (child / 'pyvenv.cfg').is_file()):
                                manifest['excluded'].append(str(child))
                                continue
                            # Never archive a live SQLite sidecar independently of its snapshot.
                            if child.name.endswith(('-wal', '-shm', '-journal')):
                                base = Path(str(child).rsplit('-', 1)[0])
                                if base.is_file() and is_sqlite(base):
                                    manifest['excluded'].append(str(child))
                                    continue
                            add(child)
                        return
                    elif stat.S_ISREG(info.st_mode):
                        copied = Path(temporary) / 'file'
                        if is_sqlite(path):
                            sqlite_copy(path, copied)
                            entry['kind'] = 'sqlite_online_backup'
                        else:
                            # Archive bytes are fixed before hashing; a changing file is disclosed.
                            with path.open('rb') as source, copied.open('wb') as target:
                                import shutil
                                shutil.copyfileobj(source, target, 1024 * 1024)
                            after = path.stat()
                            if (info.st_size, info.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                                manifest['unstable_files'].append(str(path))
                            entry['kind'] = 'file'
                        digest = hashlib.sha256()
                        with copied.open('rb') as stream:
                            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                                digest.update(chunk)
                            member.size = stream.tell()
                            stream.seek(0)
                            archive.addfile(member, stream)
                        entry.update(sha256=digest.hexdigest(), size=member.size)
                        copied.unlink()
                    else:
                        manifest['excluded'].append(str(path))
                        return
                    manifest['entries'].append(entry)
                for root in roots:
                    if not root.exists() and not root.is_symlink():
                        manifest['missing'].append(str(root))
                    else:
                        add(root)
                manifest['finished_at'] = datetime.now(timezone.utc).isoformat()
                encoded = json.dumps(manifest, sort_keys=True).encode()
                member = tarfile.TarInfo('FABRIC-SNAPSHOT-MANIFEST.json')
                member.mode = 0o600
                member.size = len(encoded)
                archive.addfile(member, io.BytesIO(encoded))
        return {'schema': manifest['schema'], 'archive_bytes': output.stat().st_size,
                'entries': len(manifest['entries']), 'missing_roots': len(manifest['missing']),
                'excluded_entries': len(manifest['excluded']),
                'unstable_files': len(manifest['unstable_files']),
                'sqlite_databases': sum(e['kind'] == 'sqlite_online_backup' for e in manifest['entries']),
                'dependency_environments': len(manifest['dependency_environments'])}
    except BaseException:
        output.unlink(missing_ok=True)
        raise


def verify(archive_path):
    """Verify member bytes and metadata without extracting or disclosing payloads."""
    with tarfile.open(archive_path, 'r:gz') as archive:
        manifest = json.load(archive.extractfile('FABRIC-SNAPSHOT-MANIFEST.json'))
        expected = {'FABRIC-SNAPSHOT-MANIFEST.json'}
        for item in manifest['entries']:
            name = item['path'].lstrip('/')
            expected.add(name)
            member = archive.getmember(name)
            if (member.mode, member.uid, member.gid) != (item['mode'], item['uid'], item['gid']):
                raise ValueError('metadata_mismatch')
            if 'sha256' in item:
                digest = hashlib.sha256()
                stream = archive.extractfile(member)
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(chunk)
                if digest.hexdigest() != item['sha256']:
                    raise ValueError('content_mismatch')
            elif item['kind'] == 'symlink' and member.linkname != item['target']:
                raise ValueError('symlink_mismatch')
        if set(archive.getnames()) != expected:
            raise ValueError('inventory_mismatch')
    return {'verified_entries': len(manifest['entries']), 'archive_verified': True}


def health():
    result = {}
    for service in SERVICES:
        run = subprocess.run(['systemctl', 'is-active', service], capture_output=True, text=True, timeout=10)
        state = run.stdout.strip()
        result[service] = state if state in ('active', 'inactive', 'failed', 'activating', 'unknown') else 'unknown'
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    try:
        os.umask(0o077)
        report = capture(args.output)
        report.update(verify(args.output))
        report['services'] = health()
        print(json.dumps(report, sort_keys=True))
    except Exception:
        print('{"snapshot_failed":true,"details_withheld":true}')
        raise SystemExit(1)
