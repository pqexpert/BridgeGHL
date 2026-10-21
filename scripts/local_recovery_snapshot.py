"""Bounded encrypted laptop recovery, carried over the existing SSH trust path."""
import argparse
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
from datetime import datetime, timezone

SCHEMA = "ziji-local-recovery/1"
AAD = SCHEMA.encode()
LIMIT = 10 * 1024 * 1024
DEST = Path("/var/lib/ziji-fabric/local-recovery/duck")
ALLOWED = {
    "state/memory.json", "state/input-lock.json", "state/runtime-lock.json",
    "state/codex-binding.json", "state/api-key", "state/transport.json",
    "launchers/ziji-local", "launchers/ziji-codex",
    "services/ziji-local-model.service", "services/ziji-local-tunnel.service",
    "services/ziji-local-recovery.service", "services/ziji-local-recovery.timer",
    "config/model-lock.json", "config/system.txt", "config/sources.json",
    "source/local_inference.py", "source/local_codex.py",
    "source/local_inference_transport.py", "source/local_recovery_snapshot.py",
}
REQUIRED = {"state/memory.json", "state/input-lock.json", "state/runtime-lock.json",
            "state/codex-binding.json", "state/api-key",
            "services/ziji-local-model.service", "config/model-lock.json", "config/system.txt",
            "config/sources.json", "source/local_inference.py", "source/local_codex.py"}

def sha(data):
    return hashlib.sha256(data).hexdigest()

def sources(home, inference_root):
    state = home / ".local/share/ziji-local"
    result = {}
    for name in ("memory.json", "input-lock.json", "runtime-lock.json", "codex-binding.json", "api-key", "transport.json"):
        result["state/" + name] = state / name
    for name in ("ziji-local", "ziji-codex"):
        result["launchers/" + name] = home / ".local/bin" / name
    for name in ("ziji-local-model.service", "ziji-local-tunnel.service", "ziji-local-recovery.service", "ziji-local-recovery.timer"):
        result["services/" + name] = home / ".config/systemd/user" / name
    for name in ("model-lock.json", "system.txt", "sources.json"):
        result["config/" + name] = inference_root / "config/local-inference" / name
    for name in ("local_inference.py", "local_codex.py", "local_inference_transport.py"):
        result["source/" + name] = inference_root / "scripts" / name
    transport_source = inference_root.parent / "BridgeGHL-local-transport/scripts/local_inference_transport.py"
    if transport_source.exists():
        result["source/local_inference_transport.py"] = transport_source
    result["source/local_recovery_snapshot.py"] = Path(__file__).resolve()
    return result

def capture(paths):
    files, items = {}, {}
    for name, path in paths.items():
        if name not in ALLOWED:
            raise ValueError("source_not_allowlisted")
        if not path.exists():
            if name in REQUIRED:
                raise ValueError("required_source_missing")
            continue
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as stream:
            before = os.fstat(stream.fileno())
            data = stream.read(LIMIT + 1)
            after = os.fstat(stream.fileno())
        if len(data) > LIMIT or (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError("source_unstable_or_oversize")
        files[name] = data
        items[name] = {"sha256": sha(data), "bytes": len(data), "mode": before.st_mode & 0o777}
    if not REQUIRED.issubset(files):
        raise ValueError("required_source_missing")
    fingerprint = sha(json.dumps(items, sort_keys=True).encode())
    manifest = {"schema": SCHEMA, "created_at": datetime.now(timezone.utc).isoformat(),
                "fingerprint": fingerprint, "files": items,
                "omitted": ["model weights (pinned downloadable)", "build dependencies", "private chat", "SaaS datasets", "SSH private keys"],
                "restore_note": "Compare current state before restoring; regenerate/rebind host-specific paths and tunnel SSH authorization."}
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as tar:
        for name, data in {**files, "manifest.json": json.dumps(manifest, sort_keys=True).encode()}.items():
            member = tarfile.TarInfo(name)
            member.mode = 0o600
            member.size = len(data)
            tar.addfile(member, io.BytesIO(data))
    raw = archive.getvalue()
    if len(raw) > LIMIT // 2:
        raise ValueError("snapshot_oversize")
    return raw, manifest

def seal(raw):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    key, nonce = os.urandom(32), os.urandom(12)
    encrypted = AESGCM(key).encrypt(nonce, raw, AAD)
    return {"schema": SCHEMA, "key": base64.b64encode(key).decode(),
            "nonce": base64.b64encode(nonce).decode(), "ciphertext": base64.b64encode(encrypted).decode()}

def unseal(capsule):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    if set(capsule) != {"schema", "key", "nonce", "ciphertext"} or capsule["schema"] != SCHEMA:
        raise ValueError("invalid_capsule")
    key, nonce, ciphertext = [base64.b64decode(capsule[k], validate=True) for k in ("key", "nonce", "ciphertext")]
    if len(key) != 32 or len(nonce) != 12 or len(ciphertext) > LIMIT:
        raise ValueError("invalid_capsule_size")
    raw = AESGCM(key).decrypt(nonce, ciphertext, AAD)
    files = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
        for member in tar:
            if not member.isfile() or member.name not in ALLOWED | {"manifest.json"} or member.name in files or member.size > LIMIT:
                raise ValueError("invalid_archive_member")
            files[member.name] = tar.extractfile(member).read()
            if sum(map(len, files.values())) > LIMIT:
                raise ValueError("expanded_snapshot_oversize")
    manifest = json.loads(files.pop("manifest.json"))
    if manifest.get("schema") != SCHEMA or not REQUIRED.issubset(files) or set(files) != set(manifest["files"]):
        raise ValueError("invalid_manifest")
    for name, data in files.items():
        if sha(data) != manifest["files"][name]["sha256"] or len(data) != manifest["files"][name]["bytes"]:
            raise ValueError("member_integrity_mismatch")
    if sha(json.dumps(manifest["files"], sort_keys=True).encode()) != manifest["fingerprint"]:
        raise ValueError("fingerprint_mismatch")
    return manifest, files

def write_private(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)

def receive(capsule, destination=DEST):
    manifest, files = unseal(capsule)
    destination.mkdir(mode=0o700, parents=True, exist_ok=True)
    if destination.is_symlink() or destination.stat().st_mode & 0o077:
        raise ValueError("destination_not_private")
    encrypted = base64.b64decode(capsule["nonce"]) + base64.b64decode(capsule["ciphertext"])
    identity = sha(encrypted)
    cipher_path, key_path = destination / (identity + ".aesgcm"), destination / (identity + ".key")
    write_private(cipher_path, encrypted)
    write_private(key_path, base64.b64decode(capsule["key"]))
    # Read back/decrypt the exact stored bytes before moving the pointer.
    disk = {"schema": SCHEMA, "key": base64.b64encode(key_path.read_bytes()).decode(),
            "nonce": base64.b64encode(cipher_path.read_bytes()[:12]).decode(),
            "ciphertext": base64.b64encode(cipher_path.read_bytes()[12:]).decode()}
    check, _ = unseal(disk)
    record = {"schema": SCHEMA, "ciphertext_sha256": identity, "fingerprint": check["fingerprint"],
              "created_at": manifest["created_at"], "entries": len(files), "verified": True}
    with tempfile.NamedTemporaryFile(dir=destination, delete=False) as tmp:
        tmp.write(json.dumps(record, sort_keys=True).encode() + b"\n")
        tmp_name = tmp.name
    os.replace(tmp_name, destination / "latest.json")
    return record

def read_latest(destination=DEST):
    record = json.loads((destination / "latest.json").read_text())
    identity = record["ciphertext_sha256"]
    if not re.fullmatch("[0-9a-f]{64}", identity):
        raise ValueError("invalid_stored_identity")
    encrypted = (destination / (identity + ".aesgcm")).read_bytes()
    if sha(encrypted) != identity:
        raise ValueError("stored_ciphertext_mismatch")
    key = (destination / (identity + ".key")).read_bytes()
    capsule = {"schema": SCHEMA, "key": base64.b64encode(key).decode(),
               "nonce": base64.b64encode(encrypted[:12]).decode(),
               "ciphertext": base64.b64encode(encrypted[12:]).decode()}
    manifest, files = unseal(capsule)
    if manifest["fingerprint"] != record["fingerprint"]:
        raise ValueError("stored_pointer_mismatch")
    return manifest, files

def send(config, inference_root):
    home = Path.home()
    raw, manifest = capture(sources(home, inference_root))
    receipt = home / ".local/share/ziji-local/recovery-upload.json"
    if receipt.exists() and json.loads(receipt.read_text()).get("fingerprint") == manifest["fingerprint"]:
        return {"outcome": "UNCHANGED", "fingerprint": manifest["fingerprint"], "api_calls": 0}
    target = json.loads(config.read_text())
    host, user = target["host"], target["user"]
    if not re.fullmatch(r"[A-Za-z0-9.:-]+", host) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", user):
        raise ValueError("invalid_ssh_target")
    command = ["ssh", "-i", target["key_file"], "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
               "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile="+target["known_hosts_file"],
               user+"@"+host, "local-recovery"]
    result = subprocess.run(command, input=json.dumps(seal(raw)).encode(), capture_output=True, timeout=120)
    if result.returncode:
        raise ValueError("recovery_transport_failed")
    report = json.loads(result.stdout)
    if report.get("fingerprint") != manifest["fingerprint"] or report.get("verified") is not True:
        raise ValueError("remote_recovery_readback_failed")
    receipt.write_text(json.dumps(report, sort_keys=True)+"\n")
    receipt.chmod(0o600)
    return {"outcome": "VERIFIED_ON_VPS", **report}

def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--receive", action="store_true")
    mode.add_argument("--send", action="store_true")
    mode.add_argument("--verify-stored", action="store_true")
    parser.add_argument("--ssh-config", type=Path)
    parser.add_argument("--inference-root", type=Path, default=Path.home() / "projects/BridgeGHL-local-inference")
    args = parser.parse_args()
    if args.receive:
        body = sys.stdin.buffer.read(LIMIT + 1)
        if len(body) > LIMIT:
            raise ValueError("capsule_oversize")
        report = receive(json.loads(body))
    elif args.verify_stored:
        manifest, files = read_latest()
        report = {"verified": True, "fingerprint": manifest["fingerprint"], "entries": len(files)}
    else:
        if not args.ssh_config:
            raise ValueError("ssh_config_required")
        report = send(args.ssh_config, args.inference_root)
    print(json.dumps(report, sort_keys=True))

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(json.dumps({"outcome":"FAILED","error_type":type(exc).__name__,"details_withheld":True}))
        raise SystemExit(1)
