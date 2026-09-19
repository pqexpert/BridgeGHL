#!/usr/bin/env bash
set -euo pipefail
trap 'echo "career_import_failed_line=$LINENO"' ERR
source_dir="$1"
app_dir=/home/bridgeadmin/apps/BridgeGHL
root=/var/lib/bridgeghl/career-import
sudo -n install -d -m 700 "$root"
sudo -n install -m 600 "$source_dir/source.json" "$source_dir/control.json" "$root/"
sudo -n install -m 644 "$source_dir/bootstrap_career.py" "$app_dir/scripts/bootstrap_career.py"
rm -rf "$source_dir"
service_pid=$(sudo -n systemctl show bridgeghl.service --property=MainPID --value)
python_path=$(sudo -n python3 - "$service_pid" <<'PY'
import pathlib,subprocess,sys
args=pathlib.Path('/proc/'+sys.argv[1]+'/cmdline').read_bytes().split(b'\0')
candidates=[]
for arg in args[:2]:
    p=pathlib.Path(arg.decode())
    if not p.is_absolute(): continue
    if 'python' in p.name: candidates.append(str(p))
    if p.is_file():
        with p.open('rb') as f: first=f.readline(256)
        if first.startswith(b'#!') and b'python' in first:
            candidates.append(first[2:].decode().strip().split()[0])
    candidates.append(str(p.parent/'python'))
for candidate in candidates:
    try:
        if subprocess.run([candidate,'-c','import fastapi,requests'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:
            print(candidate);break
    except OSError: pass
PY
)
test -n "$python_path"
sudo -n systemd-run --quiet --wait --pipe --collect \
  --property=EnvironmentFile=/etc/bridgeghl/bridgeghl.env \
  /usr/bin/env LIVE_WRITE_ENABLED=true HIGHLEVEL_INGEST_ENABLED=true \
  "$python_path" "$app_dir/scripts/bootstrap_career.py"
sudo -n systemctl restart bridgeghl.service
for attempt in {1..15}; do
  if curl -fsS http://127.0.0.1:8000/health >/dev/null; then break; fi
  sleep 1
done
sha=$(sudo -n "$python_path" -c "import json; print(json.load(open('$root/control.json'))['sha256'])")
sudo -n systemd-run --quiet --wait --pipe --collect \
  --property=EnvironmentFile=/etc/bridgeghl/bridgeghl.env \
  "$python_path" "$app_dir/scripts/ingest_drive.py" --file "$root/source.json" --sha256 "$sha" --receipt "$root/dry-run.json"
sudo -n systemd-run --quiet --wait --pipe --collect \
  --property=EnvironmentFile=/etc/bridgeghl/bridgeghl.env \
  "$python_path" "$app_dir/scripts/ingest_drive.py" --file "$root/source.json" --sha256 "$sha" --receipt "$root/receipt.json" --execute
