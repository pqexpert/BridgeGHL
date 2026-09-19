#!/usr/bin/env bash
set -euo pipefail
trap 'echo "career_import_failed_line=$LINENO"' ERR
source_dir="$1"
app_dir=/home/bridgeadmin/apps/BridgeGHL
root=/var/lib/bridgeghl/career-import
sudo -n install -d -m 700 "$root"
sudo -n python3 - "$root" "$source_dir/control.json" <<'PY'
import hashlib,json,os,sys
from pathlib import Path
root=Path(sys.argv[1]); expected=json.load(open(sys.argv[2]))['sha256']
if (root/'receipt.json').exists():
    if hashlib.sha256((root/'source.json').read_bytes()).hexdigest()!=expected:
        raise SystemExit('Refusing receipt reuse for a different source export')
    (root/'receipt.sha256').write_text(expected);os.chmod(root/'receipt.sha256',0o600)
PY
sudo -n install -m 600 "$source_dir/source.json" "$source_dir/control.json" "$root/"
sudo -n install -m 644 "$source_dir/bootstrap_career.py" "$app_dir/scripts/bootstrap_career.py"
rm -rf "$source_dir"
# Use an isolated administrative runner; never guess or modify service dependencies.
python_path="$root/runner/bin/python"
if ! sudo -n "$python_path" -c 'import fastapi,requests' >/dev/null 2>&1; then
  sudo -n /usr/bin/python3 -m venv "$root/runner"
  sudo -n "$python_path" -m pip install --disable-pip-version-check -q -r "$app_dir/requirements.txt"
fi
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
  "$python_path" "$app_dir/scripts/ingest_drive.py" --file "$root/source.json" --sha256 "$sha" --receipt "$root/receipt.json" --execute --resume
