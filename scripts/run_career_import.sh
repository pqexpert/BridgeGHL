#!/usr/bin/env bash
set -euo pipefail
source_dir="$1"
app_dir=/home/bridgeadmin/apps/BridgeGHL
root=/var/lib/bridgeghl/career-import
sudo -n install -d -m 700 "$root"
sudo -n install -m 600 "$source_dir/source.json" "$source_dir/control.json" "$root/"
sudo -n install -m 644 "$source_dir/bootstrap_career.py" "$app_dir/scripts/bootstrap_career.py"
rm -rf "$source_dir"
python_path=""
for candidate in "$app_dir/.venv/bin/python" "$app_dir/venv/bin/python" /usr/bin/python3; do
  if "$candidate" -c 'import fastapi,requests' >/dev/null 2>&1; then python_path="$candidate"; break; fi
done
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
