#!/usr/bin/env bash
set -euo pipefail
package="$1"
expected_sha="$2"
[[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]]
app_dir=/home/bridgeadmin/apps/BridgeGHL
stage=$(mktemp -d)
backup=$(mktemp -d)
privilege=()
if [[ $(id -u) -ne 0 ]]; then privilege=(sudo -n); fi
cleanup() { rm -rf "$stage"; rm -f "$package"; }
trap cleanup EXIT
tar -xzf "$package" -C "$stage"
test "$(cat "$stage/DEPLOYED_SHA")" = "$expected_sha"
files=(app.py ingestion.py scripts/ingest_drive.py scripts/runtime_ingestion_check.py scripts/configure_ecosystem.py scripts/project_career_fields.py DEPLOYED_SHA)
for file in "${files[@]}"; do
  test -f "$stage/$file"
  "${privilege[@]}" mkdir -p "$backup/$(dirname "$file")"
  if "${privilege[@]}" test -f "$app_dir/$file"; then
    "${privilege[@]}" cp -a "$app_dir/$file" "$backup/$file"
  fi
done
rollback() {
  for file in "${files[@]}"; do
    if "${privilege[@]}" test -f "$backup/$file"; then
      "${privilege[@]}" cp -a "$backup/$file" "$app_dir/$file"
    else
      "${privilege[@]}" rm -f "$app_dir/$file"
    fi
  done
  "${privilege[@]}" systemctl restart bridgeghl.service
  echo 'deployment_rolled_back=true'
}
trap 'rollback; cleanup' ERR
for file in "${files[@]}"; do
  "${privilege[@]}" mkdir -p "$app_dir/$(dirname "$file")"
  "${privilege[@]}" install -m 644 "$stage/$file" "$app_dir/$file"
done
if command -v restorecon >/dev/null; then "${privilege[@]}" restorecon -RF "$app_dir"; fi
"${privilege[@]}" systemctl restart bridgeghl.service
"${privilege[@]}" systemctl is-active --quiet bridgeghl.service
verified=false
for attempt in {1..10}; do
  if python3 - <<'PY'
import json
from urllib.request import urlopen
with urlopen('http://127.0.0.1:8000/openapi.json', timeout=5) as r:
    paths = json.load(r)['paths']
assert '/execute/ingest/source-record' in paths
assert '/dry-run/ingest/source-record' in paths
PY
  then verified=true; break; fi
  sleep 1
done
[[ "$verified" == true ]]
trap - ERR
echo "deployed_sha=$expected_sha"
echo 'ingestion_routes_verified=true'
# Use systemd's env parser; never source or print the protected credential file.
"${privilege[@]}" systemd-run --quiet --wait --pipe --collect \
  --property=EnvironmentFile=/etc/bridgeghl/bridgeghl.env \
  /usr/bin/python3 "$app_dir/scripts/runtime_ingestion_check.py"
