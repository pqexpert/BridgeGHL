#!/usr/bin/env bash
set -euo pipefail
package="$1"
expected_sha="$2"
task_writes="$3"
[[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]]
[[ "$task_writes" == true || "$task_writes" == false ]]
app_dir=/home/bridgeadmin/apps/BridgeGHL
stage=$(mktemp -d)
backup=$(mktemp -d)
privilege=()
if [[ $(id -u) -ne 0 ]]; then privilege=(sudo -n); fi
env_file=/etc/bridgeghl/bridgeghl.env
previous_task_flag=$("${privilege[@]}" python3 - "$env_file" <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
matches = [line.split('=', 1)[1].strip() for line in path.read_text().splitlines()
           if line.startswith('HIGHLEVEL_TASK_WRITES_ENABLED=')]
if len(matches) > 1:
    raise SystemExit('duplicate HIGHLEVEL_TASK_WRITES_ENABLED entries')
print(matches[0] if matches else '__MISSING__')
PY
)
set_task_flag() {
  "${privilege[@]}" python3 - "$env_file" "$1" <<'PY'
import os, pathlib, sys, tempfile
path = pathlib.Path(sys.argv[1])
value = sys.argv[2]
stat = path.stat()
lines = path.read_text().splitlines()
matches = [i for i, line in enumerate(lines) if line.startswith('HIGHLEVEL_TASK_WRITES_ENABLED=')]
if len(matches) > 1:
    raise SystemExit('duplicate HIGHLEVEL_TASK_WRITES_ENABLED entries')
if value == '__MISSING__':
    lines = [line for line in lines if not line.startswith('HIGHLEVEL_TASK_WRITES_ENABLED=')]
elif matches:
    lines[matches[0]] = 'HIGHLEVEL_TASK_WRITES_ENABLED=' + value
else:
    lines.append('HIGHLEVEL_TASK_WRITES_ENABLED=' + value)
fd, temporary = tempfile.mkstemp(prefix=path.name + '.', dir=path.parent)
try:
    with os.fdopen(fd, 'w') as stream:
        stream.write('\n'.join(lines) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.chown(temporary, stat.st_uid, stat.st_gid)
    os.chmod(temporary, stat.st_mode & 0o777)
    os.replace(temporary, path)
finally:
    if os.path.exists(temporary):
        os.unlink(temporary)
PY
}
cleanup() { rm -rf "$stage"; rm -f "$package"; }
trap cleanup EXIT
tar -xzf "$package" -C "$stage"
test "$(cat "$stage/DEPLOYED_SHA")" = "$expected_sha"
files=(app.py ingestion.py provider_budget.py journey_evidence.py scripts/ingest_drive.py scripts/runtime_ingestion_check.py scripts/configure_ecosystem.py scripts/project_career_fields.py scripts/accept_rsc_autonomy.py scripts/record_highlevel_cutover.py scripts/project_actor_receipts.py scripts/highlevel_request_queue.py scripts/collect_native_sources.py scripts/install_income_continuation.py scripts/income_continuation_bundle.json config/highlevel-actor-policy.json DEPLOYED_SHA)
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
  set_task_flag "$previous_task_flag"
  "${privilege[@]}" systemctl restart bridgeghl.service
  echo 'deployment_rolled_back=true'
}
trap 'rollback; cleanup' ERR
for file in "${files[@]}"; do
  "${privilege[@]}" mkdir -p "$app_dir/$(dirname "$file")"
  "${privilege[@]}" install -m 644 "$stage/$file" "$app_dir/$file"
done
# This RSC production deployment explicitly opts into the already-reviewed,
# exact-key task envelope. The application default remains fail-closed.
set_task_flag "$task_writes"
if command -v restorecon >/dev/null; then "${privilege[@]}" restorecon -RF "$app_dir"; fi
"${privilege[@]}" systemctl restart bridgeghl.service
"${privilege[@]}" systemctl is-active --quiet bridgeghl.service
export EXPECTED_TASK_WRITES="$task_writes"
verified=false
for attempt in {1..10}; do
  if python3 - <<'PY'
import json, os
from urllib.request import urlopen
with urlopen('http://127.0.0.1:8000/openapi.json', timeout=5) as r:
    paths = json.load(r)['paths']
assert '/read/journey-evidence' in paths
assert '/read/ingest/income-source' in paths
assert '/execute/ingest/source-record' in paths
assert '/dry-run/ingest/source-record' in paths
assert '/dry-run/task/create' in paths
assert '/execute/task/create' in paths
assert '/dry-run/task/delete' in paths
assert '/execute/task/delete' in paths
with urlopen('http://127.0.0.1:8000/health', timeout=8) as r:
    health = json.load(r)
assert health['state'] == 'HEALTHY'
expected = os.environ['EXPECTED_TASK_WRITES'] == 'true'
assert health['task_writes_enabled'] is expected
assert ({'create_task', 'delete_task'} <= set(health['allowed_actions'])) is expected
PY
  then verified=true; break; fi
  sleep 1
done
[[ "$verified" == true ]]
trap - ERR
echo "deployed_sha=$expected_sha"
echo 'ingestion_routes_verified=true'
echo "task_writes_enabled=$task_writes"
# Use systemd's env parser; never source or print the protected credential file.
"${privilege[@]}" systemd-run --quiet --wait --pipe --collect \
  --property=EnvironmentFile=/etc/bridgeghl/bridgeghl.env \
  /usr/bin/python3 "$app_dir/scripts/runtime_ingestion_check.py"
