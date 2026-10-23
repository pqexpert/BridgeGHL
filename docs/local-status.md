# Local and VPS work status in VS Code

Open this BridgeGHL checkout as a VS Code folder. Run **Terminal → Run Task → Ziji Fabric: local status (no inference)**. Run it again when you want a fresh snapshot. There is no background poller or new schedule.

From its terminal:

```sh
python3 scripts/local_fabric_status.py
```

The task reads only three user-service status summaries, the authenticated loopback health endpoint and selected metadata from existing local receipt files. The local bearer stays in memory and is never printed. It makes zero inference, external API or SSH calls and changes no service. Receipt timestamps are file modification times, not invented job completion timestamps. Local CLI receipts do not count every VPS-side job.

- **READY** means the local model endpoint can accept a request; it does not mean an agent is working.
- An active tunnel is a local service observation, not proof of a live VPS round trip.
- VPS execution and encrypted GitHub backup evidence remain in [the execution ledger](fabric-cutover-execution.md), with exact run links and dates. This task does not relabel historical evidence as live status.
- Hosted actor schedules remain distinct from local model availability. The current local Codex launcher produces read-only proposals; this status task does not grant editing or external-effect authority.

For actual admitted work use the existing owning workflow and its durable receipt. Do not create synthetic jobs or continual model calls merely to keep a progress display busy. The existing local recovery timer and native/hosted schedules keep their current ownership; this task adds no recurring load.

## Dated VPS worker results

The same status task also displays a private local cache at
`~/.local/share/ziji-local/vps-worker-receipts.json` when present. Each result is labelled **CACHED_VPS_RECEIPT** with its original `observed_at`; the cache file timestamp is shown separately. Refreshing the task does not refresh the VPS, perform inference or contact any provider. An old result never becomes current merely because you reopen the task.

Existing deployment verification may save this owner-only (0600) JSON file with `schema: "ziji-cached-vps-receipts/1"` and a `receipts` list. Displayed fields are limited to lane, observed_at, outcome, action_class, run_id, source_sha, artifact_sha256, native_readback, whole_actor_replaced, paid_fallback and model_calls. No draft bodies, credentials, private source facts or free-form notes are displayed. GitHub run IDs resolve under the existing BridgeGHL Actions page. The original native receipt remains authoritative; this file is a dated local view, not a new ledger or automatic synchronization process.

## Operating rule

Keep Duck awake and on AC when local inference is wanted; this is an operating instruction, not proof of its current power state. The VPS uses the existing hourly collector, source hashes and replay suppression. HighLevel work uses changed-state batches, not a real-time polling loop. Shared local inference capacity is one request at a time with no paid fallback. An empty or unchanged queue should remain idle.

Income and RSC native work remain bounded portions of their missions. Store remains a hosted exception. Distiller writer ownership must follow its actual coordinated cutover receipt; local model readiness alone changes none of these boundaries.
