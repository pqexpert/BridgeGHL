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
