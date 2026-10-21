# Fabric migration checkpoint — 2026-10-21

Controlling work: [RBO-97](https://restoration-mt.atlassian.net/browse/RBO-97). This is a dated implementation checkpoint, not a global acceptance certificate.

| Layer | Verified result | Remaining work |
| --- | --- | --- |
| IONOS / HighLevel | Bridge and MCP active; native HighLevel readback HTTP 200 | Preserve existing bounded CRM authority |
| Jira | Matching credential email required; issue read and five project permissions verified | Native receipt candidates need accountable per-lane interpretation |
| Google | Refresh-token exchange, intended account identity, Drive, five-calendar list and one owned WooCalendar event read verified | Sole-writer handover remains incomplete |
| Native source collection | PR #61 deployed; 530 Jira comments scanned, 253 receipt candidates, 66 Calendar events scanned, four private Fabric-marked events retained | Observe scheduled execution; do not equate candidate retrieval with specialist success |
| Shopify | Owner elected to retain existing hosted MCP route | Native app installation deferred; do not repeatedly retry it |
| Notion | Deferred by owner | Not a migration prerequisite |
| Recovery | Encrypted off-host archive downloaded, decrypted and checked; seven Git histories, 359 server entries, three SQLite snapshots | Not a native SaaS export or replacement-host boot test |
| Local inference | Hugging Face connector authenticated; VPS has four CPUs and ~6.7 GiB available RAM | No connected Desktop Commander device; Ollama absent on VPS; local VS Code/Ollama not verified |

Evidence: [Google binding](https://github.com/pqexpert/BridgeGHL/actions/runs/39666628336), [strict Jira identity](https://github.com/pqexpert/BridgeGHL/actions/runs/39666892719), [native collection](https://github.com/pqexpert/BridgeGHL/actions/runs/39667330246), [verified encrypted recovery](https://github.com/pqexpert/fabric-dispatch/releases/tag/server-backup-39641666530-3).

## Execution and cost boundary

The existing VPS runs deterministic collection without an LLM. The installed hourly cron entry invokes `/opt/ziji-fabric/native/collect_native_sources.py` at minute 11 UTC. Root-only snapshots and sanitized latest status live in `/var/lib/ziji-fabric/native-sources`. Provider bindings remain under `/etc/ziji-fabric/providers`; never paste their contents into a terminal transcript or chat.

The collector is read-only. It fully pages RBO-97 comments and reads a rolling past-48/next-36-hour Calendar window. It retains exact descriptions only for private, attendee-free Fabric-marked events. This read filter is NOT a writer allowlist. Snapshot persistence does not assert that feedback was interpreted, consumed or applied. The existing hosted Distiller remains the sole Calendar writer.

The existing VPS Distiller service still produces proposals from submitted snapshots using its configured external model. Moving source storage onto a server does not make inference local. The Income source home is a recovery/instruction package, not a complete native autonomous mission runner. Hosted specialist schedules remain enabled until their actual source adapters and authorized effects have replacements.

For local VS Code/Codex work, use the existing GitHub repositories and their AGENTS.md instructions. Check the local Ollama endpoint and installed model on that actual computer; this cloud session is a different environment. Desktop Commander currently reports no connected devices. The owner can attach the local device at https://mcp.desktopcommander.app/ if remote operation is desired. Do not expose Ollama publicly or assume ChatGPT connector credentials transfer to it. Hugging Face access here supports repository reads and jobs; no paid job or endpoint was started.

## Next bounded implementation

1. Read the current native collection status and compare scheduled snapshots without inventing actor success from prose markers.
2. Recover the exact private Calendar series map and historical human-input baseline, implement owner-feedback ingestion and existing protected-field/suffix behavior in the Distiller adapter.
3. Connect admitted source-bound specialist outputs to Distiller. Port Income and RSC mission units independently; retain the owner-selected hosted Store route.
4. Transfer one Calendar writer only after native comparison and rollback evidence; then retire only its matching hosted invocation. Do not disable the hosted writer merely because OAuth works.
5. Keep the daily encrypted recovery workflow and update RBO-97 with actual execution evidence.
