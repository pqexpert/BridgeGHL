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
| Local inference | Hugging Face connector authenticated; VPS has four CPUs and ~6.7 GiB available RAM | Desktop Commander duck connected; BridgeGHL checkout and Python 3.11 environment verified with 201 baseline tests; Ollama inference not installed or verified |

Evidence: [Google binding](https://github.com/pqexpert/BridgeGHL/actions/runs/39666628336), [strict Jira identity](https://github.com/pqexpert/BridgeGHL/actions/runs/39666892719), [native collection](https://github.com/pqexpert/BridgeGHL/actions/runs/39667330246), [verified encrypted recovery](https://github.com/pqexpert/fabric-dispatch/releases/tag/server-backup-39641666530-3).

## Execution and cost boundary

The existing VPS runs deterministic collection without an LLM. The installed hourly cron entry invokes `/opt/ziji-fabric/native/collect_native_sources.py` at minute 11 UTC. Root-only snapshots and sanitized latest status live in `/var/lib/ziji-fabric/native-sources`. Provider bindings remain under `/etc/ziji-fabric/providers`; never paste their contents into a terminal transcript or chat.

The collector is read-only. It fully pages RBO-97 comments and reads a rolling past-48/next-36-hour Calendar window. It retains exact descriptions only for private, attendee-free Fabric-marked events. This read filter is NOT a writer allowlist. Snapshot persistence does not assert that feedback was interpreted, consumed or applied. The existing hosted Distiller remains the sole Calendar writer.

The existing VPS Distiller service still produces proposals from submitted snapshots using its configured external model. Moving source storage onto a server does not make inference local. The Income source home is a recovery/instruction package, not a complete native autonomous mission runner. Hosted specialist schedules remain enabled until their actual source adapters and authorized effects have replacements.

For local VS Code/Codex work, use the existing GitHub repositories and their AGENTS.md instructions. Check the local Ollama endpoint and installed model on that actual computer; this cloud session is a different environment. Desktop Commander is connected to duck; the GitHub-authenticated checkout is `/home/jaden/projects/BridgeGHL`. Use Desktop Commander for local files, GitHub for reviewed live source, and the existing protected SSH workflow for IONOS. The Codex extension is installed; editor launch was not independently confirmed. Do not expose Ollama publicly or assume ChatGPT connector credentials transfer to it. Hugging Face access here supports repository reads and jobs; no paid job or endpoint was started.

## Next bounded implementation

1. Read the current native collection status and compare scheduled snapshots without inventing actor success from prose markers.
2. Recover the exact private Calendar series map and historical human-input baseline, implement owner-feedback ingestion and existing protected-field/suffix behavior in the Distiller adapter.
3. Connect admitted source-bound specialist outputs to Distiller. Port Income and RSC mission units independently; retain the owner-selected hosted Store route.
4. Transfer one Calendar writer only after native comparison and rollback evidence; then retire only its matching hosted invocation. Do not disable the hosted writer merely because OAuth works.
5. Keep the daily encrypted recovery workflow and update RBO-97 with actual execution evidence.

## Continuation — 2026-10-21 21:20 UTC

PR #63 prepares current native-snapshot inspection and repairs three PR #62 findings: malformed prior metadata no longer prevents fresh lane collection; collector deployment failure preserves binding receipts but returns failure; direct runs default to manual, while the managed cron explicitly supplies its trigger. A recorded trigger is self-reporting, not independent scheduler evidence. These changes are pending merge and live deployment.

The existing hosted Distiller task provided the exact four-series map and its owner-feedback contract. Keep private identifiers out of this public repository. Prior/current suffix comparison is only continuity evidence; it does not establish historical feedback consumption or grant write authority. The remaining work is native adapter integration, source-bound specialist execution and the deliberate sole-writer handover.

Recovery evidence superseding attempt 3: https://github.com/pqexpert/fabric-dispatch/releases/tag/server-backup-39641666530-4 (download/decryption/member verification recorded in RBO-97/11958). RBO-97/11959 corrects the earlier claim that trigger=cron alone proved a scheduled invocation. Hosted specialists and Calendar writer remain active.
