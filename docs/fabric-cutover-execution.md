# Fabric cutover execution

Accepted execution checklist, 2026-10-21. Owner approved distributed implementation using existing GitHub, IONOS VPS and local duck resources, with minimal API use and avoidable owner interruption. This is an execution ledger pointer, not a replacement Fabric Contract. Resolve current governance through `../PQEXPERT-ESTATE-OPERATING-POINTER.md`; record private/native evidence in RBO-97. Dated entries below are evidence pointers, not a claim of current runtime health.

## Finite work and completion evidence

| Item | Responsible track | Recorded state / next evidence |
| --- | --- | --- |
| 1. Consolidate source | Coordinator | Local inference [PR64](https://github.com/pqexpert/BridgeGHL/pull/64) merged at `8b12123ab4f269d429f80a7f3518d40b264e8d62`; exact weights, memory and launch configuration documented in `local-inference.md`. |
| 2. Bind existing credentials | Native execution | Existing Jira and Google bindings verified by protected run `39678040723`. Reuse the `zijifabric` environment and server-side files. Shopify hosted operation and Notion deferral remain deliberate owner choices. Verify only the binding needed for the next effect. |
| 3. Connect VPS to local inference | Local transport | In progress: authenticated private loopback transport, bounded timeout, durable offline queue and no automatic paid-model fallback. Completion needs actual VPS-to-duck response and offline behavior evidence. |
| 4. Reduce calls with deterministic work | Native execution | [PR65](https://github.com/pqexpert/BridgeGHL/pull/65) merged at `5ab283f0aa114d2a185bce854a341ce2e946d420`; removes two redundant Jira calls per hourly collection (48/day). Fresh identity validation and full comment pagination remain. Deployment readback is separate from merge. Scheduling, routing, deduplication and freshness checks should use code, not model calls. |
| 5. Move useful actor units independently | Native execution | In progress. Native source collection is not actor execution. Admit and demonstrate useful RSC/Income work one unit at a time with native output receipts. Preserve Store's chosen hosted route and its separation from BridgeGHL/CRM. |
| 6. Transfer sole Calendar writer | Distiller / coordinator | Read-only adapter in [fabric-distiller PR7](https://github.com/pqexpert/fabric-distiller/pull/7). Runtime integration, feedback coverage and owning-lane consumption remain separate. Hosted Distiller remains sole writer until a coordinated disable/enable transition and one admitted write/readback prove replacement. |
| 7. Retire matching hosted schedules | Coordinator | Pending each demonstrated replacement. Disable only the corresponding schedule; preserve cadence, useful mission behavior and a known return path. No wholesale switch or additional polling schedule. |
| 8. Extend encrypted recovery to local state | Recovery extension | Existing server/source recovery: `server-backup-39641666530-4`, RBO-97 evidence. Local capsule extension in progress. Completion requires downloading/decrypting the outer GitHub archive and verifying the included local capsule. Preserve pinned weight download identity and private state; do not commit secrets or model binaries to public Git. |
| 9. Close superseded routes and report | Coordinator | After dependent cutovers, retire only superseded routes. Maintain one compact existing progress record with evidence, remaining dependency and next bounded action. Notify the owner only for a changed condition requiring owner-only action. |

## Continuation and bounded recovery

The existing RSC recovery mandate may continue this checklist using its existing cadence and authority. Read current source, PR state and RBO-97 evidence before retrying; do not replay superseded PR52 or stale credential failures. Coordinator integrates reviewed commits and actual deployment receipts. Agents own isolated paths; one failing dependency must not invalidate independent completed work.

Use existing CI and the smallest check that resolves a concrete remaining risk. A genuine defect gets a narrow fix and one changed-cause retry; unrelated failures are recorded with their effect boundary. Do not invent acceptance tests, new dashboards, recurring probes, additional spending or new actors. No credential values or private Calendar text belong in this public document.

Source, service health, backup recovery, useful actor execution and writer handover are separate states. Report configured, deployed, executed, blocked and unknown accurately. Keep healthy hosted operation until equivalent native work is demonstrated. Full migration is complete only when each intended replacement has native evidence and its superseded hosted route is retired; this document does not assert that state.
