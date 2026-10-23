# Bounded autonomous HighLevel access

Owner scope: RBO-97 comment 12120 (2026-10-23). This extends operational access for the four existing actors, without replacing their whole missions or expanding Store customer-data or Calendar authority.

The existing hourly minute-11 native collector now supplies attributable, complete Jira evidence to two deterministic consumers. No extra scheduler or model polling is introduced.

1. The receipt projector selects the latest recognized genuine receipt for each actor, verifies publisher/editor and the pinned approval, then performs live approval/comment CAS before changed HighLevel effects. It uses existing internal Income/RSC context contacts with actor-specific source keys. No contacts, opportunities, outreach, Calendar notes or applications are created. Store and Distiller carry only enum status, timestamps, opaque receipt references and hashes. The JSON/prose receipt is never copied.
2. The narrow [request adapter](highlevel-request-queue.md) allows authenticated hosted publishers to request actual Income source reads or Income/RSC operational context writes despite the missing hosted MCP tool binding. Existing reader/writer routes, native identity/digest readback, effect journal and stable Jira receipts remain authoritative. At most one changed request per eligible lane per pass.
3. Existing Income continuation runs independently afterward. Its one-local-model-unit/day rule and completed queue identities remain unchanged.

## Request allowances and recovery

Every actual network attempt through app.highlevel_request reserves a durable counter before sending. Typed Income reads/ingestion are Income-scoped; root-validated Store/Distiller projection uses its own scope; legacy bridge operations and health default conservatively to RSC. Clients cannot choose a budget lane. Separate actors share the same durable ingestion.sqlite3 budget tables. Restarts do not reset limits.

Defaults: at most **1,000 provider requests per actor per UTC day**, **4,000 shared per UTC day**, and **20 scoped requests per ten seconds**. Environment limits may lower those ceilings. These are allowances, not targets, guaranteed free quotas, or evidence of optimal throughput. Owner-facing daily reports use ET while identifying the UTC allowance day. Unrelated consumers using the same provider credential are not counted here; no global provider-account cap is claimed.

Failures/timeouts consume their reservation. The old implicit GET retry is disabled for this counted transport. Local burst exhaustion can wait at most ten seconds before the first attempt; provider 429 or lower observed quota capacity persists a cooldown and does not resend. Remaining headers are recorded conservatively across concurrent responses. Zero reported daily capacity permits one bounded probe after 24 hours, without assuming provider reset timing. No paid fallback exists.

Changed notes use the existing pending/verified effect journal and exact native GET readback. Verified unchanged receipts use zero provider or extra Jira calls, explicitly labelled prior verified evidence rather than current native reads. Private projection state retains contact/note IDs for recovery; health exposes only sanitized delivery state and budget counts. Failed/stale collection cannot reuse a previous successful snapshot as this pass's proof. The pinned approval or source edit/revocation blocks only the affected effect.

## Deployment and stop

New code ships through the existing protected source-ingestion workflow. Reviewed non-secret policy config/highlevel-actor-policy.json installs as root0600; no new credential or owner copy/paste is required. Explicit actor-access deployment can invoke:

```text
project_actor_receipts.py --install-policy --install-collector
project_actor_receipts.py --execute
highlevel_request_queue.py --execute
```

The first command installs the exact existing continuation bundle in refresh mode (queue/counters preserved) and performs current read-only source collection. It does not wake Income inference. Preview is network-free against the captured evidence. Execution logs the accepted strict manifest before effects. The same existing cron thereafter processes changed evidence/requests automatically.

For an immediate operational stop, disable the private projection policy (enabled=false) or remove its installed file through the protected operator route. This stops both new consumers without disabling native source reads or unrelated Income work. Existing policy replacement is absent-or-identical by default, so a changed governing pin requires reviewed explicit replacement. Existing local inference pause remains separate.

Native proof must record source commit, deployment, each actor's real source receipt and HighLevel note readback, actual typed-request results and stable Jira receipts. A deployment, cached health item or mirrored receipt never proves all-four-actor mission autonomy. Native SaaS export and complete cloud retirement remain separate acceptance.
