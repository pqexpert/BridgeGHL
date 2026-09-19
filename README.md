# BridgeGHL

**A governed write bridge for CRM automation: dry-run first, execute only inside an explicit allowlist, and verify every mutation by readback.**

BridgeGHL is the server-side HighLevel write boundary for bounded automation. It keeps the HighLevel credential out of clients and prompts, rejects unsupported actions, preserves an audit trail, and treats readback—not an HTTP success code—as the completion signal.

> **Maturity:** governed reference implementation / active runtime hardening. This public repository does not claim that a production tenant or public endpoint is currently healthy unless runtime evidence says so.

## Baseline unattended mutation envelope

Allowed only when the bridge reports `HEALTHY`:

- update an **existing opportunity** via `PUT /opportunities/:id`;
- set opportunity pipeline stage;
- set opportunity status;
- assign opportunity owner;
- set only custom-field IDs explicitly listed in `HIGHLEVEL_APPROVED_CUSTOM_FIELD_IDS`;
- normalize only contact tags explicitly listed in `HIGHLEVEL_APPROVED_CONTACT_TAGS`.

Not allowed through the baseline endpoints (the separately enabled source-import surface below has its own bounded contact/opportunity creation contract):

- contact create/upsert;
- opportunity create/delete;
- appointments;
- messaging;
- workflow activation;
- bulk mutation;
- destructive synchronization;
- unallowlisted custom fields or tags.

The legacy contact-upsert routes intentionally return `410 Gone`.

## API surface

### Health

```http
GET /health
```

Health is truthful and fail-closed. `HEALTHY` requires protected runtime configuration, a writable audit path, live-write enablement, and a successful HighLevel readback check. Otherwise the service reports `DEGRADED`, `UNCONFIGURED`, or `ERROR`.

### Opportunity update

```http
POST /dry-run/opportunity/update
POST /execute/opportunity/update
```

Dry-run validates the exact target and allowlist without mutation. Execute performs pre-write readback, the bounded `PUT /opportunities/:id`, post-write readback, verification, and audit logging.

### Contact tag normalization

```http
POST /dry-run/contact/tags
POST /execute/contact/tags
```

Execute reads current tags first, mutates only effective allowlisted differences, compensates newly-added tags if a later remove step fails, then reads back and verifies the resulting state.

## Required environment

See `.env.example`.

Secrets and account identifiers belong only in the approved server-side runtime/secret store. Do not commit real values.

The two mutation allowlists default to empty. An empty allowlist disables that mutation subclass rather than widening authority.

## Audit and proof

Execute actions append JSON Lines to `AUDIT_LOG_PATH` containing the action, target ID, reason, before/requested/after state, HTTP statuses, verification state, and audit ID. Secret values are never written to the audit record.

The operating rule is:

**pre-read → bounded mutation → post-read → verify → audit → resolve or escalate**.

A mutation that cannot be read back is **Needs Runtime Verification**, not complete.

## Deployment posture

Use one sub-account, one protected PIT, one bridge caller credential, and one narrow allowlist. Keep `LIVE_WRITE_ENABLED=false` until secret binding, location mapping, audit-path writability, and readback are verified in the deployed runtime.

See `docs/deployment-runbook.md` for the activation sequence.

## Design principles

- Purpose before automation.
- Minimum sufficient authority.
- No implicit authority expansion.
- Reversible, attributable writes first.
- Evidence before completion.
- No secret sprawl.
- Fail closed.

## Where this fits

BridgeGHL is a supporting project in the broader [PQExpert.io](https://pqexpert.io/) technical portfolio. The core design problem is not “AI writes to a CRM”; it is maintaining a trustworthy boundary between reasoning, delegated authority, mutation, rollback, and proof.

## Source ingestion (separate opt-in)

The authenticated `/dry-run/ingest/source-record` and
`/execute/ingest/source-record` routes support the owner's authorized Income/RSC
source imports. Legacy unrestricted contact-upsert routes remain disabled.
This import surface is disabled unless `HIGHLEVEL_INGEST_ENABLED=true`, and
also requires the existing healthy/live-write/audit controls.

Records retain their source IDs, URLs, all supplied properties, statuses,
next actions and document links in versioned contact notes. Native contacts
are found by exact supplied email or created with DND enabled; existing contact
fields and DND are untouched. Missing-email records use an explicitly configured
existing context contact, never a fabricated recruiter. Opportunities are created
in the configured intake stage and keyed by source ID; subsequent imports preserve
native pipeline progress. Original source statuses remain in notes and are not
misrepresented as HighLevel pipeline mappings. Before production imports, verify
that existing native workflows do not send on imported contacts/opportunities.
No messaging or workflow activation endpoints are called by this importer.

Configure each admitted domain with `HIGHLEVEL_INGEST_INCOME_PIPELINE_ID`,
`HIGHLEVEL_INGEST_INCOME_STAGE_ID`, `HIGHLEVEL_INGEST_INCOME_DEFAULT_CONTACT_ID`
(or equivalent `RSC` names). Configure values only in the protected runtime.
The default contact holds the principal's career context and uncontacted leads;
it is not represented as an employer's recruiter.

`scripts/ingest_drive.py` reads a SHA-256-pinned `career-drive-export/1` JSON
from private Google Drive using protected `GOOGLE_DRIVE_ACCESS_TOKEN`, or from
an already authenticated local transfer with `--file`. It dry-runs by default;
`--execute` enables the reviewed import. It uses the existing `BRIDGE_API_KEY`,
stops at the first blocked record, and saves a private incremental receipt.
No LLM, enrichment service, new scheduler, or paid platform is required.

The local journal stores only hashed operation identities, states and native
IDs. It commits intent before mutations; uncertain effects require native
reconciliation before another create. Successful retries read back native
state and do not create duplicates. Source notes are append-only by version.
Rollback is an explicit review of the receipt's created IDs, not automatic
record deletion.

`Deploy source ingestion` deploys the exact tested commit to the existing VPS,
checks both routes, and rolls back code if startup verification fails. It
preserves protected configuration and reports only readiness booleans and
provider status. Missing source credentials/mappings remain blockers, not
claims of completed ingestion. Existing SSH host discovery is reused; it does
not establish an independently pinned host identity.
