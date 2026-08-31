# BridgeGHL

**A governed write bridge for CRM automation: dry-run first, execute only inside an explicit allowlist, and verify every mutation by readback.**

BridgeGHL is the server-side HighLevel write boundary for bounded automation. It keeps the HighLevel credential out of clients and prompts, rejects unsupported actions, preserves an audit trail, and treats readback—not an HTTP success code—as the completion signal.

> **Maturity:** governed reference implementation / active runtime hardening. This public repository does not claim that a production tenant or public endpoint is currently healthy unless runtime evidence says so.

## Current unattended mutation envelope

Allowed only when the bridge reports `HEALTHY`:

- update an **existing opportunity** via `PUT /opportunities/:id`;
- set opportunity pipeline stage;
- set opportunity status;
- assign opportunity owner;
- set only custom-field IDs explicitly listed in `HIGHLEVEL_APPROVED_CUSTOM_FIELD_IDS`;
- normalize only contact tags explicitly listed in `HIGHLEVEL_APPROVED_CONTACT_TAGS`.

Not allowed for unattended execution:

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
