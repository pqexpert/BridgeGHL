# BridgeGHL

**A governed write bridge for CRM automation: dry-run first, execute deliberately, leave an audit trail.**

BridgeGHL is a small FastAPI service for one specific problem: how do you let an AI-assisted or automated workflow update HighLevel **without handing the client direct CRM credentials or turning every suggestion into an immediate mutation?**

The answer here is deliberately boring in the best way: keep the credential server-side, expose a narrow allowlisted write surface, preview the mutation, execute only through the controlled endpoint, and record what happened.

> **Maturity:** reference implementation / active development. This public repository does not claim that a production HighLevel tenant, public endpoint, or customer deployment is currently live.

## Why this exists

Automation becomes dangerous when convenience erases the difference between *reasoning about a change* and *making the change*.

BridgeGHL separates those steps:

1. **Dry run** — validate the request and show the outbound mutation without changing CRM state.
2. **Execute** — perform the bounded mutation through the server-side credential boundary.
3. **Audit** — write a machine-readable record of the action.
4. **Read back** — the operating workflow should verify the resulting CRM state before calling the work complete.

That pattern is useful well beyond one CRM: it is a small example of how I prefer to build automation around consequential systems — explicit authority, least privilege, reversibility where possible, and evidence after execution.

## Current API surface

### Health

```http
GET /health
```

### Dry run

```http
POST /dry-run/contact/upsert
```

Validates the payload and returns the HighLevel request that would be sent. **No CRM mutation occurs.**

### Execute

```http
POST /execute/contact/upsert
```

Performs the allowed mutation and writes an audit record.

### Deprecated

```http
POST /contacts/upsert
```

Retained only for legacy compatibility; new integrations should use the dry-run / execute flow.

## Security model

- Write endpoints require an application API key.
- The HighLevel private integration token stays server-side.
- Clients do not receive direct CRM credentials.
- Execute actions are audit logged.
- The bridge is intentionally narrow rather than a generic proxy to the HighLevel API.
- Secrets belong in the deployment environment or approved secret store, never in Git history.

## Audit logging

Execute actions are written as JSON Lines so they can be inspected or shipped to another log system:

```text
/var/log/bridgeghl/audit.log
```

The operating expectation is **mutation + readback**, not “HTTP 200 therefore done.”

## Local / server operation

Environment configuration is documented in `.env.example`.

Typical service refresh:

```bash
git pull
sudo systemctl restart bridgeghl
sudo systemctl status bridgeghl --no-pager
curl http://127.0.0.1:8000/health
```

Exact deployment details are intentionally environment-specific and are not evidence that a public production instance exists.

## Design principles

- **Purpose before automation.** A tool may execute a decision; it does not decide why the work matters.
- **Minimum sufficient authority.** Expose only the mutations the workflow actually needs.
- **Preview consequential changes.** Dry-run should make the proposed state transition inspectable.
- **Evidence before completion.** Verify the resulting system state after an execute call.
- **No secret sprawl.** Credentials stay outside prompts, tickets, public repositories, and client-side code.
- **Fail closed.** Unknown fields, unsupported actions, or missing authorization should stop the write rather than broaden capability silently.

## Where this fits

BridgeGHL is one supporting project in the broader [PQExpert.io](https://pqexpert.io/) technical portfolio. The interesting part is not “AI writes to a CRM.” The interesting part is the control boundary between reasoning, authorization, mutation, and proof.

For public portfolio context, see [PQExpert.io](https://pqexpert.io/). For vulnerabilities or sensitive security findings, use the repository’s security guidance rather than opening an issue with secrets.
