# RSC Command Center autonomy through HighLevel

Status: implementation specification under the existing Fabric Contract and RBO-97.

## Operating interpretation

RSC Command Center remains the RSC Actor. GPT-facing surfaces are bounded
plugins/interfaces: they contribute a declared capability but do not become
new Actors, CRM users, authority sources, schedulers, or owners of contact
truth. HighLevel is the native action plane for RSC relationships,
opportunities, exact internal tasks, submissions, and workflow events.

The smallest useful cycle is:

`native event -> plugin read -> eligibility/dedupe -> dry-run -> allowlisted effect -> native readback -> bridge audit -> Jira receipt`

Jira remains the accountable work/evidence ledger. GitHub remains source and
deployment provenance. Notion and Drive remain context/artifact sources. None
of them is a shadow CRM.

## Plugin contract

Every RSC plugin invocation must declare:

- actor/lane: `RSC Command Center`;
- exact source record and observed version/time;
- requested action class and native target IDs;
- authority/eligibility decision, including DND and commercial-language gate;
- stable idempotency/action key;
- allowed fields/tags/stage/task envelope;
- observable done condition and stop conditions;
- audit/readback destination;
- fact, inference, unknown, and human-only residue.

The bridge exposes only bounded capability. It does not invoke a GPT, create a
new identity, infer consent, qualify a lead, promise delivery, send messages,
or write Calendar.

## Current autonomous envelope

1. HighLevel native workflows may create the reviewed internal triage task for
   the exact RSC inquiry form.
2. BridgeGHL may update an existing opportunity, normalize allowlisted contact
   tags, and—only when `HIGHLEVEL_TASK_WRITES_ENABLED=true`—create or clean up
   one exact-key Launch17 internal task.
3. Every mutation requires a healthy bridge, pre-read, narrow validation,
   durable pre-write intent, post-write native readback, and audit finalization.
4. Ambiguous provider effects are `UNRESOLVED`; the caller must inspect native
   state before any retry.
5. Messaging, contact creation, arbitrary task copy, campaign/workflow launch,
   paid media, bulk mutation, and destructive synchronization remain outside
   the unattended envelope.

## Success criteria

Autonomous RSC execution is accepted only when one real, authorized RSC event
completes the full cycle without principal relay:

1. the source event and exact native contact are read;
2. the owning plugin applies eligibility, authority, and dedupe checks;
3. the permitted HighLevel action dry-runs and executes once;
4. an identical replay produces no duplicate effect;
5. native HighLevel readback proves the intended state;
6. the bridge audit and RBO-97 receipt reference the same action identity;
7. the next accountable action is visible in HighLevel;
8. any reserved decision is escalated, while ordinary green operation needs no
   Josh intervention.

Schema, deployment, connector discovery, or a synthetic canary alone is not
production acceptance. The isolated DND/no-recipient canary proves only the
mutation, replay, cleanup, and evidence machinery. Natural use proves the
system.

## Consumer-schema rule

The MCP adapter advertises contract version
`rsc-command-center-plugin/v1` and the exact task tool names through
`bridge_health`. Deployment also verifies the backend OpenAPI routes and the
loopback MCP `tools/list` result. If ChatGPT exposes an older cached tool list,
that is a connector-consumption gap: do not bypass it with Base44, a raw token,
or an ungoverned HTTP client. Refresh/reconnect the existing BridgeGHL plugin,
then re-read health and execute the isolated acceptance before relying on the
task methods.
