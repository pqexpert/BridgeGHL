# Deployment Runbook

## Purpose
Activate BridgeGHL without widening authority or treating deployment as proof.

## Preconditions
1. Use the approved single HighLevel sub-account/location.
2. Store `HIGHLEVEL_PIT` and `BRIDGE_API_KEY` only in the protected server-side secret store/runtime.
3. Set `HIGHLEVEL_LOCATION_ID` server-side.
4. Populate exact `HIGHLEVEL_APPROVED_CUSTOM_FIELD_IDS` and/or `HIGHLEVEL_APPROVED_CONTACT_TAGS`; leave either blank to disable that mutation subclass.
5. Keep `LIVE_WRITE_ENABLED=false` during configuration.

## Deploy
1. Pull the intended reviewed commit.
2. Install dependencies and start/restart the service.
3. Confirm the audit directory exists and is writable by the service account.
4. Call `GET /health`.
5. Require `UNCONFIGURED` or `DEGRADED` rather than a false healthy state while live writes are disabled.

## Activation gate
1. Enable `LIVE_WRITE_ENABLED=true` only after protected secrets, location mapping, and allowlists are confirmed.
2. Call `GET /health` and require `state=HEALTHY`, including successful HighLevel readback.
3. Run `/dry-run/opportunity/update` for each enabled opportunity mutation class.
4. Run `/dry-run/contact/tags` for each enabled tag operation.
5. Select one non-critical existing record for a reversible canary.
6. Execute the smallest allowed mutation.
7. Require post-write readback `verified=true` and preserve the returned audit ID.
8. Reverse the canary using the same bounded path and verify the reversal by readback.

## Fail-closed conditions
Do not execute when any of these are true:
- missing caller authentication;
- missing PIT or location ID;
- audit path is not writable;
- HighLevel readback fails;
- bridge state is not `HEALTHY`;
- target ID is missing;
- custom field ID or tag is not explicitly allowlisted;
- requested action is outside the bounded mutation envelope.

## Recovery
- If post-write readback fails, mark the action **Needs Runtime Verification** and use the audit ID to reconcile manually before further writes to that target.
- Contact tag normalization automatically compensates newly-added tags if a subsequent remove operation fails; still verify final state.
- Do not broaden permissions to work around a failed canary.

## Closure evidence
Production automaticity is verified only when health, dry-run, canary, readback, audit, and rollback are all green in the deployed runtime. Repository code alone is not proof of production readiness.
