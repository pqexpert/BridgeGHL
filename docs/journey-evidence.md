# Contact journey evidence reader

Owning work: RBO-97. This repair adds one read-only capability to the existing
BridgeGHL service and MCP adapter; no new runtime, key, store or scheduler.

`read_journey_evidence(contact_id, resource, conversation_id?, limit=20,
cursor?, page=1, start_date?, end_date?, email_id?)` calls authenticated
`POST /read/journey-evidence`. POST carries the query; every provider action is GET.

Resources: conversations, messages, tasks, submissions, email. Each request first reads
and validates the exact contact and configured location. Messages and singular email reads additionally
validate the conversation parent. Email requires an individual email_id; its
returned ID, contact, location and conversation must all match. All returned children must match that contact;
conversations and messages must match the location. Wrong resource envelopes,
cross-contact/location data, failed scope/auth and missing pagination fail closed.
No contact list is substituted for conversation evidence. Calls do not retry an
unchanged authentication failure.

Only allowlisted scalar IDs, times, assignment, completion, message direction/type
and native status leave the bridge. No message body/subject, form answers, task
body/title, addresses, attachments or arbitrary metadata. Provider errors are not
forwarded. Existing server-side PIT and bridge auth remain in their existing homes.

Each invocation reads one bounded page (1–50 records); submissions require a window
of at most 31 days. end_date is exclusive; default is tomorrow UTC, with
start_date 30 days earlier, so today’s submissions are included. The result
explicitly marks end_date_exclusive and gives the queried window. Continue using the returned cursor/page
only when needed. `complete` never certifies a full collection when starting midway.
Task API lacks pagination; a truncated task response is explicitly incomplete.
A page cap, missing cursor, error or unobserved window is not zero events.

A native delivered status does not prove inbox receipt. This capability does not
expose workflow enrollment/execution logs or accept complete customer journeys.
Those remain independent native HighLevel evidence. Message-list records can be
email threads rather than individual emails. Only the documented
meta.email.email.messageIds and the compact meta.email.messageIds arrays are projected as email_message_ids (up to50
validated unique IDs across both shapes); no arbitrary metadata is returned. Use these IDs with the singular
email resource to read native status, direction, threadId and replyToMessageId.
Absent status on a thread is not a delivery failure or proof of no reply. No CRM write or message send
is enabled by this change. RSC/Income admitted work only; no Dzokden records.

## Deployment and proof
Existing deploy-ingestion ships the module with app.py, validates the route and
retains rollback. Existing deploy-mcp deploys the adapter and validates discovery.
Both use the existing protected GitHub environment and IONOS services. Refresh the
existing BridgeGHL IONOS app schema if its cached catalog still has five tools;
then invoke the reader for an already admitted inquiry. Preserve exact provider
errors if the existing PIT lacks a resource scope; do not broaden privileges or
copy secrets as a shortcut. Production deployment and native reads are separate
from passing source tests.

## Supported provider contracts (checked 2026-09-25)
- https://marketplace.gohighlevel.com/docs/ghl/conversations/search-conversation/
- https://marketplace.gohighlevel.com/docs/ghl/conversations/get-conversation/
- https://marketplace.gohighlevel.com/docs/ghl/conversations/get-messages/
- https://marketplace.gohighlevel.com/docs/ghl/conversations/get-email-by-id/
- https://marketplace.gohighlevel.com/docs/ghl/contacts/get-all-tasks/
- https://marketplace.gohighlevel.com/docs/ghl/forms/get-forms-submissions/

All use API Version v3. Existing identity must admit contacts.readonly,
conversations.readonly, conversations/message.readonly and forms.readonly as
applicable; capability documentation is not evidence of credential scope.

Instruction attestation: WAG 2026-09-24.1 (entrypoint blob
ac3458c0f194ff2f87b177d2ece48f84aa553b76), Owner Sovereignty, Collaborative
Cognition, principal guide, constitutional source registry, sole Fabric Contract,
Semantic Glossary, Control-Point Registry, Operations Guide/SOP-006, deployment
recovery and HighLevel operating-asset references read for this repair.
Authority: owner-authorized September 25 journey-evidence repair; bounded existing
service deployment. Negative boundaries: no customer writes/sends, new secret,
new vendor/runtime/scheduler, Calendar write or Dzokden data. Material unknowns:
production scope acceptance and worker app refresh until native proof.
