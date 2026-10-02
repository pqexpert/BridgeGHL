# Governed Launch17 task write

BridgeGHL remains the sole HighLevel writer. This surface admits one narrow task effect for a real, source-bound Launch17 event and does not admit contact creation, messaging, workflow enrollment, opportunity-field misuse, Calendar writes, or paid media.

## Identity and payload

The caller supplies an exact event key:

`launch17-next-action:v1:<locationId>:<formId>:<submissionId>:<doorId>`

`doorId` must be `01` through `17`. The bridge derives the task title and body from that key; callers cannot inject arbitrary copy. The due date must be timezone-aware ISO-8601. An assignee ID is optional.

Before a write, the bridge:

1. validates the API key, live-write switch, audit path and protected provider readback;
2. reads the contact and requires its native `locationId` to match `HIGHLEVEL_LOCATION_ID`;
3. lists that contact's tasks and matches only the exact derived title;
4. returns `NO_CHANGE_EXACT_REPLAY` for one verified match;
5. blocks multiple matches or a same-key state conflict.

A new task is posted only when no exact match exists. The bridge then reads the exact native task ID and verifies contact, title, due date, incomplete state and requested assignee before reporting `verified=true`.

## Routes

- `POST /dry-run/task/create`
- `POST /execute/task/create`
- `POST /dry-run/task/delete`
- `POST /execute/task/delete`

The MCP adapter exposes matching dry-run and execute tools. Dry-run is mandatory before execute in ordinary operation.

Cleanup is bounded to a task whose native task ID, contact ID and exact derived event-key title all match. Deletion is verified only when the provider readback returns not found.

## Acceptance boundary

Deployment proves only that the governed capability is available. It does not prove a visitor submission, a workflow enrollment, a qualified lead, a task created for any production contact, or completion of any of the 17 journeys. Native effect evidence must record the exact source event, contact, audit ID, task ID, provider readback and cleanup result when a synthetic task is used.

Private door 11 content must never be placed in the event key, task title, task body, audit log, Jira, or shared artifacts.
