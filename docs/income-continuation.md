# Admitted Income continuation

Bridge transports the reviewed Dispatch controller and Income runtime; it does not become an actor or grant mission authority. Exact source revisions and SHA256 values are in `scripts/income_continuation_bundle.json`. The private queue is admitted through the existing protected `zijifabric` workflow environment.

Workflow modes:
- `continuation`: validate private fresh admissions, install the pinned sources and merge queued identities; execute zero jobs.
- `continuation-status`: local filesystem status only, zero provider/model calls. Public output includes installed hashes, queue/completion counts, daily reservation, sanitized outcome and native Jira receipt ID; no private artifact URL or proposal.
- `continuation-wake`: explicitly launch one controller pass with a 240-second deadline. This proves a useful launch only if actual native artifact and receipt readback complete. It is not evidence that cron fired.

The existing minute-11 hourly source collector invokes continuation after collection, even if an unrelated source read failed. The collector's own evidence remains read-only and separate. No new scheduler is added. The initial launch and future cron share the same durable queue, nonblocking controller/Income locks, six-hour backoff and maximum one local-model invocation per UTC day. The reservation is persisted before inference, including failure or timeout. Cached Drive/Jira reconciliation uses the existing reserved artifact ID and exact receipt marker without regenerating the draft. A changed source/request identity does not automatically generate a replacement.

Root-private state is `/var/lib/ziji-fabric/dispatch/continuation`. `latest.json` holds worker evidence; `caller-latest.json` independently preserves timeout/exit status even when cron discards output. `state.json` preserves completed identities, backoff and daily reservations. Never clear it to retry or reset a budget.

Pause: `touch /var/lib/ziji-fabric/dispatch/continuation/pause`. Resume: remove only that marker. An in-flight request may finish; subsequent passes defer. Duck's separate local pause stops the inference tunnel; the controller's cheap authenticated health preflight then defers before employer/provider calls.

Only fresh, explicitly admitted canonical projections execute, for at most 36 hours after observation. Root may remove pending entries from `/etc/ziji-fabric/dispatch/income-queue.json` to revoke them. The queue does not harvest Notion or auto-admit source changes. Exhausted or stale queues wait truthfully for a canonical bridge refresh. Canonical artifact writeback remains a bridge responsibility. Hosted mission schedules retain uncovered scope.

The useful initial candidate is the existing unfinished PingWind support research packet. It remains unqualified, not packet-ready and Do Not Auto Submit; this path makes no applications, external messages, Calendar writes, paid-model fallbacks or candidate eligibility claims. Actual result belongs in the deployment/native receipt, not this source document.

Existing encrypted VPS recovery includes the queue and ledgers under /etc and /var/lib, plus installed code under /opt. Local pause drop-ins can be recreated by the reviewed Git-backed control script; the local capsule is not a full-machine image. VS Code tasks expose existing status plus local pause/resume.

## Source refresh and truthful status

`continuation-refresh` updates the pinned worker/runtime/collector and the existing cron description under both worker locks. It reads the existing private queue without rewriting it, does not refresh any admission or expiry, and runs no job. Expired or completed entries can remain intact during source maintenance. Unexpected collector content or cron edits fail before source replacement.

Status distinguishes a historical `configured` installation record from `installed`, which requires every expected deployed source hash and safe permissions. `unfinished_units` joins current queue identities to the ledger; historical completed receipts remain available separately. The actual cron description is included.

A requested wake accepts current evidence only when the worker exits successfully and its own JSON output hash equals a `latest.json` observed between that invocation's start and finish. Timeout, nonzero exit, missing output (including lock contention), or stale evidence cannot reuse an old top-level Jira success. Completed historical receipts remain visible for reconciliation. No extra inference is performed to validate status.

The collector projects only dedicated `FABRIC_INCOME_ADMISSION` comments into a separate private candidate list with author/update-author identity and source timestamps. Malformed or revoked records are retained for rejection/reconciliation by the governed controller; incidental prose is not an admission. This collection does not grant authority.
