# Existing Jira to HighLevel request transport

This adapter closes a missing hosted MCP binding for narrow operations through the existing root-only VPS bridge. It is not a new actor, scheduler, CRM proxy, admission grant or proof of whole-actor autonomy.

The existing collector supplies fresh verified full RBO-97 snapshots. The root-only runner uses the same private projection policy, authority comment 12120/version, issuer-to-lane mapping, existing bridge credentials, provider budgets and ingestion database. Invoke `scripts/highlevel_request_queue.py` for a no-provider preview or `--execute` for admitted work. One changed accepted request per Income/RSC lane per pass; no model call. Source configuration and deployment remain separate from actual request execution.

## Request

One exact marker paragraph `FABRIC_HIGHLEVEL_REQUEST`, followed by a literal JSON code block. Use ADF codeBlock/plain text so URL auto-linking cannot split JSON strings during native text extraction; do not submit a rich-text paragraph containing inline URL cards. The extracted body must contain only marker + newline + JSON object:

```json
{
  "schema_version": 1,
  "request_id": "income-read-operating-checkpoint-2026-10-23",
  "lane": "income",
  "operation": "income.read_source",
  "authority_ref": "RBO-97:12120",
  "observed_at": "<actual ISO timestamp with timezone>",
  "expires_at": "<actual later timestamp, at most 36 hours>",
  "payload": {
    "source_id": "fabric-highlevel-operating-checkpoint-2026-10-23",
    "current_source_sha256": "836b7e2cacf4eeee580963d0ef020b3c262bc07d292b88b14baf2d4aad812560"
  }
}
```

This is a schema example, not an executed request. Omitting contact_id resolves the existing Income context contact only on the protected server. Optional exact contact_id/opportunity_id are still validated by the existing native reader. The SHA is mandatory; no guessed latest version. Returned evidence includes verified source IDs/digest, native IDs, bounded title/source URL, allowlisted exact admission facts and verified legacy aliases, and readback. Admission flags are source facts, not authorization. It excludes arbitrary content/properties and does not grant execution admission.

Operations `income.write_context` and `rsc.write_context` accept payload **only**:

```json
{
  "source_id": "fabric-highlevel-automatic-access-plan-2026-10-23",
  "source_url": "https://restoration-mt.atlassian.net/browse/RBO-97?focusedCommentId=12120",
  "title": "Approved bounded HighLevel operating plan",
  "content": "<approved public operational text, at most 4000 characters>"
}
```

They always use kind=context, empty properties and the existing configured owning context contact. No contacts/opportunities are created. No email/name/company fields, Store/customer/order data, private Calendar feedback, Income admission projection, credentials, arbitrary endpoints or messaging. Source IDs must start fabric-. Store and Distiller cannot submit commands through this adapter.

## Execution and recovery

The runner checks allowlisted author and editor, current authority version and expiry, then exact-fetches authority and selected request before any effect. Modified payload under a prior request ID or duplicate request ID is CONFLICT. A write first logs the actual accepted dry-run operation plan, then invokes the existing execute route and its source-note journal/readback. Request/hash/state/result persist in the existing ingestion.sqlite3 highlevel_requests table; no second database.

A verified bridge result can finish publishing its stable `highlevel:<request_id>` Jira receipt without another provider mutation. Uncertain attempts back off six hours and permit at most three attempts under fresh authority/request CAS. Reads can retry; writes re-enter the same existing ingestion journal and exact payload: native lookup precedes creation, and unresolved prior creation refuses another POST. Exhausted attempts remain UNKNOWN without blocking later independent same-lane requests. Existing note journal preserves ambiguous mutation evidence. Actual read/write result and receipt delivery are distinct. Read budget exhaustion or unknown errors do not mean mission success.

The existing health surface may expose highlevel-request-status.json, containing only request ID/lane/operation/hash, source request comment ID, status, receipt pointer and native-readback timestamp. It excludes native record bodies and contact IDs. A hosted actor consumes that pointer on its next normal run and reads the exact private Jira receipt when needed. Cached delivery evidence is not a fresh business-source read. Existing hourly collection introduces asynchronous latency; no polling loop or forced actor run is needed.

Success today means the real existing checkpoint was natively read at its pinned digest, and the owner-approved operating-plan context was dry-run, written or reconciled and natively read back, with exact Jira receipt IDs. It does not establish new job admission, outreach, application submission or complete four-actor mission automation.

## Verified hosted handoff and operational target repair

All four existing hosted prompts were updated and read back exactly at 2026-10-23 13:26 UTC. Their schedules, enabled states, titles, timezones and timing remained unchanged. On the next ordinary run they read cached own-lane projection/request evidence through bridge_health; Income/RSC use the bounded request queue when source tools are unavailable, while Store/Distiller retain status-only access. Natural-run consumption remains pending; prompt publication is not execution proof.

Native preview39961374746 proved the RSC default context binding absent while the existing internal Income context was available. Owner-authorized root operational processes now reuse that verified internal context only when RSC has no configured default. RSC source domain/keys and request budget stay RSC. The service environment and customer routes are unchanged; no contact is created. The existing writer checks native contact identity/location before each effect. An explicitly configured RSC target always takes precedence.

RSC request12124 was reformatted in place as marker + literal JSON codeBlock after native preview showed it absent; its exact payload, request ID and timestamps were preserved. The inline-link presentation had broken literal JSON extraction. No duplicate request was published. Approval12120 is unchanged.
