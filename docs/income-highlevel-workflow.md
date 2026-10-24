# Income and HighLevel daily workflow

Owner direction: 2026-10-24. Income Accelerator owns career progress; HighLevel
holds current operational records. Prefer truthful remote and freelance/contract
opportunities compatible with the principal's actual availability. Do not infer
residence, work authorization, portable employment, flexible hours or employer
permission to work while travelling. Personal practice context stays private;
project only an admitted availability constraint when needed.

## One useful review surface

Use `scripts/income_workbench.py --limit 6` in the existing protected VPS runner
with its existing `/etc/bridgeghl/bridgeghl.env` EnvironmentFile. It makes no
provider mutation, model call, send, application or admission. Its default JSON
contains business context and native identifiers: redirect it to an existing
private operator artifact (0600), never public Actions logs or a GitHub commit.
The existing runner interpreter is
`/var/lib/bridgeghl/career-import/runner/bin/python`.

The workbench reads the existing identifier locator and journal-verified native
source notes. It orders explicit source labels: remote contract first, remote,
contract, then other/unknown. It verifies exact notes and native opportunity
identity/version for a bounded sample of at most twelve records. Non-open native
opportunities are excluded. Source labels may be old: employer posting and
candidate eligibility must still be revalidated. This is a review queue, not
qualification, an exhaustive live vacancy search, or an autonomous action grant.

Each entry exposes current native owner, next action and follow-up date, source
work mode/type/geography, gaps and exact opportunity ID. Native custom-field
values take precedence over the historical source snapshot. Unknown remains
unknown. Do not invent an owner, due date, qualification or fit score. Preserve
current native edits, stable source IDs, completed artifacts and cache aliases.
The workbench's sample cap and native-read timestamp remain visible.

Work in HighLevel's existing Career Pursuits pipeline. Open the relevant
opportunity and its contact's Notes/Conversations/Tasks; keep person identity
separate from each pursuit. The shared internal source-context contact is an
index, not an employer or recruiter. Never attach a recruiter's private identity
or communication to that shared contact. Existing unresolved source context
stays source context until the real person and destination are verified.

For one chosen pursuit, verify the employer posting, engagement terms, required
hours/time zone, geography/authorization and candidate evidence. Capture unknowns
as the next research action. Reuse existing artifacts. Assign a real next action
and useful due date only through an admitted native write; current task authority
is Launch17-specific, so career-task creation is not silently admitted by this
workbench. Preparing, submitted, interviewing and won require their own evidence.
Apply the existing Income correspondence envelope and Do Not Auto Submit.

## Record interactions where they happen

For each admitted human or system interaction, retain one stable event identity,
owning lane, source/version, UTC observation, direction/channel, execution
identity, native contact/opportunity/provider IDs when verified, factual outcome,
evidence pointer, next action and actual deadline or unknown. Separate proposed,
drafted, attempted, provider-accepted, delivered, replied, failed and unknown.
Delivered does not prove inbox receipt; a generated draft is not a send.

Keep native messages in Conversations. Put a minimal factual summary/evidence
link for an authorized external interaction in the verified person's internal
Notes; use opportunity context to identify the pursuit. Retain the actual
provider message/event ID as the deduplication identity, not a fresh timestamp on
every retry. Record a failed or uncertain outcome before recovery. A missing CRM
receipt remains pending; never resend to repair a logging failure. Reconcile the
provider first and finish only the missing writeback through the existing writer.
If contact identity or admitted write scope is unavailable, preserve a bounded
pending receipt in the owning private work record; do not invent a contact.

Bridge provider requests now append durable metadata-only `provider_interaction`
intent and response/transport-unknown events to the existing audit log, correlated
by request ID and attempt. They include actor, method, resource class, target-path
digest, timestamp, status and duration. URLs, queries, headers, message contents,
response bodies, recipient addresses and exception text are excluded. Failure to
persist intent stops the request. Intent without an outcome is unresolved. These
events prove transport, not CRM mutation readback or business completion; existing
effect journals and native readback remain required. Budget-rejected calls make
no provider request and remain governed by existing budget/route receipts.

The bridge cannot observe arbitrary personal email, LinkedIn, phone calls or
offline conversations. Gmail two-way sync and hosted AgentMail binding are not
assumed from connector presence. Before admitting a send, prove sender/recipient,
DND/STOP/suppression, deduplication and its native sent/CRM readback path. Do not
turn on broad mailbox sync, record calls, scrape contact data or expand outbound
authority to satisfy a universal logging claim. No Store data, sacred/private
context, applicant secrets, credentials or unrelated personal conversations enter
this CRM. Raw conversation duplication is unnecessary; retain minimal evidence
in its appropriate protected native home.

## Provider guidance and forum findings

Checked 2026-10-24:

- [HighLevel Notes](https://help.gohighlevel.com/support/solutions/articles/155000004555-adding-and-managing-notes)
  supports internal context on contacts, opportunities and companies.
- [Pipelines and opportunities](https://help.gohighlevel.com/support/solutions/articles/155000005062)
  distinguishes people from pursuits and recommends stages matching the actual
  process. This supports separate opportunities without duplicating contacts.
- [Conversations notes and tasks](https://ideas.gohighlevel.com/changelog/notes-and-task-management-in-conversations-module)
  describes the integrated working view; actual account availability must be
  checked rather than inferred from a changelog.
- [Community discussion](https://www.reddit.com/r/gohighlevel/comments/1k4duv9/is_it_possible_to_put_an_opportunitycontact_into/)
  reports the same contact/multiple-opportunity distinction. Forum advice is
  operational context, not API authority or permission to enable duplication.
- [Notes API](https://marketplace.gohighlevel.com/docs/ghl/contacts/get-all-notes/index.html)
  is the primary contract for the existing read path. Reuse the existing source
  journal and writer; no new webhook receiver, CRM, scheduler or Wazuh collector.

Measure real replies, verified suitable opportunities, completed deliverables,
confirmed submissions and income separately from scans, drafts and infrastructure
health. Protect availability and recovery time rather than manufacture urgency.
