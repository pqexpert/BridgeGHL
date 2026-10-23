# Native Income research continuation

RBO-97:12130 authorizes bounded research on existing unfinished HighLevel Income
records. The Bridge read-only backlog endpoint discovers existing opportunity
identities from the verified import manifest, then verifies current native
source notes, intake opportunity state and journal proof. Historical export
content only locates identities. Current journal-backed native notes are fetched
once per internal contact per pass before eligibility and duplicate decisions. It never creates jobs, contacts, applications
or outreach. Ambiguous versions, existing artifacts, duplicate role identities,
unsupported sources, missing explicit Do Not Auto Submit, and non-internal
context-contact records are deferred.

The existing Dispatch hourly controller selects at most one unit per pass,
checks the exact live owner approval version, uses the existing Ashby/Lever
runtime and locked local model, and writes the verified Drive artifact back to
HighLevel through the existing source-keyed ingestion journal. The allowance is
24 durable invocation reservations per UTC day, including failures/queues.
The shared transport holds one nonblocking inference lock across all actor
lanes. Busy work is durably deferred; cached results require no inference.

The existing minute-11 cron remains the only Income scheduler. Pause:
create /var/lib/ziji-fabric/dispatch/continuation/pause as root. Existing local
operator pause also stops model/tunnel availability. Neither removes queues.
The selector performs no model calls when no current eligible work exists.

Deploy the Bridge route first, then refresh the pinned continuation bundle.
The refresh installs the selector and shared transport code only; it does not
reconfigure SSH, change model weights/memory, invoke inference, or delete queue
state. The existing provision workflow backlog-inspect mode prints local
manifest property names and rejection counts, plus bounded live authority and
current-source access diagnosis. It performs no model calls or provider writes.
Historical metadata is not proof of current job eligibility.

RSC daily work uses the same collector cadence through its separately pinned
worker and bounded RBO-34 source helper. A missing helper is an isolated RSC
coverage gap, not a failure of unrelated collection or Income execution.

This bounded research path does not prove whole Income/RSC mission replacement.
Resume tailoring, external correspondence, applications, and broader mission
selection require their own implemented and evidenced routes. No paid model
fallback exists.
