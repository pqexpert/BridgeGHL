# HighLevel ecosystem implementation

Version: 2026-09-19.1. Account: Josh Allen / Restoration Security Consultants.
This is a bounded implementation plan under the existing estate pointer, not a new governance contract.
Controlling evidence: RBO-97. Principal direction: 2026-09-19 ecosystem setup and test authorization.
Skills consumed: Wise Action Governor 2026-09-19.3; HighLevel Write Bridge; SOP-006.

## Responsibilities and flow

Notion retains journeys, decisions, source facts and history. Google Drive holds produced artifacts and versioned intake bundles. HighLevel holds actionable relationships, opportunities, follow-up tasks and links to evidence. Native CRM changes remain authoritative for current CRM stages, assignments, DND and conversations. Imports must not overwrite those with stale source snapshots.

Use the existing BridgeGHL protected credential, writer, audit and deployment path. No additional vendor, CRM writer, model-based router or production Actor is introduced. Domain labels are segmentation, not access controls. Preserve the existing prohibition on Dzokden purchaser/order/fulfillment PII in the RSC CRM. RD laboratory data requires an admitted business purpose; it is not a production dependency.

## Agency and subaccount

Josh remains agency owner. His explicit RSC subaccount membership was missing and is now saved and read back; My Profile is now available in RSC. Add future real operators with Account user type and only their needed subaccount/modules; apply Only Assigned Data where appropriate. Do not invent staff identities for ephemeral agents. Agents use the governed bridge capability. Preserve Enhanced Security and existing credentials.

Use one canonical RSC ingestion location. Create a reusable agency snapshot only after configuration passes native acceptance; snapshots are templates, not backups of contacts, conversations or all runtime bindings. Keep credentials and record exports in their protected existing homes.

## Native schema

The reviewed manifest in `scripts/configure_ecosystem.py` provisions namespaced contact and opportunity fields without altering existing fields. Text fields retain explicit unknown values and source terminology; producers validate canonical values before projection.

Common fields: mission domain, stable source ID, source URL, Drive asset URL, evidence status, data quality, next action, follow-up date, source observation timestamp, and record mode.

Contact fields: relationship type and stage, organization role, outreach permission, last meaningful interaction. Native owner, email, phone, company, DND and tasks remain native properties.

Opportunity fields: original source status, priority, offer, environment stage, technical readiness, commercial language permission, next proof needed, blocker/risk, buyer sector/role, source attribution, owning mission. Native pipeline/stage/status/value/owner remain native properties. Company targeting is not proof of a real human contact or qualified deal.

## Pipelines and statuses

Career Pursuits keeps Source intake -> Preparing -> Applied -> Interviewing -> Offer. Native won/lost/abandoned statuses are separate from stage. Historical source status is preserved separately until current evidence permits a transition.

RSC Commercial: Source intake -> Qualified -> Discovery -> Solution and proof -> Proposal -> Decision. Qualification requires an evidenced need and accountable next action; proposal requires approved scope and substantiated claims. Won requires actual acceptance, not optimism.

Relationship Stewardship: Source intake -> Identity verified -> Context prepared -> Engaged -> Next action agreed -> Maintaining. This supports admitted RSC, Income/Syracuse and internal stakeholder contexts through domain/context fields, not duplicated identities.

TEST ONLY validation pipeline is excluded from funnel/pie reporting by default. Canary contacts have no recipient address/phone and DND enabled. Test tasks are completed so they do not become real follow-up debt.

## Producer intake specification

Every produced artifact must identify its Drive file ID, version/checksum, source URL/record ID, producer/owning mission, observed time, domain, record kind, evidence status, intended audience/classification, and record mode. Use exact source identity; never guess email addresses. Link private evidence rather than copy credentials or restricted content into CRM.

Missing optional identity enrichment, role, priority, next action or dates does not block source preservation. Mark `needs-enrichment`, preserve unknowns, and queue an enrichment task only when it has an owner and useful due date. Domain/destination authorization, stable source identity, integrity, and data-boundary violations do block the affected record. Unknown marketing permission never becomes consent.

Map source relationship and job fields before import: Email/Contact Email; Name/Contact Name; Organization/Company; Relationship Type/Stage; Status -> original source status; Next Action; Next Touchpoint/Follow-Up Date; Source/Verification Source; Drive Folder Link/Asset Link. Do not put individual relationship fields on the shared source-context contact: records without a verified person identity remain linked source context until resolved.

## Communication and refresh

Gmail two-way sync is per subaccount user and is currently unconnected. Existing Calendar connections do not establish Gmail sync. HighLevel excludes conversations with other subaccount users from Gmail sync; use explicit internal tasks and source links for those. Bulk/workflow emails use the location email provider, not this Gmail connection. No live outbound campaign is activated by this change.

Drive-native unattended refresh still requires a durable protected Google authorization. The completed career import used a checksum-pinned private staged copy of the Drive export; this proves ingestion, not continuous Drive polling. Do not claim all producers are configured until their owning instructions and actual delivered Drive artifacts have been verified.

Refresh uses source checksum + mapping version + destination identity. Preserve current native edits; changed source material becomes reviewed enrichment. Incremental receipts and native readback prevent repeated full imports. Missing bindings affect only their lane. Circuit-break after two unproductive recovery cycles; retry only when evidence or conditions changed.

## Acceptance and measurement

Already verified: 404 career source records, 176 native career opportunities; bridge protected deployment and audited resume; correct connector location; agency-owner RSC membership.

This manifest must prove: native field types, exact pipeline stages, tags, one isolated canary contact, two distinct opportunities sharing that contact, one completed native task, and resumability without duplicate creation. Persist receipts privately on the existing VPS and reference deployment evidence in RBO-97.

Remaining end-to-end acceptance: structured source-field projection, useful current follow-ups, Google OAuth/readback, registered producers delivering to Drive, automatic changed-artifact ingestion, source/native conflict handling, and agency snapshot readback. These are separate states; schema creation alone is not full ecosystem implementation.

Measure duplicate count, rejected/ambiguous records, missing owner/action/date, overdue actionable tasks, source freshness, successful readbacks and opportunities advanced by evidence. Separate tests and source-context records from real conversion metrics. No fabricated revenue, qualification, staffing capacity, national-security accreditation or dollar-cost telemetry.

## Primary provider references

- https://help.gohighlevel.com/support/solutions/articles/155000002543-manage-agency-user-roles-and-permissions-in-highlevel
- https://help.gohighlevel.com/support/solutions/articles/155000002544-sub-account-user-roles-permissions-and-assigned-data
- https://help.gohighlevel.com/support/solutions/articles/48001235216-connect-google-to-use-gmail-for-email
- https://marketplace.gohighlevel.com/docs/ghl/locations/create-custom-field/index.html
- https://marketplace.gohighlevel.com/docs/ghl/contacts/create-task/index.html
- https://marketplace.gohighlevel.com/docs/ghl/workflows/get-workflow/index.html
