# HighLevel Income source read contract

HighLevel is the operational destination. Original source IDs and historical URLs remain provenance and duplicate-detection aliases; this reader never makes Notion the current operational destination.

Use the authenticated `read_income_source` MCP tool (backend `POST /read/ingest/income-source`) with `source_id`, `contact_id`, optional `opportunity_id`, and optional `current_source_sha256`. All provider operations are GETs. It reuses the existing bridge location/Income pipeline/default-contact configuration and reads the existing ingestion SQLite journal in read-only mode.

The contact must belong to the configured location. An opportunity must belong to the configured Income pipeline and exact contact and retain its existing `[src:<income-source-key>]` marker. A context-only read is restricted to the configured Income source-context contact. Native notes must match the source key, have verified journal entries matching their exact native IDs/body hashes, contain all parts, and reproduce the complete content digest. Each selected note is retrieved again natively; opportunity identity/version is compared after reading.

A single complete journal-verified version may be read without an explicit digest. Multiple verified historical versions require the caller's reviewed exact content digest. The journal has no canonical chronological-version semantics, so the reader never guesses that newest means authoritative. Readback proves the selected version exists; it does not independently make stale historical facts current.

Existing `execute_source_record` remains the sole source-note writer. A real operating checkpoint can be a context record with no job admission at all. For actual unfinished research, an opportunity source may contain `properties.fabric_income_admission`: the explicit typed research envelope governed by the Income continuation contract. Historical imports without that property return no candidate. This reader always reports `execution_admitted:false`; it supplies evidence to the owning controller rather than granting new authority.

The response separates `operational_system:highlevel`, native location/contact/opportunity/note IDs, source ID/key/digest, `legacy_provenance_urls`, and any explicit `admission_projection`. Sensitive source content stays behind existing authenticated bridge access; no credential values are returned.

Remaining adoption work before unattended HighLevel-first execution:
1. Select/admit an actual current opportunity and its exact source digest; retain the current source-version mapping.
2. Make the existing controller consume that typed projection and revalidate it before effects, preserving its budgets, pause, locks and No Auto Submit.
3. Map old tracker identities to the same source ID/native opportunity before any URL change. Existing Income cache keys include tracker URL/version; Dispatch completion keys include tracker URL/packet/job. Do not rewrite these keys or regenerate completed artifacts to change destinations.
4. Reuse existing journaled ingestion for verified artifact/receipt writeback, preserving stage, contact fields and history.

This PR provides an operationally consumable native reader, not proof that the hourly controller has switched admission sources or that all imported records are fresh.


## Protected operating checkpoint and intake compatibility

`scripts/record_highlevel_cutover.py --execute` is an explicit, fixed non-PII operating checkpoint: it previews the existing source writer, writes through that same journaled path, and reads the exact source digest back through the native Income reader. It imports the existing bridge binding privately, uses loopback only without redirects/proxies, and persists a root-private receipt. No job admission, model invocation or outreach is present. Repeated invocation reconciles the same fixed source identity/content through existing ingestion idempotency.

The reviewed controller bundle accepts typed HighLevel references while preserving proven legacy aliases. Installer validation permits the exact `highlevel://location/.../contact/...[/opportunity/...]` identity only when it matches the typed canonical reference and its source digest; historical tracker/cache keys are unchanged. The Income execution runtime already accepts these nonempty tracker identities, so its cache algorithm and existing artifact bytes require no change. A source refresh installs the reviewed controller; it does not fabricate a next opportunity or spend model budget.

Checkpoint execution requires the existing protected workflow's explicit opt-in. Source deployment alone does not create the note. Native reader delivery, controller refresh, checkpoint readback and a future real opportunity's execution are separate evidence.
