# RSC outreach deployment and recovery

This is the bounded deployment path for restorationsecurityoutreach.com and www.restorationsecurityoutreach.com. GitHub owns source and deployment; the existing IONOS VPS serves the static release. Mail and the corporate/legacy domain remain separate.

## Current release
Website source: pqexpert/rsc-operations-console at b2feed67d768d0e29e498b8af6189bb6b23850f9.
Production acceptance passed on2026-09-24 at16:07UTC in [run36025080806](https://github.com/pqexpert/BridgeGHL/actions/runs/36025080806), executing BridgeGHL9041b917635d2236f493624314a04800d9ed57ec. The exact revision, HTTPS, canonical www redirect, CTA/policy routes, preserved non-web records, ingress, existing services and renewal configuration were verified. Private success/rollback evidence is retained in RBO-97 and /var/lib/rsc-ops/cutovers/outreach-36025080806-1. The independent cloud-browser preview returned a gateway/TLS error during closeout; its view is not claimed verified.

## Controls carried by the existing deployment
- Environment zijifabric and concurrency group bridgeghl-production preserve the existing credential custody and one deployment at a time.
- Root-only, per-attempt evidence under /var/lib/rsc-ops/cutovers records the full DNS before-state, immutable mutation journal, Nginx before-state, firewall before-state, and final or rollback receipts. Keep this evidence private; do not upload it as a public Actions artifact.
- The DNS helper updates existing record IDs individually. Both root/www A records point to the VPS; original AAAA records stay present but disabled. It neither deletes records nor replaces the zone. All other DNS records, including mail/mg, are compared before and after.
- Backup custody and integrity must pass before effects. Rollback rereads uncertain effects and restores only exact owned changes; a conflicting human/provider change stops that target.
- Only TCP80/443 are added to the actual ingress zone in runtime and permanent state. No broad firewall reload or copying of runtime rules into permanent configuration.
- Preserve SELinux enforcement. The parent /var/www and challenge path must match policy labels; a correct child label cannot compensate for a mislabeled parent.
- Nginx reload is graceful. Use bounded readiness checks before judging a new route; process reload acknowledgment is not route readiness.
- Verify the exact public revision, canonical HTTPS/www routing, native readiness CTA, policy paths, protected Nginx/service state, and existing certificate renewal timer/hook.

## Recovery and repeat execution
The cutover workflow is manual-only after acceptance. It is guarded for the original recovered four-record baseline; it is not a generic redeploy button. Never replay it to prove an already accepted outcome. Future release changes must resolve current DNS and use the existing deployment owner with a fresh bounded plan.

A failed attempt retains its private evidence. Preserve the corrected host security label; do not restore a known labeling defect. If an external effect is uncertain, read the native state before any retry. Never restore a whole zone or remove all root/www records as a shortcut.

## Customer acceptance
Infrastructure acceptance is separate from a customer event. Readiness uses the existing HighLevel form and published inquiry/triage workflows. Josh retains the relationship, calls, care and troubleshooting. Do not replay completed owner tests or enroll historic contacts. The next natural eligible inquiry provides the continuous form-to-acknowledgment-to-owner/reply evidence.

RSC owns delivery and native signal reconciliation through its existing cadence; Distiller consumes completed findings and is the sole Calendar writer. No new monitoring task or actor is required.
