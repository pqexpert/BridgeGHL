# GitHub and IONOS cutover: BridgeGHL, Fabric, and mail

## Current outreach acceptance — 2026-09-24
The existing IONOS VPS now serves [Restoration Security Outreach](https://restorationsecurityoutreach.com/) over verified HTTPS. [Production run36025080806](https://github.com/pqexpert/BridgeGHL/actions/runs/36025080806) passed DNS, exact website revision, canonical www routing, CTA/policy checks, protected records/services and renewal configuration. See [the current outreach recovery runbook](outreach-deployment.md) for the bounded execution and retained backup controls.

HighLevel's active outreach sending subdomain is mg.restorationsecurityoutreach.com, as recorded in the current private operating evidence. Preserve its mail records. The September23 mg.restoration-mt.com setup notes below are historical and are not the current outreach-domain plan.

## September23 runtime and migration snapshot
Status as of 2026-09-23. ChatGPT-to-IONOS BridgeGHL health is verified; Income actor publication and Mailgun remain open.

## Working runtime

- GitHub repository `pqexpert/BridgeGHL` is the source for BridgeGHL and the separate `mcp_server.py` adapter.
- GitHub Actions environment `zijifabric` deploys to the IONOS VPS. The protected secret value is never committed.
- `bridgeghl.service` serves the governed CRM API at loopback port 8000, using `/etc/bridgeghl/bridgeghl.env`; `bridgeghl-mcp.service` serves streamable MCP at **127.0.0.1:8001/mcp** and calls BridgeGHL through its existing API key. Keep this MCP port private.
- The MCP deploy workflow validates JSON-RPC initialization and invokes `bridge_health`. Successful run: https://github.com/pqexpert/BridgeGHL/actions/runs/35898637508
- Available governed tools: `bridge_health`, `dry_run_opportunity_update`, `execute_opportunity_update`, `dry_run_contact_tags`, and `execute_contact_tags`. Governed writes remain subject to the BridgeGHL allowlist, audit, and readback.
- Weekly `vps-capacity.yml` checks service states, health, CPU load, RAM, and disk. Latest sampled VPS: 4 CPUs, about 7.6 GiB RAM (about 7 GiB available), 238 GiB root volume (about 231 GiB free). No upgrade is warranted on those measurements. A ChatGPT weekly capacity alert watches for <1 GiB available RAM, <30 GiB disk, sustained load beyond 4 CPUs, or service/health failures. Split workload or upgrade when measured pressure persists.

## Verified ChatGPT-to-IONOS route

The dedicated tunnel `bridgeghl-ionos-rsc` (`tunnel_6ab421054e788191863ca2de68149862`) is associated with Restoration and Josh Allen's Workspace. The user provisioned a restricted Tunnels Read + Use key in `zijifabric` secret `OPENAI_TUNNEL_RUNTIME_KEY`. [Deployment run 35908420681](https://github.com/pqexpert/BridgeGHL/actions/runs/35908420681) installed checksum-pinned official tunnel-client v0.0.14 and verified `tunnel_ready_verified=true`.

`bridgeghl-tunnel.service` connects outbound to OpenAI and forwards to `http://127.0.0.1:8001/mcp`. Its health endpoint is `127.0.0.1:8002`. The key is held in root-only `/etc/bridgeghl-tunnel/runtime.env`, mapped to `CONTROL_PLANE_API_KEY`; the daemon runs as a systemd DynamicUser. No admin key is installed, no public inbound port was opened, and no VPS reboot was needed.

The connected ChatGPT app **BridgeGHL IONOS** is `asdk_app_6ab426506bb0819183fcf4deefac70fa`; initial discovered version `asdk_app_v_6ab426506bbc8191b206aa4f866cf097`. The UI reports development mode, not workspace-wide publication. All five governed tools were discovered through the private tunnel. Application-layer auth is None because access uses the workspace-associated OpenAI tunnel; the MCP listener stays loopback-only.

A [ChatGPT verification invocation](https://chatgpt.com/c/6ab426f1-ab04-83ea-8c75-68a951beda4c) of `mcp__codex_apps__bridgeghl_ionos_bridge_health` returned `ok=true`, `state=HEALTHY`, and `highlevel_readback: status=200` at **2026-09-23T19:22:42.929543Z**. No CRM mutation occurred. Approved custom-field/tag counts were both zero; health success does not broaden write authority.

Health and dry-run tool annotations now explicitly identify read-only/non-destructive behavior in deployed source ([run 35908787937](https://github.com/pqexpert/BridgeGHL/actions/runs/35908787937)); any earlier app snapshot may need Refresh to consume the new annotations.

### Actor configuration readback and remaining cleanup

- **RSC:** new BridgeGHL IONOS app attached; current GitHub/IONOS route and Calendar boundary added; Base44 and MCP for Fabric Use removed. Update succeeded and persisted after reload.
- **Dzokden:** current GitHub/IONOS source and native Shopify boundary added. Base44, MCP for Fabric Use, and the out-of-scope highlevel-write-bridge skill removed. Update succeeded and persisted after reload; dzokden-shopify-operator remains attached.
- **Income:** repository route is current. Hosted editor was prepared with BridgeGHL IONOS added, Base44/MCP for Fabric Use removed and a current route override, but repeated Update/Retry attempts showed **Unsaved changes** and failed to persist after reload. A smaller edit also failed. Do not claim Income runtime alignment. The latest four edits remain pending in the editor; investigate the native save failure before another normal Income cycle depends on this route.
- No synthetic paid actor cycles were launched to manufacture adoption evidence. Next useful receipts must identify actual route consumption.
- The old shared `MCP for Fabric Use` app still exists and exposes other tools; global disconnection and legacy Base44 PIT retirement remain open pending consumer inventory. Its historical invalid-PIT 401 is not the new IONOS route's health.

## Mailgun / LC Email and HighLevel

Candidate sending subdomain: **mg.restoration-mt.com**, using the existing owned-domain context; verify provider reservation and authorized DNS-account control before activation. Public DNS readback on 2026-09-23 found no TXT/MX answers at that name. Root MX remains Proton; root DMARC is p=quarantine. Preserve root MX/SPF/DMARC and ordinary person-to-person mail. mail.restoration-mt.com has an existing route and is not the clean candidate.

Authenticated native RSC Settings -> Email Services now confirms **LeadConnector Email System**, with shared domain `send.lcmsgsndr.com`. The `mg.restoration-mt.com` Add & Verify flow generated the records below. The direct CRM connector had returned contacts instead of email settings; that response was not used as configuration evidence.

### Preferred path if LC Email is still active
Use its existing **Dedicated Domain and IP -> Add Domain** flow for the sending subdomain. Record the exact DNS values the provider issues, add only the required subdomain records in the authoritative DNS account, then verify in HighLevel. A separate Mailgun account/API key is not a prerequisite for this LC Email flow. Do not buy a dedicated IP, add a subscription or migrate providers merely to complete a checklist.

### Alternative if a separate Mailgun provider is selected
Verify the intended account/region and HighLevel-supported integration, create/verify the dedicated sending domain there, and connect it through the applicable HighLevel provider settings. Keep Mailgun transport credentials separate from the HighLevel PIT and tunnel runtime key. Never invent DKIM selectors, tracking targets or SPF values; use that provider's native records.

### Acceptance before real outreach
Confirm SPF/DKIM and DMARC alignment, sender/reply-to identity, return-path and tracking behavior. Use one approved test recipient and read back delivery/bounce and reply handling; do not send to real CRM audiences as an infrastructure test. Preserve native DND and exclusions. Relationship class (business/career/academic/personal/test-system), provenance/confidence and outreach authority must be explicit; absent authority means **none**. Existing namespaced fields and pipelines in the ecosystem plan should be mapped rather than duplicated. Ingestion never implies permission to contact.

Sending-domain verification is separate from mailbox hosting, Google/Gmail sync, CRM authority and campaign activation. HighLevel remains the relationship/action layer; GitHub/Notion/Jira retain their existing knowledge/evidence roles.

**Current state:** LC Email setup for `mg.restoration-mt.com` has advanced to Apply and Verify; all six provider records show **Not Verified**. No DNS write or email send is proved. Cloudflare auto-configuration identified the provider; authorization completion was not verified. The dedicated Cloudflare tab reached a site-served security verification page. Preserve root Proton records; finish DNS authentication and verification before enabling outreach. [HighLevel LC Email setup](https://help.gohighlevel.com/support/solutions/articles/48001226115-dedicated-email-sending-domains-overview-setup) and [Mailgun domain verification](https://help.mailgun.com/hc/en-us/articles/32884702360603-Domain-Verification-Setup-Guide) explain the distinct routes. Track activation in issue 31.

## Other migration work

`restoration-mt.com` still serves the existing RSC website. Before moving its runtime, inventory its live Nginx route and other redirects/forwarded domains. A checked-in `nginx.conf` refers to port 3000 while live BridgeGHL uses 8000; do not deploy that old config unreviewed. Put site source and deploy workflow under GitHub and use the IONOS VPS as the production runtime after the inventory.

## Actor access routes

| Actor | Source home | Authorized domain route |
| --- | --- | --- |
| RSC Command Center | pqexpert/rsc-operations-console | BridgeGHL IONOS app attached and saved; end-to-end bridge health verified. |
| Income Accelerator | pqexpert/income-accelerator | Same governed BridgeGHL capability for admitted CRM work, retaining Income's own authority and data boundaries; hosted editor save failed; binding not yet persisted. |
| Dzokden Store Manager Ziji | pqexpert/Dzokden-Store-Manager-Ziji | Governed native Shopify route for Tantric Treasures; no Dzokden customer/order/payment/fulfillment data through RSC HighLevel or BridgeGHL. |

GitHub owns source, deployment workflows and protected deployment configuration; IONOS runs persistent services. ChatGPT remains the actor/tool invocation surface. A GitHub environment is repository-scoped: a same-named environment in another repository does not automatically share secret values. Keep provider secrets with the service that consumes them, not in all actor repositories. The local actor bootstrap pointers resolve this runbook; these source edits do not prove that hosted actors consumed the changes.


## Reusable recovery learning

[Wise Action Governor 2026-09-23.1](https://github.com/pqexpert/FungibleRD-MVP/blob/main/skills/wise-action-governor/SKILL.md) now routes recovery workers to the deployment/connector procedure. [Today's receipt](https://github.com/pqexpert/FungibleRD-MVP/blob/main/docs/learnings/BRIDGEGHL-RECOVERY-2026-09-23.md) preserves verified results, failed approaches and adoption debt under RBO-75. This source update does not assert all installed skills or hosted actors consumed it.

### Provider-issued DNS records — 2026-09-23

Hostnames below are relative to `restoration-mt.com`; these are public DNS values, not private credentials. They are recorded from the native Apply and Verify page and are **not yet deployed**.

| Type | Host | Priority | Value |
| --- | --- | --- | --- |
| TXT | mg | — | `v=spf1 include:spf.leadconnectorhq.com include:mailgun.org ~all` |
| TXT | pic._domainkey.mg | — | `k=rsa; p=MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC6n/DUfqJWhZWvMbEb0P11xcX3QMxQvG4C0S4YpCAz4Deqd2QRQbCnwUfJj4ubcoF/JYQwtjkbEVkOpMTRaps5AHPR0qCukIwf02kKkTfgJNCsxNLGDAl4a3B9Uou+i5bfUfhYy1TheSB7FyhsbX9TtJ5Mis0HyqU6rLpLbW6krQIDAQAB` |
| CNAME | email.mg | — | `mailgun.org` |
| MX | mg | 10 | `mxa.mailgun.org` |
| MX | mg | 10 | `mxb.mailgun.org` |
| TXT | _dmarc.mg | — | `v=DMARC1;p=none;` |

The provider's subdomain DMARC recommendation is monitoring-only and would override inherited root quarantine at that subdomain. It has **not** been applied; choose an aligned enforcement policy explicitly before activation. Root DMARC remains unchanged.
