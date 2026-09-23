# GitHub and IONOS cutover: BridgeGHL, Fabric, and mail

Status as of 2026-09-23. Do not treat this document as proof that a ChatGPT app or Mailgun has been linked.

## Working runtime

- GitHub repository `pqexpert/BridgeGHL` is the source for BridgeGHL and the separate `mcp_server.py` adapter.
- GitHub Actions environment `zijifabric` deploys to the IONOS VPS. The protected secret value is never committed.
- `bridgeghl.service` serves the governed CRM API at loopback port 8000, using `/etc/bridgeghl/bridgeghl.env`; `bridgeghl-mcp.service` serves streamable MCP at **127.0.0.1:8001/mcp** and calls BridgeGHL through its existing API key. Keep this MCP port private.
- The MCP deploy workflow validates JSON-RPC initialization and invokes `bridge_health`. Successful run: https://github.com/pqexpert/BridgeGHL/actions/runs/35898637508
- Available governed tools: `bridge_health`, `dry_run_opportunity_update`, `execute_opportunity_update`, `dry_run_contact_tags`, and `execute_contact_tags`. Governed writes remain subject to the BridgeGHL allowlist, audit, and readback.
- Weekly `vps-capacity.yml` checks service states, health, CPU load, RAM, and disk. Latest sampled VPS: 4 CPUs, about 7.6 GiB RAM (about 7 GiB available), 238 GiB root volume (about 231 GiB free). No upgrade is warranted on those measurements. A ChatGPT weekly capacity alert watches for <1 GiB available RAM, <30 GiB disk, sustained load beyond 4 CPUs, or service/health failures. Split workload or upgrade when measured pressure persists.

## ChatGPT/Fabric binding still required

The existing `MCP for Fabric Use` tool `highlevel_governed_write` routes to the Base44 RSC Operations Console deployment and still returns a HighLevel invalid-PIT 401. Its source function was replaced with a retired-route response and the source MCP config no longer advertises the tool (Base44 checkpoint `513c8a43b9bd41119c75d08ac6f7c5f4b3cfc2a6`), but the deployed/cached tool has **not** updated. Do not use it to infer VPS health or claim the old connection is gone.

1. In an authenticated OpenAI Platform/ChatGPT admin session, establish a private route from ChatGPT to the VPS MCP service, such as the official secure MCP tunnel (outbound connection from VPS to OpenAI) or another authenticated TLS endpoint. Never publish 8001 directly or place the Bridge API key in a query string or app description.
2. Create/repoint the ChatGPT custom app to the new MCP endpoint, review its tools, publish/refresh the tool snapshot, and invoke `bridge_health` through that actual ChatGPT app. Record the app binding and health result in this repository without tokens.
3. Remove the old Base44 Fabric MCP app/tool binding from ChatGPT, then retire the legacy Base44 `HIGHLEVEL_PIT` secret after confirming no other live consumer. An old tool invocation returning 401 means this cleanup remains open. Keep any unrelated Base44 tools separately inventoried before retiring the entire app.

ChatGPT app administration and OpenAI Platform tunnel control are not exposed through the connected tools in this environment; a private, healthy VPS endpoint alone does not complete these steps.

## Mailgun and HighLevel

Use `mg.restoration-mt.com` as a candidate **dedicated sending subdomain**, subject to confirmation that the domain belongs to this organization and is not already reserved in Mailgun. DNS currently has no records for that name. The root `restoration-mt.com` receives mail through Proton; do not replace its MX/SPF to set up Mailgun. `mail.restoration-mt.com` has an existing proxied A record and is not the clean candidate.

1. In Mailgun, create a **US-region** sending domain for `mg.restoration-mt.com`. Copy its exact domain-specific SPF, DKIM, tracking CNAME, and (if receiving is enabled) MX records into Cloudflare DNS. Keep tracking host DNS-only where Mailgun requires it. Verify the domain in Mailgun and send a test.
2. Obtain the Mailgun private API key through the Mailgun account and store it only in the appropriate protected credential store; it is **not** a BridgeGHL or HighLevel PIT.
3. In the intended HighLevel agency/subaccount, configure Email Services > SMTP Service with the Mailgun private API key and verified US Mailgun domain, activate the provider, and test outbound delivery and reply handling. Agency-wide defaults and subaccount overrides have different precedence; verify the intended RSC location explicitly.
4. Record the verified DNS, sending status, and HighLevel test results here without key values.

No Mailgun, Cloudflare DNS write, or HighLevel email-settings administration is exposed by the currently connected tools. These steps remain open; do not claim mail is connected or adjust the root Proton mail routing.

## Other migration work

`restoration-mt.com` still serves the existing RSC website. Before moving its runtime, inventory its live Nginx route and other redirects/forwarded domains. A checked-in `nginx.conf` refers to port 3000 while live BridgeGHL uses 8000; do not deploy that old config unreviewed. Put site source and deploy workflow under GitHub and use the IONOS VPS as the production runtime after the inventory.
