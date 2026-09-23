# GitHub and IONOS cutover: BridgeGHL, Fabric, and mail

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

## Mailgun and HighLevel

Use `mg.restoration-mt.com` as a candidate **dedicated sending subdomain**, subject to confirmation that the domain belongs to this organization and is not already reserved in Mailgun. DNS currently has no records for that name. The root `restoration-mt.com` receives mail through Proton; do not replace its MX/SPF to set up Mailgun. `mail.restoration-mt.com` has an existing proxied A record and is not the clean candidate.

1. In Mailgun, create a **US-region** sending domain for `mg.restoration-mt.com`. Copy its exact domain-specific SPF, DKIM, tracking CNAME, and (if receiving is enabled) MX records into Cloudflare DNS. Keep tracking host DNS-only where Mailgun requires it. Verify the domain in Mailgun and send a test.
2. Obtain the Mailgun private API key through the Mailgun account and store it only in the appropriate protected credential store; it is **not** a BridgeGHL or HighLevel PIT.
3. In the intended HighLevel agency/subaccount, configure Email Services > SMTP Service with the Mailgun private API key and verified US Mailgun domain, activate the provider, and test outbound delivery and reply handling. Agency-wide defaults and subaccount overrides have different precedence; verify the intended RSC location explicitly.
4. Record the verified DNS, sending status, and HighLevel test results here without key values.

No Mailgun, Cloudflare DNS write, or HighLevel email-settings administration is exposed by the currently connected tools. These steps remain open; do not claim mail is connected or adjust the root Proton mail routing.

## Other migration work

`restoration-mt.com` still serves the existing RSC website. Before moving its runtime, inventory its live Nginx route and other redirects/forwarded domains. A checked-in `nginx.conf` refers to port 3000 while live BridgeGHL uses 8000; do not deploy that old config unreviewed. Put site source and deploy workflow under GitHub and use the IONOS VPS as the production runtime after the inventory.

## Actor access routes

| Actor | Source home | Authorized domain route |
| --- | --- | --- |
| RSC Command Center | pqexpert/rsc-operations-console | BridgeGHL IONOS app attached and saved; end-to-end bridge health verified. |
| Income Accelerator | pqexpert/income-accelerator | Same governed BridgeGHL capability for admitted CRM work, retaining Income's own authority and data boundaries; hosted editor save failed; binding not yet persisted. |
| Dzokden Store Manager Ziji | pqexpert/Dzokden-Store-Manager-Ziji | Governed native Shopify route for Tantric Treasures; no Dzokden customer/order/payment/fulfillment data through RSC HighLevel or BridgeGHL. |

GitHub owns source, deployment workflows and protected deployment configuration; IONOS runs persistent services. ChatGPT remains the actor/tool invocation surface. A GitHub environment is repository-scoped: a same-named environment in another repository does not automatically share secret values. Keep provider secrets with the service that consumes them, not in all actor repositories. The local actor bootstrap pointers resolve this runbook; these source edits do not prove that hosted actors consumed the changes.

