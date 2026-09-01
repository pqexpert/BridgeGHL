# RSC agent MCP adapter

## Purpose

Expose the complete governed BridgeGHL mutation envelope to ChatGPT/Codex as a
private tool-only MCP server. The adapter does not contain HighLevel business
logic and never receives the HighLevel PIT.

## Tool contract

- `bridge_health`
- `dry_run_opportunity_update`
- `execute_opportunity_update`
- `dry_run_contact_tags`
- `execute_contact_tags`

The execute tools are externally mutating but non-destructive inside the
canonical reversible allowlist. BridgeGHL remains the enforcement and audit
boundary.

## Trust boundaries

1. The public MCP endpoint must be protected by an OAuth 2.1-capable gateway
   conforming to the MCP authorization contract.
2. The MCP adapter is reachable only from that gateway.
3. The adapter authenticates to BridgeGHL with `BRIDGE_API_KEY`.
4. BridgeGHL alone holds `HIGHLEVEL_PIT`, `HIGHLEVEL_LOCATION_ID`, exact
   allowlists, and the audit log.
5. Neither credential is returned in MCP tool output.

Do not expose the adapter port directly to the internet. A shared bearer token
in a prompt or tool input is not an acceptable substitute for OAuth.

## Runtime environment

- `BRIDGE_BASE_URL=http://bridgeghl:3000`
- `BRIDGE_API_KEY=<same protected caller credential used by BridgeGHL>`
- `BRIDGE_TIMEOUT_SECONDS=30`

## Deployment

Run the MCP adapter beside BridgeGHL on the private container network. Terminate
TLS and OAuth at the ingress. Route the authenticated MCP path to the adapter's
streamable HTTP endpoint. Keep BridgeGHL's own HTTP port private except for
operator health access.

## Acceptance sequence

1. Start with `LIVE_WRITE_ENABLED=false`.
2. Confirm MCP initialization and tool discovery.
3. Call `bridge_health`; require truthful `DEGRADED` while live writes are off.
4. Bind protected BridgeGHL runtime configuration and exact allowlists.
5. Enable live writes and require `HEALTHY` with HighLevel readback.
6. Exercise both dry-run tools with representative and invalid payloads.
7. Run one reversible execute canary for each enabled mutation class.
8. Require `verified=true`, preserve audit IDs, and reverse the canaries.
9. Refresh the private ChatGPT app so the final tool descriptors are reloaded.

Official design references:

- https://developers.openai.com/plugins/build/mcp-server
- https://developers.openai.com/plugins/plan/tools
- https://developers.openai.com/plugins/build/auth
