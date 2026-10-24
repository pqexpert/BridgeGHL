# Duck local inference: operation and evidence

Observed 2026-10-24, approximately 05:54–05:56 UTC. This is a dated component
report, not new Fabric authority or proof of whole-Actor autonomy.

## Live changes and verification

OmniRoute 3.8.52 was running locally but had only a Codex OAuth cloud connection.
Its local address did not mean the model ran on Duck. After reviewing
[the bounded plan](../config/local-inference/omniroute-local-plan.json), two
authenticated loopback providers were registered through OmniRoute's management
API. The Codex connection was disabled through that API and read back inactive.
Its credentials were retained for explicit operator rollback.

| OmniRoute model | Local model server | Verification |
| --- | --- | --- |
| `duck-proposal/ziji-local` | Qwen3 4B, CPU, `127.0.0.1:18181` | HTTP 200; `LOCAL_OK`; 1.098 s small probe |
| `duck-coder/ziji-local-coder` | Qwen2.5 Coder 7B, Vulkan, `127.0.0.1:18081` | HTTP 200; `LOCAL_OK`; 0.810 s small probe |

Each new connection has `maxConcurrent=1`. This is per connection, not a global
lock across clients and models. Keep work serialized on this laptop.

A dedicated **Duck local-only inference** key allows only these two model names
and their two connection IDs, with no management scopes, no combos, automatic
combos disabled, and request-content logging disabled. Its private file is
`~/.local/share/ziji-local/omniroute-api-key` (0600). Do not paste its contents into
chat, logs, GitHub, or Jira. A local request using this restricted key returned
HTTP 200. A cloud model request using it returned HTTP 403 `permission_error`.
The existing broad key was retained; use the restricted key for new local clients.
The active provider set currently contains only the two reviewed local providers.
Future administrative changes can change this boundary, so recheck it.

The metadata receipt is
`~/.local/share/ziji-local/observability/omniroute-cutover.json` (0600).
It contains the prior activation state and created IDs, no credentials or prompts.
The original direct local proposal client also completed a bounded proposal in
14.12 seconds, with 734 prompt tokens and 86 output tokens. These probes establish
availability and routing; they do not establish frontier-model quality or full
task completion. The small model did not follow every requested output label.
Treat its text as an untrusted proposal for review.

These changes do not move this ChatGPT/Codex conversation to local weights, cancel
other cloud clients, stop VPS hosting charges, or establish account-wide zero
spend. Billing was not inspected. Existing Fabric mission clients retain their
previous admitted routes; OmniRoute is not automatically a production actor or
executor. No agents were spawned.

## View health, logs, and anomalies

In VS Code, use **Terminal → Run Task**:

- **Ziji Fabric: local inference anomalies (no inference)**: one metadata report.
- **Ziji Fabric: watch local inference and record metadata**: refresh every 30
  seconds until Ctrl-C; no inference or external API calls.
- **Ziji Fabric: live model logs**: the existing journal streams for both models.

Equivalent terminal commands from this repository:

```bash
python3 scripts/local_inference_observe.py
python3 scripts/local_inference_observe.py --watch --record
journalctl _SYSTEMD_USER_UNIT=ziji-local-model.service _SYSTEMD_USER_UNIT=ziji-local-llm.service --follow --lines=30 --output=short-iso
```

User services are stored in the system journal on this host. The
`_SYSTEMD_USER_UNIT` selector is required here; earlier `journalctl --user` and
`journalctl -u` queries missed these entries. Journal priorities are aggregated
in the metadata report; raw messages are never copied into its JSONL file. Raw
journal viewing is for the local operator and may show request paths or runtime
messages; do not forward it wholesale to other systems.

OmniRoute's dashboard is at <http://127.0.0.1:20128>; its log view is
<http://127.0.0.1:20128/dashboard/logs>. Login/API authentication remains enabled.
The existing gateway listener still binds `0.0.0.0:20128`; the two model servers
bind loopback only. The dashboard URL is local, but the gateway listener is not
restricted to loopback. Listener hardening and gateway service supervision would
require a separate controlled restart; neither is claimed here.
Raw request/response logging remains disabled to avoid retaining prompt content.
The observer instead checks local health, service state, memory, journal
availability, provider routing, and the dedicated key policy. It reads SQLite
with the installed modern Node runtime, read-only. It never reads token columns.
It is an on-demand observation, not an enforcement daemon or security scanner.

The optional metadata file is
`~/.local/share/ziji-local/observability/events.jsonl`, mode 0600, capped near 1 MiB
by truncating before the next append. It is a bounded status stream, not a durable
audit archive. Its event records exclude prompts, answers, credentials, and journal
message content. Native mutation evidence remains in the separate cutover receipt.

The latest observations had roughly 2.7 GiB available memory and under 1 GiB free
swap. Low free swap indicates allocated swap, not necessarily active paging.
The final one-second `vmstat` interval showed zero swap-in and swap-out.
Avoid additional resident models or concurrent workers until resource measurements
support them. Both model services were active with zero recorded systemd restarts.
OmniRoute is still the existing foreground CLI process, not a new systemd service;
closing that terminal or a process failure can stop the gateway. Existing direct
Fabric model services are separately managed by systemd. No automatic gateway
recovery has been claimed or installed in this change.

## Wazuh: running; new ingestion prepared, not activated

The manager, indexer, and dashboard are active. This does not prove there are no
security alerts. Alert contents were not inspected: protected Wazuh configuration
and alert access require administrator privileges, and passwordless sudo is not
available in this session.

Prepared snippets:

- [Metadata localfile input](../config/local-inference/wazuh-localfile.example.xml)
- [Observation and anomaly rules](../config/local-inference/wazuh-rules.example.xml)

An authorized administrator can back up the existing configuration, check that
rule IDs 100810–100811 are unused, merge the localfile inside the existing
`ossec_config`, and merge the rules into the existing local rules file. Do not
overwrite either file with a snippet. Test one metadata event with
`/var/ossec/bin/wazuh-logtest`, validate the manager configuration, then activate
and read back ingestion/alerts through the established Wazuh procedure. No Wazuh
configuration or service was changed by this work. The watcher must be running
for new metadata events to be emitted.

Wazuh's documented local-file JSON collection is the basis for the proposed
[log input](https://documentation.wazuh.com/current/user-manual/capabilities/log-data-collection/monitoring-log-files.html).

## A compact prompt for local proposals

Use [the task prompt](../config/local-inference/task-prompt.txt) with one task,
one acceptance check, a small relevant evidence packet, and current authority.
Replace its placeholders before submitting it. Keep context comfortably below
the proposal server's 4096-token context, allowing room for the pinned system
prompt and up to 512 output tokens. The complete repository is not a useful
per-request prompt. Resolve governance at the execution boundary and supply only
the evidence needed for this proposal.

The existing direct client accepts stdin:

```bash
python3 scripts/local_inference.py ask --lane income < /path/to/private-prepared-task.txt
```

It validates its pinned inputs, sends one request directly to loopback, disables
environment proxies, rejects redirects, and has no cloud fallback. Output is a
proposal and is never executed. The installed pinned system prompt was not
edited. Prompt wording helps focus a small model; route restrictions and external
authorization controls enforce the cost and effect boundaries.

OpenAI's [configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference)
documents custom model-provider settings. A local harness or gateway alone does
not establish a local model; verify the selected backend and credentials.

## Rollback

To remove this optional gateway path, deactivate the two Duck local connections
and revoke the dedicated key in OmniRoute's authenticated dashboard, using IDs
from the private cutover receipt. Remove unused custom nodes only after their
connections are removed. Re-enabling the previous Codex connection restores
cloud usage and requires an explicit operator decision. Never restore it as an
automatic fallback on a failed local request. The existing direct local model
routes and Fabric controls remain separate.
