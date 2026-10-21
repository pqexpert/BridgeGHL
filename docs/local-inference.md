# Local Ziji assistance on duck

Owner-authorized local inference reduces metered model calls for concise summaries,
code drafts and bounded proposals. This capability is not a fifth Fabric Actor,
a new Calendar writer, or evidence of the full server migration.

## Installed model and runtime
- Official Hugging Face repository: Qwen/Qwen3-4B-GGUF.
- Revision: bc640142c66e1fdd12af0bd68f40445458f3869b.
- File: Qwen3-4B-Q4_K_M.gguf, 2,497,280,256 bytes.
- SHA-256: 7485fe6f11af29433bc51cab58009521f205840f5b4ae3a32fa7f92e8534fdf5.
- llama.cpp source: 620d2225cc436ab0b001e3200e590e3b286ef9ab (v0.7.0).
- CPU inference, six threads, single slot, 4096-token context, 512-token output cap.
- No fine tuning or online weight changes. Pinned sampling settings are in model-lock.json;
  identical weights and seed do not guarantee identical answers across runtime/hardware changes.
- Observed host: Ryzen 9 5900HX, about 14 GiB RAM and 6 GiB available before install.
  Four-bit 4B was selected to fit the active desktop without a discrete NVIDIA GPU.

## Use
From the local terminal:
```sh
printf '%s' 'Summarize this evidence: ...' | ~/.local/bin/ziji-local
~/.local/bin/ziji-codex 'Draft a small Python function for ...'
systemctl --user status ziji-local-model
```
The second command uses the existing VS Code Codex binary with the local model over
the Responses API, fixed read-only sandbox and disabled shell, apps, plugins,
multi-agent and automatic memory features. It returns proposals; it is not an
autonomous editor or a download of OpenAI model weights. The normal Codex config
is not modified. A changed Codex binary requires a deliberate binding refresh.

The first command accepts --lane engineering|income|rsc|store|distiller.
Only one lane is attached to a request; this label does not authorize data transfer.
Neither client loads arbitrary memory, scans documents or executes model output.
Do not pipe sensitive cross-lane records into the wrong request.

## Governance and memory
Canva DAHS8QVjVng was read on 2026-10-21, updated_at 1792617999.
Its current truth/communication, principal-preservation, non-harm and agency guidance
is reflected in the bounded system prompt. GitHub sources and blob identities are
recorded in sources.json. The projection does not amend the sole Fabric Contract.
LLMLMS Knowledge Lifecycle governs correction: compare existing canon, fix the
smallest owning control, verify actual adoption. No new learning service is needed.

Only these curated preferences are loaded:
- Think rigorously, speak accessibly, write clearly for action.
- Prefer low-cost local execution with bounded success criteria.
- Preserve healthy work and repair the specific failed dependency.
- Benefit beings and loved ones through compassion, truthfulness, non-harm and respect for agency.

Private chat history, health records, relationship records and customer data were not imported.
Local memory SHA-256: 5bef004ad045995bf1d6bb24cc12adfaa43032f77adab3cc12df450e43b88cb3.
Weights, runtime files and model/system/memory/source inputs have integrity locks.
Changing approved configuration requires an intentional lock refresh; no model can do that.
Current authority and native state must still be resolved before any external effect.

## Enforced boundary
The localhost-only service requires a private API key. Built-in tools, agent mode,
web UI and MCP proxy are disabled. It inherits a minimal environment, no provider
credentials. systemd applies NoNewPrivileges, read-only home/system, private tmp,
a 5 GiB memory cap and 400% CPU quota. The model has no callable effect executor.
The host owner/root can change files and controls: these are verifiable boundaries,
not a claim of inviolability, spiritual realization or guaranteed correct judgment.
Direct raw API clients must apply their own instructions; use the provided clients
for the pinned governance/memory envelope.

Receipts from ziji-local contain hashes, token usage and elapsed time, not prompt
or answer bodies. Codex runs are ephemeral. Inputs and generated text stay local
for inference; installation fetches public source/model files from their publishers.

## Recovery and rollback
Source/configuration belongs to BridgeGHL; model binaries are not committed to Git.
Local state is under ~/.local/share/ziji-local. Keep memory.json, input-lock.json,
runtime-lock.json and codex-binding.json private. api-key is locally generated and
may be regenerated with clients stopped; it is not a SaaS credential.
Model recovery: download the exact revision/file above and verify SHA-256 before use.
Runtime recovery: build the pinned llama.cpp commit with CMake Release and target
llama-server, CPU only. Generate runtime hashes after checking its source identity.
Install the service from config/local-inference; adjust its checkout path if needed.
This user service runs with the user's service manager; uninterrupted laptop uptime
or operation after logout is not claimed.

Stop/rollback:
```sh
systemctl --user disable --now ziji-local-model.service
```
No VPS or hosted actor schedule was switched by this local deployment.
The existing encrypted VPS backup does not automatically include this laptop state.
