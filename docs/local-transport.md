# Private local inference transport

The duck model remains bound to 127.0.0.1:18181. Its dedicated SSH key opens only a reverse listener at VPS 127.0.0.1:18182. The VPS does not expose an inference port publicly. Strict host-key verification pins the existing trusted VPS ed25519 identity. Administrator credentials stay in the existing protected GitHub environment.

Run the manual provision-local-transport workflow with phase provision after the recovery receiver is on main. It provisions ziji-local-link with a forced recovery receiver and remote forwarding restricted to the loopback listener. Only this dedicated public key is committed. Local private key and bearer are never committed.

Install ziji-local-tunnel.service in the duck user service directory. Its SSH config must specify Host ziji-local-link, HostName 74.208.52.45, User ziji-local-link, IdentityFile pointing to the dedicated transport/id_ed25519, IdentitiesOnly yes, BatchMode yes, StrictHostKeyChecking yes, UserKnownHostsFile pointing to the pinned transport/known_hosts, ExitOnForwardFailure yes, ServerAliveInterval 30, ServerAliveCountMax 3, and RemoteForward 127.0.0.1:18182 127.0.0.1:18181. Keep configuration private. The service reconnects after network outages.

The recovery sender uploads a verified encrypted capsule through the forced receiver. Run workflow phase verify afterward. It installs exact verified model configuration, system policy, curated memory, source manifest and inference-only bearer in root-only VPS configuration; hashes are recorded in input-lock.json. It returns only status and hashes, never credentials or proposals.

scripts/local_inference_transport.py --request-file PRIVATE_JSON accepts exactly request_id, lane and prompt. Lanes engineering, income, rsc, store and distiller only propose text. Requests use the fixed authenticated loopback endpoint with redirects and proxy use disabled. Changed input hashes fail closed. Duplicate successful IDs return their existing receipt without another model call; conflicting IDs are rejected. A per-ID lock serializes retries. Unavailable local inference queues the same bounded request on the VPS; retry with the same ID after connectivity returns. There is no cloud or paid fallback and no external action execution. This does not itself replace any actor scheduler or authorize writes.

Private queues/results and restored inputs reside under /var/lib/ziji-fabric and /etc/ziji-fabric, covered by the existing encrypted recovery backup. Rollback stops the duck tunnel service; remove only the dedicated account's SSH configuration if retiring transport. Existing production services and schedules remain independent.

## Local operator controls

Run the installed repository's `python3 scripts/local_fabric_control.py status`, `pause`, or `resume`. Status uses the existing local health/receipt view and shows the pause marker even while the model is unavailable; it makes no model or external API call.

Pause stops the SSH tunnel first, then the local model. A private marker and dedicated systemd condition drop-ins preserve that choice across reboots. Existing drop-ins are not rewritten; a conflicting control file fails closed. Recovery stays enabled. Credentials, queued requests, receipts, user projects and model inputs remain intact. Repeating pause does not overwrite the original resume set. Resume starts only units recorded active before pause, leaves their enablement unchanged, and restores the pause marker if a start fails. It does not activate a previously inactive unit.

This is an emergency stop for local inference, **not a global stop**: it cannot revoke already-issued remote provider effects or pause unrelated workers. Use the VPS worker's separately governed pause control before its effects when needed. Closing or suspending duck makes the local endpoint unavailable; the VPS must retain queued work and defer with bounded backoff, never substitute a paid model. Resuming requires the laptop to be awake and connected; these controls do not override lid/power policy or promise uninterrupted laptop availability.

Deployment only installs the command/status files; no pause, resume or new schedule is performed by installation. The operator's first pause installs the two narrowly named condition drop-ins without replacing other unit configuration.
