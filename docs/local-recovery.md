# Local inference recovery extension

The laptop captures only an explicit allowlist: memory, input/runtime/Codex locks, local model bearer, launchers, user service definitions, model lock, system text, source manifest and inference scripts. Model weights remain recoverable from the committed Hugging Face revision and SHA256 lock. SSH private keys, private chats and SaaS datasets are excluded. Restore requires regenerating and authorizing a host SSH key.

Run on duck after the restricted receiver is provisioned:

```sh
python3 scripts/local_recovery_snapshot.py --send --ssh-config /home/jaden/.local/share/ziji-local/transport/connection.json
```

The connection JSON contains host, user, key_file and known_hosts_file. The host key is pinned; SSH uses batch mode and never prints credentials. Unchanged content returns UNCHANGED without a network call. Changed content is encrypted in memory with AES256GCM, transmitted over authenticated SSH to the forced receiver, decrypted and checked there, then stored as ciphertext and a separate 0600 capsule key in the account-owned 0700 directory /var/lib/ziji-fabric/local-recovery/duck. The key travels only inside SSH. This staging pair is readable to that restricted account and root; encryption alone does not protect it against a compromise of that account.

The existing server snapshot includes this directory. Its outer archive is encrypted under the existing protected FABRIC_BACKUP_KEY_V1 and uploaded only to private pqexpert/fabric-dispatch releases. Neither the staging key nor plaintext state is committed or logged. Per-capsule keys do not rotate the existing off-host recovery key.

Receiver invocation: /usr/bin/python3 /opt/ziji-fabric/local-link/local_recovery_snapshot.py --receive. Requires Python cryptography. SSH authorization forces this command, restricts reverse forwarding to the chosen loopback model port and grants no general shell command. Each validated upload advances latest.json atomically; failed uploads leave the previous pointer intact. A local upload receipt means VERIFIED_ON_VPS, not yet VERIFIED_ON_GITHUB.

For recovery, download and decrypt the outer GitHub archive using the existing recovery procedure. Locate the three files selected by latest.json within the server archive. In a private temporary directory, call read_latest(Path(directory)) from scripts/local_recovery_snapshot.py; it returns the verified manifest and a mapping of allowlisted archive names to bytes without extracting arbitrary paths. Compare current state before applying selected files. Restore owner/mode and regenerate machine-specific launch paths. Never print the returned contents or keys. The module checks ciphertext authentication, SHA256 for each member and the content fingerprint. Use model-lock.json to redownload and verify exact model weights before starting inference.

Focused verification: tests/test_local_recovery_snapshot.py proves authenticated roundtrip including the model bearer, restrictive file permissions, rejection of traversal and preservation of the previous snapshot after tampering. No production restore or service restart is needed for that proof.
