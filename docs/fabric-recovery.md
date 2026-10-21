# Fabric server recovery

The `Encrypted Fabric recovery backup` workflow uses the existing IONOS SSH
binding and GitHub credential. It runs daily at 07:43 UTC and on relevant main
changes. Each successful run writes a new encrypted release asset to the existing
**private** `pqexpert/fabric-dispatch` repository. It never replaces an older backup.

Recovery secret: `FABRIC_BACKUP_KEY_V1` in `pqexpert/BridgeGHL`, environment
`zijifabric`. The workflow creates it only if absent; an existing but unbound key
is a binding error, never an excuse to rotate and lose old recovery access.
Do not delete this key while backups depend on it. GitHub account loss is not
covered by keeping the recovery key and archive in the same GitHub account.

## What is captured

- Existing Fabric and BridgeGHL source, protected configuration, operational
  state, audit logs, relevant systemd/cron files and nginx configuration.
- Seven Git repository histories as complete Git bundles: BridgeGHL, Dispatch,
  Distiller, Income, RSC console, Store and the canonical LLMLMS source home.
- A private manifest of byte hashes, ownership/modes, source heads, SQLite
  snapshots, exclusions and files observed changing during capture.

SQLite uses its online backup API, including committed WAL data. Ordinary files
are individually copied; this is not a cross-service atomic snapshot. Virtual
environments/dependencies/caches are omitted and rebuilt from captured source.
Symlinks are preserved; external targets, TLS material outside nginx, OS packages,
ACLs/xattrs and provider-side datasets are not implied by this coverage.
Jira, Notion, Google Calendar/Drive, Shopify and HighLevel remain authoritative
for their native datasets. A server backup does not export those services.

## Verification and restoration

The backup job downloads the uploaded asset, authenticates AES-256-GCM decryption,
compares the recovered archive and member hashes, and verifies each Git bundle
has no missing prerequisite history. Snapshot verification checks internal file
hashes, inventory, modes, ownership and link targets. These checks do not boot a
replacement production host.

To recover, use an authorized runner with the protected key bound as `BACKUP_KEY`
and the private release asset downloaded. Import `scripts/fabric_github_backup.py`
and call `decrypt(encrypted_path, staging_archive_path,
base64.b64decode(os.environ['BACKUP_KEY'], validate=True))`. Use a private staging
directory. Never print the key or decrypted content into workflow logs.
Inspect `manifest.json` and `FABRIC-SNAPSHOT-MANIFEST.json` before restoring.
Restore Git history with `git clone <repository>.bundle <directory>`.

Restore server files only into a selected recovery target after comparing current
state. Recreate service users using recorded ownership, reinstall dependencies
from captured requirements, restore protected config permissions, and enable only
the admitted services. Read native service/provider state after restoration.
Do not blindly replay unfinished external mutations from old records, enable a
second Calendar writer, or overwrite legitimate newer state.

## Migration boundary

Backups, bridge deployment, hosted source adapters and Calendar handover are
separate work. A missing native credential blocks only its lane's cutover.
Keep the existing hosted mission/writer running until the replacement can perform
its admitted work. No full-migration claim follows from this backup workflow.
