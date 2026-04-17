# BridgeGHL

Secure, governed bridge between ChatGPT-driven workflows and HighLevel.

## Architecture

This service exposes a **controlled write surface** with two execution modes:

- `/dry-run/contact/upsert` → validate + show outbound request (no mutation)
- `/execute/contact/upsert` → perform controlled mutation with audit logging

Legacy endpoint:
- `/contacts/upsert` → deprecated

## Security Model

- All write endpoints require `x-api-key`
- HighLevel access is isolated behind server-side PIT
- No direct client access to HighLevel
- Audit logging required for every execute call

## Environment Variables

See `.env.example`

## Endpoints

### Health
```
GET /health
```

### Dry Run
```
POST /dry-run/contact/upsert
```

Validates payload and returns the exact HighLevel request that would be sent.

### Execute
```
POST /execute/contact/upsert
```

Performs actual HighLevel mutation and writes audit log.

## Audit Logging

All execute actions are written to:

```
/var/log/bridgeghl/audit.log
```

Format: JSONL (one JSON object per line)

## Deployment Notes

After pulling updates:

```bash
git pull
sudo systemctl restart bridgeghl
sudo systemctl status bridgeghl --no-pager
curl http://127.0.0.1:8000/health
```

## Canonical Flow

1. Dry-run validation
2. Review outbound request
3. Execute mutation
4. Verify audit log

This enforces safe, observable, and auditable write operations.
