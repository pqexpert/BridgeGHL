"""Add exactly the approved HighLevel go CNAME, preserving the existing zone."""
import json
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from rsc_outreach_dns import Ionos, digest, persist_remote, shape

NAME = 'go.restorationsecurityoutreach.com'
VALUE = 'sites.ludicrous.cloud'
WANTED = dict(name=NAME, type='CNAME', content=VALUE, ttl=300, prio=0, disabled=False)


def target(records):
    return [r for r in records if r['name'].rstrip('.').lower() == NAME]


def exact(rows):
    return len(rows) == 1 and shape(rows[0]) == WANTED


def acceptable(rows):
    # A pre-existing correct active CNAME is a no-op regardless of its TTL.
    return len(rows) == 1 and all(shape(rows[0])[k] == WANTED[k]
                                 for k in ('name', 'type', 'content', 'prio', 'disabled'))


def apply(api, persist):
    zone, before = api.current()
    persist('go-dns-before.json', dict(zone=zone, records=before, desired=WANTED))
    existing = target(before)
    if existing and not acceptable(existing):
        raise RuntimeError('go_record_conflict_no_write')
    fresh_zone, fresh = api.current()
    if fresh_zone != zone or digest(fresh) != digest(before):
        raise RuntimeError('go_dns_pre_effect_compare_failed_no_write')
    if existing:
        receipt = dict(result='noop', name=NAME, value=VALUE,
                       protected_records_preserved=True, record_id=existing[0]['id'])
        persist('go-dns-success.json', receipt)
        return receipt
    protected = digest(before)
    persist('go-dns-create-intent.json', dict(zone=zone, desired=WANTED,
            protected_digest=protected, authority='RBO-97 comment 11475'))
    # Re-read after private backup acknowledgment, immediately before the single POST.
    check_zone, check = api.current()
    if check_zone != zone or digest(check) != protected:
        raise RuntimeError('go_dns_final_compare_failed_no_write')
    owned_id = None
    try:
        result = api.request('POST', '/zones/' + zone + '/records', [WANTED])
        if isinstance(result, list) and len(result) == 1 and exact(result):
            owned_id = result[0]['id']
        persist('go-dns-create-result.json', dict(response=result, owned_id=owned_id))
        after_zone, after = api.current()
        persist('go-dns-after.json', dict(zone=after_zone, records=after))
        rows = target(after)
        if after_zone != zone or not exact(rows):
            raise RuntimeError('go_dns_record_readback_failed')
        if owned_id and rows[0]['id'] != owned_id:
            raise RuntimeError('go_dns_created_identity_changed')
        others = [r for r in after if r['name'].rstrip('.').lower() != NAME]
        if digest(others) != protected:
            raise RuntimeError('go_dns_protected_records_changed')
        receipt = dict(result='created', name=NAME, value=VALUE, record_id=rows[0]['id'],
                       protected_records_preserved=True, protected_digest=protected,
                       private_evidence=os.environ.get('BACKUP_DIR'),
                       source=os.environ.get('GITHUB_SHA'), run=os.environ.get('GITHUB_RUN_ID'))
        persist('go-dns-success.json', receipt)
        return receipt
    except Exception:
        # Read ambiguous effects once; never blindly retry creation or delete an unowned ID.
        current_zone, current = api.current()
        persist('go-dns-failure-readback.json', dict(zone=current_zone, records=current,
                                                  owned_id=owned_id))
        rows = target(current)
        if owned_id and current_zone == zone and exact(rows) and rows[0]['id'] == owned_id:
            persist('go-dns-rollback-intent.json', dict(zone=zone, record=rows[0]))
            # A second comparison protects against concurrent edits before owned-ID deletion.
            rollback_zone, rollback_records = api.current()
            if rollback_zone != zone or target(rollback_records) != rows:
                raise RuntimeError('go_dns_rollback_conflict_no_delete')
            api.request('DELETE', '/zones/' + zone + '/records/' + owned_id)
            final_zone, final_records = api.current()
            persist('go-dns-rollback-readback.json', dict(zone=final_zone, records=final_records))
            if final_zone != zone or target(final_records):
                raise RuntimeError('go_dns_rollback_unverified')
            print('owned_go_cname_rollback_verified=true')
        raise


if __name__ == '__main__':
    try:
        print(json.dumps(apply(Ionos(), persist_remote), sort_keys=True))
    except Exception as error:
        print('go_dns_operation_failed=' + (str(error) if isinstance(error, RuntimeError) else type(error).__name__))
        raise SystemExit(1)
