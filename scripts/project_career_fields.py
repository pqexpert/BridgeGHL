"""Fill empty ecosystem fields on already verified career opportunities only."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import configure_ecosystem as config
from scripts.ingest_drive import source_records
from ingestion import IngestRecord, journal, record_key

VERSION = 'career-field-projection/1'
SOURCE_SHA = 'f8b27811b7d431c40a6dd3b41ddce272f6f1c7dccce538c6812eea6269320ccc'
SOURCE = Path('/var/lib/bridgeghl/career-import')


def values(record, observed):
    p = record['properties']
    missing = [label for label, key in [('status', 'Status'), ('next-action', 'Next Action'),
                                      ('follow-up-date', 'date:Follow-Up Date:start')]
               if not p.get(key)]
    result = {'Mission Domain': 'income', 'Source Record ID': record['source_id'],
              'Source URL': record['source_url'], 'Source Observed At': observed,
              'Record Mode': 'live', 'Owning Mission': 'Income Accelerator',
              'Evidence Status': 'source snapshot; current state requires verification',
              'Data Quality': 'needs-enrichment: ' + ', '.join(missing) if missing else 'source fields present; verify before action',
              'Original Source Status': p.get('Status') or 'unknown',
              'Next Action': p.get('Next Action') or 'Review source and establish the next action',
              'Priority': p.get('Priority') or 'unknown'}
    if p.get('date:Follow-Up Date:start'):
        result['Next Follow-Up Date'] = p['date:Follow-Up Date:start']
    for key in ('Drive Folder Link', 'Asset Link'):
        url = p.get(key)
        if isinstance(url, str) and url.startswith(('https://drive.google.com/', 'https://docs.google.com/')):
            result['Drive Asset URL'] = url
            break
    return {'Ecosystem ' + k: str(v) for k, v in result.items()}


def existing_values(native):
    return {f['id']: f.get('fieldValue', f.get('field_value', f.get('value'))) for f in native.get('customFields', [])}


def missing_fields(native, desired):
    current = existing_values(native)
    return {fid: value for fid, value in desired.items() if current.get(fid) in (None, '', [])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    config.require(config.bridge.HIGHLEVEL_LOCATION_ID == config.LOCATION, 'Location mismatch')
    config.require(config.bridge.bridge_health_snapshot().state == 'HEALTHY', 'Bridge health gate failed')
    raw = (SOURCE / 'source.json').read_bytes()
    config.require(hashlib.sha256(raw).hexdigest() == SOURCE_SHA, 'Pinned source differs')
    bundle = json.loads(raw)
    setup = json.loads((config.ROOT / 'receipt.json').read_text())
    config.require(setup.get('verified') and setup['location_id'] == config.LOCATION, 'Configuration canary not verified')
    workflows = config.call('GET', '/workflows/', params={'locationId': config.LOCATION}).get('workflows', [])
    config.require(all(w.get('status') == 'draft' for w in workflows), 'Published workflow requires trigger review')
    receipts = json.loads((SOURCE / 'receipt.json').read_text())
    by_source = {r['source_id']: r['result'] for r in receipts if r.get('result', {}).get('verified')}
    records = list(source_records(bundle, ['job_applications']))
    config.require(len(records) == 176, 'Unexpected source opportunity count')
    print(json.dumps({'phase': 'dry_run', 'projection': VERSION, 'records': len(records),
                      'source_sha256': SOURCE_SHA, 'destination': config.LOCATION,
                      'scope': 'fill empty ecosystem fields only', 'native_stage_owner_changes': False}), flush=True)
    if not args.execute:
        return
    # Prove the v3 custom-field update shape on synthetic opportunities first.
    test_field = setup['fields']['opportunity:Ecosystem Record Mode']
    for test_id in setup['canary']['opportunity_ids']:
        test_path = '/opportunities/' + test_id
        test_native = config.call('GET', test_path)['opportunity']
        config.require(test_native.get('locationId') == config.LOCATION
                       and test_native.get('name', '').startswith('TEST ONLY - ')
                       and test_native.get('contactId') == setup['canary']['contact_id'], 'Test identity mismatch')
        if existing_values(test_native).get(test_field) != 'test':
            config.bridge.append_audit_log({'action': VERSION, 'opportunity_id': test_id, 'result': 'test_intent'})
            config.call('PUT', test_path, body={'customFields': [{'id': test_field, 'fieldValue': 'test'}]})
        config.require(existing_values(config.call('GET', test_path)['opportunity']).get(test_field) == 'test',
                       'Custom-field test readback failed; real records untouched')
    result = {'version': VERSION, 'source_sha256': SOURCE_SHA, 'location_id': config.LOCATION, 'records': []}
    with journal(str(config.ROOT / 'projection.sqlite3')) as db:
        for record in records:
            source_id = record['source_id']
            receipt = by_source.get(source_id, {})
            oid = receipt.get('opportunity_id')
            config.require(bool(oid), 'Missing verified source binding')
            path = '/opportunities/' + oid
            native = config.call('GET', path)['opportunity']
            marker = '[src:' + record_key(IngestRecord(**record)) + ']'
            config.require(native.get('locationId') == config.LOCATION and native.get('name', '').endswith(marker)
                           and native.get('contactId') == receipt['contact_id'], 'Native identity mismatch')
            desired = {setup['fields']['opportunity:' + name]: value for name, value in values(record, bundle['exported_at']).items()}
            delta = missing_fields(native, desired)
            key = VERSION + ':' + SOURCE_SHA + ':' + config.LOCATION + ':' + oid
            prior = db.execute('SELECT state,native_id FROM effects WHERE key=?', (key,)).fetchone()
            if delta and prior:
                raise RuntimeError('Prior projection differs; reconcile without replay')
            if delta:
                # Store original native fields privately before effect. Never log source PII.
                config.persist('before-' + oid + '.json', native)
                db.execute('INSERT INTO effects VALUES (?,?,?)', (key, 'pending', oid)); db.commit()
                config.bridge.append_audit_log({'action': VERSION, 'source_key': record_key(IngestRecord(**record)),
                                                'opportunity_id': oid, 'result': 'intent', 'fields': list(delta)})
                config.call('PUT', path, body={'customFields': [{'id': fid, 'fieldValue': value} for fid, value in delta.items()]})
                readback = config.call('GET', path)['opportunity']
                actual = existing_values(readback)
                config.require(all(actual.get(fid) == value for fid, value in delta.items()), 'Projected field readback mismatch')
                for protected in ('name', 'status', 'pipelineId', 'pipelineStageId', 'contactId', 'assignedTo', 'monetaryValue'):
                    config.require(native.get(protected) == readback.get(protected), 'Protected native field changed')
                for fid, value in existing_values(native).items():
                    if fid not in delta:
                        config.require(actual.get(fid) == value, 'Existing custom field changed')
                db.execute('UPDATE effects SET state=? WHERE key=?', ('verified', key)); db.commit()
            result['records'].append({'source_id': source_id, 'opportunity_id': oid, 'fields_added': len(delta), 'verified': True})
            config.persist('projection-receipt.json', result)
            if len(result['records']) % 25 == 0:
                print(json.dumps({'verified_opportunities': len(result['records'])}), flush=True)
    print(json.dumps({'phase': 'verified', 'opportunities': len(result['records']), 'messages_sent': 0}), flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'state': 'BLOCKED', 'error_type': type(exc).__name__,
                          'detail': str(exc)[:160] if isinstance(exc, RuntimeError) else 'Inspect private runtime receipt'}), flush=True)
        sys.exit(1)
