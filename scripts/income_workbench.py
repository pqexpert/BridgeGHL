"""On-demand Income review queue from verified HighLevel sources. GET-only, no LLM."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ingestion import assemble, read_income_source, source_key, verified_notes
from scripts.income_backlog import discovery_manifest, role_url


def preference(properties):
    """Order explicit source labels, never infer work authorization or flexibility."""
    remote = str(properties.get('Work Mode', '')).strip().lower() == 'remote'
    contract = str(properties.get('Type', '')).strip().lower() in {'contract', 'freelance', 'contractor'}
    return 0 if remote and contract else 1 if remote else 2 if contract else 3


def build(limit, *, location_id, pipeline_id, default_contact_id, journal_path,
          fields, call, bindings=None):
    if type(limit) is not int or not 1 <= limit <= 12:
        raise ValueError('workbench_limit')
    if not all((location_id, pipeline_id, default_contact_id)):
        raise ValueError('workbench_binding')
    rows = discovery_manifest()[0] if bindings is None else bindings
    cache = {}
    def get(method, path, **kwargs):
        if method != 'GET':
            raise ValueError('workbench_read_only')
        # Only shared contact/list reads are cached. Exact note and opportunity
        # readback remain fresh, including the existing source reader's CAS.
        cacheable = path in ('/contacts/'+default_contact_id, '/contacts/'+default_contact_id+'/notes')
        if cacheable and path in cache:
            return cache[path]
        result = call(method, path, **kwargs)
        if cacheable:
            cache[path] = result
        return result
    contact = get('GET', '/contacts/'+default_contact_id).get('contact', {})
    if contact.get('id') != default_contact_id or contact.get('locationId') != location_id:
        raise ValueError('workbench_contact_scope')
    notes = get('GET', '/contacts/'+default_contact_id+'/notes').get('notes')
    if not isinstance(notes, list) or len(notes) > 10000:
        raise ValueError('workbench_notes_unbounded')
    verified = verified_notes(journal_path, default_contact_id)
    candidates, excluded = [], Counter()
    for sid, _, binding in rows:
        if binding['contact_id'] != default_contact_id:
            excluded['non_internal_context'] += 1
            continue
        versions = assemble(notes, source_key(sid), default_contact_id, verified)
        if len(versions) != 1:
            excluded['source_missing_or_ambiguous'] += 1
            continue
        digest, version = next(iter(versions.items()))
        candidates.append((preference(version['source']['properties']), sid, binding, digest))
    entries = []
    # This is a bounded review sample, not an autonomous work admission or a
    # claim to have checked every native opportunity or employer posting.
    for rank, sid, binding, digest in sorted(candidates)[:limit]:
        current = read_income_source({**binding, 'source_id': sid, 'current_source_sha256': digest},
            location_id, pipeline_id, default_contact_id, journal_path, get)
        native = get('GET', '/opportunities/'+binding['opportunity_id']).get('opportunity', {})
        if (native.get('id') != binding['opportunity_id'] or native.get('locationId') != location_id
            or native.get('pipelineId') != pipeline_id or native.get('contactId') != default_contact_id
            or not native.get('name', '').endswith('[src:'+source_key(sid)+']')
            or native.get('updatedAt') != current['opportunity_updated']):
            raise ValueError('workbench_native_changed')
        if native.get('status') != 'open':
            excluded['native_not_open'] += 1
            continue
        custom = {f['id']: f.get('fieldValue', f.get('field_value', f.get('value')))
                  for f in native.get('customFields', [])}
        def field(label):
            return custom.get(fields.get('opportunity:Ecosystem '+label)) or 'unknown'
        p = current['source']['properties']
        next_action, follow_up = field('Next Action'), field('Next Follow-Up Date')
        role = role_url(p)
        employer_url = (('https://jobs.ashbyhq.com/' if role[0] == 'ashby' else 'https://jobs.lever.co/')
                        +role[1]+'/'+role[2]) if role else None
        entries.append({'source_id': sid, 'source_sha256': digest,
            'opportunity_id': binding['opportunity_id'], 'title': current['source']['title'],
            'source_url': current['source']['source_url'], 'native_stage_id': native.get('pipelineStageId'),
            'crm_url': 'https://app.gohighlevel.com/v2/location/'+location_id+'/opportunities/list',
            'employer_role_url': employer_url,
            'assigned_to': native.get('assignedTo') or 'unknown', 'next_action': next_action,
            'follow_up': follow_up, 'source_work_mode': p.get('Work Mode') or 'unknown',
            'source_type': p.get('Type') or 'unknown', 'source_geography_fit': p.get('Geography Fit') or 'unknown',
            'preference_band': ('remote_contract', 'remote', 'contract', 'other_or_unknown')[rank],
            'gaps': [key for key, value in {'owner': native.get('assignedTo'),
                'next_action': None if next_action == 'unknown' else next_action,
                'follow_up': None if follow_up == 'unknown' else follow_up}.items() if not value],
            'native_readback': True, 'employer_posting_revalidated': False,
            'execution_admitted': False})
    return {'schema': 'income-workbench/1', 'observed_at': datetime.now(timezone.utc).isoformat(),
        'discovery_records': len(rows), 'sampled': min(limit, len(candidates)),
        'excluded': dict(excluded), 'entries': entries, 'coverage': 'bounded_source_label_ranked_sample',
        'ranking': 'explicit source remote+contract, remote, contract, other/unknown; not qualification',
        'provider_mutations': 0, 'model_calls': 0, 'messages_sent': 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--limit', type=int, default=6)
    args = parser.parse_args()
    import app
    from ingestion import configuration
    from provider_budget import actor_scope
    config = configuration('income')
    journal = Path(app.AUDIT_LOG_PATH).parent/'ingestion.sqlite3'
    setup = json.loads(Path('/var/lib/bridgeghl/ecosystem/receipt.json').read_text())
    if setup.get('verified') is not True or setup.get('location_id') != app.HIGHLEVEL_LOCATION_ID:
        raise ValueError('workbench_configuration_unverified')
    def call(method, path):
        status, data = app.highlevel_request(method, app.HIGHLEVEL_BASE_URL.rstrip('/')+path)
        if not 200 <= status < 300:
            raise ValueError('workbench_provider_read_failed')
        return data
    with actor_scope('income', journal):
        report = build(args.limit, location_id=app.HIGHLEVEL_LOCATION_ID,
            pipeline_id=config['pipeline_id'], default_contact_id=config['default_contact_id'],
            journal_path=journal, fields=setup['fields'], call=call)
    # Full output belongs in an existing private operator artifact, never CI logs.
    print(json.dumps(report, sort_keys=True))


if __name__ == '__main__':
    main()
