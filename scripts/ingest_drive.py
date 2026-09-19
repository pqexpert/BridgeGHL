"""Import a pinned private Drive export through BridgeGHL, without an LLM."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlsplit

import requests


def source_records(bundle, collections):
    if bundle.get('schema_version') != 'career-drive-export/1':
        raise ValueError('Unsupported export schema')
    pages = {p['url'].split('?')[0].rstrip('/').split('/')[-1].replace('-', ''): p
             for p in bundle.get('current_pursuit_pages', [])}
    seen = set()
    for collection in collections:
        source = bundle['collections'][collection]
        if not source['complete']:
            raise ValueError('Incomplete collection: ' + collection)
        for row in source['rows']:
            url = row.get('url', '')
            if not url.startswith('https://app.notion.com/p/'):
                raise ValueError('Missing stable source page URL')
            sid = url.rstrip('/').split('/')[-1].replace('-', '')
            seen.add(sid)
            if not re.fullmatch('[0-9a-f]{32}', sid):
                raise ValueError('Invalid source ID')
            title = row.get('Company') or row.get('Name') or row.get('Asset Name') or row.get('Skill') or row.get('Recommendation') or sid
            if collection == 'job_applications':
                title += ' — ' + row.get('Role', '')
            yield {'domain': 'income', 'source_id': sid, 'source_url': url,
                   'kind': 'opportunity' if collection == 'job_applications' else ('contact' if collection == 'relationships' else 'context'),
                   'title': title[:500], 'properties': row, 'content': pages.get(sid, {}).get('text', ''),
                   'contact_email': (row.get('Contact Email') if collection == 'job_applications' else row.get('Email') if collection == 'relationships' else None) or None,
                   'contact_name': (row.get('Contact Name') if collection == 'job_applications' else row.get('Name') if collection == 'relationships' else None) or None,
                   'company': (row.get('Company') or row.get('Organization')) if collection in ('job_applications', 'relationships') else None}
    if 'assets' in collections:
        for sid, page in pages.items():
            if sid not in seen:
                yield {'domain': 'income', 'source_id': sid, 'source_url': page['url'].split('?')[0],
                       'kind': 'context', 'title': page.get('title', sid)[:500], 'content': page.get('text', ''),
                       'properties': {'drive_assets': bundle.get('current_drive_assets', []),
                                      'principal_direction': bundle.get('principal_direction')}}


def load_bundle(args):
    if args.file:
        raw = Path(args.file).read_bytes()
    else:
        if not re.fullmatch(r'[A-Za-z0-9_-]+', args.drive_file_id):
            raise ValueError('Invalid Drive file ID')
        token = os.getenv('GOOGLE_DRIVE_ACCESS_TOKEN')
        if not token:
            raise ValueError('GOOGLE_DRIVE_ACCESS_TOKEN is missing from the protected runtime')
        response = requests.get('https://www.googleapis.com/drive/v3/files/' + args.drive_file_id,
                                params={'alt': 'media'}, headers={'Authorization': 'Bearer ' + token},
                                timeout=30, stream=True, allow_redirects=False)
        if response.status_code != 200:
            raise ValueError('Drive returned HTTP ' + str(response.status_code))
        parts, size = [], 0
        for chunk in response.iter_content(65536):
            size += len(chunk)
            if size > 5_000_000: raise ValueError('Export exceeds 5 MB bound')
            parts.append(chunk)
        raw = b''.join(parts)
    if len(raw) > 5_000_000 or hashlib.sha256(raw).hexdigest() != args.sha256:
        raise ValueError('Export size/hash mismatch')
    return json.loads(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--file')
    source.add_argument('--drive-file-id')
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--collections', default='job_applications,relationships,assets,experience,skills,recommendations')
    parser.add_argument('--source-id')
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--receipt', required=True)
    args = parser.parse_args()
    records = list(source_records(load_bundle(args), args.collections.split(',')))
    if args.source_id: records = [r for r in records if r['source_id'] == args.source_id.replace('-', '')]
    if not records: raise ValueError('No matching records')
    base = os.getenv('BRIDGE_URL', 'http://127.0.0.1:8000').rstrip('/')
    u = urlsplit(base)
    if u.scheme != 'https' and not (u.scheme == 'http' and u.hostname in ('127.0.0.1', 'localhost')):
        raise ValueError('Bridge URL requires HTTPS outside localhost')
    key = os.environ['BRIDGE_API_KEY']
    receipts = []
    path = Path(args.receipt)
    path.parent.mkdir(parents=True, exist_ok=True)
    for record in records:
        response = requests.post(base + '/dry-run/ingest/source-record', json=record, headers={'X-API-Key': key}, timeout=40)
        result = response.json()
        if response.status_code == 200 and result.get('accepted') and args.execute:
            response = requests.post(base + '/execute/ingest/source-record', json=record, headers={'X-API-Key': key}, timeout=120)
            result = response.json()
        receipts.append({'source_id': record['source_id'], 'http_status': response.status_code, 'result': result})
        # Save incrementally, privately. No personal data is printed in CI output.
        fd = os.open(str(path), os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        with os.fdopen(fd, 'w') as f: json.dump(receipts, f, indent=2)
        if response.status_code != 200 or not (result.get('verified') if args.execute else result.get('accepted')):
            detail=result.get('detail',{})
            safe={k:detail[k] for k in ('error','provider_status','operation','resource','reason_category','bridge_state') if isinstance(detail,dict) and k in detail}
            print(json.dumps({'state': 'BLOCKED', 'processed': len(receipts), 'total': len(records), 'http_status': response.status_code,'detail':safe}))
            return 1
        if len(receipts) % 25 == 0:
            print(json.dumps({'state': 'PROGRESS', 'processed': len(receipts), 'total': len(records)}), flush=True)
    print(json.dumps({'state': 'VERIFIED' if args.execute else 'DRY_RUN', 'records': len(receipts)}))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, KeyError, requests.RequestException) as exc:
        # Exceptions may contain provider URLs or credentials; print only their class.
        print(json.dumps({'state': 'BLOCKED', 'error_type': type(exc).__name__}))
        sys.exit(1)
