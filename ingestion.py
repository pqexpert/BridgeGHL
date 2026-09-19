"""Bounded, source-keyed CRM ingestion; no enrichment or messaging dependency."""
import hashlib
import json
import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Literal, Optional
from urllib.parse import quote

from fastapi import Header, HTTPException
from pydantic import BaseModel, Field


class IngestRecord(BaseModel):
    domain: Literal['income', 'rsc']
    source_id: str = Field(min_length=1, max_length=200)
    source_url: str = Field(min_length=1, max_length=2000)
    kind: Literal['contact', 'opportunity', 'context']
    title: str = Field(min_length=1, max_length=500)
    properties: dict = Field(default_factory=dict)
    content: str = Field(default='', max_length=200000)
    contact_email: Optional[str] = None
    contact_name: Optional[str] = None
    company: Optional[str] = None

    class Config:
        extra = 'forbid'


def record_key(record):
    return hashlib.sha256((record.domain + ':' + record.source_id).encode()).hexdigest()[:32]


def note_bodies(record):
    content = json.dumps({'title': record.title, 'source_url': record.source_url,
                         'properties': record.properties, 'content': record.content},
                        sort_keys=True, ensure_ascii=False, indent=2)
    if len(content) > 250000:
        raise HTTPException(422, 'Source record exceeds 250000 characters; no truncation performed')
    digest = hashlib.sha256(content.encode()).hexdigest()
    parts = [content[i:i+12000] for i in range(0, len(content), 12000)]
    return [f'bridge-source:{record_key(record)}:{digest}:{i+1}/{len(parts)}\n{p}'
            for i, p in enumerate(parts)]


@contextmanager
def journal(path):
    import fcntl
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    # Serialize all bridge ingestion processes, including multiple Uvicorn workers.
    with open(path + '.lock', 'a') as lock:
        os.chmod(path + '.lock', 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX)
        db = sqlite3.connect(path)
        os.chmod(path, 0o600)
        db.execute('CREATE TABLE IF NOT EXISTS effects (key TEXT PRIMARY KEY, state TEXT, native_id TEXT)')
        db.commit()
        try:
            yield db
        finally:
            db.close()


def configuration(domain):
    prefix = 'HIGHLEVEL_INGEST_' + domain.upper() + '_'
    return {k: os.getenv(prefix + k.upper(), '') for k in
            ('pipeline_id', 'stage_id', 'default_contact_id')}


def register_routes(bridge):
    def call(method, path, **kwargs):
        try:
            status, data = bridge.highlevel_request(method, bridge.HIGHLEVEL_BASE_URL.rstrip('/') + path, **kwargs)
        except Exception:
            raise HTTPException(502, 'Provider transport failed; inspect native state before retry')
        if not 200 <= status < 300:
            message=str(data.get('message') or data.get('error') or data.get('detail') or '').lower()
            kind=next((k for k in ('duplicate','scope','version','required','invalid','not found','limit') if k in message),'other')
            raise HTTPException(502, {'error': 'provider_rejected', 'provider_status': status,
                'operation':method, 'resource':path.split('/')[1], 'reason_category':kind})
        return data

    def check_contact(cid):
        if not re.fullmatch(r'[A-Za-z0-9_-]+', cid):
            raise HTTPException(422, 'Invalid contact id')
        contact = call('GET', '/contacts/' + cid).get('contact', {})
        if contact.get('id') != cid or contact.get('locationId') != bridge.HIGHLEVEL_LOCATION_ID:
            raise HTTPException(409, 'Contact identity/location mismatch')
        return contact

    def effect(db, key, find, create, verify):
        prior = db.execute('SELECT state,native_id FROM effects WHERE key=?', (key,)).fetchone()
        existing = find()
        if existing:
            verify(existing)
            db.execute('INSERT OR REPLACE INTO effects VALUES (?,?,?)', (key, 'verified', existing))
            db.commit()
            return existing
        if prior and prior[0] != 'rejected':
            raise HTTPException(409, 'Prior effect is absent or ambiguous; refusing duplicate creation')
        db.execute('INSERT OR REPLACE INTO effects VALUES (?,?,?)', (key, 'pending', None))
        db.commit()  # Persist intent BEFORE provider mutation, including crash/timeout cases.
        try:
            native_id = create()
        except HTTPException as exc:
            if isinstance(exc.detail,dict) and exc.detail.get('provider_status') in (400,401,403,404,422):
                db.execute('UPDATE effects SET state=? WHERE key=?',('rejected',key));db.commit()
            raise
        db.execute('UPDATE effects SET native_id=? WHERE key=?', (native_id, key))
        db.commit()
        verify(native_id)
        db.execute('UPDATE effects SET state=? WHERE key=?', ('verified', key))
        db.commit()
        return native_id

    def validate(record):
        config = configuration(record.domain)
        missing = []
        if record.contact_email:
            if not re.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+', record.contact_email):
                raise HTTPException(422, 'A valid source email is required; inferred emails are not accepted')
        elif not config['default_contact_id']:
            missing.append('default_contact_id')
        if record.kind == 'opportunity':
            missing += [k for k in ('pipeline_id', 'stage_id') if not config[k]]
        bodies = note_bodies(record)
        return config, missing, bodies

    def dry_run(record: IngestRecord, x_api_key: Optional[str] = Header(default=None)):
        bridge.require_api_key(x_api_key)
        config, missing, bodies = validate(record)
        return {'accepted': not missing, 'mode': 'dry_run', 'source_key': record_key(record),
                'missing_configuration': missing, 'note_parts': len(bodies),
                'contact_effect': 'find_or_create_by_exact_email' if record.contact_email else 'existing_default_contact',
                'opportunity_effect': 'find_or_create_by_source_key' if record.kind == 'opportunity' else 'none',
                'preserve_existing_contact_fields': True, 'preserve_existing_opportunity_stage': True,
                'rollback': 'New records/notes are journaled by native ID; no automatic deletion.',
                'messaging': False}

    def execute(record: IngestRecord, x_api_key: Optional[str] = Header(default=None)):
        bridge.require_api_key(x_api_key)
        if os.getenv('HIGHLEVEL_INGEST_ENABLED', '').lower() != 'true':
            raise HTTPException(409, 'HIGHLEVEL_INGEST_ENABLED is not true')
        config, missing, bodies = validate(record)
        if missing:
            raise HTTPException(409, {'missing_configuration': missing})
        # Reuse the bridge's authentication, health and audit boundary.
        health = bridge.bridge_health_snapshot()
        if health.state != 'HEALTHY':
            raise HTTPException(409, {'bridge_state': health.state})
        key = record_key(record)
        if record.kind == 'opportunity':
            pipelines = call('GET', '/opportunities/pipelines', params={'locationId': bridge.HIGHLEVEL_LOCATION_ID}).get('pipelines', [])
            pipeline = next((p for p in pipelines if p.get('id') == config['pipeline_id']), None)
            if not pipeline or not any(s.get('id') == config['stage_id'] for s in pipeline.get('stages', [])):
                raise HTTPException(409, 'Configured pipeline/stage does not exist in this location')
        bridge.append_audit_log({'action': 'ingest_source_record', 'source_key': key, 'result': 'intent',
                                 'ts': datetime.now(timezone.utc).isoformat()})
        path = os.path.join(os.path.dirname(bridge.AUDIT_LOG_PATH), 'ingestion.sqlite3')
        with journal(path) as db:
            if record.contact_email:
                email = record.contact_email.strip().lower()
                def find_contact():
                    result = call('GET', '/contacts/search/duplicate', params={'locationId': bridge.HIGHLEVEL_LOCATION_ID, 'email': email})
                    contact = result.get('contact') or {}
                    if contact and str(contact.get('email', '')).lower() != email:
                        raise HTTPException(409, 'Contact search returned a different email')
                    return contact.get('id')
                def create_contact():
                    body = {'locationId': bridge.HIGHLEVEL_LOCATION_ID, 'email': email, 'dnd': True,
                            'source': 'BridgeGHL ' + record.domain + ' source import'}
                    if record.contact_name: body['name'] = record.contact_name
                    if record.company: body['companyName'] = record.company
                    return call('POST', '/contacts/', body=body).get('contact', {}).get('id')
                def verify_contact(cid):
                    contact = check_contact(cid or '')
                    if str(contact.get('email', '')).lower() != email:
                        raise HTTPException(409, 'Contact email readback mismatch')
                cid = effect(db, 'contact:' + hashlib.sha256((bridge.HIGHLEVEL_LOCATION_ID + email).encode()).hexdigest(),
                             find_contact, create_contact, verify_contact)
            else:
                cid = config['default_contact_id']
                check_contact(cid)
            oid = None
            if record.kind == 'opportunity':
                marker = '[src:' + key + ']'
                name = record.title[:160] + ' ' + marker
                def find_opp():
                    result = call('GET', '/opportunities/search', params={'locationId': bridge.HIGHLEVEL_LOCATION_ID,
                                  'pipelineId': config['pipeline_id'], 'q': marker, 'limit': 100, 'status': 'all'})
                    matches = [o for o in result.get('opportunities', []) if o.get('name', '').endswith(marker)]
                    if len(matches) > 1 or len(result.get('opportunities', [])) >= 100:
                        raise HTTPException(409, 'Opportunity search is ambiguous')
                    return matches[0]['id'] if matches else None
                def verify_opp(oid):
                    o = call('GET', '/opportunities/' + quote(oid or '', safe='')).get('opportunity', {})
                    contact_id = o.get('contactId') or (o.get('contact') or {}).get('id')
                    if (not o.get('name', '').endswith(marker) or o.get('pipelineId') != config['pipeline_id']
                            or contact_id != cid or o.get('locationId') != bridge.HIGHLEVEL_LOCATION_ID):
                        raise HTTPException(409, 'Opportunity identity readback mismatch')
                oid = effect(db, 'opportunity:' + key, find_opp,
                             lambda: call('POST', '/opportunities/', body={'locationId': bridge.HIGHLEVEL_LOCATION_ID,
                              'pipelineId': config['pipeline_id'], 'pipelineStageId': config['stage_id'],
                              'name': name, 'status': 'open', 'contactId': cid}).get('opportunity', {}).get('id'), verify_opp)
            note_ids = []
            for body in bodies:
                def find_note():
                    notes = call('GET', '/contacts/' + cid + '/notes').get('notes', [])
                    matches = [n for n in notes if n.get('body') == body]
                    if len(matches) > 1: raise HTTPException(409, 'Duplicate source note requires reconciliation')
                    return matches[0]['id'] if matches else None
                def verify_note(nid):
                    n = call('GET', '/contacts/' + cid + '/notes/' + quote(nid or '', safe='')).get('note', {})
                    if n.get('body') != body:
                        raise HTTPException(409, 'Note content readback mismatch')
                note_ids.append(effect(db, 'note:' + cid + ':' + hashlib.sha256(body.encode()).hexdigest(), find_note,
                    lambda: call('POST', '/contacts/' + cid + '/notes', body={'body': body}).get('note', {}).get('id'), verify_note))
        receipt = {'source_key': key, 'contact_id': cid, 'opportunity_id': oid, 'note_ids': note_ids, 'verified': True}
        bridge.append_audit_log({'action': 'ingest_source_record', 'result': 'verified', **receipt,
                                'ts': datetime.now(timezone.utc).isoformat()})
        return receipt

    bridge.app.post('/dry-run/ingest/source-record')(dry_run)
    bridge.app.post('/execute/ingest/source-record')(execute)
