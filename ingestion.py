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
from pathlib import Path

from fastapi import Header, HTTPException
from pydantic import BaseModel, Field


class SourceReadError(ValueError):
    pass

def fail(condition, reason):
    if not condition: raise SourceReadError(reason)

def source_key(source_id):
    return hashlib.sha256(("income:"+source_id).encode()).hexdigest()[:32]

def verified_notes(journal_path, contact_id):
    path = Path(journal_path)
    fail(path.is_file() and not path.is_symlink(), "verified_journal_unavailable")
    # No table creation, locks, journal updates or write-capable connection.
    with sqlite3.connect("file:"+quote(str(path.resolve()),safe="/")+"?mode=ro", uri=True) as db:
        return {key:native_id for key,state,native_id in db.execute(
            "SELECT key,state,native_id FROM effects WHERE key LIKE ?", ("note:"+contact_id+":%",))
            if state == "verified" and native_id}

def assemble(notes, key, contact_id, verified):
    groups = {}
    for note in notes:
        body = note.get("body", "")
        if not isinstance(body, str) or len(body) > 13000: continue
        header, sep, part = body.partition("\n")
        match = re.fullmatch(r"bridge-source:"+re.escape(key)+r":([a-f0-9]{64}):([0-9]+)/([0-9]+)",header)
        if not sep or not match: continue
        digest, index, total = match.group(1), int(match.group(2)), int(match.group(3))
        journal_key = "note:"+contact_id+":"+hashlib.sha256(body.encode()).hexdigest()
        if verified.get(journal_key) != note.get("id"): continue
        fail(1 <= index <= total <= 21, "source_parts_invalid")
        group = groups.setdefault(digest, {"total":total,"parts":{}})
        fail(group["total"] == total and index not in group["parts"], "source_parts_ambiguous")
        group["parts"][index] = (part, note["id"], body)
    complete = {}
    for digest, group in groups.items():
        if set(group["parts"]) != set(range(1,group["total"]+1)): continue
        raw = "".join(group["parts"][i][0] for i in range(1,group["total"]+1))
        fail(hashlib.sha256(raw.encode()).hexdigest() == digest, "source_digest_mismatch")
        data = json.loads(raw)
        fail(isinstance(data,dict) and isinstance(data.get("properties"),dict), "source_shape_invalid")
        complete[digest] = {"source":data,"parts":group["parts"]}
    return complete

def read_income_source(binding, location_id, pipeline_id, default_contact_id, journal_path, call):
    """GET-only exact source read. Explicit digest or a single verified version is required."""
    source_id, cid = binding["source_id"], binding["contact_id"]
    oid = binding.get("opportunity_id")
    for value in (cid, oid or cid):
        fail(isinstance(value,str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}",value), "native_identity_invalid")
    fail(isinstance(source_id,str) and 0 < len(source_id) <= 200, "source_identity_invalid")
    key = source_key(source_id)
    contact = call("GET","/contacts/"+cid).get("contact",{})
    fail(contact.get("id")==cid and contact.get("locationId")==location_id, "contact_scope_mismatch")
    opportunity = None
    if oid:
        opportunity = call("GET","/opportunities/"+oid).get("opportunity",{})
        fail(opportunity.get("id")==oid and opportunity.get("locationId")==location_id
             and opportunity.get("pipelineId")==pipeline_id and opportunity.get("contactId")==cid
             and opportunity.get("name","").endswith("[src:"+key+"]"), "opportunity_scope_mismatch")
    else:
        fail(cid == default_contact_id, "context_contact_scope_mismatch")
    verified = verified_notes(journal_path,cid)
    notes = call("GET","/contacts/"+cid+"/notes").get("notes")
    fail(isinstance(notes,list) and len(notes)<=10000, "source_notes_unavailable_or_unbounded")
    versions = assemble(notes,key,cid,verified)
    digest = binding.get("current_source_sha256")
    if digest is None:
        fail(len(versions)==1, "explicit_current_source_digest_required")
        digest = next(iter(versions))
    fail(isinstance(digest,str) and re.fullmatch(r"[a-f0-9]{64}",digest), "source_digest_invalid")
    fail(digest in versions, "selected_source_not_verified_complete")
    selected = versions[digest]
    # Native readback of each selected exact note, not list-response inference.
    for _,nid,body in selected["parts"].values():
        actual = call("GET","/contacts/"+cid+"/notes/"+nid).get("note",{})
        fail(actual.get("id")==nid and actual.get("body")==body, "source_note_changed")
    if oid:
        current = call("GET","/opportunities/"+oid).get("opportunity",{})
        fail(all(current.get(k)==opportunity.get(k) for k in
                 ("id","locationId","pipelineId","contactId","name","updatedAt","status","pipelineStageId")),
             "opportunity_changed")
    source = selected["source"]
    explicit = source["properties"].get("fabric_income_admission")
    fail(explicit is None or isinstance(explicit,dict), "admission_projection_invalid")
    if explicit is not None:
        fail(bool(oid), "opportunity_required_for_research_projection")
    return {"domain":"income","operational_system":"highlevel","source_id":source_id,
            "source_key":key,"source_sha256":digest,"source_version_count":len(versions),
            "source_versions":sorted(versions),"default_context_contact_id":default_contact_id,"location_id":location_id,"contact_id":cid,
            "opportunity_id":oid,"opportunity_updated":opportunity.get("updatedAt") if opportunity else None,
            "opportunity_status":opportunity.get("status") if opportunity else None,"note_ids":[x[1] for x in selected["parts"].values()],
            "native_readback":True,"source":source,"admission_projection":explicit,
            "execution_admitted":False,"provider_mutations":0,"model_calls":0,
            "legacy_provenance_urls":sorted({v["source"].get("source_url","") for v in versions.values()
                if isinstance(v["source"].get("source_url"),str)}),
            "identity_rule":"Preserve original source ID and verified completion/cache aliases; operational system is HighLevel."}


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


class IncomeSourceRead(BaseModel):
    source_id: str = Field(min_length=1, max_length=200)
    contact_id: str = Field(min_length=1, max_length=128)
    opportunity_id: Optional[str] = Field(default=None, max_length=128)
    current_source_sha256: Optional[str] = Field(default=None, min_length=64, max_length=64)

    class Config:
        extra = 'forbid'


class IncomeBacklogRead(BaseModel):
    cursor: int = Field(default=0,ge=0)
    limit: int = Field(default=8,ge=1,le=12)
    class Config:
        extra = "forbid"


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
        except HTTPException:
            raise
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
            # Only a definitive provider rejection can reopen this key. A
            # structured unknown may have completed and must stay pending even
            # when the provider status happened to be a 4xx.
            detail = exc.detail if isinstance(exc.detail, dict) else {}
            if (detail.get('error') == 'provider_rejected'
                    and detail.get('provider_status') in (400,401,403,404,422)):
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

    def execute_record(record: IngestRecord, x_api_key: Optional[str] = Header(default=None)):
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

    def read_income_record(record: IncomeSourceRead, x_api_key: Optional[str] = Header(default=None)):
        bridge.require_api_key(x_api_key)
        cfg = configuration('income')
        if not cfg['pipeline_id'] or not cfg['default_contact_id']:
            raise HTTPException(409, 'Income source binding unavailable')
        path = os.path.join(os.path.dirname(bridge.AUDIT_LOG_PATH), 'ingestion.sqlite3')
        try:
            return read_income_source(record.model_dump(exclude_none=True), bridge.HIGHLEVEL_LOCATION_ID,
                cfg['pipeline_id'], cfg['default_contact_id'], path, call)
        except SourceReadError as exc:
            raise HTTPException(409, str(exc)) from None
        except (OSError, sqlite3.Error, json.JSONDecodeError):
            raise HTTPException(409, "income_source_read_unavailable") from None

    def execute(record: IngestRecord, x_api_key: Optional[str] = Header(default=None)):
        from provider_budget import actor_scope
        if os.getenv('HIGHLEVEL_INGEST_ENABLED', '').lower() != 'true':
            return execute_record(record, x_api_key)
        path = os.path.join(os.path.dirname(bridge.AUDIT_LOG_PATH), 'ingestion.sqlite3')
        with actor_scope(record.domain, path):
            return execute_record(record, x_api_key)

    def read_income(record: IncomeSourceRead, x_api_key: Optional[str] = Header(default=None)):
        from provider_budget import actor_scope
        path = os.path.join(os.path.dirname(bridge.AUDIT_LOG_PATH), 'ingestion.sqlite3')
        with actor_scope('income', path):
            return read_income_record(record, x_api_key)

    def read_backlog(record: IncomeBacklogRead, x_api_key: Optional[str] = Header(default=None)):
        bridge.require_api_key(x_api_key)
        from scripts.income_backlog import read_backlog as reader
        from provider_budget import actor_scope
        cfg=configuration('income')
        path=os.path.join(os.path.dirname(bridge.AUDIT_LOG_PATH),'ingestion.sqlite3')
        if not all(cfg.values()):raise HTTPException(409,'income_backlog_binding_unavailable')
        try:
            with actor_scope('income',path):
                return reader(record.cursor,record.limit,location_id=bridge.HIGHLEVEL_LOCATION_ID,
                    pipeline_id=cfg['pipeline_id'],stage_id=cfg['stage_id'],
                    default_contact_id=cfg['default_contact_id'],journal_path=path,call=call)
        except PermissionError:
            raise HTTPException(409,'income_backlog_permission_denied') from None
        except FileNotFoundError:
            raise HTTPException(409,'income_backlog_locator_missing') from None
        except sqlite3.Error:
            raise HTTPException(409,'income_backlog_journal_unavailable') from None
        except (OSError,ValueError,KeyError,TypeError):
            raise HTTPException(409,'income_backlog_source_unavailable') from None

    bridge.app.post('/read/ingest/income-backlog')(read_backlog)
    bridge.app.post('/read/ingest/income-source')(read_income)
    bridge.app.post('/dry-run/ingest/source-record')(dry_run)
    bridge.app.post('/execute/ingest/source-record')(execute)

    # Internal root projector uses this same writer under a validated actor scope.
    # No HTTP route accepts an alternate actor or expands CRM domains.
    return {'execute': execute_record, 'dry_run': dry_run}
