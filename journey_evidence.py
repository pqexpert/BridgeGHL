"""Contact-scoped, metadata-only evidence. No CRM writes or message content."""
from datetime import date, datetime, timedelta, timezone
from typing import Literal
from fastapi import Header, HTTPException
from pydantic import BaseModel, Field, model_validator
import requests

ID = r'^[A-Za-z0-9_-]{1,100}$'
class EvidenceQuery(BaseModel):
    contact_id: str = Field(pattern=ID)
    resource: Literal['conversations', 'messages', 'tasks', 'submissions']
    conversation_id: str | None = Field(default=None, pattern=ID)
    limit: int = Field(default=20, ge=1, le=50)
    cursor: str | None = Field(default=None, max_length=100, pattern=r'^[A-Za-z0-9_:T.+-]+$')
    page: int = Field(default=1, ge=1, le=100)
    start_date: date | None = None
    end_date: date | None = None

    @model_validator(mode='after')
    def bounded(self):
        if self.resource == 'messages' and not self.conversation_id:
            raise ValueError('messages requires conversation_id')
        if self.resource == 'submissions':
            self.end_date = self.end_date or datetime.now(timezone.utc).date()
            self.start_date = self.start_date or self.end_date - timedelta(days=30)
            if not 0 <= (self.end_date - self.start_date).days <= 31:
                raise ValueError('submission window must be at most 31 days')
        return self

FIELDS = {
    'conversations': ('id', 'contactId', 'locationId', 'lastMessageDate', 'lastMessageType', 'lastMessageDirection', 'unreadCount', 'assignedTo'),
    'messages': ('id', 'contactId', 'locationId', 'conversationId', 'dateAdded', 'direction', 'status', 'type', 'messageType', 'source', 'userId'),
    'tasks': ('id', 'contactId', 'assignedTo', 'dueDate', 'completed'),
    'submissions': ('id', 'contactId', 'createdAt', 'formId'),
}

def failure(error, status=502):
    raise HTTPException(status_code=status, detail={'error': error})

def read_evidence(bridge, query):
    """Every read validates parent identity before admitting a child resource."""
    if not bridge.HIGHLEVEL_LOCATION_ID or not bridge.HIGHLEVEL_PIT:
        failure('provider_binding_missing', 503)
    def get(path, params=None):
        try:
            status, data = bridge.highlevel_request('GET', bridge.HIGHLEVEL_BASE_URL.rstrip('/') + path, params=params, timeout=6, retry_reads=False)
        except requests.RequestException:
            failure('provider_unavailable', 503)
        if status in (401, 403):
            failure('provider_scope_or_auth_rejected', 424)
        if not 200 <= status < 300:
            failure('provider_read_failed', 502)
        if not isinstance(data, dict):
            failure('provider_resource_type_mismatch')
        return data
    contact = get('/contacts/' + query.contact_id).get('contact')
    if not isinstance(contact, dict) or contact.get('id') != query.contact_id:
        failure('contact_identity_mismatch')
    if contact.get('locationId') != bridge.HIGHLEVEL_LOCATION_ID:
        failure('location_mismatch', 403)
    resource = query.resource
    pagination = {'limit': query.limit, 'complete': False}
    if resource == 'conversations':
        params = {'locationId': bridge.HIGHLEVEL_LOCATION_ID, 'contactId': query.contact_id, 'limit': query.limit, 'sort': 'desc'}
        if query.cursor: params['startAfterDate'] = query.cursor
        data = get('/conversations/search', params)
        rows = data.get('conversations')
        total = data.get('total')
        if type(total) is not int or total < 0: failure('provider_pagination_mismatch')
        pagination.update(total=total, complete=query.cursor is None and isinstance(rows, list) and len(rows) >= total)
        if isinstance(rows, list) and rows:
            pagination['next_cursor'] = rows[-1].get('lastMessageDate') if isinstance(rows[-1], dict) else None
    elif resource == 'messages':
        conversation = get('/conversations/' + query.conversation_id)
        if conversation.get('id') != query.conversation_id or conversation.get('contactId') != query.contact_id or conversation.get('locationId') != bridge.HIGHLEVEL_LOCATION_ID:
            failure('conversation_identity_mismatch', 403)
        params = {'limit': query.limit}
        if query.cursor: params['lastMessageId'] = query.cursor
        data = get('/conversations/' + query.conversation_id + '/messages', params)
        envelope = data.get('messages')
        if not isinstance(envelope, dict) or type(envelope.get('nextPage')) is not bool:
            failure('provider_resource_type_mismatch')
        rows = envelope.get('messages')
        pagination.update(complete=query.cursor is None and not envelope['nextPage'], has_more=envelope['nextPage'], next_cursor=envelope.get('lastMessageId') if envelope['nextPage'] else None)
    elif resource == 'tasks':
        data = get('/contacts/' + query.contact_id + '/tasks')
        rows = data.get('tasks')
        pagination.update(complete=isinstance(rows, list) and len(rows) <= query.limit, pagination_supported=False)
    else:
        data = get('/forms/submissions', {'locationId': bridge.HIGHLEVEL_LOCATION_ID, 'q': query.contact_id, 'limit': query.limit, 'page': query.page, 'startAt': query.start_date.isoformat(), 'endAt': query.end_date.isoformat()})
        rows = data.get('submissions')
        meta = data.get('meta')
        if not isinstance(meta, dict) or 'nextPage' not in meta or meta.get('currentPage') != query.page or not (meta['nextPage'] is None or type(meta['nextPage']) is int):
            failure('provider_pagination_mismatch')
        pagination.update(complete=query.page == 1 and meta['nextPage'] is None, has_more=meta['nextPage'] is not None, next_page=meta['nextPage'], page=query.page, start_date=query.start_date.isoformat(), end_date=query.end_date.isoformat())
    if not isinstance(rows, list): failure('provider_resource_type_mismatch')
    projected = []
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get('id'), str) or row.get('contactId') != query.contact_id:
            failure('resource_contact_mismatch')
        if resource in ('conversations', 'messages') and row.get('locationId') != bridge.HIGHLEVEL_LOCATION_ID:
            failure('resource_location_mismatch', 403)
        if resource == 'messages' and row.get('conversationId') != query.conversation_id:
            failure('resource_conversation_mismatch', 403)
        if 'locationId' in row and row['locationId'] != bridge.HIGHLEVEL_LOCATION_ID:
            failure('resource_location_mismatch', 403)
        # Only scalar allowlisted metadata; no provider bodies, subjects, addresses,
        # free-text task titles, form answers, attachment URLs or arbitrary meta.
        projected.append({key: row[key] for key in FIELDS[resource] if key in row and (row[key] is None or isinstance(row[key], (str, int, bool, float)))})
    if len(rows) > query.limit: pagination['complete'] = False
    return {'ok': True, 'resource': resource, 'contact_id': query.contact_id,
            'observed_at': datetime.now(timezone.utc).isoformat(),
            'records': projected[:query.limit], 'pagination': pagination,
            'metadata_only': True, 'workflow_enrollment': 'not_exposed_by_this_reader',
            'acceptance': 'Evidence only; delivery status is not inbox receipt or complete journey acceptance.'}

def register_routes(bridge):
    @bridge.app.post('/read/journey-evidence')
    def journey_evidence(query: EvidenceQuery, x_api_key: str | None = Header(default=None)):
        bridge.require_api_key(x_api_key)
        return read_evidence(bridge, query)
