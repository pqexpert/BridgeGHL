"""Read-only native evidence collector. No inference, messages or domain writes.

Private snapshots prove retrieval, never specialist execution or writer handover.
Uses existing root-only provider bindings and the existing host cron service.
"""
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import urllib.parse

from bind_native_providers import request, required, JIRA_ROOT, ProbeError

ROOT = Path('/opt/ziji-fabric/native')
STATE = Path('/var/lib/ziji-fabric/native-sources')
PROVIDERS = Path('/etc/ziji-fabric/providers')
CRON = Path('/etc/cron.d/ziji-fabric-native-sources')

def atomic(path, value, mode=0o600):
    if path.is_symlink():
        raise ProbeError('path_symlink')
    fd, tmp = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(value)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)

def config(lane):
    p = PROVIDERS / lane / 'credentials.json'
    if p.is_symlink() or p.stat().st_mode & 0o077 or p.stat().st_uid != 0:
        raise ProbeError('unsafe_provider_binding')
    return json.loads(p.read_text())

def text_content(node):
    if isinstance(node, dict):
        value = node.get('text', '')
        return value + '\n' + ''.join(text_content(x) for x in node.get('content', []))
    return ''

def jira_snapshot(cfg, call=request):
    import base64
    # Binding installation audits project permissions. Each collection still
    # verifies the active identity, but does not repeat unused write probes.
    required(cfg, ('email', 'api_token'))
    if cfg.get('base_url') != JIRA_ROOT or cfg.get('project_allowlist') != ['RBO']:
        raise ProbeError('jira_binding_scope_mismatch')
    headers = {'Authorization':'Basic ' + base64.b64encode((cfg['email']+':'+cfg['api_token']).encode()).decode()}
    identity = call(JIRA_ROOT + '/rest/api/3/myself', headers)
    if (not identity.get('active') or not identity.get('accountId') or
        not isinstance(identity.get('emailAddress'), str) or
        identity['emailAddress'].lower() != cfg['email'].lower()):
        raise ProbeError('jira_identity_mismatch')
    comments, start = [], 0
    for _ in range(100):
        page = call(cfg['base_url'] + '/rest/api/3/issue/RBO-97/comment?' + urllib.parse.urlencode(
            {'startAt':start, 'maxResults':100, 'orderBy':'created'}), headers)
        rows = page.get('comments')
        if not isinstance(rows, list) or page.get('startAt') != start or not isinstance(page.get('total'), int):
            raise ProbeError('jira_pagination_invalid')
        for row in rows:
            body = text_content(row.get('body', {}))
            if any(marker in body for marker in ('FABRIC_RUN', 'FABRIC_FINDING', 'FABRIC_CHECKPOINT', 'FABRIC_HANDOFF', 'FABRIC_CALENDAR_FEEDBACK_BASELINE')):
                comments.append({'id':row['id'], 'created':row['created'], 'updated':row['updated'],
                                 'body':body, 'reference':'https://restoration-mt.atlassian.net/browse/RBO-97?focusedCommentId='+row['id']})
        start += len(rows)
        if start >= page['total']:
            return {'coverage':'all_issue_comments', 'comments_scanned':start, 'receipt_candidates':comments,
                    'source_verified':True, 'actor_execution_verified':False}
        if not rows:
            raise ProbeError('jira_pagination_incomplete')
    raise ProbeError('jira_pagination_limit')

def calendar_snapshot(cfg, now, call=request):
    token = call('https://oauth2.googleapis.com/token', body={'grant_type':'refresh_token',
        'client_id':cfg['client_id'], 'client_secret':cfg['client_secret'], 'refresh_token':cfg['refresh_token']})
    headers = {'Authorization':'Bearer '+token['access_token']}
    owner = call('https://www.googleapis.com/drive/v3/about?fields=user(emailAddress)',headers)
    if owner.get('user',{}).get('emailAddress','').lower() != cfg['expected_email']:
        raise ProbeError('google_identity_mismatch')
    root = 'https://www.googleapis.com/calendar/v3/calendars/' + urllib.parse.quote(cfg['calendar_id'],safe='')
    params = {'timeMin':(now-dt.timedelta(hours=48)).isoformat(), 'timeMax':(now+dt.timedelta(hours=36)).isoformat(),
              'singleEvents':'true', 'maxResults':250, 'orderBy':'startTime'}
    events, scanned, seen = [], 0, set()
    for _ in range(20):
        page = call(root+'/events?'+urllib.parse.urlencode(params),headers)
        rows = page.get('items')
        if not isinstance(rows,list):
            raise ProbeError('calendar_items_invalid')
        scanned += len(rows)
        for event in rows:
            description = event.get('description','')
            if (event.get('visibility') == 'private' and not event.get('attendees')
                and event.get('eventType','default') == 'default' and 'FABRIC CURRENT NOTE' in description):
                # Exact descriptions are kept only in root-only private storage.
                events.append(event)
        nxt = page.get('nextPageToken')
        if not nxt:
            return {'source_verified':True, 'coverage':{'timeMin':params['timeMin'],'timeMax':params['timeMax']},
                    'events_scanned':scanned, 'events':events, 'calendar_writes':0,
                    'owner_feedback_consumed':False, 'writer_allowlist_established':False}
        if nxt in seen:
            raise ProbeError('calendar_pagination_repeated')
        seen.add(nxt)
        params['pageToken'] = nxt
    raise ProbeError('calendar_pagination_limit')

def source_watch_status(now):
    """Read daily source metadata locally; no network or admission inference."""
    path = STATE / 'source-watch' / 'latest.json'
    try:
        if path.is_symlink() or path.stat().st_size > 65536:
            raise ValueError('unsafe_source_metadata')
        value = json.loads(path.read_text())
        observed = dt.datetime.fromisoformat(value['observed_at'])
        if (value.get('stage') != 'SOURCE_IDENTITY_OBSERVATION' or observed.tzinfo is None or
            observed > now + dt.timedelta(minutes=5) or now - observed > dt.timedelta(hours=36)):
            raise ValueError('stale_source_metadata')
        return {k:value.get(k) for k in ('status', 'coverage', 'missing_paths', 'observed_at', 'cadence', 'pending_review_count',
                'pending_index', 'changed_repositories', 'deployed_pins_changed', 'policy_applied')}
    except (OSError, ValueError, KeyError, TypeError):
        return {'status':'UNKNOWN_OR_STALE', 'cadence':'existing_daily_backup_07_43_UTC',
                'effect':'Only source observation unavailable; other collection lanes continue.'}

def collect(trigger='manual'):
    if STATE.is_symlink():
        raise ProbeError('state_path_symlink')
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    STATE.chmod(0o700)
    with (STATE/'run.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        now = dt.datetime.now(dt.timezone.utc)
        report = {'stage':'NATIVE_SOURCE_COLLECTION', 'observed_at':now.isoformat(),
                  'trigger':trigger, 'model_calls':0, 'calendar_writes':0, 'external_messages':0, 'actor_execution_verified':False}
        if (STATE/'latest.json').exists():
            try:
                prior = json.loads((STATE/'latest.json').read_text())
                if not isinstance(prior, dict):
                    raise ValueError('prior_not_object')
                report['previous_collection'] = {k:prior.get(k) for k in ('observed_at','trigger')}
            except (ValueError, OSError):
                report['previous_collection'] = {'status':'unreadable_prior_metadata'}
        for lane, fn in (('jira',lambda: jira_snapshot(config('jira'))),
                         ('calendar',lambda: calendar_snapshot(config('google'),now))):
            try:
                snapshot = fn()
                snapshot['observed_at'] = now.isoformat()
                latest = STATE/(lane+'.json')
                if latest.exists():
                    atomic(STATE/(lane+'-previous.json'),latest.read_text())
                raw = json.dumps(snapshot, sort_keys=True)
                atomic(latest,raw)
                if latest.read_text() != raw:
                    raise ProbeError('snapshot_readback_failed')
                report[lane] = {'status':'retrieved_and_persisted',
                    'items':len(snapshot.get('receipt_candidates',snapshot.get('events',[]))),
                    'scanned':snapshot.get('comments_scanned',snapshot.get('events_scanned')),
                    'snapshot_sha256':hashlib.sha256(raw.encode()).hexdigest()}
            except Exception as e:
                report[lane] = {'status':str(e) if isinstance(e,ProbeError) else 'collection_failed_details_withheld'}
        report['source_watch'] = source_watch_status(now)
        atomic(STATE/'latest.json',json.dumps(report,sort_keys=True))
        return report

def install(source, binding_source):
    if os.geteuid() != 0 or ROOT.is_symlink() or CRON.is_symlink():
        raise ProbeError('installation_boundary_invalid')
    ROOT.mkdir(parents=True,exist_ok=True,mode=0o700)
    ROOT.chmod(0o700)
    cron = '# Native read-only evidence; no model or provider writes.\n11 * * * * root /usr/bin/python3 /opt/ziji-fabric/native/collect_native_sources.py --trigger cron >/dev/null 2>&1\n'
    legacy_cron = cron.replace(' --trigger cron', '')
    if CRON.exists() and CRON.read_text() not in (cron, legacy_cron):
        raise ProbeError('existing_cron_conflict_preserved')
    atomic(ROOT/'bind_native_providers.py',binding_source)
    atomic(ROOT/'collect_native_sources.py',source)
    result = collect('installation_validation')
    ready = all(result[x]['status']=='retrieved_and_persisted' for x in ('jira','calendar'))
    if ready:
        atomic(CRON,cron,0o644)
    result['cron_installed'] = CRON.exists() and CRON.read_text()==cron
    result['calendar_writer_enabled'] = False
    return result

if __name__ == '__main__':
    try:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--trigger', choices=('manual','cron'), default='manual')
        print(json.dumps(collect(parser.parse_args().trigger),sort_keys=True))
    except Exception:
        print('{"stage":"NATIVE_SOURCE_COLLECTION","status":"failed_details_withheld"}')
        sys.exit(1)
