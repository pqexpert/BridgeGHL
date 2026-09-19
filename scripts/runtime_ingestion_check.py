"""Print bounded runtime readiness without credentials, contact data or IDs."""
import json
import os
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

fields = ['BRIDGE_API_KEY', 'HIGHLEVEL_PIT', 'HIGHLEVEL_LOCATION_ID', 'GOOGLE_DRIVE_ACCESS_TOKEN',
          'HIGHLEVEL_INGEST_INCOME_PIPELINE_ID', 'HIGHLEVEL_INGEST_INCOME_STAGE_ID',
          'HIGHLEVEL_INGEST_INCOME_DEFAULT_CONTACT_ID']
result = {'configured': {k: bool(os.getenv(k)) for k in fields},
          'ingest_enabled': os.getenv('HIGHLEVEL_INGEST_ENABLED', '').lower() == 'true',
          'live_write_enabled': os.getenv('LIVE_WRITE_ENABLED', '').lower() in ('true', '1', 'yes', 'on')}
if os.getenv('HIGHLEVEL_PIT') and os.getenv('HIGHLEVEL_LOCATION_ID'):
    from urllib.parse import urlencode
    result['provider_checks'] = []
    for version in ('v3', '2021-07-28'):
        request = Request('https://services.leadconnectorhq.com/opportunities/pipelines?' + urlencode({'locationId': os.environ['HIGHLEVEL_LOCATION_ID']}),
            headers={'Authorization': 'Bearer ' + os.environ['HIGHLEVEL_PIT'], 'Version': version, 'Accept': 'application/json'})
        check = {'version': version}
        try:
            with urlopen(request, timeout=15) as response:
                data = json.load(response)
                check['status'] = response.status
                check['pipeline_count'] = len(data.get('pipelines', []))
        except HTTPError as exc:
            check['status'] = exc.code
            try:
                error = json.load(exc)
                message = str(error.get('message') or error.get('error') or error.get('error_description') or '')
                for secret in (os.getenv('HIGHLEVEL_PIT'), os.getenv('HIGHLEVEL_LOCATION_ID')):
                    if secret: message = message.replace(secret, '[redacted]')
                check['error_message'] = message[:200]
                check['scope_error'] = 'scope' in message.lower()
                check['version_error'] = 'version' in message.lower()
                check['token_error'] = 'token' in message.lower()
            except Exception:
                check['unparsed_error'] = True
        except URLError:
            check['status'] = 'transport_error'
        result['provider_checks'].append(check)
print(json.dumps(result, sort_keys=True))
