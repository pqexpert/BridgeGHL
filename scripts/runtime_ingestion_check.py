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
    request = Request('https://services.leadconnectorhq.com/opportunities/pipelines?' + urlencode({'locationId': os.environ['HIGHLEVEL_LOCATION_ID']}),
        headers={'Authorization': 'Bearer ' + os.environ['HIGHLEVEL_PIT'], 'Version': 'v3', 'Accept': 'application/json'})
    try:
        with urlopen(request, timeout=15) as response:
            data = json.load(response)
            result['provider_status'] = response.status
            result['pipeline_count'] = len(data.get('pipelines', []))
    except HTTPError as exc: result['provider_status'] = exc.code
    except URLError: result['provider_status'] = 'transport_error'
print(json.dumps(result, sort_keys=True))
