"""Read only credential-name metadata relevant to the remaining Fabric migration.

Never reads secret values, creates credentials, or changes repository settings.
Each repository/environment is independent; failed access is reported as a status.
"""
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
REPOSITORIES = ('BridgeGHL', 'fabric-dispatch', 'fabric-distiller', 'income-accelerator',
                'rsc-operations-console', 'Dzokden-Store-Manager-Ziji', 'FungibleRD-MVP')

CANDIDATE = re.compile(r'JIRA|ATLASSIAN|GOOGLE|GCAL|OAUTH|REFRESH|SHOPIFY|NOTION', re.I)
BASE = 'https://api.github.com/repos/pqexpert/'


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('redirect_refused')


def pages(url, field):
    """Return metadata from all pages; retain page HTTP statuses on partial failure."""
    result, statuses, page = [], [], 1
    while True:
        query = urllib.parse.urlencode({'per_page': 100, 'page': page})
        request = urllib.request.Request(url + '?' + query, headers={
            'Authorization': 'Bearer ' + os.environ['SOURCE_TOKEN'],
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28'})
        try:
            with urllib.request.build_opener(NoRedirect).open(request, timeout=30) as response:
                statuses.append(response.status)
                body = json.loads(response.read())
                items = body.get(field)
                if not isinstance(items, list):
                    return result, statuses, False
                result.extend(items)
                links = response.headers.get('Link', '')
                if not re.search(r';\s*rel="next"', links):
                    return result, statuses, True
        except urllib.error.HTTPError as error:
            statuses.append(error.code)
            return result, statuses, False
        except Exception:
            # No exception text: URLs, token-bearing headers or provider bodies stay private.
            statuses.append(None)
            return result, statuses, False
        page += 1


def secret_names(url):
    items, statuses, complete = pages(url, 'secrets')
    names = sorted({item['name'] for item in items if isinstance(item, dict)
                    and isinstance(item.get('name'), str)
                    and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', item['name'])
                    and CANDIDATE.search(item['name'])})
    return {'candidate_names': names, 'http_statuses': statuses, 'complete': complete}


def main():
    if not os.environ.get('SOURCE_TOKEN', '').strip():
        print(json.dumps({'stage': 'FABRIC_MIGRATION_BINDING_NAMES', 'credential_binding_present': False}))
        return
    for repo in REPOSITORIES:
        root = BASE + urllib.parse.quote(repo, safe='')
        scope = {'stage': 'FABRIC_MIGRATION_BINDING_NAMES', 'repository': 'pqexpert/' + repo}
        print(json.dumps({**scope, 'scope': 'repository', **secret_names(root + '/actions/secrets')}, sort_keys=True))
        environments, statuses, complete = pages(root + '/environments', 'environments')
        print(json.dumps({**scope, 'scope': 'environment_listing', 'http_statuses': statuses,
                          'complete': complete}, sort_keys=True))
        for environment in environments:
            name = environment.get('name') if isinstance(environment, dict) else None
            if not isinstance(name, str):
                continue
            print(json.dumps({**scope, 'scope': 'environment', 'environment': name,
                              **secret_names(root + '/environments/' + urllib.parse.quote(name, safe='') + '/secrets')}, sort_keys=True))


if __name__ == '__main__':
    main()
