"""Bounded IONOS record updates; no record deletion or whole-zone replacement."""
import hashlib
import ipaddress
import json
import os
import unicodedata
import urllib.error
import urllib.request

DOMAIN = 'restorationsecurityoutreach.com'
WWW = 'www.' + DOMAIN
REVISION = 'b2feed67d768d0e29e498b8af6189bb6b23850f9'
ORIGINAL = {
    (DOMAIN, 'A'): '212.227.222.65',
    (DOMAIN, 'AAAA'): '2001:8d8:fe::65',
    (WWW, 'A'): '74.208.236.247',
    (WWW, 'AAAA'): '2607:f1c0:100f:f000::200',
}


def shape(record):
    kind = record['type']
    content = record['content']
    if kind in ('A', 'AAAA'):
        content = str(ipaddress.ip_address(content))
    elif kind in ('CNAME', 'MX', 'NS'):
        content = content.rstrip('.').lower()
    return dict(name=record['name'].rstrip('.').lower(), type=kind,
                content=content, ttl=int(record.get('ttl', 3600)),
                prio=int(record.get('prio', 0)), disabled=bool(record.get('disabled', False)))


def web(record):
    return (record['name'].rstrip('.').lower() in (DOMAIN, WWW)
            and record['type'] in ('A', 'AAAA', 'CNAME', 'ALIAS'))


def digest(records):
    rows = [dict(id=r['id'], **shape(r)) for r in records if r['type'] != 'SOA']
    return hashlib.sha256(json.dumps(sorted(rows, key=lambda r: r['id']),
                                     sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def make_snapshot(zone, records, target):
    target = str(ipaddress.IPv4Address(target))
    old = [r for r in records if web(r)]
    if len(old) != 4 or len({r['id'] for r in old}) != 4:
        raise RuntimeError('expected_four_original_web_identities')
    seen = set()
    desired = {}
    for r in old:
        s = shape(r)
        key = (s['name'], s['type'])
        if (key in seen or key not in ORIGINAL or s['content'] != ORIGINAL[key]
                or s['ttl'] != 3600 or s['disabled'] or s['prio'] != 0):
            raise RuntimeError('original_web_preimage_mismatch')
        seen.add(key)
        if s['type'] == 'A':
            s.update(content=target, ttl=300)
        else:
            # Keep both IPv6 records and their IDs for exact recovery; do not serve the old host.
            s['disabled'] = True
        desired[r['id']] = s
    return dict(zone=zone, records=records, desired=desired, target=target,
                protected_digest=digest([r for r in records if not web(r)]), revision=REVISION)


class Ionos:
    def __init__(self):
        self.key = ''.join(c for c in os.environ['IONOS_API_KEY'].strip()
                           if not c.isspace() and unicodedata.category(c) != 'Cf')
        if not self.key or any(ord(c) < 32 or ord(c) > 126 for c in self.key):
            raise RuntimeError('invalid_credential_encoding')

    def request(self, method, path, data=None):
        req = urllib.request.Request('https://api.hosting.ionos.com/dns/v1' + path,
              data=None if data is None else json.dumps(data).encode(), method=method,
              headers={'X-API-Key': self.key, 'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                body = response.read()
                return json.loads(body) if body else None
        except urllib.error.HTTPError as error:
            raise RuntimeError('ionos_http_' + str(error.code)) from None

    def current(self):
        zones = [z for z in self.request('GET', '/zones') if z['name'].rstrip('.') == DOMAIN]
        if len(zones) != 1:
            raise RuntimeError('zone_identity_ambiguous')
        zone = zones[0]['id']
        return zone, self.request('GET', '/zones/' + zone)['records']

    def update(self, zone, identity, wanted):
        # Official IONOS record-update schema: never PUT the containing zone.
        payload = {key: wanted[key] for key in ('content', 'ttl', 'prio', 'disabled')}
        return self.request('PUT', '/zones/' + zone + '/records/' + identity, payload)


class Change:
    def __init__(self, api, before, persist):
        self.api, self.before, self.persist = api, before, persist
        self.sequence = 0

    def save(self, phase, records):
        self.sequence += 1
        self.persist('dns-step-%02d.json' % self.sequence,
                     dict(phase=phase, zone=self.before['zone'], records=records))

    def current(self):
        zone, records = self.api.current()
        if zone != self.before['zone']:
            raise RuntimeError('zone_changed')
        return records

    def protected(self, records):
        if digest([r for r in records if not web(r)]) != self.before['protected_digest']:
            raise RuntimeError('protected_dns_changed')

    def apply(self):
        records = self.current()
        if digest(records) != digest(self.before['records']):
            raise RuntimeError('dns_pre_effect_compare_failed')
        expected = {r['id']: shape(r) for r in records if web(r)}
        # IPv4 first; original IPv6 is disabled only after both IPv4 records are admitted.
        identities = sorted(self.before['desired'], key=lambda k: self.before['desired'][k]['type'])
        for identity in identities:
            fresh = self.current()
            self.protected(fresh)
            actual = {r['id']: shape(r) for r in fresh if web(r)}
            if actual != expected:
                raise RuntimeError('web_dns_concurrent_change')
            self.save('before-update:' + identity, fresh)  # Durable/read back before every effect.
            self.api.update(self.before['zone'], identity, self.before['desired'][identity])
            expected[identity] = self.before['desired'][identity]
            after = self.current()
            self.save('after-update:' + identity, after)
            if {r['id']: shape(r) for r in after if web(r)} != expected:
                raise RuntimeError('record_update_readback_failed')
            self.protected(after)
        return self.verify()

    def verify(self):
        after = self.current()
        self.protected(after)
        if {r['id']: shape(r) for r in after if web(r)} != self.before['desired']:
            raise RuntimeError('dns_final_readback_failed')
        self.save('verified', after)
        return dict(web_record_ids_preserved=True, active_ipv4=2, disabled_original_ipv6=2,
                    protected_digest=self.before['protected_digest'], protected_records_preserved=True)

    def rollback(self):
        originals = {r['id']: shape(r) for r in self.before['records'] if web(r)}
        # Reread even after an uncertain PUT. Restore only exact before/owned after states.
        for identity, original in originals.items():
            fresh = self.current()
            current = {r['id']: shape(r) for r in fresh if web(r)}
            if current.get(identity) == original:
                continue
            if current.get(identity) != self.before['desired'][identity]:
                raise RuntimeError('rollback_target_conflict')
            self.save('before-rollback:' + identity, fresh)
            self.api.update(self.before['zone'], identity, original)
            self.save('after-rollback:' + identity, self.current())
        after = self.current()
        if {r['id']: shape(r) for r in after if web(r)} != originals:
            raise RuntimeError('rollback_original_record_mismatch')
        self.protected(after)
        self.save('rollback-verified', after)
        return dict(dns_rollback_verified=True, restored_original_records=4)


def persist_remote(name, value):
    import pathlib
    import re
    import shlex
    import subprocess
    directory = os.environ['BACKUP_DIR']
    if not re.fullmatch(r'/var/lib/rsc-ops/cutovers/outreach-[0-9]+-[0-9]+', directory):
        raise RuntimeError('invalid_backup_identity')
    if not re.fullmatch(r'[a-z0-9.-]+\.json', name):
        raise RuntimeError('invalid_backup_name')
    blob = json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
    code = '''import hashlib,json,os,pathlib,sys
p=pathlib.Path(sys.argv[1]); data=sys.stdin.buffer.read()
if p.exists():
    assert p.read_bytes()==data, 'immutable_backup_conflict'
else:
    fd=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'wb') as f: f.write(data); f.flush(); os.fsync(f.fileno())
st=p.stat(); assert st.st_uid==0 and st.st_mode & 0o777==0o600
assert p.parent.stat().st_uid==0 and p.parent.stat().st_mode & 0o777==0o700
print(hashlib.sha256(p.read_bytes()).hexdigest())
'''
    cmd = ['ssh', '-o', 'LogLevel=ERROR', '-o', 'UserKnownHostsFile=' + str(pathlib.Path.home()/'.ssh/rsc-hosts'),
           '-o', 'StrictHostKeyChecking=yes', '-i', str(pathlib.Path.home()/'.ssh/rsc-cutover'),
           os.environ['REMOTE_USER'] + '@' + os.environ['REMOTE_HOST'],
           'sudo -n python3 -c ' + shlex.quote(code) + ' ' + shlex.quote(directory + '/' + name)]
    result = subprocess.run(cmd, input=blob, capture_output=True, check=True)
    if result.stdout.decode().strip() != hashlib.sha256(blob).hexdigest():
        raise RuntimeError('durable_backup_readback_failed')


def main(mode):
    import pathlib
    import socket
    import time
    api = Ionos()
    path = pathlib.Path('/tmp/rsc-outreach-dns-before.json')
    if mode == 'snapshot':
        zone, records = api.current()
        before = make_snapshot(zone, records, socket.gethostbyname(os.environ['REMOTE_HOST']))
        persist_remote('dns-before.json', before)
        path.write_text(json.dumps(before))
        path.chmod(0o600)
        print('durable_dns_backup_verified=true; original_web_records=4; record_deletions=0')
        return
    if not path.exists():
        raise RuntimeError('dns_before_snapshot_missing')
    before = json.loads(path.read_text())
    # Revalidate backup custody, including content, immediately before mutation/recovery.
    persist_remote('dns-before.json', before)
    invocation = mode + '-' + str(time.time_ns()) + '-'
    change = Change(api, before, lambda name, value: persist_remote(invocation + name, value))
    if mode == 'apply':
        print(json.dumps(change.apply(), sort_keys=True))
    elif mode == 'rollback':
        print(json.dumps(change.rollback(), sort_keys=True))
    elif mode == 'verify':
        print(json.dumps(change.verify(), sort_keys=True))
    else:
        raise RuntimeError('invalid_dns_mode')


if __name__ == '__main__':
    import sys
    try:
        main(sys.argv[1])
    except Exception as error:
        print('dns_operation_failed=' + (str(error) if isinstance(error, RuntimeError) else type(error).__name__))
        raise SystemExit(1)
