"""Permit only approved web ports in the active IPv4 ingress zone; preserve other rules."""
import json
import os
from pathlib import Path
import subprocess
import sys


def run(*args):
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError('host_command_failed:' + args[0])
    return result.stdout.strip()


def rules():
    return {mode: run('firewall-cmd', *flag, '--list-all-zones')
            for mode, flag in (('runtime', ()), ('permanent', ('--permanent',)))}


def protected(rule, zone):
    current = None
    result = []
    for line in rule.splitlines():
        if line and not line[0].isspace():
            current = line.split()[0]
        if current == zone and line.strip().startswith('ports:'):
            tokens = sorted(set(line.split(':', 1)[1].split()) - {'80/tcp', '443/tcp'})
            line = '  ports: ' + ' '.join(tokens)
        result.append(line.rstrip())
    return '\n'.join(result)


def save(path, data):
    # This file lives in the run's root-only directory; never exported to Actions artifacts.
    with open(path, 'x', encoding='utf-8') as file:
        os.chmod(path, 0o600)
        file.write(json.dumps(data)); file.flush(); os.fsync(file.fileno())
    if json.loads(path.read_text()) != data or path.stat().st_uid != 0:
        raise RuntimeError('ingress_backup_readback_failed')


def main(mode):
    directory = Path(os.environ['BACKUP_DIR'])
    state = directory / 'firewall-before.json'
    if mode == 'apply':
        run('systemctl', 'is-active', 'firewalld')
        routes = json.loads(run('ip', '-json', '-4', 'route', 'show', 'default'))
        interfaces = {r['dev'] for r in routes if r.get('dev')}
        if len(interfaces) != 1:
            raise RuntimeError('ingress_interface_ambiguous')
        interface = next(iter(interfaces))
        query = subprocess.run(['firewall-cmd', '--get-zone-of-interface=' + interface],
                               capture_output=True, text=True)
        zone = query.stdout.strip()
        if zone in ('', 'no zone'):
            zone = run('firewall-cmd', '--get-default-zone')
        elif query.returncode:
            raise RuntimeError('ingress_zone_query_failed')
        if zone not in run('firewall-cmd', '--get-zones').split():
            raise RuntimeError('ingress_zone_unknown')
        before = rules()
        original = {}
        for label, flags in (('runtime', []), ('permanent', ['--permanent'])):
            original[label] = run('firewall-cmd', *flags, '--zone=' + zone, '--list-ports').split()
        saved = dict(zone=zone, interface=interface, rules=before, ports=original)
        save(state, saved)
        if rules() != before:
            raise RuntimeError('ingress_pre_effect_compare_failed')
        for label, flags in (('runtime', []), ('permanent', ['--permanent'])):
            for port in ('80/tcp', '443/tcp'):
                if port not in original[label]:
                    run('firewall-cmd', *flags, '--zone=' + zone, '--add-port=' + port)
    elif not state.exists():
        print('ingress_rollback_not_needed=true')
        return
    saved = json.loads(state.read_text())
    zone = saved['zone']
    if mode == 'rollback':
        for label, flags in (('runtime', []), ('permanent', ['--permanent'])):
            ports = run('firewall-cmd', *flags, '--zone=' + zone, '--list-ports').split()
            for port in ('80/tcp', '443/tcp'):
                if port not in saved['ports'][label] and port in ports:
                    run('firewall-cmd', *flags, '--zone=' + zone, '--remove-port=' + port)
    after = rules()
    for label in saved['rules']:
        if protected(after[label], zone) != protected(saved['rules'][label], zone):
            raise RuntimeError('unrelated_firewall_rules_changed')
        flags = [] if label == 'runtime' else ['--permanent']
        ports = run('firewall-cmd', *flags, '--zone=' + zone, '--list-ports').split()
        for port in ('80/tcp', '443/tcp'):
            if (port in ports) != ((port in saved['ports'][label]) if mode == 'rollback' else True):
                raise RuntimeError('web_port_readback_failed')
    save(directory / ('firewall-' + mode + '-verified.json'), after)
    print('ingress_' + mode + '_verified=true; only_tcp80_tcp443=true; other_rules_preserved=true; reload=false')


if __name__ == '__main__':
    try:
        main(sys.argv[1])
    except Exception as error:
        print('ingress_failed=' + str(error))
        raise SystemExit(1)
