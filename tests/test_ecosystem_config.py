import sqlite3
import pytest
from scripts import configure_ecosystem as config


def test_manifest_has_unique_fields_and_separate_test_pipeline():
    fields = config.fields()
    assert len({(f['model'], f['name']) for f in fields}) == len(fields)
    assert config.PIPELINES['TEST ONLY - BridgeGHL Validation'] == ['Draft test', 'Verified test']


def test_uncertain_write_is_not_replayed(monkeypatch):
    db = sqlite3.connect(':memory:')
    db.execute('CREATE TABLE effects (key TEXT PRIMARY KEY,state TEXT,native_id TEXT)')
    monkeypatch.setattr(config.bridge, 'append_audit_log', lambda x: None)
    effects = []
    def create():
        effects.append('attempt')
        raise TimeoutError()
    with pytest.raises(TimeoutError):
        config.ensure(db, 'test', lambda: None, create, lambda x: None)
    with pytest.raises(RuntimeError, match='no write retry'):
        config.ensure(db, 'test', lambda: None, create, lambda x: None)
    assert len(effects) == 1
    native = {'id': 'native1'}
    assert config.ensure(db, 'test', lambda: native, create, lambda x: None) == native
    assert len(effects) == 1


def test_duplicate_native_identity_blocks():
    with pytest.raises(RuntimeError, match='Ambiguous'):
        config.unique([{'id': 'a'}, {'id': 'b'}], lambda x: True)


def test_native_id_recovers_pending_contact_without_search_visibility(monkeypatch):
    db = sqlite3.connect(':memory:')
    db.execute('CREATE TABLE effects (key TEXT PRIMARY KEY,state TEXT,native_id TEXT)')
    db.execute('INSERT INTO effects VALUES (?,?,?)', ('canary', 'pending', 'native1'))
    monkeypatch.setattr(config.bridge, 'append_audit_log', lambda x: None)
    def forbidden_create():
        pytest.fail('A successful but not yet searchable record must not be recreated')
    result = config.ensure(db, 'canary', lambda: None, forbidden_create,
                           lambda x: config.require(x['id'] == 'native1', 'Wrong identity'),
                           lambda native_id: {'id': native_id})
    assert result['id'] == 'native1'
