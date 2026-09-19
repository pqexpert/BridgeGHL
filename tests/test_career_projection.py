from scripts.project_career_fields import values, missing_fields


def test_unknown_status_and_dates_do_not_become_application_or_deadline():
    data = values({'source_id': 's', 'source_url': 'https://source', 'properties': {}}, '2026-09-19')
    assert data['Ecosystem Original Source Status'] == 'unknown'
    assert 'Ecosystem Next Follow-Up Date' not in data
    assert 'needs-enrichment' in data['Ecosystem Data Quality']
    assert not any(k in data for k in ['status', 'assignedTo', 'pipelineStageId'])


def test_native_human_values_are_preserved_and_missing_fields_filled():
    native = {'customFields': [{'id': 'a', 'fieldValue': 'Human decision'}, {'id': 'b', 'value': ''}]}
    assert missing_fields(native, {'a': 'Older source', 'b': 'Source evidence', 'c': 'New'}) == {'b': 'Source evidence', 'c': 'New'}
