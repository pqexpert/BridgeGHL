"""One correlated Income diagnostic; loaded after fabric_binding_probe.py.

Reuse the original synthetic event key. Never request tools or business effects.
"""
EXPECTED_AGENT = 'agt_69ea58fb03788191b2421a67ab6abe04'
EVENT_KEY = 'fabric-binding-v1-20260908-income'


def diagnose(config, call):
    result = {'stage':'binding', 'outcome':'unknown', 'agent_identity':'unverified'}
    item = config.get('agents', {}).get('income', {})
    raw = item.get('id', '').strip()
    if raw.startswith('agt_') and raw != EXPECTED_AGENT:
        result.update(outcome='configured_agent_mismatch', agent_identity='mismatch')
        return result
    try:
        ident = trigger_id(raw, item.get('url', ''))
        if raw == EXPECTED_AGENT:
            result['agent_identity'] = 'configured_match'
        root = 'https://api.chatgpt.com/v1/workspace_agents/' + ident
        token = config.get('workspace_key', '').strip()
        result['stage'] = 'trigger'
        status, accepted = call(root + '/trigger', token, {
            'conversation_key':EVENT_KEY,
            'input':'Authorized synthetic Fabric transport canary. Lane: income. Return a brief acknowledgement and stop. Do not call any tools, read private sources, create tasks, send messages, change Calendar/CRM/Shopify/Notion/Jira, schedule work, or disclose credentials. This tests invocation only; do not claim production acceptance.'
        }, {'Idempotency-Key':EVENT_KEY, 'OpenAI-Beta':'workspace_agent_runs=v1'})
        run_id = accepted.get('agent_trigger_run_id', '')
        if status != 202 or not re.fullmatch(r'apirun_[A-Za-z0-9_-]+',run_id):
            raise ProbeError('accepted_status_or_run_id_invalid')
        result['stage']='run_status'
        _, state = call(root + '/runs/' + run_id, token)
        if state.get('id') != run_id or state.get('api_trigger_id') != ident:
            raise ProbeError('run_identity_mismatch')
        if state.get('agent_id') != EXPECTED_AGENT:
            result.update(outcome='published_agent_mismatch',agent_identity='mismatch')
            return result
        result['agent_identity']='provider_verified'
        outcome=state.get('status')
        if outcome not in ('queued','in_progress','completed','failed','suspended'):
            raise ProbeError('run_status_invalid')
        result['outcome']=outcome
    except ProbeError as error:
        result['outcome']=str(error)
    return result


def main(config):
    report=diagnose(config, request)
    print(json.dumps({'stage':'INCOME_BINDING_DIAGNOSTIC','results':report,'fabric_live':False},sort_keys=True))
    return 0
