"""Read back and complete only the synthetic workflow acceptance task."""
import json, sys, uuid
from pathlib import Path
sys.path.insert(0, '/home/bridgeadmin/apps/BridgeGHL')
import app as bridge
ROOT=Path('/var/lib/bridgeghl/ecosystem')
def call(method,path,body=None):
    status,data=bridge.highlevel_request(method,bridge.HIGHLEVEL_BASE_URL.rstrip('/')+path,body=body)
    assert 200 <= status < 300
    return data
def main():
    plan=json.loads((ROOT/'agent-field-plan.json').read_text())
    cid=plan['canary_contact_id']
    contact=call('GET','/contacts/'+cid)['contact']
    assert contact.get('dnd') is True and not contact.get('email') and not contact.get('phone')
    assert 'mode:test' in contact.get('tags',[])
    tasks=call('GET','/contacts/'+cid+'/tasks')['tasks']
    matches=[t for t in tasks if t.get('title')=='Review RSC readiness inquiry']
    assert len(matches)==1
    task=matches[0]
    assert task.get('assignedTo') and task.get('dueDate')
    assert 'Do not infer campaign consent' in task.get('body','')
    aid=str(uuid.uuid4())
    bridge.append_audit_log(dict(action='complete_isolated_workflow_test_task',audit_id=aid,target_id=task['id'],before_completed=task.get('completed'),reason='Owner-approved build test cleanup; synthetic DND/no-recipient contact only',result='intent'))
    if task.get('completed') is not True:
        call('PUT','/contacts/'+cid+'/tasks/'+task['id']+'/completed',{'completed':True})
    after=call('GET','/contacts/'+cid+'/tasks/'+task['id'])['task']
    assert after.get('completed') is True
    receipt=dict(workflow_task_native_verified=True,owner_present=True,due_date_present=True,canary_task_completed=True,audit_id=aid,messages_sent=0,real_contacts_changed=0)
    bridge.append_audit_log(dict(receipt,action='complete_isolated_workflow_test_task',target_id=task['id']))
    p=ROOT/'inquiry-workflow-acceptance.json'
    p.write_text(json.dumps(receipt)); p.chmod(0o600)
    print(json.dumps(receipt))
if __name__=='__main__':
    try: main()
    except Exception as e:
        print('workflow_acceptance_failed=true; error_class='+type(e).__name__)
        sys.exit(1)
