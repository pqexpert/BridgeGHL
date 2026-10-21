"""Shared internal Jira evidence writer. No domain actions or model calls."""
import base64, datetime as dt, fcntl, hashlib, json, os, pathlib, re
from bind_native_providers import request, JIRA_ROOT, ProbeError
from collect_native_sources import config, text_content, atomic
STATE=pathlib.Path("/var/lib/ziji-fabric/receipts")
ISSUES={"RBO-97","RBO-34"}
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":")).encode()).hexdigest()
def headers():
    cfg=config("jira")
    if cfg.get("base_url")!=JIRA_ROOT or cfg.get("project_allowlist")!=["RBO"]:raise ProbeError("jira_scope")
    return {"Authorization":"Basic "+base64.b64encode((cfg["email"]+":"+cfg["api_token"]).encode()).decode()}
def matches(body,key):
    return bool(re.search(r"(?<![A-Za-z0-9_:/.-])"+re.escape(key)+r"(?![A-Za-z0-9_:/.-])",body))
def lookup(issue,key,h,call):
    # One full index on first use; later searches fetch only comments newer than
    # the known first page. This is private server evidence, never a public log.
    path=STATE/(issue+"-index.json")
    known=json.loads(path.read_text()) if path.exists() else {}
    cached=dict(known);start=0
    for _ in range(100):
        page=call(JIRA_ROOT+"/rest/api/3/issue/"+issue+"/comment?orderBy=-created&maxResults=100&startAt="+str(start),h)
        rows=page.get("comments")
        if not isinstance(rows,list) or page.get("startAt")!=start or not isinstance(page.get("total"),int):raise ProbeError("jira_pagination")
        overlap=False
        for row in rows:
            cid=str(row["id"]);body=text_content(row.get("body",{}))
            if cid in known:overlap=True
            cached[cid]=body
        start+=len(rows)
        if overlap or start>=page["total"]:break
        if not rows:raise ProbeError("jira_incomplete")
    else:raise ProbeError("jira_limit")
    atomic(path,json.dumps(cached,sort_keys=True))
    found=[cid for cid,body in cached.items() if matches(body,key)]
    if len(found)>1:raise ProbeError("duplicate_existing_receipts")
    return found[0] if found else None
def readback(issue,cid,key,h,call,receipt):
    value=call(JIRA_ROOT+"/rest/api/3/issue/"+issue+"/comment/"+cid,h)
    if str(value.get("id"))!=cid or not matches(text_content(value.get("body",{})),key):raise ProbeError("jira_readback")
    if json.dumps(receipt,sort_keys=True) not in text_content(value.get("body",{})):raise ProbeError("jira_receipt_content_mismatch")
    return True
def publish(issue,replay_id,receipt,call=request):
    if issue not in ISSUES or not re.fullmatch(r"[a-z0-9][A-Za-z0-9_:/.-]{1,239}",replay_id):raise ProbeError("receipt_scope")
    if not isinstance(receipt,dict) or receipt.get("lane") not in {"income","rsc","store","distiller","dispatch"}:raise ProbeError("receipt_lane")
    if receipt.get("outcome") not in {"SUCCESS","NO_CHANGE","BLOCKED","FAIL"}:raise ProbeError("receipt_outcome")
    if len(json.dumps(receipt))>20000:raise ProbeError("receipt_size")
    STATE.mkdir(parents=True,exist_ok=True,mode=0o700);STATE.chmod(0o700)
    with (STATE/"write.lock").open("a") as lock:
        os.chmod(STATE/"write.lock",0o600);fcntl.flock(lock,fcntl.LOCK_EX)
        return _publish(issue,replay_id,receipt,call)
def _publish(issue,key,receipt,call):
    h=headers();path=STATE/(digest({"issue":issue,"key":key})+".json")
    fingerprint=digest(receipt);prior=json.loads(path.read_text()) if path.exists() else None
    if prior and prior["request_hash"]!=fingerprint:raise ProbeError("receipt_id_conflict")
    if prior and prior.get("status") in {"VERIFIED","EXISTING"}:return {k:prior[k] for k in ("status","comment_id","native_readback","replay_id")}
    cid=lookup(issue,key,h,call)
    base={"issue":issue,"replay_id":key,"request_hash":fingerprint,"receipt":receipt,"observed_at":dt.datetime.now(dt.timezone.utc).isoformat()}
    if cid:
        readback(issue,cid,key,h,call,receipt)
        result=dict(base,status="EXISTING",comment_id=cid,native_readback=True)
    elif prior:
        # PENDING is written before POST. An absent provider readback following
        # an ambiguous timeout never proves the write did not happen.
        result=dict(base,status="UNKNOWN",comment_id=None,native_readback=False)
    else:
        result=dict(base,status="PENDING",comment_id=None,native_readback=False)
        atomic(path,json.dumps(result,sort_keys=True))
        body={"body":{"type":"doc","version":1,"content":[{"type":"paragraph","content":[{"type":"text","text":("FABRIC_HANDOFF " if receipt.get("action_class")=="merge_handoff" else "FABRIC_RUN " if key.endswith(":daily") else "FABRIC_FINDING ")+key+"\n"+json.dumps(receipt,sort_keys=True)}]}]}}
        try:
            response=call(JIRA_ROOT+"/rest/api/3/issue/"+issue+"/comment",h,body)
            cid=str(response["id"]);readback(issue,cid,key,h,call,receipt)
            result.update(status="VERIFIED",comment_id=cid,native_readback=True)
        except Exception:
            result.update(status="UNKNOWN",native_readback=False)
    atomic(path,json.dumps(result,sort_keys=True))
    return {k:result[k] for k in ("status","comment_id","native_readback","replay_id")}
