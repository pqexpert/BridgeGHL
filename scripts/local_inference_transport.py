"""VPS local-model client: bounded proposal, durable queue, never cloud fallback."""
import fcntl
import argparse, hashlib, json, os, pathlib, re, urllib.error, urllib.request
CONFIG = pathlib.Path("/etc/ziji-fabric/local-inference")
STATE = pathlib.Path("/var/lib/ziji-fabric/local-inference")
LANES = {"engineering","income","rsc","store","distiller"}
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):return None
def sha(b): return hashlib.sha256(b).hexdigest()
def atomic(path, data):
    path.parent.mkdir(parents=True,mode=0o700,exist_ok=True)
    temp=path.with_name(path.name+".tmp-"+os.urandom(8).hex())
    fd=os.open(str(temp),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    try:
        with os.fdopen(fd,"w") as f: json.dump(data,f,sort_keys=True);f.write("\n");f.flush();os.fsync(f.fileno())
        os.replace(temp,path)
    finally:
        if temp.exists():temp.unlink()
def prepare(request):
    if set(request)!={"request_id","lane","prompt"}: raise ValueError("request_fields")
    if not isinstance(request["request_id"],str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,96}",request["request_id"]):raise ValueError("request_id")
    if request["lane"] not in LANES: raise ValueError("lane")
    if not isinstance(request["prompt"],str) or not 1<=len(request["prompt"])<=16000: raise ValueError("prompt_size")
    values={}
    manifest=json.loads((CONFIG/"input-lock.json").read_text())
    for name in ("system.txt","memory.json","model-lock.json","sources.json"):
        content=(CONFIG/name).read_bytes()
        if sha(content)!=manifest[name]:raise ValueError("input_integrity")
        values[name]=content.decode()
    lock=json.loads(values["model-lock.json"])
    body={"model":"ziji-local","messages":[
        {"role":"system","content":values["system.txt"]+"\nCurated preferences: "+values["memory.json"]+"\nSole lane: "+request["lane"]},
        {"role":"user","content":request["prompt"]}],
        "max_tokens":lock["max_output_tokens"],"temperature":lock["temperature"],"top_p":lock["top_p"],
        "top_k":lock["top_k"],"min_p":lock["min_p"],"seed":lock["seed"],"stream":False,
        "chat_template_kwargs":{"enable_thinking":False}}
    fingerprint=sha(json.dumps({"request":request,"inputs":manifest},sort_keys=True).encode())
    return body,fingerprint,manifest
def run_locked(request):
    body,fingerprint,manifest=prepare(request)
    receipt=STATE/"results"/(request["request_id"]+".json")
    queued=STATE/"queued"/(request["request_id"]+".json")
    for p in (receipt,queued):
        if p.exists():
            prior=json.loads(p.read_text())
            if prior["request_hash"]!=fingerprint:raise ValueError("request_id_conflict")
            if p==receipt:return prior
    payload={"request":request,"request_hash":fingerprint,"input_hashes":manifest,"status":"QUEUED_LOCAL_UNAVAILABLE","paid_fallback":False}
    key=(CONFIG/"api-key").read_text().strip()
    req=urllib.request.Request("http://127.0.0.1:18182/v1/chat/completions",data=json.dumps(body).encode(),
          headers={"Content-Type":"application/json","Authorization":"Bearer "+key})
    try:
        with urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect()).open(req,timeout=180) as response:
            result=json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code<500: raise ValueError("local_http_"+str(exc.code)) from None
        atomic(queued,payload);return {k:v for k,v in payload.items() if k!="request"}
    except (urllib.error.URLError,TimeoutError,OSError):
        atomic(queued,payload);return {k:v for k,v in payload.items() if k!="request"}
    msg=result["choices"][0]["message"]
    if msg.get("tool_calls"):raise ValueError("tool_request_rejected")
    result={"request_id":request["request_id"],"lane":request["lane"],"request_hash":fingerprint,
        "status":"SUCCESS","proposal":msg.get("content") or "","usage":result.get("usage"),
        "input_hashes":manifest,"external_effects":False,"paid_fallback":False}
    atomic(receipt,result)
    if queued.exists():queued.unlink()
    return result
def run(request):
    prepare(request)
    locks=STATE/"locks";locks.mkdir(parents=True,mode=0o700,exist_ok=True)
    fd=os.open(str(locks/(request["request_id"]+".lock")),os.O_CREAT|os.O_RDWR,0o600)
    with os.fdopen(fd,"w") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        return run_locked(request)
def main():
    parser=argparse.ArgumentParser();parser.add_argument("--request-file",required=True);args=parser.parse_args()
    request=json.loads(pathlib.Path(args.request_file).read_text())
    try: print(json.dumps(run(request),sort_keys=True))
    except Exception as exc:
        print(json.dumps({"status":"FAILED","error":str(exc) if isinstance(exc,ValueError) else type(exc).__name__,"paid_fallback":False}))
        raise SystemExit(1)
if __name__=="__main__":main()
