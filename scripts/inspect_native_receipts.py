"""Read-only bounded Jira acceptance audit; bodies encrypted for the operator."""
import base64, hashlib, json, os, pathlib, shlex, subprocess, sys, tempfile
PUBLIC="0ec2c3c7086e4a31d2045c0e1db021d5d25f19a228932b15d5a5da2a1f460627"
def remote():
    sys.path.insert(0,"/opt/ziji-fabric/native")
    from collect_native_sources import config,text_content
    from bind_native_providers import request
    cfg=config("jira")
    h={"Authorization":"Basic "+base64.b64encode((cfg["email"]+":"+cfg["api_token"]).encode()).decode()}
    path=pathlib.Path("/var/lib/ziji-fabric/native-sources/jira.json")
    cache=json.loads(path.read_text()) if path.exists() else {}
    rows={str(r["id"]):r for r in cache.get("receipt_candidates",[])}
    for cid in ("11923","11924"):
        if cid not in rows:
            row=request(cfg["base_url"]+"/rest/api/3/issue/RBO-97/comment/"+cid,h)
            rows[cid]={"id":cid,"body":text_content(row.get("body",{}))}
    latest=request(cfg["base_url"]+"/rest/api/3/issue/RBO-97/comment?orderBy=-created&maxResults=100",h)
    for row in latest.get("comments",[]):
        rows[str(row["id"])]={"id":str(row["id"]),"body":text_content(row.get("body",{}))}
    selected=[r for cid,r in rows.items() if cid in ("11923","11924") or ("approved distributed cutover" in r["body"].lower())]
    runs=[r for r in rows.values() if "FABRIC_RUN" in r["body"]][-5:]
    report={"cache_observed_at":cache.get("observed_at"),"selected":selected,"run_samples":runs,"recent_page_size":len(latest.get("comments",[]))}
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey,X25519PublicKey
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    key=X25519PrivateKey.generate();shared=key.exchange(X25519PublicKey.from_public_bytes(bytes.fromhex(PUBLIC)))
    nonce=os.urandom(12);raw=json.dumps(report).encode()
    cipher=AESGCM(hashlib.sha256(shared).digest()).encrypt(nonce,raw,b"ziji-receipt-audit")
    print(json.dumps({"ephemeral_public":key.public_key().public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw).hex(),"nonce":nonce.hex(),"ciphertext":base64.b64encode(cipher).decode()}))
def main():
    root=pathlib.Path(__file__).resolve().parents[1]
    from provision_local_transport import write
    host=os.environ["REMOTE_HOST"];user=os.environ["REMOTE_USER"]
    import re
    if not re.fullmatch(r"[A-Za-z0-9.-]+",host) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*",user):raise ValueError("target")
    key=os.environ["SSH_PRIVATE_KEY"].strip().replace("\\n","\n")
    if not key.startswith("-----BEGIN"):key=base64.b64decode(key).decode()
    with tempfile.TemporaryDirectory() as directory:
        d=pathlib.Path(directory);write(d/"key",(key+"\n").encode())
        pin=(root/"config/local-transport/server-host-key.pub").read_text().strip()
        write(d/"known_hosts",(host+" "+pin+"\n").encode())
        source=pathlib.Path(__file__).read_text()
        code="import base64;exec(compile(base64.b64decode("+repr(base64.b64encode(source.encode()).decode())+"),'<audit>','exec'))"
        cmd="sudo -n python3 -c "+shlex.quote(code)+" --remote"
        p=subprocess.run(["ssh","-i",str(d/"key"),"-o","IdentitiesOnly=yes","-o","BatchMode=yes","-o","StrictHostKeyChecking=yes","-o","UserKnownHostsFile="+str(d/"known_hosts"),user+"@"+host,cmd],capture_output=True,text=True,timeout=130)
        for line in p.stdout.splitlines():
            if line.startswith("{"):print(line)
        if p.returncode:print(json.dumps({"status":"AUDIT_FAILED","exit_code":p.returncode}))
        sys.exit(p.returncode)
if __name__=="__main__":
    if "--remote" in sys.argv:remote()
    else:main()
