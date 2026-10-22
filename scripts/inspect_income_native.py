"""Read-only native Income failure inspection; no private values in output."""
import base64
import hashlib
import os
import stat
import json
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request

def encrypted_private_export():
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey,X25519PublicKey
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    root=Path("/var/lib/ziji-fabric/income")
    paths=[Path("/etc/ziji-fabric/income/intake.json"),root/"last-report.json"]+sorted(root.glob("unit-*.json"))
    if len(paths)>22:raise ValueError("income_export_bound")
    values={}
    for path in paths:
        if not path.exists():continue
        st=path.lstat()
        if not stat.S_ISREG(st.st_mode) or st.st_uid!=0 or st.st_mode & 0o077 or st.st_size>500000:
            raise ValueError("income_private_file_required")
        values[str(path)]=json.loads(path.read_text())
    raw=json.dumps(values,sort_keys=True).encode()
    if len(raw)>1000000:raise ValueError("income_export_size")
    public="0ec2c3c7086e4a31d2045c0e1db021d5d25f19a228932b15d5a5da2a1f460627"
    key=X25519PrivateKey.generate();shared=key.exchange(X25519PublicKey.from_public_bytes(bytes.fromhex(public)))
    nonce=os.urandom(12)
    cipher=AESGCM(hashlib.sha256(shared).digest()).encrypt(nonce,raw,b"ziji-income-inspection")
    return {"ephemeral_public":key.public_key().public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw).hex(),
            "nonce":nonce.hex(),"ciphertext":base64.b64encode(cipher).decode()}

def inspect():
    root=Path("/var/lib/ziji-fabric/income")
    states=[]
    for path in sorted(root.glob("unit-*.json")):
        x=json.loads(path.read_text())
        states.append({"phase":x.get("phase"),"proposal_present":bool(x.get("proposal")),
                       "reserved_file_id_present":bool(x.get("file_id")),
                       "artifact_digest_present":bool(x.get("artifact_sha256"))})
    cfg=json.loads(Path("/etc/ziji-fabric/providers/google/credentials.json").read_text())
    data=urllib.parse.urlencode({"grant_type":"refresh_token","client_id":cfg["client_id"],
        "client_secret":cfg["client_secret"],"refresh_token":cfg["refresh_token"]}).encode()
    req=urllib.request.Request("https://oauth2.googleapis.com/token",data=data,
        headers={"Content-Type":"application/x-www-form-urlencoded","User-Agent":"ZijiFabric/1.0 (career research)"})
    with urllib.request.urlopen(req,timeout=30) as response:token=json.load(response)
    scopes=token.get("scope","").split()
    report={"stage":"INCOME_NATIVE_INSPECTION","read_only":True,"unit_states":states,
            "drive_scope_names":[x.rsplit("/",1)[-1] for x in scopes if x.startswith("https://www.googleapis.com/auth/drive")],
            "provider_write_attempted":False,"model_calls":0}
    intake=json.loads(Path("/etc/ziji-fabric/income/intake.json").read_text())
    folder=intake["drive_folder"]
    if not re.fullmatch(r"[A-Za-z0-9_-]+",folder):raise ValueError("folder_id_invalid")
    url="https://www.googleapis.com/drive/v3/files/"+folder+"?fields=id,mimeType,capabilities(canAddChildren)"
    req=urllib.request.Request(url,headers={"Authorization":"Bearer "+token["access_token"],
        "User-Agent":"ZijiFabric/1.0 (career research)","Accept":"application/json"})
    try:
        with urllib.request.urlopen(req,timeout=30) as response:value=json.load(response)
        report.update(folder_read="PASS",can_add_children=value.get("capabilities",{}).get("canAddChildren"))
    except urllib.error.HTTPError as exc:
        report["folder_http_status"]=exc.code
        try:
            value=json.loads(exc.read(16000));errors=value.get("error",{}).get("errors",[])
            report["provider_reason_codes"]=[x["reason"] for x in errors if re.fullmatch(r"[A-Za-z0-9_]{1,80}",x.get("reason",""))]
        except Exception:report["provider_reason_codes"]=[]
    report["private_export"]=encrypted_private_export()
    return report


def candidate():
    """Probe current GitHub OAuth candidate in memory; never install credentials."""
    report={"stage":"INCOME_OAUTH_CANDIDATE","provider_writes":0,"model_calls":0,"binding_changed":False}
    keys=("GOOGLE_CLIENT_ID","GOOGLE_CLIENT_SECRET","GOOGLE_REFRESH_TOKEN")
    if any(not os.environ.get(k) for k in keys):
        return {**report,"outcome":"BLOCKED","reason":"candidate_incomplete"}
    report["expected_client_match"]=hashlib.sha256(os.environ[keys[0]].encode()).hexdigest()=="6d2db9801e2c9c90792968ccb8974b0806616bd4daf1df196e0e5f7636fad3e2"
    if not report["expected_client_match"]:
        return {**report,"outcome":"BLOCKED","reason":"candidate_client_mismatch"}
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,*args,**kwargs):return None
    opener=urllib.request.build_opener(NoRedirect)
    stage="refresh"
    try:
        data=urllib.parse.urlencode({"grant_type":"refresh_token","client_id":os.environ[keys[0]],
              "client_secret":os.environ[keys[1]],"refresh_token":os.environ[keys[2]]}).encode()
        with opener.open(urllib.request.Request("https://oauth2.googleapis.com/token",data=data),timeout=30) as r:
            token=json.load(r)
        scopes=token.get("scope","").split()
        report["drive_scope_names"]=[s.rsplit("/",1)[-1] for s in scopes if s.startswith("https://www.googleapis.com/auth/drive")]
        report["drive_write_scope_present"]=bool(set(scopes)&{"https://www.googleapis.com/auth/drive","https://www.googleapis.com/auth/drive.file"})
        headers={"Authorization":"Bearer "+token["access_token"]}
        stage="identity"
        with opener.open(urllib.request.Request("https://www.googleapis.com/drive/v3/about?fields=user(emailAddress)",headers=headers),timeout=30) as r:
            identity=json.load(r)
        report["identity_match"]=identity.get("user",{}).get("emailAddress","").lower()=="restorationconsultantsoutreach@gmail.com"
        if not report["identity_match"]:return {**report,"outcome":"BLOCKED","reason":"identity_mismatch"}
        stage="folder"
        url="https://www.googleapis.com/drive/v3/files/1j_pa8A2sXBSA9vy87B6nyKo-myZ3HDaE?fields=id,mimeType,capabilities(canAddChildren)&supportsAllDrives=true"
        with opener.open(urllib.request.Request(url,headers=headers),timeout=30) as r:folder=json.load(r)
        report["folder_read"]="PASS"
        report["can_add_children"]=folder.get("capabilities",{}).get("canAddChildren",False)
        report["outcome"]="READY_FOR_REVIEW" if report["drive_write_scope_present"] and report["can_add_children"] else "BLOCKED"
        return report
    except urllib.error.HTTPError as exc:
        report.update(outcome="BLOCKED",failed_stage=stage,http_status=exc.code)
        try:
            error=json.loads(exc.read(16000)).get("error")
            if error in ("invalid_grant","invalid_client","unauthorized_client","invalid_scope","access_denied"):
                report["oauth_error"]=error
        except Exception:pass
        return report
    except Exception:
        return {**report,"outcome":"BLOCKED","failed_stage":stage,"reason":"request_or_response_error"}


if __name__=="__main__":
    result=candidate()
    print(json.dumps(result,sort_keys=True))
    raise SystemExit(0 if result["outcome"]=="READY_FOR_REVIEW" else 1)
