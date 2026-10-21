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
