"""Isolated existing Shopify credentials probe; identity/scopes only, no store writes."""
import json
import os
from pathlib import Path
import re
import subprocess

def probe(config,provider,base):
    target=base/"shopify"/"credentials.json"
    existing=target.exists() or target.is_symlink()
    try:
        receipt,binding=provider.shopify(config)
        if "read_products" not in receipt["granted_scopes"] and "write_products" not in receipt["granted_scopes"]:
            return {"status":"BLOCKED","reason":"catalog_scope_missing","identity_verified":True,
                    "binding":"not_changed","existing_binding_present":existing,"store_writes":0}
        installed=provider.persist(base,"shopify",binding)
        return {"status":"VERIFIED",**receipt,"binding":installed,
                "existing_binding_present":existing,"catalog_read_authorized":True,"store_writes":0}
    except provider.ProbeError as exc:
        return {"status":"BLOCKED","reason":str(exc),"binding":"not_changed",
                "existing_binding_present":existing,"store_writes":0}

def server(payload):
    if os.geteuid()!=0:raise ValueError("root_required")
    import types
    provider=types.ModuleType("store_provider")
    exec(compile(payload["provider_source"],"store_provider","exec"),provider.__dict__)
    receipt=probe(payload["config"],provider,Path("/etc/ziji-fabric/providers"))
    return {"stage":"NATIVE_STORE_READ_ONLY_BINDING",**receipt,"other_bindings_changed":False,
            "scheduled_worker_enabled":False,"connector_credentials_exported":False}

def deploy():
    user,host=os.environ["REMOTE_USER"],os.environ["REMOTE_HOST"]
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*",user) or not re.fullmatch(r"[A-Za-z0-9.:-]+",host):
        raise ValueError("ssh_target_invalid")
    payload={"source":Path(__file__).read_text(),
        "provider_source":Path(__file__).with_name("bind_native_providers.py").read_text(),
        "config":{k:os.environ.get(k,"").strip() for k in ("SHOPIFY_CLIENT_ID","SHOPIFY_CLIENT_SECRET","SHOPIFY_SHOP_DOMAIN")}}
    remote="sudo -n python3 -c 'import json,sys;p=json.load(sys.stdin);s={\"__name__\":\"store_probe\"};exec(compile(p[\"source\"],\"store_probe\",\"exec\"),s);print(json.dumps(s[\"server\"](p),sort_keys=True))'"
    cmd=["ssh","-i",str(Path.home()/".ssh/fabric-preflight"),"-o","BatchMode=yes","-o","ConnectTimeout=15",
        "-o","IdentitiesOnly=yes","-o","StrictHostKeyChecking=yes",
        "-o","UserKnownHostsFile="+str(Path.home()/".ssh/fabric-known-hosts"),user+"@"+host,remote]
    result=subprocess.run(cmd,input=json.dumps(payload),text=True,capture_output=True,timeout=120)
    if result.returncode:raise ValueError("remote_store_probe_failed")
    receipt=json.loads(result.stdout)
    if receipt.get("stage")!="NATIVE_STORE_READ_ONLY_BINDING":raise ValueError("receipt_invalid")
    print(json.dumps(receipt,sort_keys=True))

if __name__=="__main__":
    try:deploy()
    except Exception:
        print('{"stage":"NATIVE_STORE_READ_ONLY_BINDING","status":"FAIL","reason":"transport_or_local_error_details_withheld"}')
        raise SystemExit(1)
