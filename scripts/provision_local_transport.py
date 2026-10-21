"""Provision a restricted loopback SSH link through existing deployment credentials."""
import argparse, base64, hashlib, importlib.util, json, os, pathlib, re, subprocess, sys, tempfile
ROOT=pathlib.Path(globals().get("__file__","/opt/provision.py")).resolve().parents[1]
DEST=pathlib.Path("/opt/ziji-fabric/local-link")
def command(argv, **kw):
    return subprocess.run(argv,check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,**kw)
def write(path,data,mode=0o600):
    path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_bytes(data);path.chmod(mode)
def remote(payload):
    if os.geteuid()!=0:raise ValueError("root_required")
    files=payload["files"]
    if payload["phase"]=="provision":
        try:command(["id","ziji-local-link"])
        except subprocess.CalledProcessError:
            command(["useradd","--create-home","--shell","/bin/sh","ziji-local-link"])
        for name in ("local_recovery_snapshot.py","local_inference_transport.py"):
            write(DEST/name,base64.b64decode(files[name]),0o644)
        try:command(["python3","-c","from cryptography.hazmat.primitives.ciphers.aead import AESGCM"])
        except subprocess.CalledProcessError:
            command(["apt-get","update"])
            command(["apt-get","install","-y","python3-cryptography"])
        directory=pathlib.Path("/var/lib/ziji-fabric/local-recovery/duck")
        directory.mkdir(parents=True,exist_ok=True);directory.chmod(0o700)
        command(["chown","ziji-local-link:ziji-local-link",str(directory)])
        pub=files["public_key"].strip()
        if not re.fullmatch(r"ssh-ed25519 [A-Za-z0-9+/=]+(?: [A-Za-z0-9_-]+)?",pub):raise ValueError("public_key")
        home=pathlib.Path("/home/ziji-local-link/.ssh")
        home.mkdir(mode=0o700,exist_ok=True);home.chmod(0o700)
        options='restrict,port-forwarding,permitlisten="127.0.0.1:18182",command="/usr/bin/python3 /opt/ziji-fabric/local-link/local_recovery_snapshot.py --receive"'
        write(home/"authorized_keys",(options+" "+pub+"\n").encode())
        command(["chown","-R","ziji-local-link:ziji-local-link",str(home)])
        conf=pathlib.Path("/etc/ssh/sshd_config.d/60-ziji-local-link.conf")
        text="Match User ziji-local-link\n    AuthenticationMethods publickey\n    PasswordAuthentication no\n    AllowTcpForwarding remote\n    GatewayPorts no\n    PermitListen 127.0.0.1:18182\n    PermitOpen none\n    X11Forwarding no\n    AllowAgentForwarding no\n    PermitTTY no\nMatch all\n"
        prior=conf.read_bytes() if conf.exists() else None
        write(conf,text.encode(),0o644)
        try:
            command(["/usr/sbin/sshd","-t"])
            effective=command(["/usr/sbin/sshd","-T","-C","user=ziji-local-link,host=localhost,addr=127.0.0.1"],text=True).stdout
            for expected in ("allowtcpforwarding remote","gatewayports no","permitopen none","permitlisten 127.0.0.1:18182"):
                if expected not in effective.splitlines():raise ValueError("sshd_scope_not_applied")
        except Exception:
            if prior is None:conf.unlink()
            else:write(conf,prior,0o644)
            raise
        command(["systemctl","reload","ssh"])
        return {"status":"PROVISIONED","source_sha":payload["source_sha"],"listen":"127.0.0.1:18182","account":"ziji-local-link"}
    spec=importlib.util.spec_from_file_location("recovery",DEST/"local_recovery_snapshot.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    manifest,restored=module.read_latest(pathlib.Path("/var/lib/ziji-fabric/local-recovery/duck"))
    target=pathlib.Path("/etc/ziji-fabric/local-inference");target.mkdir(mode=0o700,parents=True,exist_ok=True);target.chmod(0o700)
    mapping={"model-lock.json":"config/model-lock.json","system.txt":"config/system.txt","sources.json":"config/sources.json","memory.json":"state/memory.json","api-key":"state/api-key"}
    hashes={}
    for name,original in mapping.items():
        data=restored[original]
        if name!="api-key":hashes[name]=hashlib.sha256(data).hexdigest()
        write(target/name,data)
    write(target/"input-lock.json",json.dumps(hashes,sort_keys=True).encode())
    spec=importlib.util.spec_from_file_location("client",DEST/"local_inference_transport.py")
    client=importlib.util.module_from_spec(spec);spec.loader.exec_module(client)
    result=client.run({"request_id":"transport-proof-"+payload["source_sha"][:12],"lane":"engineering","prompt":"Reply with a brief statement that you provide proposals only and cannot execute external actions."})
    return {"status":result["status"],"input_hashes":hashes,"paid_fallback":False,"external_effects":False,"proposal_sha256":hashlib.sha256(result.get("proposal","").encode()).hexdigest()}
def main():
    parser=argparse.ArgumentParser();parser.add_argument("--phase",choices=["provision","verify"],default="provision");parser.add_argument("--remote",action="store_true");args=parser.parse_args()
    if args.remote:
        try:print(json.dumps(remote(json.load(sys.stdin)),sort_keys=True))
        except Exception as exc:
            print(json.dumps({"status":"FAILED","error":type(exc).__name__}));sys.exit(1)
        return
    host=os.environ["REMOTE_HOST"];user=os.environ["REMOTE_USER"]
    if not re.fullmatch(r"[A-Za-z0-9.-]+",host) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*",user):raise ValueError("target")
    key=os.environ["SSH_PRIVATE_KEY"].strip()
    if not key.startswith("-----BEGIN"):key=base64.b64decode(key).decode()
    files={name:base64.b64encode((ROOT/"scripts"/name).read_bytes()).decode() for name in ("local_recovery_snapshot.py","local_inference_transport.py")}
    files["public_key"]=(ROOT/"config/local-transport/duck.pub").read_text()
    payload={"phase":args.phase,"files":files,"source_sha":command(["git","rev-parse","HEAD"],cwd=ROOT,text=True).stdout.strip()}
    with tempfile.TemporaryDirectory() as directory:
        d=pathlib.Path(directory);write(d/"key",(key+"\n").encode())
        pin=(ROOT/"config/local-transport/server-host-key.pub").read_text().strip()
        write(d/"known_hosts",(host+" "+pin+"\n").encode())
        code=base64.b64encode(pathlib.Path(__file__).read_bytes()).decode()
        # The base64 alphabet has no shell metacharacters; double quotes preserve its literal.
        remote_cmd='sudo -n python3 -c "import base64;exec(compile(base64.b64decode(\''+code+'\'),\'/opt/provision.py\',\'exec\')))" --remote'
        result=subprocess.run(["ssh","-i",str(d/"key"),"-o","IdentitiesOnly=yes","-o","BatchMode=yes","-o","ConnectTimeout=15","-o","StrictHostKeyChecking=yes","-o","UserKnownHostsFile="+str(d/"known_hosts"),user+"@"+host,remote_cmd],input=json.dumps(payload),text=True,capture_output=True,timeout=480)
        for line in result.stdout.splitlines():
            if line.startswith("{"):print(line)
        if result.returncode:print(json.dumps({"status":"FAILED","stage":"restricted_transport_remote","exit_code":result.returncode}))
        sys.exit(result.returncode)
if __name__=="__main__":main()
