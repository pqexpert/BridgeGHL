"""Deterministic RSC merged-PR evidence; no model, domain writes or notifications."""
import base64, hashlib, json, os, pathlib, re, shlex, subprocess, sys, tempfile, urllib.request
REPOS={"pqexpert/BridgeGHL","pqexpert/rsc-operations-console"}
STATE=pathlib.Path("/var/lib/ziji-fabric/merge-handoff")
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None
def api(path):
    if not path.startswith("/repos/"):raise ValueError("github_path")
    req=urllib.request.Request("https://api.github.com"+path,headers={"Authorization":"Bearer "+os.environ["SOURCE_TOKEN"],"Accept":"application/vnd.github+json","X-GitHub-Api-Version":"2022-11-28"})
    with urllib.request.build_opener(NoRedirect).open(req,timeout=25) as response:return json.load(response)
def target(pr):
    text=(pr.get("body") or "")+"\n"+pr["head"]["ref"]
    keys=set(re.findall(r"https://restoration-mt\.atlassian\.net/browse/(RBO-[1-9][0-9]*)(?![0-9])",text))
    keys.update(re.findall(r"(?<![A-Za-z0-9])RBO-[1-9][0-9]*(?![A-Za-z0-9])",pr["head"]["ref"]))
    # Reuse the existing explicitly scoped native receipt writer.
    if len(keys)==1 and next(iter(keys)) in {"RBO-97","RBO-34"}:
        return next(iter(keys)),"EXPLICIT",sorted(keys)
    return "RBO-97","UNROUTED",sorted(keys)
def gather(repo,number):
    if repo not in REPOS or not isinstance(number,int) or number<1:raise ValueError("scope")
    meta=api("/repos/"+repo);pr=api("/repos/"+repo+"/pulls/"+str(number))
    if not pr.get("merged") or pr["base"]["ref"]!=meta["default_branch"] or pr["base"]["repo"]["full_name"]!=repo:
        return {"status":"NO_OP","reason":"not_merged_default_branch"}
    sha=pr["merge_commit_sha"]
    if not re.fullmatch("[0-9a-f]{40}",sha):raise ValueError("merge_sha")
    check=api("/repos/"+repo+"/commits/"+sha+"/check-runs?per_page=100")
    runs=api("/repos/"+repo+"/actions/runs?head_sha="+sha+"&per_page=100")
    issue,routing,candidates=target(pr)
    sources={}
    for source_repo,path in [(repo,"AGENTS.md"),("pqexpert/FungibleRD-MVP","docs/llmlms/CAPABILITY-SKILL-CATALOG.md"),("pqexpert/FungibleRD-MVP","skills/wise-action-governor/SKILL.md")]:
        doc=api("/repos/"+source_repo+"/contents/"+path);sources[source_repo+"/"+path]=doc["sha"]
    revision=api("/repos/"+repo+"/pulls/"+str(number))
    if not revision.get("merged") or revision["merge_commit_sha"]!=sha or revision["base"]["ref"]!=meta["default_branch"]:raise ValueError("pre_effect_pr_changed")
    evidence=[{"name":x["name"],"status":x["status"],"conclusion":x["conclusion"],"url":x["html_url"]} for x in check.get("check_runs",[]) if x["head_sha"]==sha]
    workflows=[{"name":x["name"],"status":x["status"],"conclusion":x["conclusion"],"url":x["html_url"]} for x in runs.get("workflow_runs",[]) if x["head_sha"]==sha]
    receipt={"lane":"rsc","action_class":"merge_handoff","outcome":"SUCCESS","repository":repo,"pr_number":number,
      "merge_sha":sha,"pr_head_sha":pr["head"]["sha"],"merged_at":pr["merged_at"],"pr_url":pr["html_url"],
      "checked_revision":sha,"source_versions":sources,"routing":routing,"explicit_issue_candidates":candidates,
      "checks":evidence,"workflow_runs":workflows,"check_coverage_complete":check.get("total_count",0)<=100,
      "workflow_coverage_complete":runs.get("total_count",0)<=100,"deployment_state":"PENDING_VERIFICATION",
      "native_acceptance_state":"PENDING_VERIFICATION","completed_fact":"Default-branch merge verified; CI results are revision-specific evidence only.",
      "next_action":"Owning RSC cycle verifies actual deployment and native acceptance; no automatic closeout.",
      "model_calls":0,"external_messages":0,"domain_writes":0}
    return {"status":"READY","issue":issue,"replay_id":"github:"+repo+":pr:"+str(number)+":merge:"+sha,"receipt":receipt}
def server(payload):
    import fcntl
    sys.path.insert(0,"/opt/ziji-fabric/native")
    from collect_native_sources import atomic
    if os.geteuid()!=0:raise ValueError("root_required")
    STATE.mkdir(parents=True,exist_ok=True,mode=0o700);STATE.chmod(0o700)
    enabled=STATE/"enabled"
    mode=payload["mode"]
    if mode in {"activate","standby"}:
        if mode=="activate":atomic(enabled,"enabled\n")
        elif enabled.exists():enabled.unlink()
        return {"status":"ACTIVE" if enabled.exists() else "STANDBY","model_calls":0}
    if mode=="event" and not enabled.exists():return {"status":"STANDBY","model_calls":0}
    event=payload["event"]
    if event["status"]!="READY":return event
    receipt=event["receipt"]
    if receipt["repository"] not in REPOS or receipt["action_class"]!="merge_handoff":raise ValueError("event_scope")
    if mode=="observe":return {"status":"READY","routing":receipt["routing"],"model_calls":0,"jira_writes":0}
    from native_jira_receipt import publish
    identity=event["replay_id"];cache=STATE/(hashlib.sha256(identity.encode()).hexdigest()+".json")
    with (STATE/"event.lock").open("a") as lock:
        os.chmod(lock.name,0o600);fcntl.flock(lock,fcntl.LOCK_EX)
        # A merge event has immutable identity. Replays reuse its initial observation.
        if cache.exists():event=json.loads(cache.read_text())
        else:atomic(cache,json.dumps(event,sort_keys=True))
        result=publish(event["issue"],identity,event["receipt"])
    return dict(result,issue=event["issue"],model_calls=0,domain_writes=0)
def main():
    mode=os.environ.get("HANDOFF_MODE","event")
    if mode not in {"event","observe","prove","activate","standby"}:raise ValueError("mode")
    event={}
    if mode in {"event","observe","prove"}:
        if os.environ.get("GITHUB_EVENT_NAME")=="pull_request_target":
            raw=json.loads(pathlib.Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
            repo=raw["repository"]["full_name"];number=raw["number"]
        else:repo=os.environ["HANDOFF_REPO"];number=int(os.environ["HANDOFF_PR"])
        event=gather(repo,number)
    host,user=os.environ["REMOTE_HOST"],os.environ["REMOTE_USER"]
    if not re.fullmatch("[A-Za-z0-9.-]+",host) or not re.fullmatch("[A-Za-z_][A-Za-z0-9_-]*",user):raise ValueError("ssh_target")
    key=os.environ["SSH_PRIVATE_KEY"].strip().replace("\\n","\n")
    if "PRIVATE KEY" not in key:key=base64.b64decode(key).decode()
    with tempfile.TemporaryDirectory() as temp:
        root=pathlib.Path(temp);keypath=root/"key";keypath.write_text(key+"\n");keypath.chmod(0o600)
        pin=(pathlib.Path(__file__).resolve().parents[1]/"config/local-transport/server-host-key.pub").read_text().strip()
        known=root/"known";known.write_text(host+" "+pin+"\n")
        code=base64.b64encode(pathlib.Path(__file__).read_bytes()).decode()
        runner="import base64,json,sys; s={'__name__':'remote_handoff'}; exec(compile(base64.b64decode("+repr(code)+"),'handoff','exec'),s); print(json.dumps(s['server'](json.load(sys.stdin)),sort_keys=True))"
        cmd=["ssh","-i",str(keypath),"-o","BatchMode=yes","-o","IdentitiesOnly=yes","-o","ConnectTimeout=15","-o","StrictHostKeyChecking=yes","-o","UserKnownHostsFile="+str(known),user+"@"+host,"sudo -n python3 -c "+shlex.quote(runner)]
        result=subprocess.run(cmd,input=json.dumps({"mode":mode,"event":event}),text=True,capture_output=True,timeout=240)
        if result.returncode:raise ValueError("remote_handoff_failed")
        receipt=json.loads(result.stdout);print(json.dumps(receipt,sort_keys=True))
        if receipt.get("status")=="UNKNOWN":raise ValueError("receipt_unknown")
if __name__=="__main__":
    try:main()
    except Exception as exc:
        print(json.dumps({"status":"FAILED","error":str(exc) if isinstance(exc,ValueError) else type(exc).__name__,"model_calls":0}));raise SystemExit(1)
