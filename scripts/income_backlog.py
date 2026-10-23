"""Read existing imported Income opportunities for bounded native research selection.

The import manifest locates records only. Current native notes, opportunity state,
and the existing verified ingestion journal establish facts; this module grants no
execution authority and performs no provider writes.
"""
import collections
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

ROOT = Path("/var/lib/bridgeghl/career-import")

def normalized(value):
    return re.sub(r"[^a-z0-9]", "", str(value).lower())

def role_url(properties):
    matches = set()
    for value in properties.values():
        if not isinstance(value, str): continue
        for url in re.findall(r"https://[^\s<>\"\)]+", value):
            part = urlsplit(url.rstrip(".,;"))
            match = re.fullmatch(r"/([a-z0-9-]{1,60})/([a-f0-9-]{36})/?", part.path)
            if part.hostname in ("jobs.ashbyhq.com", "jobs.lever.co") and match:
                matches.add(("ashby" if part.hostname == "jobs.ashbyhq.com" else "lever",
                             match.group(1), match.group(2)))
    return next(iter(matches)) if len(matches) == 1 else None

def facts(properties, source_id):
    p = {normalized(k): v for k,v in properties.items()}
    stop = p.get("donotautosubmit", p.get("donotsubmit"))
    # Exact checkbox encodings observed in the imported canonical source.
    if not (stop is True or isinstance(stop,str) and stop.strip().lower() in ("yes","true","__yes__")):
        return None, "explicit_no_submit_missing"
    for key,value in p.items():
        if any(s in key for s in ("assetlink","intelbrief","nativeartifact")) and value not in (None,"",[],False):
            return None, "artifact_already_recorded"
    if normalized(p.get("status","")) not in ("harvested","research","sourceintake","new","researching"):
        return None, "not_unfinished_research"
    role = role_url(properties)
    if role is None: return None, "supported_role_url_missing_or_ambiguous"
    title = p.get("role")
    if not isinstance(title,str) or not title.strip(): return None, "role_title_missing"
    packet = p.get("packetid") or ("NATIVE-" + source_id.upper())
    if not isinstance(packet,str) or not re.fullmatch(r"[A-Za-z0-9-]{1,100}",packet):
        return None, "packet_identity_invalid"
    return dict(zip(("provider","board","job_id"),role),
                title=title.strip(),packet_id=packet,
                do_not_submit=True,duplicate_clear=True,artifact_absent=True), None

def manifest(root=ROOT):
    source = json.loads((root/"source.json").read_text())
    collection = source["collections"]["job_applications"]
    if collection.get("complete") is not True or len(collection["rows"]) > 1000:
        raise ValueError("discovery_manifest_incomplete")
    receipts = json.loads((root/"receipt.json").read_text())
    bindings = {}
    for row in receipts:
        result = row.get("result",{})
        if result.get("verified") and result.get("opportunity_id"):
            sid = row["source_id"]
            if sid in bindings and bindings[sid] != result: raise ValueError("discovery_binding_ambiguous")
            bindings[sid] = result
    rows = []
    counts = collections.Counter()
    for row in collection["rows"]:
        url = row.get("url","")
        sid = url.split("?")[0].rstrip("/").split("/")[-1].replace("-","")
        if not re.fullmatch(r"[a-f0-9]{32}",sid) or sid not in bindings: continue
        role = role_url(row)
        if role: counts[role] += 1
        rows.append((sid,row,bindings[sid]))
    return sorted(rows),counts


LOCATOR = Path("/home/bridgeadmin/apps/BridgeGHL/config/income-discovery.json")

def discovery_manifest(root=ROOT):
    doc=json.loads(LOCATOR.read_text())
    if doc.get("schema") != 1 or not isinstance(doc.get("records"),list) or len(doc["records"])>1000:
        raise ValueError("locator_shape_invalid")
    rows=[];seen=set()
    for row in doc["records"]:
        if set(row)!={"source_id","contact_id","opportunity_id"} or any(
                not isinstance(v,str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}",v) for v in row.values()):
            raise ValueError("locator_identity_invalid")
        if row["source_id"] in seen:raise ValueError("locator_duplicate")
        seen.add(row["source_id"])
        rows.append((row["source_id"],{},{"contact_id":row["contact_id"],"opportunity_id":row["opportunity_id"]}))
    return sorted(rows),{}

def install_locator():
    """Project identifiers only; full import remains root-private."""
    import os,pwd,grp,subprocess,tempfile
    if os.geteuid()!=0:raise ValueError("root_required")
    user=subprocess.check_output(["systemctl","show","bridgeghl.service","-p","User","--value"],text=True).strip() or "root"
    group=subprocess.check_output(["systemctl","show","bridgeghl.service","-p","Group","--value"],text=True).strip()
    account=pwd.getpwuid(int(user)) if user.isdigit() else pwd.getpwnam(user)
    gid=(int(group) if group.isdigit() else grp.getgrnam(group).gr_gid) if group else account.pw_gid
    rows,_=manifest()
    records=[{"source_id":sid,"contact_id":binding["contact_id"],"opportunity_id":binding["opportunity_id"]}
             for sid,_,binding in rows]
    if LOCATOR.is_symlink() or LOCATOR.parent.is_symlink():raise ValueError("locator_symlink")
    doc={"schema":1,"records":records}
    fd,tmp=tempfile.mkstemp(dir=LOCATOR.parent)
    try:
        with os.fdopen(fd,"w") as stream:
            json.dump(doc,stream,sort_keys=True);stream.write("\n");stream.flush();os.fsync(stream.fileno())
        os.chown(tmp,0,gid);os.chmod(tmp,0o640);os.replace(tmp,LOCATOR)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)
    # Actual service identity must read the projection, without printing IDs.
    code="import json;from pathlib import Path;d=json.loads(Path("+repr(str(LOCATOR))+").read_text());assert d['schema']==1"
    subprocess.run(["runuser","-u",account.pw_name,"--","python3","-c",code],check=True,capture_output=True)
    return {"locator_records":len(records),"service_user":account.pw_name,"locator_mode":"0640",
            "service_readback":True,"full_private_export_exposed":False}

def binding_diagnosis():
    """Root-only read diagnosis: no secret contents, model calls or writes."""
    import importlib.util,stat,urllib.error
    env=Path('/etc/bridgeghl/bridgeghl.env')
    result={"read_only":True,"model_calls":0,"provider_mutations":0}
    if env.exists():
        meta=env.lstat()
        result['binding_file']={"uid":meta.st_uid,"gid":meta.st_gid,"mode":oct(stat.S_IMODE(meta.st_mode)),
                                "regular":stat.S_ISREG(meta.st_mode),"symlink":env.is_symlink()}
    else:result['binding_file']={"exists":False}
    stage='load_installed_selector'
    try:
        spec=importlib.util.spec_from_file_location('native_backlog_diagnosis',
            '/opt/ziji-fabric/dispatch/continuation/native_income_backlog.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        stage='live_authority';module.authority();result[stage]='VERIFIED'
        stage='current_bridge_backlog'
        native=module.bridge('/read/ingest/income-backlog',{'cursor':0,'limit':8})
        result[stage]={k:native.get(k) for k in ('scanned','next_cursor','total_discovery_records','rejection_counts','coverage')}
        result[stage]['candidate_count']=len(native.get('items',[]))
        result['status']='VERIFIED'
    except Exception as exc:
        safe={'native_research_authority_changed','jira_binding_scope_mismatch',
              'bridge_binding_private_file_required','bridge_binding_unavailable','bridge_response_too_large'}
        result.update(status='DEFERRED',stage=stage,error_type=type(exc).__name__)
        if isinstance(exc,ValueError) and str(exc) in safe:result['reason']=str(exc)
        if isinstance(exc,urllib.error.HTTPError):
            result['http_status']=exc.code
            try:
                detail=json.loads(exc.read(2048)).get('detail')
                if detail in {'income_backlog_permission_denied','income_backlog_locator_missing','income_backlog_journal_unavailable','income_backlog_source_unavailable'}:
                    result['reason']=detail
            except (ValueError,TypeError,AttributeError):pass
    return result

def inspect_manifest(root=ROOT):
    rows,counts = manifest(root)
    reasons = collections.Counter()
    candidates = 0
    for sid,row,_ in rows:
        value,reason = facts(row,sid)
        if value and counts[role_url(row)] == 1: candidates += 1
        else: reasons[reason or "duplicate_role_identity"] += 1
    no_submit=collections.Counter(type(row.get("Do Not Auto Submit")).__name__+":"+str(row.get("Do Not Auto Submit"))[:60] for _,row,_ in rows)
    return {"read_only":True,"provider_reads":"bounded_live_diagnosis","model_calls":0,
            "no_submit_value_distribution":dict(no_submit),
            "binding_diagnosis":binding_diagnosis(),
            "discovery_records":len(rows),"historical_candidate_count":candidates,
            "historical_rejection_counts":dict(reasons),
            "property_names":sorted({k for _,row,_ in rows for k in row}),
            "current_native_candidates_verified":False}

def read_backlog(cursor, limit, *, location_id, pipeline_id, stage_id,
                 default_contact_id, journal_path, call, root=ROOT):
    from ingestion import read_income_source, SourceReadError,assemble,source_key,verified_notes
    rows,counts = discovery_manifest(root)
    if not rows: return {"items":[],"next_cursor":0,"scanned":0,"coverage":"verified_import_manifest_only"}
    start = cursor % len(rows)
    scan = [rows[(start+i)%len(rows)] for i in range(min(limit,len(rows)))]
    items,reasons = [],collections.Counter()
    # Reuse list/contact GETs within this pass only; exact selected note GETs and
    # opportunity second-read remain uncached for native CAS.
    cache = {}
    def cached(method,path,**kwargs):
        cacheable = method == "GET" and path.startswith("/contacts/") and (path.endswith("/notes") or path.count("/")==2)
        if cacheable and path in cache:return cache[path]
        result=call(method,path,**kwargs)
        if cacheable:cache[path]=result
        return result
    # The historical export locates IDs only. Read current native notes once for
    # the shared internal context, then derive current eligibility and duplicates.
    notes=cached("GET","/contacts/"+default_contact_id+"/notes").get("notes")
    if not isinstance(notes,list) or len(notes)>10000:raise ValueError("source_notes_unavailable_or_unbounded")
    verified=verified_notes(journal_path,default_contact_id)
    current_sources={};current_counts=collections.Counter()
    for sid,_,binding in rows:
        if binding["contact_id"]!=default_contact_id:continue
        try:
            versions=assemble(notes,source_key(sid),default_contact_id,verified)
            if len(versions)!=1:continue
            source=next(iter(versions.values()))["source"]
            current_sources[sid]=source
            role=role_url(source["properties"])
            if role:current_counts[role]+=1
        except SourceReadError:continue
    for sid,discovery,binding in scan:
        if binding["contact_id"] != default_contact_id:
            reasons["non_internal_context_record"]+=1;continue
        if sid not in current_sources:
            reasons["current_source_missing_or_ambiguous"]+=1;continue
        preliminary,reason=facts(current_sources[sid]["properties"],sid)
        if reason:reasons[reason]+=1;continue
        if current_counts[role_url(current_sources[sid]["properties"])]!=1:
            reasons["duplicate_current_role_identity"]+=1;continue
        ref={"source_id":sid,"contact_id":binding["contact_id"],"opportunity_id":binding["opportunity_id"]}
        try:
            current=read_income_source(ref,location_id,pipeline_id,default_contact_id,journal_path,cached)
            values,reason=facts(current["source"]["properties"],sid)
            if reason:reasons[reason]+=1;continue
            if values != preliminary:reasons["current_source_changed_during_read"]+=1;continue
            opportunity=call("GET","/opportunities/"+ref["opportunity_id"]).get("opportunity",{})
            if opportunity.get("status")!="open" or opportunity.get("pipelineStageId")!=stage_id:
                reasons["native_opportunity_not_intake"]+=1;continue
            aliases=[u for u in current["legacy_provenance_urls"] if re.fullmatch(r"https://app.notion.com/p/[a-f0-9]{32}",u)]
            if len(set(aliases))!=1:reasons["legacy_identity_ambiguous"]+=1;continue
            items.append({"reference":{**ref,"location_id":location_id,"current_source_sha256":current["source_sha256"]},
                          "facts":values,"tracker_url":aliases[0],
                          "native_opportunity_updated":opportunity.get("updatedAt"),
                          "native_readback":True})
        except SourceReadError as exc:
            reasons[str(exc)]+=1
    return {"items":items,"next_cursor":(start+len(scan))%len(rows),"scanned":len(scan),
            "total_discovery_records":len(rows),"rejection_counts":dict(reasons),
            "coverage":"verified_import_manifest_only","provider_mutations":0,"model_calls":0}
