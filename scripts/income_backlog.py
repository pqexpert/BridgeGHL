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
    if stop not in (True, "YES", "Yes", "yes", "true"): return None, "explicit_no_submit_missing"
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

def inspect_manifest(root=ROOT):
    rows,counts = manifest(root)
    reasons = collections.Counter()
    candidates = 0
    for sid,row,_ in rows:
        value,reason = facts(row,sid)
        if value and counts[role_url(row)] == 1: candidates += 1
        else: reasons[reason or "duplicate_role_identity"] += 1
    return {"read_only":True,"provider_calls":0,"model_calls":0,
            "discovery_records":len(rows),"historical_candidate_count":candidates,
            "historical_rejection_counts":dict(reasons),
            "property_names":sorted({k for _,row,_ in rows for k in row}),
            "current_native_candidates_verified":False}

def read_backlog(cursor, limit, *, location_id, pipeline_id, stage_id,
                 default_contact_id, journal_path, call, root=ROOT):
    from ingestion import read_income_source, SourceReadError
    rows,counts = manifest(root)
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
    for sid,discovery,binding in scan:
        # Discovery filters save calls; never establish current eligibility.
        preliminary,reason=facts(discovery,sid)
        if reason: reasons[reason]+=1;continue
        if counts[role_url(discovery)] != 1: reasons["duplicate_role_identity"]+=1;continue
        if binding["contact_id"] != default_contact_id:
            reasons["non_internal_context_record"]+=1;continue
        ref={"source_id":sid,"contact_id":binding["contact_id"],"opportunity_id":binding["opportunity_id"]}
        try:
            current=read_income_source(ref,location_id,pipeline_id,default_contact_id,journal_path,cached)
            values,reason=facts(current["source"]["properties"],sid)
            if reason:reasons[reason]+=1;continue
            if values != preliminary:reasons["source_changed_from_discovery_requires_review"]+=1;continue
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
