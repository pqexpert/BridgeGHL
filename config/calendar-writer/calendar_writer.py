"""Native Distiller: bounded feedback ingestion and the sole description writer.

Model output can only select an already admitted source candidate. Native code
owns credentials, allowlists, timing, suffix preservation, CAS and readback.
"""
import argparse
import base64
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import html
import json
import os
from pathlib import Path
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo
import calendar_handover as handover

UTC=timezone.utc
ET=ZoneInfo("America/New_York")
SLOTS={"05:15":"DELTA","07:00":"DELTA","11:45":"FULL","15:00":"DELTA","17:00":"DELTA","20:45":"DELTA"}
STATE=Path("/var/lib/ziji-fabric/calendar-writer")
CONFIG=Path("/etc/ziji-fabric/providers/google/calendar-handover.json")
ACTIVATION=Path("/etc/ziji-fabric/calendar-writer-activation.json")
JIRA="https://api.atlassian.com/ex/jira/20f21ae4-e364-40df-b5a1-b2512fd862d6"
GAPI="https://www.googleapis.com/calendar/v3"
LANES={"income","rsc","store","academic","private","distiller"}
PROTECTED_WORDS=re.compile(r"kalachakra|dharma|sadhana|practice|sleep|health|pepper",re.I)
RELEVANT=re.compile(r"emir|academic|study|assignment|rsc|restoration|dzokden|store|service|income|career|recruit|job|evening|relationship",re.I)

def utcnow():return datetime.now(UTC)
def iso(x):return x.isoformat()
def hashobj(x):return handover.digest(json.dumps(x,sort_keys=True,separators=(",",":")))
def save(path,value):
    path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    handover.private_write(path,value)
def read(path,default=None):
    return handover.private_read(path) if path.exists() else default
def text(node):
    if isinstance(node,dict):return node.get("text","")+"".join(text(x) for x in node.get("content",[]))
    return ""

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None

def request(url,headers=None,body=None,method=None):
    data=json.dumps(body).encode() if body is not None else None
    hdr={"Content-Type":"application/json",**(headers or {})}
    req=urllib.request.Request(url,data=data,headers=hdr,method=method or ("POST" if body is not None else "GET"))
    try:
        with urllib.request.build_opener(NoRedirect()).open(req,timeout=35) as res:return json.load(res)
    except urllib.error.HTTPError as exc:
        raise ValueError("provider_http_"+str(exc.code)) from None
    except (urllib.error.URLError,TimeoutError):
        raise ValueError("provider_unavailable") from None

class Google:
    def __init__(self,binding,call=request):
        self.binding=binding;self.call=call
        cfg=handover.private_read(Path("/etc/ziji-fabric/providers/google/credentials.json"))
        if cfg.get("calendar_id")!=binding["calendar_id"] or cfg.get("expected_email")!=binding["expected_email"]:
            raise ValueError("google_binding_mismatch")
        # Existing collector's token+identity preflight is reused, not inferred.
        sys.path.insert(0,"/opt/ziji-fabric/native")
        from bind_native_providers import request as native_request
        token=native_request("https://oauth2.googleapis.com/token",body={
            "client_id":cfg["client_id"],"client_secret":cfg["client_secret"],
            "refresh_token":cfg["refresh_token"],"grant_type":"refresh_token"})
        self.headers={"Authorization":"Bearer "+token["access_token"]}
        who=self.call("https://www.googleapis.com/drive/v3/about?fields=user(emailAddress)",self.headers)
        if who.get("user",{}).get("emailAddress","").lower()!=binding["expected_email"].lower():
            raise ValueError("google_identity_mismatch")
        calendar=self.call(GAPI+"/users/me/calendarList/"+urllib.parse.quote(binding["calendar_id"],safe=""),self.headers)
        if calendar.get("accessRole")!="owner" or calendar.get("summary")!="WooCalendar":
            raise ValueError("calendar_owner_mismatch")
        self.base=GAPI+"/calendars/"+urllib.parse.quote(binding["calendar_id"],safe="")+"/events"
    def events(self,start,end):
        params={"timeMin":iso(start),"timeMax":iso(end),"singleEvents":"true","maxResults":250,"orderBy":"startTime"}
        result=[];seen=set()
        for page in range(1,41):
            data=self.call(self.base+"?"+urllib.parse.urlencode(params),self.headers)
            result.extend(data.get("items",[]));nxt=data.get("nextPageToken")
            if not nxt:return result,{"start":iso(start),"end":iso(end),"pages":page,"scanned":len(result),"complete":True}
            if nxt in seen:raise ValueError("calendar_pagination_repeated")
            seen.add(nxt);params["pageToken"]=nxt
        raise ValueError("calendar_pagination_limit")
    def get(self,eid):return self.call(self.base+"/"+urllib.parse.quote(eid,safe=""),self.headers)
    def patch(self,eid,description,etag):
        if not etag:raise ValueError("etag_required")
        return self.call(self.base+"/"+urllib.parse.quote(eid,safe="")+"?sendUpdates=none",
            {**self.headers,"If-Match":etag},{"description":description},"PATCH")

class Jira:
    def __init__(self,call=request):
        cfg=handover.private_read(Path("/etc/ziji-fabric/providers/jira/credentials.json"))
        if cfg.get("base_url")!=JIRA or cfg.get("project_allowlist")!=["RBO"]:raise ValueError("jira_binding_mismatch")
        self.call=call
        self.headers={"Authorization":"Basic "+base64.b64encode((cfg["email"]+":"+cfg["api_token"]).encode()).decode()}
        self.comments=None
    def list(self):
        if self.comments is not None:return self.comments
        rows=[];start=0
        for _ in range(100):
            page=self.call(JIRA+"/rest/api/3/issue/RBO-97/comment?maxResults=100&startAt="+str(start),self.headers)
            chunk=page.get("comments",[]);rows.extend(chunk);start+=len(chunk)
            if start>=page.get("total",0):self.comments=rows;return rows
            if not chunk:raise ValueError("jira_pagination_gap")
        raise ValueError("jira_pagination_limit")
    def receipt(self,key,payload):
        for row in self.list():
            if text(row.get("body",{})).startswith(key+"\n"):
                return {"id":row["id"],"verified":True,"replayed":True}
        body=key+"\n"+json.dumps(payload,sort_keys=True)
        saved=self.call(JIRA+"/rest/api/3/issue/RBO-97/comment",self.headers,
            {"body":{"type":"doc","version":1,"content":[{"type":"paragraph","content":[{"type":"text","text":body}]}]}})
        check=self.call(JIRA+"/rest/api/3/issue/RBO-97/comment/"+str(saved["id"]),self.headers)
        if text(check.get("body",{}))!=body:raise ValueError("jira_receipt_readback")
        self.comments.append(check)
        return {"id":str(check["id"]),"verified":True,"replayed":False}

def windows(now,mode,state):
    if not state.get("baseline_complete"):
        return [(now-timedelta(days=14),now+timedelta(hours=36),"baseline")]
    if mode=="DELTA":return [(now-timedelta(hours=48),now+timedelta(hours=36),"recent")]
    # Seven two-day slices sweep the historical window each week without 6x scans.
    offset=now.astimezone(ET).date().toordinal()%7*2
    return [(now-timedelta(hours=72),now+timedelta(hours=36),"recent"),
            (now-timedelta(days=14-offset),now-timedelta(days=max(0,12-offset)),"historical_slice")]

def lane_for(event,binding):
    series=event.get("recurringEventId")
    label=next((k for k,v in binding["writable_series"].items() if v==series),None)
    if label and label.startswith("income"):return "income"
    title=event.get("summary","")
    if PROTECTED_WORDS.search(title):return "private"
    if re.search(r"emir|academic|assignment",title,re.I):return "academic"
    if re.search(r"rsc|restoration",title,re.I):return "rsc"
    if re.search(r"dzokden|store|service",title,re.I):return "store"
    if re.search(r"income|career|recruit|job",title,re.I):return "income"
    return "distiller"

def relevant(event,binding):
    if event.get("visibility")!="private" or event.get("attendees") or event.get("eventType","default")!="default":return False
    if event.get("status")=="cancelled":return False
    if PROTECTED_WORDS.search(event.get("summary","")):return False
    return event.get("recurringEventId") in binding["writable_series"].values() or bool(RELEVANT.search(event.get("summary","")))

def feedback_body(event):
    desc=event.get("description","")
    if handover.START in desc:
        _,suffix=handover.split_description(desc)
        response=suffix.split(handover.HUMAN,1)[1]
    else:
        suffix=desc;response=desc
    return suffix, handover.response_has_text(response)

def gather(google,binding,state,now,mode):
    events={};coverage=[]
    for lo,hi,kind in windows(now,mode,state):
        rows,window=google.events(lo,hi);window["kind"]=kind;coverage.append(window)
        for event in rows:
            if relevant(event,binding):events[event["id"]]=event
    pending={r["event_id"] for r in state.get("feedback",{}).values() if r.get("status")!="CONSUMED"}
    for eid in pending-set(events):
        event=google.get(eid)
        if relevant(event,binding):events[eid]=event
    return events,coverage

def ingest(events,binding,state,jira,now):
    feedback=state.setdefault("feedback",{});baselines=state.setdefault("baselines",{})
    added=0;ambiguous=[]
    for eid,event in events.items():
        try:suffix,has_text=feedback_body(event)
        except ValueError:ambiguous.append(eid);continue
        prior=baselines.get(eid,{})
        if prior.get("description") and handover.START in event.get("description",""):
            try:
                old_prefix=handover.split_description(prior["description"])[0]
                new_prefix=handover.split_description(event["description"])[0]
                if old_prefix!=new_prefix:
                    ambiguous.append(eid)
                    state.setdefault("ambiguous_prefixes",{})[eid]={"observed_at":iso(now),"reason":"machine_prefix_changed_outside_writer"}
            except ValueError:
                ambiguous.append(eid)
        dig=handover.digest(suffix.strip())
        baselines[eid]={"suffix_digest":dig,"raw_suffix":suffix,"description":event.get("description",""),
                       "observed_at":iso(now),"etag":event.get("etag")}
        if eid in state.get("ambiguous_prefixes",{}):ambiguous.append(eid)
        if not has_text:continue
        key="calendar-feedback:"+binding["calendar_id"]+":"+eid+":"+dig
        if key not in feedback:
            lane=lane_for(event,binding)
            feedback[key]={"event_id":eid,"event_time":event.get("start"),"observed_at":iso(now),
                "previous_digest":prior.get("suffix_digest"),"current_digest":dig,"private_suffix":suffix,
                "classification":"OWNER_REPORTED" if prior else "AMBIGUOUS",
                "owning_lane":lane,"status":"PENDING_VERIFICATION",
                "next_verification":"Owning lane must inspect exact private source; provider completion is not asserted."}
            added+=1
        receipt=feedback[key]
        if not receipt.get("jira_capture"):
            # No description, title, personal text or customer data leaves private state.
            minimal={k:receipt[k] for k in ("event_id","event_time","observed_at","previous_digest","current_digest","classification","owning_lane","status","next_verification")}
            minimal["restricted_pointer"]="vps:calendar-writer/state.json#feedback/"+handover.digest(key)
            minimal["owning_issue"]="RBO-128" if receipt["owning_lane"]=="academic" else "RBO-97"
            receipt["jira_capture"]=jira.receipt("CALENDAR_FEEDBACK_CAPTURED "+key,minimal)
        # A durable lane inbox is a handoff, not provider verification or consumption.
        lane=receipt["owning_lane"]
        save(STATE/"inbox"/lane/(handover.digest(key)+".json"),{"feedback_key":key,**receipt})
    return added,ambiguous

def lane_receipts(jira,now):
    found={}
    for row in jira.list():
        body=text(row.get("body",{}))
        header,sep,payload=body.partition("\n")
        match=re.fullmatch(r"FABRIC_RUN (income|rsc|store):[^\s]+",header.strip())
        if not match or not sep:continue
        lane=match.group(1)
        try:
            doc=json.loads(payload)
            status=doc.get("outcome",doc.get("terminal_state"))
            if status not in ("SUCCESS","NO_CHANGE","BLOCKED","FAIL"):continue
            at=handover.stamp(doc.get("observed_at") or row.get("updated",row.get("created")))
        except (ValueError,TypeError):continue
        if lane in found and found[lane]["observed_at"]>=iso(at):continue
        if now-at>timedelta(hours=36):status="STALE"
        found[lane]={"lane":lane,"status":status,"reference":"https://restoration-mt.atlassian.net/browse/RBO-97?focusedCommentId="+str(row["id"]),"observed_at":iso(at)}
    return [found.get(lane,{"lane":lane,"status":"PENDING","reference":"https://restoration-mt.atlassian.net/browse/RBO-97","observed_at":None}) for lane in ("income","rsc","store")]

def actionable_candidates(receipts,now):
    candidates=[]
    for receipt in receipts:
        base=Path("/var/lib/ziji-fabric/actors")/receipt["lane"]
        latest=read(base/"latest.json",{})
        candidate=latest.get("calendar_candidate")
        if not isinstance(candidate,dict):continue
        if candidate.get("privacy_reviewed") is not True or candidate.get("owner_action_required") is not True:continue
        if not all(isinstance(candidate.get(k),str) and candidate[k].strip() for k in ("action","why","safe_default","reference","source_observed_at")):continue
        if any(len(candidate[k])>1200 for k in ("action","why","safe_default","reference")):continue
        try:
            at=handover.stamp(candidate["source_observed_at"])
            if now-at>timedelta(hours=36) or at>now+timedelta(minutes=5):continue
            packet=Path(latest.get("packet_path",""))
            if not packet.is_absolute() or not packet.resolve().is_relative_to((base/"packets").resolve()):continue
            evidence=read(packet,{})
            if evidence.get("calendar_candidate")!=candidate:continue
        except (ValueError,OSError):continue
        candidates.append({**receipt,**candidate,"observed_at":candidate["source_observed_at"]})
    return candidates

def eligible_for_lane(event,binding,now,lane):
    if not handover.eligible(event,set(binding["writable_series"].values()),now):return False
    names=("income1","income2","daily") if lane=="income" else ("secondary","daily")
    return event.get("recurringEventId") in {binding["writable_series"].get(k) for k in names}

def select_candidate(candidates,request_id):
    # Native receipt reconciliation has a deterministic ordering; no model is
    # needed merely to restate terminal evidence. Judgment remains with actors.
    order={"FAIL":0,"BLOCKED":1,"STALE":2,"PENDING":3,"SUCCESS":4,"NO_CHANGE":5}
    if not candidates:return None,"NO_ADMITTED_OWNER_ACTION"
    return min(candidates,key=lambda row:(order[row["status"]],row["lane"])),"DETERMINISTIC_NO_MODEL"

def consume_feedback(state,jira,now):
    count=0
    for lane in ("income","rsc","store"):
        base=Path("/var/lib/ziji-fabric/actors")/lane
        latest=read(base/"latest.json",{})
        for item in latest.get("feedback_consumed",[]):
            key=item.get("feedback_key");receipt=state.get("feedback",{}).get(key)
            if not receipt or receipt.get("status")=="CONSUMED" or receipt["owning_lane"]!=lane:continue
            if item.get("current_digest")!=receipt["current_digest"]:continue
            packet=Path(item.get("packet_path",""))
            if not packet.is_absolute() or not packet.resolve().is_relative_to((base/"packets").resolve()):continue
            evidence=read(packet,{})
            matching=[x for x in evidence.get("feedback_consumed",[]) if x.get("feedback_key")==key
                      and x.get("current_digest")==receipt["current_digest"]]
            if not matching or item.get("status") not in ("INTERPRETED_PENDING_VERIFICATION","VERIFIED"):continue
            minimal={"event_id":receipt["event_id"],"digest":receipt["current_digest"],
                "owning_lane":lane,"capture_receipt":receipt["jira_capture"]["id"],
                "verification_status":item["status"],"owning_packet_sha256":hashobj(evidence),
                "calendar_acknowledgment":"NEXT_ELIGIBLE_OR_JUSTIFIED_NO_CHANGE"}
            receipt["consumption_receipt"]=jira.receipt("FABRIC_CALENDAR_FEEDBACK_CONSUMED "+key,minimal)
            receipt["status"]="CONSUMED";receipt["consumed_at"]=iso(now)
            receipt["verification_status"]=item["status"];count+=1
    return count

def note(candidate,pending,now):
    lane=candidate["lane"];status=candidate["status"]
    expiry=now+timedelta(hours=36)
    if candidate.get("observed_at") and candidate["status"] not in ("STALE","PENDING"):
        expiry=min(expiry,handover.stamp(candidate["observed_at"])+timedelta(hours=36))
    action=candidate["action"]
    acknowledgment="JOSH UPDATE RECEIVED: private feedback is captured; verification remains pending.\n" if pending else ""
    return (handover.START+"\nLANE: "+lane+"\nPURPOSE: Source-backed owner attention\nSTATUS: "+status+
        "\nPRIORITY: Protect existing commitments\n"+acknowledgment+"DO NEXT: "+action+
        "\nWHY: "+candidate["why"]+" Source observed "+str(candidate["observed_at"])+
        "\nDEFAULT IF SILENT: "+candidate["safe_default"]+
        "\nCONTINGENCY: If evidence conflicts, pause this action and inspect its owning source."+
        "\nVERIFIED: Receipt status only; no third-party submission or completion inferred."+
        "\nRECHECK BY: "+iso(expiry)+"\nDETAILS:\n"+candidate["reference"]+"\n"+handover.END),iso(expiry)

def write_one(google,event,prefix,baseline):
    current=google.get(event["id"])
    if current!=event:raise ValueError("calendar_changed_before_write")
    _,suffix=handover.split_description(current["description"])
    if baseline.get("description")!=current["description"]:raise ValueError("human_or_prefix_changed")
    description=prefix+suffix
    if description==current["description"]:return {"status":"NO_CHANGE","event_id":event["id"]}
    before=google.get(event["id"])
    if before!=current:raise ValueError("calendar_changed_at_cas")
    google.patch(event["id"],description,before.get("etag"))
    after=google.get(event["id"])
    if after.get("description")!=description or handover.protected(after)!=handover.protected(before):
        raise ValueError("calendar_readback_mismatch")
    if handover.split_description(after["description"])[1]!=suffix:raise ValueError("suffix_readback_mismatch")
    return {"status":"VERIFIED","event_id":event["id"],"description_sha256":handover.digest(description),
            "suffix_sha256":handover.digest(suffix),"protected_fields_verified":True,"etag":after.get("etag")}

def run(mode="DELTA",write=False,scheduled=False):
    now=utcnow();local=now.astimezone(ET)
    if scheduled and local.strftime("%H:%M") not in SLOTS:return {"status":"NOT_DUE","calendar_writes":0}
    if scheduled:mode=SLOTS[local.strftime("%H:%M")]
    STATE.mkdir(mode=0o700,parents=True,exist_ok=True);os.chmod(STATE,0o700)
    fd=os.open(STATE/"run.lock",os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,"w") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        runid=local.strftime("%Y-%m-%d_%H-%M")+"_"+mode
        path=STATE/"runs"/(runid+("_write" if write else "_read")+".json")
        prior=read(path)
        if prior and prior.get("status") in ("VERIFIED","NO_CHANGE"):return prior

        if write:
            active=handover.private_read(ACTIVATION)
            if active.get("hosted_writer_disabled") is not True or active.get("enabled") is not True:
                raise ValueError("single_writer_activation_missing")
        binding=handover.private_read(CONFIG)
        if len(binding.get("writable_series",{}))!=4:raise ValueError("four_series_required")
        state=read(STATE/"state.json",{"feedback":{},"baselines":{}})
        google=Google(binding);jira=Jira()
        # Recover only the exact prior intent, never retry a blind PATCH.
        intent_path=STATE/"intents"/(runid+".json")
        intent=read(intent_path)
        if intent and not intent.get("resolved"):
            current=google.get(intent["before"]["id"])
            if (handover.digest(current.get("description",""))==intent["description_sha256"]
                    and handover.protected(current)==handover.protected(intent["before"])
                    and handover.split_description(current["description"])[1]==intent["suffix"]):
                result={"status":"VERIFIED","event_id":current["id"],"recovered":True,
                        "description_sha256":intent["description_sha256"],"protected_fields_verified":True}
                state.setdefault("effects",{})[intent["effectkey"]]=result
                state.setdefault("baselines",{})[current["id"]]={"description":current["description"],
                    "raw_suffix":intent["suffix"],"suffix_digest":handover.digest(intent["suffix"].strip()),
                    "observed_at":iso(now),"etag":current.get("etag")}
                save(STATE/"state.json",state)
                intent["resolved"]=True;save(intent_path,intent)
            elif current!=intent["before"]:
                raise ValueError("uncertain_write_changed_native_state_inspect")
            else:
                intent["resolved"]=True;intent["effect_absent_verified"]=True;save(intent_path,intent)
        events,coverage=gather(google,binding,state,now,mode)
        added,ambiguous=ingest(events,binding,state,jira,now)
        state["coverage"]=coverage
        if any(x["kind"]=="baseline" for x in coverage):state["baseline_complete"]=True
        state["last_feedback_scan"]=iso(now)
        save(STATE/"state.json",state)
        consumed=consume_feedback(state,jira,now)
        save(STATE/"state.json",state)
        candidates=lane_receipts(jira,now)
        # Cache model selection across unchanged source versions, not across changed facts.
        fingerprint=hashobj(candidates)
        admitted=actionable_candidates(candidates,now)
        selected,modelstatus=select_candidate(admitted,"distiller_"+fingerprint[:48])
        report={"run_id":runid,"mode":mode,"observed_at":iso(now),"coverage":coverage,
            "feedback_added":added,"feedback_consumed":consumed,"pending_feedback":sum(r["status"]!="CONSUMED" for r in state["feedback"].values()),
            "ambiguous_event_count":len(ambiguous),"lane_receipts":candidates,"model_status":modelstatus,
            "calendar_writes":0,"calendar_results":[],"paid_fallback":False,"status":"NO_CHANGE"}
        if selected:
            attributable=[r for r in state["feedback"].values() if r["owning_lane"]==selected["lane"]
                          and r["classification"]=="OWNER_REPORTED" and r.get("jira_capture",{}).get("verified")]
            prefix,expiry=note(selected,bool(attributable),now);report["note_expiry"]=expiry
            eligible=[e for e in events.values() if eligible_for_lane(e,binding,now,selected["lane"]) and e["id"] not in ambiguous]
            eligible.sort(key=lambda e:e["start"]["dateTime"])
            report["eligible_instances"]=len(eligible)
            # One consequential update suffices; maxima remain 6FULL/2DELTA.
            if write and eligible:
                event=eligible[0]
                effectkey=hashobj({"event":event["id"],"sources":candidates,"feedback":sorted(state["feedback"])})
                effects=state.setdefault("effects",{})
                if effectkey not in effects:
                    suffix=handover.split_description(event["description"])[1]
                    intent={"before":event,"suffix":suffix,"description_sha256":handover.digest(prefix+suffix),
                            "effectkey":effectkey,"resolved":False}
                    save(intent_path,intent)
                    report["write_started"]=True;save(path,report)
                    result=write_one(google,event,prefix,state["baselines"][event["id"]])
                    report["calendar_results"].append(result)
                    if result["status"]=="VERIFIED":report["calendar_writes"]+=1
                    effects[effectkey]=result
                    state["baselines"][event["id"]]["description"]=prefix+suffix
                    state["baselines"][event["id"]]["etag"]=result.get("etag")
                    save(STATE/"state.json",state)
                    intent["resolved"]=True;save(intent_path,intent)
                    report["status"]="VERIFIED"
        if mode=="FULL" or added or report["calendar_writes"]:
            report["checkpoint"]=jira.receipt("FABRIC_CHECKPOINT native-distiller:"+runid,report)
        if mode=="FULL" and report.get("checkpoint",{}).get("verified"):
            state["last_good_full"]={"observed_at":iso(now),"run_id":runid}
            save(STATE/"state.json",state)
        report["last_good_full"]=state.get("last_good_full")
        save(path,report);save(STATE/"latest.json",report)
        return report

if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--mode",choices=["FULL","DELTA"],default="DELTA")
    p.add_argument("--write",action="store_true");p.add_argument("--scheduled",action="store_true")
    args=p.parse_args()
    try:print(json.dumps(run(args.mode,args.write,args.scheduled),sort_keys=True))
    except Exception as exc:
        failed={"status":"FAIL","observed_at":iso(utcnow()),"mode":args.mode,
            "reason":str(exc) if isinstance(exc,ValueError) else type(exc).__name__,
            "calendar_writes":"UNKNOWN_IF_STARTED","paid_fallback":False}
        try:save(STATE/"failure.json",failed)
        except OSError:pass
        print(json.dumps(failed))
        raise SystemExit(1)
