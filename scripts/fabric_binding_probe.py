"""Bounded provider probe. Secrets stay in memory; stdout contains only status codes.

Agent calls are synthetic acknowledgements with no tools or domain effects requested.
This is not a Fabric production acceptance test or an actor migration.
"""
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request


class ProbeError(Exception):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def trigger_id(raw, url):
    raw, url = raw.strip(), url.strip()
    direct = raw if re.fullmatch(r"agtch_[A-Za-z0-9_-]+", raw) else None
    via_url = None
    if url:
        parsed = urllib.parse.urlsplit(url)
        match = re.fullmatch(r"/v1/workspace_agents/(agtch_[A-Za-z0-9_-]+)/trigger", parsed.path)
        if parsed.scheme != "https" or parsed.netloc != "api.chatgpt.com" or parsed.query or parsed.fragment or not match:
            raise ProbeError("trigger_url_invalid")
        via_url = match.group(1)
    if direct and via_url and direct != via_url:
        raise ProbeError("trigger_id_url_mismatch")
    if raw and not direct and not re.fullmatch(r"agt_[A-Za-z0-9_-]+", raw):
        raise ProbeError("trigger_id_invalid")
    if not direct and not via_url:
        raise ProbeError("published_trigger_missing")
    return direct or via_url


def request(url, token, body=None, extra_headers=None):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "https" or parsed.netloc not in ("api.openai.com", "api.chatgpt.com"):
        raise ProbeError("provider_not_allowed")
    if not token or "\n" in token or "\r" in token:
        raise ProbeError("credential_missing_or_invalid_format")
    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    headers.update(extra_headers or {})
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.build_opener(NoRedirect).open(req, timeout=25) as response:
            return response.status, json.loads(response.read(1048576))
    except urllib.error.HTTPError as error:
        # Do not include response bodies, headers, URLs, tokens, or request objects.
        raise ProbeError("http_" + str(error.code)) from None
    except (OSError, ValueError):
        raise ProbeError("network_or_response_error") from None


def probe(config, call=request, now=time.monotonic, pause=time.sleep):
    results = {}
    # A fixed read verifies the intended inference credential independently of agents.
    try:
        status, body = call("https://api.openai.com/v1/models", config.get("model_key", ""))
        if status != 200 or not isinstance(body.get("data"), list):
            raise ProbeError("model_response_invalid")
        results["model_auth"] = "verified"
    except ProbeError as error:
        results["model_auth"] = str(error)

    for lane in ("income", "rsc", "store"):
        item = config.get("agents", {}).get(lane, {})
        try:
            ident = trigger_id(item.get("id", ""), item.get("url", ""))
            token = config.get("workspace_key", "").strip()
            if not token:
                raise ProbeError("workspace_token_missing")
            # One stable key per revised probe; reruns never enqueue a new canary.
            key = "fabric-binding-v1-20260908-" + lane
            root = "https://api.chatgpt.com/v1/workspace_agents/" + ident
            status, accepted = call(root + "/trigger", token, {
                "conversation_key": key,
                "input": "Authorized synthetic Fabric transport canary. Lane: " + lane + ". Return a brief acknowledgement and stop. Do not call any tools, read private sources, create tasks, send messages, change Calendar/CRM/Shopify/Notion/Jira, schedule work, or disclose credentials. This tests invocation only; do not claim production acceptance."
            }, {"Idempotency-Key": key, "OpenAI-Beta": "workspace_agent_runs=v1"})
            run_id = accepted.get("agent_trigger_run_id", "")
            if status != 202 or not re.fullmatch(r"apirun_[A-Za-z0-9_-]+", run_id):
                raise ProbeError("accepted_status_or_run_id_invalid")
            deadline = now() + 80
            while now() < deadline:
                _, state = call(root + "/runs/" + run_id, token)
                if state.get("api_trigger_id") != ident or state.get("id") != run_id:
                    raise ProbeError("run_identity_mismatch")
                if item.get("id", "").strip().startswith("agt_") and state.get("agent_id") != item["id"].strip():
                    raise ProbeError("agent_identity_mismatch")
                outcome = state.get("status")
                if outcome == "completed":
                    results[lane] = "invocation_completed"
                    break
                if outcome in ("failed", "suspended"):
                    raise ProbeError("run_" + outcome)
                if outcome not in ("queued", "in_progress"):
                    raise ProbeError("run_status_invalid")
                pause(5)
            else:
                raise ProbeError("run_pending_timeout_no_retrigger")
        except ProbeError as error:
            results[lane] = str(error)
    return results


def main(config):
    results = probe(config)
    print(json.dumps({"stage": "PROVIDER_BINDING_PROBE", "results": results,
                      "fabric_live": False, "credential_values_disclosed": False}, sort_keys=True))
    return 0 if all(value in ("verified", "invocation_completed") for value in results.values()) else 1
