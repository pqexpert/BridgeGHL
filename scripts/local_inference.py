#!/usr/bin/env python3
"""Pinned local inference; no model output is executed. Python standard library only."""
import argparse, hashlib, json, os, pathlib, sys, time, urllib.request, uuid
ROOT = pathlib.Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config/local-inference"
STATE = pathlib.Path.home() / ".local/share/ziji-local"
def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(8*1024*1024), b""):
            h.update(b)
    return h.hexdigest()
def checked_inputs():
    manifest = json.loads((STATE / "input-lock.json").read_text())
    paths = {"model_lock": CONFIG / "model-lock.json", "system": CONFIG / "system.txt",
             "memory": STATE / "memory.json", "sources": CONFIG / "sources.json"}
    for key, path in paths.items():
        if digest(path) != manifest[key]:
            raise ValueError("Input integrity mismatch: " + key)
    return json.loads(paths["model_lock"].read_text()), paths["system"].read_text(), json.loads(paths["memory"].read_text()), manifest
def verify_model(lock):
    model = STATE / "models" / lock["filename"]
    if model.stat().st_size != lock["bytes"] or digest(model) != lock["sha256"]:
        raise ValueError("Model checksum mismatch")
    return model
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["verify", "serve", "ask"])
    parser.add_argument("--lane", choices=["engineering", "income", "rsc", "store", "distiller"], default="engineering")
    args = parser.parse_args()
    lock, system, memory, manifest = checked_inputs()
    if args.mode in ("verify", "serve"):
        model = verify_model(lock)
        binary = STATE / "runtime/llama.cpp/build/bin/llama-server"
        runtime_lock = json.loads((STATE / "runtime-lock.json").read_text())
        for rel, expected in runtime_lock.items():
            if digest(STATE / rel) != expected:
                raise ValueError("Runtime integrity mismatch: " + rel)
        if args.mode == "verify":
            print(json.dumps({"verified":True, "model_sha256":lock["sha256"], "input_hashes":manifest}))
            return
        argv = [str(binary), "-m", str(model), "--host", "127.0.0.1", "--port", "18181",
                "-c", str(lock["context_tokens"]), "-t", str(lock["threads"]), "-np", "1",
                "-ngl", "0", "--jinja", "--api-key-file", str(STATE / "api-key"),
                "--no-webui", "--no-agent", "--no-ui-mcp-proxy", "-n", str(lock["max_output_tokens"]),
                "--chat-template-kwargs", '{"enable_thinking":false}', "--temp", str(lock["temperature"]),
                "--top-p", str(lock["top_p"]), "--top-k", str(lock["top_k"]), "--min-p", str(lock["min_p"])]
        os.execve(str(binary), argv, {"PATH":"/usr/bin:/bin", "HOME":str(STATE / "empty-home"),
                  "LANG":"C.UTF-8", "OMP_NUM_THREADS":str(lock["threads"])})
    prompt = sys.stdin.read(16001)
    if not prompt.strip() or len(prompt) > 16000:
        raise ValueError("Provide 1-16000 characters on stdin")
    # Read only explicitly curated preferences. No directory scans, chat import, RAG or memory mutation.
    body = {"model":"ziji-local", "messages":[
        {"role":"system","content":system + "\nCurated preferences: " + json.dumps(memory) +
         "\nThis request's sole lane: " + args.lane},
        {"role":"user","content":prompt}],
        "max_tokens":lock["max_output_tokens"],"temperature":lock["temperature"],
        "top_p":lock["top_p"],"top_k":lock["top_k"],"min_p":lock["min_p"],
        "seed":lock["seed"], "stream":False,
        "chat_template_kwargs":{"enable_thinking":False}}
    key = (STATE / "api-key").read_text().strip()
    request = urllib.request.Request("http://127.0.0.1:18181/v1/chat/completions",
        data=json.dumps(body).encode(), headers={"Content-Type":"application/json", "Authorization":"Bearer "+key})
    started=time.monotonic()
    with urllib.request.urlopen(request, timeout=240) as response:
        result=json.load(response)
    message=result["choices"][0]["message"]
    if message.get("tool_calls"):
        raise ValueError("Unexpected tool request rejected")
    answer=message.get("content") or ""
    receipt={"id":str(uuid.uuid4()),"mode":"proposal_only","lane":args.lane,
        "model_sha256":lock["sha256"],"inputs":manifest,"elapsed_seconds":round(time.monotonic()-started,2),
        "usage":result.get("usage"),"input_sha256":hashlib.sha256(prompt.encode()).hexdigest(),
        "output_sha256":hashlib.sha256(answer.encode()).hexdigest(),"external_effects":False}
    receipts=STATE/"receipts"
    receipts.mkdir(mode=0o700,exist_ok=True)
    (receipts/(receipt["id"]+".json")).write_text(json.dumps(receipt,indent=2)+"\n")
    print(answer)
    print(json.dumps(receipt),file=sys.stderr)
if __name__=="__main__":
    try: main()
    except Exception as exc:
        print("Local inference failed: "+type(exc).__name__+": "+str(exc), file=sys.stderr)
        sys.exit(1)
