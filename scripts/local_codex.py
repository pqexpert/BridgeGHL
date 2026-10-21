#!/usr/bin/env python3
"""Use the installed Codex harness with local weights and a fixed proposal boundary."""
import json, os, pathlib, subprocess, sys
from local_inference import checked_inputs, STATE, CONFIG
def main():
    lock, system, memory, hashes = checked_inputs()
    prompt = " ".join(sys.argv[1:]) if len(sys.argv)>1 else sys.stdin.read(16001)
    if not prompt.strip() or len(prompt)>16000:
        raise ValueError("Provide 1-16000 characters")
    binding=json.loads((STATE/"codex-binding.json").read_text())
    from local_inference import digest
    binary=pathlib.Path(binding["binary"])
    if digest(binary)!=binding["sha256"]:
        raise ValueError("Codex binary changed; rebind after review")
    workspace=STATE/"workspace"
    workspace.mkdir(mode=0o700,exist_ok=True)
    opts={
      "model": "ziji-local", "review_model":"ziji-local", "model_provider":"ziji_local",
      "model_context_window": lock["context_tokens"], "model_auto_compact_token_limit":3500,
      "model_instructions_file":str(CONFIG/"system.txt"),
      "developer_instructions":"Curated preferences: "+json.dumps(memory)+"\nReturn a concise proposal only.",
      "model_providers.ziji_local.name":"Ziji local pinned model",
      "model_providers.ziji_local.base_url":"http://127.0.0.1:18181/v1",
      "model_providers.ziji_local.wire_api":"responses",
      "model_providers.ziji_local.env_key":"ZIJI_LOCAL_API_KEY",
      "model_providers.ziji_local.requires_openai_auth":False,
      "model_providers.ziji_local.supports_websockets":False,
      "model_providers.ziji_local.request_max_retries":0,
      "model_providers.ziji_local.stream_max_retries":0,
      "web_search":"disabled", "features.apps":False, "features.shell_tool":False,
      "features.plugins":False, "features.remote_plugin":False,
      "features.multi_agent":False, "features.memories":False, "features.shell_snapshot":False,
      "memories.generate_memories":False, "memories.use_memories":False,
      "sandbox_mode":"read-only", "approval_policy":"never",
    }
    argv=[str(binary),"exec","--ephemeral","--skip-git-repo-check","-C",str(workspace)]
    for key,value in opts.items(): argv += ["-c",key+"="+json.dumps(value)]
    argv += ["-"]
    env={"PATH":"/usr/bin:/bin","HOME":str(pathlib.Path.home()),"LANG":"C.UTF-8",
         "ZIJI_LOCAL_API_KEY":(STATE/"api-key").read_text().strip()}
    return subprocess.run(argv,input=prompt,text=True,env=env,timeout=240).returncode
if __name__=="__main__":
    sys.exit(main())
