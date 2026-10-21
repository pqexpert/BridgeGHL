# Native RSC merge evidence

This is the existing bounded RSC event role implemented without model calls. GitHub Actions reacts to actual merged BridgeGHL pull requests; trusted main code verifies the current repository/default branch/merge SHA and exact-revision checks, resolves explicit scoped Jira references, rechecks the PR, and passes an evidence-only receipt over pinned SSH to the existing native Jira writer. No PR text is executed. No domain actions, outbound messages, deployment, issue closure or polling occur.

The native mode starts STANDBY until the coordinator removes only BridgeGHL from the existing hosted event trigger and invokes this workflow with mode activate. First run mode prove against an actual merged PR; verify the returned Jira comment/readback before changing the trigger. Automatic events in standby cannot write Jira. Mode standby reversibly stops native event effects before restoring hosted routing.

The RSC console repository has no authenticated cross-repository forwarding credential. Its hosted event trigger remains. Manual proof of an RSC PR is not evidence of recurring RSC event delivery.

Stable replay identity is unchanged: github:owner/repo:pr:number:merge:sha. A private per-event snapshot preserves its first observation, the shared native writer suppresses existing identities, and ambiguous writes remain UNKNOWN. These merge-event receipts do not refresh later workflow completion; the regular owning RSC cycle consumes pending verification. Explicit RBO-34/RBO-97 links can route directly; other, missing or ambiguous owning links produce UNROUTED in RBO-97 with their exact candidates retained. This narrows automatic routing to the existing native binding.

Runtime prerequisite: /opt/ziji-fabric/native/native_jira_receipt.py from the reviewed shared receipt helper. Private state and activation flag live under /var/lib/ziji-fabric/merge-handoff and are captured by the existing encrypted backup. No new credentials are copied to the laptop or VPS by this workflow.
