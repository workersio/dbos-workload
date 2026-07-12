<!-- emit:begin -->
## Snapshot (generated -- do not edit inside the emit markers)

status: planned=0 ready=0 running=0 done=6
result: null=0 green=6 finding=0 void=0 blocked=0

| flow \ rung | L0 | L1 | L2 | L3 | L4 |
| --- | --- | --- | --- | --- | --- |
| durable-workflow | 1 | 1 | 0 | 1 | 0 |
| enqueue-task | 1 | 1 | 0 | 1 | 0 |
<!-- emit:end -->

threshold: 40

Ranked backlog of situations not yet emitted as scenarios. The first batch
(durable/enqueue × solo/contention/crash) is already promoted to scenarios/ and
so is not listed here. These are the next targets — most require the row-4 model
refresh to add the flow/event/persona they name (recorded in the note).

| score | cast | flows | event | rung | source | note |
|-------|------|-------|-------|------|--------|------|
| ~~74~~ REFUTED | 2×runner + recovery-executor | durable-workflow | concurrent-recovery | L3 | usage | NOT A BUG (e9, source probe). Under cross-executor recovery of a LIVE workflow, DBOS CONVERGES the recorded outcome. The step-record race makes the loser raise DBOSWorkflowConflictIDError, but the workflow finalizer catches it explicitly (_core.py:594-602 "Aborting duplicate execution" -> await_workflow_result -> returns the WINNER's SUCCESS result) and never terminalizes ERROR; a workflow completed before recovery returns its recorded result directly (_core.py:566-576). update_workflow_outcome's missing SUCCESS-clobber guard (_sys_db.py:887-889 guards only CANCELLED) is therefore unreachable — the loser never writes ERROR. False-DLQ is SUCCESS-guarded (_sys_db.py:820-824) and needs >101 steals. Only residual observable = at-least-once step SIDE EFFECTS (documented, weight 0-1, #767-shape). No correctness/data-loss red. Scout + strategy-critic(REFRAME) + source probe. |
| 70 | 1×runner + 1×ops-operator | durable-workflow, cancel | crash-restart | L3 | usage | queue × cancel × restart: cancel a workflow, resume onto a queue, crash mid-drain — needs cancel/resume flows (next refresh) |
| 58 | 1×producer | enqueue-task | crash-restart | L2 | usage | dedup-id × concurrency-limit contention under recovery — extends enqueue flow with worker_concurrency observation |
| 52 | 1×explorer | send, recv | none | L1 | api-floor | notifications OAOO under concurrent recv on one topic — vendor uses single recv only (scout gap #6); needs notifications flow |
| ~~48~~ REFUTED | 1×explorer | write_stream_from_step | none | L1 | api-floor | NOT A BUG (e7). Steps are at-least-once for body side effects; the vendor's own tests/test_streaming.py:604-659 (test_stream_write_from_step) asserts one stream value PER ATTEMPT. write_stream_from_step lacking an OAOO record is intended, not a gap. Removed the stream-write flow/scenario/finding; test-reviewer returned REMOVE. |
| 44 | 1×ops-operator | resume, fork | none | L1 | api-floor | illegal state-transition contracts: resume a SUCCESS, cancel an already-CANCELLED, fork a still-PENDING — no DBOSNonExistentWorkflow/InvalidTransition assertions exist (scout gap #5) |
