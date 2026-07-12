<!-- emit:begin -->
## Snapshot (generated -- do not edit inside the emit markers)

status: planned=0 ready=0 running=0 done=7
result: null=0 green=6 finding=1 void=0 blocked=1

| flow \ rung | L0 | L1 | L2 | L3 | L4 |
| --- | --- | --- | --- | --- | --- |
| durable-workflow | 1 | 1 | 0 | 1 | 0 |
| enqueue-task | 1 | 1 | 0 | 2 | 0 |
| management | 1 | 0 | 0 | 0 | 0 |
<!-- emit:end -->

threshold: 40

Ranked backlog of situations not yet emitted as scenarios. The first batch
(durable/enqueue × solo/contention/crash) is already promoted to scenarios/ and
so is not listed here. These are the next targets — most require the row-4 model
refresh to add the flow/event/persona they name (recorded in the note).

| score | cast | flows | event | rung | source | note |
|-------|------|-------|-------|------|--------|------|
| ~~74~~ REFUTED | 2×runner + recovery-executor | durable-workflow | concurrent-recovery | L3 | usage | NOT A BUG (e9, source probe). Under cross-executor recovery of a LIVE workflow, DBOS CONVERGES the recorded outcome. The step-record race makes the loser raise DBOSWorkflowConflictIDError, but the workflow finalizer catches it explicitly (_core.py:594-602 "Aborting duplicate execution" -> await_workflow_result -> returns the WINNER's SUCCESS result) and never terminalizes ERROR; a workflow completed before recovery returns its recorded result directly (_core.py:566-576). update_workflow_outcome's missing SUCCESS-clobber guard (_sys_db.py:887-889 guards only CANCELLED) is therefore unreachable — the loser never writes ERROR. False-DLQ is SUCCESS-guarded (_sys_db.py:820-824) and needs >101 steals. Only residual observable = at-least-once step SIDE EFFECTS (documented, weight 0-1, #767-shape). No correctness/data-loss red. Scout + strategy-critic(REFRAME) + source probe. |
| 70 (BLOCKED-CLASS) | 1×runner + 1×ops-operator | durable-workflow, cancel | crash-restart | L3 | usage | queue × cancel × restart — the interesting part (cancel racing a live re-drain) needs TWO live executors, same harness block as enqueue-cap-under-recovery (e11). Single-process cancel×queue is REFUTED (e10 scout: cancel sets queue_name=None + CANCELLED, excluded from dequeue/cap, _sys_db.py:917-936). Deferred to harness multi-executor support. |
| 58 (BLOCKED-CLASS) | 1×producer | enqueue-task | crash-restart | L2 | usage | dedup-id × concurrency-limit under recovery — the cap-breach is the e11 finding (source-confirmed, execution-blocked on multi-executor harness support); dedup-survival across recovery REFUTED (e10 scout: clear_queue_assignment never nulls deduplication_id, _sys_db.py:4018-4020). |
| ~~52~~ REFUTED | 1×explorer | send, recv | none | L1 | api-floor | NOT A BUG (e12 source probe). send/recv/set_event/get_event are OAOO-guarded and vendor-tested: send_bulk from a workflow does _check_operation_execution_txn + message_uuid PK on_conflict_do_nothing + step record in ONE txn (_sys_db.py:2818-2903), dedup asserted at tests/test_dbos.py:1031-1050; recv re-reads its recorded result (_sys_db.py:2977-2990); set_event is last-write-wins upsert. Only un-OAOO path (send from a bare step) = same at-least-once precedent already refuted for write_stream. |
| ~~48~~ REFUTED | 1×explorer | write_stream_from_step | none | L1 | api-floor | NOT A BUG (e7). Steps are at-least-once for body side effects; the vendor's own tests/test_streaming.py:604-659 (test_stream_write_from_step) asserts one stream value PER ATTEMPT. write_stream_from_step lacking an OAOO record is intended, not a gap. Removed the stream-write flow/scenario/finding; test-reviewer returned REMOVE. |
| 44 (CONFIRMED, weight 1) | 1×ops-operator | management | none | L1 | api-floor | REACHABLE RED (e12 source probe): DBOS.fork_workflow(nonexistent_id, 1) raises a raw Exception("Workflow ... not found") (_sys_db.py:1179-1181), NOT the typed DBOSNonExistentWorkflowError its sibling verbs raise (retrieve_workflow _client.py:520-523; handle.get_result :123,147). No pre-existence check in fork_workflow (_workflow_commands.py:30-56, _client.py:1128-1147); no test covers it. Error-contract violation, weight 1 (wrong-error). REFUTED sub-cases: resume-terminal (guarded notin_[SUCCESS,ERROR] _sys_db.py:963-970), cancel-cancelled (idempotent), fork out-of-range/from-PENDING (vendor-tested / tolerant, tests/test_async.py:957-990). Single-process, no event, no two executors — buildable now; crystallizing it proves the full producer->executor->RED->finding pipeline end-to-end. |
