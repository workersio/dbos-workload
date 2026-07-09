# Run evidence — queued-task-cleanup-after-cancel

Exploration: `queued-task-cleanup-after-cancel`
Promise: `async-workflows-checkpoint-deterministically` (area: workflows)
Rung: `rung-002-queued-async-task-retention-gc-pressure`

## Verdict: FINDING (RED confirmed)

Invariant `queued_gc_workflow_tasks_released_after_terminal` **FAIL**. After a
batch of queued async workflows is cancelled (`cancel_indices [0,2,4]`) and DBOS
completes shutdown ("Attempting to shut down DBOS. 3 workflows remain active" →
"DBOS successfully shut down"), the workload snapshots live asyncio tasks and
finds **3 `_execute_workflow_async` coroutine tasks still alive** —
`done=false, cancelled=false`. Terminal state should release the executor tasks;
they survive.

`released_snapshot`:
```
count: 3
tasks:
  - coro=<coroutine object _execute_workflow_async ...> done=false cancelled=false
  - coro=<coroutine object _execute_workflow_async ...> done=false cancelled=false
  - coro=<coroutine object _execute_workflow_async ...> done=false cancelled=false
```

The three surviving tasks correspond to the cancelled queued workflows
(`...-case-003-7112-0`, `-2`, `-4`), matching the ids DBOS logged as still
active at shutdown.

## Runs

| purpose | batch (exploration id) | run id | target/image | state | invariants |
|---|---|---|---|---|---|
| draft confirm (pre-fix, red invisible) | nd73xpnjjpgq2cb8y7nspf4pns8a7vc8 | 01KX3XH2YDD4R1CV0SFAFQKWRM | image 0133d42 | failed | hasInvariantViolation=False, 0 parsed (legacy emit) |
| draft confirm (post-fix, red surfaced) | nd77z6ft317jxeada3mmf19gxs8a7f4b | 01KX3XZ2KMD2XAMEKTQ5TRBY6Y | image b80b603 | failed | hasInvariantViolation=True, 11 parsed (10 PASS, 1 FAIL) |

Raw command:
```
.workers/run-with-postgres.sh .workers/python-runtime.sh \
  .workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py \
  --rung rung-002-queued-async-task-retention-gc-pressure --all-cases --sequential
```
Depth 1 (baseline, no faults), timeout 600, mem 2048. Failing case `case-003`,
workload-internal seed `7112`.

## Harness fix applied this episode

The async workload emitted invariant lines as `INVARIANT <name> <status>
<summary>` (3 fields) and signalled failure with the legacy `FINDING-CANDIDATE`
marker. The runtime parser needs `INVARIANT <id> <name> PASS|FAIL <summary>`
(id token first) and keys page-level failure on `WORKLOAD-FAIL`. Both were
corrected (commit b80b603) so the confirmed red — and the 10 PASS invariants —
now reach the status-page panel. The oracle itself is unchanged.

## Interpretation / open question

This is a real terminal-state task-retention leak by the workload's oracle, but
the overview flags **workload-model-artifact risk** for this async area (one
recovery candidate was previously closed as a workload-model artifact). Before
any upstream filing, a human must confirm the surviving `_execute_workflow_async`
tasks are held by DBOS internals and not merely by the workload's own snapshot
reference. `reported: null` until that triage. Published to the internal status
page as an intercepted red.
