# Area: runtime-shutdown-event-loop-liveness

## Current State

Current status: new area opened from target commit `79298a0` / PR `#722`;
one executor-ready rung is queued in work item `E-017`.

Recent issue/PR scan used for this area:

- PR `#722`: "Fix Destroy Hang" fixed a `DBOS.destroy()` deadlock when timeout
  waiters were live on the adopted main event loop.
- The fix added `BackgroundEventLoop.target_loop()`, made
  `submit_coroutine()` fail loudly when called from its own target loop, and
  changed `DBOS._destroy()` to cancel timeout tasks directly when destroy runs
  on the loop that owns those tasks.
- Existing workload areas cover async checkpoint order, message/event
  cancellation, queue worker recovery, and scheduler/debouncer timing. They do
  not cover runtime shutdown liveness while DBOS has pending timeout tasks on an
  adopted application event loop.

## Product Promise

DBOS shutdown and background-event-loop helpers must not deadlock when DBOS has
adopted an application event loop and timeout waiter tasks are still pending.
`DBOS.destroy()` must complete within a bounded window, clear or cancel its
timeout tasks, and leave the runtime reusable. Blocking coroutine submission
from the target loop's own thread must fail loudly instead of hanging forever.

## Why This Matters

Async applications commonly launch DBOS inside FastAPI or another running event
loop, then shut down from that same loop during process teardown. If shutdown
blocks on a coroutine scheduled to the loop it is already occupying, deploys,
tests, and local servers can hang indefinitely. A hung shutdown can also hide
unfinished workflows, leak DB connections, and make later test or process
startup unreliable.

## Evidence

- Code:
  - `target/dbos/_dbos.py`: `_destroy()` now detects when it runs on the
    timeout-task owning loop and cancels tasks directly.
  - `target/dbos/_event_loop.py`: `target_loop()` centralizes loop selection,
    and `submit_coroutine()` rejects same-loop blocking calls.
  - `target/dbos/_core.py`: workflow timeout helpers use
    `submit_coroutine_nowait(..., task_set=dbos._timeout_tasks)`.
- Tests:
  - `target/tests/test_async.py::test_destroy_from_adopted_main_loop_does_not_deadlock`
    launches DBOS from a running loop, creates a pending timeout task, calls
    `DBOS.destroy()` from the same loop thread, and asserts the worker thread
    completes inside a bounded timeout.
  - `target/tests/test_async.py::test_submit_coroutine_from_own_loop_raises_instead_of_hanging`
    verifies same-loop blocking submission raises a deadlock-class error.
- Existing workloads/runs:
  - `async-checkpoint-determinism` / `E-004` covers async workflow checkpoint,
    recovery, cancellation, and child context; it does not target runtime
    shutdown while timeout tasks are pending.
  - `message-event-cancellation` / `E-012` covers listener cleanup and stream
    resume; it does not cover `DBOS.destroy()` self-deadlock.
  - `scheduler-debouncer-timing` / `E-008` is an environment-sensitive worker
    starvation candidate, not event-loop shutdown liveness.
- Recent churn:
  - `79298a00fec1eb51e81c4ab1d5d7706b17084592` / PR `#722` changed shutdown
    and event-loop behavior directly.

## What Not To Repeat

- Do not only run the two product tests from PR `#722`; the workload should
  compose launch, timeout-task creation, destroy, relaunch, and same-loop guard
  observations into one runtime lifecycle oracle.
- Do not classify slow workflow completion as a shutdown bug unless the workload
  proves a pending timeout task and a bounded destroy/relaunch invariant.
- Do not assume every pending task is a DBOS timeout task; the oracle must track
  DBOS-owned timeout tasks separately from unrelated application tasks.
- Do not write artifacts under `/workspace`; use `/tmp/...` for any run logs.

## Adversarial Model

The area attacks the assumption that shutdown code can always schedule cleanup
work to the background event loop and block for the result. The adversarial
state is an application event loop adopted by DBOS as its target loop, with
DBOS-owned timeout waiter tasks still pending, followed by shutdown from that
same loop thread. A second adversarial check calls blocking submission from the
target loop to ensure it raises instead of hanging.

## Rung Index

```yaml
schema_version: 1
columns: [id, path, status, order, level, workload_file, matrix, summary]
rungs:
  - [
      "rung-000-product-regression-baseline",
      "inline:rung-000-product-regression-baseline",
      "not_run_optional",
      "0",
      "baseline",
      "read-only:target/tests/test_async.py",
      "2 existing product tests",
      "read-only evidence for PR #722 destroy and same-loop submit deadlock fixes",
    ]
  - [
      "rung-001-adopted-loop-timeout-destroy-liveness",
      "inline:rung-001-adopted-loop-timeout-destroy-liveness",
      "ready",
      "1",
      "runtime-lifecycle",
      ".workers/workloads/runtime-shutdown-event-loop-liveness/runtime_shutdown_event_loop_liveness_workload.py",
      "3 cases",
      "destroy and same-loop coroutine submission must be bounded when timeout waiters live on DBOS adopted event loops",
    ]
```

## Rung Details

### Rung: rung-000-product-regression-baseline

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-product-regression-baseline
frontier: runtime-shutdown-event-loop-liveness
status: not_run_optional
order: 0
level: baseline
workload_file: read-only:target/tests/test_async.py
seeds: [0]
updated_at: 2026-06-24T00:00:00Z
```

#### Source Contract

- Frontier ID: `runtime-shutdown-event-loop-liveness`.
- Rung ID: `rung-000-product-regression-baseline`.
- Protected product promise: the narrow PR `#722` regression tests pass on the
  target evidence ref.
- Replay command: optional read-only product pytest selection:
  `pytest target/tests/test_async.py -k 'destroy_from_adopted_main_loop_does_not_deadlock or submit_coroutine_from_own_loop_raises_instead_of_hanging'`.
- Seed policy: fixed seed `0`.
- Invariant oracle: the selected product tests pass at the target evidence ref
  or at an explicitly refreshed target ref.

### Rung: rung-001-adopted-loop-timeout-destroy-liveness

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-adopted-loop-timeout-destroy-liveness
frontier: runtime-shutdown-event-loop-liveness
status: ready
order: 1
level: runtime-lifecycle
workload_file: .workers/workloads/runtime-shutdown-event-loop-liveness/runtime_shutdown_event_loop_liveness_workload.py
seeds: [7220, 7221, 7222]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - target/dbos/_dbos.py
  - target/dbos/_event_loop.py
  - target/dbos/_core.py
  - target/tests/test_async.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/resilience-testing-and-fault-injection/overview.md
gate_results:
  surface_evidence: ready_from_pr_722_and_target_destroy_event_loop_code
  duplicate_check: existing_async_and_message_workloads_do_not_cover_shutdown_self_deadlock_with_timeout_tasks
  oracle_critic: ready_with_thread_join_bounds_timeout_task_ledger_and_relaunch_reusability
  executor_feasibility: default_profile; no_external_service_beyond_standard_dbos_setup
```

#### Source Contract

- Frontier ID: `runtime-shutdown-event-loop-liveness`.
- Rung ID: `rung-001-adopted-loop-timeout-destroy-liveness`.
- Protected product promise: DBOS runtime shutdown is bounded and reusable when
  timeout waiters are live on an adopted event loop, and same-loop blocking
  coroutine submission fails loudly instead of deadlocking.
- Replay command:
  `python .workers/workloads/runtime-shutdown-event-loop-liveness/runtime_shutdown_event_loop_liveness_workload.py --rung rung-001-adopted-loop-timeout-destroy-liveness --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `7220`, `7221`, `7222`; every run must persist loop
  identity, worker thread name, workflow IDs, timeout seconds, timeout-task
  counts before/after destroy, destroy duration, same-loop submit outcome,
  relaunch result, and any thread stacks captured on timeout.
- Invariant oracle: thread join, timeout-task ledger, public workflow result,
  same-loop submit classification, and relaunch smoke result must agree within
  bounded time.

#### Goal

Build one workload that proves DBOS lifecycle shutdown remains bounded under
the event-loop state that previously deadlocked, then proves the runtime can be
started again and execute a trivial workflow. The workload should include a
small same-loop `submit_coroutine` guard case so regressions fail fast rather
than hanging.

#### Workload File

- Expected path:
  `.workers/workloads/runtime-shutdown-event-loop-liveness/runtime_shutdown_event_loop_liveness_workload.py`.
- Create or reuse: create a new file. Existing workload files do not share this
  runtime shutdown oracle or bounded-hang harness shape.
- Why one file is enough: the cases share launch/destroy scaffolding, event-loop
  thread control, timeout-task ledgering, bounded joins, and relaunch smoke
  checks.

#### Workload Shape

- Type: runtime lifecycle / liveness workload with a bounded thread harness.
- Build profile: `default`.
- Setup: standard DBOS runtime setup. The workload may use the default Postgres
  build profile, but the product risk is event-loop shutdown and does not need
  Kafka or other optional services.
- Entry points:
  - `DBOS(config)`, `DBOS.launch`, `DBOS.destroy`,
    `DBOS.workflow`, `SetWorkflowTimeout`, workflow handle/result paths.
  - `BackgroundEventLoop.start`, `target_loop`, `submit_coroutine`, and
    `stop` for the direct same-loop guard case.
- Fault model: pending DBOS-owned timeout waiter tasks on an adopted target loop
  plus shutdown from that same loop thread.

#### Parameter Matrix

| Case | Seed | Scenario | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 7220 | adopted-loop-destroy-with-timeout-task | launch DBOS inside a dedicated event loop, run a workflow under `SetWorkflowTimeout`, wait until `dbos._timeout_tasks` is non-empty, then call `DBOS.destroy()` on that loop | destroy returns before the join bound, timeout tasks are cleared/cancelled, and no worker thread remains alive |
| case-002 | 7221 | same-loop-submit-coroutine-guard | start `BackgroundEventLoop` from a running loop, then call blocking `submit_coroutine()` from that same loop | raises deadlock-class `RuntimeError` promptly and closes/stops without leaked task warnings |
| case-003 | 7222 | destroy-then-relaunch-smoke | after case-001 teardown, create a fresh DBOS instance and run a trivial workflow with a timeout | relaunch and workflow result succeed; timeout-task count returns to zero after bounded cleanup |

#### Invariants

- Must hold for adopted-loop destroy:
  - The workload proves DBOS adopted the running loop as its target loop.
  - A DBOS-owned timeout task exists before destroy is invoked.
  - `DBOS.destroy(destroy_registry=True)` returns within the modeled bound
    (default expectation: 20 seconds or lower if executor chooses a tighter
    bound after local syntax/sanity review).
  - The event-loop worker thread is not alive after join, or the run is a
    liveness finding with stack evidence.
  - Timeout-task count is zero after destroy or the tasks are cancelled with
    bounded evidence.
- Must hold for same-loop submit guard:
  - `submit_coroutine()` invoked from its own target loop raises a
    deadlock-class `RuntimeError` promptly.
  - The coroutine is closed or otherwise does not emit an unawaited coroutine
    warning in the run artifact.
- Must hold for relaunch:
  - A fresh DBOS instance launches after the destroy case.
  - A trivial workflow under `SetWorkflowTimeout` returns the modeled result.
  - No timeout task from the prior instance remains observable.
- Must never happen:
  - The workload reports green without proving a pending timeout task before
    destroy.
  - A hang is classified without thread duration and stack or loop-state
    evidence.
  - The workload relies on an unbounded process hang as the only oracle; it
    must use bounded thread/process joins and emit diagnostics.

#### Expected Signatures

- Success: all three cases satisfy bounded destroy, same-loop guard, timeout
  cleanup, and relaunch smoke invariants.
- Finding: destroy thread remains alive past the bound, same-loop submit hangs
  or returns successfully instead of raising, timeout tasks survive teardown,
  relaunch fails due to leaked runtime state, or public workflow result/status
  contradicts the shutdown ledger.
- Setup block: the workload cannot launch an isolated DBOS runtime or cannot
  create a controlled event loop thread in the cloud environment.
- Low signal: the workload only checks that `DBOS.destroy()` can be called when
  no timeout tasks are pending.

## Oracle Contract

The oracle is a bounded lifecycle ledger: event-loop identity, DBOS adopted loop
identity, timeout-task count, destroy start/end timestamps, worker-thread join
state, same-loop submit outcome, and relaunch workflow result. It must fail for
missing risky state, unbounded shutdown, leaked timeout tasks, wrong
same-loop-submit behavior, or runtime reuse failure.

## Stale Conditions

Mark stale if DBOS changes `BackgroundEventLoop`, `DBOS._destroy`, timeout-task
tracking, `SetWorkflowTimeout`, workflow timeout implementation, or application
event-loop adoption semantics. Mark stale if target ref advances past PR `#722`
with a new shutdown lifecycle model that changes the expected same-loop
submission contract.
