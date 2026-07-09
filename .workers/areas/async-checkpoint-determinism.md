# Area: async-checkpoint-determinism

## Current State

Current status: one composed async checkpoint/cancellation rung completed
green; deeper queued async task-retention and preemptible-step cancellation
rungs are executor-ready.

Recent issue/PR scan used for this frontier:

- Issue `#688`: `asyncio.gather()` of distinct async steps could record
  nondeterministic checkpoint order, causing `DBOSUnexpectedStepError` on
  multi-worker recovery.
- PR `#715`: fixed `#714` by detecting patching nondeterminism instead of
  corrupting checkpoint counters or zombie-polling.
- Issue `#714`: concurrent `DBOS.patch_async` /
  `DBOS.deprecate_patch_async` could race the function-id counter and make
  `persist()` poll forever.
- PR `#711`: fixed `#710` by keeping strong references to queued async
  workflow tasks so GC cannot destroy pending tasks mid-execution.
- PR `#732`: fixed async cancellation while launching a child workflow; this
  landed after the current target ref `0c41e6df...` and is strong stale-drift
  evidence for cancellation/context propagation.
- Issue `#660` / PR `#671`: added `preemptible=True` for async steps so a
  cancelled workflow can interrupt a running step, avoid recording a poisoned
  operation output, and resume the step from scratch.
- CI run `28050935566` on PR branch `kraftp/race` failed in mypy
  (`tests/test_failures.py:1168` unused ignore), not a behavioral flaky test;
  checked and not used as product-surface evidence.

## Product Promise

Async DBOS workflows preserve deterministic checkpoint positions, workflow
context, child workflow ownership, and terminal liveness when operations are
scheduled concurrently, recovered on another executor, cancelled, or replayed.

## Why This Matters

Async users naturally compose steps, child workflows, sleeps, messages, patches,
and external awaits with `asyncio.gather`, `TaskGroup`, cancellation, queue
workers, and recovery. A checkpoint-position race can turn a valid workflow into
permanent retry, duplicated effects, missing child lineage, or a hung handle
even when each individual API has a narrow regression test.

## Evidence

- Code:
  - `target/dbos/_context.py`: `DBOSContext.snapshot_step_ctx` and
    `create_start_workflow_child` mutate the per-workflow `function_id`
    counter and assign child workflow IDs.
  - `target/dbos/_core.py`: `execute_workflow_by_id`,
    `start_workflow_async`, child workflow recording, and async task pinning.
  - `target/dbos/_dbos.py`: `patch_async` and `deprecate_patch_async` probe,
    revalidate, and reserve checkpoint positions on the event loop.
  - `target/dbos/_sys_db.py`: checkpoint lookup/recording for operations and
    patch markers.
- Tests:
  - `target/tests/test_concurrency.py::test_gather_distinct_steps_deterministic_order`
    covers the simple `#688` gather-order regression without recovery faults.
  - `target/tests/test_async.py::test_concurrent_patch_async` covers clean
    failure for concurrent patch markers from `#714/#715`.
  - `target/tests/test_queue.py::test_enqueued_async_workflow_survives_gc`
    covers the `#710/#711` queued async task strong-reference regression.
  - `target/tests/test_async_workflow_management.py` covers narrow
    preemptible-step cancel/resume, `run_step_async` option, sync rejection,
    and outer-cancel leak regressions from `#660/#671`.
  - `target/tests/test_async.py` contains child workflow and cancellation
    tests, but the `#732` cancellation/context fix is newer than the pinned
    target.
- Recent churn:
  - `#688`, `#710`, `#714`, `#711`, `#715`, `#732`, and `#671` are all
    recent async checkpoint/liveness/context/preemption issues or fixes.
  - `#732` after the pinned target makes child-launch cancellation an especially
    useful drift gate.

## What Not To Repeat

- Do not repeat the simple product tests for gather ordering, concurrent patch
  detection, or queued async GC pinning as standalone workload cases.
- Do not add another #710/#711 check unless it preserves the production failure
  mechanism: queued async workflows whose original async task handles are
  discarded, suspended on frame-local awaitables, under forced cyclic GC, with
  terminal state and task-release oracles.
- Do not repeat the narrow `#660/#671` product tests unless the workload adds
  preempted operation-output isolation, retry-bypass, resume, option-path, or
  runtime-boundary evidence beyond a direct cancel/resume assertion.
- Do not assert that `recover_pending_workflows()` is an execution barrier.
  Recovery cases must use explicit gates and await recovered handles or public
  status APIs before asserting terminal state.
- Do not treat every concurrent async interleaving as supported. Some
  interleavings, especially concurrent patch markers, have a defensible product
  contract of clean bounded failure rather than success.

## Adversarial Model

The frontier attacks the assumption that async checkpoint positions are stable
when sibling tasks reserve DBOS operations around awaits, thread offloads,
recovery boundaries, queue worker handoff, or cancellation cleanup.

The model uses explicit application gates, reversed completion order, forced
recovery/relaunch windows, bounded wait timeouts, and read-only DBOS state
inspection to distinguish product bugs from expected scheduling variation.

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
      "read-only:target/tests/test_concurrency.py,target/tests/test_async.py,target/tests/test_queue.py",
      "3 existing product tests",
      "read-only evidence for #688, #714/#715, and #710/#711 narrow regressions",
    ]
  - [
      "rung-001-async-checkpoint-recovery-cancel-compose",
      "inline:rung-001-async-checkpoint-recovery-cancel-compose",
      "passed",
      "1",
      "cross-frontier",
      ".workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py",
      "3 cases",
      "compose async gather, patch nondeterminism, recovery, cancellation, and child context propagation without duplicating narrow unit tests",
    ]
  - [
      "rung-002-queued-async-task-retention-gc-pressure",
      "inline:rung-002-queued-async-task-retention-gc-pressure",
      "ready",
      "2",
      "liveness-regression",
      ".workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py",
      "4 cases",
      "queued async workflows must remain strongly reachable under cyclic GC pressure until terminal state, then release task references without leaking",
    ]
  - [
      "rung-003-preemptible-step-cancel-resume-isolation",
      "inline:rung-003-preemptible-step-cancel-resume-isolation",
      "ready",
      "3",
      "preemption-regression",
      ".workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py",
      "4 cases",
      "preemptible async steps cancel mid-await, bypass retries, avoid poisoned step outputs, and resume to one successful durable result",
    ]
```

## Rung Details

### Rung: rung-000-product-regression-baseline

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-product-regression-baseline
frontier: async-checkpoint-determinism
status: not_run_optional
order: 0
level: baseline
workload_file: read-only:target/tests/test_concurrency.py,target/tests/test_async.py,target/tests/test_queue.py
seeds: [0]
updated_at: 2026-06-24T00:00:00Z
```

#### Source Contract

- Frontier ID: `async-checkpoint-determinism`.
- Rung ID: `rung-000-product-regression-baseline`.
- Protected product promise: async checkpoint order, patch nondeterminism
  handling, and queued async task lifetime match current product tests.
- Replay command: read-only product pytest selection if an executor wants setup
  proof; no generated workload code is needed for this rung.
- Seed policy: fixed seed `0`; no generated variance.
- Invariant oracle: the selected product tests pass at target ref
  `0c41e6df...`.

### Rung: rung-001-async-checkpoint-recovery-cancel-compose

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-async-checkpoint-recovery-cancel-compose
frontier: async-checkpoint-determinism
status: passed
order: 1
level: cross-frontier
workload_file: .workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py
seeds: [7310, 7311, 7312]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/issues/688
  - https://github.com/dbos-inc/dbos-transact-py/issues/714
  - https://github.com/dbos-inc/dbos-transact-py/issues/710
  - https://github.com/dbos-inc/dbos-transact-py/pull/711
  - https://github.com/dbos-inc/dbos-transact-py/pull/715
  - https://github.com/dbos-inc/dbos-transact-py/pull/732
  - target/dbos/_context.py
  - target/dbos/_core.py
  - target/dbos/_dbos.py
  - target/dbos/_sys_db.py
  - target/tests/test_concurrency.py
  - target/tests/test_async.py
  - target/tests/test_queue.py
```

#### Source Contract

- Frontier ID: `async-checkpoint-determinism`.
- Rung ID: `rung-001-async-checkpoint-recovery-cancel-compose`.
- Protected product promise: async DBOS operations reserve deterministic
  checkpoint positions and preserve workflow/child context across concurrent
  scheduling, recovery, cancellation, and replay.
- Replay command:
  `python .workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py --rung rung-001-async-checkpoint-recovery-cancel-compose --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `7310`, `7311`, `7312`; every run must persist the
  seed, derived delays, workflow IDs, and gate timing.
- Invariant oracle: terminal status, public handle result/error, workflow step
  list, child workflow lineage, patch marker/error classification, and modeled
  effect ledger must all agree within a bounded timeout.

#### Goal

Build one workload that composes the recent async issue surfaces instead of
repeating the narrow product tests. The workload should prove that valid
concurrent async workflows recover deterministically, and invalid concurrent
patch marker usage fails cleanly without hanging.

#### Workload File

- Expected path:
  `.workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py`.
- Create or reuse: create this file for this frontier; later rungs can reuse it
  while the oracle remains a checkpoint/context/effect ledger.
- Why one file is enough: the cases share setup, async workflow definitions,
  gate control, status/step inspection, and bounded liveness assertions.

#### Workload Shape

- Type: product-runtime adversarial workload with public DBOS APIs plus
  read-only system state inspection where needed.
- Entry points:
  - `DBOS.step` async functions called through `asyncio.gather`
  - `DBOS.patch_async` and `DBOS.deprecate_patch_async`
  - `DBOS.start_workflow_async`, queued async execution, workflow handle result
  - `DBOS._recover_pending_workflows` only with explicit gates and no barrier
    assumption
  - `DBOS.cancel_workflow`, `DBOS.get_workflow_status_async`,
    `DBOS.list_workflow_steps_async`
- Fault model: reversed async completion order, explicit pause before/after
  checkpointed operations, executor destroy/relaunch or recovery call while
  gated, cancellation while a parent is starting children, and bounded handle
  retrieval.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 7310 | gather-distinct-steps-recover | reverse completion order, gate after first checkpoint, recover and await handle | valid `asyncio.gather` workflow returns once, records step function IDs in declaration order, and never raises `DBOSUnexpectedStepError` |
| case-002 | 7311 | concurrent-patch-plus-steps | sibling tasks call patch/deprecate and normal steps around explicit yields | invalid concurrent patch interleaving fails within timeout with `DBOSPatchNondeterminismError`-class semantics, not zombie polling or duplicate markers |
| case-003 | 7312 | cancel-during-async-child-start | cancel/destroy parent while async child start is in flight, then inspect parent and child | child workflow ID and `parent_workflow_id` are nonblank and modeled, parent terminal/error state is bounded, and no blank-context child survives |

#### Invariants

- Must hold for valid gather/recovery case:
  - The workflow reaches terminal `SUCCESS` once and the public result equals
    the modeled result.
  - `list_workflow_steps` ordered by `function_id` matches the declared gather
    order, not task completion order.
  - The modeled effect ledger has exactly one effect per logical step.
  - Recovery/relaunch does not produce `DBOSUnexpectedStepError`, duplicate
    operation rows, or an indefinitely pending handle.
- Must hold for concurrent patch case:
  - The workflow reaches terminal `ERROR` or raises through the public handle
    within the bounded timeout.
  - The error classification is the product's nondeterminism/patch error
    family, not `DBOSWorkflowConflictIDError`, generic timeout, or endless
    polling.
  - Patch markers and normal step rows do not show duplicate function IDs or
    a partially successful ledger that contradicts terminal state.
- Must hold for child cancellation case:
  - Any child workflow created by the parent has a nonblank workflow ID and
    the expected `parent_workflow_id`.
  - Parent status, child status, and durable child records agree after
    cancellation/recovery cleanup.
  - The workload must not assume cancellation is instantaneous; it should poll
    bounded public status until terminal or declared timeout.
- Must never happen:
  - A workload-level timeout is reported as a product finding without durable
    status/handle evidence.
  - Recovery assertions rely on `_recover_pending_workflows()` returning after
    recovered bodies are already blocked or complete.

#### Expected Signatures

- Success: all three cases satisfy the ledger, status, and bounded liveness
  invariants.
- Finding: any duplicate effect, wrong step order after recovery, unsupported
  error classification, child with blank/missing parent context, contradictory
  public/durable terminal state, or unbounded polling with durable evidence.
- Setup block: target cannot launch isolated DBOS runtime, recover workflows,
  or inspect statuses under the selected build profile.
- Low signal: the workload only runs the simple existing product regression
  tests or asserts scheduler timing without explicit gates.

### Rung: rung-002-queued-async-task-retention-gc-pressure

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-002-queued-async-task-retention-gc-pressure
frontier: async-checkpoint-determinism
status: ready
order: 2
level: liveness-regression
workload_file: .workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py
seeds: [7110, 7111, 7112, 7113]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/issues/710
  - https://github.com/dbos-inc/dbos-transact-py/pull/711
  - target/dbos/_core.py
  - target/dbos/_dbos.py
  - target/tests/test_queue.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
gate_results:
  surface_evidence: ready_from_issue_710_pr_711_target_async_task_pinning_code_and_product_gc_test
  duplicate_check: distinct_from_rung_001_because_it_targets_async_task_reachability_under_queue_handoff_and_gc_pressure_not_checkpoint_order_or_child_context
  oracle_critic: ready_with_task_pin_count_pending_future_ledger_generator_exit_absence_terminal_status_and_release_invariants
  executor_feasibility: default_profile; no_optional_services; forced_gc_and_bounded_inprocess_queue_workers
```

#### Source Contract

- Frontier ID: `async-checkpoint-determinism`.
- Rung ID: `rung-002-queued-async-task-retention-gc-pressure`.
- Protected product promise: enqueued async workflow tasks remain strongly
  reachable after queue dequeue and handle conversion to polling handles, cannot
  be destroyed by cyclic GC while suspended, and release their strong references
  after terminal completion, cancellation, or modeled error.
- Replay command:
  `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py --rung rung-002-queued-async-task-retention-gc-pressure --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `7110`, `7111`, `7112`, `7113`; every run must
  persist workflow IDs, queue configuration, task-pin snapshots, GC counts,
  weakref liveness, interruption ledger, terminal statuses, public handle
  outcomes, and task-release observations.
- Invariant oracle: the modeled active async workflow count, `dbos._workflow_tasks`
  snapshots, pending future weakrefs, public workflow statuses, handle results,
  and application interruption ledger must agree within bounded waits.

#### Goal

Extend the narrow #710/#711 product regression into a workload that resembles
production queue pressure. Multiple enqueued async workflows suspend on futures
that are only strongly reachable through their coroutine frames. The workload
forces cyclic GC while public callers hold only polling handles, then unblocks
the workflows in modeled order and proves terminal results and task cleanup.

#### Workload File

- Expected path:
  `.workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py`.
- Create or reuse: reuse the existing async checkpoint workload file. This rung
  shares the async runtime setup, queue handoff, public status, and liveness
  oracle family with the area, but has a distinct `--rung` selector.
- Why this is not only a product-test wrapper: it varies workflow count, queue
  concurrency, release order, cancellation/error branches, and repeated GC
  cycles while asserting both no premature task destruction and no strong
  reference leak after terminal state.

#### Workload Shape

- Type: background-job liveness/stateful workload.
- Build profile: `default`.
- Setup: real Postgres through `.workers/run-with-postgres.sh`; no Kafka or
  optional services.
- Entry points:
  - `DBOS.register_queue()` / `Queue`
  - `DBOS.enqueue_workflow()` for async workflow functions
  - queue worker dequeue through normal runtime execution
  - public handle polling / `DBOS.get_workflow_status`
  - read-only `dbos._workflow_tasks` snapshots as diagnostic evidence for the
    #710/#711 regression guard.
- Fault model: discard async task handles via the queue/dequeue path, suspend
  workflows on frame-local futures exposed only through weakrefs, run repeated
  `gc.collect()` cycles under bounded allocation pressure, then release,
  cancel, or fail workflows according to the case model.

#### Parameter Matrix

| Case | Seed | Scenario | Fault model | Primary oracle |
|---|---:|---|---|---|
| `case-001` | 7110 | one queued async workflow suspends on a frame-local future and is unblocked after forced GC | baseline #710 reproduction path through the real DBOS queue | no `GeneratorExit`, workflow remains pinned while pending, result `done`, task set released |
| `case-002` | 7111 | six queued async workflows with queue concurrency 2 suspend, experience repeated GC/allocation pressure, then release in reverse enqueue order | production-like concurrent queue pressure with polling handles only | active pin count matches modeled running workflows, all terminal successes, no duplicate/interrupted ledger entries |
| `case-003` | 7112 | cancel half of the suspended async workflows after GC and release the rest | terminal cleanup branch under cancellation | cancelled workflows terminal or modeled cancelled/error, released workflows succeed, no pinned task leak after all terminal |
| `case-004` | 7113 | one suspended workflow raises a modeled application error after surviving GC while siblings succeed | error cleanup branch and error classification | modeled app error persists, successes persist, no `GeneratorExit`, all task references release |

#### Invariants

- Must hold: every workflow reaches the modeled suspended state before GC
  assertions start; otherwise classify setup as blocked, not green.
- Must hold: while workflows are suspended, `dbos._workflow_tasks` contains at
  least the modeled active async workflow tasks and the weakly exposed futures
  are still alive.
- Must hold: after repeated forced GC cycles, the interruption ledger contains
  no `GeneratorExit`, `RuntimeError: coroutine ignored GeneratorExit`, or other
  premature task-destruction signature.
- Must hold: public workflow status does not become `ERROR` with a GC/GeneratorExit
  error unless the case explicitly models an application error.
- Must hold: after releasing, cancelling, or erroring all modeled workflows,
  public handles and durable statuses match the case model within a bounded
  wait.
- Must hold: after terminal observation, `dbos._workflow_tasks` eventually
  returns to empty, proving the strong-reference fix does not leak completed
  tasks.
- Must never happen: the workload passes by keeping extra strong references to
  task or future objects in the harness, by never using the queue/dequeue path,
  by only running the product test, or by ignoring task-release cleanup.

#### Finding Classification

- Product finding: any `GeneratorExit`/destroyed-pending-task signature, GC
  error terminal state, missing pending future after GC, contradictory public
  handle/status result, or leaked `_workflow_tasks` entries after all modeled
  workflows are terminal.
- Workload bug: the harness keeps strong references to futures/tasks outside
  the modeled weakrefs, the workflows never suspend before GC, the queue worker
  never dequeues, or cancellation/error cases do not reach terminal states.
- Low signal: one direct `start_workflow_async` call without queue handoff, one
  product-test clone, or final-success-only assertions without GC and task
  release observations.

### Rung: rung-003-preemptible-step-cancel-resume-isolation

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-003-preemptible-step-cancel-resume-isolation
frontier: async-checkpoint-determinism
status: ready
order: 3
level: preemption-regression
workload_file: .workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py
seeds: [6600, 6601, 6602, 6603]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/issues/660
  - https://github.com/dbos-inc/dbos-transact-py/pull/671
  - target/dbos/_core.py
  - target/dbos/_dbos.py
  - target/tests/test_async_workflow_management.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/risk-based-testing/overview.md
gate_results:
  surface_evidence: ready_from_issue_660_pr_671_target_preemptible_step_source_and_product_tests
  duplicate_check: distinct_from_rung_001_generic_async_cancellation_and_rung_002_task_gc_because_it_targets_mid_step_preemption_checkpoint_isolation_retry_bypass_and_resume
  oracle_critic: ready_with_preemption_ledger_retry_ledger_operation_output_rows_status_result_and_task_cleanup_invariants
  executor_feasibility: default_profile; real_postgres; no_optional_services; bounded_application_gates
```

#### Source Contract

- Frontier ID: `async-checkpoint-determinism`.
- Rung ID: `rung-003-preemptible-step-cancel-resume-isolation`.
- Protected product promise: `preemptible=True` async steps cancel while inside
  a long await when their workflow is cancelled, do not record a preempted step
  output/error, bypass retry storms, and can be resumed to record exactly one
  successful step output.
- Replay command:
  `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py --rung rung-003-preemptible-step-cancel-resume-isolation --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `6600`, `6601`, `6602`, `6603`; every run must
  persist workflow IDs, gate order, invocation counts, cancellation ledger,
  retry validator calls, status/result observations, and step row snapshots.
- Invariant oracle: application cancellation ledger, retry ledger, public
  workflow status/result, `list_workflow_steps`, optional read-only
  operation-output rows, and task cleanup observations must agree with the case
  model.

#### Goal

Extend the narrow preemptible-step product tests into a workload that composes
mid-step cancellation, retry configuration, resume, option-path parity, a
non-preemptible control, and a runtime-boundary replay check. The workload
should catch regressions where preemption either becomes a no-op or poisons the
durable checkpoint state used by resume.

#### Workload File

- Expected path:
  `.workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py`.
- Create or reuse: reuse the async checkpoint workload file with a distinct
  `--rung rung-003-preemptible-step-cancel-resume-isolation` selector.
- Why this is not only a product-test wrapper: it varies decorator vs
  `run_step_async` configuration, retry settings, non-preemptible controls,
  runtime-boundary resume, and operation-output oracle points rather than only
  asserting one direct cancel/resume path.

#### Workload Shape

- Type: product-runtime async cancellation/stateful workload.
- Build profile: `default`.
- Setup: real Postgres through `.workers/run-with-postgres.sh`; no Kafka or
  optional services.
- Entry points:
  - `DBOS.step(preemptible=True, retries_allowed=True, ...)`
  - `DBOS.run_step_async({"preemptible": True, ...}, ...)`
  - `DBOS.start_workflow_async`, `DBOS.cancel_workflow_async`,
    `DBOS.resume_workflow_async`, and workflow handle result/error APIs
  - `DBOS.list_workflow_steps_async` plus read-only operation-output inspection
    if public step listing cannot prove absence of a preempted row.
- Fault model: cancel after a step-start gate but before application release,
  compare preemptible and non-preemptible branches, force retry-capable step
  options, then resume and inspect durable step rows.

#### Parameter Matrix

| Case | Seed | Scenario | Fault model | Primary oracle |
|---|---:|---|---|---|
| `case-001` | 6600 | decorated preemptible step with retries is cancelled mid-await, then resumed | cancellation could record a durable error row or enter retry machinery | one pre-resume invocation, no retry ledger, cancelled public handle, resume returns modeled output, exactly one successful step row |
| `case-002` | 6601 | `DBOS.run_step_async({"preemptible": True})` is cancelled mid-await, then resumed | option path could ignore `preemptible=True` or diverge from decorator semantics | application step sees cancellation before gate release, resume succeeds, operation output is not poisoned |
| `case-003` | 6602 | matched preemptible and non-preemptible long steps are cancelled before their gates release | preemptible flag could be a no-op or cancellation could require application release | preemptible branch settles and records cancellation ledger before gate release; non-preemptible control does not record application cancellation until released |
| `case-004` | 6603 | preempted workflow is resumed after relaunch/recovery-style runtime boundary | preempted attempt could leave durable state that blocks rerun in a fresh DBOS instance | post-boundary resume reruns step once, final status/result/step list match the model |

#### Invariants

- Must hold: every case reaches the modeled in-step blocked state before
  cancellation is injected.
- Must hold: preemptible application code observes cancellation without the
  workload releasing the application gate first.
- Must hold: preempted cancellation produces exactly one application invocation
  before resume and does not call retry validators or exhaust retries.
- Must hold: before resume, the preempted attempt has no durable step
  output/error row.
- Must hold: after resume, the final workflow returns the modeled result and
  exactly one successful step output row exists for the logical step.
- Must hold: decorator and `run_step_async` option paths agree on cancellation,
  resume, and operation-output isolation.
- Must hold: the non-preemptible control does not falsely report application
  interruption before gate release.
- Must never happen: the workload passes by releasing the application gate
  before observing preemptible cancellation, by asserting only public
  cancellation without inspecting step rows, or by treating bounded timeout as
  a product finding without durable status/ledger evidence.

#### Finding Classification

- Product finding: preemptible cancellation hangs until app release, retry
  validators run for preemption, `DBOSMaxStepRetriesExceeded` is recorded,
  preempted operation-output rows poison resume, duplicated/missing final step
  rows appear, `run_step_async` ignores `preemptible=True`, or task/poller
  cleanup leaks after terminal observation.
- Workload bug: the workload cancels before the step is actually running,
  releases the gate before checking preemption, keeps unmodeled strong
  references that alter runtime behavior, or assumes cancellation is
  instantaneous without bounded polling.
- Low signal: a clone of `target/tests/test_async_workflow_management.py`
  without operation-output, retry, option-path, or runtime-boundary oracles.

## Oracle Contract

The oracle is a modeled checkpoint/context/preemption ledger keyed by workflow
ID and logical operation. Public status, handle result/error, step list, child
lineage, retry/cancellation ledger entries, task-retention observations, and
modeled application effects must agree. The oracle may allow expected
product-level nondeterminism errors for invalid concurrent patch usage, but it
must not weaken liveness, duplicate-effect, function-id uniqueness, child
context, preempted-operation-output isolation, or retry-bypass requirements to
make the workload pass.

## Stale Conditions

Mark stale if DBOS changes the async checkpoint counter model, patch
nondeterminism contract, child workflow ID/parent context semantics, recovery
handle behavior, queued async task ownership/pinning, `_workflow_tasks`
lifecycle, `preemptible=True` semantics, step retry cancellation
classification, or if the harness target ref advances past `#732` and rung 001
needs to be reframed as regression-proof rather than drift detection.
