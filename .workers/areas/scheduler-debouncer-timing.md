---
key: scheduler-debouncer-timing
title: Scheduler & debouncer timing
description: "Scheduled and debounced work fires predictably: exactly-once execution across concurrent bounces on one key, latest-input-wins, and no silent drop, duplicate, or per-key executor starvation."
order: 110
---

# Area: scheduler-debouncer-timing

## Current State

Current status: completed green, with loop-1 queued cross-frontier rungs.
Scheduler overlap semantics were treated as observational where product policy
was unclear.

Evidence:

- `evidence-key:frontiers/scheduler-debouncer-timing/frontier.md`
- `evidence-key:runs/run-20260620T141500Z-scheduler-debouncer-timing-rung-004-bounded-seed-sweep/summary.md`
- `.workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py`
- Issue `#724`: reported that one active debounce key could occupy one
  executor worker thread for the full debounce window.
- PR `#739`: "Improve Debouncer Workflow" made the internal debouncer
  workflow async after the current target ref `0c41e6df...`.
- Issue `#718`: open feature/contract discussion for preventing overlapping
  scheduled runs with per-schedule dedup/concurrency semantics; this keeps
  overlap observations non-failing until DBOS defines the policy.

## Product Promise

Timed, scheduled, and debounced work starts predictably, preserves latest
intended input, honors max-wait/delay behavior, and avoids stale handles or
unbounded worker pressure.

## What Not To Repeat

- Do not re-run latest-input/max-wait/delayed-row sweeps without new oracle
  depth.
- Do not turn unclear scheduler overlap semantics into a failing invariant
  without first grounding the contract.

## Loop-1 Search Directions

| Direction | Why It Is Deeper |
|---|---|
| Contract clarification for scheduled overlap | A stronger oracle may be possible if code/docs define overlap policy; open issue `#718` is currently a feature/semantics discussion, not a bug oracle. |
| Debouncer plus lifecycle commands | Cancel/resume/delete while a debounced row is delayed can expose stale row or handle bugs. |
| Debouncer plus queue live config | Worker pressure and queue limiter changes can interact with delayed debounce rows. |
| Time jumps and recovery | Recovery after sleep/debounce scheduling can expose delayed-row replay problems. |
| Scheduled work plus declared queues | Scheduler-origin rows can bypass queue controls if trigger/backfill/live paths do not preserve `queue_name`. |

## Rung Design Requirements

State whether each assertion is contractual or observational. Record timing
windows, clock assumptions, queue configuration, and durable rows used for
verification.

## Stale Conditions

Mark stale if scheduler/debouncer APIs or overlap semantics are clarified or
rewritten.

## Rung Index

Evidence source: `evidence-key:frontiers/scheduler-debouncer-timing/rungs.index.yaml`

```yaml
schema_version: 1
columns: [id, path, status, order, level, workload_file, matrix, summary]
rungs:
  - [
      "rung-000-timing-smoke",
      "rungs/rung-000-timing-smoke.md",
      "passed",
      "0",
      "baseline",
      ".workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py",
      "1 case",
      "prove durable sleep, schedule, and debouncer smoke without policy assertions",
    ]
  - [
      "rung-001-debouncer-delayed-row",
      "rungs/rung-001-debouncer-delayed-row.md",
      "passed",
      "1",
      "adversarial",
      ".workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py",
      "3 cases",
      "latest-input, max-wait, delayed-row, and handle conservation for debouncer",
    ]
  - [
      "rung-002-many-key-worker-pressure",
      "rungs/rung-002-many-key-worker-pressure.md",
      "passed",
      "2",
      "adversarial",
      ".workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py",
      "3 cases",
      "many debounce keys and delayed rows without unbounded worker/thread pressure",
    ]
  - [
      "rung-003-schedule-overlap-observation",
      "rungs/rung-003-schedule-overlap-observation.md",
      "passed",
      "3",
      "basic",
      ".workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py",
      "3 cases",
      "observe scheduled overlap behavior without asserting unresolved product policy",
    ]
  - [
      "rung-004-bounded-seed-sweep",
      "rungs/rung-004-bounded-seed-sweep.md",
      "passed",
      "4",
      "sweep",
      ".workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py",
      "24 cases",
      "rare-bug search across debounce timing, max-wait, and delayed-row update windows",
    ]
  - [
      "rung-005-scheduled-queue-controls-compose",
      "inline:loop-1-added-rung-rung-005-scheduled-queue-controls-compose",
      "queued",
      "5",
      "cross-frontier",
      ".workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py",
      "3 cases",
      "scheduled trigger/backfill/live rows must route to the declared queue and obey queue controls rather than the internal scheduler queue",
    ]
  - [
      "rung-006-async-debouncer-worker-starvation",
      "inline:loop-1-added-rung-rung-006-async-debouncer-worker-starvation",
      "queued",
      "6",
      "adversarial",
      ".workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py",
      "3 cases",
      "many long-window debounce keys must not occupy scarce executor threads or starve unrelated workflows while preserving latest-input semantics",
    ]
```

## Rung Details

These are the known rung contracts and outcomes for this frontier. Treat completed, finding, and closed rungs as current reality and design constraints, not as active executor queue rows.

### Loop-1 Added Rung: rung-005-scheduled-queue-controls-compose

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-005-scheduled-queue-controls-compose
frontier: scheduler-debouncer-timing
status: queued
order: 5
level: cross-frontier
workload_file: .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py
seeds: [3464, 3465, 3466]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - target/dbos/_scheduler.py
  - target/dbos/_dbos.py
  - target/tests/test_scheduler.py
  - .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py
  - .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/tools.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/tools.md
  - /Users/viswa/.agents/skills/wio/references/test-feedback-loops/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-feedback-loops/tools.md
gate_results:
  oracle_critic: ready_after_case_matrix_and_stale_conditions_added
  executor_feasibility: default_real_postgres_profile_is_realistic
```

#### Product Promise

Scheduled work created with `queue_name` routes to the declared DB-backed queue
and inherits that queue's controls across public schedule enqueue paths. It must
not silently fall back to the internal scheduler queue, bypass concurrency or
limiter controls, duplicate backfill effects, lose handle result retrieval, or
leave active queue rows after terminal completion.

#### Why This Is New

Existing scheduler rungs observe trigger/delete behavior but do not route
scheduled rows through a declared queue. Existing queue rungs cover explicit
`enqueue_workflow` and client enqueue paths, not scheduler-origin rows. Product
tests cover `queue_name` storage plus live/trigger smoke and backfill
idempotency separately, but do not combine trigger, backfill, live scheduler
tick, queue controls, row cleanup, and independent terminal slot conservation.

Do not use `@DBOS.scheduled` for this rung; that decorator path uses the
internal queue and is not the public `create_schedule(..., queue_name=...)`
contract under test.

#### Workload Shape

- Type: background-job/stateful queue workload.
- Build profile: `default`.
- Runtime setup: real Postgres through `.workers/run-with-postgres.sh`; no
  SQLite pass is meaningful for this rung.
- Expected workload file: reuse
  `.workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py`
  unless implementation clarity requires a new scheduler/queue file.
- Queue configuration for all cases:
  - Queue name: `wio_sched_queue_<case_id>_<seed>`.
  - Schedule name: `wio-sched-queue-<case_id>-<seed>`.
  - `concurrency=1`.
  - `worker_concurrency=1`.
  - limiter: `limit=2`, `period=1.0` seconds.
  - `polling_interval_sec=0.05`.
- Scheduled workflow body:
  - Append a start row to an independent in-memory or app-table ledger with
    `slot_id`, `workflow_id`, `queue_name`, `scheduled_at`, `context`, and
    monotonic start timestamp.
  - Block on a harness gate long enough to expose more-than-one concurrent
    start if queue controls are bypassed.
  - Return deterministic result `{"slot_id": ..., "seed": ...}`.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Slots | Expected Focus |
|---|---:|---|---|---|---|
| case-001 | 3464 | trigger-plus-explicit-backfill | trigger and backfill use the same scheduler enqueue helper but may route differently from ordinary queue enqueues | one trigger slot recorded at runtime plus hourly backfill slots `2025-01-01T01:00:00+00:00`, `2025-01-01T02:00:00+00:00`, `2025-01-01T03:00:00+00:00` from window `(2025-01-01T00:30:00+00:00, 2025-01-01T03:30:00+00:00)` | declared queue row propagation, result retrieval, queue cleanup |
| case-002 | 3465 | repeated-backfill-idempotency-under-blocked-queue | deterministic backfill workflow IDs may duplicate effects when the same window is replayed while earlier rows are queued or terminal | hourly backfill slots `2025-02-01T01:00:00+00:00`, `2025-02-01T02:00:00+00:00`, `2025-02-01T03:00:00+00:00` from window `(2025-02-01T00:30:00+00:00, 2025-02-01T03:30:00+00:00)`; second backfill of the same window returns the same workflow IDs | exactly-once terminal effects for replayed backfill slots |
| case-003 | 3466 | live-tick-plus-trigger-backlog | live scheduler and manual trigger may race into different queues or overstart despite `concurrency=1` | one generated live tick from `* * * * * *` captured in the derived slot ledger plus one manual trigger slot | live scheduler row routing, bounded concurrency, no internal queue fallback |

Case definitions must persist a derived slot ledger, because trigger/live
timestamps are runtime-generated. For explicit backfill cases, the listed
timestamps and schedule name fix expected workflow IDs as
`sched-<schedule_name>-<slot_isoformat>`.

#### Replay Commands

- Single case:
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-005-scheduled-queue-controls-compose --case case-001`
- Full matrix:
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-005-scheduled-queue-controls-compose --all-cases --sequential`

#### Required Artifacts

Each case must write seed, derived slot ledger, schedule name, queue
configuration, expected workflow IDs, observed workflow IDs, public handle
results, schedule rows, workflow status snapshots before release and at
terminal state, queue status snapshots, start/finish ledger rows, cleanup poll
result, product commit, and redacted Postgres URLs.

#### Invariants

- Must hold: every scheduled workflow row for this schedule has
  `queue_name == modeled_queue_name`.
- Must hold: no workflow row for this schedule uses `_dbos_internal_queue`.
- Must hold: every modeled slot has exactly one terminal successful effect and
  handle result.
- Must hold: repeated explicit backfill of the same window returns the same
  workflow IDs and does not create additional ledger effects.
- Must hold: observed simultaneous workflow-body starts never exceed the
  configured queue concurrency of 1.
- Must hold: the limiter window does not allow more than two starts in any
  modeled one-second interval after the blocker is released.
- Must hold: after all modeled slots are terminal and the cleanup poll has had a
  healthy Postgres window, no active queue rows remain for this queue.
- Must never happen: the workload passes because the live tick was not observed,
  the queue was undeclared, the schedule used the internal queue path, or only
  command completion was checked.

#### Expected Signatures

- Success: all case slots reach terminal success with queue row propagation,
  concurrency/limiter, idempotent backfill, result retrieval, and cleanup
  invariants satisfied.
- Finding: any scheduled row on the internal queue, overstart, duplicate
  terminal slot effect, lost result retrieval, active-row leak, or model/result
  disagreement.
- Setup block: real Postgres cannot start, queue registration fails, the live
  tick cannot be observed within the bounded window, or runtime dependencies
  cannot import DBOS under the default build profile.
- Low signal: the runner only checks that schedule APIs return, does not block
  workflow bodies, does not inspect durable rows, or treats scheduler overlap
  policy as the product oracle.

#### Stale Conditions

Mark stale if DBOS changes `create_schedule(..., queue_name=...)` validation,
`_enqueue_scheduled_workflow` queue routing, `backfill_schedule` deterministic
workflow ID semantics, `workflow_status.queue_name`, DB-backed queue cleanup or
limiter semantics, or the scheduler documentation explicitly states that
scheduled rows are not expected to obey declared queue controls.

### Loop-1 Added Rung: rung-006-async-debouncer-worker-starvation

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-006-async-debouncer-worker-starvation
frontier: scheduler-debouncer-timing
status: queued
order: 6
level: adversarial
workload_file: .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py
seeds: [3470, 3471, 3472]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/issues/724
  - https://github.com/dbos-inc/dbos-transact-py/pull/739
  - target/dbos/_debouncer.py
  - target/dbos/_dbos_config.py
  - target/dbos/_core.py
  - target/tests/test_queue.py
  - .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-data-and-fixtures/overview.md
gate_results:
  surface_evidence: ready_from_issue_724_pr_739_and_current_sync_debouncer_recv_loop
  recent_issue_pr_flake_check: pr_739_checks_passed_postgres_and_sqlite_python_3_10_through_3_14_no_flaky_failure_used
  oracle_critic: ready_with_unrelated_workflow_liveness_plus_thread_growth_and_debounce_semantics_oracle
  executor_feasibility: default_real_postgres_profile_after_target_refresh_pinned_target_lacks_async_debouncer_workflow
```

#### Source Contract

- Frontier ID: `scheduler-debouncer-timing`.
- Rung ID: `rung-006-async-debouncer-worker-starvation`.
- Protected product promise: active debounce windows preserve trailing-edge
  latest-input semantics without occupying scarce executor worker threads or
  starving unrelated workflows.
- Replay command:
  `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-006-async-debouncer-worker-starvation --case <case-id>`.
- Seed policy: exact seeds `3470`, `3471`, `3472`; every run must persist
  generated keys, debounce periods, max executor thread setting, active thread
  counts, active internal queue rows, unrelated workflow IDs, and result
  timing.
- Invariant oracle: while debounce workflows are actively waiting, unrelated
  workflow handle results, public statuses, thread-count deltas, and the
  debounced latest-value ledger must all match the model within bounded time.

#### Goal

Exercise the issue `#724` failure mode directly. Existing rung 002 submitted
many debounce keys and checked eventual debounced results plus coarse thread
growth, but it did not make unrelated workflow liveness under a constrained
executor thread pool the primary oracle. PR `#739` changes the internal
debouncer workflow from synchronous `DBOS.recv(...)` to async
`DBOS.recv_async(...)`; this rung should become executable after refreshing the
target past PR `#739` and should be treated as stale/not executable as a
regression-proof rung against the current pinned checkout.

#### Workload Shape

- Type: Postgres stateful timing/liveness workload.
- Build profile: `default` with real Postgres through
  `.workers/run-with-postgres.sh`.
- Runtime setup:
  - Launch an isolated DBOS app with `runtimeConfig.max_executor_threads = 4`
    or an equivalent `DBOSConfig` max-executor-thread setting.
  - Use long debounce periods so internal debouncer workflows remain active
    while unrelated workflows are submitted.
  - Record `threading.active_count()`, `workflow_status` rows on
    `_dbos_internal_queue`, and public handle timings before releasing debounce
    windows.
- Existing coverage comparison:
  - Rung 002 checks many-key completion and row cleanup, but its 50-key case
    uses short windows and a high thread-growth cap; it can pass without proving
    that non-debounce work progresses while keys are waiting.
  - PR `#739` has no product test file changes, so the worker-starvation
    contract is not represented by a narrow target test.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 3470 | long-debounce-keys-plus-direct-workflows | submit 8 distinct debounce keys with 8-12s windows under `max_executor_threads=4`, then immediately start 4 unrelated workflows | unrelated workflows finish within the short liveness bound while debouncer rows remain active; debounced latest values still fire later |
| case-002 | 3471 | hot-key-ack-plus-unrelated-queue | spam one hot key with repeated updates and two cold keys, then enqueue unrelated queue work with `worker_concurrency=1` | duplicate-key ack/get-event path remains bounded, unrelated queue work is not starved, and hot/cold latest values match the model |
| case-003 | 3472 | client-debouncer-pressure | use `DebouncerClient` for multiple long-window keys while runtime workflows run direct DBOS work | client-facing handles return the surviving workflow IDs, unrelated work completes before debounce timeout, and no stale internal debouncer rows remain after idle |

#### Invariants

- Must hold:
  - Active debouncer rows are present when unrelated workflows are submitted,
    proving the workload reached the pressure window.
  - Unrelated direct and queued workflows complete within the modeled liveness
    bound without waiting for debounce windows to expire.
  - Thread-count growth stays within the configured executor-thread budget plus
    explicitly recorded DBOS background threads; it must not grow roughly one
    active worker per debounce key.
  - After debounce windows settle, each modeled key produces exactly one target
    effect with the latest modeled value and every returned debounced handle
    resolves to the surviving workflow.
  - Internal debouncer rows are terminal or absent after a bounded idle period.
- Must never happen:
  - A workload claims success after unrelated workflows are submitted only
    after debounce windows expire.
  - The executor treats the current pinned target's synchronous debouncer as a
    stale-drift regression-proof pass; PR `#739` must be present for a green
    regression-proof run.

#### Expected Signatures

- Success: pressure window is observed, unrelated workflows finish inside the
  short bound while debouncer rows are still active, latest-input semantics
  hold, thread growth is bounded, and no stale internal rows remain.
- Finding: unrelated workflows starve until debounce timeout, active thread
  count grows with debounce-key count beyond the modeled cap, hot-key ack hangs,
  latest debounced value is lost, returned handles disagree, or internal
  debouncer rows remain active after idle.
- Setup block: the executor cannot set `max_executor_threads`, cannot observe
  active internal queue rows, or cannot keep the debounce pressure window open
  without unbounded sleeps.
- Low signal: workload only reruns rung 002 many-key completion, only checks
  thread count after all debounce windows expire, or does not submit unrelated
  work during active debounce waits.

#### Stale Conditions

Mark stale if DBOS replaces the debouncer with true `DELAYED` workflow rows,
changes internal queue semantics for debouncer workflows, changes
`max_executor_threads` configuration, or target ref advances past PR `#739` and
this rung should be reframed from stale-drift discovery to regression-proof.

### Rung: rung-000-timing-smoke

Evidence source: `evidence-key:frontiers/scheduler-debouncer-timing/rungs/rung-000-timing-smoke.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-timing-smoke
frontier: scheduler-debouncer-timing
status: ready
order: 0
level: baseline
workload_file: .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py
seeds:
  - 3400
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 000 Timing Smoke

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072708734371000Z.prompt.md`.
- Frontier ID: `scheduler-debouncer-timing`.
- Rung ID: `rung-000-timing-smoke`.
- Protected product promise: preserve the concrete `scheduler-debouncer-timing` promise from `frontier.md` and `strategy/candidates/scheduler-debouncer-timing.md`.
- Replay command: `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-000-timing-smoke --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree.

##### Goal

- Build and run: prove durable sleep, schedule, and debouncer smoke without policy assertions.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `scheduler-debouncer-timing` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: product-native setup proof.
- Entry points: durable sleep, scheduled workflows, debouncer keys, delayed rows, returned handles, queue/workflow status rows, max-wait windows, and worker pressure artifacts.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | setup | durable sleep/schedule/debouncer primitives run under Postgres | run one sleep, one scheduled item, one debounced item | all primitives reach terminal smoke state | setup smoke oracle |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3400 | run-one-sleep-one-scheduled-item-one-debounced-i | none unless case says setup block | durable sleep/schedule/debouncer primitives run under Postgres | setup smoke oracle |


##### Invariants

- Must hold: every debounced input is modeled by key, value, submission time, max-wait, and expected surviving handle.
- Must hold: latest modeled value wins per key and earlier superseded handles do not produce terminal effects.
- Must hold: delayed rows are created, updated, executed, or removed exactly as the debounce model predicts.
- Must hold: many-key pressure remains bounded by the configured worker/thread/poller limits.
- Must never happen: scheduler overlap observation is reported as a product failure before policy is settled.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/scheduler-debouncer-timing.md`
  - `evidence-key:frontiers/scheduler-debouncer-timing/frontier.md`
- Suggested command family:
  - `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-000-timing-smoke --case case-001`
  - `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-000-timing-smoke --all-cases --sequential`
- Setup assumptions:
  - Use the harness repository for generated workload code.
  - Do not modify `/Users/viswa/code/workers/dbos-transact-py`.
  - Use real Postgres when the area promise depends on durable DBOS state.
  - Record exact dependency/setup blockers instead of weakening the oracle.
- Per-case evidence to record:
  - seed, derived case JSON, DBOS workflow IDs, public API calls, expected model state, observed results, terminal rows/statuses, product commit, and redacted DB connection details.
- Replay notes:
  - Seed alone is not enough when timing/order is calibrated; persist the derived operation schedule.

##### Expected Signatures

- Success: every matrix case reaches its target window and all invariants pass with replay artifacts.
- Finding: any model/result disagreement, duplicate side effect, illegal state transition, stale durable state, lost event/result, unexpected terminal state, or bounded liveness failure.
- Setup block: dependency bootstrap, DB isolation, optional service provisioning, or target-window calibration prevents execution under the allowed scope.
- Low signal: the runner only wraps existing tests, checks command completion, or never reaches the mapped failure mechanism.
- Goal drift: the runner changes area, product promise, oracle, or broadens into a general DBOS suite.

##### Stop Conditions

- Stop when: all matrix cases pass with artifacts, one strong finding is captured, or a setup/window blocker is documented.
- Escalate when: reaching the target requires product source edits, existing workload code, unbounded compute, or a different oracle.

### Rung: rung-001-debouncer-delayed-row

Evidence source: `evidence-key:frontiers/scheduler-debouncer-timing/rungs/rung-001-debouncer-delayed-row.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-debouncer-delayed-row
frontier: scheduler-debouncer-timing
status: selected
order: 1
level: adversarial
workload_file: .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py
seeds:
  - 3410
  - 3411
  - 3412
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 001 Debouncer Delayed Row

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072708734371000Z.prompt.md`.
- Frontier ID: `scheduler-debouncer-timing`.
- Rung ID: `rung-001-debouncer-delayed-row`.
- Protected product promise: preserve the concrete `scheduler-debouncer-timing` promise from `frontier.md` and `strategy/candidates/scheduler-debouncer-timing.md`.
- Replay command: `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-001-debouncer-delayed-row --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree.

##### Goal

- Build and run: latest-input, max-wait, delayed-row, and handle conservation for debouncer.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `scheduler-debouncer-timing` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: durable sleep, scheduled workflows, debouncer keys, delayed rows, returned handles, queue/workflow status rows, max-wait windows, and worker pressure artifacts.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | timing/order | latest input wins inside debounce window | submit key A values v1/v2/v3 inside window | only v3 terminal effect remains | latest-value model agrees |
| case-002 | boundary timing | max-wait fires despite continuing updates | submit repeated values until max-wait boundary | one execution at bounded max-wait with latest value | max-wait and handle oracle |
| case-003 | handle conservation | returned handles map to superseded or winning execution consistently | capture each returned handle then await after window | only winning handle has terminal modeled result | handle/status model agrees |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3410 | submit-key-a-values-v1-v2-v3-inside-window | none unless case says setup block | latest input wins inside debounce window | latest-value model agrees |
| case-002 | 3411 | submit-repeated-values-until-max-wait-boundary | none unless case says setup block | max-wait fires despite continuing updates | max-wait and handle oracle |
| case-003 | 3412 | capture-each-returned-handle-then-await-after-wi | none unless case says setup block | returned handles map to superseded or winning execution consistently | handle/status model agrees |


##### Invariants

- Must hold: every debounced input is modeled by key, value, submission time, max-wait, and expected surviving handle.
- Must hold: latest modeled value wins per key and earlier superseded handles do not produce terminal effects.
- Must hold: delayed rows are created, updated, executed, or removed exactly as the debounce model predicts.
- Must hold: many-key pressure remains bounded by the configured worker/thread/poller limits.
- Must never happen: scheduler overlap observation is reported as a product failure before policy is settled.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/scheduler-debouncer-timing.md`
  - `evidence-key:frontiers/scheduler-debouncer-timing/frontier.md`
- Suggested command family:
  - `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-001-debouncer-delayed-row --case case-001`
  - `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-001-debouncer-delayed-row --all-cases --sequential`
- Setup assumptions:
  - Use the harness repository for generated workload code.
  - Do not modify `/Users/viswa/code/workers/dbos-transact-py`.
  - Use real Postgres when the area promise depends on durable DBOS state.
  - Record exact dependency/setup blockers instead of weakening the oracle.
- Per-case evidence to record:
  - seed, derived case JSON, DBOS workflow IDs, public API calls, expected model state, observed results, terminal rows/statuses, product commit, and redacted DB connection details.
- Replay notes:
  - Seed alone is not enough when timing/order is calibrated; persist the derived operation schedule.

##### Expected Signatures

- Success: every matrix case reaches its target window and all invariants pass with replay artifacts.
- Finding: any model/result disagreement, duplicate side effect, illegal state transition, stale durable state, lost event/result, unexpected terminal state, or bounded liveness failure.
- Setup block: dependency bootstrap, DB isolation, optional service provisioning, or target-window calibration prevents execution under the allowed scope.
- Low signal: the runner only wraps existing tests, checks command completion, or never reaches the mapped failure mechanism.
- Goal drift: the runner changes area, product promise, oracle, or broadens into a general DBOS suite.

##### Stop Conditions

- Stop when: all matrix cases pass with artifacts, one strong finding is captured, or a setup/window blocker is documented.
- Escalate when: reaching the target requires product source edits, existing workload code, unbounded compute, or a different oracle.

### Rung: rung-002-many-key-worker-pressure

Evidence source: `evidence-key:frontiers/scheduler-debouncer-timing/rungs/rung-002-many-key-worker-pressure.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-002-many-key-worker-pressure
frontier: scheduler-debouncer-timing
status: ready
order: 2
level: adversarial
workload_file: .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py
seeds:
  - 3420
  - 3421
  - 3422
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 002 Many Key Worker Pressure

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072708734371000Z.prompt.md`.
- Frontier ID: `scheduler-debouncer-timing`.
- Rung ID: `rung-002-many-key-worker-pressure`.
- Protected product promise: preserve the concrete `scheduler-debouncer-timing` promise from `frontier.md` and `strategy/candidates/scheduler-debouncer-timing.md`.
- Replay command: `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-002-many-key-worker-pressure --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree.

##### Goal

- Build and run: many debounce keys and delayed rows without unbounded worker/thread pressure.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `scheduler-debouncer-timing` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: durable sleep, scheduled workflows, debouncer keys, delayed rows, returned handles, queue/workflow status rows, max-wait windows, and worker pressure artifacts.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | scale pressure | many keys do not create unbounded workers | submit bounded 50-key matrix | active workers/pending rows remain within cap | worker pressure artifact |
| case-002 | isolation | one hot key does not starve cold keys | spam key A while keys B/C wait | B/C execute inside bounded window | per-key liveness model |
| case-003 | delayed row cleanup | superseded delayed rows are cleaned or reused safely | submit replacement storm then idle | no orphan delayed rows remain for modeled keys | delayed-row ledger |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3420 | submit-bounded-50-key-matrix | none unless case says setup block | many keys do not create unbounded workers | worker pressure artifact |
| case-002 | 3421 | spam-key-a-while-keys-b-c-wait | none unless case says setup block | one hot key does not starve cold keys | per-key liveness model |
| case-003 | 3422 | submit-replacement-storm-then-idle | none unless case says setup block | superseded delayed rows are cleaned or reused safely | delayed-row ledger |


##### Invariants

- Must hold: every debounced input is modeled by key, value, submission time, max-wait, and expected surviving handle.
- Must hold: latest modeled value wins per key and earlier superseded handles do not produce terminal effects.
- Must hold: delayed rows are created, updated, executed, or removed exactly as the debounce model predicts.
- Must hold: many-key pressure remains bounded by the configured worker/thread/poller limits.
- Must never happen: scheduler overlap observation is reported as a product failure before policy is settled.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/scheduler-debouncer-timing.md`
  - `evidence-key:frontiers/scheduler-debouncer-timing/frontier.md`
- Suggested command family:
  - `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-002-many-key-worker-pressure --case case-001`
  - `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-002-many-key-worker-pressure --all-cases --sequential`
- Setup assumptions:
  - Use the harness repository for generated workload code.
  - Do not modify `/Users/viswa/code/workers/dbos-transact-py`.
  - Use real Postgres when the area promise depends on durable DBOS state.
  - Record exact dependency/setup blockers instead of weakening the oracle.
- Per-case evidence to record:
  - seed, derived case JSON, DBOS workflow IDs, public API calls, expected model state, observed results, terminal rows/statuses, product commit, and redacted DB connection details.
- Replay notes:
  - Seed alone is not enough when timing/order is calibrated; persist the derived operation schedule.

##### Expected Signatures

- Success: every matrix case reaches its target window and all invariants pass with replay artifacts.
- Finding: any model/result disagreement, duplicate side effect, illegal state transition, stale durable state, lost event/result, unexpected terminal state, or bounded liveness failure.
- Setup block: dependency bootstrap, DB isolation, optional service provisioning, or target-window calibration prevents execution under the allowed scope.
- Low signal: the runner only wraps existing tests, checks command completion, or never reaches the mapped failure mechanism.
- Goal drift: the runner changes area, product promise, oracle, or broadens into a general DBOS suite.

##### Stop Conditions

- Stop when: all matrix cases pass with artifacts, one strong finding is captured, or a setup/window blocker is documented.
- Escalate when: reaching the target requires product source edits, existing workload code, unbounded compute, or a different oracle.

### Rung: rung-003-schedule-overlap-observation

Evidence source: `evidence-key:frontiers/scheduler-debouncer-timing/rungs/rung-003-schedule-overlap-observation.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-003-schedule-overlap-observation
frontier: scheduler-debouncer-timing
status: ready
order: 3
level: basic
workload_file: .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py
seeds:
  - 3430
  - 3431
  - 3432
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 003 Schedule Overlap Observation

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072708734371000Z.prompt.md`.
- Frontier ID: `scheduler-debouncer-timing`.
- Rung ID: `rung-003-schedule-overlap-observation`.
- Protected product promise: preserve the concrete `scheduler-debouncer-timing` promise from `frontier.md` and `strategy/candidates/scheduler-debouncer-timing.md`.
- Replay command: `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-003-schedule-overlap-observation --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree.

##### Goal

- Build and run: observe scheduled overlap behavior without asserting unresolved product policy.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `scheduler-debouncer-timing` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: durable sleep, scheduled workflows, debouncer keys, delayed rows, returned handles, queue/workflow status rows, max-wait windows, and worker pressure artifacts.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | observational | scheduled overlap policy is recorded without asserting failure | run long schedule body with next tick due | artifact records overlap/skip/queue behavior | observation artifact only |
| case-002 | observational | catch-up behavior is visible after pause | pause worker then resume schedule | artifact records missed/catch-up ticks | no product-failure assertion |
| case-003 | observational | status rows describe overlapping schedule attempts | query rows during long schedule body | status/timestamp rows captured | policy evidence only |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3430 | run-long-schedule-body-with-next-tick-due | none unless case says setup block | scheduled overlap policy is recorded without asserting failure | observation artifact only |
| case-002 | 3431 | pause-worker-then-resume-schedule | none unless case says setup block | catch-up behavior is visible after pause | no product-failure assertion |
| case-003 | 3432 | query-rows-during-long-schedule-body | none unless case says setup block | status rows describe overlapping schedule attempts | policy evidence only |


##### Invariants

- Must hold: every debounced input is modeled by key, value, submission time, max-wait, and expected surviving handle.
- Must hold: latest modeled value wins per key and earlier superseded handles do not produce terminal effects.
- Must hold: delayed rows are created, updated, executed, or removed exactly as the debounce model predicts.
- Must hold: many-key pressure remains bounded by the configured worker/thread/poller limits.
- Must never happen: scheduler overlap observation is reported as a product failure before policy is settled.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/scheduler-debouncer-timing.md`
  - `evidence-key:frontiers/scheduler-debouncer-timing/frontier.md`
- Suggested command family:
  - `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-003-schedule-overlap-observation --case case-001`
  - `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-003-schedule-overlap-observation --all-cases --sequential`
- Setup assumptions:
  - Use the harness repository for generated workload code.
  - Do not modify `/Users/viswa/code/workers/dbos-transact-py`.
  - Use real Postgres when the area promise depends on durable DBOS state.
  - Record exact dependency/setup blockers instead of weakening the oracle.
- Per-case evidence to record:
  - seed, derived case JSON, DBOS workflow IDs, public API calls, expected model state, observed results, terminal rows/statuses, product commit, and redacted DB connection details.
- Replay notes:
  - Seed alone is not enough when timing/order is calibrated; persist the derived operation schedule.

##### Expected Signatures

- Success: every matrix case reaches its target window and all invariants pass with replay artifacts.
- Finding: any model/result disagreement, duplicate side effect, illegal state transition, stale durable state, lost event/result, unexpected terminal state, or bounded liveness failure.
- Setup block: dependency bootstrap, DB isolation, optional service provisioning, or target-window calibration prevents execution under the allowed scope.
- Low signal: the runner only wraps existing tests, checks command completion, or never reaches the mapped failure mechanism.
- Goal drift: the runner changes area, product promise, oracle, or broadens into a general DBOS suite.

##### Stop Conditions

- Stop when: all matrix cases pass with artifacts, one strong finding is captured, or a setup/window blocker is documented.
- Escalate when: reaching the target requires product source edits, existing workload code, unbounded compute, or a different oracle.

### Rung: rung-004-bounded-seed-sweep

Evidence source: `evidence-key:frontiers/scheduler-debouncer-timing/rungs/rung-004-bounded-seed-sweep.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-004-bounded-seed-sweep
frontier: scheduler-debouncer-timing
status: ready
order: 4
level: sweep
workload_file: .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py
seeds:
  - 3440
  - 3441
  - 3442
  - 3443
  - 3444
  - 3445
  - 3446
  - 3447
  - 3448
  - 3449
  - 3450
  - 3451
  - 3452
  - 3453
  - 3454
  - 3455
  - 3456
  - 3457
  - 3458
  - 3459
  - 3460
  - 3461
  - 3462
  - 3463
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 004 Bounded Seed Sweep

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072708734371000Z.prompt.md`.
- Frontier ID: `scheduler-debouncer-timing`.
- Rung ID: `rung-004-bounded-seed-sweep`.
- Protected product promise: preserve the concrete `scheduler-debouncer-timing` promise from `frontier.md` and `strategy/candidates/scheduler-debouncer-timing.md`.
- Replay command: `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-004-bounded-seed-sweep --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree.

##### Goal

- Build and run: rare-bug search across debounce timing, max-wait, and delayed-row update windows.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `scheduler-debouncer-timing` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: durable sleep, scheduled workflows, debouncer keys, delayed rows, returned handles, queue/workflow status rows, max-wait windows, and worker pressure artifacts.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | bounded sweep | latest-value preserves the frontier oracle | generate bounded latest-value variant from seed | case reaches latest-value evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-002 | bounded sweep | max-wait preserves the frontier oracle | generate bounded max-wait variant from seed | case reaches max-wait evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-003 | bounded sweep | many-key-pressure preserves the frontier oracle | generate bounded many-key-pressure variant from seed | case reaches many-key-pressure evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-004 | bounded sweep | hot-key-isolation preserves the frontier oracle | generate bounded hot-key-isolation variant from seed | case reaches hot-key-isolation evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-005 | bounded sweep | delayed-row-cleanup preserves the frontier oracle | generate bounded delayed-row-cleanup variant from seed | case reaches delayed-row-cleanup evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-006 | bounded sweep | schedule-observation preserves the frontier oracle | generate bounded schedule-observation variant from seed | case reaches schedule-observation evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-007 | bounded sweep | latest-value preserves the frontier oracle | generate bounded latest-value variant from seed | case reaches latest-value evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-008 | bounded sweep | max-wait preserves the frontier oracle | generate bounded max-wait variant from seed | case reaches max-wait evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-009 | bounded sweep | many-key-pressure preserves the frontier oracle | generate bounded many-key-pressure variant from seed | case reaches many-key-pressure evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-010 | bounded sweep | hot-key-isolation preserves the frontier oracle | generate bounded hot-key-isolation variant from seed | case reaches hot-key-isolation evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-011 | bounded sweep | delayed-row-cleanup preserves the frontier oracle | generate bounded delayed-row-cleanup variant from seed | case reaches delayed-row-cleanup evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-012 | bounded sweep | schedule-observation preserves the frontier oracle | generate bounded schedule-observation variant from seed | case reaches schedule-observation evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-013 | bounded sweep | latest-value preserves the frontier oracle | generate bounded latest-value variant from seed | case reaches latest-value evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-014 | bounded sweep | max-wait preserves the frontier oracle | generate bounded max-wait variant from seed | case reaches max-wait evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-015 | bounded sweep | many-key-pressure preserves the frontier oracle | generate bounded many-key-pressure variant from seed | case reaches many-key-pressure evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-016 | bounded sweep | hot-key-isolation preserves the frontier oracle | generate bounded hot-key-isolation variant from seed | case reaches hot-key-isolation evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-017 | bounded sweep | delayed-row-cleanup preserves the frontier oracle | generate bounded delayed-row-cleanup variant from seed | case reaches delayed-row-cleanup evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-018 | bounded sweep | schedule-observation preserves the frontier oracle | generate bounded schedule-observation variant from seed | case reaches schedule-observation evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-019 | bounded sweep | latest-value preserves the frontier oracle | generate bounded latest-value variant from seed | case reaches latest-value evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-020 | bounded sweep | max-wait preserves the frontier oracle | generate bounded max-wait variant from seed | case reaches max-wait evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-021 | bounded sweep | many-key-pressure preserves the frontier oracle | generate bounded many-key-pressure variant from seed | case reaches many-key-pressure evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-022 | bounded sweep | hot-key-isolation preserves the frontier oracle | generate bounded hot-key-isolation variant from seed | case reaches hot-key-isolation evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-023 | bounded sweep | delayed-row-cleanup preserves the frontier oracle | generate bounded delayed-row-cleanup variant from seed | case reaches delayed-row-cleanup evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |
| case-024 | bounded sweep | schedule-observation preserves the frontier oracle | generate bounded schedule-observation variant from seed | case reaches schedule-observation evidence point | debounce window model, latest input, returned handle, max-wait, delayed-row lifecycle, and bounded worker pressure agree |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3440 | generate-bounded-latest-value-variant-from-seed | none unless case says setup block | latest-value preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-002 | 3441 | generate-bounded-max-wait-variant-from-seed | none unless case says setup block | max-wait preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-003 | 3442 | generate-bounded-many-key-pressure-variant-from- | none unless case says setup block | many-key-pressure preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-004 | 3443 | generate-bounded-hot-key-isolation-variant-from- | none unless case says setup block | hot-key-isolation preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-005 | 3444 | generate-bounded-delayed-row-cleanup-variant-fro | none unless case says setup block | delayed-row-cleanup preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-006 | 3445 | generate-bounded-schedule-observation-variant-fr | none unless case says setup block | schedule-observation preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-007 | 3446 | generate-bounded-latest-value-variant-from-seed | none unless case says setup block | latest-value preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-008 | 3447 | generate-bounded-max-wait-variant-from-seed | none unless case says setup block | max-wait preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-009 | 3448 | generate-bounded-many-key-pressure-variant-from- | none unless case says setup block | many-key-pressure preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-010 | 3449 | generate-bounded-hot-key-isolation-variant-from- | none unless case says setup block | hot-key-isolation preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-011 | 3450 | generate-bounded-delayed-row-cleanup-variant-fro | none unless case says setup block | delayed-row-cleanup preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-012 | 3451 | generate-bounded-schedule-observation-variant-fr | none unless case says setup block | schedule-observation preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-013 | 3452 | generate-bounded-latest-value-variant-from-seed | none unless case says setup block | latest-value preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-014 | 3453 | generate-bounded-max-wait-variant-from-seed | none unless case says setup block | max-wait preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-015 | 3454 | generate-bounded-many-key-pressure-variant-from- | none unless case says setup block | many-key-pressure preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-016 | 3455 | generate-bounded-hot-key-isolation-variant-from- | none unless case says setup block | hot-key-isolation preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-017 | 3456 | generate-bounded-delayed-row-cleanup-variant-fro | none unless case says setup block | delayed-row-cleanup preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-018 | 3457 | generate-bounded-schedule-observation-variant-fr | none unless case says setup block | schedule-observation preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-019 | 3458 | generate-bounded-latest-value-variant-from-seed | none unless case says setup block | latest-value preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-020 | 3459 | generate-bounded-max-wait-variant-from-seed | none unless case says setup block | max-wait preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-021 | 3460 | generate-bounded-many-key-pressure-variant-from- | none unless case says setup block | many-key-pressure preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-022 | 3461 | generate-bounded-hot-key-isolation-variant-from- | none unless case says setup block | hot-key-isolation preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-023 | 3462 | generate-bounded-delayed-row-cleanup-variant-fro | none unless case says setup block | delayed-row-cleanup preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |
| case-024 | 3463 | generate-bounded-schedule-observation-variant-fr | none unless case says setup block | schedule-observation preserves the frontier oracle | debounce window model, latest input, returned handle, max-wait, delayed-row life |


##### Invariants

- Must hold: every debounced input is modeled by key, value, submission time, max-wait, and expected surviving handle.
- Must hold: latest modeled value wins per key and earlier superseded handles do not produce terminal effects.
- Must hold: delayed rows are created, updated, executed, or removed exactly as the debounce model predicts.
- Must hold: many-key pressure remains bounded by the configured worker/thread/poller limits.
- Must never happen: scheduler overlap observation is reported as a product failure before policy is settled.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/scheduler-debouncer-timing.md`
  - `evidence-key:frontiers/scheduler-debouncer-timing/frontier.md`
- Suggested command family:
  - `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-004-bounded-seed-sweep --case case-001`
  - `python .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-004-bounded-seed-sweep --all-cases --sequential`
- Setup assumptions:
  - Use the harness repository for generated workload code.
  - Do not modify `/Users/viswa/code/workers/dbos-transact-py`.
  - Use real Postgres when the area promise depends on durable DBOS state.
  - Record exact dependency/setup blockers instead of weakening the oracle.
- Per-case evidence to record:
  - seed, derived case JSON, DBOS workflow IDs, public API calls, expected model state, observed results, terminal rows/statuses, product commit, and redacted DB connection details.
- Replay notes:
  - Seed alone is not enough when timing/order is calibrated; persist the derived operation schedule.

##### Expected Signatures

- Success: every matrix case reaches its target window and all invariants pass with replay artifacts.
- Finding: any model/result disagreement, duplicate side effect, illegal state transition, stale durable state, lost event/result, unexpected terminal state, or bounded liveness failure.
- Setup block: dependency bootstrap, DB isolation, optional service provisioning, or target-window calibration prevents execution under the allowed scope.
- Low signal: the runner only wraps existing tests, checks command completion, or never reaches the mapped failure mechanism.
- Goal drift: the runner changes area, product promise, oracle, or broadens into a general DBOS suite.

##### Stop Conditions

- Stop when: all matrix cases pass with artifacts, one strong finding is captured, or a setup/window blocker is documented.
- Escalate when: reaching the target requires product source edits, existing workload code, unbounded compute, or a different oracle.
