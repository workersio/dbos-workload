# Area: schedule-registry-concurrency

## Current State

Current status: new area opened from upstream PR `#741`; one executor-ready
rung is queued in work item `E-018`.

Recent issue/PR scan used for this area:

- PR `#741`: "Fix Concurrent Schedule Creation" fixed concurrent
  `applySchedules` conflicts after the current target evidence ref by replacing
  the delete-then-create sequence with an idempotent upsert on
  `schedule_name`.
- The fix also made re-applying an existing schedule assign a fresh
  `schedule_id`, while preserving `status` and `last_fired_at`, so the dynamic
  scheduler stops the old thread and starts a new one with updated fields.
- Existing scheduler work covers trigger/backfill/live queue routing,
  debouncer timing, scheduled workflow identity query behavior, and rotten
  schedule-context introspection. It does not cover concurrent schedule
  registry mutation or live-thread replacement after public schedule reapply.

## Product Promise

Public schedule application is atomic, idempotent, and live-update safe under
concurrent callers. Re-applying the same `schedule_name` must leave exactly one
durable schedule row, preserve operator-controlled state such as pause status
and `last_fired_at` where the API contract requires it, replace mutable fields
with the latest intended definition, restart the live scheduler thread when the
definition changes, and avoid duplicate live executions or stale contexts.

## Why This Matters

Deploys, startup hooks, or multiple workers can call `DBOS.apply_schedules()`
for the same logical schedule at nearly the same time. A delete-then-create
implementation can raise unique-constraint failures, temporarily remove a
schedule, or leave a live scheduler thread using stale context. Operators then
see flaky startup, missing scheduled work, duplicate live ticks, or updates that
only take effect after process restart.

## Evidence

- Code:
  - `target/dbos/_dbos.py`: `DBOS.apply_schedules()` builds
    `WorkflowSchedule` rows and currently deletes by `schedule_name` before
    creating the replacement row.
  - `target/dbos/_client.py`: `DBOSClient.apply_schedules()` has the same
    delete-then-create pattern.
  - `target/dbos/_sys_db.py`: `create_schedule()` raises if the unique
    `schedule_name` row already exists; `list_schedules()` and
    `get_schedule()` expose schedule id, context, status, last-fire state,
    timezone, and queue name for oracle checks.
  - `target/dbos/_scheduler.py`: `dynamic_scheduler_loop()` keys active
    schedule threads by `schedule_id`, so a reapply that keeps the old id can
    leave the old thread running with stale context.
- Tests:
  - PR `#741` added `test_apply_schedules_concurrent`, asserting concurrent
    public apply calls never raise and leave exactly one schedule.
  - PR `#741` added `test_apply_schedules_live_update`, asserting a running
    scheduler picks up changed context after reapply.
- Existing workloads/runs:
  - `scheduler-debouncer-timing` / `E-001` validates schedule-origin queue
    routing, live/backfill/trigger slots, and queue controls, not concurrent
    registry mutation.
  - `workflow-attributes-query` / `E-009` validates schedule identity query
    filtering and import/export visibility, not concurrent upsert semantics.
  - `control-plane-state-introspection` / `E-013` validates listing schedules
    with rotten context, not create/update races.
- Recent churn:
  - PR `#741` merged at `2026-06-24T16:45:12Z` with merge commit
    `e49d631cf3124b2b2a5722759c4d92c786cf731c`, after the current target
    evidence ref.

## What Not To Repeat

- Do not only reproduce PR `#741`'s product tests; the workload must compose
  concurrent public apply, persisted schedule row inspection, pause/last-fire
  preservation, live scheduler update, and stale-thread/duplicate-tick checks.
- Do not treat a final row count of one as sufficient; a workload can miss
  transient failures, stale live context, or duplicate scheduled effects if it
  only checks `list_schedules()` at the end.
- Do not fold this into scheduler queue-control work unless the oracle is about
  schedule registry mutation. Queue routing and rate limits are adjacent but
  not the protected promise here.
- Do not use SQLite as the product signal. The conflict model depends on
  concurrent Postgres schedule-row writes.

## Adversarial Model

The area attacks the assumption that applying a schedule is a single logical
operation even when several workers or clients publish the same schedule
definition concurrently. The adversarial states are simultaneous
`DBOS.apply_schedules()` and `DBOSClient.apply_schedules()` calls for the same
`schedule_name`, followed by a changed definition while the live scheduler has
already started a thread for the old definition.

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
      "read-only:target/tests/test_scheduler.py",
      "2 upstream tests",
      "read-only evidence for PR #741 concurrent apply and live update fixes",
    ]
  - [
      "rung-001-concurrent-apply-live-update",
      "inline:rung-001-concurrent-apply-live-update",
      "ready",
      "1",
      "stateful-concurrency",
      ".workers/workloads/schedule-registry-concurrency/schedule_registry_concurrency_workload.py",
      "4 cases",
      "concurrent apply_schedules calls and live reapply must leave one current schedule, preserve required state, restart live threads, and avoid duplicate stale-context ticks",
    ]
```

## Rung Details

### Rung: rung-000-product-regression-baseline

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-product-regression-baseline
frontier: schedule-registry-concurrency
status: not_run_optional
order: 0
level: baseline
workload_file: read-only:target/tests/test_scheduler.py
seeds: [0]
updated_at: 2026-06-24T00:00:00Z
```

#### Source Contract

- Frontier ID: `schedule-registry-concurrency`.
- Rung ID: `rung-000-product-regression-baseline`.
- Protected product promise: the narrow PR `#741` regression tests pass on the
  evidence ref that includes the upstream fix.
- Replay command: optional read-only product pytest selection:
  `pytest target/tests/test_scheduler.py -k 'apply_schedules_concurrent or apply_schedules_live_update'`.
- Seed policy: fixed seed `0`.
- Invariant oracle: the selected product tests pass at an explicitly refreshed
  target ref containing PR `#741`.

### Rung: rung-001-concurrent-apply-live-update

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-concurrent-apply-live-update
frontier: schedule-registry-concurrency
status: ready
order: 1
level: stateful-concurrency
workload_file: .workers/workloads/schedule-registry-concurrency/schedule_registry_concurrency_workload.py
seeds: [7410, 7411, 7412, 7413]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - target/dbos/_dbos.py
  - target/dbos/_client.py
  - target/dbos/_sys_db.py
  - target/dbos/_scheduler.py
  - target/tests/test_scheduler.py
  - https://github.com/dbos-inc/dbos-transact-py/pull/741
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
gate_results:
  surface_evidence: ready_from_pr_741_and_target_delete_create_apply_schedules_code
  duplicate_check: existing_scheduler_workloads_do_not_cover_registry_write_concurrency_or_live_thread_replacement
  oracle_critic: ready_with_row_model_context_ledger_thread_replacement_and_bounded_live_tick_invariants
  executor_feasibility: default_postgres_profile_required; no_optional_services
```

#### Source Contract

- Frontier ID: `schedule-registry-concurrency`.
- Rung ID: `rung-001-concurrent-apply-live-update`.
- Protected product promise: public schedule application is atomic,
  idempotent, and live-update safe when multiple callers apply the same
  schedule and when a running schedule definition changes.
- Replay command:
  `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/schedule-registry-concurrency/schedule_registry_concurrency_workload.py --rung rung-001-concurrent-apply-live-update --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `7410`, `7411`, `7412`, `7413`; every run must
  persist caller order, barrier release timing, input definitions, exceptions,
  schedule row snapshots, observed schedule ids, fired context ledger,
  workflow IDs, last-fire/status values, and cleanup outcome.
- Invariant oracle: an independent schedule model plus durable schedule rows,
  public list/get APIs, and a live firing ledger must agree after each modeled
  phase.

#### Goal

Build one workload that stresses public schedule registry writes under
concurrency, then proves live scheduler threads adopt the latest definition
without stale-context ticks after reapply. The workload should expose both
startup-style apply races and operator-style updates to an already active
schedule.

#### Workload File

- Expected path:
  `.workers/workloads/schedule-registry-concurrency/schedule_registry_concurrency_workload.py`.
- Create or reuse: create a new file. Existing schedule workloads do not share
  this registry-write concurrency oracle.
- Why one file is enough: all cases share the same schedule definition model,
  barriered apply harness, schedule-row snapshots, live firing ledger, and
  cleanup semantics.

#### Workload Shape

- Type: API/background-job stateful concurrency workload.
- Build profile: `default`.
- Setup: real Postgres through `.workers/run-with-postgres.sh`; no Kafka or
  optional external services.
- Entry points:
  - `DBOS.apply_schedules()`
  - `DBOSClient.apply_schedules()`
  - `DBOS.list_schedules()`, `DBOS.get_schedule()`
  - `DBOS.pause_schedule()`, `DBOS.resume_schedule()`
  - live dynamic scheduler firing a registered workflow.
- Bounded concurrency:
  - Use a barrier to release callers together.
  - Keep worker count between 4 and 12.
  - Bound live scheduler waiting with explicit retry windows and diagnostic
    thread dumps or event logs on timeout.

#### Parameter Matrix

| Case | Seed | Scenario | Fault model | Primary oracle |
|---|---:|---|---|---|
| `case-001` | 7410 | eight `DBOS.apply_schedules()` callers concurrently apply the same definition | delete-then-create unique conflict / transient missing schedule | no caller exception, exactly one row, expected context/schedule/queue fields |
| `case-002` | 7411 | mixed `DBOS` and `DBOSClient` callers concurrently apply the same definition with the same schedule name | divergent public/client implementation race | no caller exception, one durable row visible through both APIs, one modeled definition |
| `case-003` | 7412 | let an existing schedule record `last_fired_at`, pause it, then concurrently reapply a changed definition | upsert overwrites operator state or fails to replace mutable fields | one row, pause status and last-fire semantics match model, mutable fields match latest definition |
| `case-004` | 7413 | run a live every-second schedule with context version 1, reapply context version 2, then observe subsequent ticks | stale schedule thread keyed by old `schedule_id` keeps firing old context or duplicates live ticks | fresh schedule id after reapply, bounded v2 ticks observed, no v1 ticks after the modeled grace window, no duplicate workflow IDs |

#### Required Artifacts

Each case must write:

- rung/case/seed and target commit;
- public/client caller definitions and barrier timing;
- per-caller outcome, exception class/message, and elapsed time;
- schedule snapshots before apply, after concurrent apply, after reapply, and
  after cleanup;
- expected model state for schedule id, schedule name, cron, context,
  `status`, `last_fired_at`, `cron_timezone`, and `queue_name`;
- fired workflow ledger with scheduled timestamp, workflow id, context version,
  start timestamp, and terminal status;
- cleanup proof that the schedule row was deleted and no modeled live thread
  can keep adding ledger entries after cleanup.

#### Invariants

- Must hold: no public or client apply caller raises a unique-constraint,
  missing-row, serialization, or schedule conflict error.
- Must hold: after each apply phase, `list_schedules()` and `get_schedule()`
  expose exactly one row for the modeled `schedule_name`.
- Must hold: the durable row's mutable fields match the latest modeled
  definition, including cron, context, timezone, queue name, workflow name, and
  workflow class name.
- Must hold: modeled operator state is not accidentally reset; paused status
  remains paused across reapply unless the public contract intentionally says
  apply should reactivate, and `last_fired_at` is not cleared after a live
  schedule has fired.
- Must hold: changed live definitions produce a fresh `schedule_id` so the
  dynamic scheduler replaces the old thread.
- Must hold: after the reapply grace window, fired ledger entries for the
  schedule use only the latest context version.
- Must hold: workflow IDs for live ticks are unique for modeled scheduled
  timestamps, and no duplicate ledger effect appears for the same workflow ID.
- Must never happen: the workload passes because the live schedule never fired,
  because only a final row count was checked, because exceptions from worker
  threads were swallowed, or because cleanup deleted the schedule before the
  stale-thread window was observed.

#### Finding Classification

- Product finding: any caller exception from concurrent apply, more than one
  durable row for a schedule name, stale mutable fields, lost operator state
  under the modeled contract, unchanged `schedule_id` after live definition
  update, stale context ticks after the grace window, duplicate workflow IDs, or
  cleanup failing to quiesce live entries.
- Workload bug: the workload never starts the live scheduler, uses SQLite for
  the conflict case, fails to release callers concurrently, does not surface
  thread exceptions, or treats undefined pause/reactivation policy as a failing
  product invariant without recording the observed contract.
- Low signal: only repeats PR `#741`'s unit tests without mixed public/client
  callers, persisted state snapshots, live firing ledger, or state preservation
  checks.

#### Stale Conditions

Mark stale if target ref advances past PR `#741` and the workload is intended
only to reproduce the pre-fix race; if `apply_schedules()` contract changes
from "apply desired schedule set" to "always reset schedule state"; if schedule
threads stop being keyed by `schedule_id`; if public/client schedule APIs
diverge intentionally; or if DBOS replaces per-process dynamic scheduler
threads with a different live-scheduling model.
