# Area: control-plane-state-introspection

## Current State

Current status: new area opened from recent control-plane deserialization
evidence; one executor-ready rung queued.

Recent issue/PR scan used for this frontier:

- PR `#694`: "Fix list schedules context desez" fixed a schedule-listing path
  where one stale serialized context could make listing schedules fail for all
  schedules.
- The fix added `safe_deserialize_schedule_context(...)` and wired it through
  runtime `DBOS.list_schedules` / `get_schedule`, `DBOSClient` schedule
  listing, and conductor protocol schedule output.
- PR `#694` integration checks passed across Postgres and SQLite for Python
  3.10 through 3.14.
- Existing scheduler frontiers cover timing, queue routing, and scheduler-owned
  workflow identity. They do not cover control-plane query resilience when
  durable app-owned serialized fields become stale after code changes.

## Product Promise

DBOS control-plane introspection APIs remain available and actionable when
durable records contain stale or partially undeserializable application data.
One rotten schedule context must not deny listing, filtering, retrieving,
pausing, resuming, deleting, or inspecting unrelated schedules. Good records
must still deserialize normally, while bad records must be surfaced with enough
raw context and schedule identity for operators to understand and repair them.

## Why This Matters

Schedules are long-lived control-plane records. Their contexts can contain
application classes, default pickled objects, portable JSON values, or data
written by older app versions. Operators need `list_schedules`, `get_schedule`,
and conductor/UI schedule views precisely when application code has changed or
state is stale. If one context fails deserialization and aborts the whole list
operation, operators lose the ability to inspect or delete the schedule that
caused the problem.

## Evidence

- PR:
  - https://github.com/dbos-inc/dbos-transact-py/pull/694
- Code:
  - `target/dbos/_serialization.py`: `safe_deserialize_schedule_context(...)`
    catches context deserialization failures and returns the raw serialized
    string.
  - `target/dbos/_dbos.py`: runtime `list_schedules` and `get_schedule` use the
    safe context path.
  - `target/dbos/_client.py`: `DBOSClient.list_schedules` and `get_schedule`
    use the safe context path.
  - `target/dbos/_conductor/protocol.py`: conductor `ScheduleOutput` uses the
    same safe context path for schedule views.
- Tests:
  - `target/tests/test_scheduler.py::test_list_schedules_undeserializable_context`
    covers one runtime list call with one bad pickled class and one good dict.
  - Existing schedule tests cover filtering, client list/get, async list/get,
    pause/resume/delete, trigger, and backfill, but not with rotten context
    records mixed into the schedule table.

## What Not To Repeat

- Do not repeat only the product test that removes one class and calls
  `DBOS.list_schedules()`.
- Do not treat returning a raw serialized string for a bad context as a bug;
  that is the current explicit fallback contract.
- Do not make this a scheduler timing or overlap oracle. The surface is
  control-plane introspection and operator repair, not schedule execution
  cadence.

## Adversarial Model

The frontier attacks the assumption that control-plane list/get paths can fully
deserialize every app-owned field. It mixes good and bad schedule contexts,
multiple serializers, runtime and client APIs, filters, conductor formatting,
and lifecycle commands after the application code or serializer environment has
changed.

The model records the raw serialized context before making it stale, then
checks that public control-plane APIs return all modeled schedules, preserve
good contexts, expose raw bad contexts, and still allow operator actions on the
affected and unaffected schedules.

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
      "1 existing product test",
      "read-only evidence for one runtime list_schedules rotten-context regression",
    ]
  - [
      "rung-001-rotten-schedule-context-introspection",
      "inline:rung-001-rotten-schedule-context-introspection",
      "queued",
      "1",
      "control-plane",
      ".workers/workloads/control-plane-state-introspection/control_plane_state_introspection_workload.py",
      "3 cases",
      "schedule list/get/control-plane paths must remain available with mixed good and rotten contexts across runtime, client, conductor, filters, lifecycle repair, and serializer variants",
    ]
```

## Rung Details

### Rung: rung-000-product-regression-baseline

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-product-regression-baseline
frontier: control-plane-state-introspection
status: not_run_optional
order: 0
level: baseline
workload_file: read-only:target/tests/test_scheduler.py
seeds: [0]
updated_at: 2026-06-24T00:00:00Z
```

#### Source Contract

- Frontier ID: `control-plane-state-introspection`.
- Rung ID: `rung-000-product-regression-baseline`.
- Protected product promise: one stale schedule context does not make
  `DBOS.list_schedules()` fail.
- Replay command: optional read-only product pytest selection; no generated
  workload code is needed for this baseline.
- Seed policy: fixed seed `0`.
- Invariant oracle: the selected product regression test passes at the target
  evidence ref or the executor's explicitly refreshed DBOS ref.

### Rung: rung-001-rotten-schedule-context-introspection

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-rotten-schedule-context-introspection
frontier: control-plane-state-introspection
status: queued
order: 1
level: control-plane
workload_file: .workers/workloads/control-plane-state-introspection/control_plane_state_introspection_workload.py
seeds: [6940, 6941, 6942]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/pull/694
  - target/dbos/_serialization.py
  - target/dbos/_dbos.py
  - target/dbos/_client.py
  - target/dbos/_conductor/protocol.py
  - target/tests/test_scheduler.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-data-and-fixtures/overview.md
gate_results:
  surface_evidence: ready_from_pr_694_and_target_safe_deserialize_schedule_context
  duplicate_check: existing_scheduler_frontiers_do_not_cover_stale_app_data_in_control_plane_queries
  oracle_critic: ready_with_mixed_record_count_good_context_raw_bad_context_and_lifecycle_repair_oracle
  executor_feasibility: default_profile_realistic_postgres_and_sqlite_both_meaningful
```

#### Source Contract

- Frontier ID: `control-plane-state-introspection`.
- Rung ID: `rung-001-rotten-schedule-context-introspection`.
- Protected product promise: schedule introspection and repair APIs remain
  usable when some schedule contexts cannot be deserialized by the current app
  code or serializer environment.
- Replay command:
  `python .workers/workloads/control-plane-state-introspection/control_plane_state_introspection_workload.py --rung rung-001-rotten-schedule-context-introspection --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `6940`, `6941`, `6942`; every run must persist
  schedule names, workflow names, raw serialized context strings, serializer
  mode, filter inputs, public API observations, conductor output observations,
  lifecycle command results, and final schedule table state.
- Invariant oracle: schedule count, schedule identity, context value class,
  raw bad-context fallback, filter results, client/runtime/conductor parity,
  lifecycle command success, and final cleanup state must agree with the
  independent schedule model.

#### Goal

Build one workload that proves schedule control-plane introspection is robust
under stale app-owned context data. The workload should cover all code paths
that PR `#694` touched, and it should show that operators can still find and
repair the bad schedule without losing visibility into unrelated good
schedules.

#### Workload File

- Expected path:
  `.workers/workloads/control-plane-state-introspection/control_plane_state_introspection_workload.py`.
- Create or reuse: create this file for this frontier; later control-plane
  rungs can reuse it for stale workflow inputs, attributes, schedule identity,
  and conductor formatting.
- Why one file is enough: the cases share schedule creation, stale class
  simulation, serializer setup, public API calls, conductor protocol helpers,
  and cleanup.

#### Workload Shape

- Type: product-runtime control-plane workload with public DBOS and DBOSClient
  APIs plus protocol formatting checks.
- Entry points:
  - `DBOS.apply_schedules`, `DBOS.list_schedules`, `DBOS.get_schedule`
  - `DBOSClient.list_schedules`, `DBOSClient.get_schedule`,
    `pause_schedule`, `resume_schedule`, and `delete_schedule`
  - async list/get schedule APIs when convenient for parity
  - `ScheduleOutput.from_schedule(...)` for conductor formatting, without
    needing a live conductor websocket
  - read-only system schedule table inspection only to anchor raw serialized
    contexts and cleanup
- Fault model: remove or rename the Python class used by a serialized default
  context, mix bad and good contexts in one list result, apply list filters,
  retrieve one bad schedule by name, exercise client/runtime/conductor paths,
  and repair or delete schedules after introspection.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 6940 | runtime-client-mixed-rotten-contexts | one bad pickled class plus multiple good contexts, then class removed | runtime and client list/get return all modeled schedules; good contexts deserialize; bad context is raw serialized string |
| case-002 | 6941 | filtered-conductor-rotten-contexts | rotten context appears under status, workflow, and schedule-name filters; conductor output is formatted with and without context loading | filters do not skip or explode; conductor output includes schedule identity and safe context string or omits context when requested |
| case-003 | 6942 | lifecycle-repair-after-rotten-context | pause/resume/delete/update around a bad-context schedule and unrelated good schedules | operators can repair/delete the bad schedule; unrelated schedules remain visible and executable; final cleanup leaves no schedules |

#### Invariants

- Must hold for introspection:
  - Runtime and client list calls return the modeled schedule names and counts
    even when one or more contexts are rotten.
  - `get_schedule` for a bad schedule returns that schedule with context equal
    to the captured raw serialized string, not an exception or `None`.
  - Good schedule contexts still deserialize to their modeled values.
  - Filters by status, workflow name, and schedule-name prefix behave the same
    with bad contexts present as they do with only good contexts.
  - Conductor protocol formatting does not throw; when `load_context=True`, it
    exposes the safe fallback string for bad contexts, and when
    `load_context=False`, context is omitted as requested.
- Must hold for repair/lifecycle:
  - Pause, resume, delete, and cleanup commands can target the bad schedule
    after introspection.
  - Unrelated good schedules remain visible and keep their contexts before and
    after bad-schedule repair.
  - Final list state matches the modeled cleanup plan.
- Must never happen:
  - A single bad context denies the entire list response.
  - The bad schedule disappears from list/get results without an explicit
    delete.
  - A good context is converted to a raw serialized string because another
    schedule is rotten.

#### Setup And Classification

- Build profile: `default`.
- Backend: Postgres and SQLite are both meaningful because this is schedule
  control-plane behavior over durable serialized context fields.
- Target ref handling: current pinned target contains PR `#694`; if an executor
  refreshes `./target`, record the new DBOS ref in the artifact.
- Expected runtime: bounded seconds per case; avoid waiting for scheduler ticks
  except when the lifecycle repair case explicitly validates that an unrelated
  good schedule can still be triggered.

#### Stale Conditions

Mark this rung stale if DBOS changes schedule context serialization, schedule
list/get output shape, `safe_deserialize_schedule_context`, conductor schedule
protocol formatting, client schedule API semantics, or the supported fallback
contract for undeserializable schedule context.
