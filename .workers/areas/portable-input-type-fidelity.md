# Area: portable-input-type-fidelity

## Current State

Current status: new area opened from recent issue/PR evidence; one
executor-ready rung queued.

Recent issue/PR scan used for this frontier:

- Issue `#697`: portable JSON serialization made scheduled workflow
  `scheduled_at: datetime` values arrive as `str` when the workflow was started
  by cron or restored after failure, while direct in-process calls received a
  real `datetime`.
- PR `#700`: added `coerce_portable_args_to_hints(...)` so portable JSON
  workflow arguments annotated as `datetime` or `date` are parsed from strings
  before workflow execution when no custom argument validator is configured.
- PR `#700` integration checks passed across Postgres and SQLite for Python
  3.10 through 3.14.
- Existing frontiers cover durable error serialization and scheduler timing or
  queue routing. They do not cover successful portable input type restoration
  across scheduler, queued, class/instance method, validation, and replay paths.

## Product Promise

Workflows using portable JSON receive arguments compatible with their Python
type hints or configured validators across scheduled triggers, backfills,
queued rows, recovered rows, class methods, instance methods, and direct
portable row insertion. `datetime` and `date` values are normalized
deterministically; invalid values are rejected or fail with modeled terminal
errors instead of being silently accepted as valid typed inputs.

## Why This Matters

DBOS portable JSON intentionally erases Python-only value types into JSON
strings so that cross-language clients, scheduler rows, and SQL insertion paths
can interoperate. Python workflows still declare concrete signatures such as
`scheduled_at: datetime` and may use timezone arithmetic, `.date()`, or
`.isoformat()` inside the durable workflow body. If DBOS passes raw strings to
those workflows on only some entry paths, users get route-dependent behavior,
failed recoveries, corrupted date logic, or validator drift that is difficult
to diagnose from durable workflow status alone.

## Evidence

- Issue and PR:
  - https://github.com/dbos-inc/dbos-transact-py/issues/697
  - https://github.com/dbos-inc/dbos-transact-py/pull/700
- Code:
  - `target/dbos/_serialization.py`: `DBOSPortableJSONSerializer` serializes
    through JSON, and `coerce_portable_args_to_hints(...)` parses string values
    for `datetime` and `date` hints, including class/instance first-parameter
    alignment.
  - `target/dbos/_core.py`: `execute_workflow_by_id` applies portable hint
    coercion only when portable serialization is in use and no workflow
    `validate_args` callable is configured, then runs validation if present.
  - `target/dbos/_scheduler.py`: schedule trigger and backfill paths create
    portable workflow inputs whose first positional argument is
    `scheduled_at`.
- Tests:
  - `target/tests/test_scheduler.py::test_scheduled_workflow_datetime_with_portable_serializer`
    covers the narrow scheduled workflow datetime regression from PR `#700`.
  - `target/tests/test_serialization.py::test_directinsert_datetime_validation`
    covers pydantic validation for a classmethod direct-insert datetime row.
  - Existing tests do not compose scheduler trigger plus backfill, class and
    instance method parameter alignment, no-validator hint coercion, invalid
    strings, date-vs-datetime distinction, and replay/relaunch parity.

## What Not To Repeat

- Do not repeat only the product test that asserts a scheduled workflow's first
  argument is a `datetime` under `DBOSPortableJSONSerializer`.
- Do not convert every parse failure into a product bug. When no validator is
  configured, unparseable strings may reach workflow code and fail there; the
  oracle should require bounded, modeled terminal behavior rather than silent
  success.
- Do not use this frontier to test durable error metadata. That remains in
  `serialization-error-fidelity`.

## Adversarial Model

The frontier attacks value-type erasure at DBOS entry boundaries. It combines
portable JSON rows with type hints, validator and no-validator workflows,
scheduled trigger/backfill/live paths, class and instance method signatures,
timezone and date-only values, invalid string values, and relaunch/recovery
reads.

The model records an application-visible type ledger from inside the workflow
body and independently checks public workflow status/result paths. Read-only
system state inspection is allowed only to anchor replay/relaunch identity,
input serialization shape, and terminal status parity.

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
      "read-only:target/tests/test_scheduler.py,target/tests/test_serialization.py",
      "2 existing product test families",
      "read-only evidence for scheduled portable datetime coercion and pydantic direct-insert validation",
    ]
  - [
      "rung-001-scheduled-datetime-portable-roundtrip",
      "inline:rung-001-scheduled-datetime-portable-roundtrip",
      "queued",
      "1",
      "contract",
      ".workers/workloads/portable-input-type-fidelity/portable_input_type_fidelity_workload.py",
      "3 cases",
      "portable JSON workflow inputs must preserve datetime/date semantics across scheduler, direct portable rows, class/instance methods, validators, invalid values, and relaunch reads",
    ]
```

## Rung Details

### Rung: rung-000-product-regression-baseline

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-product-regression-baseline
frontier: portable-input-type-fidelity
status: not_run_optional
order: 0
level: baseline
workload_file: read-only:target/tests/test_scheduler.py,target/tests/test_serialization.py
seeds: [0]
updated_at: 2026-06-24T00:00:00Z
```

#### Source Contract

- Frontier ID: `portable-input-type-fidelity`.
- Rung ID: `rung-000-product-regression-baseline`.
- Protected product promise: portable JSON scheduler and direct-insert
  datetime paths match the narrow product tests added around issue `#697`.
- Replay command: optional read-only product pytest selection; no generated
  workload code is needed for this baseline.
- Seed policy: fixed seed `0`.
- Invariant oracle: the selected product tests pass at the target evidence ref
  or the executor's explicitly refreshed DBOS ref.

### Rung: rung-001-scheduled-datetime-portable-roundtrip

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-scheduled-datetime-portable-roundtrip
frontier: portable-input-type-fidelity
status: queued
order: 1
level: contract
workload_file: .workers/workloads/portable-input-type-fidelity/portable_input_type_fidelity_workload.py
seeds: [7000, 7001, 7002]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/issues/697
  - https://github.com/dbos-inc/dbos-transact-py/pull/700
  - target/dbos/_serialization.py
  - target/dbos/_core.py
  - target/dbos/_scheduler.py
  - target/tests/test_scheduler.py
  - target/tests/test_serialization.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-data-and-fixtures/overview.md
gate_results:
  surface_evidence: ready_from_issue_697_and_pr_700
  existing_frontier_check: distinct_from_serialization_error_fidelity_and_scheduler_debouncer_timing
  oracle_critic: ready_with_body_observed_type_ledger_and_terminal_status_parity
  executor_feasibility: default_profile_realistic_postgres_and_sqlite_both_meaningful
```

#### Source Contract

- Frontier ID: `portable-input-type-fidelity`.
- Rung ID: `rung-001-scheduled-datetime-portable-roundtrip`.
- Protected product promise: portable JSON workflow input values that have
  `datetime` or `date` type hints are restored or validated consistently across
  supported DBOS entry paths, including scheduler-origin rows and
  class/instance method signatures.
- Replay command:
  `python .workers/workloads/portable-input-type-fidelity/portable_input_type_fidelity_workload.py --rung rung-001-scheduled-datetime-portable-roundtrip --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `7000`, `7001`, `7002`; every run must persist
  workflow IDs, schedule names, timezone inputs, serialized input snapshots,
  body-observed type names, status/result observations, and relaunch/recovery
  read paths.
- Invariant oracle: workflow body type ledger, public result/status APIs,
  serialized portable input rows, validation errors, and relaunch/recovery
  observations must agree with the per-case model within a bounded timeout.

#### Goal

Build one workload that proves portable JSON type restoration is not limited to
the narrow scheduled workflow regression test. The workload should expose
route-dependent raw string leakage, class/instance parameter misalignment,
date-vs-datetime coercion mistakes, validation ordering drift, and replay
inconsistency.

#### Workload File

- Expected path:
  `.workers/workloads/portable-input-type-fidelity/portable_input_type_fidelity_workload.py`.
- Create or reuse: create this file for this frontier; later rungs can reuse it
  while the oracle remains type-ledger plus public status/result parity.
- Why one file is enough: the cases share DBOS portable serializer setup,
  schedule creation, direct portable row insertion, type-ledger workflows,
  public handle reads, and cleanup.

#### Workload Shape

- Type: product-runtime adversarial workload using public DBOS APIs plus
  limited read-only system DB inspection for serialized input/status evidence.
- Entry points:
  - `DBOS.configure(..., serializer=DBOSPortableJSONSerializer())`
  - `DBOSClient.create_schedule`, `trigger_schedule`, and `backfill_schedule`
  - `DBOS.workflow(..., serialization_type=WorkflowSerializationFormat.PORTABLE)`
  - `validate_args=pydantic_args_validator` and no-validator workflows
  - direct insertion of portable JSON `workflow_status.inputs` rows only for
    foreign-client/direct-row cases
  - runtime handle, retrieved handle, client handle, and relaunch read paths
- Fault model: timezone offsets, date-only values, naive/aware datetime
  strings, invalid strings, booleans/integers where a date is expected,
  classmethod and instance method first-parameter alignment, and relaunch after
  terminal workflow status is stored.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 7000 | scheduled-trigger-backfill-datetime | trigger and backfill a portable scheduled workflow with aware/UTC boundaries and nested context | scheduler-origin rows deliver `scheduled_at` as `datetime`, preserve context, and produce identical public results after relaunch |
| case-002 | 7001 | class-instance-date-param-alignment | classmethod and instance-method workflows receive stringified `datetime` and `date` values through portable rows | leading `cls`/`self` is skipped correctly, `datetime` remains `datetime`, `date` remains `date`, and no raw string reaches the body for hinted valid values |
| case-003 | 7002 | validation-boundaries-invalid-values | compare no-validator hint coercion with pydantic validation for valid ISO values, invalid strings, bools, and ints | valid values are typed, invalid values fail or validate according to the modeled path, and no invalid value is silently accepted as a valid typed date/datetime |

#### Invariants

- Must hold for valid hinted values:
  - Body-observed type names match the signature hints: `datetime` for
    `datetime`, `date` for `date`, and original JSON-compatible types for
    unrelated context.
  - `datetime` values preserve the modeled instant and timezone/UTC
    normalization policy used by DBOS and `dateutil.isoparse`.
  - `date` hints are not widened into `datetime` objects.
  - Classmethod and instance method workflows align stored positional
    arguments with user parameters after skipping `cls` or `self`.
  - Runtime handle, retrieved handle, client/relaunch result, and terminal
    status agree with the type ledger.
- Must hold for invalid values:
  - A validator-configured workflow rejects invalid dates before user body
    effects are committed, with a durable terminal error visible through
    public retrieval paths.
  - A no-validator workflow that receives an unparseable hinted string either
    records the body-observed raw value and fails with a modeled application
    error or returns an explicit modeled rejection. It must not silently record
    that invalid value as a valid typed datetime/date.
  - Retrieval paths must not hang, retry forever, or mutate the terminal
    classification during relaunch.
- Must hold for all cases:
  - Workflow IDs, schedule names, queue names if used, and inserted row IDs are
    unique per seed.
  - The workload writes artifacts under `/tmp/...`, not under `/workspace`.
  - Cleanup removes schedules and test rows without relying on archived DBOS
    workload implementations.

#### Setup And Classification

- Build profile: `default`.
- Backend: both Postgres and SQLite are meaningful for scheduler and portable
  serializer semantics. Direct SQL row insertion must branch on DBOS schema
  shape and record the backend used.
- Target ref handling: current pinned target contains PR `#700`; if an executor
  refreshes `./target`, record the new DBOS ref in the artifact.
- Expected runtime: bounded seconds per case; use explicit timeouts around
  handle reads and relaunch checks.

#### Stale Conditions

Mark this rung stale if DBOS changes portable JSON datetime/date conversion
semantics, scheduler input shape, validation ordering, `validate_args`
contract, workflow class/instance registration metadata, or durable
`workflow_status.inputs` representation enough that the source contract above
no longer describes the product surface.
