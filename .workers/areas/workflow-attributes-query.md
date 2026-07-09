# Area: workflow-attributes-query

## Current State

Current status: completed green through bounded sweep. Scheduled workflow
identity query and legacy scheduler app-version rungs are green; a temporal
introspection regression-guard rung from PRs `#681` / `#682` / `#674` / `#685` is
executor-ready. No product finding.

Evidence:

- `evidence-key:frontiers/workflow-attributes-query/frontier.md`
- `evidence-key:runs/run-20260620T165300Z-workflow-attributes-query-rung-004-bounded-seed-sweep/summary.md`
- `.workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py`
- Recent PR `#726` (`Make Schedule Name Queryable Attribute`, merged
  2026-06-16) adds DBOS-owned `schedule_name` to workflow status rows,
  `DBOS.list_workflows`, `DBOSClient.list_workflows`, scheduler enqueue paths,
  and workflow export/import. The PR passed the full integration matrix across
  Python 3.10-3.14 on Postgres and SQLite. The current target ref lacks this
  column/filter and the new scheduler tests.
- Recent PR `#699` (`Fix Legacy Scheduler Workflow Versioning`, merged
  2026-06-01) ensures the legacy `@DBOS.scheduled` decorator enqueues workflow
  rows with the latest application version. The PR passed the full integration
  matrix across Python 3.10-3.14 on Postgres and SQLite.
- PR `#681` (`Completed At`) added workflow `completed_at`, completed-window
  filters, operation-output completion timing, and aggregate latency fields.
- PR `#682` (`Consistently Use Database Time for Workflows`) moved terminal,
  cancellation, and resume timestamps to database time so query windows do not
  depend on application clock skew.
- PR `#674` (`Group workflow aggregates by time`) added workflow aggregate
  time buckets.
- PR `#685` (`Improved Aggregates`) added selectable workflow aggregate
  outputs such as counts, minimum creation time, and latency maxes through
  system database and conductor paths.

## Product Promise

Workflow attributes are queryable, mutable, and visible through public/client
APIs consistently across creation, update, replay, fork, lifecycle transitions,
and backend-specific filtering.

## What Not To Repeat

- Do not repeat equality/type/latest-value/status/fork-history sweeps without a
  new boundary.
- Do not ignore backend-specific contract differences; unsupported SQLite
  behavior must be explicit, not accidental.

## Loop-1 Search Directions

| Direction | Why It Is Deeper |
|---|---|
| Attributes plus lifecycle cleanup | Query results after delete/cancel/resume/fork can diverge from status rows. |
| Attributes plus queues | Queued workflows and result retrieval may expose different attribute visibility windows. |
| Client/conductor paths | Local runtime and client APIs may serialize/filter attributes differently. |
| Attribute update races | Concurrent update and query windows can expose stale or partial state. |
| Scheduled workflow identity | DBOS-owned scheduler identity is persisted beside user attributes and can diverge across trigger/backfill/live enqueue, public/client list filters, export/import, and schedule deletion. |
| Scheduled app-version metadata | Legacy scheduled decorator rows use the internal queue and can carry stale application versions that break version-aware listing, recovery, or deployment reasoning. |
| Temporal introspection windows | Completion/dequeue filters, terminal/resume timestamp transitions, operation-output timing, time buckets, selectable aggregates, and latency maxes can diverge across runtime, client, conductor, queue, relaunch, and export/import paths. |

## Rung Design Requirements

Model attribute history by workflow id, update point, status, fork relation, and
query predicate. Include backend-specific expectations.

## Stale Conditions

Mark stale if attribute query API, filter semantics, or supported backend matrix
changes.

## Rung Index

Evidence source: `evidence-key:frontiers/workflow-attributes-query/rungs.index.yaml`

```yaml
schema_version: 1
columns: [id, path, status, order, level, workload_file, matrix, summary]
rungs:
  - [
      "rung-000-attribute-smoke",
      "rungs/rung-000-attribute-smoke.md",
      "passed",
      "0",
      "baseline",
      ".workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py",
      "1 case",
      "prove create/list/update/query attributes through public APIs",
    ]
  - [
      "rung-001-attribute-query-postgres",
      "rungs/rung-001-attribute-query-postgres.md",
      "passed",
      "1",
      "adversarial",
      ".workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py",
      "3 cases",
      "Postgres attribute query predicates, latest value, and lifecycle status filters",
    ]
  - [
      "rung-002-replay-fork-attribute-history",
      "rungs/rung-002-replay-fork-attribute-history.md",
      "passed",
      "2",
      "adversarial",
      ".workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py",
      "3 cases",
      "attribute updates across replay, fork, client mutation, and cleanup",
    ]
  - [
      "rung-003-cross-backend-negative-contract",
      "rungs/rung-003-cross-backend-negative-contract.md",
      "passed",
      "3",
      "contract",
      ".workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py",
      "3 cases",
      "explicitly assert unsupported or different SQLite filtering behavior",
    ]
  - [
      "rung-004-bounded-seed-sweep",
      "rungs/rung-004-bounded-seed-sweep.md",
      "passed",
      "4",
      "sweep",
      ".workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py",
      "24 cases",
      "rare-bug search across query predicates and update timing",
    ]
  - [
      "rung-005-scheduled-workflow-identity-query",
      "rungs/rung-005-scheduled-workflow-identity-query.md",
      "queued",
      "5",
      "adversarial",
      ".workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py",
      "3 cases",
      "DBOS-owned schedule identity is queryable, isolated from manual/user-attribute rows, and durable across export/import",
    ]
  - [
      "rung-006-legacy-scheduler-latest-app-version",
      "inline:loop-1-added-rung-rung-006-legacy-scheduler-latest-app-version",
      "queued",
      "6",
      "versioning",
      ".workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py",
      "3 cases",
      "legacy @DBOS.scheduled rows must use latest application version across ticks, async decorator workflows, relaunch/recovery, and public/client list observations",
    ]
  - [
      "rung-007-temporal-introspection-windows",
      "inline:loop-1-added-rung-rung-007-temporal-introspection-windows",
      "ready",
      "7",
      "regression-guard",
      ".workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py",
      "4 cases",
      "completion/dequeue timestamps and operation-output timing must remain consistent across terminal transitions, filters, aggregates, queue starts, relaunch, and export/import",
    ]
```

## Rung Details

These are the known rung contracts and outcomes for this frontier. Treat completed, finding, and closed rungs as current reality and design constraints, not as active executor queue rows.

### Loop-1 Added Rung: rung-007-temporal-introspection-windows

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-007-temporal-introspection-windows
frontier: workflow-attributes-query
status: ready
order: 7
level: regression-guard
workload_file: .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py
seeds: [6810, 6811, 6812, 6813]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/pull/681
  - https://github.com/dbos-inc/dbos-transact-py/pull/682
  - target/dbos/_sys_db.py
  - target/dbos/_dbos.py
  - target/dbos/_client.py
  - target/dbos/_conductor/conductor.py
  - target/tests/test_workflow_introspection.py
  - .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
gate_results:
  surface_evidence: ready_from_pr_681_682_and_current_target_temporal_filters
  duplicate_check: distinct_from_attribute_predicate_and_schedule_identity_rungs_because_it_targets_time_window_membership_and_latency_oracles
  oracle_critic: ready_with_independent_temporal_ledger_public_client_status_and_aggregate_parity
  executor_feasibility: default_postgres_profile_reuses_existing_workflow_attributes_workload
```

#### Source Contract

- Frontier ID: `workflow-attributes-query`.
- Rung ID: `rung-007-temporal-introspection-windows`.
- Protected product promise: workflow temporal introspection exposes durable
  completion/dequeue timestamps consistently across runtime, client, conductor,
  queued/direct workflows, terminal transitions, resume, filtering, aggregation,
  and export/import.
- Replay command:
  `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-007-temporal-introspection-windows --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `6810`, `6811`, `6812`, `6813`; every run must
  persist the seed, derived workflow IDs, modeled time windows, and filter
  membership expectations.
- Invariant oracle: independent temporal ledger, public runtime/client list
  results, workflow status rows, operation-output timing rows, and aggregate
  results agree.

#### Why This Is New

Existing workflow-attributes rungs validate user attributes, schedule identity,
and legacy app-version metadata. Target product tests cover individual
`completed_at`, step timing, and aggregate cases. This rung composes temporal
membership across terminal transitions, queue starts, relaunch/import, runtime
and client APIs, and aggregate timing so a regression cannot pass by only
setting a timestamp or only returning a nonempty filtered list.

#### Workload Shape

- Type: Python module/integration stateful introspection workload.
- Build profile: `default`.
- Runtime setup: real Postgres through `.workers/run-with-postgres.sh`.
  SQLite is diagnostic only because timestamp precision differs.
- Expected workload file: extend
  `.workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py`
  with a `rung-007-temporal-introspection-windows` mode.
- Entry points:
  - `DBOS.list_workflows` / `DBOS.list_workflows_async`
  - `DBOSClient.list_workflows` / async client list paths where available
  - `DBOS.get_workflow_status`, workflow handles, queue enqueue/start paths
  - `DBOS.cancel_workflow`, `DBOS.resume_workflow`
  - `DBOS.list_workflow_steps`, workflow aggregate APIs, and export/import when
    needed for status preservation.
- Ledger:
  - created/dequeued/completed/resumed/cancelled windows keyed by workflow ID
  - expected membership for each temporal filter and composed status/name/queue
    filter
  - public/client/conductor observations where the workload can reach them
  - read-only `workflow_status` and `operation_outputs` rows for diagnostics.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 6810 | success-error-cancel-resume-terminal-windows | stale or missing `completed_at`; client/runtime filter drift | modeled completion windows return exactly terminal workflow IDs; resumed active row is excluded until final release |
| case-002 | 6811 | queued-delayed-direct-dequeue-windows | `dequeued_at` missing, application-time drift, direct workflow leaking into queue windows | queued rows match dequeue window only after start; direct rows never match dequeue filters; delay eligibility precedes dequeue |
| case-003 | 6812 | relaunch-export-import-temporal-preservation | temporal fields are recomputed or dropped across restart/import | imported and relaunched statuses preserve modeled created/completed/dequeued fields and filter membership |
| case-004 | 6813 | step-timing-and-aggregate-latency | aggregate latency buckets disagree with row timestamps or include null bookkeeping rows | monotonic step timings, non-negative queue/total latency, completed operation-output windows exclude null bookkeeping rows |

#### Invariants

- Must hold: `created_at <= completed_at` for terminal rows and
  `completed_at is None` for active/resumed rows.
- Must hold: cancelling a nonterminal workflow sets `completed_at`, resuming
  clears it, and final completion sets a fresh timestamp.
- Must hold: `completed_after` / `completed_before` and `dequeued_after` /
  `dequeued_before` filters equal the independent model across runtime and
  client APIs.
- Must hold: direct workflows never appear in dequeue-window queries.
- Must hold: queued workflows have `dequeued_at >= created_at`; delayed
  workflows have `dequeued_at >= delay_until_epoch_ms`.
- Must hold: export/import and relaunch do not recompute or drop temporal
  fields.
- Must hold: operation-output step timing is monotonic, aggregate queue/total
  latency is non-negative where defined, and completed operation-output windows
  do not include bookkeeping rows with null completion time.

#### Expected Signatures

- Success: every public/client/status/aggregate observation equals the
  independent temporal ledger for all four cases.
- Finding: missing or stale `completed_at`, resumed workflow leaking into
  completion windows, direct workflow leaking into dequeue windows, queue/dequeue
  timestamp ordering violation, runtime/client/conductor filter drift,
  aggregate latency mismatch, operation-output timing regression, or temporal
  fields lost across relaunch/export/import.
- Setup block: target runtime cannot isolate time windows, queue start cannot be
  gated, export/import path is unavailable, or Postgres setup cannot provide
  stable millisecond-enough timestamp evidence.
- Low signal: the executor only reruns product tests, checks timestamp
  non-nullness without modeled windows, or uses current wall-clock comparisons
  without persisting derived membership expectations.
- Goal drift: the workload turns into another user-attribute predicate sweep,
  broad scheduler test, or raw performance timing benchmark.

#### Stale Conditions

Mark stale if DBOS changes workflow timestamp fields, database-time semantics,
workflow list/client/conductor temporal filters, operation-output timing,
workflow aggregate semantics, export/import status preservation, or if target
ref advances past a new introspection/timestamp PR that changes the public
contract.

### Loop-1 Added Rung: rung-006-legacy-scheduler-latest-app-version

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-006-legacy-scheduler-latest-app-version
frontier: workflow-attributes-query
status: queued
order: 6
level: versioning
workload_file: .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py
seeds: [6990, 6991, 6992]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/pull/699
  - target/dbos/_scheduler_decorator.py
  - target/dbos/_context.py
  - target/dbos/_queue.py
  - target/dbos/_sys_db.py
  - target/tests/test_scheduler_decorator.py
  - .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-data-and-fixtures/overview.md
gate_results:
  surface_evidence: ready_from_pr_699_and_target_scheduler_decorator_code
  duplicate_check: distinct_from_scheduled_workflow_identity_query_because_it_targets_legacy_decorator_app_version_metadata
  oracle_critic: ready_with_workflow_status_app_version_public_client_and_relaunch_parity
  executor_feasibility: default_profile_realistic_postgres_and_sqlite_both_meaningful
```

#### Source Contract

- Frontier ID: `workflow-attributes-query`.
- Rung ID: `rung-006-legacy-scheduler-latest-app-version`.
- Protected product promise: workflows enqueued by the legacy
  `@DBOS.scheduled` decorator carry the latest DBOS application version in
  durable workflow status and public list/query observations, even though that
  decorator uses the internal scheduler queue rather than the newer
  `create_schedule(...)` API.
- Replay command:
  `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-006-legacy-scheduler-latest-app-version --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `6990`, `6991`, `6992`; every run must persist app
  version names/timestamps, scheduled workflow ID prefixes, observed workflow
  IDs, public/client status rows, queue snapshots if inspected, relaunch
  boundary, and terminal results.
- Invariant oracle: runtime and client list/status observations, durable
  `workflow_status.app_version`, modeled latest-version timeline, and terminal
  handle results must agree within bounded time.

#### Goal

Extend the workflow query workload to cover version metadata for legacy
decorator-driven scheduled rows. The product test proves one sync scheduled
function uses a manually-created newer version; this rung should cover version
rollover timing, async scheduled workflows, relaunch/recovery, and public/client
query parity without duplicating scheduled identity filtering from rung 005.

#### Workload File

- Expected path:
  `.workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py`.
- Create or reuse: reuse the existing workflow-attributes workload file; add a
  separate rung dispatch for legacy scheduled app-version cases.
- Why one file is enough: the existing frontier already owns public workflow
  listing, status comparison, backend-specific classification, and query
  artifact structure.

#### Workload Shape

- Type: product-runtime versioning workload with public DBOS workflow list/status
  APIs plus read-only status inspection when needed.
- Entry points:
  - `@DBOS.scheduled` with sync and async `@DBOS.workflow` functions
  - `SystemDatabase.create_application_version` and
    `update_application_version_timestamp` as setup evidence for latest-version
    selection
  - `DBOS.list_workflows`, `DBOS.retrieve_workflow`, `DBOSClient.list_workflows`
  - optional read-only `workflow_status` snapshots for app-version parity
- Fault model: create multiple app versions with timestamp ordering, let legacy
  scheduler ticks enqueue rows, update latest version before later ticks,
  relaunch the DBOS runtime, and compare runtime/client observations with the
  modeled latest-version timeline.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 6990 | sync-decorator-latest-version | create older/newer app versions before first scheduled tick | all scheduled workflow rows use the modeled latest version, not process default or stale version |
| case-002 | 6991 | async-decorator-version-rollover | async scheduled workflow runs before and after latest-version timestamp changes | rows before/after rollover match the version that was latest when enqueued; terminal results remain retrievable |
| case-003 | 6992 | relaunch-scheduled-version-parity | relaunch runtime after scheduled rows are enqueued and query through runtime/client APIs | app-version metadata survives relaunch and runtime/client list observations agree with durable status rows |

#### Invariants

- Must hold:
  - Every workflow ID with the modeled legacy scheduler prefix has
    `app_version` equal to the expected latest version for its enqueue window.
  - Runtime and client list/status APIs agree with durable status rows for
    workflow ID, name, status, app version, and terminal result availability.
  - Async scheduled functions preserve app-version metadata just like sync
    scheduled functions.
  - Relaunch does not rewrite or drop app-version metadata.
- Must not happen:
  - Legacy scheduled rows inherit an empty/default app version when a newer
    application version exists.
  - Rows from one scheduled function are used as evidence for another because
    the workflow ID prefix was too broad.
  - The workload treats unresolved scheduler overlap/tick cadence as a bug; the
    oracle is about metadata on rows that did enqueue.

#### Setup And Classification

- Build profile: `default`.
- Backend: Postgres and SQLite are both meaningful because app-version metadata
  is durable workflow status.
- Target ref handling: current pinned target contains PR `#699`; if an executor
  refreshes `./target`, record the new DBOS ref in the artifact.
- Expected runtime: bounded by scheduled tick cadence; use short second-level
  cron expressions and explicit polling deadlines.

#### Stale Conditions

Mark this rung stale if DBOS removes the legacy `@DBOS.scheduled` decorator,
changes application-version selection semantics, changes internal scheduler
queue version propagation, or changes workflow status/list app-version fields.

### Rung: rung-000-attribute-smoke

Evidence source: `evidence-key:frontiers/workflow-attributes-query/rungs/rung-000-attribute-smoke.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-attribute-smoke
frontier: workflow-attributes-query
status: ready
order: 0
level: baseline
workload_file: .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py
seeds:
  - 3800
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 000 Attribute Smoke

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072711428752000Z.prompt.md`.
- Frontier ID: `workflow-attributes-query`.
- Rung ID: `rung-000-attribute-smoke`.
- Protected product promise: preserve the concrete `workflow-attributes-query` promise from `frontier.md` and `strategy/candidates/workflow-attributes-query.md`.
- Replay command: `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-000-attribute-smoke --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results.

##### Goal

- Build and run: prove create/list/update/query attributes through public APIs.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `workflow-attributes-query` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: product-native setup proof.
- Entry points: workflow attributes on start/update/client APIs, Postgres query predicates, status filters, replay/fork history, SQLite negative contract, and cleanup/list APIs.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | setup | attribute create/list/update/query APIs run | create workflow with attributes, update, query | latest attributes returned | attribute smoke oracle |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3800 | create-workflow-with-attributes-update-query | none unless case says setup block | attribute create/list/update/query APIs run | attribute smoke oracle |


##### Invariants

- Must hold: every attribute mutation is modeled as whole-dict replace or clear before DBOS query results are inspected.
- Must hold: Postgres attribute filters return exactly modeled workflow IDs across status and lifecycle filters.
- Must hold: replay/fork/client mutations preserve independent attribute history and do not leak stale values.
- Must hold: backend-specific unsupported behavior is asserted as an explicit contract, not a silent skip.
- Must never happen: the workload passes by checking only that some workflow was returned.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/workflow-attributes-query.md`
  - `evidence-key:frontiers/workflow-attributes-query/frontier.md`
- Suggested command family:
  - `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-000-attribute-smoke --case case-001`
  - `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-000-attribute-smoke --all-cases --sequential`
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

### Loop-1 Added Rung: rung-005-scheduled-workflow-identity-query

Evidence source: loop-1 producer pass on recent PR `#726` and current target
ref `0c41e6dfb46440184d19a52cdecc64a8c5f40d60`.

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-005-scheduled-workflow-identity-query
frontier: workflow-attributes-query
status: queued
order: 5
level: adversarial
workload_file: .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py
seeds:
  - 3820
  - 3821
  - 3822
updated_at: 2026-06-24
```

#### Source Contract

- Frontier ID: `workflow-attributes-query`.
- Rung ID: `rung-005-scheduled-workflow-identity-query`.
- Protected product promise: DBOS-owned scheduler identity (`schedule_name`) is
  queryable through workflow status/list APIs, composes with workflow
  name/status/queue/user-attribute filters, distinguishes schedules sharing one
  workflow function, excludes manual workflow runs, and survives workflow
  export/import.
- Replay command: `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-005-scheduled-workflow-identity-query --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed,
  generated schedule names, workflow IDs, API path used, and any exported
  workflow payload summary needed for replay.
- Invariant oracle: an independent schedule-origin model maps workflow IDs to
  schedule names and manual/null origin; public DBOS and client list/status
  results must exactly match that model for single-name, name-list, composed
  filter, and export/import observations.

#### Why This Is Not A Duplicate

Completed rungs in this frontier cover user-supplied workflow attributes,
Postgres JSON predicates, lifecycle/fork history, SQLite negative behavior, and
bounded seed search. They do not cover a DBOS-owned indexed identity field that
is populated by scheduler code, surfaced on public/client status APIs, and
preserved by export/import.

Scheduler rungs cover trigger/backfill/live queue routing and timing. They do
not assert that scheduled workflow rows are queryable by schedule identity or
that imported historical runs remain queryable after the schedule object is
deleted.

#### Target And PR Evidence

- Current target `target/dbos/_schemas/system_database.py` has
  `workflow_status.attributes` but no `workflow_status.schedule_name` column or
  schedule-name index.
- Current target `target/dbos/_scheduler.py` creates scheduled workflow status
  with `attributes: None` and no schedule identity field.
- Current target `target/dbos/_sys_db.py`, `target/dbos/_dbos.py`, and
  `target/dbos/_client.py` list workflows by attributes and lifecycle fields
  but have no `schedule_name` filter or returned status field.
- PR `#726` adds the status column/index, sets `schedule_name` in live,
  backfill, and trigger enqueue paths, plumbs `schedule_name` through
  `DBOS.list_workflows` and `DBOSClient.list_workflows`, and includes
  scheduler tests for two schedules sharing a workflow function and for
  export/delete/import persistence.
- `gh pr checks 726 --repo dbos-inc/dbos-transact-py` showed all integration
  jobs passing for Python 3.10-3.14 on both Postgres and SQLite.

#### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: `DBOS.create_schedule`, `DBOS.trigger_schedule`,
  `DBOS.backfill_schedule`, normal `DBOS.start_workflow`,
  `DBOS.list_workflows`, `DBOSClient.list_workflows`, `DBOS.get_workflow_status`,
  and internal export/import only where no public export/import API exists.
- Existing workload file: reuse
  `.workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py`
  because the actor remains workflow query/status observation and the oracle is
  still a query-result model. Add a distinct rung dispatcher rather than
  broadening the old attribute predicate cases.
- Build profile: `default`; both Postgres and SQLite are meaningful because
  `schedule_name` is a plain text filter, unlike JSON attribute filtering.
- Setup: create unique schedule names per seed, use isolated DBOS app/database
  state, clean up schedules after cases, and record schedule/workflow IDs.

#### Attack Plan

| Case | Seed | Axis | Assumption Attacked | Perturbation | Oracle |
|---|---:|---|---|---|---|
| case-001 | 3820 | trigger identity | Workflow name is enough to distinguish scheduled runs | Create two schedules sharing the same workflow function, trigger both, and also start a manual workflow with the same or adjacent function identity | `list_workflows(schedule_name="sched-a")` returns only A, list filter returns A/B exactly, nonexistent schedule returns empty, and manual workflow status has null schedule identity |
| case-002 | 3821 | composed live/backfill filters | Schedule identity is populated uniformly across scheduler entry points and composes with existing filters | Mix trigger and explicit backfill windows, optionally declared queue, and status/name/queue filters | Every scheduled row has the modeled schedule name; composed filters equal model intersection; manual or other-schedule rows never leak |
| case-003 | 3822 | export/import durability | Schedule identity is derived from the current schedule table instead of persisted workflow state | Trigger a schedule, export the workflow, delete workflow, optionally delete the schedule, import the workflow, then query by schedule name | Imported status still has the original schedule name and `list_workflows(schedule_name=...)` finds the workflow; user `attributes` remain separate and are not overwritten by schedule identity |

#### Invariants

- Must hold: every workflow ID in the case model has exactly one origin:
  manual/null or a single schedule name.
- Must hold: public `DBOS.list_workflows` and `DBOSClient.list_workflows` return
  exactly the modeled workflow IDs for a single `schedule_name`, a list of
  schedule names, and any composed `name`, `status`, `queue_name`, or
  `attributes` filter used by the case.
- Must hold: `get_workflow_status` agrees with list results for the same
  workflow ID and exposes null schedule identity for manual/direct runs.
- Must hold: exported and reimported workflows preserve schedule identity even
  when the schedule table row is no longer present.
- Must never happen: a run from one schedule is returned by another schedule's
  filter, a manual run is returned by a schedule filter, user workflow
  attributes are mistaken for `schedule_name`, or the workload passes by
  checking only nonempty result counts.

#### Expected Signatures

- Success: all cases reach their trigger/backfill/export windows and every
  public/client/status observation equals the independent schedule-origin
  model.
- Finding: schedule identity is missing on one scheduler entry path, filters
  return same-function rows from the wrong schedule, manual rows leak into
  schedule queries, export/import drops schedule identity, schedule deletion
  breaks historical queryability, or user attributes collide with
  `schedule_name`.
- Setup block: schedule creation/trigger/backfill cannot be isolated under the
  target build profile, or export/import is unavailable through any stable
  runtime path.
- Low signal: the executor only checks PR-added unit tests, only validates that
  `schedule_name` exists, or does not compare results against an independent
  model.
- Goal drift: the workload turns into scheduler queue-routing validation,
  broad schedule CRUD testing, or another bounded user-attribute seed sweep.

#### Stale Conditions

Mark stale if the target ref advances beyond PR `#726`, the public workflow
status/list API changes its schedule identity contract, export/import is
removed or made private-only, or schedule-origin identity moves out of workflow
status into a documented alternate query surface.

### Rung: rung-001-attribute-query-postgres

Evidence source: `evidence-key:frontiers/workflow-attributes-query/rungs/rung-001-attribute-query-postgres.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-attribute-query-postgres
frontier: workflow-attributes-query
status: selected
order: 1
level: adversarial
workload_file: .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py
seeds:
  - 3810
  - 3811
  - 3812
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 001 Attribute Query Postgres

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072711428752000Z.prompt.md`.
- Frontier ID: `workflow-attributes-query`.
- Rung ID: `rung-001-attribute-query-postgres`.
- Protected product promise: preserve the concrete `workflow-attributes-query` promise from `frontier.md` and `strategy/candidates/workflow-attributes-query.md`.
- Replay command: `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-001-attribute-query-postgres --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results.

##### Goal

- Build and run: Postgres attribute query predicates, latest value, and lifecycle status filters.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `workflow-attributes-query` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: workflow attributes on start/update/client APIs, Postgres query predicates, status filters, replay/fork history, SQLite negative contract, and cleanup/list APIs.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | query predicate | Postgres equality and type predicates return exact modeled IDs | create workflows with string/int/bool attrs | query returns exact set | predicate model agrees |
| case-002 | latest value | attribute replacement removes stale keys | replace dict then query old and new keys | old key absent, new value present | whole-dict replace oracle |
| case-003 | status filter | attribute predicate composes with lifecycle status | mix success/error/cancelled workflows with attrs | filtered IDs equal status+attr model | composed query oracle |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3810 | create-workflows-with-string-int-bool-attrs | none unless case says setup block | Postgres equality and type predicates return exact modeled IDs | predicate model agrees |
| case-002 | 3811 | replace-dict-then-query-old-and-new-keys | none unless case says setup block | attribute replacement removes stale keys | whole-dict replace oracle |
| case-003 | 3812 | mix-success-error-cancelled-workflows-with-attrs | none unless case says setup block | attribute predicate composes with lifecycle status | composed query oracle |


##### Invariants

- Must hold: every attribute mutation is modeled as whole-dict replace or clear before DBOS query results are inspected.
- Must hold: Postgres attribute filters return exactly modeled workflow IDs across status and lifecycle filters.
- Must hold: replay/fork/client mutations preserve independent attribute history and do not leak stale values.
- Must hold: backend-specific unsupported behavior is asserted as an explicit contract, not a silent skip.
- Must never happen: the workload passes by checking only that some workflow was returned.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/workflow-attributes-query.md`
  - `evidence-key:frontiers/workflow-attributes-query/frontier.md`
- Suggested command family:
  - `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-001-attribute-query-postgres --case case-001`
  - `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-001-attribute-query-postgres --all-cases --sequential`
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

### Rung: rung-002-replay-fork-attribute-history

Evidence source: `evidence-key:frontiers/workflow-attributes-query/rungs/rung-002-replay-fork-attribute-history.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-002-replay-fork-attribute-history
frontier: workflow-attributes-query
status: ready
order: 2
level: adversarial
workload_file: .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py
seeds:
  - 3820
  - 3821
  - 3822
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 002 Replay Fork Attribute History

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072711428752000Z.prompt.md`.
- Frontier ID: `workflow-attributes-query`.
- Rung ID: `rung-002-replay-fork-attribute-history`.
- Protected product promise: preserve the concrete `workflow-attributes-query` promise from `frontier.md` and `strategy/candidates/workflow-attributes-query.md`.
- Replay command: `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-002-replay-fork-attribute-history --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results.

##### Goal

- Build and run: attribute updates across replay, fork, client mutation, and cleanup.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `workflow-attributes-query` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: workflow attributes on start/update/client APIs, Postgres query predicates, status filters, replay/fork history, SQLite negative contract, and cleanup/list APIs.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | replay | attribute update inside workflow is checkpointed once | recover/replay workflow that updates attrs | one update step and latest attrs | step/history model |
| case-002 | fork | fork inherits modeled attributes at fork point | update attrs before and after fork | fork/original attrs match history | fork attribute oracle |
| case-003 | client mutation | client update during lifecycle transition is visible once | client updates attrs while workflow blocked/resumed | latest attrs and list filters agree | client mutation model |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3820 | recover-replay-workflow-that-updates-attrs | none unless case says setup block | attribute update inside workflow is checkpointed once | step/history model |
| case-002 | 3821 | update-attrs-before-and-after-fork | none unless case says setup block | fork inherits modeled attributes at fork point | fork attribute oracle |
| case-003 | 3822 | client-updates-attrs-while-workflow-blocked-resu | none unless case says setup block | client update during lifecycle transition is visible once | client mutation model |


##### Invariants

- Must hold: every attribute mutation is modeled as whole-dict replace or clear before DBOS query results are inspected.
- Must hold: Postgres attribute filters return exactly modeled workflow IDs across status and lifecycle filters.
- Must hold: replay/fork/client mutations preserve independent attribute history and do not leak stale values.
- Must hold: backend-specific unsupported behavior is asserted as an explicit contract, not a silent skip.
- Must never happen: the workload passes by checking only that some workflow was returned.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/workflow-attributes-query.md`
  - `evidence-key:frontiers/workflow-attributes-query/frontier.md`
- Suggested command family:
  - `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-002-replay-fork-attribute-history --case case-001`
  - `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-002-replay-fork-attribute-history --all-cases --sequential`
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

### Rung: rung-003-cross-backend-negative-contract

Evidence source: `evidence-key:frontiers/workflow-attributes-query/rungs/rung-003-cross-backend-negative-contract.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-003-cross-backend-negative-contract
frontier: workflow-attributes-query
status: ready
order: 3
level: contract
workload_file: .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py
seeds:
  - 3830
  - 3831
  - 3832
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 003 Cross Backend Negative Contract

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072711428752000Z.prompt.md`.
- Frontier ID: `workflow-attributes-query`.
- Rung ID: `rung-003-cross-backend-negative-contract`.
- Protected product promise: preserve the concrete `workflow-attributes-query` promise from `frontier.md` and `strategy/candidates/workflow-attributes-query.md`.
- Replay command: `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-003-cross-backend-negative-contract --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results.

##### Goal

- Build and run: explicitly assert unsupported or different SQLite filtering behavior.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `workflow-attributes-query` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: workflow attributes on start/update/client APIs, Postgres query predicates, status filters, replay/fork history, SQLite negative contract, and cleanup/list APIs.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | backend contract | SQLite unsupported filtering is explicit | run same attr filter against SQLite if configured | clear unsupported/different result contract recorded | backend negative oracle |
| case-002 | backend contract | Postgres path remains authoritative | compare Postgres expected set to SQLite behavior note | Postgres must pass exact predicate model | Postgres positive oracle |
| case-003 | error handling | unsupported backend does not return silently wrong set | request unsupported predicate | specific error/skip contract captured | no false-positive result set |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3830 | run-same-attr-filter-against-sqlite-if-configure | none unless case says setup block | SQLite unsupported filtering is explicit | backend negative oracle |
| case-002 | 3831 | compare-postgres-expected-set-to-sqlite-behavior | none unless case says setup block | Postgres path remains authoritative | Postgres positive oracle |
| case-003 | 3832 | request-unsupported-predicate | none unless case says setup block | unsupported backend does not return silently wrong set | no false-positive result set |


##### Invariants

- Must hold: every attribute mutation is modeled as whole-dict replace or clear before DBOS query results are inspected.
- Must hold: Postgres attribute filters return exactly modeled workflow IDs across status and lifecycle filters.
- Must hold: replay/fork/client mutations preserve independent attribute history and do not leak stale values.
- Must hold: backend-specific unsupported behavior is asserted as an explicit contract, not a silent skip.
- Must never happen: the workload passes by checking only that some workflow was returned.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/workflow-attributes-query.md`
  - `evidence-key:frontiers/workflow-attributes-query/frontier.md`
- Suggested command family:
  - `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-003-cross-backend-negative-contract --case case-001`
  - `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-003-cross-backend-negative-contract --all-cases --sequential`
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

Evidence source: `evidence-key:frontiers/workflow-attributes-query/rungs/rung-004-bounded-seed-sweep.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-004-bounded-seed-sweep
frontier: workflow-attributes-query
status: ready
order: 4
level: sweep
workload_file: .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py
seeds:
  - 3840
  - 3841
  - 3842
  - 3843
  - 3844
  - 3845
  - 3846
  - 3847
  - 3848
  - 3849
  - 3850
  - 3851
  - 3852
  - 3853
  - 3854
  - 3855
  - 3856
  - 3857
  - 3858
  - 3859
  - 3860
  - 3861
  - 3862
  - 3863
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 004 Bounded Seed Sweep

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072711428752000Z.prompt.md`.
- Frontier ID: `workflow-attributes-query`.
- Rung ID: `rung-004-bounded-seed-sweep`.
- Protected product promise: preserve the concrete `workflow-attributes-query` promise from `frontier.md` and `strategy/candidates/workflow-attributes-query.md`.
- Replay command: `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-004-bounded-seed-sweep --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results.

##### Goal

- Build and run: rare-bug search across query predicates and update timing.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `workflow-attributes-query` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: workflow attributes on start/update/client APIs, Postgres query predicates, status filters, replay/fork history, SQLite negative contract, and cleanup/list APIs.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | bounded sweep | predicate-equality preserves the frontier oracle | generate bounded predicate-equality variant from seed | case reaches predicate-equality evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-002 | bounded sweep | predicate-type preserves the frontier oracle | generate bounded predicate-type variant from seed | case reaches predicate-type evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-003 | bounded sweep | latest-replace preserves the frontier oracle | generate bounded latest-replace variant from seed | case reaches latest-replace evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-004 | bounded sweep | status-compose preserves the frontier oracle | generate bounded status-compose variant from seed | case reaches status-compose evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-005 | bounded sweep | fork-history preserves the frontier oracle | generate bounded fork-history variant from seed | case reaches fork-history evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-006 | bounded sweep | backend-negative preserves the frontier oracle | generate bounded backend-negative variant from seed | case reaches backend-negative evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-007 | bounded sweep | predicate-equality preserves the frontier oracle | generate bounded predicate-equality variant from seed | case reaches predicate-equality evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-008 | bounded sweep | predicate-type preserves the frontier oracle | generate bounded predicate-type variant from seed | case reaches predicate-type evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-009 | bounded sweep | latest-replace preserves the frontier oracle | generate bounded latest-replace variant from seed | case reaches latest-replace evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-010 | bounded sweep | status-compose preserves the frontier oracle | generate bounded status-compose variant from seed | case reaches status-compose evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-011 | bounded sweep | fork-history preserves the frontier oracle | generate bounded fork-history variant from seed | case reaches fork-history evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-012 | bounded sweep | backend-negative preserves the frontier oracle | generate bounded backend-negative variant from seed | case reaches backend-negative evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-013 | bounded sweep | predicate-equality preserves the frontier oracle | generate bounded predicate-equality variant from seed | case reaches predicate-equality evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-014 | bounded sweep | predicate-type preserves the frontier oracle | generate bounded predicate-type variant from seed | case reaches predicate-type evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-015 | bounded sweep | latest-replace preserves the frontier oracle | generate bounded latest-replace variant from seed | case reaches latest-replace evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-016 | bounded sweep | status-compose preserves the frontier oracle | generate bounded status-compose variant from seed | case reaches status-compose evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-017 | bounded sweep | fork-history preserves the frontier oracle | generate bounded fork-history variant from seed | case reaches fork-history evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-018 | bounded sweep | backend-negative preserves the frontier oracle | generate bounded backend-negative variant from seed | case reaches backend-negative evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-019 | bounded sweep | predicate-equality preserves the frontier oracle | generate bounded predicate-equality variant from seed | case reaches predicate-equality evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-020 | bounded sweep | predicate-type preserves the frontier oracle | generate bounded predicate-type variant from seed | case reaches predicate-type evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-021 | bounded sweep | latest-replace preserves the frontier oracle | generate bounded latest-replace variant from seed | case reaches latest-replace evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-022 | bounded sweep | status-compose preserves the frontier oracle | generate bounded status-compose variant from seed | case reaches status-compose evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-023 | bounded sweep | fork-history preserves the frontier oracle | generate bounded fork-history variant from seed | case reaches fork-history evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |
| case-024 | bounded sweep | backend-negative preserves the frontier oracle | generate bounded backend-negative variant from seed | case reaches backend-negative evidence point | attribute history by workflow id, update point, fork relation, status, backend, and query predicate agrees with public list/query results |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3840 | generate-bounded-predicate-equality-variant-from | none unless case says setup block | predicate-equality preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-002 | 3841 | generate-bounded-predicate-type-variant-from-see | none unless case says setup block | predicate-type preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-003 | 3842 | generate-bounded-latest-replace-variant-from-see | none unless case says setup block | latest-replace preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-004 | 3843 | generate-bounded-status-compose-variant-from-see | none unless case says setup block | status-compose preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-005 | 3844 | generate-bounded-fork-history-variant-from-seed | none unless case says setup block | fork-history preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-006 | 3845 | generate-bounded-backend-negative-variant-from-s | none unless case says setup block | backend-negative preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-007 | 3846 | generate-bounded-predicate-equality-variant-from | none unless case says setup block | predicate-equality preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-008 | 3847 | generate-bounded-predicate-type-variant-from-see | none unless case says setup block | predicate-type preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-009 | 3848 | generate-bounded-latest-replace-variant-from-see | none unless case says setup block | latest-replace preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-010 | 3849 | generate-bounded-status-compose-variant-from-see | none unless case says setup block | status-compose preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-011 | 3850 | generate-bounded-fork-history-variant-from-seed | none unless case says setup block | fork-history preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-012 | 3851 | generate-bounded-backend-negative-variant-from-s | none unless case says setup block | backend-negative preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-013 | 3852 | generate-bounded-predicate-equality-variant-from | none unless case says setup block | predicate-equality preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-014 | 3853 | generate-bounded-predicate-type-variant-from-see | none unless case says setup block | predicate-type preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-015 | 3854 | generate-bounded-latest-replace-variant-from-see | none unless case says setup block | latest-replace preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-016 | 3855 | generate-bounded-status-compose-variant-from-see | none unless case says setup block | status-compose preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-017 | 3856 | generate-bounded-fork-history-variant-from-seed | none unless case says setup block | fork-history preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-018 | 3857 | generate-bounded-backend-negative-variant-from-s | none unless case says setup block | backend-negative preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-019 | 3858 | generate-bounded-predicate-equality-variant-from | none unless case says setup block | predicate-equality preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-020 | 3859 | generate-bounded-predicate-type-variant-from-see | none unless case says setup block | predicate-type preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-021 | 3860 | generate-bounded-latest-replace-variant-from-see | none unless case says setup block | latest-replace preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-022 | 3861 | generate-bounded-status-compose-variant-from-see | none unless case says setup block | status-compose preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-023 | 3862 | generate-bounded-fork-history-variant-from-seed | none unless case says setup block | fork-history preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |
| case-024 | 3863 | generate-bounded-backend-negative-variant-from-s | none unless case says setup block | backend-negative preserves the frontier oracle | attribute history by workflow id, update point, fork relation, status, backend, |


##### Invariants

- Must hold: every attribute mutation is modeled as whole-dict replace or clear before DBOS query results are inspected.
- Must hold: Postgres attribute filters return exactly modeled workflow IDs across status and lifecycle filters.
- Must hold: replay/fork/client mutations preserve independent attribute history and do not leak stale values.
- Must hold: backend-specific unsupported behavior is asserted as an explicit contract, not a silent skip.
- Must never happen: the workload passes by checking only that some workflow was returned.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/workflow-attributes-query.md`
  - `evidence-key:frontiers/workflow-attributes-query/frontier.md`
- Suggested command family:
  - `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-004-bounded-seed-sweep --case case-001`
  - `python .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-004-bounded-seed-sweep --all-cases --sequential`
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
