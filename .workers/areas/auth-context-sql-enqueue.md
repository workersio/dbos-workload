# Area: auth-context-sql-enqueue

## Current State

Current status: completed with one confirmed SQL-origin auth finding. The
finding was filed upstream as issue `#743`; PR `#744` is open and proposes the
required-role terminal-error fix. No adjacent rung is currently executor-ready.

Recent issue/PR scan used for this frontier:

- Issue `#683`: applications that enqueue workflows from PostgreSQL need to
  attach authenticated user and roles metadata inside the same SQL transaction
  as application writes.
- PR `#684`: "Add Options to SQL Enqueue" added optional
  `authenticated_user`, `authenticated_roles`, and `delay_until_epoch_ms`
  parameters to the PostgreSQL `dbos.enqueue_workflow(...)` helper.
- Issue `#743`: SQL-enqueued required-role denial can leave a dequeued workflow
  stuck `PENDING` with null output/error.
- PR `#744`: "Simplify Recovery and Fix Role Validation" finalizes
  `DBOSNotAuthorizedError` as workflow `ERROR` across direct start, queue,
  recovery, queue+recovery, and async execution paths.
- The current target ref includes the SQL helper options and narrow product
  tests for direct SQL auth metadata and delayed SQL enqueue.
- Existing auth tests cover Python middleware/context, DBOSClient enqueue auth
  metadata, role recovery, and tracing. Existing queue/lifecycle frontiers cover
  queue controls and recovery, but not SQL-origin auth metadata as a durable
  workflow ownership and authorization contract.

## Product Promise

Workflow auth metadata supplied through trusted DBOS entry points is preserved
as durable workflow ownership context across SQL-side enqueue, queue execution,
public/runtime/client list and status APIs, recovery/relaunch, export/import,
and required-role execution. SQL-origin workflows must neither lose
`authenticated_user` / `authenticated_roles` nor accidentally borrow auth
context from Python middleware, clients, or neighboring workflows.

## Why This Matters

`dbos.enqueue_workflow(...)` is the path applications use when they need to
write application rows and enqueue DBOS work atomically in PostgreSQL. If that
path drops or corrupts auth metadata, operators cannot filter work by user,
authorization checks can fail after recovery, and audit/ownership views can
diverge from workflows enqueued through Python or DBOSClient APIs. The bug
surface is at the boundary between SQL function arguments, durable system-table
rows, queue workers, Python runtime context restoration, and public
introspection APIs.

## Evidence

- Issue:
  - https://github.com/dbos-inc/dbos-transact-py/issues/683
  - https://github.com/dbos-inc/dbos-transact-py/issues/743
- PR:
  - https://github.com/dbos-inc/dbos-transact-py/pull/684
  - https://github.com/dbos-inc/dbos-transact-py/pull/744
- Code:
  - `target/dbos/_migration.py`: migration thirty-eight defines
    `dbos.enqueue_workflow(...)` with `authenticated_user`,
    `authenticated_roles`, and `delay_until_epoch_ms`, inserts those fields into
    `workflow_status`, and marks SQL-created inputs as portable JSON.
  - `target/dbos/_sys_db.py`: `list_workflows`, `get_workflow_status`,
    workflow export/import, and queue/dequeue paths load
    `authenticated_user`, `authenticated_roles`, and `assumed_role` from durable
    workflow status rows.
  - `target/dbos/_roles.py` and `target/dbos/_context.py`: required-role checks
    select an assumed role from restored authenticated roles and expose auth
    values through runtime context and spans.
- Tests:
  - `target/tests/test_pgsql_client.py::test_pgsql_enqueue_with_auth_metadata`
    covers one SQL enqueue row preserving user and roles through
    `retrieve_workflow(...).get_status()`.
  - `target/tests/test_pgsql_client.py::test_pgsql_enqueue_with_delay` covers
    one SQL-origin delayed row.
  - `target/tests/test_auth.py::test_roles_recovery` and
    `test_roles_recovery_async` cover Python-origin role recovery.
  - `target/tests/test_client.py::test_client_auth` covers DBOSClient auth
    metadata and list observations.

## What Not To Repeat

- Do not repeat only the product test that enqueues one SQL workflow and reads
  `handle.get_status()`.
- Do not turn this into a generic workflow-attributes query sweep; auth
  metadata has authorization and ownership meaning, not just arbitrary JSON.
- Do not assert that the SQL helper authenticates users. The trust boundary is
  that a trusted database caller may attach already-authenticated metadata.
- Do not require SQLite parity for the SQL helper; this is a PostgreSQL stored
  function surface. SQLite can remain useful only as a negative/setup
  classifier if the workload supports it.

## Adversarial Model

The frontier attacks the assumption that every DBOS entry point records and
restores auth context the same way. It mixes SQL-origin enqueues, Python
context-origin enqueues, DBOSClient-origin enqueues, delayed SQL rows, duplicate
SQL enqueue attempts, required-role workflow bodies, recovery/relaunch, and
export/import. The independent model records the intended owner, roles, queue
state, and expected terminal result for each workflow ID, then compares durable
rows and public observations after each boundary.

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
      "read-only:target/tests/test_pgsql_client.py",
      "3 existing product tests",
      "read-only evidence for SQL auth metadata, SQL delay, and Python role recovery",
    ]
  - [
      "rung-001-sql-auth-context-recovery-query",
      "inline:rung-001-sql-auth-context-recovery-query",
      "finding_candidate",
      "1",
      "auth-sql-boundary",
      ".workers/workloads/auth-context-sql-enqueue/auth_context_sql_enqueue_workload.py",
      "4 cases",
      "SQL-origin auth metadata must survive queued execution, required-role checks, delayed promotion, duplicate enqueue, relaunch/recovery, export/import, and runtime/client list/status observations",
    ]
```

## Rung Details

### Rung: rung-000-product-regression-baseline

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-product-regression-baseline
frontier: auth-context-sql-enqueue
status: not_run_optional
order: 0
level: baseline
workload_file: read-only:target/tests/test_pgsql_client.py
seeds: [0]
updated_at: 2026-06-24T00:00:00Z
```

#### Source Contract

- Frontier ID: `auth-context-sql-enqueue`.
- Rung ID: `rung-000-product-regression-baseline`.
- Protected product promise: the target's narrow product tests demonstrate that
  SQL enqueue can persist auth metadata and delayed status, and Python-origin
  workflows can restore roles during recovery.
- Replay command: optional read-only product pytest selection under the target
  test harness:
  `.workers/run-with-postgres.sh .workers/python-runtime.sh -m pytest tests/test_pgsql_client.py::test_pgsql_enqueue_with_auth_metadata tests/test_pgsql_client.py::test_pgsql_enqueue_with_delay tests/test_auth.py::test_roles_recovery -q`.
- Seed policy: fixed seed `0`.
- Invariant oracle: selected product regression tests pass at the target
  evidence ref or the executor's explicitly refreshed DBOS ref.

### Rung: rung-001-sql-auth-context-recovery-query

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-sql-auth-context-recovery-query
frontier: auth-context-sql-enqueue
status: finding_candidate
order: 1
level: auth-sql-boundary
workload_file: .workers/workloads/auth-context-sql-enqueue/auth_context_sql_enqueue_workload.py
seeds: [6830, 6831, 6832, 6833]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/issues/683
  - https://github.com/dbos-inc/dbos-transact-py/pull/684
  - https://github.com/dbos-inc/dbos-transact-py/issues/743
  - https://github.com/dbos-inc/dbos-transact-py/pull/744
  - target/dbos/_migration.py
  - target/dbos/_sys_db.py
  - target/dbos/_roles.py
  - target/dbos/_context.py
  - target/tests/test_pgsql_client.py
  - target/tests/test_auth.py
  - target/tests/test_client.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-data-and-fixtures/overview.md
gate_results:
  surface_evidence: ready_from_issue_683_pr_684_target_sql_helper_and_auth_tests
  duplicate_check: distinct_from_workflow_attributes_query_because_it_targets_authorization_metadata_and_sql_function_origin
  oracle_critic: ready_with_modeled_owner_roles_required_role_result_public_client_sql_and_export_import_parity
  executor_feasibility: default_profile_realistic_with_postgres_service_required_and_sqlite_as_not_applicable_classifier
```

#### Source Contract

- Frontier ID: `auth-context-sql-enqueue`.
- Rung ID: `rung-001-sql-auth-context-recovery-query`.
- Protected product promise: SQL-origin queued workflows created by
  `dbos.enqueue_workflow(...)` preserve authenticated user and roles as durable
  workflow ownership context, restore those roles when executing required-role
  workflow bodies, and expose the same metadata through runtime, DBOSClient,
  SQL status rows, recovery/relaunch, and export/import observations.
- Replay command:
  `python .workers/workloads/auth-context-sql-enqueue/auth_context_sql_enqueue_workload.py --rung rung-001-sql-auth-context-recovery-query --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `6830`, `6831`, `6832`, `6833`; every run must
  persist workflow IDs, SQL parameters, modeled user/roles, role required by
  each workflow, queue/delay options, duplicate enqueue attempt outcome,
  relaunch/recovery boundary, exported workflow status payloads, and final
  runtime/client/SQL observations.
- Invariant oracle: the independent model's `(workflow_id, origin, user,
  roles, allowed_role, queue_state, terminal_result)` tuple must match
  `DBOS.get_workflow_status`, `DBOSClient.list_workflows(user=...)`,
  direct SQL `workflow_status`, recovered execution observations, and
  export/import payloads. Forbidden-role cases must fail with
  `DBOSNotAuthorizedError` without losing modeled auth metadata.

#### Goal

Build a PostgreSQL-backed auth/context workload that exercises the SQL helper
as a first-class DBOS producer, not only as a row inserter. The workload should
compare SQL-origin workflows against Python-context and DBOSClient-origin
control rows, then push the SQL rows through queue execution, delay, duplicate
enqueue, relaunch/recovery, and export/import boundaries.

#### Workload File

- Expected path:
  `.workers/workloads/auth-context-sql-enqueue/auth_context_sql_enqueue_workload.py`.
- Create or reuse: create a new workload file. No existing frontier owns
  authorization metadata at the SQL helper boundary.
- Why a new file is justified: the workload needs direct SQL calls to the
  Postgres stored function, auth/required-role workflow definitions, public
  runtime/client list comparisons, and export/import checks. Folding this into
  workflow attributes would hide the authorization-specific oracle.

#### Workload Shape

- Type: PostgreSQL SQL-function/API scenario plus queued background workflow
  flow.
- Entry points:
  - SQL `"{schema}".enqueue_workflow(...)`
  - `DBOSContextSetAuth`, `DBOS.required_roles`, `DBOS.workflow`,
    `DBOS.enqueue_workflow`, and `DBOS.retrieve_workflow`
  - `DBOSClient.enqueue`, `DBOSClient.list_workflows`, and
    `DBOS.get_workflow_status`
  - `SystemDatabase.export_workflow` / `import_workflow` or public wrappers if
    available
  - `DBOS._recover_pending_workflows()` only after the workload creates a
    controlled pending/relaunch boundary and does not assume recovery is a
    barrier before workflow bodies can execute
- Setup:
  - Postgres required. Use the default build profile with
    `.workers/run-with-postgres.sh`.
  - Use unique workflow IDs, users, role names, queue names, and database
    artifacts per seed.
  - Store workload artifacts under `/tmp/...`, not `/workspace`.

#### Parameter Matrix

| Case | Seed | Adversarial Class | Setup | Expected Observation | Oracle |
|---|---:|---|---|---|---|
| case-001-sql-role-allowed | 6830 | permission boundary | SQL enqueue required-role workflow with `authenticated_user="alice"` and roles including required `admin` | workflow reaches `SUCCESS`; body observes user/roles and selected role; runtime/client/SQL status rows preserve auth metadata | model roles equal status/list/export rows; result contains modeled user and role |
| case-002-sql-role-denied | 6831 | denied permission / error handling | SQL enqueue same workflow with roles missing required `admin` | workflow reaches terminal authorization error without rewriting user/roles | terminal error is `DBOSNotAuthorizedError`; status/list/SQL rows still show modeled user/roles |
| case-003-delay-duplicate-relaunch | 6832 | timing/order and replay | SQL enqueue delayed workflow with auth metadata, duplicate same workflow ID, relaunch before delay promotion, then allow execution | duplicate does not alter original modeled args/auth; delayed row promotes once and completes under modeled auth | one terminal effect; auth metadata unchanged before/after relaunch and duplicate |
| case-004-export-import-client-parity | 6833 | migration/portability | complete SQL-origin auth workflow, export/import into a clean DBOS system database or isolated schema, then inspect through runtime and client | imported row preserves auth metadata and result; list-by-user returns only the modeled imported workflow | exported payload, imported status, client list, and direct SQL agree |

#### Invariants

- Must hold after every public observation: each workflow ID maps to exactly one
  modeled owner tuple `(authenticated_user, authenticated_roles)`; no observation
  returns a different user, reordered roles, invalid JSON roles, or metadata from
  another origin.
- Must hold for successful required-role cases: the workflow body observes the
  modeled user and roles, selects an allowed required role, and records exactly
  one terminal side effect.
- Must hold for denied cases: the terminal error is authorization-specific, and
  no success side effect is recorded.
- Must hold for duplicate/replay cases: a duplicate SQL enqueue with the same
  workflow ID cannot replace auth metadata, inputs, queue options, or terminal
  result.
- Must hold for relaunch/recovery: after the workload explicitly unblocks or
  allows the workflow to run, public handles and direct SQL agree on terminal
  state and auth metadata within a bounded wait.
- Must hold for export/import: exported and imported status payloads preserve
  authenticated user and roles exactly.

#### Falsification Check

Plausible bugs this rung should catch:

- The SQL helper writes `authenticated_roles` as invalid JSON or a Python-style
  string, so public status/list deserialization fails or roles do not match.
- SQL-origin rows restore `authenticated_user` but not roles into the runtime
  context, causing required-role workflows to fail after queue execution or
  recovery.
- Duplicate SQL enqueue updates the row timestamp and accidentally overwrites
  auth metadata or input with the replay attempt.
- Export/import omits auth metadata, so operators lose ownership context after
  migration or repair.
- List-by-user works for DBOSClient-origin rows but misses SQL-origin rows.

The failing assertion should identify the exact workflow ID, origin, expected
owner tuple, observed public/client/SQL values, and case seed.

#### Replay And Artifact Expectations

- Suggested command:
  `python .workers/workloads/auth-context-sql-enqueue/auth_context_sql_enqueue_workload.py --rung rung-001-sql-auth-context-recovery-query --all-cases --sequential`.
- Persist a JSON artifact per case with seed, generated names, SQL call
  parameters excluding secrets, workflow IDs, status timelines, export/import
  payload snippets for auth fields, terminal result/error, and direct SQL rows.
- Use bounded waits for delayed promotion and recovery; classify inability to
  reach the risky state as `blocked_workload` or `blocked_setup`, not green.

#### Stale Conditions

Mark this rung stale if DBOS removes or renames the PostgreSQL
`enqueue_workflow(...)` helper, changes auth metadata column names or role JSON
format, adds explicit SQL `assumed_role` semantics that need a stronger oracle,
changes workflow export/import payload shape, or makes SQL enqueue available on
non-Postgres backends. After a target refresh that includes PR `#744`, treat
the denied-role pending case as a regression guard for the filed finding rather
than as an active current-target bug.
