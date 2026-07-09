# Area: system-db-retry-idempotence

## Current State

Current status: new area opened from target commit `3df88c4` / PR `#740`;
one executor-ready rung is queued in work item `E-016`.

Recent issue/PR scan used for this area:

- PR `#740`: "Resilience Improvements" fixed two retry/idempotence issues:
  database connection retry could increment `function_id` multiple times, and
  `run_step_async` could block the event loop when invoked with a sync step.
- The same PR added focused product tests for `recv_consume`,
  `record_child_workflow`, and `record_get_result` retry/idempotence behavior.
- Existing workload areas cover stored retry-class errors, datasource
  transaction OAOO, message/stream delivery, async checkpoint ordering, and
  recovery queue ownership. They do not cover sys-db retry re-entry after a
  committed internal operation row or notification consume.

## Product Promise

DBOS system database retry loops preserve exactly-once durable semantics when a
connection failure causes retry re-entry after a write may already have
committed. Message receives, child workflow recording, and implicit child
`get_result` checkpoints must not duplicate effects, skip checkpoint slots,
consume additional messages, or misclassify idempotent replays as
nondeterministic workflow conflicts.

## Why This Matters

DBOS trades availability for correctness during transient database connection
failures. If a retry loop re-enters an internal sys-db operation after the first
attempt committed, users can see duplicated child edges, consumed-but-lost
messages, corrupted workflow step order, stuck parent workflows, or conflict
errors for a replay that should be idempotent. Those failures are hard to
minimize from production because the visible symptom usually appears later
during recovery, replay, or handle retrieval.

## Evidence

- Code:
  - `target/dbos/_sys_db.py`: `db_retry`, `record_get_result`,
    `record_child_workflow`, and `recv_consume`.
  - `target/dbos/_core.py`: child workflow invocation records implicit
    `DBOS.getResult` rows in the caller context and records child workflow
    edges before execution.
  - `target/dbos/_utils.py`: retry classification for Postgres and SQLite
    connection failures.
- Tests:
  - `target/tests/test_failures.py::test_recv_consume_idempotent_on_db_retry`
    proves a direct second `recv_consume` call returns the same recorded
    message and leaves the other message unconsumed.
  - `target/tests/test_failures.py::test_recv_consume_idempotent_on_timeout`
    proves a no-message receive records `None` and does not consume a later
    message on replay.
  - `target/tests/test_failures.py::test_record_child_workflow_idempotent_on_db_retry`
    proves same-child replay is idempotent, different-child replay conflicts,
    and empty child IDs are rejected.
  - `target/tests/test_failures.py::test_record_get_result_increments_function_id_once_on_db_retry`
    injects a retry around `record_get_result` and checks `function_id`
    advances exactly once.
- Existing workloads/runs:
  - `serialization-error-fidelity` / `E-010` covers stored DBAPI errors in
    child/result retrieval paths, not retry re-entry after committed sys-db
    writes.
  - `message-event-cancellation` / `E-012` covers stream/listener delivery and
    resume offsets, not `recv_consume` idempotence after a retry.
  - `async-checkpoint-determinism` / `E-004` covers async checkpoint ordering,
    child lineage, and cancellation, not committed sys-db write replay.
  - `recovery-db-faults` / `E-002` covers stale queued recovery ownership, not
    per-operation retry idempotence.
- Recent churn:
  - `3df88c4bcc3a2b73d91b765b459f4c7beb6a690c` / PR `#740` changed these exact
    sys-db retry paths and added focused tests.

## What Not To Repeat

- Do not only rerun the new product tests from PR `#740`; the generated
  workload must compose multiple public workflow paths and verify the durable
  row model around retry re-entry.
- Do not classify a generic stored DBAPI error retrieval hang as this area;
  that belongs to `serialization-error-fidelity`.
- Do not assume `recover_pending_workflows()` is a barrier. If recovery is used
  for a future rung, await handles or public terminal state explicitly.
- Do not mark a retry case green unless the workload proves the fault was
  injected after the risky operation point or uses an explicit replay surrogate
  with equivalent committed-state evidence.

## Adversarial Model

The area attacks the assumption that every `db_retry` body is safe to execute
again after an earlier attempt may have committed. The modeled dependency fault
is a transient retriable database connection failure at, or immediately after,
the sys-db write boundary. The workload should use deterministic retry
injection or an equivalent replay call after durable evidence exists, then
check independent ledgers rather than only handle completion.

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
      "read-only:target/tests/test_failures.py",
      "4 existing product tests",
      "read-only evidence for PR #740 focused retry/idempotence fixes",
    ]
  - [
      "rung-001-committed-sysdb-retry-reentry",
      "inline:rung-001-committed-sysdb-retry-reentry",
      "ready",
      "1",
      "resilience",
      ".workers/workloads/system-db-retry-idempotence/system_db_retry_idempotence_workload.py",
      "4 cases",
      "compose committed sys-db retry re-entry across recv consume, recv timeout, child edge recording, and implicit child get_result checkpoint rows",
    ]
```

## Rung Details

### Rung: rung-000-product-regression-baseline

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-product-regression-baseline
frontier: system-db-retry-idempotence
status: not_run_optional
order: 0
level: baseline
workload_file: read-only:target/tests/test_failures.py
seeds: [0]
updated_at: 2026-06-24T00:00:00Z
```

#### Source Contract

- Frontier ID: `system-db-retry-idempotence`.
- Rung ID: `rung-000-product-regression-baseline`.
- Protected product promise: the narrow PR `#740` regression tests pass on the
  target evidence ref.
- Replay command: optional read-only product pytest selection:
  `pytest target/tests/test_failures.py -k 'recv_consume_idempotent_on_db_retry or recv_consume_idempotent_on_timeout or record_child_workflow_idempotent_on_db_retry or record_get_result_increments_function_id_once_on_db_retry'`.
- Seed policy: fixed seed `0`.
- Invariant oracle: the selected product tests pass at the target evidence ref
  or at an explicitly refreshed target ref.

### Rung: rung-001-committed-sysdb-retry-reentry

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-committed-sysdb-retry-reentry
frontier: system-db-retry-idempotence
status: ready
order: 1
level: resilience
workload_file: .workers/workloads/system-db-retry-idempotence/system_db_retry_idempotence_workload.py
seeds: [7400, 7401, 7402, 7403]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - target/dbos/_sys_db.py
  - target/dbos/_core.py
  - target/dbos/_utils.py
  - target/tests/test_failures.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/resilience-testing-and-fault-injection/overview.md
gate_results:
  surface_evidence: ready_from_pr_740_and_target_sys_db_retry_paths
  duplicate_check: existing_workloads_cover_result_error_retrieval_async_checkpoint_and_message_streams_not_committed_sysdb_retry_reentry
  oracle_critic: ready_with_independent_operation_output_notification_child_and_function_id_ledger
  executor_feasibility: default_profile_real_postgres_with_sqlite_optional; no_external_service_needed
```

#### Source Contract

- Frontier ID: `system-db-retry-idempotence`.
- Rung ID: `rung-001-committed-sysdb-retry-reentry`.
- Protected product promise: committed sys-db writes remain idempotent when a
  retry re-enters the same logical DBOS operation after a transient database
  connection failure.
- Replay command:
  `python .workers/workloads/system-db-retry-idempotence/system_db_retry_idempotence_workload.py --rung rung-001-committed-sysdb-retry-reentry --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `7400`, `7401`, `7402`, `7403`; every run must
  persist workflow IDs, function IDs, child IDs, sent messages, consumed
  message UUIDs/payloads, injected retry points, status rows, operation output
  rows, notification consumed flags, and public handle observations.
- Invariant oracle: a simple independent ledger tracks one logical DBOS
  operation per function ID. Public result/status, `operation_outputs`,
  `notifications`, and child workflow rows must agree with that ledger after
  retry re-entry and after relaunch/retrieval when the case uses it.

#### Goal

Build one workload that turns PR `#740` focused regression cases into a
realistic DBOS workflow/session resilience scenario. The workload should prove
that retry re-entry preserves exactly-once durable semantics across receive,
child workflow, and result-recording paths that real applications compose.

#### Workload File

- Expected path:
  `.workers/workloads/system-db-retry-idempotence/system_db_retry_idempotence_workload.py`.
- Create or reuse: create a new file. Existing workload files do not share this
  sys-db retry-injection shape or oracle family.
- Why one file is enough: the cases share DBOS launch/setup, retry injection,
  read-only system table inspection, workflow/result handles, and the same
  operation ledger oracle.

#### Workload Shape

- Type: product-runtime resilience workload with scoped deterministic DB fault
  injection and read-only system table inspection.
- Build profile: `default`.
- Setup: real DBOS runtime with Postgres; SQLite may be an optional local
  secondary check but must not replace the Postgres cloud run.
- Entry points:
  - `DBOS.workflow`, `DBOS.step`, `DBOS.send`, `DBOS.recv`,
    `DBOS.start_workflow`, workflow handle `get_result`, and `DBOS.retrieve_workflow`.
  - Read-only inspection of `SystemSchema.operation_outputs`,
    `SystemSchema.notifications`, and `SystemSchema.workflow_status`.
  - If needed for deterministic injection, a scoped engine/proxy wrapper around
    the target `SystemDatabase.engine.begin()` that fires only for the selected
    case and thread, then restores the original engine.
- Fault model: one retriable DB connection failure after durable state has been
  written or immediately before retry observes already-written state. The case
  must record whether it used direct second-call replay or engine-fault
  injection and why that preserves the committed-state risk.

#### Parameter Matrix

| Case | Seed | Scenario | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 7400 | receive-two-messages-committed-retry | consume one message, replay the same logical `DBOS.recv` function ID after durable output exists | same message is returned; the second message remains unconsumed; one `DBOS.recv` operation row exists |
| case-002 | 7401 | receive-timeout-then-late-message | record `None` for an empty receive, send a late message, replay the same function ID | replay returns recorded `None`; late message remains unconsumed; no duplicate operation row appears |
| case-003 | 7402 | child-edge-committed-retry | record child workflow edge, then replay same parent/function/child and also attempt different child at same function ID | same-child replay is idempotent; different-child replay raises conflict; durable child row is unchanged |
| case-004 | 7403 | implicit-get-result-function-id-retry | parent invokes child and records implicit `DBOS.getResult` while retry injection fires once in `record_get_result` | parent context advances exactly one function ID; one `DBOS.getResult` row exists; parent/child public results and child row agree after retrieval/relaunch |

#### Invariants

- Must hold for receive cases:
  - The workload proves the risky state was reached: at least one durable
    `operation_outputs` row exists for `DBOS.recv` before replay or retry
    observation.
  - Re-entering the same workflow/function ID returns the recorded value, not a
    newly consumed message.
  - `notifications.consumed` flags match the independent message ledger: exactly
    one original message consumed for case-001, no late message consumed for
    case-002.
  - There is exactly one operation row for the receive function ID.
- Must hold for child edge case:
  - Same parent/function/child replay leaves one child row and no conflict.
  - Different child at the same parent/function ID raises
    `DBOSWorkflowConflictIDError` or an equivalent public conflict
    classification.
  - Empty child ID is rejected before a durable row can be written.
- Must hold for implicit get-result case:
  - The injected retry fires exactly once at the intended boundary.
  - Parent function ID advances exactly once for the implicit `DBOS.getResult`
    checkpoint.
  - Exactly one `DBOS.getResult` row exists for that parent/function ID and it
    references the modeled child workflow ID.
  - Parent and child public handles, durable statuses, and operation rows agree
    after bounded retrieval.
- Must never happen:
  - A case passes without proving either deterministic retry injection or an
    equivalent committed-state replay surrogate.
  - A workload timeout is classified as a product finding without durable row
    evidence showing which invariant failed.
  - The workload writes artifacts under `/workspace`; any replay artifacts must
    go under `/tmp/...`.

#### Expected Signatures

- Success: all four cases satisfy the operation-output, notification,
  child-edge, function-id, and public handle invariants.
- Finding: duplicate or skipped `function_id`, consumed late/second message,
  duplicate operation row, same-child conflict, different-child non-conflict,
  empty-child durable row, parent/child public-durable mismatch, or retry
  injection that causes unbounded handle polling with durable evidence.
- Setup block: the workload cannot launch isolated DBOS runtime with Postgres,
  cannot restore the scoped engine/proxy, or cannot inspect system tables.
- Low signal: the workload only calls the PR `#740` unit tests or only asserts
  that no exception occurred.

## Oracle Contract

The oracle is an independent logical-operation ledger keyed by workflow ID,
function ID, and operation name. It models the expected consumed message set,
child edge set, and get-result checkpoint set before checking DBOS public APIs
and read-only system rows. The oracle must fail for duplicate effects, skipped
checkpoint positions, wrong conflict classification, or public/durable
disagreement. It must not compute expected state by replaying DBOS production
logic.

## Stale Conditions

Mark stale if DBOS removes or materially changes `db_retry`, `record_get_result`,
`record_child_workflow`, `recv_consume`, the `operation_outputs` schema, the
notification consume model, child workflow row semantics, or target ref advances
past PR `#740` with a new resilience change that changes the retry contract.
