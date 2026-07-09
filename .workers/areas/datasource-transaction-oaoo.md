# Area: datasource-transaction-oaoo

## Current State

Current status: completed green through local and cloud validation, including
the post-target transactional send visibility rung. No product finding; a new
datasource DBAPI retry liveness rung from PR `#680` is executor-ready.

Evidence:

- `evidence-key:frontiers/datasource-transaction-oaoo/frontier.md`
- `evidence-key:runs/run-20260620T153000Z-datasource-transaction-oaoo-rung-004-bounded-seed-sweep/summary.md`
- `.workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py`
- PR `#719`: "Send in Transaction" was merged after the current target ref
  `0c41e6df...` and adds `DBOSClient.send_in_transaction`,
  `send_bulk_in_transaction`, and notification visibility tests.
- PR `#709`: "Add enqueue_in_transaction for caller-owned SQLAlchemy
  transactions" introduced the adjacent enqueue transaction helper that the
  existing passed rung covered.
- WIO cloud run `01KVVHT4C5Z3BWJW10ZJEJ0BG0` on refreshed target `99dc457`
  and harness commit `fd04500` passed all 8 transactional send invariants for
  rung `rung-005-transactional-send-visibility`.
- PR `#680`: "Retry Serialization Errors in Datasources" added datasource
  retry loops for sync/async transaction bodies on Postgres SQLSTATE `40001`
  and `40P01`, plus SQLite locked-table/database errors.

## Product Promise

Application transactions and datasource operations execute exactly once from the
user perspective while DBOS records operation outputs, retries safe failures,
and keeps application/system state consistent.

## What Not To Repeat

- Do not repeat committed transaction replay, rollback/enqueue, retry cleanup,
  or bounded OAOO sweep without a new joined surface.
- Do not mock away the database boundary; this frontier depends on real
  application/system row agreement.

## Loop-1 Search Directions

| Direction | Why It Is Deeper |
|---|---|
| Transaction plus workflow recovery | Replay once is different when recovery happens between app commit and system record observation. |
| Transaction plus queues/messages | Enqueue/send visibility across rollback and retry can expose cross-API drift. |
| Nested/caller-owned transactions | Caller-owned transaction APIs may create edge cases not covered by existing rungs. |
| Cleanup after failure | System-row cleanup and app-row rollback can diverge after repeated transient failures. |

## Rung Design Requirements

Every rung must model intent, commit/rollback, DBOS operation id, side-effect
rows, retry count, and agreement between app DB and system DB.

## Stale Conditions

Mark stale if datasource transaction APIs, operation output records, or
transactional enqueue/send semantics change.

## Rung Index

Evidence source: `evidence-key:frontiers/datasource-transaction-oaoo/rungs.index.yaml`

```yaml
schema_version: 1
columns: [id, path, status, order, level, workload_file, matrix, summary]
rungs:
  - [
      "rung-000-transaction-smoke",
      "rungs/rung-000-transaction-smoke.md",
      "passed",
      "0",
      "baseline",
      ".workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py",
      "1 case",
      "cloud and fresh local runs passed transaction/datasource setup, app DB writes, and operation record reads",
    ]
  - [
      "rung-001-transaction-replay-once",
      "rungs/rung-001-transaction-replay-once.md",
      "passed",
      "1",
      "adversarial",
      ".workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py",
      "3 cases",
      "cloud and fresh local runs passed committed transaction replay and one-side-effect invariants",
    ]
  - [
      "rung-002-rollback-enqueue-boundary",
      "rungs/rung-002-rollback-enqueue-boundary.md",
      "passed",
      "2",
      "adversarial",
      ".workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py",
      "3 cases",
      "cloud and fresh local runs passed enqueue_in_transaction visibility, commit/rollback, and pre-commit invisibility",
    ]
  - [
      "rung-003-retry-cleanup-failure",
      "rungs/rung-003-retry-cleanup-failure.md",
      "passed",
      "3",
      "failure",
      ".workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py",
      "6 cases",
      "cloud and fresh local runs passed retry, cleanup, system-row loss, and failing datasource transaction cases",
    ]
  - [
      "rung-004-bounded-seed-sweep",
      "rungs/rung-004-bounded-seed-sweep.md",
      "passed",
      "4",
      "sweep",
      ".workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py",
      "24 cases",
      "cloud and fresh local 24-case seed sweep passed; no product finding surfaced",
    ]
  - [
      "rung-005-transactional-send-visibility",
      "inline:rung-005-transactional-send-visibility",
      "passed",
      "5",
      "cross-frontier",
      ".workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py",
      "3 cases",
      "cloud run 01KVVHT4C5Z3BWJW10ZJEJ0BG0 passed transactional send visibility, rollback conservation, idempotent delivery, and enqueue+send atomicity from PR #719",
    ]
  - [
      "rung-006-datasource-dbapi-retry-liveness",
      "inline:rung-006-datasource-dbapi-retry-liveness",
      "ready",
      "6",
      "concurrency",
      ".workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py",
      "4 cases",
      "real datasource DBAPI serialization/deadlock/locked retry liveness with durable output and replay agreement from PR #680",
    ]
```

## Rung Details

These are the known rung contracts and outcomes for this frontier. Treat completed, finding, and closed rungs as current reality and design constraints, not as active executor queue rows.

### Rung: rung-000-transaction-smoke

Evidence source: `evidence-key:frontiers/datasource-transaction-oaoo/rungs/rung-000-transaction-smoke.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-transaction-smoke
frontier: datasource-transaction-oaoo
status: passed
order: 0
level: baseline
workload_file: .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py
seeds:
  - 3300
updated_at: 2026-06-20T18:20:00Z
```

#### Rung 000 Transaction Smoke

##### Run Status

- Status: passed in WIO cloud and fresh local verification.
- Evidence: `evidence-key:runs/run-20260620T142847Z-datasource-transaction-oaoo-rung-000-transaction-smoke/summary.md`.
- Fresh local verification: `evidence-key:runs/run-20260620T181600Z-datasource-transaction-oaoo-rung-000-transaction-smoke-local/results.json`.

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072709969639000Z.prompt.md`.
- Frontier ID: `datasource-transaction-oaoo`.
- Rung ID: `rung-000-transaction-smoke`.
- Protected product promise: preserve the concrete `datasource-transaction-oaoo` promise from `frontier.md` and `strategy/candidates/datasource-transaction-oaoo.md`.
- Replay command: `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-000-transaction-smoke --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree.

##### Goal

- Build and run: prove transaction/datasource setup, app DB writes, and operation record reads.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `datasource-transaction-oaoo` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: product-native setup proof.
- Entry points: DBOS transactions, datasource operations, app DB side-effect rows, operation outputs, retry counters, enqueue/send-in-transaction APIs, and cleanup rows.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | setup | transaction/datasource harness reaches app and system DB | run one committed transaction and read app/system records | one app row and one DBOS operation output are visible | app/system record oracle |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3300 | run-one-committed-transaction-and-read-app-syste | none unless case says setup block | transaction/datasource harness reaches app and system DB | app/system record oracle |


##### Invariants

- Must hold: every modeled operation is classified as committed, rolled back, retried, or rejected before DBOS state is inspected.
- Must hold: committed transaction side effects and DBOS operation outputs occur exactly once for a workflow operation id.
- Must hold: rolled-back transactions leave no app side-effect row and no visible enqueue/send effect.
- Must hold: retry/recovery never duplicates a completed transaction output or loses the final modeled result.
- Must never happen: app rows and DBOS system operation records disagree after cleanup/retry.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/datasource-transaction-oaoo.md`
  - `evidence-key:frontiers/datasource-transaction-oaoo/frontier.md`
- Suggested command family:
  - `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-000-transaction-smoke --case case-001`
  - `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-000-transaction-smoke --all-cases --sequential`
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

### Rung: rung-001-transaction-replay-once

Evidence source: `evidence-key:frontiers/datasource-transaction-oaoo/rungs/rung-001-transaction-replay-once.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-transaction-replay-once
frontier: datasource-transaction-oaoo
status: passed
order: 1
level: adversarial
workload_file: .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py
seeds:
  - 3310
  - 3311
  - 3312
updated_at: 2026-06-20T18:20:00Z
```

#### Rung 001 Transaction Replay Once

##### Run Status

- Status: passed in WIO cloud and fresh local verification.
- Evidence: `evidence-key:runs/run-20260620T142847Z-datasource-transaction-oaoo-rung-001-transaction-replay-once/summary.md`.
- Fresh local verification: `evidence-key:runs/run-20260620T181700Z-datasource-transaction-oaoo-rung-001-transaction-replay-once-local/results.json`.

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072709969639000Z.prompt.md`.
- Frontier ID: `datasource-transaction-oaoo`.
- Rung ID: `rung-001-transaction-replay-once`.
- Protected product promise: preserve the concrete `datasource-transaction-oaoo` promise from `frontier.md` and `strategy/candidates/datasource-transaction-oaoo.md`.
- Replay command: `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-001-transaction-replay-once --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree.

##### Goal

- Build and run: replay committed transaction outputs and assert side effects occur once.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `datasource-transaction-oaoo` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: DBOS transactions, datasource operations, app DB side-effect rows, operation outputs, retry counters, enqueue/send-in-transaction APIs, and cleanup rows.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | retry/idempotency | committed transaction output is replayed without duplicate side effect | rerun same workflow/operation id after committed transaction | public result replays and app row count remains one | operation output and app row count agree |
| case-002 | duplicate/replay | same operation id with changed payload does not overwrite committed output | reinvoke workflow id with different payload after commit | returned output remains first modeled value | operation output immutable |
| case-003 | timing/order | failure after app write but before workflow return replays recorded output | gate after transaction commit then interrupt/retry workflow | retry returns committed result without second app write | ledger count one and handle result agrees |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3310 | rerun-same-workflow-operation-id-after-committed | none unless case says setup block | committed transaction output is replayed without duplicate side effect | operation output and app row count agree |
| case-002 | 3311 | reinvoke-workflow-id-with-different-payload-afte | none unless case says setup block | same operation id with changed payload does not overwrite committed ou | operation output immutable |
| case-003 | 3312 | gate-after-transaction-commit-then-interrupt-ret | none unless case says setup block | failure after app write but before workflow return replays recorded ou | ledger count one and handle result agrees |


##### Invariants

- Must hold: every modeled operation is classified as committed, rolled back, retried, or rejected before DBOS state is inspected.
- Must hold: committed transaction side effects and DBOS operation outputs occur exactly once for a workflow operation id.
- Must hold: rolled-back transactions leave no app side-effect row and no visible enqueue/send effect.
- Must hold: retry/recovery never duplicates a completed transaction output or loses the final modeled result.
- Must never happen: app rows and DBOS system operation records disagree after cleanup/retry.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/datasource-transaction-oaoo.md`
  - `evidence-key:frontiers/datasource-transaction-oaoo/frontier.md`
- Suggested command family:
  - `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-001-transaction-replay-once --case case-001`
  - `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-001-transaction-replay-once --all-cases --sequential`
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

### Rung: rung-002-rollback-enqueue-boundary

Evidence source: `evidence-key:frontiers/datasource-transaction-oaoo/rungs/rung-002-rollback-enqueue-boundary.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-002-rollback-enqueue-boundary
frontier: datasource-transaction-oaoo
status: passed
order: 2
level: adversarial
workload_file: .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py
seeds:
  - 3320
  - 3321
  - 3322
updated_at: 2026-06-20T18:20:00Z
```

#### Rung 002 Rollback Enqueue Boundary

##### Run Status

- Status: passed in WIO cloud and fresh local verification.
- Evidence: `evidence-key:runs/run-20260620T143920Z-datasource-transaction-oaoo-rung-002-rollback-enqueue-boundary/summary.md`.
- Fresh local verification: `evidence-key:runs/run-20260620T181800Z-datasource-transaction-oaoo-rung-002-rollback-enqueue-boundary-local/results.json`.

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072709969639000Z.prompt.md`.
- Frontier ID: `datasource-transaction-oaoo`.
- Rung ID: `rung-002-rollback-enqueue-boundary`.
- Protected product promise: preserve the concrete `datasource-transaction-oaoo` promise from `frontier.md` and `strategy/candidates/datasource-transaction-oaoo.md`.
- Replay command: `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-002-rollback-enqueue-boundary --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree.

##### Goal

- Build and run: enqueue_in_transaction visibility, commit/rollback, and pre-commit invisibility.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `datasource-transaction-oaoo` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: DBOS transactions, datasource operations, app DB side-effect rows, operation outputs, retry counters, enqueue/send-in-transaction APIs, and cleanup rows.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | commit boundary | enqueue/send-in-transaction is invisible before commit | block before commit and poll queue/message visibility | no consumer sees uncommitted work | pre-commit visibility is empty |
| case-002 | rollback boundary | rollback removes both app write and queued/send effect | raise after enqueue/send inside transaction | no app row, queue row, or message delivery remains | rollback conservation oracle |
| case-003 | commit replay | commit then replay does not enqueue/send twice | commit transaction with enqueue/send, then retry same workflow | one downstream effect and one app row | downstream ledger count one |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3320 | block-before-commit-and-poll-queue-message-visib | none unless case says setup block | enqueue/send-in-transaction is invisible before commit | pre-commit visibility is empty |
| case-002 | 3321 | raise-after-enqueue-send-inside-transaction | none unless case says setup block | rollback removes both app write and queued/send effect | rollback conservation oracle |
| case-003 | 3322 | commit-transaction-with-enqueue-send-then-retry- | none unless case says setup block | commit then replay does not enqueue/send twice | downstream ledger count one |


##### Invariants

- Must hold: every modeled operation is classified as committed, rolled back, retried, or rejected before DBOS state is inspected.
- Must hold: committed transaction side effects and DBOS operation outputs occur exactly once for a workflow operation id.
- Must hold: rolled-back transactions leave no app side-effect row and no visible enqueue/send effect.
- Must hold: retry/recovery never duplicates a completed transaction output or loses the final modeled result.
- Must never happen: app rows and DBOS system operation records disagree after cleanup/retry.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/datasource-transaction-oaoo.md`
  - `evidence-key:frontiers/datasource-transaction-oaoo/frontier.md`
- Suggested command family:
  - `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-002-rollback-enqueue-boundary --case case-001`
  - `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-002-rollback-enqueue-boundary --all-cases --sequential`
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

### Rung: rung-003-retry-cleanup-failure

Evidence source: `evidence-key:frontiers/datasource-transaction-oaoo/rungs/rung-003-retry-cleanup-failure.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-003-retry-cleanup-failure
frontier: datasource-transaction-oaoo
status: passed
order: 3
level: failure
workload_file: .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py
seeds:
  - 3330
  - 3331
  - 3332
  - 3333
  - 3334
  - 3335
updated_at: 2026-06-20T18:20:00Z
```

#### Rung 003 Retry Cleanup Failure

##### Run Status

- Status: passed in WIO cloud and fresh local verification.
- Evidence: `evidence-key:runs/run-20260620T145900Z-datasource-transaction-oaoo-rung-003-retry-cleanup-failure/summary.md`.
- Fresh local verification: `evidence-key:runs/run-20260620T181900Z-datasource-transaction-oaoo-rung-003-retry-cleanup-failure-local/results.json`.

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072709969639000Z.prompt.md`.
- Frontier ID: `datasource-transaction-oaoo`.
- Rung ID: `rung-003-retry-cleanup-failure`.
- Protected product promise: preserve the concrete `datasource-transaction-oaoo` promise from `frontier.md` and `strategy/candidates/datasource-transaction-oaoo.md`.
- Replay command: `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-003-retry-cleanup-failure --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree.

##### Goal

- Build and run: serialization/retry/cleanup failures while application and system records must agree.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `datasource-transaction-oaoo` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: DBOS transactions, datasource operations, app DB side-effect rows, operation outputs, retry counters, enqueue/send-in-transaction APIs, and cleanup rows.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | partial failure | interrupt before operation output observation | kill/recover after app commit before result read | recovered result matches app row | no duplicate app row |
| case-002 | partial failure | interrupt after observing output before cleanup | recover while cleanup rows still exist | cleanup finishes and output remains readable | operation record/result stable |
| case-003 | partial failure | interrupt during side-effecting transaction | fail after one side effect gate | retry either commits once or rolls back completely | no partial unmodeled row |
| case-004 | retry/idempotency | recover then replay preserves committed result | recover pending workflow and invoke same workflow id | same output returned, retry count modeled | operation output immutable |
| case-005 | late result read | late handle read does not trigger re-execution | read result after cleanup delay | handle result matches operation output | side-effect count unchanged |
| case-006 | cleanup | cleanup cannot delete required committed operation output | run cleanup after committed transaction | result remains retrievable | operation output row still modeled |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3330 | kill-recover-after-app-commit-before-result-read | none unless case says setup block | interrupt before operation output observation | no duplicate app row |
| case-002 | 3331 | recover-while-cleanup-rows-still-exist | none unless case says setup block | interrupt after observing output before cleanup | operation record/result stable |
| case-003 | 3332 | fail-after-one-side-effect-gate | none unless case says setup block | interrupt during side-effecting transaction | no partial unmodeled row |
| case-004 | 3333 | recover-pending-workflow-and-invoke-same-workflo | none unless case says setup block | recover then replay preserves committed result | operation output immutable |
| case-005 | 3334 | read-result-after-cleanup-delay | none unless case says setup block | late handle read does not trigger re-execution | side-effect count unchanged |
| case-006 | 3335 | run-cleanup-after-committed-transaction | none unless case says setup block | cleanup cannot delete required committed operation output | operation output row still modeled |


##### Invariants

- Must hold: every modeled operation is classified as committed, rolled back, retried, or rejected before DBOS state is inspected.
- Must hold: committed transaction side effects and DBOS operation outputs occur exactly once for a workflow operation id.
- Must hold: rolled-back transactions leave no app side-effect row and no visible enqueue/send effect.
- Must hold: retry/recovery never duplicates a completed transaction output or loses the final modeled result.
- Must never happen: app rows and DBOS system operation records disagree after cleanup/retry.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/datasource-transaction-oaoo.md`
  - `evidence-key:frontiers/datasource-transaction-oaoo/frontier.md`
- Suggested command family:
  - `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-003-retry-cleanup-failure --case case-001`
  - `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-003-retry-cleanup-failure --all-cases --sequential`
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

Evidence source: `evidence-key:frontiers/datasource-transaction-oaoo/rungs/rung-004-bounded-seed-sweep.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-004-bounded-seed-sweep
frontier: datasource-transaction-oaoo
status: passed
order: 4
level: sweep
workload_file: .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py
seeds:
  - 3340
  - 3341
  - 3342
  - 3343
  - 3344
  - 3345
  - 3346
  - 3347
  - 3348
  - 3349
  - 3350
  - 3351
  - 3352
  - 3353
  - 3354
  - 3355
  - 3356
  - 3357
  - 3358
  - 3359
  - 3360
  - 3361
  - 3362
  - 3363
updated_at: 2026-06-20T18:20:00Z
```

#### Rung 004 Bounded Seed Sweep

##### Run Status

- Status: passed in WIO cloud and fresh local verification.
- Evidence: `evidence-key:runs/run-20260620T153000Z-datasource-transaction-oaoo-rung-004-bounded-seed-sweep/summary.md`.
- Fresh local verification: `evidence-key:runs/run-20260620T182000Z-datasource-transaction-oaoo-rung-004-bounded-seed-sweep-local/results.json`.

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072709969639000Z.prompt.md`.
- Frontier ID: `datasource-transaction-oaoo`.
- Rung ID: `rung-004-bounded-seed-sweep`.
- Protected product promise: preserve the concrete `datasource-transaction-oaoo` promise from `frontier.md` and `strategy/candidates/datasource-transaction-oaoo.md`.
- Replay command: `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-004-bounded-seed-sweep --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree.

##### Goal

- Build and run: rare-bug search across retry points, commit timing, and cleanup ordering.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `datasource-transaction-oaoo` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: DBOS transactions, datasource operations, app DB side-effect rows, operation outputs, retry counters, enqueue/send-in-transaction APIs, and cleanup rows.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | bounded sweep | commit-replay preserves the frontier oracle | generate bounded commit-replay variant from seed | case reaches commit-replay evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-002 | bounded sweep | rollback-no-effect preserves the frontier oracle | generate bounded rollback-no-effect variant from seed | case reaches rollback-no-effect evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-003 | bounded sweep | enqueue-commit preserves the frontier oracle | generate bounded enqueue-commit variant from seed | case reaches enqueue-commit evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-004 | bounded sweep | enqueue-rollback preserves the frontier oracle | generate bounded enqueue-rollback variant from seed | case reaches enqueue-rollback evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-005 | bounded sweep | retry-after-commit preserves the frontier oracle | generate bounded retry-after-commit variant from seed | case reaches retry-after-commit evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-006 | bounded sweep | cleanup-after-result preserves the frontier oracle | generate bounded cleanup-after-result variant from seed | case reaches cleanup-after-result evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-007 | bounded sweep | commit-replay preserves the frontier oracle | generate bounded commit-replay variant from seed | case reaches commit-replay evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-008 | bounded sweep | rollback-no-effect preserves the frontier oracle | generate bounded rollback-no-effect variant from seed | case reaches rollback-no-effect evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-009 | bounded sweep | enqueue-commit preserves the frontier oracle | generate bounded enqueue-commit variant from seed | case reaches enqueue-commit evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-010 | bounded sweep | enqueue-rollback preserves the frontier oracle | generate bounded enqueue-rollback variant from seed | case reaches enqueue-rollback evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-011 | bounded sweep | retry-after-commit preserves the frontier oracle | generate bounded retry-after-commit variant from seed | case reaches retry-after-commit evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-012 | bounded sweep | cleanup-after-result preserves the frontier oracle | generate bounded cleanup-after-result variant from seed | case reaches cleanup-after-result evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-013 | bounded sweep | commit-replay preserves the frontier oracle | generate bounded commit-replay variant from seed | case reaches commit-replay evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-014 | bounded sweep | rollback-no-effect preserves the frontier oracle | generate bounded rollback-no-effect variant from seed | case reaches rollback-no-effect evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-015 | bounded sweep | enqueue-commit preserves the frontier oracle | generate bounded enqueue-commit variant from seed | case reaches enqueue-commit evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-016 | bounded sweep | enqueue-rollback preserves the frontier oracle | generate bounded enqueue-rollback variant from seed | case reaches enqueue-rollback evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-017 | bounded sweep | retry-after-commit preserves the frontier oracle | generate bounded retry-after-commit variant from seed | case reaches retry-after-commit evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-018 | bounded sweep | cleanup-after-result preserves the frontier oracle | generate bounded cleanup-after-result variant from seed | case reaches cleanup-after-result evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-019 | bounded sweep | commit-replay preserves the frontier oracle | generate bounded commit-replay variant from seed | case reaches commit-replay evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-020 | bounded sweep | rollback-no-effect preserves the frontier oracle | generate bounded rollback-no-effect variant from seed | case reaches rollback-no-effect evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-021 | bounded sweep | enqueue-commit preserves the frontier oracle | generate bounded enqueue-commit variant from seed | case reaches enqueue-commit evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-022 | bounded sweep | enqueue-rollback preserves the frontier oracle | generate bounded enqueue-rollback variant from seed | case reaches enqueue-rollback evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-023 | bounded sweep | retry-after-commit preserves the frontier oracle | generate bounded retry-after-commit variant from seed | case reaches retry-after-commit evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |
| case-024 | bounded sweep | cleanup-after-result preserves the frontier oracle | generate bounded cleanup-after-result variant from seed | case reaches cleanup-after-result evidence point | transaction intent, commit/rollback decision, operation id, retry count, app side-effect rows, and DBOS operation records agree |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3340 | generate-bounded-commit-replay-variant-from-seed | none unless case says setup block | commit-replay preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-002 | 3341 | generate-bounded-rollback-no-effect-variant-from | none unless case says setup block | rollback-no-effect preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-003 | 3342 | generate-bounded-enqueue-commit-variant-from-see | none unless case says setup block | enqueue-commit preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-004 | 3343 | generate-bounded-enqueue-rollback-variant-from-s | none unless case says setup block | enqueue-rollback preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-005 | 3344 | generate-bounded-retry-after-commit-variant-from | none unless case says setup block | retry-after-commit preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-006 | 3345 | generate-bounded-cleanup-after-result-variant-fr | none unless case says setup block | cleanup-after-result preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-007 | 3346 | generate-bounded-commit-replay-variant-from-seed | none unless case says setup block | commit-replay preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-008 | 3347 | generate-bounded-rollback-no-effect-variant-from | none unless case says setup block | rollback-no-effect preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-009 | 3348 | generate-bounded-enqueue-commit-variant-from-see | none unless case says setup block | enqueue-commit preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-010 | 3349 | generate-bounded-enqueue-rollback-variant-from-s | none unless case says setup block | enqueue-rollback preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-011 | 3350 | generate-bounded-retry-after-commit-variant-from | none unless case says setup block | retry-after-commit preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-012 | 3351 | generate-bounded-cleanup-after-result-variant-fr | none unless case says setup block | cleanup-after-result preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-013 | 3352 | generate-bounded-commit-replay-variant-from-seed | none unless case says setup block | commit-replay preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-014 | 3353 | generate-bounded-rollback-no-effect-variant-from | none unless case says setup block | rollback-no-effect preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-015 | 3354 | generate-bounded-enqueue-commit-variant-from-see | none unless case says setup block | enqueue-commit preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-016 | 3355 | generate-bounded-enqueue-rollback-variant-from-s | none unless case says setup block | enqueue-rollback preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-017 | 3356 | generate-bounded-retry-after-commit-variant-from | none unless case says setup block | retry-after-commit preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-018 | 3357 | generate-bounded-cleanup-after-result-variant-fr | none unless case says setup block | cleanup-after-result preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-019 | 3358 | generate-bounded-commit-replay-variant-from-seed | none unless case says setup block | commit-replay preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-020 | 3359 | generate-bounded-rollback-no-effect-variant-from | none unless case says setup block | rollback-no-effect preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-021 | 3360 | generate-bounded-enqueue-commit-variant-from-see | none unless case says setup block | enqueue-commit preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-022 | 3361 | generate-bounded-enqueue-rollback-variant-from-s | none unless case says setup block | enqueue-rollback preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-023 | 3362 | generate-bounded-retry-after-commit-variant-from | none unless case says setup block | retry-after-commit preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |
| case-024 | 3363 | generate-bounded-cleanup-after-result-variant-fr | none unless case says setup block | cleanup-after-result preserves the frontier oracle | transaction intent, commit/rollback decision, operation id, retry count, app sid |


##### Invariants

- Must hold: every modeled operation is classified as committed, rolled back, retried, or rejected before DBOS state is inspected.
- Must hold: committed transaction side effects and DBOS operation outputs occur exactly once for a workflow operation id.
- Must hold: rolled-back transactions leave no app side-effect row and no visible enqueue/send effect.
- Must hold: retry/recovery never duplicates a completed transaction output or loses the final modeled result.
- Must never happen: app rows and DBOS system operation records disagree after cleanup/retry.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/datasource-transaction-oaoo.md`
  - `evidence-key:frontiers/datasource-transaction-oaoo/frontier.md`
- Suggested command family:
  - `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-004-bounded-seed-sweep --case case-001`
  - `python .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-004-bounded-seed-sweep --all-cases --sequential`
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

### Rung: rung-005-transactional-send-visibility

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-005-transactional-send-visibility
frontier: datasource-transaction-oaoo
status: passed
order: 5
level: cross-frontier
workload_file: .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py
seeds: [3370, 3371, 3372]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/pull/719
  - https://github.com/dbos-inc/dbos-transact-py/pull/709
  - target/dbos/_client.py
  - target/dbos/_sys_db.py
  - target/tests/test_client.py
  - .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-data-and-fixtures/overview.md
gate_results:
  surface_evidence: ready_from_pr_719_on_current_target_base_and_existing_enqueue_rung_gap
  recent_issue_pr_flake_check: pr_719_checks_passed_postgres_and_sqlite_python_3_10_through_3_14_no_flaky_failure_used
  oracle_critic: ready_with_notification_visibility_receiver_liveness_and_transaction_conservation_oracle
  executor_feasibility: default_real_postgres_profile_after_target_refresh_pinned_target_lacks_send_in_transaction
run_evidence:
  run_id: 01KVVHT4C5Z3BWJW10ZJEJ0BG0
  target_ref: 99dc457f596f31b18e8712239dd3746e226441db
  workload_commit: fd0450077fe73ef1b356833034c10ab1ab055992
  exit_code: 0
  invariant_count: 8
  has_invariant_violation: false
```

#### Source Contract

- Frontier ID: `datasource-transaction-oaoo`.
- Rung ID: `rung-005-transactional-send-visibility`.
- Protected product promise: messages sent through a caller-owned SQLAlchemy
  transaction become visible to the destination workflow only after caller
  commit, disappear completely on rollback, remain idempotent under duplicate
  keys, and compose atomically with `enqueue_in_transaction` in the same
  transaction.
- Replay command:
  `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-005-transactional-send-visibility --case <case-id>`.
- Seed policy: exact seeds `3370`, `3371`, and `3372`; every run must persist
  generated workflow IDs, topic names, idempotency keys, transaction decision,
  receiver timing, and notification/status row observations.
- Invariant oracle: caller transaction decision, public receiver result,
  `notifications` rows, workflow status rows, and modeled enqueue/send ledger
  must agree before commit, after commit, and after rollback.

#### Goal

Build the datasource transaction workload rung around PR `#719` without
repeating the passed enqueue-only rung. Historical discovery started from target
ref `0c41e6df...`, which contained `enqueue_in_transaction` from PR `#709` but
did not yet contain `send_in_transaction`. Executor later refreshed the target
to `99dc457...` and proved the transactional send surface green in cloud.

#### Execution Evidence

- Status: passed in WIO cloud.
- Run: `01KVVHT4C5Z3BWJW10ZJEJ0BG0`.
- Target ref: `99dc457f596f31b18e8712239dd3746e226441db`.
- Workload commit: `fd0450077fe73ef1b356833034c10ab1ab055992`.
- Replay:
  `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-005-transactional-send-visibility --all-cases --sequential`.
- Result: all 8 invariants passed, covering pre-commit invisibility, duplicate
  idempotency-key conservation, commit delivery, rollback no-delivery plus
  fallback liveness, and enqueue plus send atomic commit/rollback.

#### Workload Shape

- Type: Postgres stateful API/client workload.
- Build profile: `default` with real Postgres through
  `.workers/run-with-postgres.sh`; SQLite is setup-only evidence because the
  oracle depends on transaction isolation and notification rows.
- Entry points:
  - `DBOSClient.send_in_transaction`
  - `DBOSClient.send_bulk_in_transaction`
  - `DBOSClient.enqueue_in_transaction`
  - `DBOS.start_workflow`, workflow handle result retrieval, and read-only
    `notifications` / `workflow_status` observations.
- Existing coverage comparison:
  - `rung-002-rollback-enqueue-boundary` passed enqueue commit/rollback and
    pre-commit invisibility. Its workload records
    `send_in_transaction_api: not present in DBOSClient/DBOS public API for target commit`.
  - `message-event-cancellation` passed duplicate-send, timeout, fallback,
    fork, stream, and replay cases, but not caller-owned SQLAlchemy transaction
    visibility for notifications.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 3370 | send-commit-wakes-blocked-receiver | start receiver workflow waiting on a topic, call `send_in_transaction` inside a caller-owned transaction, poll from another connection before commit, then commit | notification row and receiver result are invisible before commit; after commit the receiver returns exactly the modeled message |
| case-002 | 3371 | send-rollback-no-delivery | start receiver workflow, call `send_in_transaction`, roll back, assert notification absence, then send a fallback normal message | rolled-back message never wakes the receiver or leaves a notification row; fallback delivery proves receiver liveness without masking rollback leakage |
| case-003 | 3372 | enqueue-plus-send-same-transaction | in one caller-owned transaction enqueue a receiver workflow and send it a message, inspect from another connection before commit, then commit; repeat rollback branch with distinct IDs | workflow row and notification are invisible before commit, both appear atomically after commit, and neither survives rollback |

#### Invariants

- Must hold:
  - Pre-commit observations from a separate connection see no notification row,
    no receiver result, and no queued workflow row for the modeled transaction.
  - Commit makes exactly one modeled notification visible and the receiver
    returns the modeled payload once.
  - Rollback leaves no modeled notification row, no queued/send effect, and no
    stale receiver completion from the rolled-back payload.
  - Duplicate idempotency keys in the same transaction produce one delivery per
    modeled destination.
  - Enqueue plus send in one transaction is atomic: a receiver workflow cannot
    observe a message before its own workflow row commits, and rollback removes
    both.
- Must never happen:
  - The workload treats absence of `send_in_transaction` at target ref
    `0c41e6df...` as a DBOS product finding.
  - The workload relies only on handle completion; it must inspect notification
    and workflow rows at the transaction boundaries.

#### Expected Signatures

- Success: all cases reach the modeled transaction window, pre-commit
  invisibility holds, commit delivers exactly once, rollback leaves no durable
  send/enqueue effect, and public receiver results agree with the ledger.
- Finding: uncommitted notification visibility, rolled-back notification
  delivery, receiver hang after rollback fallback, duplicate idempotent
  delivery, enqueue/send split-brain across commit or rollback, or row/result
  disagreement after terminal completion.
- Setup block: target checkout does not expose `DBOSClient.send_in_transaction`,
  Postgres setup cannot provide transaction isolation, or the receiver timing
  window cannot be reached without product source edits.
- Low signal: workload only reruns PR `#719` product tests, checks command
  completion, or repeats enqueue-only visibility from rung 002.

## Oracle Contract

The oracle is a transaction ledger checked at three observation points:
pre-commit from a separate connection, post-commit or post-rollback durable
rows, and public receiver/handle results. The workload must pair notification
row counts with receiver liveness so a rollback pass cannot hide a stranded
receiver and a successful receiver cannot hide an uncommitted row leak.

## Stale Conditions

Mark stale if DBOS changes caller-owned transaction helper semantics,
notification schema/idempotency behavior, or target ref advances past PR `#719`
and this rung needs to be reframed from stale-drift discovery to
regression-proof.

### Rung: rung-006-datasource-dbapi-retry-liveness

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-006-datasource-dbapi-retry-liveness
frontier: datasource-transaction-oaoo
status: ready
order: 6
level: concurrency
workload_file: .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py
seeds: [6800, 6801, 6802, 6803]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/pull/680
  - https://github.com/dbos-inc/dbos-transact-py/issues/679
  - target/dbos/_datasource.py
  - target/dbos/_datasource_postgres.py
  - target/dbos/_datasource_sqlite.py
  - target/tests/test_datasource.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/risk-based-testing/overview.md
gate_results:
  surface_evidence: ready_from_pr_680_datasource_retry_loop_for_dbapi_serialization_deadlock_and_sqlite_lock_errors
  duplicate_check: existing_datasource_rung_003_covers_cleanup_replay_but_not_real_dbapi_concurrency_retry_liveness
  product_test_gap: target_tests_inject_operationalerror_and_do_not_force_real_concurrent_40001_40P01_or_sqlite_lock_windows
  oracle_critic: ready_with_attempt_ledger_durable_app_rows_datasource_outputs_replay_and_bounded_liveness
```

#### Source Contract

- Frontier ID: `datasource-transaction-oaoo`.
- Rung ID: `rung-006-datasource-dbapi-retry-liveness`.
- Protected product promise: datasource transaction retry loops must treat
  retryable DBAPI concurrency failures as transient, retry the datasource body
  with a fresh DBOS datasource session, record only the eventual success in
  `datasource_outputs`, and preserve exactly-once app side effects and replay.
- Replay command:
  `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-006-datasource-dbapi-retry-liveness --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `6800`, `6801`, `6802`, and `6803`; every run must
  persist generated workflow IDs, datasource backend, conflict rows, lock timing
  windows, retry attempt counts, datasource step IDs, output/error rows, and
  replay observations.
- Invariant oracle: attempt ledger, app table rows, public workflow result,
  `datasource_outputs`, and replay call counts agree after the conflict,
  retry, terminal completion, and second invocation with the same workflow ID.

#### Goal

Exercise PR `#680` beyond the product tests by forcing real database
concurrency failures instead of raising a synthetic `OperationalError` inside
the user function. The workload must prove both liveness and exactly-once
durability: retryable DBAPI failures are not recorded as terminal datasource
errors, non-retryable DBAPI errors still are, and no retry attempt leaks a
stale datasource session or duplicate app mutation.

#### Workload Shape

- Type: Postgres and SQLite stateful datasource workload.
- Build profile: default real Postgres through `.workers/run-with-postgres.sh`;
  SQLite case may create a temporary SQLite datasource while DBOS itself still
  uses the harness system database.
- Entry points:
  - `SQLAlchemyDatasource.run_tx_step`
  - `AsyncSQLAlchemyDatasource.run_tx_step_async`
  - `sql_session()` inside retrying datasource transaction bodies
  - read-only `datasource_outputs` and modeled app rows after completion
- Existing coverage comparison:
  - `rung-003-retry-cleanup-failure` covers datasource cleanup/replay after
    partial failures, but not retryable DBAPI classification or real database
    concurrency windows.
  - Target tests `test_sync_ds_retries_on_serialization_error` and
    `test_async_ds_retries_on_serialization_error` inject Postgres
    `SerializationFailure`; they do not create a real concurrent serialization
    conflict, deadlock, SQLite lock, or stale-session retry oracle.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 6800 | sync-postgres-serializable-write-conflict | two sync datasource workflows update the same row under `SERIALIZABLE`; one transaction must hit SQLSTATE `40001` and retry | both workflows complete, final app value equals two modeled commits, loser attempt count is greater than one, and each workflow has one success output row and no recorded serialization error |
| case-002 | 6801 | async-postgres-deadlock-retry | two async datasource workflows lock two rows in opposite order to force SQLSTATE `40P01` for one participant | deadlock loser retries with a fresh async datasource session, both results return, row updates are exactly once, and replay does not re-execute |
| case-003 | 6802 | async-postgres-nonretryable-dbapi | async datasource body executes a syntax-error statement after a marker attempt | non-retryable DBAPI error records once, public replay raises the same stored error without another attempt, and no retry sleep/span event is required |
| case-004 | 6803 | sqlite-locked-datasource-retry | hold an external SQLite write lock until the first datasource attempt observes a locked-database/table error, then release within the bounded retry budget | datasource retries after lock release, records one success output, and no stale SQLite session leaks into replay |

#### Invariants

- Must hold:
  - Retryable Postgres `40001` / `40P01` and SQLite locked errors are not
    recorded as terminal `datasource_outputs.error` rows.
  - The eventual successful attempt records exactly one datasource output per
    workflow step, and replay with the same workflow ID returns that output
    without executing the datasource body again.
  - App-table mutations reflect committed modeled attempts only; rolled-back
    conflict/deadlock attempts leave no durable partial row.
  - After a retryable DBAPI failure, `sql_session()` points at a live fresh
    transaction/session for the next attempt.
  - Case timings stay bounded; retry backoff liveness is part of the oracle.
- Must never happen:
  - A synthetic raised `OperationalError` is the only exercised retry signal.
  - A non-retryable DBAPI error is retried until success or overwritten by a
    later successful output.
  - Replay hides a duplicated app mutation, stale session, or terminal error
    row written by an earlier retry attempt.

#### Expected Signatures

- Success: all cases reach their modeled retry/non-retry window, terminal
  app rows and datasource output rows match the model, replay performs no new
  datasource body execution, and retry liveness stays within the bound.
- Finding: recorded retryable DBAPI error, duplicate app mutation, lost
  successful output, replay re-execution, stale datasource session after retry,
  non-retryable error incorrectly retried, or bounded liveness failure.
- Setup block: reliable real conflict/deadlock/lock windows cannot be reached
  in the allowed harness without product source edits or unbounded timing.
- Low signal: the workload only reruns PR `#680` unit tests, injects
  `OperationalError` directly for the positive cases, or checks command
  completion without durable row/replay oracles.
