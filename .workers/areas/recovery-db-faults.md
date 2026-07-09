# Area: recovery-db-faults

## Current State

Current status: completed with one confirmed loop-1 finding candidate from
stale concurrent queued recovery snapshots. The finding was filed upstream as
issue `#742`; PR `#744` is open and proposes the recovery-path fix.

Important closed candidate: `recovery-db-faults-gate-timeout-after-recovery`
was closed as `closed_workload_model_artifact`. The workload assumed
`recover_pending_workflows()` returns all handles before recovered workflow code
can execute. DBOS does not document or implement that barrier.

Evidence:

- `evidence-key:frontiers/recovery-db-faults/frontier.md`
- `evidence-key:findings/recovery-db-faults-gate-timeout-after-recovery.md`
- `evidence-key:runs/run-20260620T080100Z-recovery-db-faults-rung-005-finding-minimization/summary.md`
- `.workers/runs/E-002.md`
- https://github.com/dbos-inc/dbos-transact-py/issues/742
- https://github.com/dbos-inc/dbos-transact-py/pull/744

## Product Promise

Postgres-checkpointed workflows resume from completed steps after database,
executor, or recovery interruption without duplicate effects, stranded pending
rows, missing handles, or incorrect terminal state.

## What Not To Repeat

- Do not assume recovery handle collection is a global execution barrier.
- Do not promote gate timeout as a DBOS bug unless the workload explicitly
  unblocks recovered workflows under a defensible contract.
- Do not use SQLite for this frontier.

## Loop-1 Search Directions

| Direction | Why It Is Deeper |
|---|---|
| Recovery plus queue ownership | Existing recovery rungs focused workflow rows; queued recovery and queue assignment clearing can expose different durable-state bugs. |
| Multi-executor recovery race | Two recoverers or stale executor IDs can test ownership and idempotency without relying on the invalid barrier assumption. |
| DB reconnect around result retrieval | Result retrieval after recovery is a user-facing promise distinct from body execution. |
| Recovery plus lifecycle command | Cancel/resume/delete/fork during recovery joins two established frontier surfaces. |
| Concurrent queued recovery snapshots | Two recoverers can capture the same queued `PENDING` row before one clears assignment; a stale second recovery must not execute the queued workflow outside queue ownership. |

## Rung Design Requirements

Every new rung must state when application gates open, who owns Postgres, what
DBOS recovery contract is being asserted, and how terminal state is observed.
For queued recovery rungs, also state which executor owns each row before and
after `clear_queue_assignment`, whether queue workers or recovery workers may
run the body, and how stale recovery snapshots are detected.

## Stale Conditions

Mark stale if DBOS changes `recover_pending_workflows()` semantics, recovery
handle behavior, `clear_queue_assignment` behavior, queue worker ownership, or
the system DB schema used by pending workflow recovery. After a target refresh
that includes PR `#744`, treat rung 006 as a regression guard for the filed
finding rather than as an active current-target bug.

## Rung Index

Evidence source: `evidence-key:frontiers/recovery-db-faults/rungs.index.yaml`

```yaml
schema_version: 1
columns: [id, path, status, order, level, workload_file, matrix, summary]
rungs:
  - [
      "rung-000-product-harness-baseline",
      "rungs/rung-000-product-harness-baseline.md",
      "passed",
      "0",
      "baseline",
      "read-only:/Users/viswa/code/workers/dbos-transact-py/tests/test_failures.py",
      "1 case",
      "prove DBOS Postgres recovery test harness runs before new workload code",
    ]
  - [
      "rung-001-recovery-db-restart-single-window",
      "rungs/rung-001-recovery-db-restart-single-window.md",
      "passed",
      "1",
      "adversarial",
      ".workers/workloads/recovery-db-faults/recovery_db_faults_workload.py",
      "3 cases",
      "small DB restart during explicit recovery with pending-row and step-count oracle",
    ]
  - [
      "rung-002-recovery-window-matrix",
      "rungs/rung-002-recovery-window-matrix.md",
      "passed",
      "2",
      "adversarial",
      ".workers/workloads/recovery-db-faults/recovery_db_faults_workload.py",
      "6 cases",
      "expand across scan, queue-clear, execute, and result-retrieve windows",
    ]
  - [
      "rung-003-replay-dlq-liveness",
      "rungs/rung-003-replay-dlq-liveness.md",
      "passed",
      "3",
      "failure",
      ".workers/workloads/recovery-db-faults/recovery_db_faults_workload.py",
      "8 cases",
      "repeat recovery faults until terminal success or DLQ without duplicate completed effects",
    ]
  - [
      "rung-004-bounded-seed-sweep",
      "rungs/rung-004-bounded-seed-sweep.md",
      "finding_candidate",
      "4",
      "sweep",
      ".workers/workloads/recovery-db-faults/recovery_db_faults_workload.py",
      "24 cases",
      "case-011 found parsed invariant failure after 13 accepted cases; move to minimization",
    ]
  - [
      "rung-005-finding-minimization",
      "rungs/rung-005-finding-minimization.md",
      "closed_workload_model_artifact",
      "5",
      "adversarial",
      ".workers/workloads/recovery-db-faults/recovery_db_faults_workload.py",
      "7 cases",
      "minimized rung-004 case-011 to five workflows, 250ms outage, zero restart offset; closed as workload-model artifact because the oracle assumed recovery barrier semantics",
    ]
  - [
      "rung-006-concurrent-queued-recovery-ownership",
      "inline:loop-1-added-rung-rung-006-concurrent-queued-recovery-ownership",
      "finding_candidate",
      "6",
      "cross-frontier",
      ".workers/workloads/recovery-db-faults/recovery_db_faults_workload.py",
      "3 cases",
      "case-001 cloud replay confirmed stale recoverer executes queued workflow body after another recoverer already cleared queue assignment",
    ]
```

## Rung Details

These are the known rung contracts and outcomes for this frontier. Treat completed, finding, and closed rungs as current reality and design constraints, not as active executor queue rows.

### Loop-1 Added Rung: rung-006-concurrent-queued-recovery-ownership

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-006-concurrent-queued-recovery-ownership
frontier: recovery-db-faults
status: finding_candidate
order: 6
level: cross-frontier
workload_file: .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py
seeds: [224, 225, 226]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - target/dbos/_recovery.py
  - target/dbos/_queue.py
  - target/dbos/_sys_db.py
  - target/tests/test_queue.py
  - .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py
  - .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/resilience-testing-and-fault-injection/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-feedback-loops/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-data-and-fixtures/overview.md
```

#### Product Promise

A queued workflow that was dequeued by a dead executor must remain owned by its
queue during recovery. Concurrent or stale recovery attempts for the same
queued `PENDING` row may clear assignment, return polling handles, or observe
that another recoverer already cleared it, but they must not execute the queued
workflow body through direct recovery, bypass queue concurrency, duplicate
ledger effects, strand the row under the dead executor, or make public handle
result retrieval disagree with durable terminal state.

#### Why This Is New

Completed recovery rung 002 covered one queued `restart-during-queue-clear`
window and proved a queued row can return to `ENQUEUED` or terminal after a
fault. It did not model two recoverers acting on the same stale pending snapshot
after one recoverer has already cleared queue assignment. The current target
path in `dbos/_recovery.py` returns a polling handle when
`clear_queue_assignment()` succeeds; if `clear_queue_assignment()` returns
false, the code falls through to `execute_workflow_by_id(...)`. This rung
attacks whether that stale-snapshot fall-through can run queue-owned work
outside the queue worker, while queue-composed controls only covered ordinary
enqueue/relaunch result durability.

This rung does not assume `recover_pending_workflows()` is a barrier. Gates must
open independently of recovery handle collection, and assertions are made on
durable rows, the independent ledger, queue worker ownership, and handle
results after explicitly allowing progress.

#### Workload Shape

- Type: background-job resilience/stateful workload.
- Build profile: `default`.
- Runtime setup: real Postgres through `.workers/run-with-postgres.sh`; SQLite is
  not meaningful.
- Expected workload file: reuse
  `.workers/workloads/recovery-db-faults/recovery_db_faults_workload.py` unless
  the executor determines a separate file is needed to keep stale-snapshot
  orchestration readable.
- Queue configuration for all cases:
  - Queue name: `wio_recovery_race_<case_id>_<seed>`.
  - `concurrency=1`.
  - `worker_concurrency=1`.
  - `polling_interval_sec=0.05`.
- Executor model:
  - `wio-worker-a` dequeues queued work and dies while each modeled row is
    `PENDING`.
  - `wio-recoverer-b` and `wio-recoverer-c` both operate on the same captured
    `GetPendingWorkflowsOutput` snapshot, either by concurrent
    `DBOS._recover_pending_workflows([wio-worker-a])` calls with a synchronization
    hook after `get_pending_workflows`, or by an equivalent harness wrapper that
    calls the public recovery path with duplicated stale pending rows.
  - A healthy queue worker under `wio-worker-d` drains rows only after the
    recovery race has reached the modeled post-clear state.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
|---|---:|---|---|---|---|
| case-001 | 224 | dual-recoverer-one-queued-row | two recoverers capture the same single queued `PENDING` row; recoverer B clears assignment before recoverer C uses the stale snapshot | 1 queued workflow, one blocking gate, one ledgered body effect | stale recoverer must not direct-execute queue-owned work or duplicate the effect |
| case-002 | 225 | dual-recoverer-batch-concurrency | two recoverers race over the same three queued `PENDING` rows while queue concurrency is 1 | 3 queued workflows, each blocks after ledger start, queue worker released sequentially | no overstart, no duplicate body effect, all rows terminal or explained |
| case-003 | 226 | db-reconnect-after-clear-before-drain | recoverer B clears assignment, Postgres restarts before queue worker drains, recoverer C retries with stale snapshot after DB returns | 2 queued workflows, 750ms owned Postgres restart, handle retrieval after drain | no dead-executor pending row, queue/result retrieval agree after reconnect |

#### Replay Commands

- Single case:
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py --rung rung-006-concurrent-queued-recovery-ownership --case case-001`
- Full matrix:
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py --rung rung-006-concurrent-queued-recovery-ownership --all-cases --sequential`

#### Required Artifacts

Each case must write seed, derived workflow IDs, queue configuration, executor
IDs, captured pending snapshots per recoverer, clear-assignment attempts and
row counts, status snapshots before recovery, after clear, after stale
recoverer attempt, after queue drain, and after cleanup, public recovery handles,
original handles, retrieved handles, ledger rows with executor/thread marker and
start/finish timestamps, `list_queued_workflows(queue_name=...)` snapshots plus
executor-filtered queued-list snapshots for `wio-worker-a` and `wio-worker-d`
after clear and after redequeue, Postgres restart timestamps when applicable,
product commit, and redacted Postgres URLs.

#### Invariants

- Must hold: after the recovery race, no modeled row remains `PENDING` with
  `executor_id == wio-worker-a`.
- Must hold: a stale recoverer for a queued row does not create a second ledger
  start or finish for that workflow.
- Must hold: every modeled workflow body start is attributed to the modeled
  queue-drain executor/window, not to the stale recovery executor/window.
- Must hold: queue concurrency of 1 is respected after assignment is cleared,
  including batch case starts.
- Must hold: every modeled workflow reaches `SUCCESS` or an explicitly
  documented terminal/DLQ state after a healthy queue-drain window.
- Must hold: public handle results from original handles, recovery handles, and
  retrieved handles agree with the durable terminal row.
- Must hold: after terminal completion and cleanup polling, no active
  `ENQUEUED`, `DELAYED`, or `PENDING` rows remain for the modeled queue.
- Diagnostic only unless anchored in docs or API contract: stale
  `list_queued_workflows(..., executor_id=wio-worker-a)` membership after
  `clear_queue_assignment()` should be recorded as queue ownership evidence, but
  should not be promoted alone if DBOS treats `executor_id` as last executor
  rather than current owner.
- Must never happen: the workload passes by treating a missing recovery handle
  as success, by relying on recovery handle collection as a barrier, or by
  checking only that some terminal state exists without ledger and queue-owner
  evidence.

#### Producer Gate Notes

- Oracle gate: passed for queue ownership, duplicate-effect, queue-concurrency,
  terminal-state, and handle-result invariants. The oracle does not depend on
  `recover_pending_workflows()` being a barrier.
- Feasibility gate: passed against the current recovery workload style. Existing
  recovery rungs already wrap `get_pending_workflows`, `_recover_workflow`, and
  `clear_queue_assignment`; this rung can use the same harness-local hook style
  to force or observe a duplicated stale pending snapshot without product source
  edits.
- Contract caution: executor-filtered queued-list behavior is evidence to
  capture, not a standalone product finding, unless the executor establishes
  that the public API promises current queue ownership semantics for
  `executor_id`.

#### Expected Signatures

- Success: all stale recoverer attempts are no-op/handle observations from the
  model's perspective, queue workers drain all modeled rows once, terminal rows
  and handle results agree, and active queue rows are cleaned up.
- Finding: direct execution of a queue-owned row by stale recovery, duplicate
  ledger effect, queue concurrency overstart, dead-executor pending row after a
  healthy recovery window, SQL/handle mismatch, or active queue-row leak.
- Setup block: the harness cannot safely coordinate two recovery snapshots,
  cannot force queued rows to `PENDING` under `wio-worker-a`, cannot own/restart
  Postgres for case 003, or cannot distinguish recovery-executor versus
  queue-executor ledger attribution.
- Low signal: the case only repeats rung 002 `restart-during-queue-clear`, uses
  one recoverer, never obtains a stale snapshot, or does not observe durable
  queue rows.

#### Loop-1 Execution Result

Status: `finding_candidate`.

Primary evidence:

- Run record: `.workers/runs/E-002.md`.
- Harness commit: `12caea1e77fb2eca04e16d8c919269916b6d56ce`.
- Focused replay: WIO run `01KVVCVG53BKCQZ0MG9S1P1BC6`, exploration
  `nd7fmfhm255nrkbfm3z7semxkx897k4c`, stdout SHA
  `5da4d5cb5d765993f8b481978bb49b1e7e1f65a415f23b24e171d6b2f10243a7`.

Finding signature:

- The workload created a public queued row with queue listening disabled and
  confirmed it was `ENQUEUED` on `wio_recovery_race_case_001_224`.
- The workload modeled the dead dequeued state by transitioning that durable
  queued row to `PENDING` under `wio-worker-a`.
- Recoverer B used the captured pending snapshot, `clear_queue_assignment`
  returned `true`, and the row moved back to `ENQUEUED`.
- Stale recoverer C used the same pending snapshot after B had already cleared
  assignment. Its `clear_queue_assignment` attempt returned `false`, but the
  recovery path still executed the queued workflow body.
- The independent ledger recorded both `step_one` and `step_two` under
  `wio-recoverer-c`, failing `rung6_no_stale_recoverer_body_effect`.

Classification caution:

- This is stronger than the earlier closed recovery candidate because it does
  not assume `recover_pending_workflows()` is a barrier.
- The executor-death state is modeled by durable row transition rather than a
  live killed worker process. Treat this as a confirmed finding candidate for
  the stale-snapshot recovery path, with the durable state setup documented in
  `.workers/runs/E-002.md`.

#### Stale Conditions

Mark stale if DBOS changes `_recover_workflow` behavior for queued workflows,
`clear_queue_assignment()` status transitions, queue worker dequeue ownership,
`workflow_status.executor_id` semantics, public handle result retrieval, or the
system DB queue schema.

### Rung: rung-000-product-harness-baseline

Evidence source: `evidence-key:frontiers/recovery-db-faults/rungs/rung-000-product-harness-baseline.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-product-harness-baseline
frontier: recovery-db-faults
status: passed
order: 0
level: baseline
workload_file: read-only:/Users/viswa/code/workers/dbos-transact-py/tests/test_failures.py
seeds:
  - 0
updated_at: 2026-06-20T02:08:24Z
```

#### Rung 000: Product Harness Baseline

##### Goal

- Build and run: no new workload code. Run a product-native recovery test read-only after dependency and Postgres setup are proven.
- Preserve: the Postgres-backed DBOS test harness, workflow recovery fixture path, and product checkout behavior needed by later adversarial rungs.

##### Workload File

- Expected path: `read-only:/Users/viswa/code/workers/dbos-transact-py/tests/test_failures.py`.
- Create or reuse: reuse read-only.
- Why one file is enough for this rung: this is a setup/product-harness proof, not an adversarial workload.
- When to create a new file instead: only after this baseline passes or is explicitly rejected for a setup reason that the new harness can avoid without removing Postgres recovery behavior.

##### Workload Shape

- Type: baseline product integration test.
- Entry points: `DBOS.start_workflow`, `DBOS._recover_pending_workflows`, workflow handle `get_result`.
- Sequence: bootstrap dependencies, prove Postgres ownership/isolation, run one existing recovery-focused pytest case.
- Variance: none.

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 0 | product-test-recovery-during-retries | none | product pytest fixtures | package manager, DB config, migrations, workflow recovery path, and Postgres fixture viability |

##### Invariants

- Must hold: the command reaches the selected product test and exits zero in Postgres mode.
- Eventually must hold: dependency bootstrap and Postgres setup either pass or produce an explicit setup blocker.
- Must never happen: runner drops or restarts an unowned ambient database service to satisfy this baseline.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/DEVELOPING.md`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/conftest.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_failures.py`
- Suggested command family:
  - From `/Users/viswa/code/workers/dbos-transact-py`: `PGPASSWORD=dbos pdm run pytest tests/test_failures.py::test_recovery_during_retries -q`
  - If PDM is unavailable, bootstrap PDM or record `setup-block:pdm-missing`; do not silently switch to ambient Python if dependencies are incomplete.
- Setup assumptions:
  - Use Postgres mode, not `DBOS_DATABASE=SQLITE`.
  - Use an isolated owned Postgres service/container or prove `dbostestpy` and `dbostestpy_dbos_sys` are safe to drop.
- Per-case evidence to record:
  - Python version, PDM path/version, dependency bootstrap result, `PGPASSWORD` source redacted, Postgres endpoint/container ownership, command, exit code, and first relevant failure line.
- Replay notes:
  - Record exact product commit, DBOS env vars, Postgres image/container ID when used, and whether product checkout was updated before running.

##### Expected Signatures

- Success: selected pytest exits zero in Postgres mode.
- Finding: not applicable unless a product assertion fails after setup is valid; record separately as baseline product regression evidence.
- Setup block: `pdm` missing, dependency install failure, Postgres connection/auth failure, unsafe database ownership, or test fixture cannot create/drop isolated DBs.
- Low signal: command runs in SQLite mode, skips the selected test, or uses a different recovery path.
- Goal drift: runner creates adversarial workload code before proving or rejecting baseline setup.

##### Stop Conditions

- Stop when: one baseline case passes, or a concrete setup blocker is recorded.
- Escalate when: passing this baseline requires modifying product files, using existing workload code, or destructively operating on an unowned Postgres server.

### Rung: rung-001-recovery-db-restart-single-window

Evidence source: `evidence-key:frontiers/recovery-db-faults/rungs/rung-001-recovery-db-restart-single-window.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-recovery-db-restart-single-window
frontier: recovery-db-faults
status: selected
order: 1
level: adversarial
workload_file: .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py
seeds:
  - 101
  - 103
  - 107
updated_at: 2026-06-20T02:08:24Z
```

#### Rung 001: Recovery DB Restart Single Window

##### Goal

- Build and run: one harness-local Python workload that starts checkpointed workflows, interrupts the original executor, restarts Postgres around explicit recovery windows, and checks the independent model.
- Preserve: Postgres-backed workflow recovery under executor/database interruption, pending-row conservation, no completed-step re-execution, and terminal liveness.

##### Workload File

- Expected path: `.workers/workloads/recovery-db-faults/recovery_db_faults_workload.py`.
- Create or reuse: create new; do not copy existing workload implementations.
- Why one file is enough for this rung: all cases share the same actor, product goal, DBOS public APIs, recovery API, Postgres fault mechanism, and oracle. Only schedule/fault-window parameters vary.
- When to create a new file instead: only if Workload Runner cannot implement both normal and queued workflow shapes without making the oracle ambiguous; otherwise keep one parameterized file.

##### Workload Shape

- Type: module/integration safety-liveness simulation.
- Entry points: `DBOS.start_workflow`, `DBOS._recover_pending_workflows(["wio-worker-a"])`, workflow handle retrieval/result, and read-only SQL inspection of DBOS system tables.
- Sequence:
  - Launch DBOS with `DBOS__VMID=wio-worker-a` against isolated Postgres.
  - Start one workflow with two checkpointed steps. Step 1 writes to an independent side-effect ledger and completes. Step 2 blocks on a harness-controlled gate.
  - Destroy/stop the first executor after `workflow_status` is `PENDING` and the model marks step 1 completed.
  - Launch recovery under `DBOS__VMID=wio-worker-b`.
  - Inject a Postgres restart at the case's fault window.
  - Restore Postgres healthy, unblock the workflow if applicable, run bounded recovery, and check model/SQL/handle invariants.
- Variance: seed controls workflow IDs, fault window, restart downtime, and workflow shape while preserving the same task.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | dependency response | pending scan retry preserves all dead-executor rows | restart Postgres before or during `get_pending_workflows` | recovery either retries later or reports setup failure without losing modeled IDs | no accepted workflow remains `PENDING` under `wio-worker-a`; terminal row matches model |
| case-002 | timing/order | discovery-to-execute interruption does not skip handles | restart after pending IDs are captured but before executing by ID | case artifact lists discovered pending IDs before fault | every discovered ID is terminal or DLQ after healthy recovery |
| case-003 | retry/idempotency | completed step checkpoints prevent duplicate side effects | restart while recovered execution persists or retrieves result after step 1 completed | side-effect ledger is inspected before and after recovery | ledger count for completed step is exactly one; handle result agrees with terminal row |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 101 | restart-before-scan | owned Postgres restart, down 750ms | one direct workflow, two steps, first complete | recovery handles transient DB failure during pending-row scan |
| case-002 | 103 | restart-after-scan-before-execute | owned Postgres restart, down 750ms | one direct workflow, two steps, first complete | pending ID conservation after discovery |
| case-003 | 107 | restart-during-recovered-execute | owned Postgres restart, down 1500ms | one direct workflow, first step side-effect ledger | completed step is not re-executed |

##### Invariants

- Must hold:
  - `accepted_workflows == terminal_workflows + intentionally_dlq_workflows` after the healthy recovery window.
  - No row for modeled workflows has `status = PENDING` and `executor_id = wio-worker-a`.
  - Completed-step ledger count is exactly one per workflow.
  - Recovery result or retrieved handle result matches the model's expected terminal result.
- Eventually must hold:
  - After Postgres is healthy for 10 seconds and at most 3 recovery attempts, each modeled workflow reaches terminal state or a documented DLQ state.
- Must never happen:
  - A case passes only because no workflow reached the risky blocked/recovery state.
  - The workload treats a missing recovery handle as success without SQL/terminal explanation.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_recovery.py`
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_sys_db.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_failures.py`
  - `/Users/viswa/code/workers/dbos-transact-py/chaos-tests/conftest.py`
- Suggested command family:
  - Harness command shape: `python .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py --rung rung-001 --case case-001`
  - Product dependency execution should run from the product checkout or with `PYTHONPATH=/Users/viswa/code/workers/dbos-transact-py` after dependencies are installed.
- Setup assumptions:
  - Rung 000 passed or was rejected only for a setup issue this harness explicitly solves.
  - Workload owns the Postgres service/container it restarts.
  - Workload uses unique database names with a `wio_recovery_` prefix and records cleanup.
- Per-case evidence to record:
  - seed, workflow IDs, executor IDs, app version, DB URLs redacted, fault window, restart start/end timestamps, discovered pending IDs, recovery handles returned, final `workflow_status` rows, side-effect ledger counts, and relevant DBOS logs.
- Replay notes:
  - Record exact seed plus derived schedule (`fault_window`, `restart_down_ms`, `restart_offset_ms`) and product commit. If calibration changes offset, persist the final derived case JSON next to run results.

##### Expected Signatures

- Success: all three cases reach target windows, terminal/DLQ liveness holds, no dead-executor pending rows remain, and completed-step ledger counts are one.
- Finding: any invariant violation above, especially a dead `PENDING` row after healthy recovery or duplicate ledger effect.
- Setup block: dependencies/imports fail, Postgres ownership cannot be proven, restart control unavailable, migrations cannot run, or calibration cannot reach the recovery window in 5 attempts for a matrix row.
- Low signal: workload only exercises direct happy-path completion, uses SQLite, or fails to inspect durable state.
- Goal drift: runner replaces this with product chaos tests only, a seed sweep only, or broad recovery suite design.

##### Stop Conditions

- Stop when: all 3 matrix cases pass with artifacts, one strong finding is captured, or a setup/window blocker is documented.
- Escalate when: reaching the recovery window requires modifying product source, depending on existing workload code, or broadening beyond one workflow file and this matrix.

### Rung: rung-002-recovery-window-matrix

Evidence source: `evidence-key:frontiers/recovery-db-faults/rungs/rung-002-recovery-window-matrix.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-002-recovery-window-matrix
frontier: recovery-db-faults
status: passed
order: 2
level: adversarial
workload_file: .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py
seeds:
  - 111
  - 113
  - 127
  - 131
  - 137
  - 139
updated_at: 2026-06-20T07:06:45Z
```

#### Rung 002: Recovery Window Matrix

##### Goal

- Build and run: reuse the rung-001 workload file with a wider sequential matrix across direct and queued recovery windows.
- Preserve: the same DBOS recovery promise and oracle while adding the queue-assignment path and multiple pending rows.

##### Workload File

- Expected path: `.workers/workloads/recovery-db-faults/recovery_db_faults_workload.py`.
- Create or reuse: reuse the rung-001 file and add parameters only.
- Why one file is enough for this rung: direct and queued cases both call `recover_pending_workflows` and share the pending-row/terminal/step-idempotency oracle.
- When to create a new file instead: if queued workflow setup requires separate service orchestration that obscures direct recovery evidence.

##### Workload Shape

- Type: module/integration safety-liveness simulation.
- Entry points: direct workflow start, queued workflow start, `clear_queue_assignment`, `recover_pending_workflows`, handle retrieval, SQL model checks.
- Sequence: same as rung 001, but matrix rows vary workflow count, queued vs direct workflow shape, and restart window.
- Variance: bounded seeds, workflow counts up to 3, restart down time up to 1500ms.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | dependency response | scan retry covers all rows | restart before pending scan with 3 rows | pending snapshot before/after fault is conserved | all modeled IDs terminal/DLQ, none stranded |
| case-002 | timing/order | per-row loop does not skip later IDs after one failure | restart after first pending ID is processed | first ID may recover earlier, later IDs must not disappear | all accepted IDs have terminal explanation |
| case-003 | retry/idempotency | direct recovery replay respects completed step outputs | restart during direct workflow execution | completed-step ledger remains stable | ledger count exactly one |
| case-004 | timing/order | queued recovery clears assignment safely | restart during `clear_queue_assignment` | queued row returns to `ENQUEUED` or terminal, not dead pending | status/queue row matches model |
| case-005 | partial failure/recovery | handle retrieval after DB fault still observes terminal truth | restart after execution before `get_result` | direct SQL and handle agree after DB returns | terminal row and handle result match |
| case-006 | dependency response | transient outage is not mistaken for unrecoverable workflow failure | restart with longer down time below case timeout | recovery may retry but must not mark unexpected DLQ | terminal state follows model retry budget |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 111 | restart-before-scan | down 750ms | 3 direct workflows | full pending-row conservation |
| case-002 | 113 | restart-after-first-id | down 750ms | 3 direct workflows | no per-row skip after mid-loop failure |
| case-003 | 127 | restart-during-direct-execute | down 1500ms | 2 direct workflows with ledgered step 1 | idempotency under replay |
| case-004 | 131 | restart-during-queue-clear | down 750ms | 2 queued workflows | queue assignment clear safety |
| case-005 | 137 | restart-before-result-retrieve | down 750ms | 1 direct workflow | handle/result and SQL terminal agreement |
| case-006 | 139 | restart-longer-transient | down 1500ms | 1 direct workflow, recovery budget 3 | transient DB failure not terminal corruption |

##### Invariants

- Must hold: model ID set equals terminal/DLQ ID set after healthy recovery; no dead-executor pending rows; ledgered completed steps remain single-effect.
- Eventually must hold: all recoverable cases terminal within 15 seconds after Postgres is healthy.
- Must never happen: queued workflow remains `PENDING` with dead executor when it should be `ENQUEUED`, terminal, or intentionally DLQ.

##### Execution Map

- Suggested files to inspect: same as rung 001 plus queue tests under `/Users/viswa/code/workers/dbos-transact-py/tests/queuedworkflow.py` and `/Users/viswa/code/workers/dbos-transact-py/tests/test_client.py`.
- Suggested command family: `python .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py --rung rung-002 --all-cases --sequential`.
- Setup assumptions: rung 001 passed without setup drift; same isolated Postgres ownership applies.
- Per-case evidence to record: all rung-001 evidence plus per-row recovery order and queued workflow status transitions.
- Replay notes: persist derived case JSON for each matrix row because offsets may calibrate differently across machines.

##### Expected Signatures

- Success: all 6 cases pass with target-window evidence and invariant artifacts.
- Finding: any stranded row, skipped modeled ID, duplicate ledger effect, unexpected DLQ, or SQL/handle mismatch.
- Setup block: target queue-clear window cannot be reached after 5 calibration attempts or queue setup cannot run in the harness.
- Low signal: only one workflow actually reaches `PENDING`, or queued rows are not distinguishable from direct rows.
- Goal drift: runner adds broad concurrency/load instead of the specified window matrix.

##### Stop Conditions

- Stop when: 6 cases pass, one strong finding is recorded, or a setup/window blocker is reached.
- Escalate when: the queue path needs a different oracle or product change to observe safely.

##### Result

- Status: passed.
- Commit: `b94d721695379003ae66d0b8b2ea62918066a240`.
- Prepared image: `pd71edm09s800958079cn6ptzs8909vt`.
- Evidence run: `evidence-key:runs/run-20260620T032157Z-recovery-db-faults-rung-002-recovery-window-matrix/summary.md`.
- Cloud workloads:
  - `case-001`: exploration `nd7b9v0xycb8wj4kdg4f8m72wd8911ck`, workload `01KVHX8GNHYWWPZKAKQQ6BP9E4`.
  - `case-002`: exploration `nd7fdk6gn2vyfv5wd5nmyqvw45890y1j`, workload `01KVHX8PG2YG2F3QMS4VJ0ZEFW`.
  - `case-003`: exploration `nd7831m7bpksz9bppdw65a1kpd890g03`, workload `01KVHX8WAVMZFC4YNN576XF25Y`.
  - `case-004`: exploration `nd71k6a4fwzs4yqy209fk00qws890bv4`, workload `01KVHX924BA54TMTDX8HMHQKR6`.
  - `case-005`: exploration `nd7fc0sx9atthc6zajz2bzzjp58907qt`, workload `01KVHXFVXXA2GMXTRSA2PC599Z`.
  - `case-006`: exploration `nd74m9hw1h2h8g2pkpsb89tvys891yyf`, workload `01KVHXG2EF2N354DES6ERMCF9E`.
- Verification: each workload has `state=succeeded`, `exitCode=0`, `failureCategory=null`, `hasInvariantViolation=false`, 10 parsed invariants, and only `PASS` invariant statuses.
- Notes: expected DBOS background thread `OperationalError` warnings occur during the deliberate Postgres stop/start windows and are not product failures unless a recovery/idempotency/result invariant fails.

### Rung: rung-003-replay-dlq-liveness

Evidence source: `evidence-key:frontiers/recovery-db-faults/rungs/rung-003-replay-dlq-liveness.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-003-replay-dlq-liveness
frontier: recovery-db-faults
status: deferred
order: 3
level: failure
workload_file: .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py
seeds:
  - 149
  - 151
  - 157
  - 163
  - 167
  - 173
  - 179
  - 181
updated_at: 2026-06-19T21:35:00Z
```

#### Rung 003: Replay DLQ Liveness

##### Goal

- Build and run: reuse the recovery workload to apply repeated transient DB faults and recovery attempts until each workflow reaches modeled terminal success or DLQ.
- Preserve: no completed-step re-execution while testing bounded liveness and recovery-attempt accounting.

##### Workload File

- Expected path: `.workers/workloads/recovery-db-faults/recovery_db_faults_workload.py`.
- Create or reuse: reuse and add retry/DLQ parameters.
- Why one file is enough for this rung: it is the same recovery actor and oracle, adding only repeated attempts and DLQ budget.
- When to create a new file instead: if DLQ-specific workflow definitions would make the main file unmaintainable; keep the same output schema either way.

##### Workload Shape

- Type: failure/replay safety-liveness simulation.
- Entry points: `DBOS.start_workflow`, `DBOS._recover_pending_workflows`, `DBOS.resume_workflow` only where matrix says resume after DLQ, SQL status/step inspection.
- Sequence: create workflows with `max_recovery_attempts`, force/reach pending state after a completed first step, inject transient DB faults across repeated recoveries, then leave DB healthy and verify terminal/DLQ behavior.
- Variance: recovery budget, number of outage cycles, and whether the modeled workflow is supposed to succeed or DLQ.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | partial failure/recovery | repeated transient outages still converge when DB becomes healthy | one outage then healthy recovery | workflow reaches `SUCCESS` | terminal success and ledger count one |
| case-002 | retry/idempotency | recovery attempts are counted without re-running completed steps | two recovery outages before success | attempts increase within budget | ledger count one and final success |
| case-003 | partial failure/recovery | exhausted recovery budget becomes DLQ, not permanent pending | repeated recovery attempts over max budget | status becomes `MAX_RECOVERY_ATTEMPTS_EXCEEDED` | DLQ terminal and no dead pending |
| case-004 | retry/idempotency | completed workflow is not DLQ'ed by later duplicate invocations | invoke/recover completed workflow repeatedly | completed result is stable | terminal success stable, ledger count one |
| case-005 | timing/order | outage after DLQ transition does not resurrect active row | restart after DLQ update | state remains DLQ until explicit resume | terminal stability |
| case-006 | partial failure/recovery | explicit resume after DLQ can progress without duplicate prior effect | resume DLQ workflow after DB healthy | resumed handle completes | expected result and ledger invariants |
| case-007 | dependency response | longer DB outage is setup/retry failure, not silent success | DB down beyond per-attempt timeout but within rung budget | case records recoverable failure then retries | no missing modeled ID |
| case-008 | retry/idempotency | multiple workflows do not share recovery-attempt state | mixed success and DLQ workflows | each ID follows its own budget | per-ID terminal state matches model |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 149 | one-outage-then-healthy | 1 restart, down 750ms | 1 workflow, max attempts 3 | liveness after simple transient |
| case-002 | 151 | two-outages-then-healthy | 2 restarts, down 750ms | 1 workflow, max attempts 5 | attempts and idempotency |
| case-003 | 157 | exceed-recovery-budget | DB healthy between forced attempts | 1 workflow, max attempts 2 | DLQ terminal transition |
| case-004 | 163 | duplicate-completed-invocations | no DB outage after success | 1 completed workflow | no post-success DLQ or duplicate effect |
| case-005 | 167 | restart-after-dlq-update | restart down 750ms after DLQ row observed | 1 workflow, max attempts 1 | DLQ stability |
| case-006 | 173 | resume-after-dlq | no restart after resume | 1 resumed workflow | resume liveness and idempotency |
| case-007 | 179 | outage-beyond-attempt-timeout | restart down 3000ms | 1 workflow, max attempts 3 | explicit failure artifact, no silent pass |
| case-008 | 181 | mixed-success-dlq | 2 restarts, down 750ms | 3 workflows with mixed budgets | per-ID attempt isolation |


##### Invariants

- Must hold: every modeled ID reaches modeled `SUCCESS` or `MAX_RECOVERY_ATTEMPTS_EXCEEDED`; no modeled ID remains active under the dead executor; completed step effects remain single-count.
- Eventually must hold: after the last planned outage, terminal/DLQ convergence within 20 seconds.
- Must never happen: DLQ appears before modeled attempts are exhausted, completed success later flips to DLQ, or a resumed DLQ case duplicates the completed first step.

##### Execution Map

- Suggested files to inspect: `/Users/viswa/code/workers/dbos-transact-py/tests/test_failures.py::test_dead_letter_queue` and `dbos/_sys_db.py` recovery-attempt update logic.
- Suggested command family: `python .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py --rung rung-003 --all-cases --sequential`.
- Setup assumptions: rung 002 produced useful signal or passed; exclusive Postgres ownership remains mandatory.
- Per-case evidence to record: recovery attempt counts before/after each attempt, DLQ transition timestamp when relevant, terminal status, ledger count, handle result/error, and any swallowed DB exception logs.
- Replay notes: persist the recovery-attempt timeline as structured JSON, not only seed, because attempt counts are the oracle.

##### Expected Signatures

- Success: all 8 cases reach modeled terminal state with stable ledger counts.
- Finding: early/late DLQ, terminal flip, duplicate side effect, orphan pending row, or per-ID attempt count bleed.
- Setup block: workload cannot safely force/reach repeated recovery attempts without product modification.
- Low signal: cases only call `update_workflow_outcome` without executor interruption or DB fault evidence.
- Goal drift: runner turns this into broad max-retry unit tests without Postgres restart/recovery.

##### Stop Conditions

- Stop when: all 8 cases pass, one strong finding is captured, or repeated attempts cannot be made safely in the harness.
- Escalate when: DLQ/resume behavior requires a separate strategy candidate because it dominates recovery DB-fault evidence.

### Rung: rung-004-bounded-seed-sweep

Evidence source: `evidence-key:frontiers/recovery-db-faults/rungs/rung-004-bounded-seed-sweep.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-004-bounded-seed-sweep
frontier: recovery-db-faults
status: deferred
order: 4
level: sweep
workload_file: .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py
seeds:
  - 200
updated_at: 2026-06-19T21:35:00Z
```

#### Rung 004: Bounded Seed Sweep

##### Goal

- Build and run: use the proven workload file for a bounded sweep after smaller rungs establish value.
- Preserve: the same recovery DB-fault oracle while searching for rare offset and multi-workflow interleavings.

##### Workload File

- Expected path: `.workers/workloads/recovery-db-faults/recovery_db_faults_workload.py`.
- Create or reuse: reuse; this rung is not valuable unless rungs 001-003 already made the harness trustworthy.
- Why one file is enough for this rung: it is a seed/parameter sweep of the same modeled workload, not a new area.
- When to create a new file instead: do not create a separate file for this sweep; if a new failure mechanism is needed, send back to Strategy.

##### Workload Shape

- Type: bounded sweep over safety-liveness simulation.
- Entry points: same as rung 001.
- Sequence: execute 24 sequential cases generated from the fixed matrix below; stop early on first strong finding.
- Variance: seed, workflow count, direct/queued mix, restart offset bucket, and restart downtime.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| all-cases | timing/order | rare restart offsets do not strand recovered workflows | vary offset bucket around scan/execute/result windows | target window reached or calibration artifact explains miss | no dead pending rows, terminal/DLQ liveness, ledger count one |
| all-cases | dependency response | different transient down times do not corrupt recovery state | vary down time 250-1500ms | DB returns healthy and recovery continues | model ID set equals terminal/DLQ set |
| all-cases | scale/concurrency pressure | small multi-workflow batches do not skip IDs | vary workflow count 1-5 sequentially in one executor generation | pending snapshot covers all modeled IDs | every modeled ID terminal or explained |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| cases-001-006 | 200-205 | before-scan offsets `[0,25]` | down `[250,750,1500]` | 1-3 direct workflows | scan retry conservation |
| cases-007-012 | 206-211 | after-scan offsets `[0,25]` | down `[250,750,1500]` | 1-5 direct workflows | mid-loop pending ID conservation |
| cases-013-018 | 212-217 | during-execute offsets `[0,50]` | down `[250,750,1500]` | 1-3 direct workflows with ledger | idempotency under replay |
| cases-019-024 | 218-223 | queue-clear/result offsets `[0,50]` | down `[250,750,1500]` | 1-3 queued/direct mixed workflows | queue and handle-result liveness |

##### Invariants

- Must hold: all lower-rung invariants.
- Eventually must hold: after each case's final fault, healthy recovery reaches terminal/DLQ within 20 seconds.
- Must never happen: sweep retries indefinitely or reports aggregate pass while hiding a failed matrix row.

##### Execution Map

- Suggested files to inspect: no new product files beyond earlier rungs.
- Suggested command family: `python .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py --rung rung-004 --cases 001-024 --sequential --stop-on-finding`.
- Setup assumptions: earlier rungs passed and target-window calibration is reliable.
- Per-case evidence to record: derived case JSON, target-window evidence, final invariant report, and minimized candidate if a finding appears.
- Replay notes: seed alone is insufficient; persist the derived schedule for each failed case.

##### Expected Signatures

- Success: 24 cases complete sequentially with invariant artifacts and no hidden calibration misses.
- Finding: any lower-rung invariant violation.
- Setup block: sweep runtime exceeds 30 minutes before 24 cases or calibration misses more than 6 cases.
- Low signal: more than 25 percent of cases miss target windows without useful failure evidence.
- Goal drift: runner broadens to unbounded chaos/load.

##### Stop Conditions

- Stop when: 24 cases pass, first strong finding occurs, 30-minute budget is exhausted, or target-window miss rate exceeds 25 percent.
- Escalate when: sweep shows repeated calibration misses that require a redesigned fault hook.

### Rung: rung-005-finding-minimization

Evidence source: `evidence-key:frontiers/recovery-db-faults/rungs/rung-005-finding-minimization.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-005-finding-minimization
frontier: recovery-db-faults
status: closed_workload_model_artifact
order: 5
level: adversarial
workload_file: .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py
seeds:
  - 0
  - from-finding
  - from-finding
updated_at: 2026-06-20T22:23:50Z
```

#### Rung 005: Finding Minimization

##### Goal

- Build and run: shrink a confirmed finding from rungs 001-004 into the smallest deterministic replay case.
- Preserve: the same product promise, failure mechanism, and oracle that produced the finding.

##### Result

- Outcome: minimized the rung-004 candidate to five workflows, `250ms` Postgres outage, and `0ms` restart offset.
- Final classification: closed as a workload-model artifact, not a DBOS product bug.
- Reason: the workload oracle assumed `_recover_pending_workflows` was a barrier before recovered workflow code could execute. DBOS returns handles while recovered workflow code may already be running, and existing product tests await those handles for completion rather than relying on a barrier.

##### Workload File

- Expected path: `.workers/workloads/recovery-db-faults/recovery_db_faults_workload.py`.
- Create or reuse: reuse and add a `--replay-case <json>` mode if missing.
- Why one file is enough for this rung: minimization should remove parameters from the existing failing case, not invent a new workload.
- When to create a new file instead: only if a minimized product-native regression test is intentionally requested after verifier review.

##### Workload Shape

- Type: deterministic replay/minimization.
- Entry points: same failing path as the source case.
- Sequence: load the captured failed case JSON, reduce workflow count, restart offsets, down time, and optional branches one at a time while preserving the invariant violation.
- Variance: none beyond controlled minimization attempts.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | timing/order | source finding is reproducible without broad sweep noise | replay exact failed schedule | same invariant violation repeats | original failing invariant |
| case-002 | timing/order | extra workflow count is not required unless proved | shrink to one workflow if possible | violation persists or shrink rejected | original failing invariant |
| case-003 | dependency response | long DB outage is not required unless proved | reduce down time to smallest reproducing bucket | violation persists or shrink rejected | original failing invariant |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 0 | exact-replay | exact from failed artifact | exact from failed artifact | prove reproducibility |
| case-002 | from-finding | shrink-workflow-count | exact fault from failed artifact | reduce modeled workflows | find smallest ID set |
| case-003 | from-finding | shrink-fault-duration-offset | reduce down/offset buckets | smallest preserving shape | find deterministic fault schedule |

##### Invariants

- Must hold: the minimized case reproduces the same invariant violation, or the rung records the smallest non-reducible artifact.
- Eventually must hold: a verifier can rerun one command and observe the same failure.
- Must never happen: minimization changes the oracle or product promise to make replay easier.

##### Execution Map

- Suggested files to inspect: source rung artifact and workload replay code only.
- Suggested command family: `python .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py --rung rung-005 --replay-case <failed-case.json> --minimize`.
- Setup assumptions: an earlier rung produced a confirmed finding and recorded structured case JSON.
- Per-case evidence to record: original case path, minimized case JSON, replay command, invariant failure message, and rejected shrink attempts.
- Replay notes: final artifact must include seed, derived schedule, workflow IDs or deterministic generation inputs, product commit, and Postgres/container setup.

##### Expected Signatures

- Success: one deterministic minimized case reproduces the original finding.
- Finding: same as source finding; this rung does not create new finding classes.
- Setup block: source finding lacks enough structured replay data.
- Low signal: violation cannot be reproduced even with exact source case.
- Goal drift: runner changes workload behavior or oracle instead of minimizing.

##### Stop Conditions

- Stop when: a 1-3 case minimized artifact is produced, exact replay fails to reproduce, or source artifacts are insufficient.
- Escalate when: the only reproducer requires broad nondeterministic chaos without target-window evidence.
