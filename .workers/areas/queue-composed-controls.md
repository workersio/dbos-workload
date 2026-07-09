# Area: queue-composed-controls

## Current State

Current status: base composed-control rungs are green; async partition
worker-concurrency finding evidence is preserved; rate-limit partial-index plan
guard rung ready.

Evidence:

- `evidence-key:frontiers/queue-composed-controls/frontier.md`
- `evidence-key:runs/run-20260620T010010Z-queue-composed-controls-rung-005-bounded-seed-sweep/summary.md`
- `.workers/workloads/queue-composed-controls/queue_composed_controls_workload.py`
- `.workers/runs/E-006.md`
- Issue `#696` / PR `#698`: rate-limit scheduler query missed the partial
  index predicate and caused high CPU.

## Product Promise

DBOS queues preserve dedupe, priority, delay, partition, rate-limit, live config,
result retrieval, cleanup semantics, and scheduler query scalability under
composed controls.

## What Not To Repeat

- Do not add a seed sweep over the same composed-control matrix unless it adds a
  new oracle or adversarial axis.
- Do not treat green queue rungs as proof that queue interactions with recovery,
  transactions, lifecycle, or client result retrieval are exhausted.

## Loop-1 Search Directions

| Direction | Why It Is Deeper |
|---|---|
| Queue plus transaction boundary | Enqueue/send-in-transaction can create visibility and rollback bugs not covered by queue-only rungs. |
| Queue plus recovery/relaunch | Result retrieval and terminal conservation after worker shutdown can be joined with DB recovery windows. |
| Queue delete/cancel plus descendants | Lifecycle cleanup and queue result rows may disagree after parent/child or fork operations. |
| Multi-queue fairness | Existing rungs emphasized one composed queue; multiple queues with live config changes can reveal starvation or overstart. |
| Rate-limit query plan | Existing rate-limit rungs prove behavior, not whether the scheduler query uses the Postgres partial index required for production-scale backlogs. |

## Rung Design Requirements

New rungs must identify the queue invariant that would fail, the modeled
accepted work set, and the durable rows used as oracle evidence.

## Stale Conditions

Mark stale if DBOS changes queue schema, worker polling, limiter semantics, or
transactional enqueue APIs.

Mark `rung-008-rate-limit-partial-index-plan` stale if DBOS removes
`workflow_status.rate_limited`, changes the partial index predicate, changes
rate-limit counting away from `start_queued_workflows`, or replaces the
scheduler SQL with an intentionally different scalable access path.

## Rung Index

Evidence source: `evidence-key:frontiers/queue-composed-controls/rungs.index.yaml`

```yaml
schema_version: 1
columns: [id, path, status, order, level, workload_file, matrix, summary]
rungs:
  - [
      "rung-001-single-queue-ledger-controls",
      "rungs/rung-001-single-queue-ledger-controls.md",
      "passed",
      "1",
      "adversarial",
      ".workers/workloads/queue-composed-controls/queue_composed_controls_workload.py",
      "3 cases",
      "single DB-backed queue composed-control cloud run passed dedupe, delay, priority, rate limit, live config, result, and cleanup oracles",
    ]
  - [
      "rung-002-partition-isolation-matrix",
      "rungs/rung-002-partition-isolation-matrix.md",
      "passed",
      "2",
      "adversarial",
      ".workers/workloads/queue-composed-controls/queue_composed_controls_workload.py",
      "4 cases",
      "add partition-key isolation and partition-local ordering without dedupe",
    ]
  - [
      "rung-003-live-config-rate-limit-matrix",
      "rungs/rung-003-live-config-rate-limit-matrix.md",
      "passed",
      "3",
      "adversarial",
      ".workers/workloads/queue-composed-controls/queue_composed_controls_workload.py",
      "6 cases",
      "broaden live queue config changes across concurrency, worker concurrency, polling, and limiter backlog",
    ]
  - [
      "rung-004-executor-relaunch-result-durability",
      "rungs/rung-004-executor-relaunch-result-durability.md",
      "passed",
      "4",
      "failure",
      ".workers/workloads/queue-composed-controls/queue_composed_controls_workload.py",
      "6 cases",
      "simulate application interruption without DB restart and verify result retrieval plus cleanup",
    ]
  - [
      "rung-005-bounded-seed-sweep",
      "rungs/rung-005-bounded-seed-sweep.md",
      "passed",
      "5",
      "sweep",
      ".workers/workloads/queue-composed-controls/queue_composed_controls_workload.py",
      "24 cases",
      "bounded rare-bug sweep over seeds, delays, release offsets, priorities, partitions, and limiter windows",
    ]
  - [
      "rung-006-finding-minimization",
      "rungs/rung-006-finding-minimization.md",
      "not_applicable_no_finding",
      "6",
      "adversarial",
      ".workers/workloads/queue-composed-controls/queue_composed_controls_workload.py",
      "1-3 cases",
      "shrink a queue composed-control finding into a deterministic replay artifact",
    ]
  - [
      "rung-007-async-partition-worker-concurrency",
      "inline:loop-1-added-rung-rung-007-async-partition-worker-concurrency",
      "queued",
      "7",
      "recent-churn",
      ".workers/workloads/queue-composed-controls/queue_composed_controls_workload.py",
      "3 cases",
      "async partitioned queue worker_concurrency must be enforced per partition and persisted partition keys must propagate into dequeued workflow context",
    ]
  - [
      "rung-008-rate-limit-partial-index-plan",
      "inline:producer-20260624-rate-limit-partial-index-plan",
      "ready",
      "8",
      "performance-regression",
      ".workers/workloads/queue-composed-controls/queue_composed_controls_workload.py",
      "3 cases",
      "Postgres rate-limit scheduler query must use the partial rate_limited index under realistic backlog/selectivity and preserve limiter correctness",
    ]
```

## Rung Details

These are the known rung contracts and outcomes for this frontier. Treat completed, finding, and closed rungs as current reality and design constraints, not as active executor queue rows.

### Rung: rung-001-single-queue-ledger-controls

Evidence source: `evidence-key:frontiers/queue-composed-controls/rungs/rung-001-single-queue-ledger-controls.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-single-queue-ledger-controls
frontier: queue-composed-controls
status: selected
order: 1
level: adversarial
workload_file: .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py
seeds:
  - 2101
  - 2103
  - 2107
updated_at: 2026-06-19T21:52:30Z
```

#### Rung 001: Single Queue Ledger Controls

##### Goal

- Build and run: one harness-local Python workload that registers one Postgres-backed DBOS queue, drives public enqueue APIs with generated request plans, and checks an independent request/execution ledger.
- Preserve: dedupe rejection, delayed eligibility, priority ordering, rate limit behavior, live concurrency config reload, handle result retrieval, and terminal cleanup on a durable database-backed queue.

##### Workload File

- Expected path: `.workers/workloads/queue-composed-controls/queue_composed_controls_workload.py`.
- Create or reuse: create new; do not copy existing workload implementations.
- Why one file is enough for this rung: all cases share one actor, one non-partitioned queue shape, one product promise, one DBOS public API surface, and one request-ledger oracle. Only seed-derived request plan, delay, priority, limiter, and config-update timing vary.
- When to create a new file instead: only if the runner cannot keep the ledger, queue setup, and status snapshots clear in one parameterized harness; otherwise keep one workload file and one sequential matrix.

##### Workload Shape

- Type: background-job/stateful queue workload.
- Entry points: `DBOS.register_queue`, `DBOS.enqueue_workflow`, `SetEnqueueOptions`, queue setters such as `set_concurrency`, workflow handles, `DBOS.list_workflows` or equivalent read-only status APIs, and read-only SQL inspection of DBOS system tables when needed.
- Sequence:
  - Launch DBOS against isolated Postgres with a unique `wio_queue_` database prefix and one queue named from the seed.
  - Register a database-backed queue with `concurrency=1`, `priority_enabled=True`, a small limiter, and short polling interval.
  - Define a queued workflow that writes exactly one independent ledger row at start, optionally blocks on a harness gate, and returns a deterministic result from the modeled request key.
  - Enqueue one blocking accepted request to create backlog.
  - Enqueue delayed and prioritized accepted requests using `SetEnqueueOptions`.
  - Attempt a duplicate enqueue for a still-delayed or still-queued dedupe ID under a distinct workflow ID and record it as `rejected_duplicate`.
  - Update queue config while backlog exists, then release gates and collect results.
  - Check ledger, handle, status, delay/order, limiter, and cleanup invariants after each meaningful phase and at terminal state.
- Variance: seed controls request keys, dedupe IDs, priorities, delay seconds, release offset, limiter period, and config update timing while preserving the same task.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | duplicate/replay | duplicate dedupe IDs are rejected without executing while the accepted row is delayed | enqueue delayed request with dedupe ID, immediately enqueue a duplicate with a different workflow ID | duplicate raises `DBOSQueueDeduplicatedError`; accepted row later executes | no ledger row for rejected duplicate; accepted dedupe key executes exactly once |
| case-002 | timing/order | delay expiration and priority selection compose under a blocked concurrency-1 queue | enqueue blocked request, then delayed prioritized work and immediate lower-priority work; release only after delay expires | start timestamps show the blocked first row, then eligible priority ordering | no delayed execution before eligibility; start order matches modeled priority window |
| case-003 | dynamic config | live queue config reload and limiter changes do not lose or overstart accepted work | build backlog at concurrency 1, then call queue setters before release | workers start newly allowed work after config reload and all accepted requests finish | active starts stay within modeled cap; terminal results and cleanup match ledger |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 2101 | duplicate-while-delayed | no dependency fault; duplicate offset 50ms | 1 blocking request, 1 delayed dedupe request, 1 rejected duplicate | dedupe rejection before delayed execution |
| case-002 | 2103 | priority-after-release | no dependency fault; release after delay plus 150ms | 1 blocker, 3 accepted requests with priorities `[3, 1, 2]`, delay 0.5s | priority and delay ordering under backlog |
| case-003 | 2107 | live-config-backlog | no dependency fault; config update before release | 1 blocker, 4 accepted requests, limiter `2/0.8s`, concurrency update `1 -> 2` | dynamic config, limiter, terminal conservation |

##### Invariants

- Must hold:
  - `accepted_request_keys == terminal_success_keys` for accepted requests by the end of the case.
  - `rejected_duplicate_keys` have no execution-ledger row and no successful handle result.
  - For each accepted dedupe ID in the case, execution count is exactly one until terminal reuse is explicitly modeled.
  - A delayed request's `started_at` is greater than or equal to the modeled `eligible_at` minus 100ms tolerance.
  - Within the case's eligible priority window, lower priority numbers start before higher numbers after the initial blocking request is released.
  - Observed simultaneous starts do not exceed modeled concurrency after accounting for the live config update.
  - Handle result equals the independent expected result for every accepted request.
  - No modeled workflow remains `ENQUEUED`, `DELAYED`, or `PENDING` after terminal completion and cleanup polling.
- Eventually must hold:
  - After release and a healthy Postgres window of 10 seconds, all accepted requests are terminal or the case records a bounded liveness failure.
- Must never happen:
  - The workload passes because the risky delayed, duplicate, or backlog phase was not reached.
  - A duplicate rejection is counted as success without proving the workflow body did not execute.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_dbos.py`
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_queue.py`
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_context.py`
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_sys_db.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_queue.py`
- Suggested command family:
  - `python .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-001 --case case-001`
  - `python .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-001 --all-cases --sequential`
- Setup assumptions:
  - Use real Postgres, not SQLite, for a useful green result.
  - The workload owns or proves isolation for its Postgres databases before creating or dropping them.
  - The first rung does not require DB restart, Kafka, product pytest execution, or product source edits.
  - If dependencies are missing, runner should build setup/wrapper artifacts under `.workers/` and record the blocker or repair.
- Per-case evidence to record:
  - seed, derived request plan, queue config before/after updates, enqueue decisions, dedupe exception details, workflow IDs, status snapshots, ledger rows with timestamps, handle results, terminal rows, cleanup poll results, product commit, and redacted DB connection details.
- Replay notes:
  - Persist the exact seed and derived JSON request plan in the run directory because timer-derived eligibility and release offsets are more durable than seed alone.

##### Expected Signatures

- Success: all 3 cases reach their named target windows, accepted requests execute exactly once, rejected duplicates never execute, delay/priority/config invariants hold, handle results match the ledger, and cleanup leaves no active modeled rows.
- Finding: any duplicate execution, rejected duplicate side effect, early delayed execution, priority inversion inside the modeled window, over-concurrency, missing result, or stranded active row.
- Setup block: DBOS imports/dependencies fail, Postgres isolation cannot be established, migrations cannot run, or the target window cannot be reached after 4 calibration attempts for a matrix row.
- Low signal: the workload uses SQLite for the final classification, checks only command completion, or never creates delayed/duplicate/backlog phases.
- Goal drift: runner replaces the workload with existing product tests, a seed sweep only, DB restart, Kafka, or broad queue benchmarking.

##### Stop Conditions

- Stop when: all 3 matrix cases pass with artifacts, one strong invariant violation is captured, or a setup/window blocker is documented.
- Escalate when: reaching the target windows requires product source changes, existing workload code, or a different oracle.

### Rung: rung-002-partition-isolation-matrix

Evidence source: `evidence-key:frontiers/queue-composed-controls/rungs/rung-002-partition-isolation-matrix.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-002-partition-isolation-matrix
frontier: queue-composed-controls
status: ready
order: 2
level: adversarial
workload_file: .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py
seeds:
  - 2111
  - 2113
  - 2117
  - 2119
updated_at: 2026-06-19T21:52:30Z
```

#### Rung 002: Partition Isolation Matrix

##### Goal

- Build and run: reuse the rung-001 workload file with a partitioned queue mode and a four-case sequential matrix.
- Preserve: DBOS partitioned queue isolation, partition-local ordering, priority behavior within partition scope, result retrieval, and cleanup.

##### Workload File

- Expected path: `.workers/workloads/queue-composed-controls/queue_composed_controls_workload.py`.
- Create or reuse: reuse the rung-001 file and add a `partitioned` queue mode.
- Why one file is enough for this rung: the same public queue APIs, ledger model, status snapshots, and terminal cleanup oracle apply. The queue configuration and request options vary.
- When to create a new file instead: only if partition setup requires a materially separate harness process boundary.

##### Workload Shape

- Type: background-job/stateful partitioned queue workload.
- Entry points: `DBOS.register_queue(..., partition_queue=True, worker_concurrency=1, priority_enabled=True)`, `SetEnqueueOptions(queue_partition_key=...)`, workflow handles, and status/list APIs.
- Sequence:
  - Register a partitioned database-backed queue.
  - Start one blocking request in partition `blocked`.
  - Enqueue a follower in the same partition and verify it remains queued while the first request is blocked.
  - Enqueue work in partition `normal` while `blocked` is still blocked and verify bounded progress.
  - Vary priority, delay, release timing, and request counts across the matrix.
  - Release the blocked partition and check terminal conservation plus cleanup.
- Variance: seed controls partition keys, follower counts, priority choices, delay windows, and release offsets.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | partition isolation | one blocked partition cannot starve another partition | block partition `blocked`, enqueue partition `normal` before release | normal partition reaches terminal first | normal terminal result appears before blocked release |
| case-002 | timing/order | partition-local worker concurrency holds while other partitions progress | enqueue two followers in blocked partition and two normal requests | blocked followers wait; normal requests complete | per-partition start order and terminal conservation hold |
| case-003 | priority | priority ordering applies inside a partition without crossing partitions incorrectly | enqueue mixed priorities in both partitions | each partition's eligible start order follows priority after gates | priority model holds per partition |
| case-004 | delay | delayed normal partition work does not execute early while blocked partition is held | add delayed normal request and immediate blocked follower | delayed start waits for eligibility; blocked follower waits for release | delay and partition invariants hold together |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 2111 | blocked-plus-normal | no dependency fault | 1 blocked request, 1 blocked follower, 1 normal request | cross-partition progress |
| case-002 | 2113 | multi-follower | no dependency fault | 1 blocked request, 2 blocked followers, 2 normal requests | per-partition worker concurrency |
| case-003 | 2117 | partition-priority | no dependency fault | 2 partitions, priorities `[2, 1, 3]` per partition | priority inside partition scopes |
| case-004 | 2119 | partition-delay | no dependency fault | delayed normal request, immediate blocked follower | delayed eligibility plus partition isolation |

##### Invariants

- Must hold:
  - Every enqueue on the partitioned queue supplies a `queue_partition_key`.
  - No matrix case uses `deduplication_id` with a partition key; DBOS rejects that combination and it belongs outside this accepted-work partition oracle.
  - A blocked partition follower does not start before the blocked partition gate is released.
  - A normal partition request reaches terminal state while the blocked partition is still blocked when capacity is modeled available.
  - Accepted request executions and handle results match the independent ledger.
  - Active modeled rows are cleaned up after terminal completion.
- Eventually must hold:
  - After the blocked partition gate is released, all accepted requests are terminal within 10 seconds of healthy Postgres.
- Must never happen:
  - The workload declares partition isolation without proving the blocked partition was actually in `PENDING` and a follower was queued behind it.

##### Execution Map

- Suggested files to inspect: same as rung 001 plus `tests/test_queue.py::test_queue_partitions`.
- Suggested command family: `python .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-002 --all-cases --sequential`.
- Setup assumptions: rung 001 passed or its setup blockers were repaired; use the same isolated Postgres policy.
- Per-case evidence to record: all rung-001 evidence plus partition keys, per-partition status snapshots, and timestamps proving normal partition terminal progress before blocked release.
- Replay notes: persist derived partition keys and request plan with the seed.

##### Expected Signatures

- Success: all 4 cases pass with evidence that blocked and normal partitions were both active and invariants held.
- Finding: normal partition starvation, blocked follower starts early, partition-local priority inversion, early delayed execution, result mismatch, or stranded active rows.
- Setup block: partitioned queue setup cannot run or target partition windows cannot be reached after 4 calibration attempts per row.
- Low signal: the workload runs only one partition or releases the blocked partition before the normal partition observation.
- Goal drift: runner adds dedupe to partitioned accepted-work cases instead of preserving DBOS's documented rejection boundary.

##### Stop Conditions

- Stop when: 4 cases pass with artifacts, one strong finding is recorded, or a setup/window blocker is reached.
- Escalate when: partition observation requires private mutation rather than public queue APIs and read-only inspection.

### Rung: rung-003-live-config-rate-limit-matrix

Evidence source: `evidence-key:frontiers/queue-composed-controls/rungs/rung-003-live-config-rate-limit-matrix.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-003-live-config-rate-limit-matrix
frontier: queue-composed-controls
status: deferred
order: 3
level: adversarial
workload_file: .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py
seeds:
  - 2123
  - 2129
  - 2131
  - 2137
  - 2141
  - 2143
updated_at: 2026-06-19T21:52:30Z
```

#### Rung 003: Live Config Rate Limit Matrix

##### Goal

- Build and run: reuse the queue composed-controls workload with a deeper live-config matrix after rung 001 proves the harness.
- Preserve: database-backed queue workers reload concurrency, worker concurrency, limiter, priority, and polling interval while accepted backlog remains conserved.

##### Workload File

- Expected path: `.workers/workloads/queue-composed-controls/queue_composed_controls_workload.py`.
- Create or reuse: reuse the previous workload and add parameters only.
- Why one file is enough for this rung: the same queue actor, model, status observations, and ledger oracle apply. The matrix varies live config knobs and request scale.

##### Workload Shape

- Type: background-job/stateful config-change workload.
- Entry points: queue setters for concurrency, worker concurrency where public, limiter, priority enabled, polling interval, public enqueues, handles, and status APIs.
- Sequence: create backlog, observe pre-update limits, apply one live config change per case, release gates, and verify start waves plus terminal conservation.
- Variance: bounded seeds, request counts up to 8, polling interval down to 50ms, limiter periods up to 1.2s, and one config change per matrix row.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | dynamic config | concurrency increase is reloaded without losing queued rows | `concurrency 1 -> 3` with 6 queued requests | newly allowed starts appear after config update | max active starts follows model and all accepted finish |
| case-002 | dynamic config | concurrency decrease does not cancel or strand already accepted work | `concurrency 3 -> 1` while work is active | no new over-cap starts after current active set drains | terminal conservation plus active-count bound |
| case-003 | rate limiting | limiter update changes future start windows only | `limit 1/0.8s -> 3/0.8s` under backlog | later waves widen after update | start-window counts match limiter model |
| case-004 | polling | short polling interval does not execute delayed rows too early | update polling interval while delayed rows exist | workers observe config but delayed eligibility still holds | no early delayed ledger rows |
| case-005 | priority | enabling priority before backlog release affects eligible order | priority disabled at setup, enabled before priority enqueues | priority options become meaningful only after enabled | rejected/accepted options and ordering match model |
| case-006 | mixed config | sequential config changes do not corrupt row cleanup | apply concurrency then limiter update in one case | all accepted rows terminal and cleaned | ledger/result/status agreement |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 2123 | concurrency-increase | no dependency fault | 6 accepted requests, one blocker | config reload expands active starts |
| case-002 | 2129 | concurrency-decrease | no dependency fault | 5 accepted requests, 3 initially active | no post-decrease overstart |
| case-003 | 2131 | limiter-increase | no dependency fault | 6 accepted requests, limiter wave timestamps | limiter window model |
| case-004 | 2137 | polling-plus-delay | no dependency fault | 4 delayed requests and blocker | delay invariant under polling update |
| case-005 | 2141 | priority-enable-before-release | no dependency fault | 5 prioritized requests | priority option boundary |
| case-006 | 2143 | concurrency-then-limiter | no dependency fault | 8 accepted requests | cleanup after mixed config changes |

##### Invariants

- Must hold: accepted/rejected classification is conserved, start waves obey the modeled config in effect at the time, handle results match ledger, and no active modeled rows remain after cleanup polling.
- Eventually must hold: all accepted work is terminal within 15 seconds after final release/config update.
- Must never happen: a config-change case passes without recording queue config before and after the update.

##### Execution Map

- Suggested files to inspect: same as rung 001 plus database-backed queue setter tests near the top of `tests/test_queue.py`.
- Suggested command family: `python .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-003 --all-cases --sequential`.
- Setup assumptions: rung 001 green or useful; no DB restart or Kafka.
- Per-case evidence to record: queue config snapshots, update timestamps, status snapshots, active-count samples, limiter windows, ledger rows, terminal rows, cleanup poll results.
- Replay notes: persist derived config timeline in case JSON.

##### Expected Signatures

- Success: all 6 cases pass with config reload evidence and invariant artifacts.
- Finding: lost accepted rows, over-concurrency after config decrease, limiter window violation, early delayed execution, priority option mismatch, or cleanup leak.
- Setup block: queue setter API cannot be invoked from harness without product changes or config windows cannot be observed after bounded calibration.
- Low signal: config update happens before any backlog exists.
- Goal drift: runner turns this into a throughput benchmark rather than correctness with a ledger oracle.

##### Stop Conditions

- Stop when: 6 cases pass, one strong finding is recorded, or a setup/window blocker is reached.
- Escalate when: reliable active-count evidence requires intrusive product instrumentation rather than public handles and read-only status snapshots.

### Rung: rung-004-executor-relaunch-result-durability

Evidence source: `evidence-key:frontiers/queue-composed-controls/rungs/rung-004-executor-relaunch-result-durability.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-004-executor-relaunch-result-durability
frontier: queue-composed-controls
status: deferred
order: 4
level: failure
workload_file: .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py
seeds:
  - 2147
  - 2153
  - 2159
  - 2161
  - 2167
  - 2171
updated_at: 2026-06-19T21:52:30Z
```

#### Rung 004: Executor Relaunch Result Durability

##### Goal

- Build and run: extend the composed-controls workload to interrupt and relaunch the DBOS application process without restarting Postgres.
- Preserve: durable queue result retrieval and terminal cleanup after application interruption while composed controls are present.

##### Workload File

- Expected path: `.workers/workloads/queue-composed-controls/queue_composed_controls_workload.py`.
- Create or reuse: reuse the existing workload if it supports subprocess launch/relaunch cleanly; otherwise add a small subprocess mode inside the same file.
- Why one file is enough for this rung: the product promise, queue controls, ledger oracle, and Postgres state model remain the same. Only the executor lifecycle changes.

##### Workload Shape

- Type: background-job failure/recovery workload.
- Entry points: same public queue APIs plus harness-owned process start/stop of the workload worker mode.
- Sequence:
  - Start a DBOS worker process and register queue configuration.
  - Enqueue accepted and rejected requests from the controller side.
  - Stop the worker at a named point: after acceptance, after delayed transition, during backlog, or before result collection.
  - Relaunch a worker with the same app version and Postgres databases.
  - Retrieve results and inspect terminal/cleanup state.
- Variance: seeds choose shutdown point, request mix, release offset, and whether partitioned mode is used.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | recovery/result durability | accepted queued handles survive app interruption before execution | stop worker after accepted rows are durable, relaunch | queued work completes after relaunch | handle/status/ledger agreement |
| case-002 | timing/order | delayed rows remain delayed across relaunch | stop while rows are `DELAYED`, relaunch after eligibility | delayed work executes after eligibility, not before | delay invariant plus terminal conservation |
| case-003 | duplicate/replay | duplicate rejection before interruption does not become execution after relaunch | reject duplicate, stop worker, relaunch | rejected duplicate remains non-executed | no rejected duplicate ledger row |
| case-004 | partition isolation | blocked partition state survives relaunch without starving normal partition | stop with blocked partition pending, relaunch and release | normal partition progresses and blocked completes after release | partition oracle holds |
| case-005 | dynamic config | persisted queue config is visible after relaunch | update concurrency/limiter, stop, relaunch | relaunched worker uses persisted config | start waves match updated config |
| case-006 | cleanup | terminal cleanup after relaunch removes active queue rows | stop before final cleanup poll, relaunch | cleanup converges | no active modeled rows remain |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 2147 | stop-after-acceptance | app process stop/relaunch, no DB restart | 4 accepted requests | durable accepted rows complete |
| case-002 | 2153 | stop-while-delayed | app process stop/relaunch | 3 delayed requests | delayed eligibility across relaunch |
| case-003 | 2159 | duplicate-then-stop | app process stop/relaunch | 1 accepted dedupe, 1 rejected duplicate | rejected duplicate remains non-executed |
| case-004 | 2161 | partition-stop-release | app process stop/relaunch | blocked and normal partitions | partition isolation after relaunch |
| case-005 | 2167 | config-stop-relaunch | app process stop/relaunch | backlog plus live config update | persisted config reload |
| case-006 | 2171 | stop-before-cleanup | app process stop/relaunch | terminal work with cleanup pending | active row cleanup convergence |

##### Invariants

- Must hold: all accepted modeled requests either complete successfully or have an explicit terminal failure modeled by the case; rejected duplicates never execute; handle results and terminal SQL rows agree with the ledger; no modeled active rows remain after cleanup polling.
- Eventually must hold: after relaunch and healthy Postgres for 20 seconds, all accepted work is terminal and retrievable.
- Must never happen: this rung requires DB restart, Kafka, or product source modification.

##### Execution Map

- Suggested files to inspect: DBOS launch/destroy paths in `_dbos.py`, queue worker loop in `_queue.py`, and queue recovery examples in `tests/test_queue.py`.
- Suggested command family: `python .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-004 --all-cases --sequential`.
- Setup assumptions: same isolated Postgres service; runner can launch a harness subprocess or equivalent worker mode under `.workers/`.
- Per-case evidence to record: worker process IDs, stop/relaunch timestamps, queue config persisted before stop, status snapshots before stop and after relaunch, ledger rows, terminal rows, handle results, cleanup poll results.
- Replay notes: persist derived lifecycle schedule and product commit in case JSON.

##### Expected Signatures

- Success: all 6 cases pass with relaunch evidence and durable invariant artifacts.
- Finding: lost accepted work, duplicate side effect after relaunch, early delayed execution, partition starvation, config rollback, missing result, or active row leak.
- Setup block: clean process relaunch cannot be represented in WIO/harness scope after bounded setup.
- Low signal: worker stop happens only after all work is already terminal and cleaned.
- Goal drift: runner adds DB restart or recovery internals instead of app interruption without DB restart.

##### Stop Conditions

- Stop when: 6 cases pass, one strong finding is captured, or process-boundary setup is blocked.
- Escalate when: reliable relaunch requires changing product source or running a broad product test suite.

### Rung: rung-005-bounded-seed-sweep

Evidence source: `evidence-key:frontiers/queue-composed-controls/rungs/rung-005-bounded-seed-sweep.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-005-bounded-seed-sweep
frontier: queue-composed-controls
status: deferred
order: 5
level: sweep
workload_file: .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py
seeds:
  - 2201
  - 2203
  - 2207
  - 2213
  - 2221
  - 2237
  - 2239
  - 2243
  - 2251
  - 2267
  - 2269
  - 2273
  - 2281
  - 2287
  - 2293
  - 2297
  - 2309
  - 2311
  - 2333
  - 2339
  - 2341
  - 2347
  - 2351
  - 2357
updated_at: 2026-06-19T21:52:30Z
```

#### Rung 005: Bounded Seed Sweep

##### Goal

- Build and run: only after earlier rungs produce useful green or finding-prone evidence, run a bounded 24-case sweep using the same workload and oracle.
- Preserve: same queue composed-control product promise while searching for rare timing/order bugs across controlled seeds.

##### Workload File

- Expected path: `.workers/workloads/queue-composed-controls/queue_composed_controls_workload.py`.
- Create or reuse: reuse the existing workload; do not create a new generator or change the oracle.
- Why one file is enough for this rung: a sweep is depth for the existing workload, not a new area.

##### Workload Shape

- Type: bounded stateful seed sweep.
- Entry points: same as earlier rungs.
- Sequence: run 24 sequential cases, each derived from the seed into one of the already-proven schedule templates.
- Variance: seed chooses template, request count 3-10, delays 0.2-1.0s, release offsets 0-300ms, priority sets, partition keys, limiter windows, and optional executor relaunch only if rung 004 was green.

##### Attack Plan

| Case Family | Axis | Assumption Attacked | Perturbation | Oracle |
| --- | --- | --- | --- | --- |
| dedupe-delay | duplicate/replay and timing/order | duplicate rejection remains side-effect-free across varied delay windows | vary delay, duplicate offset, and release timing | rejected duplicate never executes; accepted executes once |
| priority-config | dynamic config and priority | live config reload preserves priority/order under backlog | vary concurrency, limiter, and priority lists | start waves and terminal conservation match model |
| partition | partition isolation | blocked partition cannot starve other partitions | vary partition cardinality and release offsets | normal partition bounded progress |
| relaunch | recovery/result durability | accepted results survive app relaunch | optional only after rung 004 proves setup | handle/status/ledger agreement |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| cases-001-006 | 2201..2237 | dedupe-delay templates | no dependency fault | 3-6 requests | duplicate plus delay variance |
| cases-007-012 | 2239..2273 | priority-config templates | no dependency fault | 5-10 requests | priority/config variance |
| cases-013-018 | 2281..2311 | partition templates | no dependency fault | 2-4 partitions | partition isolation variance |
| cases-019-024 | 2333..2357 | relaunch templates if enabled, otherwise mixed config | app process relaunch only when rung 004 green | 4-8 requests | durable result retrieval or mixed backlog |

##### Invariants

- Must hold: all invariants from the schedule template selected for each seed.
- Eventually must hold: all accepted work is terminal within the template's bounded healthy window.
- Must never happen: the sweep changes the product promise, oracle, or adversarial axis.

##### Execution Map

- Suggested command family: `python .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-005 --all-cases --sequential`.
- Setup assumptions: earlier rungs proved the workload has signal and the runner has enough budget for 24 sequential cases.
- Per-case evidence to record: seed, selected template, derived JSON plan, all relevant ledger/status/result artifacts, and cleanup evidence.
- Replay notes: persist each derived plan because seed-only replay is weak when generator logic changes.

##### Expected Signatures

- Success: all 24 cases pass with artifacts and no low-signal cases.
- Finding: any invariant violation from the selected template.
- Setup block: earlier rungs were not green/useful, or the environment cannot support bounded 24-case execution.
- Low signal: too many cases miss target windows or rely on timing without recorded predicates.
- Goal drift: runner treats the sweep as a new workload or broad benchmark.

##### Stop Conditions

- Stop when: 24 cases pass, one finding is captured, or more than 3 cases are low-signal due to missed target windows.
- Escalate when: a failure appears but the derived plan is too broad to replay without minimization.

### Rung: rung-006-finding-minimization

Evidence source: `evidence-key:frontiers/queue-composed-controls/rungs/rung-006-finding-minimization.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-006-finding-minimization
frontier: queue-composed-controls
status: deferred
order: 6
level: adversarial
workload_file: .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py
seeds:
  - finding-derived
  - finding-derived
  - finding-derived
updated_at: 2026-06-19T21:52:30Z
```

#### Rung 006: Finding Minimization

##### Goal

- Build and run: shrink a concrete queue composed-control finding from any earlier rung into one deterministic replay case.
- Preserve: the same product promise, adversarial axis, and invariant that failed.

##### Workload File

- Expected path: `.workers/workloads/queue-composed-controls/queue_composed_controls_workload.py`.
- Create or reuse: reuse the workload that found the issue; add a replay mode if needed.
- Why one file is enough for this rung: minimization should remove dimensions from the failing case, not invent a new harness.

##### Workload Shape

- Type: deterministic replay/minimization workload.
- Entry points: same as the failing rung.
- Sequence: start from the failing derived plan, remove optional requests, shrink delays/offsets, reduce partitions/priorities/config changes, and keep the smallest case that still violates the same invariant.
- Variance: none beyond the minimization attempts recorded in the run directory.

##### Attack Plan

| Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- |
| finding-derived | same as failing rung | shrink only inputs, request count, timing, or config knobs from the failing derived plan | invariant still fails with fewer moving parts | same failed invariant and evidence point |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | finding-derived | replay failing derived plan | same as finding | original failing data shape | confirm reproducibility |
| case-002 | finding-derived | minimized candidate | same as finding | smallest candidate after removing optional dimensions | preserve failure |
| case-003 | finding-derived | final minimized replay | same as finding | final reduced request/config plan | durable regression artifact |

##### Invariants

- Must hold for minimization quality:
  - The same invariant that failed earlier still fails.
  - The minimized plan records all generated inputs, timing offsets, config changes, and observed evidence.
  - Removed dimensions are listed so a future reducer can see what was proven unnecessary.
- Must never happen:
  - The minimization changes the oracle, switches to a different product promise, or declares a different failure as the same finding.

##### Execution Map

- Suggested command family: `python .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-006 --replay <case-json>`.
- Setup assumptions: an earlier rung produced a finding with derived case JSON.
- Per-case evidence to record: original finding path, minimized plan diff, replay command, failed invariant, ledger/status/result artifacts, and product commit.
- Replay notes: the final minimized JSON should be self-contained enough to run without the generator seed.

##### Expected Signatures

- Success: one minimized replay case reliably reproduces the same invariant failure.
- Finding: same as the earlier rung, now with a smaller replay artifact.
- Setup block: original finding artifacts are missing or cannot be replayed.
- Low signal: minimization only reproduces under broad timing luck without recorded interleaving predicates.
- Goal drift: runner switches to a new bug or broad sweep.

##### Stop Conditions

- Stop when: a minimized replay is captured, the original finding cannot be reproduced after 3 attempts, or the evidence shows the original case was low-signal.
- Escalate when: a product change is required to observe the same invariant.

### Loop-1 Added Rung: rung-007-async-partition-worker-concurrency

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-007-async-partition-worker-concurrency
frontier: queue-composed-controls
status: queued
order: 7
level: recent-churn
workload_file: .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py
seeds: [2431, 2437, 2441]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/pull/727
  - https://github.com/dbos-inc/dbos-transact-py/pull/729
  - target/dbos/_core.py
  - target/dbos/_queue.py
  - target/dbos/_sys_db.py
  - target/dbos/_context.py
  - target/tests/test_queue.py
  - .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-data-and-fixtures/overview.md
gate_results:
  surface_evidence: ready_from_pr_727_and_target_execute_workflow_by_id_partition_gap
  recent_issue_pr_flake_check: pr_727_checks_passed_all_python_postgres_sqlite_jobs
  oracle_critic: ready_with_per_partition_running_ledger_and_status_row_partition_oracle
  executor_feasibility: default_real_postgres_profile_reuses_existing_queue_workload
```

#### Product Promise

Partitioned DBOS queues enforce `worker_concurrency` independently for each
partition and preserve the persisted `queue_partition_key` in the dequeued
workflow context. Async queued workflows, child enqueues from queued workflow
bodies, live queue config reload, handle results, and cleanup must agree with
the partition ledger.

#### Why This Is New

Existing `rung-002` proves blocked-partition isolation with
`worker_concurrency=1`. Existing `rung-003` covers live queue config changes on
backlog. PR `#727` added a narrower async regression test after the pinned
target: each partition should independently saturate `worker_concurrency=2`,
and `execute_workflow_by_id` should propagate the persisted partition key into
`SetEnqueueOptions` while executing a dequeued workflow. This rung composes
those recent-churn edges in the workload instead of rerunning the product test.

#### Workload Shape

- Type: background-job/stateful async queue workload.
- Build profile: `default`.
- Runtime setup: real Postgres through `.workers/run-with-postgres.sh`; a
  SQLite run is diagnostic only because this frontier depends on durable queue
  rows and worker polling.
- Expected workload file: extend
  `.workers/workloads/queue-composed-controls/queue_composed_controls_workload.py`
  with a `rung-007-async-partition-worker-concurrency` mode.
- Entry points:
  - `DBOS.register_queue_async(..., partition_queue=True, worker_concurrency=...)`
  - `DBOS.enqueue_workflow_async`
  - `SetEnqueueOptions(queue_partition_key=...)`
  - queued async workflow body that records start/finish ledger rows
  - workflow handle `get_result`, public status APIs, and read-only queue rows
- Ledger:
  - independent per-partition running counters with monotonic timestamps
  - max running per partition
  - public status snapshots including `queue_partition_key`
  - child workflow IDs and child status rows when a parent enqueues a child

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 2431 | async-two-partition-saturation | two partitions each enqueue four gated async workflows with `worker_concurrency=2` | every partition independently reaches exactly two running workflows, total running may reach four, and no partition exceeds two |
| case-002 | 2437 | inherited-partition-child-enqueue | queued async parent runs from partition `parent-a` and enqueues a child on the same partitioned queue without an explicit new `SetEnqueueOptions` block | child status inherits `queue_partition_key=parent-a`, child result is retrievable, and no `queue_partition_key IS NULL` row is stranded |
| case-003 | 2441 | live-worker-concurrency-partition-update | start partitioned async backlog with `worker_concurrency=1`, update to `2` while both partitions have queued followers, then release gates | each partition expands to two running after config reload without cross-partition starvation, overstart, or lost results |

#### Invariants

- Must hold:
  - Every accepted workflow row on the partitioned queue has a non-null
    `queue_partition_key`.
  - For each partition, `max_running <= modeled_worker_concurrency` at all
    observed points.
  - In cases where capacity exists, every partition reaches the modeled
    saturation level independently before gates are released.
  - A child enqueued from a dequeued partitioned parent inherits the parent's
    partition key unless the case explicitly overrides it.
  - Public handle results, status rows, and ledger rows agree for every
    accepted parent and child workflow.
  - Active modeled queue rows are cleaned up after terminal completion.
- Eventually must hold:
  - After gates are released and Postgres is healthy for 15 seconds, all
    accepted workflows are terminal and retrievable.
- Must never happen:
  - The workload claims partition concurrency success after running only one
    partition or after releasing gates before saturation is observed.
  - The inherited-partition case uses an explicit inner
    `SetEnqueueOptions(queue_partition_key=...)`; that would remove the
    `execute_workflow_by_id` propagation risk.

#### Replay Commands

- Single case:
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-007-async-partition-worker-concurrency --case case-001`
- Full matrix:
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-007-async-partition-worker-concurrency --all-cases --sequential`

#### Expected Signatures

- Success: all three cases reach their target windows, per-partition running
  limits and saturation evidence match the model, child partition inheritance
  holds, handle results agree, and cleanup converges.
- Finding: shared worker-concurrency limit across partitions, per-partition
  overstart, child row with missing/wrong partition key, partition starvation,
  missing result, duplicate ledger effect, or stranded active row.
- Setup block: async queue worker polling, Postgres setup, or target-window
  calibration cannot reach the modeled saturation/inheritance windows.
- Low signal: only one partition runs, saturation is inferred from terminal
  counts rather than observed running counters, or child inheritance is masked
  by explicit partition options.

#### Stale Conditions

Mark stale if DBOS changes partitioned queue semantics, removes partition-key
context inheritance for child enqueues by design, changes worker-concurrency
accounting, or the target ref advances past PR `#727` and this rung should be
reframed as regression-proof rather than current pinned-target bug-hunt.

### Producer Added Rung: rung-008-rate-limit-partial-index-plan

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-008-rate-limit-partial-index-plan
frontier: queue-composed-controls
status: ready
order: 8
level: performance-regression
workload_file: .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py
seeds: [6960, 6961, 6962]
updated_at: 2026-06-24
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/issues/696
  - https://github.com/dbos-inc/dbos-transact-py/pull/698
  - target/dbos/_sys_db.py
  - target/dbos/_migration.py
  - target/dbos/_schemas/system_database.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/performance-load-and-stress-testing/overview.md
```

#### Product Promise

Rate-limited DBOS queues must keep the scheduler's recent-start counting query
scalable on Postgres. Under a realistic mix of many non-rate-limited rows, old
rate-limited rows, and a small recent rate-limited window, the query used by
`start_queued_workflows` must be eligible for and actually use
`idx_workflow_status_rate_limited` (or an equivalent partial-index access path)
while preserving modeled limiter behavior.

#### Why This Is New

Existing queue rungs prove functional rate-limit behavior: accepted work starts
in modeled windows, live limiter changes are obeyed, and terminal rows clean up.
Issue `#696` was different: the queue behavior could still be correct while
Postgres ignored the partial index because the ORM predicate compiled to
`rate_limited IS true` instead of matching the index predicate
`rate_limited = TRUE`, causing high CPU at production row counts. This rung adds
a query-plan and selectivity oracle rather than another behavioral seed sweep.

#### Workload Shape

- Type: Postgres-backed queue performance regression guard with functional
  sanity checks.
- Build profile: `default`.
- Runtime setup: real Postgres through `.workers/run-with-postgres.sh`; SQLite
  is not meaningful for this rung because the bug is Postgres partial-index
  predicate matching.
- Expected workload file: extend
  `.workers/workloads/queue-composed-controls/queue_composed_controls_workload.py`
  with a `rung-008-rate-limit-partial-index-plan` mode.
- Entry points:
  - `DBOS.register_queue` or `DBOS.register_queue_async` with a limiter.
  - `DBOS.enqueue_workflow` to seed enough real queue rows for DBOS schema
    compatibility, plus read-only or harness-owned SQL inserts if the executor
    needs larger historical row volume without running thousands of workflows.
  - `SystemDatabase.start_queued_workflows` query shape through normal queue
    polling where feasible.
  - Read-only SQL `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` for the
    rate-limit count predicate matching `target/dbos/_sys_db.py`.
- Data model:
  - many rows in the same system table with `rate_limited=false`;
  - many old `rate_limited=true` rows outside the limiter period;
  - a small recent `rate_limited=true` window for the tested queue;
  - optional partition key rows when the case requires partition selectivity.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 6960 | single-queue-rate-limit-plan | 50k non-rate-limited rows plus old/recent rate-limited rows for one queue | planner uses `idx_workflow_status_rate_limited` or equivalent partial-index scan for the recent-start count |
| case-002 | 6961 | multi-queue-selectivity-plan | many rows across unrelated queue names and one hot rate-limited queue | plan remains queue-selective and does not degrade to full `workflow_status` scan |
| case-003 | 6962 | partitioned-rate-limit-plan-plus-functional-sanity | partitioned queue with recent rows in two partitions and a small live enqueue run | partition filter preserves partial-index access and limiter still gates starts according to the modeled recent count |

#### Invariants

- Must hold:
  - The Postgres schema contains `idx_workflow_status_rate_limited` with a
    predicate equivalent to `rate_limited = TRUE`.
  - The generated `EXPLAIN` JSON for the rate-limit count query includes an
    index-backed plan using `idx_workflow_status_rate_limited` or an equivalent
    partial index explicitly recorded by name.
  - The plan must not contain a full sequential scan of `workflow_status` for
    the rate-limit count when the case data volume threshold is met.
  - The query returns the modeled recent-start count for the selected
    queue/partition.
  - For the functional sanity case, `start_queued_workflows` or the queue worker
    refuses to start more work when the modeled recent count is already at the
    limiter and starts work after the window advances.
- Performance sanity bound:
  - Record wall-clock query latency and buffer counts. Treat extreme latency or
    buffer growth as diagnostic unless paired with a plan/index invariant
    failure; avoid noisy absolute timing as the sole oracle.
- Must never happen:
  - The case passes by inspecting only the migration text without executing the
    compiled query plan against populated Postgres data.
  - The case passes on SQLite.
  - The case rewrites production SQL to a hand-authored query that no longer
    matches the predicate shape in `target/dbos/_sys_db.py`.

#### Replay Commands

- Single case:
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-008-rate-limit-partial-index-plan --case case-001 --seed 6960`
- Full matrix:
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-008-rate-limit-partial-index-plan --all-cases --sequential`

#### Evidence To Record

- Target commit and DBOS schema name.
- Seed, row counts by `queue_name`, `rate_limited`, status, partition key, and
  time window.
- The exact SQL text or SQLAlchemy-compiled SQL used for the rate-limit count.
- `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` output.
- Index definitions from `pg_indexes` for the system schema.
- Modeled and observed recent-start count.
- Functional sanity ledger for case 003.

#### Expected Signatures

- Success: all cases use the partial-index access path, return the modeled
  recent count, and preserve functional limiter behavior in case 003.
- Finding: missing/wrong partial index, compiled predicate that prevents partial
  index use, full sequential scan at modeled volume, wrong recent count, or
  limiter starts work despite the modeled recent count being at the limit.
- Setup block: Postgres setup unavailable, cannot populate enough rows inside
  the workload budget, or executor cannot obtain a production-equivalent
  compiled query without changing DBOS product code.
- Low signal: only measures wall-clock time, only checks behavior, or inspects
  migration strings without a populated-query plan.

#### Stale Conditions

Mark stale if DBOS replaces the rate-limit counting query, changes the partial
index predicate or name by design, removes `rate_limited`, or moves queue
scheduling to a different durable store.
