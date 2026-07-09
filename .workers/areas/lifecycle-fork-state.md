# Area: lifecycle-fork-state

## Current State

Current status: completed with two minimized findings; cancellation cascade and
terminal immutability regression corridor ready.

Promoted findings:

- Replacement children are used for fork execution but are not linked into the
  forked parent child graph; `delete_children=True` leaves replacements behind.
- `global_timeout` cancels a future `DELAYED` workflow.

Evidence:

- `evidence-key:findings/lifecycle-fork-state-replacement-children-missing-child-links.md`
- `evidence-key:findings/lifecycle-fork-state-global-timeout-cancels-delayed.md`
- `evidence-key:frontiers/lifecycle-fork-state/frontier.md`
- Target PR `#701` / merge `629b187`: `cancel_children=True` recursive
  cancellation through `DBOS.cancel_workflow` / `DBOSClient.cancel_workflow`.
- Target PR `#703` / merge `0cf79de`: guarded `update_workflow_outcome`
  preserves terminal `CANCELLED` during cancel-vs-complete races.

## Product Promise

Operators and clients can inspect, cancel, resume, fork, delete, time out,
recover, and query durable workflow state without resurrecting terminal work,
losing child/event/attribute ownership, or leaking system/application rows.

## What Not To Repeat

- Do not rediscover replacement-child graph ownership.
- Do not rediscover future delayed workflow cancellation by `global_timeout`.
- Do not add lifecycle rows unless the expected behavior is defensible from
  public APIs, code, docs, or product tests.

## Loop-1 Search Directions

| Direction | Why It Is Deeper |
|---|---|
| Fork plus event/stream prefixes under recovery | Replacement children was one graph bug; forked event/stream/attribute prefix behavior can fail differently. |
| Delete/cancel plus queue/result rows | Cleanup may be correct for workflow rows but wrong for queue or app transaction rows. |
| Lifecycle plus attributes query | Attribute query visibility after fork/delete/resume can disagree with workflow status. |
| Timeout plus retry/DLQ | Timeout interactions with retries and DLQ are distinct from delayed-row cancellation. |

## Rung Design Requirements

Every rung must define terminal-state immutability, descendant ownership, copied
prefixes, cleanup rows, and public query observations.

## Stale Conditions

Mark stale if DBOS clarifies replacement-child graph semantics or global timeout
semantics in code/docs.

Mark `rung-005-cancel-children-terminal-immutability` stale if DBOS changes the
public `cancel_children` default/meaning, allows cancelled workflows to complete
without explicit resume, or removes queued-row cleanup from cancellation.

## Rung Index

Evidence source: `evidence-key:frontiers/lifecycle-fork-state/rungs.index.yaml`

```yaml
schema_version: 1
columns: [id, path, status, order, level, workload_file, matrix, summary]
rungs:
  - [
      "rung-000-lifecycle-smoke",
      "rungs/rung-000-lifecycle-smoke.md",
      "not_run_optional",
      "0",
      "baseline",
      "read-only:/Users/viswa/code/workers/dbos-transact-py/tests/test_workflow_management.py",
      "1 case",
      "optional read-only product pytest smoke; skipped because cloud workload rungs established the harness",
    ]
  - [
      "rung-001-state-machine-core",
      "rungs/rung-001-state-machine-core.md",
      "passed",
      "1",
      "adversarial",
      ".workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py",
      "3 cases",
      "cloud and fresh local runs passed cancel/resume/delete/delay/success/error lifecycle state-machine invariants",
    ]
  - [
      "rung-002-child-fork-event-attributes",
      "rungs/rung-002-child-fork-event-attributes.md",
      "finding_minimized",
      "2",
      "adversarial",
      ".workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py",
      "6 cases + minimization",
      "cloud and fresh local runs found replacement_children are used for fork execution but not linked into the forked parent child graph; delete_children leaves replacements behind",
    ]
  - [
      "rung-003-recovery-during-management",
      "rungs/rung-003-recovery-during-management.md",
      "finding_minimized",
      "3",
      "failure",
      ".workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py",
      "7 cases",
      "cloud run found and minimized global_timeout cancelling a delayed workflow whose delay_until_epoch_ms is still in the future",
    ]
  - [
      "rung-004-bounded-seed-sweep",
      "rungs/rung-004-bounded-seed-sweep.md",
      "known_findings_confirmed",
      "4",
      "sweep",
      ".workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py",
      "24 cases",
      "24-case cloud bounded sweep completed; failures were duplicate confirmations of the two minimized lifecycle findings",
    ]
  - [
      "rung-005-cancel-children-terminal-immutability",
      "rungs/rung-005-cancel-children-terminal-immutability.md",
      "ready",
      "5",
      "adversarial",
      ".workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py",
      "4 cases",
      "executor-ready PR #701/#703 regression corridor for recursive cancellation, queued descendants, result retrieval, and terminal CANCELLED immutability",
    ]
```

## Rung Details

These are the known rung contracts and outcomes for this frontier. Treat completed, finding, and closed rungs as current reality and design constraints, not as active executor queue rows.

### Rung: rung-000-lifecycle-smoke

Evidence source: `evidence-key:frontiers/lifecycle-fork-state/rungs/rung-000-lifecycle-smoke.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-lifecycle-smoke
frontier: lifecycle-fork-state
status: ready
order: 0
level: baseline
workload_file: read-only:/Users/viswa/code/workers/dbos-transact-py/tests/test_workflow_management.py
seeds:
  - 3200
updated_at: 2026-06-20T07:48:29Z
```

#### Rung 000: Lifecycle Smoke

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072707741733000Z.prompt.md`.
- Frontier ID: `lifecycle-fork-state`.
- Rung ID: `rung-000-lifecycle-smoke`.
- Protected product promise: DBOS durable lifecycle management commands can run under the prepared Postgres product harness.
- Replay command: `.workers/run-with-postgres.sh .workers/python-runtime.sh -m pytest tests/test_workflow_management.py::test_cancel_resume_queue tests/test_workflow_management.py::test_fork_steps tests/test_workflow_management.py::test_delete_workflow tests/test_queue.py::test_delay_cancel_resume_list tests/test_attributes.py::test_update_workflow_attributes -q`.
- Seed policy: seed `3200` is recorded only as the baseline case ID anchor; this rung does not generate workload code.
- Invariant oracle: existing product tests must reach their semantic assertions; no new adversarial oracle is introduced at baseline.

##### Goal

- Build and run: a read-only product-native pytest smoke for cancel/resume, fork, delete, delayed cancel/resume, and attribute update setup.
- Preserve: product/runtime viability before Workload Runner creates `.workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py`.

##### Workload File

- Expected path: `read-only:/Users/viswa/code/workers/dbos-transact-py/tests/test_workflow_management.py` plus referenced queue/attribute test files.
- Create or reuse: reuse read-only product tests; do not create workload code for this baseline.
- Why one file is enough for this rung: this is a setup gate, not a frontier workload.
- When to create a new file instead: create the lifecycle workload only at `rung-001-state-machine-core`.

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3200 | product-pytest-smoke | none | DBOS Postgres app with workflow management tests | prepared target checkout, pytest runtime, Postgres migrations, queue delay, lifecycle APIs, and attribute APIs execute |


##### Invariants

- Must hold: selected product tests exit zero under Postgres.
- Must hold: setup failures are reported as dependency/Postgres/bootstrap blockers, not as lifecycle product findings.
- Must never happen: runner writes new adversarial workload code before this baseline if it chooses to run rung 000.

##### Execution Map

- Suggested files to inspect:
  - `.workers/run-with-postgres.sh`
  - `.workers/python-runtime.sh`
  - `.workers/build.sh`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_workflow_management.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_queue.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_attributes.py`
- Suggested command family:
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh -m pytest <targets> -q`
- Setup assumptions:
  - `.workers/build.sh` has prepared `.workers/vendor/dbos-transact-py` and `.workers/vendor/dbos-venv`.
  - Postgres is owned by `.workers/run-with-postgres.sh`.
- Per-case evidence to record:
  - target commit, pytest target list, exit code, Postgres readiness, and any setup-block message.
- Replay notes:
  - Record the exact pytest target list because this case intentionally aggregates several product tests.

##### Expected Signatures

- Success: pytest exits zero and reaches the named lifecycle product tests.
- Finding: none for this baseline; product assertion failures should trigger frontier review before claiming a workload finding.
- Setup block: missing prepared target checkout, Python dependency failure, Postgres startup failure, or migration failure.
- Low signal: command runs unrelated tests or checks only import success.
- Goal drift: runner implements lifecycle workload code in rung 000.

##### Stop Conditions

- Stop when: baseline passes, a setup blocker is documented, or a selected product test assertion fails and needs frontier revision.
- Escalate when: product source changes would be needed to run the baseline.

### Rung: rung-001-state-machine-core

Evidence source: `evidence-key:frontiers/lifecycle-fork-state/rungs/rung-001-state-machine-core.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-state-machine-core
frontier: lifecycle-fork-state
status: selected
order: 1
level: adversarial
workload_file: .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py
seeds:
  - 3210
  - 3211
  - 3212
updated_at: 2026-06-20T07:48:29Z
```

#### Rung 001: State Machine Core

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072707741733000Z.prompt.md`.
- Frontier ID: `lifecycle-fork-state`.
- Rung ID: `rung-001-state-machine-core`.
- Protected product promise: cancel, resume, delete, delay, success, and error lifecycle commands obey durable DBOS workflow-state legality.
- Replay command: `python .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-001-state-machine-core --case <case-id>`.
- Seed policy: exact seeds in front matter; each case must persist `case.json` with workflow IDs, gates reached, DBOS calls, offsets, and expected model transitions.
- Invariant oracle: independent lifecycle state machine checked after every command against `DBOS.get_workflow_status`, `DBOS.list_workflows`, `DBOS.list_queued_workflows`, and modeled side-effect counters.

##### Goal

- Build and run: the smallest adversarial DBOS lifecycle state-machine workload using public `DBOS` APIs.
- Preserve: terminal-state immutability, allowed cancellation/resume semantics, delayed queue clearing, and delete cleanup without adding fork/recovery complexity yet.

##### Workload File

- Expected path: `.workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py`.
- Create or reuse: create this one parameterized Python workload file for rung 001.
- Why one file is enough for this rung: all cases share the same actor, DBOS app setup, state-machine oracle, and public management API surface.
- When to create a new file instead: only if Workload Runner cannot keep later recovery process control separate from this deterministic lifecycle harness.

##### Workload Shape

- Type: Python module/integration stateful sequence.
- Entry points: `DBOS.start_workflow`, `DBOS.enqueue_workflow`, `DBOS.cancel_workflow`, `DBOS.resume_workflow`, `DBOS.delete_workflow`, `DBOS.set_workflow_delay`, `DBOS.get_workflow_status`, `DBOS.list_workflows`, `DBOS.list_queued_workflows`, `SetWorkflowID`, `SetEnqueueOptions`, `DBOS.workflow`, `DBOS.step`.
- Sequence:
  - Create an isolated Postgres DBOS app and deterministic workflow IDs with prefix `lfs-r001-<case>-<seed>`.
  - Define blocking workflows with step counters and gates such as `after_step_one`, `after_final_step_before_return`, and `release_workflow`.
  - Before each DBOS management call, update the independent model with the expected accepted or rejected transition.
  - After each call, query public status/list APIs and compare status, queue membership, completed timestamp presence, and side-effect counters.
- Variance: seed chooses workflow IDs and gate labels only; operation order is fixed per case for reproducible adversarial coverage.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | timing/order | `CANCELLED` cannot be overwritten by a late successful return | start workflow, record final step, block before return, call `DBOS.cancel_workflow`, release workflow, then call `DBOS.resume_workflow` twice | original handle raises `DBOSAwaitedWorkflowCancelledError`; first resume reaches `SUCCESS`; second resume does not rerun steps | final status sequence `PENDING -> CANCELLED -> SUCCESS`; step counters show checkpointed work is not duplicated |
| case-002 | timing/order | cancel/resume clears delayed queue state without stale queue leakage | enqueue with `SetEnqueueOptions(delay_seconds=60)`, verify `DELAYED`, call `DBOS.cancel_workflow`, assert absent from `list_queued_workflows`, then `DBOS.resume_workflow` | cancelled delayed workflow never dequeues before resume; resumed workflow runs immediately | no queued row while `CANCELLED`; final `SUCCESS`; `delay_until_epoch_ms` cleared or irrelevant after resume |
| case-003 | invalid transitions | `SUCCESS` and `ERROR` are immutable under stale operator commands | run one successful workflow and one failing workflow, then call `cancel_workflow`, `resume_workflow`, `set_workflow_delay`, and `delete_workflow(delete_children=False)` in modeled order | success/error rows ignore cancel/resume/delay until explicit delete; failing result remains error | status/side effects unchanged before delete; deleted IDs no longer appear in `list_workflows` |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3210 | cancel-after-final-step-before-return | none | blocking workflow with two durable steps and one return gate | guarded `CANCELLED` terminal state and idempotent resume |
| case-002 | 3211 | delayed-cancel-before-poller-then-resume | none | queued workflow with `delay_seconds=60` and 100 ms poller | delayed queue cleanup, cancelled queue absence, immediate resume |
| case-003 | 3212 | stale-commands-after-success-and-error | none | one successful workflow plus one workflow raising deterministic exception | terminal success/error immutability before explicit delete |


##### Invariants

- Must hold: every DBOS management operation is classified as allowed, no-op, or delete in the independent model before observing DBOS state.
- Must hold: status rows match the model after each command using `DBOS.get_workflow_status`.
- Must hold: `list_workflows(workflow_ids=[...])` returns exactly the modeled live workflow IDs before delete and none after delete.
- Must hold: `list_queued_workflows()` excludes cancelled and deleted workflow IDs.
- Must hold: step/transaction counters are exactly the modeled count; resume cannot duplicate checkpointed steps.
- Eventually must hold: resumed workflows reach `SUCCESS` within a bounded wait after gates open.
- Must never happen: `SUCCESS` or `ERROR` flips to `ENQUEUED`, `PENDING`, `DELAYED`, or `CANCELLED` due to stale lifecycle commands.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_dbos.py` around lifecycle API methods.
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_sys_db.py` around `update_workflow_outcome`, `cancel_workflows`, `resume_workflows`, `set_workflow_delay`, and `delete_workflows`.
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_workflow_management.py::test_cancel_resume_queue`.
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_queue.py::test_delay_cancel_resume_list`.
- Suggested command family:
  - `python .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-001-state-machine-core --case case-001`
  - `python .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-001-state-machine-core --all-cases --sequential`
- Setup assumptions:
  - Use real Postgres through `.workers/run-with-postgres.sh` in cloud.
  - Cases must isolate DBOS application ID/schema or wipe the owned DB before each case.
- Per-case evidence to record:
  - seed, derived operation list, workflow IDs, gate timestamps, management calls, statuses after each command, queued-list snapshots, side-effect counters, and exception class names.
- Replay notes:
  - Persist gate order and offsets; seed alone is insufficient if a cancellation window was calibrated.

##### Expected Signatures

- Success: all three cases reach their named windows and all invariants pass.
- Finding: terminal status flip, duplicate step count, delayed queue row leakage, stale delay metadata causing resumed workflow not to run, or delete leaving a queryable status row.
- Setup block: unable to create an isolated Postgres-backed DBOS app or deterministic blocking workflow.
- Low signal: workload only reruns existing product tests or checks final completion without per-command model comparisons.
- Goal drift: workload adds fork/recovery/event-stream complexity before this core state machine is implemented.

##### Stop Conditions

- Stop when: all three cases pass sequentially with replay artifacts, one strong invariant violation is captured, or target windows cannot be reached within 12 calibration attempts.
- Escalate when: Workload Runner needs product source edits, existing workload code, or a different oracle.

### Rung: rung-002-child-fork-event-attributes

Evidence source: `evidence-key:frontiers/lifecycle-fork-state/rungs/rung-002-child-fork-event-attributes.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-002-child-fork-event-attributes
frontier: lifecycle-fork-state
status: ready
order: 2
level: adversarial
workload_file: .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py
seeds:
  - 3220
  - 3221
  - 3222
  - 3223
  - 3224
  - 3225
updated_at: 2026-06-20T08:06:00Z
```

#### Rung 002: Child Fork Event Attributes

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072707741733000Z.prompt.md`.
- Frontier ID: `lifecycle-fork-state`.
- Rung ID: `rung-002-child-fork-event-attributes`.
- Protected product promise: forked and child workflow state carries exactly the modeled steps, events, streams, attributes, and delete semantics.
- Replay command: `python .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-002-child-fork-event-attributes --case <case-id>`.
- Seed policy: exact seeds in front matter; persist fork start step, original/fork IDs, child replacement map, modeled event/stream prefixes, attributes, and delete mode.
- Invariant oracle: independent fork graph model checked against `forked_from`, `was_forked_from`, child links, `get_all_events`, stream reads, `list_workflows(attributes=...)`, and application transaction-output rows.

##### Goal

- Build and run: the same lifecycle workload expanded into fork graph and state-copy boundaries.
- Preserve: exact fork prefix copying, child replacement, event/stream retention, attribute filtering, and child delete cleanup.

##### Workload File

- Expected path: `.workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py`.
- Create or reuse: reuse `.workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py` from rung 001 and add fork graph cases behind this rung ID.
- Why one file is enough for this rung: the actor, Postgres setup, and state-machine oracle are the same; only the modeled graph and copied state dimensions expand.
- When to create a new file instead: only if fork graph setup makes the core lifecycle oracle unreadable in one parameterized module.

##### Workload Shape

- Type: Python module/integration stateful fork graph workload.
- Entry points: `DBOS.start_workflow`, `DBOS.fork_workflow`, `DBOS.delete_workflow`, `DBOS.update_workflow_attributes`, `DBOS.list_workflows`, `DBOS.get_event`, `DBOS.read_stream`, `DBOS.write_stream`, `DBOS.close_stream`, `DBOS.set_event`, `DBOS.start_workflow` for child workflows, `DBOSClient.fork_workflow`, and read-only child/transaction-output inspection when public APIs are insufficient.
- Sequence:
  - Create a root workflow that records steps, events, streams, attributes, and child workflow IDs in a deterministic order.
  - Update the independent fork graph model before each fork/delete/attribute operation.
  - Fork at explicit start steps and hold fork completion behind a gate long enough to inspect copied prefixes.
  - For child replacement cases, fork selected children first, then fork the parent with a replacement map.
  - Compare public status and client-visible event/stream/attribute outputs to the model before and after forked execution.
- Variance: seed controls IDs and payload values; fork start steps, replacement maps, and delete modes are fixed per case.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | stale state/cache | fork copies only modeled step outputs before the selected start step | run a 4-step workflow, fork at steps 1, 2, and 4, then change step payload source before releasing forks | forked results differ only for re-executed suffix steps | original step counters stay fixed; fork suffix counters match start-step model |
| case-002 | stale state/cache | forked event state reflects fork point, not latest original mutation | original sets `key1=v1`, later `key1=v2`, then forks at event boundaries | fork at early point sees `v1` or no key as modeled; later fork sees `v2` | `get_event` and `get_all_events` equal modeled event prefix |
| case-003 | stale state/cache | stream prefix copying is per key and gapless | original writes interleaved streams and closes one key before fork | fork reads only modeled prefix and close state | `read_stream` values and closure match model per stream key |
| case-004 | data shape/boundary | workflow attributes used for listing survive fork/update/delete | set attributes at start, update original, fork, clear original attributes | fork and original appear in `list_workflows(attributes=...)` only when modeled | filtered list IDs equal model after each update |
| case-005 | invalid transitions | replacement children affect only the forked parent | fork children 0, 2, 4 with changed step multiplier, fork parent with replacement map | forked parent result uses replacements; original parent/children stay unchanged | child graph and results match replacement map exactly |
| case-006 | recovery and cleanup | delete mode removes exactly modeled descendants and app transaction rows | delete one parent with `delete_children=false`, another with `delete_children=true` | first leaves children queryable; second removes child statuses and transaction outputs | status rows and transaction-output rows match delete mode |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3220 | fork-step-prefixes-1-2-4 | none | 4-step workflow with mutable suffix payload | exact step-output copy boundary and no original mutation |
| case-002 | 3221 | fork-event-prefix-before-after-update | none | `set_event` before and after fork points | event prefix visibility by fork start step |
| case-003 | 3222 | fork-stream-interleaved-prefix | none | two stream keys, interleaved writes, one close | per-key stream order, prefix, and close retention |
| case-004 | 3223 | fork-attribute-update-clear-filter | none | attributes with string, int, and boolean values | attribute inheritance and list filter correctness |
| case-005 | 3224 | replacement-children-parent-refork | none | five children, three replacements, changed child multiplier | replacement child graph and parent result correctness |
| case-006 | 3225 | delete-parent-with-and-without-children | none | parent/child workflows with transaction outputs | child status and app transaction cleanup semantics |


##### Invariants

- Must hold: every fork is represented in the model with `original`, `forked_from`, `start_step`, copied prefix, and re-executed suffix before DBOS state is inspected.
- Must hold: original workflow rows and child rows are not mutated by forked execution except for documented `was_forked_from` metadata.
- Must hold: fork event and stream reads equal modeled prefixes; no post-fork original mutation leaks into an earlier fork.
- Must hold: attribute replacement and clearing use whole-dict semantics; list filters return exactly modeled workflow IDs.
- Must hold: delete cleanup removes exactly the modeled statuses and application transaction outputs.
- Must never happen: the workload passes without inspecting forked state before the forked suffix runs when that case requires prefix evidence.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_workflow_commands.py`
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_client.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_workflow_management.py::test_fork_steps`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_workflow_management.py::test_fork_events`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_workflow_management.py::test_fork_streams`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_workflow_management.py::test_fork_replacement_children`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_attributes.py::test_update_workflow_attributes`
- Suggested command family:
  - `python .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-002-child-fork-event-attributes --case case-001`
  - `python .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-002-child-fork-event-attributes --all-cases --sequential`
- Setup assumptions:
  - Rung 001 either exists or the runner implements the shared state-model harness before this rung.
  - Use real Postgres and isolated DBOS application state.
- Per-case evidence to record:
  - seed, fork graph JSON, start steps, replacement map, event/stream model, attributes after each operation, list filter results, child IDs, transaction-output row counts, and product commit.
- Replay notes:
  - Persist fork graph and copied-prefix model; seed alone is not enough after fork start steps are calibrated.

##### Expected Signatures

- Success: all six cases pass with fork graph, prefix, attribute, child, and delete artifacts.
- Finding: fork prefix mismatch, post-fork state leak, original row mutation, stale attribute filter, wrong replacement child result, or orphaned/deleted child/application row contrary to model.
- Setup block: dependency bootstrap, DB isolation, optional service provisioning, or target-window calibration prevents execution under the allowed scope.
- Low signal: runner only calls existing fork tests or checks final result without comparing copied event/stream/attribute prefixes.
- Goal drift: runner adds recovery/process restart before this fork graph oracle is stable.

##### Stop Conditions

- Stop when: all six matrix cases pass, one strong fork/model mismatch is captured, or setup blocks the shared harness.
- Escalate when: reaching the target requires product source edits, existing workload code, unbounded compute, or a different oracle.

### Rung: rung-003-recovery-during-management

Evidence source: `evidence-key:frontiers/lifecycle-fork-state/rungs/rung-003-recovery-during-management.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-003-recovery-during-management
frontier: lifecycle-fork-state
status: ready
order: 3
level: failure
workload_file: .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py
seeds:
  - 3230
  - 3231
  - 3232
  - 3233
  - 3234
  - 3235
updated_at: 2026-06-20T08:06:00Z
```

#### Rung 003: Recovery During Management

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072707741733000Z.prompt.md`.
- Frontier ID: `lifecycle-fork-state`.
- Rung ID: `rung-003-recovery-during-management`.
- Protected product promise: lifecycle commands remain legal and durable when recovery or executor interruption races with management actions.
- Replay command: `python .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-003-recovery-during-management --case <case-id>`.
- Seed policy: exact seeds in front matter; persist recovery window, executor IDs, workflow IDs, lifecycle command order, recovery handles, and final row/status model.
- Invariant oracle: same lifecycle state model as rungs 001-002 plus recovery ownership, dead-executor pending-row, completed-step count, DLQ, and cleanup invariants.

##### Goal

- Build and run: recovery/process-fault windows around blocked lifecycle operations.
- Preserve: one terminal state, no duplicate completed effects, no dead-executor pending rows, and no cleanup of active modeled work.

##### Workload File

- Expected path: `.workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration safety-liveness simulation.
- Entry points: `DBOS.start_workflow`, `DBOS._recover_pending_workflows`, workflow handle result/status APIs, `DBOS.cancel_workflow`, `DBOS.resume_workflow`, `DBOS.fork_workflow`, `DBOS.delete_workflow`, `garbage_collect`, `global_timeout`, and read-only system-row inspection.
- Sequence:
  - Start blocked workflows under executor `lfs-r003-a`; mark completed steps in an independent ledger.
  - Trigger recovery under executor `lfs-r003-b` at the matrix window.
  - Issue the modeled lifecycle command before claim, after handle creation, during recovered execution, or after result retrieval.
  - Restore healthy execution, release gates, then compare model, handle results, status rows, executor IDs, and side-effect ledger.
- Variance: seed chooses IDs and gate names; recovery window is fixed per case.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | partial failure/recovery | cancel before recovery claim must win over recovered execution | put workflow `PENDING`, call `cancel_workflow`, then recover dead executor | recovery does not execute cancelled body | final `CANCELLED`, no new side-effect ledger rows |
| case-002 | partial failure/recovery | resume after recovery handle creation must not duplicate completed steps | recover blocked workflow, then `resume_workflow` same ID before release | one resumed/recovered path reaches success | completed-step ledger count remains one per step |
| case-003 | partial failure/recovery | fork during recovered execution must copy only completed prefix | recover workflow blocked after step 1, fork from step 2 before releasing original | fork re-executes suffix; original recovers once | original and fork results/statuses match model without duplicate prefix effects |
| case-004 | retry/idempotency | DLQ and resume commute with lifecycle model | force max recovery attempts, then resume and recover | DLQ status is terminal until resume; resume clears DLQ for new execution | status sequence and recovery count match model |
| case-005 | recovery and cleanup | `global_timeout` cannot timeout the wrong lifecycle state | run timeout sweep near pending, cancelled, delayed, and successful workflows | only modeled expired pending rows change | terminal states and delayed rows remain legal |
| case-006 | recovery and cleanup | `garbage_collect` cannot delete active or queued lifecycle work | run cleanup while delayed/enqueued/pending/forked rows exist | only model-deletable completed/cancelled rows disappear | active queued/pending/fork rows remain queryable |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3230 | cancel-before-recovery-claim | executor handoff without DB restart | blocked workflow after completed step 1 | cancelled row is not recovered/executed |
| case-002 | 3231 | resume-after-recovery-handle-before-release | executor handoff | blocked recovered workflow with handle returned | no duplicate completed-step effect |
| case-003 | 3232 | fork-during-recovered-execution | executor handoff | recovered workflow plus fork from step 2 | original/fork prefix and suffix effects match model |
| case-004 | 3233 | dlq-resume-recover | repeated recovery attempts | max recovery attempts, then resume | DLQ terminal until resume and recovery succeeds once |
| case-005 | 3234 | timeout-sweep-near-lifecycle-states | timer/cleanup sweep | pending, cancelled, delayed, success rows | only modeled timeout rows change |
| case-006 | 3235 | garbage-collect-active-forked-queued | cleanup sweep | completed, pending, delayed, forked, child rows | active/queued/fork graph survives cleanup |


##### Invariants

- Must hold: every recovery and lifecycle command is ordered in the model before status rows are read.
- Must hold: no modeled workflow remains `PENDING` under a dead executor after healthy recovery unless the model says it was cancelled/deleted.
- Must hold: completed-step side-effect counts remain exactly one across recovery/resume/fork.
- Must hold: cleanup and timeout jobs change only modeled rows.
- Eventually must hold: after a healthy recovery window, each non-cancelled/non-deleted modeled workflow reaches success, error, or DLQ as modeled.
- Must never happen: recovery resurrects a cancelled/deleted workflow or cleanup removes delayed/queued/pending/forked active work.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_recovery.py`
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_sys_db.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_failures.py::test_recovery_during_retries`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_failures.py::test_dead_letter_queue`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_workflow_management.py::test_garbage_collection`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_workflow_management.py::test_global_timeout`
- Suggested command family:
  - `python .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-003-recovery-during-management --case case-001`
  - `python .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-003-recovery-during-management --all-cases --sequential`
- Setup assumptions:
  - Use real Postgres and isolated executor IDs.
  - Recovery windows may use product-native recovery APIs but must not modify product source.
- Per-case evidence to record:
  - seed, executor IDs, workflow IDs, recovery handle IDs, lifecycle command order, status rows, side-effect ledger counts, cleanup/timeout cutoff, and product commit.
- Replay notes:
  - Persist recovery window and gate timestamps; seed alone is insufficient for recovery interleavings.

##### Expected Signatures

- Success: all six recovery/cleanup cases reach their target windows and all model invariants pass.
- Finding: dead-executor pending row, duplicate completed-step side effect, cancelled/deleted workflow recovered, cleanup deleting active work, timeout mutating a terminal/delayed row, or model/status disagreement.
- Setup block: dependency bootstrap, DB isolation, optional service provisioning, or target-window calibration prevents execution under the allowed scope.
- Low signal: runner only calls existing failure tests or does not prove the recovery window was reached.
- Goal drift: runner adds DB restart chaos or queue-specific controls beyond lifecycle/recovery state.

##### Stop Conditions

- Stop when: all matrix cases pass with artifacts, one strong finding is captured, or a setup/window blocker is documented.
- Escalate when: reaching the target requires product source edits, existing workload code, unbounded compute, or a different oracle.

### Rung: rung-004-bounded-seed-sweep

Evidence source: `evidence-key:frontiers/lifecycle-fork-state/rungs/rung-004-bounded-seed-sweep.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-004-bounded-seed-sweep
frontier: lifecycle-fork-state
status: ready
order: 4
level: sweep
workload_file: .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py
seeds:
  - 3240
  - 3241
  - 3242
  - 3243
  - 3244
  - 3245
  - 3246
  - 3247
  - 3248
  - 3249
  - 3250
  - 3251
  - 3252
  - 3253
  - 3254
  - 3255
  - 3256
  - 3257
  - 3258
  - 3259
  - 3260
  - 3261
  - 3262
  - 3263
updated_at: 2026-06-20T08:06:00Z
```

#### Rung 004: Bounded Seed Sweep

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072707741733000Z.prompt.md`.
- Frontier ID: `lifecycle-fork-state`.
- Rung ID: `rung-004-bounded-seed-sweep`.
- Protected product promise: the lifecycle state-machine, fork graph, recovery, timeout, and cleanup invariants survive a bounded cross-product of the earlier concrete axes.
- Replay command: `python .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-004-bounded-seed-sweep --case <case-id>`.
- Seed policy: exact seeds in front matter; each case must persist the selected axis tuple, workflow graph, operation sequence, gate timestamps, and expected model transitions.
- Invariant oracle: reuse the independent lifecycle/fork/recovery model from rungs 001-003; the sweep may vary ordering and payloads but must not weaken any invariant.

##### Goal

- Build and run: a bounded rare-bug search after the concrete lifecycle, fork, and recovery rungs have proven stable.
- Preserve: the same public DBOS lifecycle APIs, isolated Postgres setup, replay artifacts, and model checks from earlier rungs.

##### Workload File

- Expected path: `.workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py`.
- Create or reuse: reuse `.workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py`.
- Why one file is enough for this rung: this is a sweep over the already-modeled axes; it should not introduce a new actor or oracle.
- When to create a new file instead: only if a minimized finding proves a separate deterministic regression harness is clearer.

##### Workload Shape

- Type: Python module/integration stateful sweep.
- Entry points: the union of rungs 001-003, especially `cancel_workflow`, `resume_workflow`, `fork_workflow`, `delete_workflow`, `update_workflow_attributes`, `set_workflow_delay`, `_recover_pending_workflows`, `global_timeout`, `garbage_collect`, `list_workflows`, and `list_queued_workflows`.
- Sequence:
  - Select one bounded axis tuple per case from the matrix below.
  - Generate deterministic workflow IDs and payloads from the seed.
  - Build the model and operation list before executing any DBOS calls.
  - Execute sequentially, checking invariants after each meaningful lifecycle/fork/recovery operation.
- Variance: seed controls IDs and payload values; the matrix controls the adversarial axis tuple.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001..case-006 | operation order | lifecycle command ordering around terminal states | rotate cancel/resume/delete/delay around success/error/cancelled rows | each operation reaches the modeled precondition | status/list/side-effect model agrees after every command |
| case-007..case-012 | fork graph | fork start step and copied prefix boundaries | vary start steps, event/stream payloads, and child replacement maps | fork graph and copied prefix are materialized | fork/event/stream/attribute model agrees |
| case-013..case-018 | recovery timing | recovery and lifecycle commands commute safely | vary recovery window before claim, after handle, and during blocked suffix | recovery window is proven by trace artifact | no dead pending row, duplicate effect, or illegal resurrection |
| case-019..case-024 | cleanup timing | timeout/garbage collection do not delete active work | vary cutoff, delayed/queued/pending/forked rows, and delete_children mode | cleanup job runs against modeled mixed state | only model-deletable rows disappear |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3240 | cancel-resume-delete-after-final-step | none | blocked two-step workflow | terminal cancel/resume/delete legality |
| case-002 | 3241 | success-stale-commands | none | successful workflow plus stale operator commands | success immutability |
| case-003 | 3242 | error-stale-commands | none | failing workflow plus stale operator commands | error immutability |
| case-004 | 3243 | delayed-cancel-resume | none | delayed queue workflow | queue metadata cleanup |
| case-005 | 3244 | delete-children-false | none | parent/child graph | child survival and app row retention |
| case-006 | 3245 | delete-children-true | none | parent/child graph | child deletion and app row cleanup |
| case-007 | 3246 | fork-step-1 | none | 4-step workflow with events/streams | fork from start step 1 |
| case-008 | 3247 | fork-step-2 | none | 4-step workflow with events/streams | fork from start step 2 |
| case-009 | 3248 | fork-step-4 | none | 4-step workflow with events/streams | fork from start step 4 |
| case-010 | 3249 | fork-attribute-filter | none | attributes replaced and cleared | attribute inheritance and filtering |
| case-011 | 3250 | fork-replacement-children | none | five child workflows and replacement map | replacement graph correctness |
| case-012 | 3251 | fork-stream-close-prefix | none | stream close before/after fork | stream prefix and close semantics |
| case-013 | 3252 | recovery-before-claim-cancel | executor handoff | pending blocked workflow | cancel wins before recovery claim |
| case-014 | 3253 | recovery-after-handle-resume | executor handoff | recovered handle before release | no duplicate completed step |
| case-015 | 3254 | recovery-during-fork | executor handoff | recovered original plus fork | fork/original liveness and no duplicate prefix |
| case-016 | 3255 | dlq-resume | repeated recovery | max attempts exceeded then resume | DLQ terminal until resume |
| case-017 | 3256 | recovery-delete-before-result | executor handoff | pending workflow deleted before result | no resurrected deleted row |
| case-018 | 3257 | recovery-attribute-update | executor handoff | recovered workflow plus attribute update | attribute update recorded once |
| case-019 | 3258 | timeout-pending-only | timeout sweep | pending, success, cancelled rows | timeout changes only eligible pending row |
| case-020 | 3259 | timeout-delayed-protected | timeout sweep | delayed queued row and pending row | delayed row remains legal |
| case-021 | 3260 | gc-completed-cutoff | cleanup sweep | completed rows around cutoff | only old completed rows removed |
| case-022 | 3261 | gc-active-protected | cleanup sweep | pending/delayed/forked active rows | active rows remain |
| case-023 | 3262 | gc-child-delete-mode | cleanup sweep | parent/child graph | delete mode respected |
| case-024 | 3263 | mixed-cleanup-recovery | cleanup plus recovery | recovered, cancelled, forked, delayed rows | model/state agreement after both jobs |


##### Invariants

- Must hold: every generated operation is classified by the lifecycle/fork/recovery model before DBOS state is inspected.
- Must hold: invariants from rungs 001-003 remain active for every case.
- Must hold: each case records its axis tuple and generated operation list so a finding can be minimized.
- Eventually must hold: non-cancelled/non-deleted live workflows reach the modeled terminal state inside the bounded healthy window.
- Must never happen: the sweep passes a case that did not exercise the selected axis tuple.

##### Execution Map

- Suggested files to inspect:
  - All files listed in rungs 001-003.
  - The generated replay artifacts from successful rungs 001-003 before starting the sweep.
- Suggested command family:
  - `python .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-004-bounded-seed-sweep --case case-001`
  - `python .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-004-bounded-seed-sweep --all-cases --sequential`
- Setup assumptions:
  - Run only after rungs 001-003 have either passed or produced stable enough artifacts to justify a sweep.
  - Use isolated Postgres and sequential case execution; do not parallelize the sweep until resource isolation is explicit.
- Per-case evidence to record:
  - seed, axis tuple, generated operation list, workflow graph, expected model states, observed public statuses, durable row snapshots, side-effect counters, product commit, and minimized failure candidate if any.
- Replay notes:
  - Persist the full generated operation sequence; seed alone is not enough for a replayable sweep failure.

##### Expected Signatures

- Success: all 24 bounded cases execute sequentially and preserve the lifecycle/fork/recovery/cleanup invariants.
- Finding: any invariant failure from rungs 001-003, plus any generated sequence whose minimized replay demonstrates a new lifecycle state-model bug.
- Setup block: dependency bootstrap, DB isolation, optional service provisioning, or target-window calibration prevents execution under the allowed scope.
- Low signal: runner only increases seed count without recording axis tuples or per-operation model checks.
- Goal drift: runner adds unrelated DBOS surfaces or broad load instead of sweeping the lifecycle model.

##### Stop Conditions

- Stop when: all matrix cases pass with artifacts, one strong finding is captured, or a setup/window blocker is documented.
- Escalate when: reaching the target requires product source edits, existing workload code, unbounded compute, or a different oracle.

### Rung: rung-005-cancel-children-terminal-immutability

Evidence source: `evidence-key:producer-20260624-pr701-pr703-cancel-children-terminal-immutability`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-005-cancel-children-terminal-immutability
frontier: lifecycle-fork-state
status: ready
order: 5
level: adversarial
workload_file: .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py
seeds:
  - 7010
  - 7011
  - 7030
  - 7031
updated_at: 2026-06-24
```

#### Rung 005: Cancel Children Terminal Immutability

##### Source Contract

- Evidence key: `evidence-key:producer-20260624-pr701-pr703-cancel-children-terminal-immutability`.
- Frontier ID: `lifecycle-fork-state`.
- Rung ID: `rung-005-cancel-children-terminal-immutability`.
- Protected product promise: recursive cancellation through public DBOS and client APIs cancels exactly the modeled workflow subtree, removes cancelled work from queues, keeps durable child ownership observable, and prevents a late workflow return from overwriting terminal `CANCELLED` until explicit resume.
- Replay command: `python .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-005-cancel-children-terminal-immutability --case <case-id> --seed <seed>`.
- Seed policy: exact seeds in the matrix; each case must persist workflow IDs, child graph, queue name, gate timestamps, cancellation call path, result calls, status/list snapshots, and expected model transitions.
- Invariant oracle: independent lifecycle graph model checked after every command against `DBOS.get_workflow_status`, `DBOS.list_workflows`, `DBOS.list_queued_workflows`, `DBOSClient` status/result APIs, child graph inspection, and side-effect counters.

##### Goal

- Build and run: a regression/finding corridor for PR `#701` and PR `#703` that composes recursive child cancellation with queue cleanup, result retrieval, and late completion races.
- Preserve: terminal `CANCELLED` immutability, explicit-resume semantics, recursive descendant ownership, and public/client observation parity.
- Avoid: replacement-child fork graph semantics and global timeout delayed-row cancellation, which are already promoted findings.

##### Workload File

- Expected path: `.workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py`.
- Create or reuse: reuse the lifecycle workload file and add this rung ID behind the existing state-model harness.
- Why one file is enough for this rung: it uses the same actor, Postgres setup, blocking workflow gates, status/list oracles, and child graph model as the lifecycle frontier.
- When to create a new file instead: only if DBOSClient-vs-runtime setup makes the shared lifecycle module ambiguous.

##### Workload Shape

- Type: Python module/integration stateful sequence.
- Entry points: `DBOS.start_workflow`, `DBOS.enqueue_workflow`, `DBOS.cancel_workflow(..., cancel_children=...)`, `DBOSClient.cancel_workflow(..., cancel_children=...)`, `DBOS.resume_workflow`, workflow handle `get_result` / `get_status`, `DBOS.get_workflow_status`, `DBOS.list_workflows`, `DBOS.list_queued_workflows`, `SetWorkflowID`, `DBOS.workflow`, and `DBOS.step`.
- Sequence:
  - Build deterministic parent, child, grandchild, and queued-child IDs using prefix `lfs-r005-<case>-<seed>`.
  - Block each workflow at a named gate after it has either started a child, enqueued a descendant, or completed its final durable step.
  - Update the independent graph/state model before issuing each cancel, release, result, or resume operation.
  - Query public runtime and client-facing APIs after each operation; record child graph, queue snapshots, status rows, result exceptions, and side-effect counters.
  - Release gates after cancellation to prove stale workflow bodies cannot overwrite `CANCELLED` with `SUCCESS` or `ERROR`.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | child ownership | recursive cancellation finds the full descendant tree and does not cancel by prefix accident | running parent starts child and grandchild; first cancel parent with `cancel_children=false`, then cancel same parent with `cancel_children=true` | first call cancels only parent; second call cancels child and grandchild; child graph remains inspectable | modeled graph statuses and child list match exactly after each command |
| case-002 | queue cleanup | recursive cancellation also handles queued descendants without leaking queue rows | parent starts a blocking child and enqueues a queued descendant behind a concurrency-1 blocker, then cancels parent with `cancel_children=true` | cancelled descendant leaves `list_queued_workflows`, blocker release does not run the cancelled descendant to success | queue snapshots, descendant status, and side-effect counters match model |
| case-003 | terminal race | late successful return cannot overwrite `CANCELLED` | workflow completes its final step, blocks before return, gets cancelled, then gate releases | original handle raises `DBOSAwaitedWorkflowCancelledError`; status remains `CANCELLED`; explicit resume reaches `SUCCESS` without duplicating the completed step | status sequence and step counter prove guarded outcome update and resume semantics |
| case-004 | client parity | client cancellation observes the same recursion/result semantics as runtime cancellation | use `DBOSClient.cancel_workflow(..., cancel_children=True)` against a running parent tree and retrieve statuses/results through client handles | client path cancels the modeled subtree, result calls raise for cancelled handles, and no runtime/client status disagreement appears | runtime and client observations compare equal for each modeled workflow ID |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 7010 | parent-child-grandchild-cancel-mode-toggle | none | three running workflows with blocking gates and durable step counters | `cancel_children=false` isolates parent; `cancel_children=true` recursively cancels descendants |
| case-002 | 7011 | recursive-cancel-with-queued-descendant | none | parent tree plus concurrency-1 queued descendant and blocked queue owner | cancelled queued descendant is removed from queue/listing and cannot complete after release |
| case-003 | 7030 | cancel-after-final-step-before-return | timing race | workflow with final durable step, cancellation before return, explicit resume afterward | terminal `CANCELLED` cannot be overwritten by late success and resume does not duplicate step output |
| case-004 | 7031 | client-recursive-cancel-result-parity | none | same tree as case 001 driven through `DBOSClient` cancel/status/result calls | client/runtime cancellation, result, and status semantics agree |

##### Invariants

- Must hold: `cancel_children=false` cancels only the requested workflow ID; descendants remain non-cancelled until a modeled recursive cancel.
- Must hold: `cancel_children=true` recursively cancels every modeled descendant exactly once, including queued descendants, and does not cancel unrelated workflow IDs sharing a prefix or queue.
- Must hold: cancelled workflows are absent from `list_queued_workflows` and have `queue_name`/dedupe behavior consistent with cancellation before any release gate is opened.
- Must hold: releasing a cancelled workflow body cannot change its status from `CANCELLED` to `SUCCESS` or `ERROR`; awaited result raises the modeled cancellation exception.
- Must hold: explicit `resume_workflow` is the only modeled path that can move a `CANCELLED` workflow back to executable state, and resumed execution does not duplicate already-recorded durable steps.
- Must hold: runtime and `DBOSClient` observations agree for status, result exception class, and recursive cancellation effect.
- Must never happen: the workload passes if it only asserts final completion, if a child was never actually linked before cancellation, or if the queued descendant never entered `ENQUEUED`.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_dbos.py` around `cancel_workflow`, `cancel_workflows`, `resume_workflow`, and `resume_workflows`.
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_client.py` around `DBOSClient.cancel_workflow` and result/status handles.
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_sys_db.py` around `update_workflow_outcome`, `cancel_workflows`, and `resume_workflows`.
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_workflow_management.py::test_cancel_after_final_step`.
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_workflow_management.py::test_cancel_workflow_children`.
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_queue.py::test_cancelling_queued_workflows`.
- Suggested command family:
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-005-cancel-children-terminal-immutability --case case-001 --seed 7010`
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-005-cancel-children-terminal-immutability --all-cases --sequential`
- Setup assumptions:
  - Use real Postgres through `.workers/run-with-postgres.sh`.
  - Cases run sequentially because they use named queues and blocking gates.
  - The workload writes replay artifacts under `/tmp/...`, not under `/workspace`.
- Per-case evidence to record:
  - target commit, seed, derived IDs, child graph, queue name, gate timestamps, cancel call path, `cancel_children` value, status/list snapshots after every command, result exception class, side-effect counters, and resume result when applicable.
- Replay notes:
  - Persist gate timestamps and model transitions; seed alone is not enough to reproduce the cancel-vs-complete timing window.

##### Expected Signatures

- Success: all four cases reach their modeled cancellation windows and preserve recursive cancellation, queue cleanup, terminal `CANCELLED`, explicit resume, and client parity invariants.
- Finding: descendant status not cancelled, unrelated workflow cancelled, cancelled queued row still listed/dequeued, cancelled workflow overwritten by `SUCCESS`/`ERROR`, duplicate durable step on resume, or runtime/client status/result disagreement.
- Setup block: cannot create an isolated Postgres-backed DBOS app, cannot reach child/queued descendant gates, or client handle setup is unavailable in the cloud profile.
- Low signal: workload only reruns product tests or checks final statuses without child graph, queue, result, and per-command model artifacts.
- Goal drift: workload investigates replacement-child fork ownership or global timeout delayed cancellation instead of the PR `#701` / `#703` cancellation semantics.

##### Stop Conditions

- Stop when: all four cases pass sequentially with replay artifacts, one strong invariant violation is captured, or the cancellation windows cannot be reached within 12 bounded calibration attempts.
- Escalate when: executor needs product source edits, archived workload implementations, unbounded parallelism, or a different oracle.
