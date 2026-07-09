# Area: serialization-error-fidelity

## Current State

Current status: one cloud-minimized finding, with loop-1 queued rungs for
portable structured error metadata fidelity and retry-class stored error result
retrieval liveness.

Promoted finding: native workflow under portable JSON config masks the modeled
application `ValueError` with a serializer infrastructure `TypeError`.

Evidence:

- `evidence-key:findings/serialization-error-fidelity-native-workflow-portable-config-masks-error.md`
- `evidence-key:frontiers/serialization-error-fidelity/frontier.md`
- `evidence-key:runs/run-20260620T214234Z-serialization-error-fidelity-rung-003-case-003-cloud-minimized/summary.md`
- Recent PR `#704` (`Fix get_result error poisoning`, merged 2026-06-03)
  split workflow-result row reads from `db_retry()` so a stored retry-class
  `DBAPIError` from a failed child workflow is returned promptly instead of
  being retried forever by result retrieval. The current target contains this
  fix and the narrow product regression test; a workload rung can preserve it
  across runtime/client/relaunch and parent-child result paths.

## Product Promise

Failed durable workflows preserve actionable application error type/message in
durable status and retrieval paths instead of replacing it with serializer
infrastructure noise. When an error is stored with DBOS portable error metadata,
public retrieval paths preserve the modeled `name`, `message`, `code`, and
JSON-compatible `data` fields.

## What Not To Repeat

- Do not rediscover the exact native workflow plus portable JSON config masking
  case.
- Do not call an error-fidelity issue a bug unless the expected durable error
  is grounded in DBOS config/serializer contract.

## Loop-1 Search Directions

| Direction | Why It Is Deeper |
|---|---|
| Retry and recovery error fidelity | The minimized finding used one failing step; retry/recovery can mask causes differently. |
| Nested exception causes | Chained exceptions and custom exception classes may lose root cause markers. |
| Client/handle/status parity | Public retrieval paths may disagree on the same durable error. |
| Serializer failure after partial output | Output serialization and exception serialization can interact after a step partially succeeds. |
| Portable structured error metadata | `PortableWorkflowError` and portable JSON error data can preserve `code` and `data` fields that marker-only rungs did not check. |
| Retry-class stored error retrieval | A stored DB/connection exception may look retryable to infrastructure wrappers; public result retrieval must raise the terminal application error promptly, not poll forever. |

## Rung Design Requirements

Every rung must define the expected error marker, retrieval paths, serializer
configuration, and which infrastructure errors are acceptable wrappers.
For structured portable error rungs, also define the expected portable
`name/message/code/data` fields and classify Python `__cause__`/`__context__`
observations as diagnostic unless the executor can anchor them in public
contract.

## Stale Conditions

Mark stale if DBOS changes serializer config semantics, native workflow error
storage, `PortableWorkflowError` semantics, portable JSON error-data fields, or
durable status error representation.

## Rung Index

Evidence source: `evidence-key:frontiers/serialization-error-fidelity/rungs.index.yaml`

```yaml
schema_version: 1
columns: [id, path, status, order, level, workload_file, matrix, summary]
rungs:
  - [
      "rung-000-serializer-smoke",
      "rungs/rung-000-serializer-smoke.md",
      "passed",
      "0",
      "baseline",
      ".workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py",
      "1 case",
      "prove serializer config variants and durable error retrieval",
    ]
  - [
      "rung-001-default-serializer-error",
      "rungs/rung-001-default-serializer-error.md",
      "passed",
      "1",
      "regression",
      ".workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py",
      "3 cases",
      "default portable serializer preserves original failing-step error",
    ]
  - [
      "rung-002-retry-recovery-error-records",
      "rungs/rung-002-retry-recovery-error-records.md",
      "passed",
      "2",
      "adversarial",
      ".workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py",
      "3 cases",
      "retry/recovery preserves original error across durable status and handle retrieval",
    ]
  - [
      "rung-003-config-matrix",
      "rungs/rung-003-config-matrix.md",
      "finding_minimized_cloud",
      "3",
      "contract",
      ".workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py",
      "3 cases",
      "case-003 cloud-minimized serializer masking to one native workflow under portable JSON config; app ValueError marker is replaced by portable JSON TypeError",
    ]
  - [
      "rung-004-portable-structured-error-metadata",
      "inline:loop-1-added-rung-rung-004-portable-structured-error-metadata",
      "queued",
      "4",
      "contract",
      ".workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py",
      "3 cases",
      "portable structured errors must preserve modeled name/message/code/data across status, handle, client, step, and relaunch read paths",
    ]
  - [
      "rung-005-retry-class-stored-error-result-liveness",
      "inline:loop-1-added-rung-rung-005-retry-class-stored-error-result-liveness",
      "queued",
      "5",
      "liveness",
      ".workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py",
      "3 cases",
      "stored retry-class DB errors must raise through public result retrieval promptly and consistently, not poison db_retry polling",
    ]
```

## Rung Details

These are the known rung contracts and outcomes for this frontier. Treat completed, finding, and closed rungs as current reality and design constraints, not as active executor queue rows.

### Loop-1 Added Rung: rung-004-portable-structured-error-metadata

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-004-portable-structured-error-metadata
frontier: serialization-error-fidelity
status: queued
order: 4
level: contract
workload_file: .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py
seeds: [3540, 3541, 3542]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - target/dbos/_serialization.py
  - target/dbos/_sys_db.py
  - target/tests/test_serialization.py
  - .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-data-and-fixtures/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-feedback-loops/overview.md
```

#### Product Promise

When a workflow or step fails with a DBOS portable structured error, DBOS must
store and return the actionable application error metadata, not just a generic
exception string. The modeled `name`, `message`, `code`, and JSON-compatible
`data` fields must agree across durable workflow status, workflow handle
retrieval, `DBOSClient` status/handle retrieval, failed step rows, and late or
relaunched reads.

#### Why This Is New

Existing serialization rungs checked whether an application marker survived
default, retry/recovery, and serializer-config paths. They did not assert the
portable structured error contract exposed by `PortableWorkflowError`,
`JsonWorkflowErrorData`, `exception_to_workflow_error_data()`, and
`deserialize_exception()` in `target/dbos/_serialization.py`. Target tests cover
portable error `name`/`message` for basic cases, but do not exercise `code` and
`data` parity across status, handle, client, step-list, and relaunch paths.

This rung does not rediscover the promoted native-workflow under portable JSON
config masking finding. It uses portable workflows or explicitly portable error
serialization and treats serializer infrastructure errors as forbidden only when
they replace modeled application metadata.

#### Workload Shape

- Type: API/client contract workload for durable error metadata.
- Build profile: `default`.
- Runtime setup: real Postgres through `.workers/run-with-postgres.sh`; SQLite
  may be used only as setup evidence, not as the final classification.
- Expected workload file: reuse
  `.workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py`
  unless executor readability requires a new file under the same frontier.
- Error model:
  - Structured error name: `WIOPortableStructuredError`.
  - Message marker:
    `WIO_STRUCTURED_ERROR frontier=serialization-error-fidelity rung=rung-004 case=<case> seed=<seed>`.
  - Code: deterministic string `WIO_CODE_<seed>_<case>`.
  - Data: JSON object with seed, case, modeled cause marker, phase, retry or
    relaunch flag, and a small nested object/list.
- Retrieval paths:
  - `DBOS.list_workflows(workflow_ids=[...])`.
  - `DBOS.retrieve_workflow(...).get_result()`.
  - `DBOSClient.list_workflows(workflow_ids=[...])`.
  - `DBOSClient.retrieve_workflow(...).get_result()`.
  - `DBOS.list_workflow_steps(...)` and `DBOSClient.list_workflow_steps(...)`.
  - Late read after `DBOS.destroy(...); DBOS(config).launch()` when the case
    models relaunch.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
|---|---:|---|---|---|---|
| case-001 | 3540 | portable-workflow-raises-structured-error | no dependency fault | one portable workflow raises `PortableWorkflowError(message, name, code, data)` from a step | name/message/code/data parity across status, handle, client, and step rows |
| case-002 | 3541 | portable-error-after-nested-cause | no dependency fault; Python chained cause is diagnostic | outer portable error includes modeled cause marker in `data` and is raised `from ValueError(inner_marker)` | data-carried cause marker survives; `__cause__` loss is diagnostic unless contract-anchored |
| case-003 | 3542 | relaunch-before-structured-error-read | DBOS app relaunch after terminal error before public reads | one portable workflow fails, DBOS is destroyed/relaunched, then all retrieval paths read the terminal error | durable structured metadata is not rewritten or flattened on late/relaunched reads |

#### Replay Commands

- Single case:
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-004-portable-structured-error-metadata --case case-001`
- Full matrix:
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-004-portable-structured-error-metadata --all-cases --sequential`

#### Required Artifacts

Each case must write seed, derived workflow ID, serializer mode, workflow
serialization type, modeled error payload, raw durable `workflow_status.error`
row plus serialization name, failed `operation_outputs.error` rows, public
status signatures, handle/client exception signatures, relaunch timestamps when
applicable, product commit, redacted DB URLs, and any diagnostic
`__cause__`/`__context__` observations.

#### Invariants

- Must hold: every public retrieval path exposes the modeled portable error
  `name`, `message` marker, `code`, and `data` object.
- Must hold: the raw durable error row has portable JSON serialization when the
  modeled case uses portable error semantics.
- Must hold: failed step rows preserve the same structured metadata or an
  explicitly modeled wrapper whose child/data contains the same metadata.
- Must hold: case-003 relaunch reads do not rewrite, flatten, or replace the
  terminal structured error.
- Must hold: client and runtime retrieval paths agree on the structured fields,
  not merely on exception type.
- Must never happen: the workload passes because any exception was raised, by
  checking only the message marker, or by ignoring a lost `code`/`data` field.
- Diagnostic only unless anchored in public contract: Python `__cause__` and
  `__context__` object identity or chain shape. The product oracle is the
  modeled cause marker carried in portable `data`.

#### Producer Gate Notes

- Surface gate: passed. Target code exposes `PortableWorkflowError`,
  `JsonWorkflowErrorData`, `exception_to_workflow_error_data()`, and
  `deserialize_exception()`; product tests assert portable error name/message
  but not full structured metadata parity.
- Fault-model/originality gate: passed. This adds a new oracle over structured
  metadata and relaunch reads, not a rerun of the native portable-config
  masking finding or a seed sweep.
- Oracle gate: ready. A concrete implementation bug such as dropping `code`,
  dropping `data`, returning a raw JSON string through `safe_deserialize`, or
  rewriting terminal error rows after relaunch would fail named invariants.
- Feasibility gate: ready. The existing serialization workload already creates
  isolated Postgres DBOS apps, records status/handle/client/step signatures, and
  supports relaunch-before-read style cases.

#### Expected Signatures

- Success: all retrieval paths return the same modeled structured metadata, no
  serializer infrastructure noise replaces the app error, and relaunch does not
  alter the terminal error.
- Finding: any path loses `code` or `data`, reports only a generic
  `PortableWorkflowError`, returns raw serialized JSON instead of structured
  error metadata, reports conflicting metadata across client/runtime paths, or
  rewrites the durable terminal error after relaunch.
- Setup block: the workload cannot configure portable workflow/error
  serialization, cannot read durable rows safely, cannot relaunch DBOS without
  registry or database collision, or dependencies/Postgres are unavailable.
- Low signal: the case only repeats rung-003 config masking, checks only
  marker text, uses a non-portable app error without structured data, or never
  observes durable status and step rows.

#### Stale Conditions

Mark stale if DBOS changes `PortableWorkflowError`, portable JSON error data,
`exception_to_workflow_error_data()`, workflow status/step error serialization,
client deserialization behavior, or public error metadata contract.

### Loop-1 Added Rung: rung-005-retry-class-stored-error-result-liveness

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-005-retry-class-stored-error-result-liveness
frontier: serialization-error-fidelity
status: queued
order: 5
level: liveness
workload_file: .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py
seeds: [3550, 3551, 3552]
updated_at: 2026-06-24
producer_evidence:
  - PR #704
  - target/dbos/_sys_db.py
  - target/tests/test_failures.py
  - target/tests/test_client.py
  - .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-data-and-fixtures/overview.md
```

#### Source Contract

- Frontier ID: `serialization-error-fidelity`.
- Rung ID: `rung-005-retry-class-stored-error-result-liveness`.
- Protected product promise: terminal failed workflows whose stored error is a
  retry-class DBAPI/connection exception are observable through public result
  retrieval paths without poisoning DBOS retry polling.
- Replay command: `python .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-005-retry-class-stored-error-result-liveness --case <case-id>`.
- Seed policy: exact seeds `3550`, `3551`, and `3552`; every run must persist
  seed, generated workflow IDs, SQL fault timing, retrieval path list, timeout
  bound, and derived DB/error marker.
- Invariant oracle: an independent result-retrieval model records each
  workflow's terminal status, expected stored DB error marker, and bounded
  retrieval outcome; every runtime/client/relaunch/parent-child path must match
  that model without hanging, rewriting durable state, or reporting unrelated
  infrastructure timeout noise.

#### Product Promise

When a workflow fails with an application-visible database/connection exception
that DBOS infrastructure would normally classify as retryable, durable workflow
result retrieval must observe the terminal failed workflow and promptly raise
the stored application error. Runtime handles, retrieved handles, client
handles, parent-child `get_result`, and post-relaunch reads must agree on the
same terminal error instead of retrying or polling forever.

#### Why This Is New

Existing serialization rungs check whether durable error content survives
serializer modes, retry/recovery, client/status paths, and portable structured
metadata. They do not assert bounded result-retrieval liveness when the stored
exception type itself is a retry-class DBAPI/connection error. PR `#704`
identified that `check_workflow_result` being wrapped by `db_retry()` could
treat the child's stored error as an infrastructure failure and repeatedly retry
the read instead of returning the terminal error.

The current target contains the PR `#704` fix and a narrow product test for one
direct/retrieved handle. This workload adds cross-path parity and relaunch
coverage without rediscovering the existing portable JSON masking finding.

#### Workload Shape

- Type: API/client liveness and error-fidelity workload.
- Build profile: `default`; final classification requires Postgres because the
  modeled fault uses SQLAlchemy/DBAPI connection invalidation.
- Expected workload file: reuse
  `.workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py`
  because setup, durable error inspection, handle/client retrieval, and
  relaunch evidence are already part of this frontier's harness.
- Fault model: a workflow step uses a dedicated SQLAlchemy engine against the
  DBOS system database, sets a short idle-in-transaction timeout or equivalent
  deterministic connection-invalidating condition, then performs a query after
  the timeout so the stored workflow error is a `DBAPIError`/retry-class
  connection exception.
- Observation model: every public result retrieval runs under a bounded thread
  or async timeout and records whether it raised the modeled DB exception,
  returned unexpectedly, raised infrastructure retry noise, or hung.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 3550 | direct-and-retrieved-handle-dbapi-error | one failed workflow stores a connection-invalidated `DBAPIError`; call original handle, retrieve handle, and status/list paths | all result reads raise modeled DB error promptly; status/list show terminal `ERROR` with matching marker |
| case-002 | 3551 | parent-child-get-result-dbapi-error | parent starts child that stores retry-class DB error and awaits/catches child `get_result` inside the parent workflow | parent terminal state and child terminal state agree with the model; child error retrieval does not poison parent or hang |
| case-003 | 3552 | relaunch-and-client-dbapi-error-read | fail workflow, destroy/relaunch DBOS, then read via runtime and `DBOSClient` handles/status | late and client retrieval paths raise the same stored error promptly and do not rewrite durable status |

#### Replay Commands

- Single case:
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-005-retry-class-stored-error-result-liveness --case case-001`
- Full matrix:
  - `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-005-retry-class-stored-error-result-liveness --all-cases --sequential`

#### Required Artifacts

Each case must write seed, derived workflow IDs, SQLAlchemy fault setup,
exception class/module/message snippets, raw durable `workflow_status.error`
row plus serialization name, status/list observations, runtime handle outcome,
retrieved handle outcome, client handle outcome when used, parent-child status
and step observations when used, relaunch timestamps when used, timeout bounds,
product commit, and redacted database URLs.

#### Invariants

- Must hold: every modeled result-retrieval path completes within the bounded
  timeout and raises the stored terminal DBAPI/connection error or an explicitly
  modeled wrapper preserving its marker.
- Must hold: `DBOS.get_workflow_status` and `DBOS.list_workflows` show terminal
  `ERROR` for the failed workflow before and after result retrieval attempts.
- Must hold: runtime handles, retrieved handles, async/client handles where
  used, and post-relaunch reads agree on the modeled error marker and do not
  rewrite the durable row.
- Must hold: in the parent-child case, the child error does not leave the
  parent handle indefinitely pending and does not create duplicate child
  execution effects while waiting on `get_result`.
- Must never happen: the workload passes by seeing any exception, by using an
  infrastructure timeout as the product outcome, or by relying on the narrow
  product regression test alone.

#### Producer Gate Notes

- Surface gate: passed. PR `#704`, `target/dbos/_sys_db.py`, and
  `target/tests/test_failures.py` show a real result-retrieval liveness hazard
  around stored retry-class DB errors.
- Fault-model/originality gate: passed. This is not another serializer marker
  check; it adds bounded liveness and cross-path parity for terminal error
  retrieval.
- Oracle gate: ready. Reintroducing `db_retry()` around stored-error
  deserialization, losing the error marker on client retrieval, or hanging a
  parent workflow on child `get_result` would fail named invariants.
- Feasibility gate: ready. The existing serialization workload already owns
  isolated Postgres setup and retrieval-path artifact collection; the narrow
  product test demonstrates the fault can be created without product edits.

#### Expected Signatures

- Success: every path raises the modeled stored DB error inside the bound,
  status remains terminal `ERROR`, and parent/client/relaunch observations
  agree.
- Finding: any result path hangs, reports only a polling/infrastructure timeout,
  retries forever, rewrites the durable error, makes parent and child terminal
  states disagree, or exposes conflicting error markers across runtime and
  client paths.
- Setup block: Postgres cannot be configured to create a deterministic
  connection-invalidated DBAPI error under the workload environment.
- Low signal: the case only reruns `test_get_result_no_hang...`, checks that a
  thread ended without verifying the error marker, or omits durable status
  evidence.

#### Stale Conditions

Mark stale if DBOS changes result retrieval retry policy, `db_retry`
classification, workflow handle polling semantics, SQLAlchemy/DBAPI error
serialization, or the public contract for terminal workflow errors.

### Rung: rung-000-serializer-smoke

Evidence source: `evidence-key:frontiers/serialization-error-fidelity/rungs/rung-000-serializer-smoke.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-serializer-smoke
frontier: serialization-error-fidelity
status: ready
order: 0
level: baseline
workload_file: .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py
seeds:
  - 3500
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 000 Serializer Smoke

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072712859935000Z.prompt.md`.
- Frontier ID: `serialization-error-fidelity`.
- Rung ID: `rung-000-serializer-smoke`.
- Protected product promise: preserve the concrete `serialization-error-fidelity` promise from `frontier.md` and `strategy/candidates/serialization-error-fidelity.md`.
- Replay command: `python .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-000-serializer-smoke --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: the durable error seen through status and handle retrieval preserves the original application exception class/message, not serializer infrastructure noise.

##### Goal

- Build and run: prove serializer config variants and durable error retrieval.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `serialization-error-fidelity` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: product-native setup proof.
- Entry points: serializer config, default/pickle/portable modes, failed step output/error rows, workflow status errors, handle retrieval, retry/recovery, and env overrides.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | setup | serializer modes and durable error retrieval run | run one failing workflow under baseline config | status and handle expose modeled error | serializer smoke oracle |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3500 | run-one-failing-workflow-under-baseline-config | none unless case says setup block | serializer modes and durable error retrieval run | serializer smoke oracle |


##### Invariants

- Must hold: each failing workflow records the modeled application exception before serializer behavior is inspected.
- Must hold: durable status error and handle retrieval expose the original failing-step class/message or an explicitly modeled wrapper preserving it.
- Must hold: retry/recovery cannot replace the application error with a serializer/configuration error.
- Must hold: serializer env/config variants are recorded per case for replay.
- Must never happen: the workload passes only because it checks that some exception occurred.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/serialization-error-fidelity.md`
  - `evidence-key:frontiers/serialization-error-fidelity/frontier.md`
- Suggested command family:
  - `python .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-000-serializer-smoke --case case-001`
  - `python .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-000-serializer-smoke --all-cases --sequential`
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

### Rung: rung-001-default-serializer-error

Evidence source: `evidence-key:frontiers/serialization-error-fidelity/rungs/rung-001-default-serializer-error.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-default-serializer-error
frontier: serialization-error-fidelity
status: selected
order: 1
level: regression
workload_file: .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py
seeds:
  - 3510
  - 3511
  - 3512
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 001 Default Serializer Error

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072712859935000Z.prompt.md`.
- Frontier ID: `serialization-error-fidelity`.
- Rung ID: `rung-001-default-serializer-error`.
- Protected product promise: preserve the concrete `serialization-error-fidelity` promise from `frontier.md` and `strategy/candidates/serialization-error-fidelity.md`.
- Replay command: `python .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-001-default-serializer-error --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: the durable error seen through status and handle retrieval preserves the original application exception class/message, not serializer infrastructure noise.

##### Goal

- Build and run: default portable serializer preserves original failing-step error.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `serialization-error-fidelity` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: serializer config, default/pickle/portable modes, failed step output/error rows, workflow status errors, handle retrieval, retry/recovery, and env overrides.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | error fidelity | default serializer preserves application ValueError | failing step raises ValueError with seed marker | status and handle include marker | original error oracle |
| case-002 | boundary data | non-json-ish payload does not mask application failure | workflow carries nested payload then raises app error | serializer succeeds or preserves app error | no serializer-noise masking |
| case-003 | retrieval path | client/status/handle retrieval agree on same durable error | read error through all public paths | class/message normalized equal | multi-path error oracle |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3510 | failing-step-raises-valueerror-with-seed-marker | none unless case says setup block | default serializer preserves application ValueError | original error oracle |
| case-002 | 3511 | workflow-carries-nested-payload-then-raises-app- | none unless case says setup block | non-json-ish payload does not mask application failure | no serializer-noise masking |
| case-003 | 3512 | read-error-through-all-public-paths | none unless case says setup block | client/status/handle retrieval agree on same durable error | multi-path error oracle |


##### Invariants

- Must hold: each failing workflow records the modeled application exception before serializer behavior is inspected.
- Must hold: durable status error and handle retrieval expose the original failing-step class/message or an explicitly modeled wrapper preserving it.
- Must hold: retry/recovery cannot replace the application error with a serializer/configuration error.
- Must hold: serializer env/config variants are recorded per case for replay.
- Must never happen: the workload passes only because it checks that some exception occurred.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/serialization-error-fidelity.md`
  - `evidence-key:frontiers/serialization-error-fidelity/frontier.md`
- Suggested command family:
  - `python .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-001-default-serializer-error --case case-001`
  - `python .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-001-default-serializer-error --all-cases --sequential`
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

### Rung: rung-002-retry-recovery-error-records

Evidence source: `evidence-key:frontiers/serialization-error-fidelity/rungs/rung-002-retry-recovery-error-records.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-002-retry-recovery-error-records
frontier: serialization-error-fidelity
status: ready
order: 2
level: adversarial
workload_file: .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py
seeds:
  - 3520
  - 3521
  - 3522
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 002 Retry Recovery Error Records

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072712859935000Z.prompt.md`.
- Frontier ID: `serialization-error-fidelity`.
- Rung ID: `rung-002-retry-recovery-error-records`.
- Protected product promise: preserve the concrete `serialization-error-fidelity` promise from `frontier.md` and `strategy/candidates/serialization-error-fidelity.md`.
- Replay command: `python .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-002-retry-recovery-error-records --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: the durable error seen through status and handle retrieval preserves the original application exception class/message, not serializer infrastructure noise.

##### Goal

- Build and run: retry/recovery preserves original error across durable status and handle retrieval.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `serialization-error-fidelity` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: serializer config, default/pickle/portable modes, failed step output/error rows, workflow status errors, handle retrieval, retry/recovery, and env overrides.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | retry | retries preserve final application error | fail after retryable step attempts | final durable error is modeled app error | retry count and error agree |
| case-002 | recovery | recovery of failed workflow does not rewrite error | recover pending/failed row then read status | same original error remains | error row stable |
| case-003 | late read | late handle read after cleanup does not lose error detail | read after delay/cleanup window | error class/message still actionable | durable retrieval oracle |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3520 | fail-after-retryable-step-attempts | none unless case says setup block | retries preserve final application error | retry count and error agree |
| case-002 | 3521 | recover-pending-failed-row-then-read-status | modeled dependency/process fault | recovery of failed workflow does not rewrite error | error row stable |
| case-003 | 3522 | read-after-delay-cleanup-window | none unless case says setup block | late handle read after cleanup does not lose error detail | durable retrieval oracle |


##### Invariants

- Must hold: each failing workflow records the modeled application exception before serializer behavior is inspected.
- Must hold: durable status error and handle retrieval expose the original failing-step class/message or an explicitly modeled wrapper preserving it.
- Must hold: retry/recovery cannot replace the application error with a serializer/configuration error.
- Must hold: serializer env/config variants are recorded per case for replay.
- Must never happen: the workload passes only because it checks that some exception occurred.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/serialization-error-fidelity.md`
  - `evidence-key:frontiers/serialization-error-fidelity/frontier.md`
- Suggested command family:
  - `python .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-002-retry-recovery-error-records --case case-001`
  - `python .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-002-retry-recovery-error-records --all-cases --sequential`
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

### Rung: rung-003-config-matrix

Evidence source: `evidence-key:frontiers/serialization-error-fidelity/rungs/rung-003-config-matrix.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-003-config-matrix
frontier: serialization-error-fidelity
status: finding_minimized_cloud
order: 3
level: contract
workload_file: .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py
seeds:
  - 3530
  - 3531
  - 3532
updated_at: 2026-06-20T21:42:34Z
```

#### Rung 003 Config Matrix

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072712859935000Z.prompt.md`.
- Frontier ID: `serialization-error-fidelity`.
- Rung ID: `rung-003-config-matrix`.
- Protected product promise: preserve the concrete `serialization-error-fidelity` promise from `frontier.md` and `strategy/candidates/serialization-error-fidelity.md`.
- Replay command: `python .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-003-config-matrix --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: the durable error seen through status and handle retrieval preserves the original application exception class/message, not serializer infrastructure noise.

##### Goal

- Build and run: serializer config/env matrix without masking application exceptions.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `serialization-error-fidelity` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: serializer config, default/pickle/portable modes, failed step output/error rows, workflow status errors, handle retrieval, retry/recovery, and env overrides.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | config | pickle mode preserves app error | run failing workflow under pickle serializer | durable error equals model | pickle config oracle |
| case-002 | config | portable mode preserves app error | run failing workflow under portable serializer | durable error equals model | portable config oracle |
| case-003 | env override | env serializer override is explicit and replayable | set serializer via env/config conflict | selected mode recorded and app error preserved | config precedence plus error oracle |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3530 | run-failing-workflow-under-pickle-serializer | none unless case says setup block | pickle mode preserves app error | pickle config oracle |
| case-002 | 3531 | run-failing-workflow-under-portable-serializer | none unless case says setup block | portable mode preserves app error | portable config oracle |
| case-003 | 3532 | set-serializer-via-env-config-conflict | none unless case says setup block | env serializer override is explicit and replayable | config precedence plus error oracle |


##### Invariants

- Must hold: each failing workflow records the modeled application exception before serializer behavior is inspected.
- Must hold: durable status error and handle retrieval expose the original failing-step class/message or an explicitly modeled wrapper preserving it.
- Must hold: retry/recovery cannot replace the application error with a serializer/configuration error.
- Must hold: serializer env/config variants are recorded per case for replay.
- Must never happen: the workload passes only because it checks that some exception occurred.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/serialization-error-fidelity.md`
  - `evidence-key:frontiers/serialization-error-fidelity/frontier.md`
- Suggested command family:
  - `python .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-003-config-matrix --case case-001`
  - `python .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-003-config-matrix --all-cases --sequential`
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

##### Execution Result

Rung 003 produced a cloud-minimized finding in `case-003`.

- Latest minimized run:
  `evidence-key:runs/run-20260620T214234Z-serialization-error-fidelity-rung-003-case-003-cloud-minimized/summary.md`
- Finding record:
  `evidence-key:findings/serialization-error-fidelity-native-workflow-portable-config-masks-error.md`
- Workload: `01KVKFMZAX777E7X8N1DDY9N93`
- Exploration: `nd7ane6v906fjanhmhv6q2j8cn891ykb`

The reproducer is one native workflow under portable JSON config with one
failing modeled application step. The expected
`WIO_SERIALIZATION_APP_ERROR ... seed=3532` marker is replaced by
`TypeError('Object of type TypeError is not portable JSON serializable')`.
