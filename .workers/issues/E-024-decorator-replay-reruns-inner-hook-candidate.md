# Invoking a completed async workflow re-executes its function body (sync does not)

Status: `ready`

Disposition: reproduced locally with a standalone SQLite-backed DBOS script
(no external services). Root cause is an async-only path; a sync control in the
same script does not re-execute. Ready to file upstream.

## Summary

Invoking an already-completed async `@DBOS.workflow` with the same workflow id
returns the durably recorded result but first **re-executes the workflow
function body** — and therefore any ordinary application decorator wrapping it
(logging, metrics, auth, tracing). The equivalent **sync** workflow does not:
it returns the recorded result without re-entering its body. Steps stay
protected in both cases (no duplicate step execution), so the gap is precisely
the workflow-function-level code on the async completed-replay path.

This was first surfaced by the `E-024` workload (async case-001) and has been
reduced to a standalone repro with a sync/async control below.

## Root Cause (target ref `3df88c4`)

- Sync invoke: `dbos/_core.py:661`
  `_get_wf_invoke_func(dbos, status)(functools.partial(func, *args, **kwargs))`.
  `persist` (`dbos/_core.py:532`) checks the completed status **first** and
  returns the recorded result without ever calling the thunk, so the body never
  re-runs.
- Async invoke: `dbos/_core.py:704`
  `Pending(functools.partial(func, *args, **kwargs)).then(_get_wf_invoke_func(...))`.
  In `Pending._wrap` (`dbos/_outcome.py:198`) the body is awaited
  (`value = await func()`) **before** `persist` runs, so the completed-status
  short-circuit happens too late — the body has already executed.

## Environment

- DBOS source: `3df88c4bcc3a2b73d91b765b459f4c7beb6a690c`
- Evidence: `.workers/runs/E-024.md`
- Work item: `.workers/work-items/e-024.md`
- Workload: `.workers/workloads/decorator-composition-fidelity/decorator_composition_fidelity_workload.py`
- Related upstream PR: `dbos-inc/dbos-transact-py#706`, merged

## Reproduction Story

Standalone local repro (no external services; uses a SQLite system database):

```bash
.workers/vendor/dbos-venv/bin/python \
  .workers/issues/repros/e024_decorator_replay_hook.py
```

The script defines an async and a sync `@DBOS.workflow`, each wrapping an
ordinary `functools.wraps` application decorator and an inner `@DBOS.step`. It
invokes each workflow twice under the same `SetWorkflowID`, counting workflow
function-body runs, application-hook runs, and step-body runs.

Observed output (target ref `3df88c4`, DBOS `2.24.0-12-g3df88c4`,
CPython 3.14.6, SQLite system DB):

```text
async: first='direct:step' replay='direct:step'
sync : first='direct:step' replay='direct:step'

                hook runs  body runs  step runs
async workflow          2          2          1
sync workflow           1          1          1

durable result preserved on replay (both)   : True
step bodies stayed protected (both)         : True
async body/hook re-ran on replay            : True
sync  body/hook did NOT re-run (control)    : True
```

Original cloud workload command (matrix evidence):

```bash
.workers/run-with-postgres.sh .workers/python-runtime.sh \
  .workers/workloads/decorator-composition-fidelity/decorator_composition_fidelity_workload.py \
  --rung rung-001-custom-decorator-entrypoint-matrix \
  --case case-001 \
  --seed 7060
```

## Expected Behavior

Completed replay for a terminal successful workflow should return the stored
DBOS result without rerunning application hook wrappers or duplicating their
side effects.

## Actual Behavior

The focused WIO run reported:

- invariant failure:
  `dbos_outer_completed_replay_does_not_rerun_inner_hook`;
- first direct run returned `direct:step`;
- second invocation with the same workflow ID returned the stored result
  `direct:step`;
- hook counts increased from `before=1 after=1` to `before=2 after=2`;
- durable workflow status was already `SUCCESS`;
- durable operation outputs still contained one step row.

## Impact

Applications often use ordinary decorators for logging, metrics, validation,
authorization, tracing, or framework integration. If completed replay reruns
those wrappers, a workflow that otherwise replays from durable DBOS state can
duplicate application-level side effects outside DBOS step/output protection.

## Evidence

- Full matrix run: `01KVYQVJ30HM7KADW4SJQ677JG`
- Focused run: `01KVYR33ZAKK9RA2EXSA6XY9DW`
- Run record: `.workers/runs/E-024.md`
- Work item: `.workers/work-items/e-024.md`

## Controls And Non-Claims

- The duplicate side effect is in the workflow function body / application
  wrapper, not in DBOS durable step output. Step bodies stay protected
  (`step runs == 1`) on replay.
- The sync workflow is a passing control in the same script: its body and hook
  run exactly once across the same double-invocation, so this is an async-path
  asymmetry, not general "replay re-runs everything" behavior.
- The durable workflow result is correct in both cases (replay returns the
  stored `direct:step`, not the mutated input).
- Not yet established for: queue/client entry points, recovery (vs direct
  re-invocation), failure/error replay, or class-method workflows. The claim is
  scoped to direct invocation of a completed async workflow.

## Upstream Duplicate/Fix Check

Checked on 2026-06-25:

- PR `#706` is merged and adds custom-decorator test coverage.
- No existing upstream issue was found for completed replay rerunning an inner
  decorator hook.
