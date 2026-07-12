---
key: management-illegal-transitions
rung: L0
cast: {ops-operator: 1}
flows: [management]
depth: 10
status: ready
result: null
replay: null
redproof: null
invariants: [illegal-transition-errors-documented]
story: >-
  An operator clicks the wrong button on a dashboard — resumes a job that already
  finished, cancels one that's already cancelled, forks a job id that doesn't
  exist. Each should come back with a clear, typed "no such workflow" error, not
  a raw internal crash.
---
L0 solo probe of the `management` flow's error contract. One ops-operator runs
three wrong-state management verbs and the error-contract oracle checks each
fails the *documented* way — a typed error, never a raw internal `Exception`,
never a silent success.

## The verbs and the contract

- **resume-terminal** — resume a workflow that already reached SUCCESS. Guarded
  no-op (`_sys_db.py:963-970` gates the UPDATE with `status notin_[SUCCESS,ERROR]`);
  the handle then yields the recorded result. GREEN control.
- **cancel-cancelled** — cancel a workflow, then cancel it again. Idempotent
  re-write of CANCELLED (`_sys_db.py:917-936`). GREEN control.
- **fork-missing** — `DBOS.fork_workflow(<never-created id>, 1)`. The sibling read
  verbs raise the typed `DBOSNonExistentWorkflowError` for an unknown id
  (`_client.py:520-523`), but `fork_workflow` has no pre-existence check and the
  sys_db layer raises a **raw `Exception("Workflow ... not found")`**
  (`_sys_db.py:1180`). EXPECTED RED — an undocumented-error contract breach.

## Oracle

The universal **error-contract** plane. The server (which holds DBOS's exception
classes) classifies each verb's outcome (`documented` = `isinstance(err,
DBOSNonExistentWorkflowError)`, `silent` = returned without raising); the flow
records each op via `ctx.errors.expect(label)` and raises an undocumented marker
only on a breach, so `fork-missing` yields `undocumented_error` (RED) while the
two controls stay green — the oracle discriminates. The `--redproof` run plants
an undocumented outcome into a green control op (must PASS).

## Expected outcome

RED on `fork-missing` (weight 1, wrong-error). Crystallizes as an error-contract
finding: `fork_workflow` on an unknown id leaks a raw `Exception` instead of the
typed `DBOSNonExistentWorkflowError` its sibling verbs raise — a caller cannot
`except DBOSNonExistentWorkflowError` around it. Single-process; no event; no
second executor.
