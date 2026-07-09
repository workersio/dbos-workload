# Run evidence — decorator-entrypoint-matrix

Exploration: `decorator-entrypoint-matrix`
Promise: `decorated-functions-replay-durably` (area: workflows)
Rung: `rung-001-custom-decorator-entrypoint-matrix`

## Verdict: FINDING (RED confirmed) — E-024

Invariant `dbos_outer_completed_replay_does_not_rerun_inner_hook` **FAIL**.
Invoking an already-completed async `@DBOS.workflow` that is wrapped
DBOS-outer returns the durably recorded result but **re-executes the inner
application hook first**, duplicating its side effects. The sync control in the
same matrix does not re-execute — the defect is on the async replay path.

## Run

| batch (exploration id) | run id | image | state | invariants |
|---|---|---|---|---|
| nd785764h21smb6smkxwqrw8n98a7xy2 | 01KX3Y9XE23VM5SYAEWD3PB33M | d255d25 | failed (exit 1) | hasInvariantViolation=True; FAIL dbos_outer_completed_replay_does_not_rerun_inner_hook |

Command: `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/decorator-composition-fidelity/decorator_composition_fidelity_workload.py --rung rung-001-custom-decorator-entrypoint-matrix --all-cases --sequential` (depth 1, no faults).

## Interpretation

Real product finding; the async-only re-execution path is a durable-replay
correctness bug. Issue draft `issues/E-024-decorator-replay-reruns-inner-hook-candidate.md`
is marked "ready to file" with a standalone SQLite repro. **Upstream filing is a
human decision** (`reported: null`). Published to the internal status page as a
red.
