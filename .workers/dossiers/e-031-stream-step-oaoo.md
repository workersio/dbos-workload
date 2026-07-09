# Dossier — e-031: write_stream from a step is not exactly-once

**Status: FILED upstream #770** (2026-07-09, `viswa-abe`, Viswa-approved).
https://github.com/dbos-inc/dbos-transact-py/issues/770 — record:
`.workers/issues/E-031-stream-step-oaoo-filed-770.md`.

## Proposed upstream issue

- **Repo:** `dbos-inc/dbos-transact-py`
- **Auth:** `viswa-abe` (correct account for dbos-inc / workersio).
- **Title (proposed):** `DBOS.write_stream from inside a step is not exactly-once — it duplicates the value on every step retry`
- **Body:** `.workers/dossiers/e-031-issue-body.md` (ordinary-user framing, ZERO
  product vocabulary, standalone repro in a collapsed `<details>` — verified clean).
- **Repro:** `.workers/dossiers/e-031-repro.py` — plain `pip install dbos
  sqlalchemy "psycopg[binary]"` + local Postgres, no fork checkout needed.

## Verification (done before drafting)

Reproduced on **released `dbos==2.26.0`** (latest PyPI), Postgres 16, Python 3.12:

```
single write from a retrying step  -> stream = ['event-1', 'event-1', 'event-1']  (3 values)
single write from a workflow        -> stream = ['event-1']  (1 values)
BUG REPRODUCED
```

The workflow-context write appears exactly once; the identical write from a
retrying step appears once per attempt. Async path (`write_stream_async` from an
async step) duplicates identically (same core via `asyncio.to_thread`).

## Root cause (for our records)

`write_stream` (`dbos/_core.py`) dispatches on caller context:
- workflow → `write_stream_from_workflow`: records an operation output + guards
  re-execution with `_check_operation_execution_txn("DBOS.writeStream")`.
  Exactly-once.
- step → `write_stream_from_step` (`dbos/_sys_db.py:4229`): inserts at
  `max(offset)+1`, NO recorded operation, NO guard.

`streams` PK is `(workflow_uuid, key, offset)` (excludes `function_id`); a
`@DBOS.step(max_attempts>1)` re-runs its body under the same `function_id`, so a
step that writes then fails re-inserts on every attempt.

Suggested fix (in the issue): record + guard the step-path write via
`_check_operation_execution_txn` on the step's `function_id`, mirroring the
workflow path.

## Local evidence

- Workload + oracle plane: `.workers/workloads/stream-step-oaoo/stream_step_oaoo_workload.py`
- Run record: `.workers/runs/E-031.md`
- Promise: `.workers/promises/streams-record-each-write-once.md`

## Filing checklist (when Viswa says go)

1. Re-confirm the repro on released dbos (already done; re-run if released version bumped).
2. `gh issue create --repo dbos-inc/dbos-transact-py --title "<title>" --body-file .workers/dossiers/e-031-issue-body.md` as `viswa-abe`.
3. Record the returned issue number in `.workers/issues/` + `loop-state.md` +
   the promise `reported:` fields (FILE CONTENTS ONLY — never in a commit message,
   which would cross-reference onto the upstream timeline; ledger DEC-009).
