# E-031 — stream write duplicated from a step — FILED upstream #770

- **Upstream issue:** https://github.com/dbos-inc/dbos-transact-py/issues/770
- **Filed:** 2026-07-09 via `gh` as `viswa-abe` (Viswa-approved).
- **Title:** DBOS.write_stream from inside a step is not exactly-once — it
  duplicates the value on every step retry.
- **Status:** open (filed).

## What was filed

Ordinary-user report: `DBOS.write_stream` from a workflow context is exactly-once
(`write_stream_from_workflow` records an operation output + guards re-execution),
but from a step context (`write_stream_from_step`, `dbos/_sys_db.py:4229`) it has
no recorded operation and no guard. The `streams` PK excludes `function_id`, and a
`@DBOS.step(max_attempts>1)` re-runs its body under the same `function_id`, so a
step that writes then fails re-inserts the value at a new offset on every attempt.
A `DBOS.read_stream` consumer sees one logical write delivered K times; the
workflow still completes SUCCESS (silent). Async path duplicates identically.
Verified on released `dbos==2.26.0`.

## Repro

Standalone script (plain `pip install dbos sqlalchemy "psycopg[binary]"` + local
Postgres, no fork checkout): `.workers/dossiers/e-031-repro.py`. Issue body:
`.workers/dossiers/e-031-issue-body.md`. Both variants + workflow-context control.

## Notes

- Suggested fix in the issue: record + guard the step-path write via
  `_check_operation_execution_txn` on the step's `function_id`, mirroring the
  workflow path.
- Local evidence: `runs/E-031.md`; workload
  `workloads/stream-step-oaoo/stream_step_oaoo_workload.py`; cloud runs published
  (control + step-retry-sync + step-retry-async explorations).
- Issue number lives in file contents only — never in a commit message (DEC-009:
  a `#NNN`/`owner/repo#NNN` in a commit on the public fork cross-references onto
  the upstream timeline).
