# E-028 — GC orphan OAOO — FILED upstream #769

- **Upstream issue:** https://github.com/dbos-inc/dbos-transact-py/issues/769
- **Filed:** 2026-07-09 via `gh` as `viswa-abe` (Viswa-approved).
- **Title:** Interrupted garbage_collect leaves orphaned transaction_outputs, so a
  reused workflow ID replays a prior workflow's step result.
- **Status:** open (filed).

## What was filed

Ordinary-user report: `garbage_collect` deletes `workflow_status` (system DB) then
`transaction_outputs` (application DB) in two steps with no cross-database
transaction. An interruption between them orphans `transaction_outputs`; a reused
workflow ID whose first `@DBOS.transaction` lands on the same `function_id` gets
the orphan replayed by `check_transaction_execution` and skips its own body =
silent stale result. Both variants covered (crash between phases; app-DB delete
fails). Verified on released `dbos==2.26.0`.

## Repro

Standalone script verified on `dbos==2.26.0` (no main install needed):
`scratchpad/e028_repro.py` (session scratch). Both variants + control confirmed.
Issue body: `scratchpad/e028_issue.md`.

## Notes

- Released `garbage_collect` has NO `batch_size` param (single non-batched app-DB
  delete) — the issue does not reference batching (that's main-only, #751).
- Local evidence: `runs/E-028.md`; cloud runs `01KX460BYM2JHVTJKT2XBQE4WN` (sys-db
  side) + `01KX4BZJHVB2V4MKPA9FDY08JE` (app-DB-batch, main/fork).
- Watch for maintainer response; the dossier draft is `dossiers/e-028-gc-orphan-oaoo.md`.
