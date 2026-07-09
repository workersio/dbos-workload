---
key: garbage-collection-durability
title: Garbage collection durability
description: "Incremental garbage collection deletes a workflow's system-DB status and its application-DB transaction outputs as one logical unit — it must never orphan transaction outputs so that a reused workflow id replays a dead workflow's recorded step result instead of executing fresh."
order: 100
---

# Area: garbage-collection-durability

## Current State

New area from the diff-directed scan of PR `#751` "Add incremental garbage
collection" (commit `533dc0d`, in-fork). No prior harvest. GC now defaults to
batched deletion (`DEFAULT_GC_BATCH_SIZE = 10_000`), so the batched cross-database
path is the DEFAULT behavior, not an opt-in.

Evidence:
- `dbos/_sys_db.py` `SystemDatabase.garbage_collect(cutoff, rows_threshold, batch_size)`
  — deletes `workflow_status` (excluding PENDING/ENQUEUED/DELAYED) via a
  created_at watermark loop, one committed txn per batch, then snapshots the
  surviving old ids and returns `(cutoff, pending_ids)`.
- `dbos/_app_db.py` `ApplicationDatabase.garbage_collect(cutoff, pending_ids, batch_size)`
  — deletes `transaction_outputs` older than cutoff, excluding `pending_ids`,
  batched.
- `dbos/_workflow_commands.py` `garbage_collect(dbos, ...)` — runs sys-db phase
  fully, THEN app-db phase. No shared transaction; two databases.
- `dbos/_app_db.py` `check_transaction_execution(session, wf_id, function_id, name)`
  — replays any `transaction_outputs` row matching `(workflow_uuid, function_id)`,
  skipping the step body (raises `DBOSUnexpectedStepError` only on name mismatch).

## Product Promise

GC deletes a workflow's system-db status and app-db transaction outputs as one
logical unit. It must never leave transaction outputs behind after the status
is gone, since a later workflow reusing the collected id would replay the dead
workflow's recorded step output instead of executing fresh.

## What Not To Repeat

- Do not re-derive the PR's own covered cases: happy-path batching, mid-**sys_db**-batch
  resumable failure, `rows_threshold`, `batch_size` validation
  (`tests/test_workflow_management.py`).
- The DELAYED-asymmetry hypothesis was falsified by source read: the returned
  `pending_ids` select is status-agnostic (`created_at < cutoff` over survivors),
  so it includes DELAYED — sys-db preserved set matches the app-db exclusion set.
  Do not file that.
- Orphan-as-storage-leak alone is self-healing (next GC cycle reaps it) —
  cosmetic. The finding is the OAOO stale-replay on id reuse.

## Deeper / Broader Search

| Direction | Why |
|---|---|
| app-db batched-loop partial failure (path b) | The PR's resumable test only injected failure on the sys-db side; app-db's own batch loop failing partway orphans transaction outputs independently of the crash-between-phases path. |
| GC vs concurrent recovery | A workflow transitioning PENDING↔terminal during the long batched delete; confirm the survivor-snapshot design holds under real concurrency, not just single-threaded. |
| notifications / operation_outputs cascade | GC deletes `workflow_status`; confirm dependent system-db rows (operation_outputs, notifications) are consistently cascaded or preserved. |

## Rung Ladder (see work-item e-028)

- rung-001-gc-orphan-oaoo: baseline full-GC-then-reuse (green control) +
  partial-GC-then-reuse (adversarial; the OAOO oracle).

## Stale Conditions

Mark stale if DBOS makes GC's two phases atomic (shared transaction /
distributed commit), changes `transaction_outputs` keying, changes
`check_transaction_execution` replay semantics, or target ref advances past
`#751` with a GC rewrite.
