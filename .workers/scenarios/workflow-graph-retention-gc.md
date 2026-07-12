---
key: workflow-graph-retention-gc
rung: L3
cast: {ops-operator: 1}
flows: [workflow-graph]
depth: 3
status: done
result: finding
replay: {run: nd7ar33zr6vktnp5f262v1bqc98ad1b6, seed: all}
redproof: {run: 01KXB0153P12GBK3A5YXKTB24Z, seed: 1}
invariants: [graph-survives-retention-gc]
story: >-
  An operator runs a routine cleanup job that trims old, finished jobs out of the
  database to keep it small. One of those finished jobs was a sub-task of a bigger
  job that is still running — paused, waiting to be resumed after a restart. The
  cleanup deletes the finished sub-task. When the bigger job resumes it waits
  forever for a sub-task result that no longer exists, and never finishes. The
  same crash-and-resume without the cleanup is the control: it completes fine.
---
L3 probe of `workflow-graph` under a retention garbage-collection world event. One
ops-operator builds a parent→child workflow graph, crashes the parent mid-flight,
and runs a retention `garbage_collect`. The persona-ledger checks that a parent
acked durable still reaches SUCCESS after recovery — a routine gc must not strand
live work.

## The graph and the two arms

- **control** (green) — a parent calls a child workflow (child → SUCCESS), the
  parent is crashed (forced `PENDING`) with the child call already recorded, then
  recovery re-runs the parent. The parent re-awaits the child's status row, finds
  it, and completes → SUCCESS. Proves graph recovery works and the oracle
  discriminates rather than always-reds.
- **strand** (expected RED) — the same, but a retention `garbage_collect`
  (cutoff = now) runs first. gc's guard is a row's OWN status only
  (`_sys_db.py:4415-4425`): the SUCCESS child (aged past cutoff) is deleted; the
  `PENDING` parent is protected. On recovery the parent re-awaits the *deleted*
  child — `check_workflow_result` returns `NoResult` for a not-found row
  (`_sys_db.py:1583/1602`) and `await_workflow_result` loops forever
  (`:1604-1609`); `get_result` on the polling handle never short-circuits on the
  recorded result (`_core.py:169-173`; `record_get_result` has "no corresponding
  check", `_sys_db.py:2519`). The parent is stranded `PENDING` — availability.

## Oracle

The universal **persona-ledger**. The operator is acked that each graph's parent
is durable (`graph-result` → the parent's result string). After crash + (gc) +
recovery + a **bounded** wait, the flow observes each parent's terminal status:
`control` present (SUCCESS) → green; `strand` absent (still `PENDING`) →
`acked_lost` → RED. The bounded wait is essential — an unbounded await would hang
the oracle itself, which is exactly the bug. `--redproof` plants an `acked_lost`
into the green control channel (must PASS).

## Expected outcome

RED on the `strand` arm (weight 2, availability): a retention gc of an aged child
strands its still-`PENDING` parent forever. The `control` arm stays green (same
crash+recovery, no gc, completes). Crystallizes as a durability finding: the
vendor gc test sweeps only independent siblings (`tests/test_workflow_management.py:1017-1076`),
never a parent/child graph, so nothing blesses deleting a referenced child.
Single-process; no second executor.
