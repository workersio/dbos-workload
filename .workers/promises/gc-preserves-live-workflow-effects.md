---
key: gc-preserves-live-workflow-effects
area: garbage-collection-durability
title: Garbage collection never resurrects dead step results
claim: >-
  Incremental garbage collection deletes a workflow's system-database status
  and its application-database transaction outputs as one logical unit. It must
  never leave a workflow's transaction outputs behind after its status is gone,
  because a later workflow that reuses the collected id would replay the dead
  workflow's recorded step output instead of executing fresh — an
  exactly-once/durability violation.
status: active
provenance: https://docs.dbos.dev/python/reference/methods#garbage_collect (incremental GC, #751; transaction outputs keyed by (workflow_uuid, function_id) and replayed by check_transaction_execution)
explorations:
  - key: partial-gc-orphan-reuse
    title: Reused id after partial GC executes fresh, never replays
    description: >-
      A completed workflow is partially collected — the system-db status-delete
      phase commits, the app-db transaction_outputs phase does not run (the
      cross-database crash window the PR designs for: "progress is preserved if
      batch fails"). A new workflow reusing that workflow id must execute its
      transaction fresh (new side effect, fresh output), not replay the
      orphaned dead output. Baseline control: after a FULL GC the reused id
      also executes fresh.
    status: ready
    result: null
    reason: null
    workload: workloads/gc-orphan-oaoo/gc_orphan_oaoo_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/gc-orphan-oaoo/gc_orphan_oaoo_workload.py --rung rung-001-gc-orphan-oaoo --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: null
---

# Garbage collection never resurrects dead step results

Evidence lineage: `areas/garbage-collection-durability.md`, work item `e-028`.
New corridor from the diff-directed scan of #751 (Add incremental garbage
collection). `transaction_outputs` is keyed by `(workflow_uuid, function_id)`
and `check_transaction_execution` (`dbos/_app_db.py`) replays any matching row
while skipping the step body; GC's two delete phases span two databases with no
shared transaction, so a partial GC can orphan transaction outputs whose status
row is already gone. Reusing that id (a normal idempotency-key pattern) then
replays the dead workflow's output. The baseline (full GC) proves the reuse
mechanics; the partial-GC case is the adversarial rung.
