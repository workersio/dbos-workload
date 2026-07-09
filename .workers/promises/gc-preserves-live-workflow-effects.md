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
    status: done
    result: red
    reason: >-
      Confirmed on cloud (run 01KX460BYM2JHVTJKT2XBQE4WN, commit fe68c86):
      after a partial GC (sys-db workflow_status deleted, app-db
      transaction_outputs orphaned) a workflow reusing the collected id
      returned the dead workflow's stale output (result-10) and skipped its
      own body (effects_n20=0). Data-correctness / OAOO violation in #751.
      Control case (full GC) executes fresh. Finding candidate — upstream
      filing held for human triage.
    workload: workloads/gc-orphan-oaoo/gc_orphan_oaoo_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/gc-orphan-oaoo/gc_orphan_oaoo_workload.py --rung rung-001-gc-orphan-oaoo --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: "run 01KX460BYM2JHVTJKT2XBQE4WN — case-002 p3 FAIL (outcome=returned:result-10, effects_n20=0); case-001 control green. Evidence: runs/E-028.md"
    freshness: new-current
    reported: 769
    published: pending
  - key: appdb-batch-partial-orphan
    title: Public-API GC with an app-db batch failure still executes fresh on reuse
    description: >-
      Stronger, public-API trigger for the same OAOO orphan: the public
      garbage_collect(batch_size=1) runs the sys-db phase fully, then a single
      transient fault in the app-db batched delete loop (batch 1 commits, batch
      2 fails) orphans the target's transaction outputs. Reusing the id must
      execute fresh; RED if the dead output is replayed. Removes the white-box
      phase-skip objection of partial-gc-orphan-reuse.
    status: done
    result: red
    reason: >-
      Confirmed on cloud (run 01KX4BZJHVB2V4MKPA9FDY08JE, commit f834259):
      public garbage_collect + one transient app-db-loop fault orphaned the
      target (a2: batch 1 committed, fault fired); reuse returned stale
      result-30, body skipped (a3 FAIL, effects_n40=0). The PR's resumable test
      only faults the sys-db side; this is the app-db-loop partial failure it
      never covers. Finding candidate — filing held for human triage.
    workload: workloads/gc-orphan-oaoo/gc_orphan_oaoo_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/gc-orphan-oaoo/gc_orphan_oaoo_workload.py --rung rung-001-gc-orphan-oaoo --case case-003
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: "run 01KX4BZJHVB2V4MKPA9FDY08JE — a3 FAIL (outcome=returned:result-30, effects_n40=0); a2 orphan-via-appdb-loop-failure PASS. Evidence: runs/E-028.md"
    freshness: new-current
    reported: 769
    published: pending
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
