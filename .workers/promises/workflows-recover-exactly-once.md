---
key: workflows-recover-exactly-once
area: recovery
title: Workflows recover exactly once
claim: >-
  A workflow interrupted by a database fault or executor crash resumes from
  its last completed step with no duplicate side effects, no stranded
  pending rows, and no second executor re-running work another recoverer
  already owns.
status: active
provenance: https://docs.dbos.dev/python/tutorials/workflow-tutorial (automatic recovery from checkpoints); concurrent-recovery ownership reported as dbos-inc/dbos-transact-py#742, fix PR #744 open
explorations:
  - key: recovery-restart-single-window
    title: Restart windows resume without duplicates
    description: >-
      A database restart injected while a recovered workflow is mid-execute
      — the hardest single interruption window; completed steps must not
      re-execute and terminal state must be correct after recovery.
    status: done
    result: null
    reason: null
    workload: workloads/recovery-db-faults/recovery_db_faults_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py --rung rung-001-recovery-db-restart-single-window --case case-003
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd7f67jfw04dfn3na4nwf82vds8a602j
  - key: concurrent-recovery-ownership
    title: Stale recoverers must not re-run queued work
    description: >-
      Two recoverers racing over the same queued workflow: a stale recovery
      snapshot must not execute the workflow body after another recoverer
      clears the assignment. Reproduces the queue-ownership break reported
      upstream as dbos-inc/dbos-transact-py#742 (fix PR #744 open).
    status: done
    result: null
    reason: null
    workload: workloads/recovery-db-faults/recovery_db_faults_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py --rung rung-006-concurrent-queued-recovery-ownership --case case-001
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: "dbos-inc/dbos-transact-py#742"
    published: nd741avva7jf2yqbhkve6q5ds58a655c
---

# Workflows recover exactly once

Evidence lineage: `areas/recovery-db-faults.md` rungs 000–006. Rungs 001–002
green at b94d7216; rung-006 carries the promoted finding #742 (stale
recovery snapshot executes a queued workflow body after ownership was
cleared), fix PR #744 open as of 2026-06-25. Rung-005 closed as a
workload-model artifact — recover_pending_workflows() is not a barrier.
