---
key: async-workflows-checkpoint-deterministically
area: workflows
title: Async workflows checkpoint deterministically
claim: >-
  Async workflows preserve deterministic checkpoint positions, workflow
  context, and child ownership when steps are scheduled concurrently,
  recovered on another executor, cancelled mid-flight, or replayed —
  concurrency never scrambles what durability recorded.
status: active
provenance: https://docs.dbos.dev/python/tutorials/workflow-tutorial (deterministic replay from checkpoints; async workflows share the same durability contract)
explorations:
  - key: async-checkpoint-recovery-baseline
    title: Gather recovery and cancel compose cleanly
    description: >-
      Async workflows composing gather, recovery replay, and cancellation
      after an async child starts; checkpoint positions, error
      classification, and child ownership must survive every path.
    status: done
    result: null
    reason: null
    workload: workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py --rung rung-001-async-checkpoint-recovery-cancel-compose --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd7e6cbbxpqhae9w885j34sekh8a6q38
  - key: preemptible-step-cancel-isolation
    title: Preempted steps stay isolated on resume
    description: >-
      Preemptible async steps cancelled and resumed must keep their
      operation outputs isolated, bypass no retries, and clean up tasks
      after terminal state — no cross-contamination between attempts.
    status: done
    result: null
    reason: null
    workload: workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py --rung rung-003-preemptible-step-cancel-resume-isolation --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd710y6x4fkyrshmfvm9xkm4ph8a7ttj
  - key: queued-task-cleanup-after-cancel
    title: Cancelled workflows leave no live tasks
    description: >-
      After a queued async workflow reaches terminal CANCELLED, no
      executor tasks for it may remain alive. RED confirmed at target
      9922c1d (image b80b603): three _execute_workflow_async coroutine
      tasks stay alive (done=false, cancelled=false) after DBOS reports
      "successfully shut down" — invariant
      queued_gc_workflow_tasks_released_after_terminal FAIL. Upstream
      filing pending human triage (workload-model-artifact risk flagged in
      overview).
    status: done
    result: finding
    reason: null
    workload: workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py --rung rung-002-queued-async-task-retention-gc-pressure --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: "run 01KX3XZ2KMD2XAMEKTQ5TRBY6Y case-003 seed 7112 — INVARIANT queued_gc_workflow_tasks_released_after_terminal FAIL; released_snapshot count=3 live _execute_workflow_async tasks"
    freshness: new-current
    reported: null
    published: nd7eqcdrap5h9thyy5wmgtncr98a7sh8
---

# Async workflows checkpoint deterministically

Evidence lineage: `areas/async-checkpoint-determinism.md` rungs 001–003.
Rung-001 green at 0c41e6df (22/22 invariants), rung-003 green at the pinned
target 3df88c4b. Rung-002 (`queued-task-cleanup-after-cancel`) is a **confirmed
RED** at target 9922c1d / image b80b603: three `_execute_workflow_async`
coroutine tasks survive terminal CANCELLED after DBOS reports clean shutdown
(invariant `queued_gc_workflow_tasks_released_after_terminal` FAIL, run
`01KX3XZ2KMD2XAMEKTQ5TRBY6Y` case-003 seed 7112 — see
`runs/queued-task-cleanup-after-cancel.md`). Upstream filing held pending human
triage (workload-model-artifact risk).
