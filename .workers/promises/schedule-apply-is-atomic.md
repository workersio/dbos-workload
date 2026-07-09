---
key: schedule-apply-is-atomic
area: scheduling
title: Schedule apply is atomic
claim: >-
  DBOS.apply_schedules() is atomic, idempotent, and live-update safe under
  concurrent callers — re-applying a schedule name leaves exactly one
  durable row, preserves operator state, and restarts the live scheduler
  without duplicate executions.
status: active
provenance: https://docs.dbos.dev/python/reference/schedules (apply semantics); concurrent-apply race fixed upstream by PR #741, not yet in the pinned target
explorations:
  - key: concurrent-apply-live-update
    title: Concurrent apply callers must not conflict
    description: >-
      Eight concurrent apply_schedules() callers on one schedule name must
      all succeed with one final durable row. On the pinned target seven of
      eight raise schedule-name conflicts from a delete-then-create race —
      fixed upstream by PR #741, pending a target bump.
    status: done
    result: null
    reason: null
    workload: workloads/schedule-registry-concurrency/schedule_registry_concurrency_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/schedule-registry-concurrency/schedule_registry_concurrency_workload.py --rung rung-001-concurrent-apply-live-update --case case-001 --seed 7410
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: "dbos-inc/dbos-transact-py#741"
    published: nd7cc8g7ac8jen443c4mmd9kfn8a74fj
---

# Schedule apply is atomic

Evidence lineage: `areas/schedule-registry-concurrency.md` rung 001. The
concurrent-apply delete-then-create race and the stale-thread live-reapply
hazard were both addressed upstream by PR #741; the pinned target 3df88c4b
predates it, so this exploration documents the break until the target bump
turns it green.
