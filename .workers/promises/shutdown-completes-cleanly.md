---
key: shutdown-completes-cleanly
area: recovery
title: Shutdown completes cleanly
claim: >-
  DBOS.destroy() completes within a bounded window even when DBOS adopted
  the application's event loop and timeout waiter tasks are still pending —
  no deadlock, tasks cancelled, and the runtime reusable afterwards.
status: active
provenance: https://docs.dbos.dev/python/reference/lifecycle (destroy semantics; adopted event-loop shutdown must not deadlock)
explorations:
  - key: adopted-loop-destroy-liveness
    title: Destroy is bounded on an adopted loop
    description: >-
      Destroy called with pending timeout tasks on an adopted application
      event loop must finish within bounds, same-loop blocking submission
      must raise instead of deadlocking, and relaunch must succeed.
    status: done
    result: null
    reason: null
    workload: workloads/runtime-shutdown-event-loop-liveness/runtime_shutdown_event_loop_liveness_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/runtime-shutdown-event-loop-liveness/runtime_shutdown_event_loop_liveness_workload.py --rung rung-001-adopted-loop-timeout-destroy-liveness --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd76y9zf4m6pxds4cp35r3enn98a7fzg
---

# Shutdown completes cleanly

Evidence lineage: `areas/runtime-shutdown-event-loop-liveness.md` rung 001,
proven green on the pinned target 3df88c4b: destroy bounds, same-loop
submit guard, and relaunch smoke all passed.
