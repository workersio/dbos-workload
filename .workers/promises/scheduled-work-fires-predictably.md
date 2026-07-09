---
key: scheduled-work-fires-predictably
area: scheduling
title: Scheduled work fires predictably
claim: >-
  Cron-scheduled and debounced workflows start when they should, carry the
  latest intended input within max-wait bounds, and compose with queue
  controls without stale handles or unbounded worker pressure.
status: active
provenance: https://docs.dbos.dev/python/tutorials/scheduled-workflows and https://docs.dbos.dev/python/reference/debouncer (predictable starts; latest-input debouncing with max-wait)
explorations:
  - key: timing-smoke
    title: Schedules and debouncers hit their windows
    description: >-
      Baseline timing contract: scheduled workflows fire in their windows
      and debounced calls collapse to the latest input within delay and
      max-wait bounds.
    status: done
    result: null
    reason: null
    workload: workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-000-timing-smoke --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd711p45tzmrwwga4bk0fa3b0s8a6jxe
  - key: scheduled-queue-controls-compose
    title: Schedules compose with queue controls
    description: >-
      Scheduled workflows admitted through queues with concurrency and rate
      controls; the timing contract and the queue bounds must both hold
      when composed.
    status: done
    result: green
    reason: null
    workload: workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-005-scheduled-queue-controls-compose --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: "run 01KX3YE46SXVJ9VH1CG96QCG7S — 13/13 invariants PASS (survived; timing + queue bounds both held)"
    freshness: new-current
    reported: null
    published: nd706f2jc0dppkkfa6zgfasqbs8a6xwf
---

# Scheduled work fires predictably

Evidence lineage: `areas/scheduler-debouncer-timing.md` rungs 000–006.
Rung-005 green at 0c41e6df (13/13 invariants; revalidated on the pinned
target before publication). Overlap policy for long-running scheduled
workflows stays observational until upstream defines the contract (#718).
