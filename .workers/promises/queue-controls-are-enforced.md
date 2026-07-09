---
key: queue-controls-are-enforced
area: queues
title: Queue controls are enforced
claim: >-
  A queue configured with concurrency caps, worker concurrency, rate limits,
  priority, dedupe, or partitions never admits more concurrent or more
  frequent workflow executions than configured — under composed controls,
  live config changes, and executor relaunch.
status: active
provenance: https://docs.dbos.dev/python/tutorials/queue-tutorial (concurrency, worker_concurrency, rate limiter, priority, dedupe, partitions are enforced bounds)
explorations:
  - key: queue-ledger-controls-baseline
    title: Single-queue ledger controls baseline
    description: >-
      One queue exercising dedupe, priority, and concurrency ledger checks
      with no induced faults; every admission and completion must respect the
      configured bounds, verified against a ground-truth ledger.
    status: done
    result: null
    reason: null
    workload: workloads/queue-composed-controls/queue_composed_controls_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-001-single-queue-ledger-controls --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd79y87tj409t61fgmwz3hk1ws8a7ngq
  - key: queue-rate-limit-under-plan-churn
    title: Rate limit holds under query-plan churn
    description: >-
      Rate-limited queue admission measured while the limiter's backing query
      flips between partial-index plans under load; the configured
      events-per-period bound must hold regardless of plan choice.
    status: done
    result: null
    reason: null
    workload: workloads/queue-composed-controls/queue_composed_controls_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-008-rate-limit-partial-index-plan --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd74776d32cqcwbf7zstz65nbn8a6ak2
  - key: queue-partition-worker-concurrency
    title: Partition worker concurrency cap
    description: >-
      Async partitioned workers against a queue with worker_concurrency set;
      the per-partition active-workflow count must never exceed the cap.
      Legacy hunt observed 3 active with a cap of 2 (E-006) — this scenario
      is the standing guard on that finding, green again on the current
      target (case-001 minimized, 900s).
    status: done
    result: null
    reason: null
    workload: workloads/queue-composed-controls/queue_composed_controls_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-007-async-partition-worker-concurrency --case case-001
    faults: []
    depth: 1
    timeout: 900
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd72q8kq7x6nr605vzt6dc7btn8a7qna
---

# Queue controls are enforced

Evidence lineage: `areas/queue-composed-controls.md` rungs 001–008;
curated history E-006 (partition worker-concurrency over-admission,
target a4237179) and E-027 (rate-limit plan churn green at 3df88c4b).
