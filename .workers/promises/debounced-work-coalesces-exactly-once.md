---
key: debounced-work-coalesces-exactly-once
area: scheduler-debouncer-timing
title: Debounced work coalesces exactly once
claim: >-
  A burst of debounce calls on one key coalesces into workflow executions that
  each run exactly once, carry the latest input that landed in their debounce
  cycle, and never silently drop a bounce that returned a handle. Under
  concurrent bouncers racing the DELAYED→ENQUEUED flip, the new DELAYED-workflow
  protocol must not double-execute a settled window or lose a bounce entirely.
status: active
provenance: https://docs.dbos.dev/python/reference/debouncer (#752 reimplements the debouncer on DELAYED workflows + the debounce_delayed_workflow SQL protocol; the dedup key is the sole coalescing mechanism)
explorations:
  - key: concurrent-bounce-coalescing
    title: Concurrent bounces coalesce to one execution with the latest input
    description: >-
      M concurrent threads issue K debounces of one key with jittered timing
      spanning the fire boundary. Oracle: each settled debounce cycle produces
      exactly one execution (no double-execute from the enqueue/bounce retry
      race), that execution carries the latest input committed in its cycle, and
      every debounce() call that returned a handle resolves — no bounce is
      silently dropped. Execution count never exceeds the number of bounces.
    status: ready
    result: null
    reason: null
    workload: workloads/debounce-coalescing/debounce_coalescing_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/debounce-coalescing/debounce_coalescing_workload.py --rung rung-001-concurrent-bounce-coalescing --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: null
---

# Debounced work coalesces exactly once

Evidence lineage: `areas/scheduler-debouncer-timing.md`, work item `e-029`.
New corridor from the diff-directed scan of #752 (Debounce With Delay), which
reimplemented the debouncer on `DELAYED` workflows + `debounce_delayed_workflow`
(`dbos/_sys_db.py`). The existing debouncer rungs (E-008 starvation, rung-001
latest-input single-threaded) target the OLD mechanism — rung-006's stale
condition literally says "mark stale if DBOS replaces the debouncer with true
DELAYED workflow rows," which #752 did. The PR's own tests are all
single-threaded; concurrent bounce coalescing on the new SQL protocol is
unharvested and untested. Distinct from #718 (schedule overlap policy).
