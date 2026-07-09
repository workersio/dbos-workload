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
    status: done
    result: green
    reason: >-
      Cloud-confirmed green (run 01KX47FF2KHTYPY50VCVFSP6BX, commit 76366ac):
      the #752 DELAYED-based debouncer coalesces concurrent bounce bursts
      correctly — each window runs exactly once with its window-latest input,
      no double-execution, no lost handle, global-latest always present, no
      cross-window input leak. Robust across cloud timing (case-002 split into
      8 windows, all green). A first run exposed a fragile single-window oracle
      (not a product bug, see runs/E-029.md); recalibrated and re-run green.
    workload: workloads/debounce-coalescing/debounce_coalescing_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/debounce-coalescing/debounce_coalescing_workload.py --rung rung-001-concurrent-bounce-coalescing --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: "run 01KX47FF2KHTYPY50VCVFSP6BX — all cases green (case-002 multi_window_bounded x8, case-003 single_execution_strong). Evidence: runs/E-029.md"
    freshness: new-current
    reported: null
    published: nd710fnc0xy88v6rk62s27ms318a7bsv
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
