---
key: workflow-lifecycle-is-safe
area: workflows
title: Lifecycle operations are safe
claim: >-
  Operators and clients can cancel, resume, fork, delete, time out, and
  recover workflows without resurrecting terminal work, orphaning children,
  or leaking system rows — terminal states are immutable and cancellation
  reaches every descendant.
status: active
provenance: https://docs.dbos.dev/python/tutorials/workflow-management (cancel/resume/fork semantics); child-cancellation hardened in PRs #701/#703
explorations:
  - key: lifecycle-state-machine-core
    title: Terminal states reject stale commands
    description: >-
      Cancel after the final step, delayed cancel before the poller, and
      stale management commands after success or error — the workflow state
      machine must refuse to resurrect terminal work.
    status: done
    result: null
    reason: null
    workload: workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-001-state-machine-core --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd76hw6cws6nepks987kdckcrx8a7rbq
  - key: cancel-children-terminal-immutability
    title: Cancellation reaches every descendant
    description: >-
      Recursive cancellation from runtime and client must clean up queued
      descendants, keep cancelled results retrievable, and hold terminal
      CANCELLED immutable — the regression guard on the child-cancellation
      fixes in PRs #701 and #703.
    status: done
    result: null
    reason: null
    workload: workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung rung-005-cancel-children-terminal-immutability --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd70te49s8yahr2zfdz7qtfjhn8a70wp
---

# Lifecycle operations are safe

Evidence lineage: `areas/lifecycle-fork-state.md` rungs 001–005. Rung-005
proven green on the pinned target 3df88c4b. Two minimized findings closed
upstream as intended behavior: fork replacement-children linkage (#735)
and global_timeout cancelling future DELAYED workflows.
