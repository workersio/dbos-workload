---
key: enqueue-crash-recovery
rung: L3
cast: {task-producer: 2}
flows: [enqueue-task]
event: {key: crash-restart, at: crashclock}
depth: 50
status: planned
result: null
replay: null
redproof: null
invariants: [task-completes-once, dedup-id-enforced]
story: >-
  The server crashes while queued tasks are running; after restart each task
  still finishes exactly once.
---
L3 recovery probe for the queue path: a crash-restart lands mid-flow; recovery
re-enqueues/re-runs, and each task must still complete exactly once (no double
execution). No-event siblings are enqueue-solo/contention. Promote after L1.
