---
key: enqueue-crash-recovery
rung: L3
cast: {task-producer: 2}
flows: [enqueue-task]
event: {key: crash-restart, at: crashclock}
depth: 50
status: done
result: green
replay: {run: nd7eztwdk9vmhr804mtrv3v4k58ac0sw, seed: all}
redproof: {run: 01KXANDK4BTE27QK0KEEY2GEP4, seed: 2937247645}
invariants: [task-completes-once, dedup-id-enforced]
story: >-
  The server crashes while queued tasks are running; after restart each task
  still finishes exactly once.
---
L3 recovery probe for the queue path: a crash-restart lands mid-flow; recovery
re-enqueues/re-runs, and each task must still complete exactly once (no double
execution). No-event siblings are enqueue-solo/contention. Promote after L1.

GREEN (e8): 50/50 seeds succeeded, zero violations; both producer ledgers witnessed
their effects. The crash-restart event fired at swept op-points across seeds
(crash-restart@op1, @op4, …) — a real mid-flow crash, not a slow L1 — and recovery
drove every queued task to exactly-once completion. Redproof caught a planted
acked-but-not-observed on task-producer-1. Was BLOCKED at e6 by the interleave 30s
step-timeout artifact; unblocked by lib fix 8952058 (step-timeout inherits
WIO_WATCHDOG_S). The queue path holds exactly-once across crash-recovery.
