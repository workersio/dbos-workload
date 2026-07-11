---
key: enqueue-crash-recovery
rung: L3
cast: {task-producer: 2}
flows: [enqueue-task]
event: {key: crash-restart, at: crashclock}
depth: 50
status: blocked
result: blocked
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


BLOCKED (e6, harness limitation): multi-actor enqueue false-reds at the interleave
scheduler's 30s step-timeout — one serialized DBOS async-queue request (enqueue +
get_result) exceeds 30s under the sandbox (flow_crash 'blocked at ...'). Not a DBOS
bug. Needs a skill/lib change (configurable interleave step_timeout / async-aware
barrier). enqueue-solo L0 (single actor, inline, no scheduler) is GREEN, so the flow
and oracle are sound; only the multi-actor rungs are gated. See ../../friction.md.
