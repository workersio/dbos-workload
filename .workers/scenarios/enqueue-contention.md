---
key: enqueue-contention
rung: L1
cast: {task-producer: 3}
flows: [enqueue-task]
depth: 30
status: blocked
result: blocked
replay: null
redproof: null
invariants: [task-completes-once, dedup-id-enforced]
story: >-
  Three producers fill the queue at the same time; every task still completes
  exactly once.
---
L1 contention on the shared queue: three concurrent producers. Each actor's task
labels are distinct; exactly-once and dedup must hold under concurrent enqueue.
Promote to ready once enqueue-solo is done.


BLOCKED (e6, harness limitation): multi-actor enqueue false-reds at the interleave
scheduler's 30s step-timeout — one serialized DBOS async-queue request (enqueue +
get_result) exceeds 30s under the sandbox (flow_crash 'blocked at ...'). Not a DBOS
bug. Needs a skill/lib change (configurable interleave step_timeout / async-aware
barrier). enqueue-solo L0 (single actor, inline, no scheduler) is GREEN, so the flow
and oracle are sound; only the multi-actor rungs are gated. See ../../friction.md.
