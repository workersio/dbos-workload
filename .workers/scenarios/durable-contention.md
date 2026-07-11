---
key: durable-contention
rung: L1
cast: {workflow-runner: 3}
flows: [durable-workflow]
depth: 30
status: ready
result: null
replay: null
redproof: null
invariants: [step-exactly-once, resumes-after-crash, workflow-terminal]
story: >-
  Three jobs run at the same time; none of them loses a step or repeats one.
---
L1 same-flow contention: three concurrent workflow-runners interleaved by the
spine's scheduler. Each actor's workflow id is distinct, so exactly-once must
hold per-actor under concurrent execution. Promote to ready once durable-solo
is done.
