---
key: durable-contention
rung: L1
cast: {workflow-runner: 3}
flows: [durable-workflow]
depth: 30
status: done
result: green
replay: {run: nd77ba67th62qb4axyhehkeb2s8aayg7, seed: all-but-1}
redproof: {run: 01KX9KS8B3ZH8PQSRB30VH7FMX, seed: 1671500713}
invariants: [step-exactly-once, resumes-after-crash, workflow-terminal]
story: >-
  Three jobs run at the same time; none of them loses a step or repeats one.
---
L1 same-flow contention: three concurrent workflow-runners interleaved by the
spine's scheduler. Each actor's workflow id is distinct, so exactly-once must
hold per-actor under concurrent execution. Promote to ready once durable-solo
is done.
