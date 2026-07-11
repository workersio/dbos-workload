---
key: enqueue-contention
rung: L1
cast: {task-producer: 3}
flows: [enqueue-task]
depth: 30
status: planned
result: null
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
