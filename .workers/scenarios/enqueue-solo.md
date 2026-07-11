---
key: enqueue-solo
rung: L0
cast: {task-producer: 1}
flows: [enqueue-task]
depth: 15
status: done
result: green
replay: {run: nd7cvgjeahdmxhsf0718asg0zh8aat50, seed: all}
redproof: {run: 01KX9KEQNPR4D4J9BQ43FV34SE, seed: 1696635219}
invariants: [task-completes-once, dedup-id-enforced]
story: >-
  You drop a few tasks in the queue; each one runs exactly once and hands back
  its result, and a duplicate with the same dedup id is refused.
---
L0 floor for enqueue-task: one task-producer enqueues K tasks plus a dedup pair.
Each task must complete once (result collectable) and the duplicate dedup enqueue
must be refused and never run. Establishes GREEN before contention/crash rungs.
