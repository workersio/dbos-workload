---
key: enqueue-contention
rung: L1
cast: {task-producer: 3}
flows: [enqueue-task]
depth: 30
status: done
result: green
replay: {run: nd7ej3vc9kgnnyhaqfsedesx498adasj, seed: all}
redproof: {run: 01KXAMMG66KF8QDS5DBB7NSRN4, seed: 2023661332}
invariants: [task-completes-once, dedup-id-enforced]
story: >-
  Three producers fill the queue at the same time; every task still completes
  exactly once.
---
L1 contention on the shared queue: three concurrent producers. Each actor's task
labels are distinct; exactly-once and dedup must hold under concurrent enqueue.
Promote to ready once enqueue-solo is done.

GREEN (e8): 30/30 seeds succeeded, zero violations; all three producer ledgers
witnessed their effects (not VOID). Redproof caught a planted acked-but-not-observed
on task-producer-2. Was BLOCKED at e6 by the interleave scheduler's hardcoded 30s
step-timeout (a serialized DBOS async-queue enqueue+get_result exceeds 30s in virtual
time); unblocked by lib fix 8952058 — the step-timeout now inherits WIO_WATCHDOG_S.
The e6 reds were a harness artifact, not a DBOS bug; concurrent enqueue holds
exactly-once and dedup.
