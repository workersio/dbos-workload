# Journal — dbos usage-first lane

## config

- wio-project: kn71mb4pcxmees43sy547v76z98a7fv0  (DBOS Workload — this repo, main)
- runs: draft-only (never pass --exploration; nothing publishes in this lane)
- max-loops: 100
- max-runs: 250
- staleness-k: 5
- api-floor-share: 0.3
- candidate-threshold: 40
- lane: greenfield usage-first (draft-only; nothing publishes)
- filing: findings accumulate in findings/ as status: held; ≤2 open issues
  per repo; the human sends — never the loop

## log

- 2026-07-11 init: .workers/ v2 scaffold (usage-first greenfield); v1 corpus
  preserved on main; lib copied from workload-harness skill @ usage-first

## log (cont.)

- 2026-07-11T20:53Z e1 producer: first usage model from 4-scout fan-out (README/docs, vendor
  tests, recovery/queue engine, notifications/scheduler). 2 hottest flows
  (durable-workflow, enqueue-task) + crash-restart event + 41-module G8 floor.
  Wrote flows_dbos.py drivers (real DBOS boot mirroring tests/conftest; crash
  injected via status->PENDING + _recover_pending_workflows) — the scaffold
  folds "first executor writes the two hottest drivers" into this bootstrap so
  check.py is green. Emitted batch of 6 scenarios (durable/enqueue ×
  solo/contention/crash); L0s ready, L1/L3 planned. candidates.md holds 6 ranked
  backlog rows (top: concurrent-recovery race 74). check.py OK. strategy-critic
  model+set audit dispatched.
- 2026-07-11T20:54Z e2 executor durable-solo L0 status: running
- 2026-07-11T20:56Z e2 strategy-critic (model+set audit) → fix-first: (1) enqueue task effect
  was in the workflow body not a @DBOS.step → recovery would legitimately double
  it and manufacture a false red on task-completes-once — FIXED (wio_task_step).
  Kept the global SUCCESS->PENDING crash reset (faithful whole-process crash; with
  the effect in a step, recovery skips it so no cross-actor false red). Documented
  that durable-crash-recovery probes step-skip-under-recovery, not yet true
  mid-flight partial resume. Model follow-ups for next row-4 refresh: (a) fan-out/
  fan-in (a workflow that enqueues child tasks) — the canonical DBOS journey the
  two disjoint flows can't express; (b) notifications send/recv as the primitive
  apps use to observe the crash promise; (c) concurrent multi-executor recovery of
  the SAME row (candidates.md top row, 74). Re-checking + re-preparing.
- 2026-07-11T21:31Z e2 executor durable-solo L0 -> GREEN 15/15 (exploration nd77qf2n1gvemg13w5px9yfszs8aa6vp),
  redproof PASS (run 01KX9GH6PHB59KVRBZMYDFGRD7). MILESTONE: full producer->executor->GREEN
  cycle proven on real DBOS. Infra path established: DBOS must run out-of-process
  (in-process hangs the sandbox watchdog); WIO_WATCHDOG_S=7200 + subprocess timeout 1200
  (both virtual-time); DBOS boot ~555s virtual/~20s real. Two driver bugs found+fixed via
  the sweep (task effect must be a @DBOS.step; wfid/label unique per invocation).
  durable-solo done; promoted durable-contention L1 -> ready. Next: enqueue-solo L0.
- 2026-07-11T21:32Z e3 executor enqueue-solo L0 status: running
- 2026-07-11T22:10Z e3 executor enqueue-solo L0 -> GREEN 15/15 (sweep nd7cvgjeahdmxhsf0718asg0zh8aat50),
  redproof PASS (01KX9KEQNPR4D4J9BQ43FV34SE). Big infra win: replaced per-invocation
  subprocess boots with a persistent one-boot-per-run DBOS server (concurrent requests)
  -> enqueue no longer times out. Three enqueue driver bugs found+fixed: task effect must
  be a @DBOS.step; enqueued task id must be Set to its label (auto-UUID broke status/result
  lookup); deduplication_id goes via SetEnqueueOptions, not an enqueue kwarg. dedup confirmed:
  DBOSQueueDeduplicatedError raised, duplicate never ran. Both L0 floors GREEN. Promoted
  enqueue-contention L1 -> ready. Next: L1 contention (durable-contention, enqueue-contention).
- 2026-07-11T22:11Z e4 executor durable-contention L1 status: running
