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
