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
- 2026-07-11T22:19Z e4 executor durable-contention L1 -> GREEN 29/30 (sweep nd77ba67th62qb4axyhehkeb2s8aayg7),
  redproof PASS (01KX9KS8B3ZH8PQSRB30VH7FMX). 1/30 "failed" state had all ledgers PASS 0
  (no INVARIANT FAIL) = post-verdict non-zero-exit infra flake, not a red (friction logged).
  Concurrent workflow execution through the persistent server is correct. Promoted
  durable-crash-recovery L3 -> ready. Next: the crash-recovery L3 (core reward-red target).
- 2026-07-11T22:19Z e5 executor durable-crash-recovery L3 status: running
- 2026-07-11T22:31Z e5 executor durable-crash-recovery L3 -> GREEN 50/50 (sweep nd7793ca00b86fra3xgtz67cp18abfkj),
  redproof PASS (01KX9M8ZQKMDDDJN371XQWMKEC), crash fired (CLOCK fired crash-restart). DBOS
  correctly skips completed steps under recovery across all 50 seeds — the recovery promise
  holds for this crash model (step-skip). No red (expected; well-guarded). The reward-red
  targets are the candidate backlog's concurrent-recovery race (74) — needs a row-4 refresh.
  Next: enqueue-contention L1, then enqueue-crash-recovery L3.

## session summary — 2026-07-11T22:52Z (initial model exhausted)

Stood up the dbos-usage greenfield lane end-to-end and proved the full loop on REAL
DBOS on the wio worker.

Results (6 scenarios, 2 flows):
- durable-workflow: L0 durable-solo GREEN 15/15, L1 durable-contention GREEN 29/30
  (1 post-verdict exit flake), L3 durable-crash-recovery GREEN 50/50 — the recovery
  step-skip promise holds; each with a passed red-proof. Flow fully floored.
- enqueue-task: L0 enqueue-solo GREEN 15/15 + red-proof. L1 enqueue-contention and
  L3 enqueue-crash-recovery BLOCKED (harness limit, not a DBOS bug — see friction).
- No real RED found. The initial 2-flow model's promises hold under these scenarios.

Infra established (all in friction.md): DBOS must run OUT-OF-PROCESS (in-process hangs
the sandbox watchdog); a persistent ONE-BOOT-PER-RUN server (concurrent requests) is
the working architecture; WIO_WATCHDOG_S=7200 (baked into python-runtime.sh) and a
1200s subprocess timeout (both VIRTUAL time) cover the ~555s-virtual DBOS boot.

Driver bugs found+fixed along the way (6): task effect must be a @DBOS.step; wfid/label
unique per invocation; enqueued task id must be Set to its label; deduplication_id via
SetEnqueueOptions; fast queue polling. Each was caught by a sweep or a critic, not by
the red-proof — the sweep + critic gates are earning their keep.

dispatcher: check.py --status = row 1 (model flows done, no findings, modules covered).
BUT candidates.md has 6 rows above threshold 40 (top: concurrent-recovery race 74) and
staleness (no red in 6 episodes) — per the skill these are row-4 refresh territory, not
a true stop. Held pending direction.
- 2026-07-12T07:34Z e7 executor stream-write-dup-on-retry L0 -> RED (run 01KXAHB5E71FVTQKJMSMDH2QRH seed 1).
  FIRST REAL FINDING: DBOS.write_stream from a @DBOS.step is NOT exactly-once across a step
  retry — write_stream_from_step (_sys_db.py:4229) has no OAOO record (unlike
  write_stream_from_workflow :4265), so a retried step duplicates the streamed value
  (vals ["v","v"], count 2, wf SUCCESS). Crystallized findings/stream-write-dup-on-retry-1.md
  (correctness, held). Already minimal (1 actor/1 flow/depth 1/seed 1). Running test-reviewer
  gate + redproof. Overnight: bg poll wedged 7.5h on empty EID (friction+bounded runner added);
  conductor landed interleave step-timeout lib fix 8952058 -> enqueue rungs now unblocked.
- 2026-07-12T08:00Z e7-REVERT executor test-reviewer gate returned REMOVE on stream-write-dup-on-retry.
  THE "FIRST REAL FINDING" WAS A FALSE POSITIVE — reverted in full. write_stream_from_step having
  no OAOO record is INTENDED: DBOS steps are at-least-once for their body side effects, and the
  vendor's own tests/test_streaming.py:604-659 (test_stream_write_from_step) asserts one stream
  value PER ATTEMPT ("each failure should still write to the stream"). My stream-write-once
  invariant fabricated a contract the product never makes; read_stream promises order+termination,
  not dedup. Removed: findings/stream-write-dup-on-retry-1.md, scenarios/stream-write-dup-on-retry.md,
  the stream-write flow + stream-user persona from usage-model.md, and all StreamWriteFlow/wio_stream_*/
  do_stream driver code. candidates.md row 48 marked REFUTED. check.py = CHECK OK (6 scenarios, 2 flows).
  GATE WORKED: test-reviewer caught this BEFORE it became a dossier to the DBOS maintainer — a bad
  report there costs credibility + the warm intro. Lesson: the e7 refresh promoted a scout "suspected
  gap" into a flow invariant WITHOUT running strategy-critic; strategy-critic must run on every model
  refresh, and a scout gap must be checked against the vendor's own tests before it becomes an invariant.
  Back to true row-1/row-4 posture: 4 GREEN scenarios (red-proofed), 0 findings, model at floor.
- 2026-07-12T08:15Z e8 executor UNBLOCKED enqueue L1+L3 on the lib-fixed image (8952058:
  interleave step-timeout inherits WIO_WATCHDOG_S instead of a hardcoded 30s).
  enqueue-contention L1 (3 producers, depth 30): 30/30 succeeded, 0 violations, all three
  producer ledgers witnessed -> GREEN. replay nd7ej3vc..., redproof 01KXAMMG66KF8QDS5DBB7NSRN4.
  enqueue-crash-recovery L3 (2 producers + crash-restart, depth 50): 50/50 succeeded, 0
  violations, crash fired at swept op-points (op1/op4/...) so recovery was genuinely
  exercised -> GREEN. replay nd7eztwd..., redproof 01KXANDK4BTE27QK0KEEY2GEP4. The e6 BLOCKED
  reds were confirmed a harness artifact, not a DBOS bug: concurrent enqueue and queue-path
  crash-recovery both hold exactly-once + dedup. check.py OK (6 scenarios, 2 flows, blocked=0).
  Model now at floor for both core flows (L0/L1/L3 all green; L2 interaction + L4 horizon open).
  Next: row-4 producer refresh for the concurrent-recovery race (candidate 74) — needs a
  concurrent-recovery event + a second live-executor persona (cross-process, not interleave).
