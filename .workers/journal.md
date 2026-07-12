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
- 2026-07-12T09:00Z e9 producer/investigation candidate-74 (concurrent-recovery race on durable-workflow)
  REFUTED before building any harness — the strategy-critic gate + a cheap in-repo source probe
  killed it. Pipeline: scout (mapped the recovery-ownership mechanism: recovery is executor_id-
  partitioned so the race is only reachable via cross-executor recovery — admin /dbos-workflow-
  recovery or _recover_pending_workflows([other_id]); the recovery upsert _sys_db.py:693-766 has no
  CAS) -> strategy-critic returned REFRAME (my 3-clause invariant was guarded-by-construction: PK
  tautology on operation_outputs, SUCCESS-guarded DLQ, OAOO convergence; pointed at one surviving
  seam: update_workflow_outcome _sys_db.py:862-891 is last-writer-wins guarding only CANCELLED, so
  a loser could in theory clobber SUCCESS->ERROR) with a PROBE GATE: check whether a racing-step
  DBOSWorkflowConflictIDError escapes or is swallowed BEFORE building a two-process Postgres harness.
  Probe (read _core.py:1784-1877 intercept/record, _outcome.py Immediate._intercept, _sys_db.py:2417-
  2496 record_operation_result, _core.py:560-614 persist): the conflict ESCAPES the step layer (no
  retry on a plain @DBOS.step, _intercept doesn't re-run on exception) BUT is caught at the WORKFLOW
  finalizer _core.py:594-602 ("Aborting duplicate execution" -> await_workflow_result -> returns the
  WINNER's SUCCESS result). DBOS deliberately CONVERGES; the loser never terminalizes ERROR, so the
  SUCCESS-clobber seam is unreachable. Verdict: DROP. Only residual = at-least-once step side effects
  (documented, weight 0-1, #767-shape). Model unchanged (no persona/event/flow added — nothing to
  falsify). candidates row 74 marked REFUTED with the mechanism. GATE VALUE: the probe read ~5
  functions and avoided building a two-live-executor cross-process Postgres harness for a non-bug.
  SIGNAL: both crystallization attempts so far (write_stream e7, concurrent-recovery e9) refuted at
  the source — DBOS's durability CORE (steps/recovery OAOO) is well-hardened; remaining candidates
  (70 queue×cancel×restart, 58 dedup×concurrency, 52 notifications-OAOO, 44 illegal-transitions)
  target the LESS-hardened management/notifications surface. Adopting "cheap source probe before
  harness build" as the standing discipline for every candidate.
- 2026-07-12T09:40Z e10 producer/model-refresh candidate-58/70 cluster (queue × recovery) probed;
  ONE reachable weight-2 seam survived and is PROMOTED. Scout probe over the queue+recovery code
  refuted 3 of 4 targets with explicit guards (cancel×queue: dequeue predicate needs queue_name==name
  and cancel sets queue_name=None _sys_db.py:917-936; dedup-survival: clear_queue_assignment never
  nulls deduplication_id _sys_db.py:4018-4020; double-dispatch correctness: converges via the same
  OAOO handler as e9). The survivor: QUEUE CONCURRENCY-CAP VIOLATION under false-death recovery.
  Mechanism: the global cap is count-PENDING-then-dispatch (_sys_db.py:3877-3899) and worker_concurrency
  is counted from a PER-PROCESS in-memory set (ActiveWorkflowById, _core.py:619-639); clear_queue_assignment
  (_recovery.py:16-22) re-enqueues a live executor's in-flight queued rows with NO liveness/CAS check.
  A second live executor that wrongly believes the first died recovers its rows, they drop out of the
  PENDING count, and the second executor dequeues+runs them AGAIN while they're still running on the
  first -> up to 2N bodies against a concurrency=N queue. CONTRACT CITATION IS THE VENDOR'S OWN TEST:
  test_queue_concurrency_under_recovery (tests/test_queue.py:1289-1353) recovers a LIVE executor
  ('local', :1343) and asserts the cap holds (counter stays 2, :1350) — but only single-process, where
  the in-memory guard hides the cross-process gap. This INVERTS the e7 write_stream lesson: there the
  vendor test CONTRADICTED my invariant (so REFUTE); here the vendor test ASSERTS my invariant (so
  BUILD). No convergence (unlike e9) — the cap breach is a real runtime concurrency violation.
  Model refresh: added event false-death-recovery (amp 20) + invariant queue-concurrency-capped on
  enqueue-task, both cited to the vendor test + cap code. No new flow (queue path already enqueue-task);
  G2 bijection intact. Oracle MUST be a cluster-wide live gauge (shared counter incremented on entry /
  decremented on exit), NOT a row count (DB settles back to N) — scout's explicit warning. Next:
  strategy-critic on the two-process harness design, then executor build (two live DBOS processes on one
  Postgres + a DB-backed concurrency gauge + a cross-process barrier holding N bodies live during recovery).
- 2026-07-12T11:30Z e11 executor enqueue-cap-under-recovery (queue concurrency-cap under false-death
  recovery) — driver BUILT and correct, but BLOCKED on a harness limitation; finding stays
  source-confirmed / execution-blocked (NOT crystallized — no reproduced red, integrity intact).
  Built a full two-live-executor harness in flows_dbos.py: executor A fills a Queue(concurrency=4)
  with 4 cap_wf bodies that block on a Postgres advisory-lock gate (A holds EXCLUSIVE, each body waits
  on SHARED — a real PG-side barrier immune to the virtual clock); a cluster-wide gauge table
  (wio_cap_gauge cur/mx) self-records the peak concurrency; A spawns executor B (DBOS__VMID=wioB) which
  recovers wioA's live queued rows so B's poller re-dispatches them past the cap. Debugged SIX real
  integration issues to get here: (1) Queue() built before DBOS() hangs boot -> move after; (2) bare
  postgresql:// gauge url defaulted to absent psycopg2 -> psycopg3; (3) A gave up before B booted
  because its waits used virtual sleeps that fast-forward past B's ~20s real boot -> DB-state
  coordination via a coord table; (4) unthrottled spins = millions of real DB reads -> pg_sleep
  (server-side, real time) throttling; (5) the actor plan runs the flow 5x with the event arming at
  op4 -> cache the race result once per SUT; (6) un-drained B pipe filled -> drain thread. FINAL
  BLOCKER (unfixable from the driver): executor B, a second full Python/DBOS process spawned by the
  driver, HANGS at `import dbos` under the deterministic sandbox. faulthandler dump: frozen in plain
  stdlib import (dis -> opcode.py, importlib._bootstrap), prints B0/B1/B2 then never returns; not
  env-based (stripping LD_PRELOAD/FAKETIME* -> stripped:[] and still hangs). The sandbox traps a
  driver-spawned grandchild at import. Single-process repro is impossible BY CONSTRUCTION (vendor test
  test_queue.py:1350 shows the in-memory ActiveWorkflowById guard blocks re-run within one process;
  the breach requires a second guard-less executor). Scenario marked BLOCKED with the full note;
  friction row added (harness needs multi-live-executor support — blocks the whole cross-executor
  finding class where DBOS is weakest). The finding is well-evidenced (vendor's own test asserts the
  cap holds under recovery; the cross-process gap is real in source) and should be revisited if/when
  the harness gains multi-SUT support — mirrors how lib fix 8952058 unblocked the enqueue rungs.
  DISPATCHER: with this blocked, remaining above-threshold candidates (52 notifications-OAOO, 44
  illegal-state-transitions) are single-process and buildable; 58/70 (queue×recovery) share the
  now-blocked cross-executor limitation. Model: 2 flows floored (durable/enqueue L0/L1/L3 green).
- 2026-07-12T12:15Z e12 producer/probe single-process candidates 52 (notifications-OAOO) + 44
  (illegal-state-transitions) scouted before any build. 52 REFUTED: send/recv/set_event/get_event
  are OAOO-guarded and vendor-tested (send_bulk = _check_operation_execution_txn + message_uuid PK
  on_conflict_do_nothing + step record in one txn, dedup asserted tests/test_dbos.py:1031-1050; recv
  re-reads its recorded result; set_event last-write-wins upsert); only un-OAOO path (send from a bare
  step) = same at-least-once precedent already refuted for write_stream. 44 mostly hardened but ONE
  reachable red: DBOS.fork_workflow(nonexistent_id,1) raises raw Exception("Workflow ... not found")
  (_sys_db.py:1180), NOT the typed DBOSNonExistentWorkflowError its siblings raise (retrieve_workflow
  _client.py:520-523); no pre-existence check, no test. Weight 1 (wrong-error / error-contract). Other
  44 sub-cases REFUTED (resume-terminal guarded notin_[SUCCESS,ERROR]; cancel idempotent; fork
  out-of-range/from-PENDING vendor-tested/tolerant tests/test_async.py:957-990). Cross-executor
  candidates 58/70 marked BLOCKED-CLASS (same harness multi-executor limit as e11). Dispatcher: the
  top remaining reachable red is the weight-1 fork-nonexistent; pursuing it because it crystallizes the
  fleet's FIRST finding and proves the full producer->executor->RED->finding pipeline end-to-end (a
  stated milestone), and it maps onto the existing error-contract oracle. Building management flow next.
- 2026-07-12T13:00Z e13 executor+crystallize management-illegal-transitions -> FLEET'S FIRST FINDING.
  Real run depth-3: all 3 seeds RED (error_contract FAIL: fork-missing raised undocumented 'Exception'
  where DBOSNonExistentWorkflowError is contracted); the two control ops (resume-terminal,
  cancel-cancelled) stayed GREEN so the oracle discriminates. Redproof: plant landed on the green
  control resume-terminal -> ORACLE_SELFTEST PASS (meaningful, not trivially-satisfied). test-reviewer
  gate: KEEP — and strengthened it: DBOS's OWN fork_workflow_async (_dbos.py:2304) + the export path
  (_sys_db.py:4708) raise the typed error, so the SYNC fork_workflow (_sys_db.py:1180 raw Exception)
  is the lone outlier; tests/test_client.py:664-666 asserts the typed error for retrieve_workflow on a
  missing id (vendor test SUPPORTS the contract — opposite of the e7 write_stream case). Transaction
  rolls back -> no state corruption -> weight 1 (wrong-error) honest. Crystallized
  findings/fork-nonexistent-raw-exception.md (status held; replay nd7cr1kb...; redproof
  01KXAWNRK248Y3J8V0C2BMS39Y). This proves the full producer->executor->RED->finding pipeline
  end-to-end on the box (the stated next milestone). check.py --status = row 1.
- 2026-07-12T13:30Z e14 producer WAVE-2 MODEL EXPANSION kickoff (row-4 refresh, Viswa-directed): row 1
  is only CURRENT-model-exhausted, not north-star done — ~25 modules parked, and the scouts flagged the
  less-hardened surface as where DBOS bugs live. Expanding into SINGLE-PROCESS parked flow families
  (cross-executor class stays parked — grandchild-import platform limit, product task filed). Launched
  4 parallel candidate-scouts (cheap source-probe + vendor-test-check discipline, single-process only,
  find reachable weight>=2 reds or refute with the guard/vendor test): (1) scheduler/cron
  (_scheduler.py/_croniter.py — per-tick exactly-once, missed-tick catchup, cron correctness);
  (2) client management ops beyond the fork finding (fork-replay correctness, gc-of-live, list
  pagination, cancel->resume round-trip); (3) debouncer (coalescing/last-wins/durability) + datasource
  decorator (transaction OAOO/rollback); (4) notifications recv/timeout/ordering/event-durability
  journey (NOT OAOO, already refuted). Next: synthesize -> strategy-critic on the refresh -> model
  refresh for confirmed reds -> build with red-proof. Skip refuted.
- 2026-07-12T14:00Z e14 WAVE-2 scout synthesis (4 parallel scouts, all single-process, vendor-test
  discipline). REACHABLE REDS: (A) gc-dangling-child -> stranded parent [weight 2, availability,
  DETERMINISTIC]: garbage_collect (_sys_db.py:4414-4424, _app_db.py:186-256) guards only a row's OWN
  status (PENDING/ENQUEUED/DELAYED), not inbound references; a terminal child aged past the gc cutoff
  is deleted while a PENDING parent holds its child_workflow_id in operation_outputs, so on parent
  recovery await_workflow_result (_sys_db.py:1604-1609, unbounded while-True poll) gets NoResult
  forever -> strand. Vendor gc test (test_workflow_management.py:1017-1148) uses INDEPENDENT siblings,
  never a parent/child graph -> real coverage hole. Oracle: parent acked to resume->SUCCESS; after
  gc+restart+bounded-wait it's still PENDING -> acked_lost/stranded. CRUX to verify: await on a DELETED
  child = hang (strand) vs raise (different bug). (B) notifications non-FIFO [weight 3, correctness,
  PROBABILISTIC on our Postgres]: notifications is the ONLY ordered-delivery table lacking a monotonic
  tiebreak (streams have offset, operation_outputs have function_id); created_at_epoch_ms is txn-scoped
  (identical under send_bulk one-txn insert), PK gen_random_uuid() random, consume ORDER BY
  created_at_epoch_ms ASC LIMIT 1 with NO secondary sort (_sys_db.py:3007-3010) -> tie broken by
  heap/index order; vendor's own test_dbos.py:1170 asserts FIFO 'a-b-c' the schema doesn't guarantee.
  Repro depends on heap-order luck on single Postgres (green-trivial risk). REFUTED/HARDENED: scheduler
  (exactly-once per deterministic sched-<name>-<isotime> id + dedup, catchup via automatic_backfill,
  croniter DST/tz all vendor-tested; only */n-seconds wrapper firing untested = likely-green gap);
  debouncer (coalescing/last-wins/atomic-checkpoint all vendor-tested); datasource-OAOO/rollback (OAOO
  atomic + vendor-tested; rollback-on-error an untested-but-likely-green gap, needs datasource fixture
  infra); notifications timeout(durable absolute deadline)/events(atomic+durable)/edges(FK->
  DBOSNonExistentWorkflowError, recv-outside-wf raises) all refuted. Build order: gc-strand first
  (deterministic weight-2), notifications-FIFO second (weight-3 but needs a deterministic repro
  strategy). Next: strategy-critic on both before build.
- 2026-07-12T15:30Z e15 WAVE-2 BUILD (producer+executor). Strategy-critic(opus) ruled: gc-dangling-child
  BUILD (crux settled at source — get_result re-awaits, never short-circuits on recorded result:
  record_get_result "no corresponding check" _sys_db.py:2519, _core.py:169-173; check_workflow_result
  returns NoResult for not-found row :1583/1602; await_workflow_result loops :1604-1609; child_workflow_id
  has NO FK _schemas/system_database.py:171; gc guards own status only :4415-4425). notifications-FIFO
  DE-RISK→SHELVED (naive send_bulk->recv == passing vendor test test_dbos.py:1150-1170; heap-luck-green on
  single Postgres; only reachable via heap-disorder injection or CockroachDB). Built flow `workflow-graph`
  (invariant graph-survives-retention-gc) + scenario workflow-graph-retention-gc (L3, cast ops-operator,
  depth 3): control arm (crash+recover, no gc -> green) + strand arm (crash+gc+recover -> PENDING strand).
  Driver do_gcstrand: parent calls child (SetWorkflowID), child SUCCESS, force parent PENDING, gc(cutoff=now)
  deletes aged child, _recover_pending_workflows (async) re-awaits deleted child -> strand; bounded
  wait_terminal so the oracle never hangs; cleanup re-materializes child to drain the hung recovery thread.
  Ledger oracle: acked graph-result parent durable; strand observed PENDING -> acked_lost (weight 2,
  availability). Model refresh: workflow-graph added to FLOWS + ops-operator flows + _workflow_commands/
  _app_db/_recovery coverage; scheduler/debouncer/datasource parked-with-reason (probed hardened e14).
  check.py OK (9 scenarios, 4 flows). AST-validated driver; local run blocked (venv cloud-only). Committed
  4e0171e, pushed, prepare requested. NEXT: cloud redproof (ORACLE_SELFTEST PASS) then real depth-3;
  crystallize if strand reds; PAGE Viswa on the red.
- 2026-07-12T16:10Z e16 CRYSTALLIZE — gc-strand RED confirmed + filed locally. Fixed the executor bug
  (recover GlobalParams.executor_id=wioA, not default ["local"] — get_pending_workflows filters on
  executor_id _sys_db.py:1953; recovering "local" re-dispatched nothing so BOTH arms falsely PENDING).
  After fix, clean discrimination on cloud depth-3 (EID nd7ar33zr6vktnp5f262v1bqc98ad1b6, all 3 seeds RED):
  control {n_rec:1, after:SUCCESS, child_after:SUCCESS} GREEN; strand {n_rec:1, after:PENDING,
  child_after:null, child_gone:true} RED; oracle FAIL 1 strand-only (discriminates). Redproof
  01KXB0153P12GBK3A5YXKTB24Z ORACLE_SELFTEST PASS. test-reviewer(opus)=KEEP (all source claims verified,
  two-sided oracle, control isolates gc, not vendor-intended, faithful crash model, non-masking cleanup).
  Wrote findings/gc-deletes-referenced-child-strands-parent.md (class availability, weight 2, status held,
  maintainer-ready repro, zero product vocab). check.py OK (9 scenarios, 4 flows). This is the fleet's 2nd
  crystallized finding (after fork-nonexistent, weight 1). PAGING Viswa per the wave-2 directive (page on
  reds). Model state: workflow-graph flow done@L3; parked modules now honestly probed (scheduler/debouncer/
  datasource hardened e14, notifications-FIFO shelved heap-luck). Next dispatcher cycle: model-refresh
  status — remaining reachable single-process surface is thin; cross-executor class still platform-blocked.
