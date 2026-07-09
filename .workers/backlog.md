# Backlog — ranked corridor pool

The loop's ranked candidate pool (skill spec-format §Backlog). Score = severity
weight (data-loss 4 / correctness 3 / availability 2 / wrong-error 1 / cosmetic 0)
× bug-likelihood, adjusted by novelty vs harvested surfaces. **Header threshold:
promote at score ≥ 6.** Below threshold + no ready/in-flight work + no trigger =
coverage exhausted (dispatcher row 1).

Source of this batch: diff-directed scan of `9922c1d..a43fead` (#752 debounce,
#751 incremental GC, #763 behavior consistency), corroborated by candidate-scout.

## Active (above threshold)

| # | corridor | commit | area | severity×likelihood | score | state |
|---|----------|--------|------|---------------------|------:|-------|
| L1 | **gc-orphan-oaoo** — GC two-phase orphan (`workflow_status` deleted, `transaction_outputs` orphaned) → reused workflow id replays dead workflow's step output instead of executing fresh | #751 | garbage-collection-durability | correctness/data 3–4 × high | 12 | **DONE → e-028 RED finding** (cloud `01KX460BYM2JHVTJKT2XBQE4WN`; filing held for triage) |
| L1b | **gc-appdb-batch-partial / gc-vs-recovery** — same-path sibling bumped by L1's red: app-db's own batched delete loop failing partway (PR's resumable test only faults the sys-db side) orphans a different row set; and GC racing concurrent recovery/status transitions. Same OAOO oracle as e-028, distinct trigger. | #751 | garbage-collection-durability | correctness/data 3–4 × med-high | 10 | ready-to-draft (deeper rung on e-028) |
| L2 | **debounce-concurrent-coalescing** — M concurrent bouncers on one key across the DELAYED→ENQUEUED flip: exactly-once execution + latest-input + no silent drop, on the new `debounce_delayed_workflow` SQL protocol | #752 | scheduler-debouncer-timing | correctness 3 × high | 9 | ready-to-draft (next executor) |
| L3 | **outcome-pipeline-async-replay** — `_core.workflow_wrapper` restructured to `.wrap(get_wf_invoke).intercept(check_and_init)`; concurrent same-id async invocations + already-completed async replay (recorded result AND recorded error re-raise) across `asyncio.to_thread` threads | #763 | workflow-invoke-outcome-pipeline (new) | correctness 3 × med-high | 8 | needs producer design |
| L4 | **debounce-post-deadline-input-mutation** — once `delay_until` is capped at `debounce_deadline_epoch_ms`, later bounces still overwrite `inputs` while the delay stays pinned | #752 | scheduler-debouncer-timing | — | — | **RETIRED (observational)** — source grounding: `debounce_timeout_sec` caps only delay TIME (`delay_until = min(delay, deadline)`), no documented guarantee that inputs freeze at the deadline. "Latest input wins, fire by deadline" is the contract. Asserting input-freeze would repeat the #718 unstated-policy trap. Not a finding. |

## Standing-pool corridors (scout refresh 2026-07-09, v0.6.0 oracle plane)

Attack top-first with v0.6.0-compliant workloads (durawatch + crashclock +
liveness/terminal sweeps + async parity; anti-vacuity VOID floors; selftest).

| # | corridor | area | trigger (source-grounded) | lib | score | state |
|---|----------|------|---------------------------|-----|------:|-------|
| S1 | **stream-step-oaoo** — `write_stream` from a STEP context is not exactly-once | stream-durability-oaoo (new) / message-event | `write_stream_from_step` (`_sys_db.py:4229`) computes offset `max+1`, records NO operation_output, NO `_check_operation_execution_txn` guard (its workflow sibling HAS it, `:4265`); `streams` PK excludes `function_id` (`_schemas`). A **step retry alone** (max_attempts>1) re-inserts a duplicate value at a new offset — crash widens it. | crashclock op-index/phase-straddle + durawatch | **12** | **DONE → e-031 RED finding** (local pg: control copies=1 GREEN; step-retry-sync K=2 copies=2 RED; step-retry-async K=4 copies=4 RED; selftest forces control RED). Cloud replay pending; filing held for Viswa. |
| S2 | **datasource-oaoo-pg-restart** — datasource retry under real PG process restart (not lock) | datasource-transaction-oaoo | `crashclock.restart_dependency` drops/restarts Postgres mid-txn → connection reset; does the retry loop double-apply the app effect / double-record `datasource_outputs`? Distinct from E-023 (SQLite lock). | crashclock.restart_dependency + durawatch | ~~10~~ → **4** | **DEMOTED (grounded 2026-07-09).** `_datasource.py:301` runs the app effect `func()` AND `_record_result` inside ONE `session.begin()` — atomic. PG restart mid-txn IS retriable (`retriable_postgres_exception`: "server closed the connection", pgcode 08/53/57). On retry the effect re-applies but the re-`INSERT` into `datasource_outputs (workflow_id, step_id)` hits the PK (23505, non-retriable) → the whole retry txn (incl. re-applied effect) rolls back → **no double-apply**. Residual (narrow): an ambiguous-commit window raises a spurious `IntegrityError` to the caller, but the top-of-fn `_check_execution_with_retry` replays the recorded output on workflow recovery → self-heals. Low yield + timing-hard. Revisit only to probe whether that spurious 23505 escapes recovery as a permanent workflow ERROR. |
| S3 | **queue-dequeue-crash-slot** — kill in ENQUEUED→PENDING dequeue window vs concurrency accounting | queue-composed-controls | op-index kill after dequeue commit, before body; `worker_concurrency` from in-memory `local_running_count` (`:3873`) vs global DB PENDING count that only WARNS (`:3894`). Double-exec or stranded slot? | crashclock op-index + interleave(2 workers) | ~~8~~ → **4** | **DEMOTED (grounded 2026-07-09).** Dequeue is ONE atomic txn (`_sys_db.py:3910`): `SELECT ENQUEUED FOR UPDATE` → `UPDATE→PENDING WHERE status=ENQUEUED` → commit. The status-guarded UPDATE means two workers can't both win the same row; a kill after commit / before body is recovered exactly-once and bodies are OAOO-protected → **no double-exec**. `worker_concurrency` is a soft in-memory limit that resets on restart (transient overshoot only). The one real observable — global `_concurrency` overshoot under concurrent disjoint-row dequeue (both read `global_pending=0` before either commits) — is handled **warn-only** (`:3894`, `available_tasks=max(0,...)`), i.e. explicitly best-effort. Asserting a hard global-concurrency bound repeats the #718 unstated-policy trap. Not a finding without a doc guarantee. |
| S4 | **recv-async-cancel-storm** — interleaved async waiter cancel racing delivery | message-event-cancellation | `_run_event_setup_async` (`:3119-3170`) double-cancel branch: leftover `notifications_map` entry → next recv `DBOSWorkflowConflictIDError` + "parks caller forever". Seed-search cancel/deliver orderings. | interleave(2-3 actors) | ~~8~~ → **3** | **DEMOTED (probe-backed 2026-07-09).** White-box probe drives the exact double-cancel path (`_run_event_setup_async` uses no `self` state → stub-drivable): `workloads/recv-cancel-storm/_probe.py`. Result: the deferred done-callback **always drains** the entry (no persistent leftover, all trials). A concurrent racer CAN observe the entry mid-window (transient), so a recv racing that instant would trip `ConflictID` — BUT reachability requires TWO recvs on the same `(workflow_uuid, topic)` at once. `notifications_map` is per-process in-memory: cross-process recovery doesn't share it, and in-process single-execution prevents a concurrent same-workflow recv. Not independently reachable → defense-in-depth, not a finding. |
| S5 | **notify-loss-db-reconnect** — LISTEN/NOTIFY dropped across DB restart → 60s fallback stall | message-event-cancellation | reconnect re-issues LISTEN (`_sys_db_postgres:159`) but never re-scans maps; NOTIFY during down-window lost; waiter stalls to `_notification_fallback_polling_interval=60s`. Availability. | crashclock.restart_dependency + durawatch(latency) | 5 | parked (below threshold; only if S1-4 claimed) |

Avoid (scout): re-running stream WORKFLOW-context writes (harvested green); GC-cascade-of-events (ON DELETE CASCADE by design, policy-ambiguous); any debounce input-freeze (#718 trap).

## Below threshold / parked

| corridor | commit | why parked | score |
|----------|--------|-----------|------:|
| whitespace-idempotency-key-500 — pure-whitespace `dbos-idempotency-key` header is truthy → `SetWorkflowID("   ")` → `DBOSException` → unhandled 500 (`_fastapi.py`/`_flask.py` fixed empty but opened whitespace) | #763 | availability/wrong-error 1–2; **reachability gap** — servers/proxies often strip header whitespace before it reaches the handler; verify the ASGI/WSGI path before investing | 3 |
| gc-cross-db-orphan-leak (leak only) — orphaned `transaction_outputs` as pure storage leak, self-healing next GC cycle | #751 | subsumed by L1 (the OAOO-on-reuse consequence is the real harm); orphan-only is cosmetic | 2 |

## Explicitly avoided (already shipped in #751/#752/#763 tests — do not re-derive)

- Debouncer: deadline-cap-on-delay, cross-workflow key collision, pinned-id
  leak/dedup race, bounce/checkpoint atomicity, post-commit crash, portable-dedup
  replay, parent-deadline non-inheritance (`tests/test_debouncer.py`).
- GC: happy-path batching, mid-**sys_db**-batch resumable, rows_threshold,
  batch_size validation (`tests/test_workflow_management.py`).
- #763: `SetWorkflowID`/client-enqueue empty-id rejection, recovery empty-id
  dead-letter, datasource precheck retry-on-serialization.
- E-008 (debounce queue starvation), #718 (schedule overlap policy).

## Skips recorded

- None this session; L1 promoted, L2–L4 held as producible depth.
