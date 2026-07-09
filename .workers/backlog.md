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
| L4 | **debounce-post-deadline-input-mutation** — once `delay_until` is capped at `debounce_deadline_epoch_ms`, later bounces still overwrite `inputs` while the delay stays pinned; input can change after the timeout the deadline promised to enforce | #752 | scheduler-debouncer-timing | correctness 2–3 × med | 6 | ready-to-draft |

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
