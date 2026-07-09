# Issue-linked workload repros (migrated from `workersio/dbos-workload`)

Prior-generation adversarial workloads folded in during the DBOS project
consolidation (2026-07-09). Each is an issue- or finding-linked reproduction.
They are **source-only** here: running them under this repo's harness
(vendor `PYTHONPATH` + runner) is follow-up integration work. The
base-vs-`_aggressive` dedup is deferred until we have red-data from running
them under fault injection.

| driver | LOC | upstream | surface |
| --- | ---: | --- | --- |
| `dbos_async_gather_handoff.py` | 353 | #688 | Async gather + handoff (P4) |
| `dbos_async_queue_gc.py` | 257 | #710 | Queue + async dequeued workflow |
| `dbos_async_to_thread_handoff.py` | 125 | #664 | async workflow + concurrent to_thread(transaction) |
| `dbos_cancel_vs_recovery.py` | 668 |  | Cancel semantics x recovery ownership x workflow state machine |
| `dbos_cancel_vs_recovery_aggressive.py` | 666 |  | Cancel semantics x recovery ownership x workflow state machine |
| `dbos_crash_resume_ledger.py` | 324 |  | Crash mid-workflow, relaunch, recover — with ledger conservation invar |
| `dbos_debounce_storm.py` | 277 | #702 | Debouncer internal queue + dedup rows |
| `dbos_debounce_storm_aggressive.py` | 431 | #702 | Debouncer internal queue + dedup rows under flood + crash + recovery gap |
| `dbos_dual_recovery_race.py` | 589 |  | Recovery ownership + queue handoff |
| `dbos_dual_recovery_race_aggressive.py` | 601 |  | Recovery ownership + queue handoff |
| `dbos_event_delivery.py` | 450 | #562 | Send / recv / ack (Family 6) |
| `dbos_event_delivery_aggressive.py` | 475 | #562 | Send / recv / ack (Family 6) |
| `dbos_event_delivery_get_event_aggressive.py` | 381 | #562 | set_event / get_event ack under multi-enqueue + crash + recovery |
| `dbos_external_idempotency.py` | 255 |  | Step retries, external side effects vs durable step completion |
| `dbos_migration_interrupt.py` | 241 |  | DBOS system migrations + launch |
| `dbos_multi_worker_queue.py` | 386 | #546 | Multi-worker queue (P2 proper) |
| `dbos_patch_async_gather.py` | 185 | #714 | patch_async + asyncio.gather |
| `dbos_pg_conflict_retry.py` | 148 | #679 | concurrent queue workers on same ledger op |
| `dbos_pg_restart_recovery.py` | 543 | #679 | Sys DB reconnect + recovery scanner + queue manager under DB outage |
| `dbos_queue_dequeue_crash.py` | 397 | #546 | Queue dequeue crash window |
| `dbos_queue_dequeue_crash_aggressive.py` | 411 | #546 | Queue dequeue crash window |
| `dbos_queue_saturation.py` | 337 | #546 | Queue fairness, worker concurrency, backpressure |
| `dbos_queue_saturation_aggressive.py` | 337 | #546 | Queue fairness, worker concurrency, backpressure |
| `dbos_schedule_dedup.py` | 147 | #718 | parent tick workflow enqueues deduplicated child on a queue |
| `dbos_startup_storm.py` | 272 |  | dependency import, DBOS import/startup, migration, queue listener initialization |
| `dbos_stream_fork.py` | 428 | #577 | Stream / fork writers (Family 7) |
| `dbos_stream_fork_aggressive.py` | 428 | #577 | Stream / fork writers (Family 7) |
| `dbos_tx_step_boundary.py` | 363 |  | Workflow transaction rules, async steps, DB session lifecycle |
| `dbos_version_dedup_skew.py` | 203 | #702 | debounce_async + application_version redeploy |

**Support files** (workflow defs + shared helpers, imported by the drivers): `dbos_async_gather_wf.py`, `dbos_async_queue_gc_wf.py`, `dbos_async_to_thread_wf.py`, `dbos_debounce_storm_wf.py`, `dbos_event_delivery_wf.py`, `dbos_external_idempotency_wf.py`, `dbos_ledger_wf.py`, `dbos_migration_interrupt_wf.py`, `dbos_patch_async_wf.py`, `dbos_pg_conflict_wf.py`, `dbos_queue_dequeue_wf.py`, `dbos_queue_saturation_wf.py`, `dbos_queue_wf.py`, `dbos_schedule_dedup_wf.py`, `dbos_stream_fork_wf.py`, `dbos_tx_step_boundary_wf.py`, `dbos_version_dedup_wf.py`, `dbos_workload_common.py`.
