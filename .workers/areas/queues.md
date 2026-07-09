---
key: queues
title: Queues
description: Queue-managed workflows respect concurrency caps, rate limits, priority, dedupe, and partition isolation — under load, config changes, and crashes.
order: 10
---

# Queues

What this area covers: the contract of `Queue(...)` controls in DBOS Transact
Python — `worker_concurrency`, `concurrency`, rate `limiter`, `priority_enabled`,
deduplication, and queue partitioning. The docs promise these are enforced
bounds, not hints: a queue configured with a cap never admits more concurrent
or more frequent workflow executions than configured, including across
executor relaunch and recovery.

Boundaries:
- In scope: composed controls (several limits at once), live config changes,
  partition isolation, result durability across relaunch.
- Out of scope until a promise names them: cross-version queue migration,
  multi-executor fairness.

Evidence lineage: the legacy hunt corpus lives in
`areas/queue-composed-controls.md` (rungs 001–008) with curated run history in
`work-items/`. Promoted finding: partition-level `worker_concurrency`
over-admission under async partitioned workers (E-006, target a4237179).
