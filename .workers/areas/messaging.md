---
key: messaging
title: Messaging
description: Workflow messages, events, and streams deliver exactly what was sent — no duplicates, no lost notifications, no stale waiters — across timeouts, cancellation, forks, and recovery.
order: 70
---

# Messaging

What this area covers: DBOS's workflow communication primitives —
`DBOS.send`/`DBOS.recv`, `set_event`/`get_event`, and streams. The docs
promise exactly-once observable delivery: a receiver sees each message once,
event readers see the latest durable value, stream consumers resume at the
right offset, and cancellation or recovery never leaks stale waiters or
duplicates notifications.

Boundaries:
- In scope: duplicate/timeout/cancel semantics, listener fallback, fork
  delivery, recovery replay, live stream resume offsets.
- Out of scope until a promise names them: Kafka ingestion (see Kafka),
  client-side prompt polling (rung 006, still queued in the legacy corpus).

Evidence lineage: legacy hunt corpus in `areas/message-event-cancellation.md`
(rungs 001–005, all green through the pinned target).
