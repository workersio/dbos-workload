---
key: kafka
title: Kafka integration
description: Kafka-triggered workflows process each event durably and exactly once — across duplicate deliveries, rebalances, broker restarts, and DBOS relaunch.
order: 30
---

# Kafka integration

What this area covers: the `@DBOS.kafka_consumer` contract in DBOS Transact
Python — each Kafka message triggers its workflow exactly once from the
application's view, offsets are only committed once the triggering workflow
is durably enqueued, and no acknowledged event is lost when the consumer,
broker, or DBOS process restarts.

Boundaries:
- In scope: duplicate-delivery idempotency, rebalance offset replay, broker
  restart, immediate-relaunch offset durability.
- Runs require the vendored standalone Kafka broker (built by
  `.workers/build.sh`) and Postgres.

Evidence lineage: legacy corpus in `areas/kafka-exactly-once-consumer.md`
(rungs 000–005). Promoted finding with upstream fix: produced offset lost
after immediate DBOS relaunch — reported as dbos-inc/dbos-transact-py#733,
fixed by PR #738 (offsets stored only after durable enqueue), which is
included in the current pinned target 3df88c4b. Rung-005 is the regression
guard for that fix.
