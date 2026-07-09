---
key: kafka-events-process-exactly-once
area: kafka
title: Kafka events process exactly once
claim: >-
  Every Kafka message triggers its consumer workflow exactly once from the
  application's view: duplicate deliveries are idempotent, offsets are
  committed only after the triggering workflow is durably enqueued, and no
  acknowledged event is lost across rebalance, broker restart, or DBOS
  relaunch.
status: active
provenance: https://docs.dbos.dev/python/tutorials/kafka-integration (exactly-once event processing); dbos-inc/dbos-transact-py#733 fixed by PR #738 (offsets stored only after durable enqueue)
explorations:
  - key: kafka-duplicate-delivery-idempotent
    title: Duplicate deliveries are idempotent
    description: >-
      The same Kafka message delivered multiple times must trigger exactly
      one durable workflow execution, keyed by message identity; ledgers of
      accepted events and workflow statuses must show no duplicates.
    status: done
    result: null
    reason: null
    workload: workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-001-duplicate-key-idempotency --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd7dwrnn2gdns9wrbxpb4r8trx8a6tpf
  - key: kafka-offset-survives-relaunch
    title: Produced offset survives immediate relaunch
    description: >-
      A produced offset must remain in the acceptance, offset, and
      workflow-status ledgers when DBOS is relaunched immediately after
      consumption. Regression guard for the offset-loss finding fixed
      upstream by PR #738 (offsets stored only after durable enqueue).
    status: done
    result: null
    reason: null
    workload: workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-005-finding-minimization --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd74zcqnemctzcbes96qrd1yg18a7gvm
---

# Kafka events process exactly once

Evidence lineage: `areas/kafka-exactly-once-consumer.md` rungs 000–005.
Promoted finding: produced offset lost after immediate DBOS relaunch —
reported upstream as dbos-inc/dbos-transact-py#733, fixed by PR #738,
included in pinned target 3df88c4b. `kafka-offset-survives-relaunch` is the
standing regression guard.
