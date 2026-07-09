---
key: system-db-retries-stay-exactly-once
area: recovery
title: System-DB retries stay exactly once
claim: >-
  When a system-database write may have committed before its connection
  failed, the retry loop re-enters without double-recording — receive
  checkpoints, child-workflow records, and implicit get-result checkpoints
  all stay exactly-once.
status: active
provenance: https://docs.dbos.dev/explanations/how-workflows-work (system database checkpointing; retry loops preserve exactly-once durable semantics)
explorations:
  - key: committed-retry-reentry
    title: Committed writes survive retry re-entry
    description: >-
      Connection failures injected after commit on receive, child-record,
      and implicit get-result paths; operation ledgers, child edges, and
      function ids must show each effect exactly once after retry.
    status: done
    result: null
    reason: null
    workload: workloads/system-db-retry-idempotence/system_db_retry_idempotence_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/system-db-retry-idempotence/system_db_retry_idempotence_workload.py --rung rung-001-committed-sysdb-retry-reentry --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd79zkznv9gy1d6t4t1q49crdn8a6fdf
---

# System-DB retries stay exactly once

Evidence lineage: `areas/system-db-retry-idempotence.md` rung 001, proven
green on the pinned target 3df88c4b (matrix run plus focused replays):
receive two-message, receive timeout, child edge conflict, and implicit
get-result function-id cases all passed.
