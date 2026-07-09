---
key: transactions-execute-once
area: transactions
title: Transactions execute once and only once
claim: >-
  A committed transaction's side effects happen exactly once even when its
  workflow is re-executed; retriable database errors are retried
  transparently; and a send inside a transaction is atomic with the commit —
  invisible before, exactly-once delivered after.
status: active
provenance: https://docs.dbos.dev/python/tutorials/transaction-tutorial (OAOO execution; checkpointed outputs are replayed, not re-run)
explorations:
  - key: transaction-replay-once
    title: Replay never re-runs committed work
    description: >-
      Workflows with checkpointed transactions are forced through replay;
      application tables must show each transaction's effect exactly once,
      with replayed executions serving recorded outputs instead of
      re-executing.
    status: done
    result: null
    reason: null
    workload: workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-001-transaction-replay-once --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd78pkp8pj2bnbvxnc3s4jnw0n8a7ymw
  - key: transactional-send-atomic-visibility
    title: Transactional send is atomic with commit
    description: >-
      Messages sent inside a transaction must be invisible to receivers
      before commit, delivered exactly once after commit, and disappear
      cleanly on rollback — including under duplicate-key conservation and
      enqueue+send composition.
    status: done
    result: null
    reason: null
    workload: workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung rung-005-transactional-send-visibility --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd77f4db7w7k579gdqs06q3hwn8a607p
---

# Transactions execute once and only once

Evidence lineage: `areas/datasource-transaction-oaoo.md` rungs 000–006;
curated history E-007 (transactional-send visibility green) and E-023
(SQLite datasource terminal-ERROR-before-retry candidate, held as a draft
until upstream triage).
