---
key: streams-record-each-write-once
area: stream-durability-oaoo
title: A stream records each logical write exactly once
claim: >-
  DBOS.write_stream is a durable exactly-once stream primitive. A single logical
  write, re-executed by the framework (a step retry or a crash replay), must
  appear in the stream exactly once — the same guarantee whether the value is
  written from a workflow context or a step context, since the public API
  (DBOS.write_stream / write_stream_async) draws no distinction. A consumer
  reading the stream must never see one logical write delivered more than once.
status: active
provenance: https://docs.dbos.dev/python/reference/methods#write_stream (write_stream_from_workflow guards re-execution via _check_operation_execution_txn; write_stream_from_step, dbos/_sys_db.py:4229, does not; streams PK excludes function_id)
explorations:
  - key: stream-workflow-context-exactly-once
    title: A workflow-context write appears exactly once
    description: >-
      Control / differential baseline: a value written to a stream from a
      WORKFLOW context appears exactly once. This establishes the product's own
      exactly-once contract for write_stream (the recorded-operation guard) and
      proves the oracle reads streams correctly. GREEN.
    status: done
    result: green
    reason: >-
      Confirmed (local pg, commit pending-cloud): workflow-context write yields
      exactly one stream copy (copies=1), workflow reaches SUCCESS, durawatch
      ladder stable. ORACLE_SELFTEST plants a second physical copy and the
      exactly-once oracle catches it (copies=2 -> RED), proving the green is
      non-vacuous.
    workload: workloads/stream-step-oaoo/stream_step_oaoo_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/stream-step-oaoo/stream_step_oaoo_workload.py --rung rung-001-stream-step-oaoo --case case-001
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: "case-001 control: copies=1 PASS; ORACLE_SELFTEST=1 forces RED. Evidence: runs/E-031.md"
    freshness: new-current
    reported: null
    published: pending
  - key: stream-step-retry-duplicate
    title: A retrying step duplicates the stream value
    description: >-
      The finding. A @DBOS.step(max_attempts>1) that calls DBOS.write_stream
      then fails is retried; write_stream_from_step has no recorded-operation
      guard, so each attempt re-inserts the same value at a new offset. The
      single logical write must appear once; RED if it appears K times. Same
      public API as the workflow-context control, which appears once.
    status: done
    result: red
    reason: >-
      Confirmed (local pg, commit pending-cloud): step-context write under a
      2-attempt retry produced TWO copies of the single logical value
      (copies=2, expected 1), workflow still SUCCESS (silent duplication).
      durawatch flags the acked stream content as a persistent mutation. The
      workflow-context control appears exactly once — differential OAOO /
      exactly-once violation. Finding candidate; upstream filing held for human
      triage. Sibling class to e-028 (recorded-operation guard omitted on one
      of two sibling code paths).
    workload: workloads/stream-step-oaoo/stream_step_oaoo_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/stream-step-oaoo/stream_step_oaoo_workload.py --rung rung-001-stream-step-oaoo --case case-002
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: "case-002 step-retry-sync: K=2, copies=2 FAIL (stream_exactly_once_case-002). Evidence: runs/E-031.md"
    freshness: new-current
    reported: null
    published: pending
  - key: stream-step-async-retry-duplicate
    title: The async write path duplicates identically
    description: >-
      Async-parity rung: DBOS.write_stream_async from a retrying async
      @DBOS.step. write_stream_async routes through the same write_stream core
      (dbos/_dbos.py:3239, via asyncio.to_thread) so it hits the same unguarded
      step path. The single logical async write must appear once; RED if it
      appears K times. Proves the gap is not sync-only.
    status: done
    result: red
    reason: >-
      Confirmed (local pg, commit pending-cloud): async step-context write under
      a 4-attempt retry (crashclock op_index K=4) produced FOUR copies of the
      single logical value (copies=4, expected 1); workflow SUCCESS. Same OAOO
      violation on the async path. Filing held for human triage.
    workload: workloads/stream-step-oaoo/stream_step_oaoo_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/stream-step-oaoo/stream_step_oaoo_workload.py --rung rung-001-stream-step-oaoo --case case-003
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: "case-003 step-retry-async: K=4, copies=4 FAIL (stream_exactly_once_case-003). Evidence: runs/E-031.md"
    freshness: new-current
    reported: null
    published: pending
---

# A stream records each logical write exactly once

Evidence lineage: `areas/stream-durability-oaoo.md`, work item `e-031`.

`DBOS.write_stream` is a durable, ordered per-workflow channel consumed by
`DBOS.read_stream` (yields each committed value in offset order). The framework
implements exactly-once for it on the **workflow** path — `write_stream_from_workflow`
records an `operation_output` and guards re-execution with
`_check_operation_execution_txn`. The **step** path (`write_stream_from_step`,
`dbos/_sys_db.py:4229`) omits that guard entirely: it inserts at
`max(offset)+1` and retries only on an offset `IntegrityError`. Because the
`streams` primary key is `(workflow_uuid, key, offset)` and excludes
`function_id`, and because a `@DBOS.step(max_attempts>1)` re-runs its body under
the same `function_id` on each retry, a step that writes a stream value then
fails re-inserts a duplicate on every attempt. The workflow-context control
proves the exactly-once contract the product itself upholds on the other path;
the step-retry and async-step-retry rungs are the adversarial and async-boundary
attacks.
