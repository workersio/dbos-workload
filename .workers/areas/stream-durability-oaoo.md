---
key: stream-durability-oaoo
title: Stream durability & exactly-once
description: "DBOS.write_stream is a durable exactly-once stream primitive: the same logical write, re-executed by the framework via step retry or crash replay, appears in the stream exactly once regardless of whether it was issued from a workflow or a step context."
order: 130
---

# Area: stream-durability-oaoo

## Current State

New area from a first-principles read of the workflow-stream primitive
(`DBOS.write_stream` / `write_stream_async`, `DBOS.read_stream`). Streams are a
durable, ordered per-workflow channel: `read_stream` yields each committed value
in offset order until the stream closes.

Evidence:
- `dbos/_core.py:2170 write_stream` routes by caller context:
  - **workflow context** → `_sys_db.write_stream_from_workflow` — records an
    `operation_output` and guards re-execution with
    `_check_operation_execution_txn("DBOS.writeStream")`. Exactly-once across
    replay.
  - **step context** → `_sys_db.write_stream_from_step` (`dbos/_sys_db.py:4229`)
    — computes `offset = coalesce(max(offset), -1) + 1` and inserts, with **no**
    recorded operation and **no** execution guard. Retries on `IntegrityError`
    only (offset race), never dedupes a re-execution.
- `dbos/_schemas/system_database.py` `streams` — `PrimaryKeyConstraint(
  workflow_uuid, key, offset)`. `function_id` is a column but **not** part of the
  key, so two writes from the same `function_id` at different offsets coexist.
- `dbos/_core.py` step retry loop — a `@DBOS.step(max_attempts=N)` re-invokes its
  body under the **same** `ctx.function_id` on each attempt; the step's own
  `operation_result` is recorded only on terminal success/failure.

## Corridor

`stream-step-oaoo` — a `@DBOS.step` that calls `DBOS.write_stream` then fails and
is retried re-inserts a duplicate stream value on every attempt (no guard),
while the identical single write from a workflow context is deduped to one copy.
Same public API, no documented distinction → a consumer of `read_stream` sees the
value K times. Work item `e-031`.
