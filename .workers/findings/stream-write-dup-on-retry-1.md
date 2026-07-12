---
key: stream-write-dup-on-retry-1
scenario: stream-write-dup-on-retry
severity: correctness
minimized: {cast: {stream-user: 1}, flows: [stream-write], depth: 1, seed: 1}
replay: {run: 01KXAHB5E71FVTQKJMSMDH2QRH, seed: 1}
status: held
story: >-
  A job streams one progress update from a step that fails once and retries; the
  watcher receives the update twice instead of once.
---
## What breaks

`DBOS.write_stream(key, value)` called from inside a `@DBOS.step` is **not
exactly-once across a step retry**. When the step body writes to the stream and
then raises (a normal, retryable failure), the failed attempt's write is already
committed; DBOS retries the step, the body re-runs, and it writes the value
again. A `read_stream` consumer then observes the value **twice**.

This contradicts the streaming contract — `read_stream` "yields each value in
order until the stream is closed or the workflow terminates" (`_dbos.py:3185-3202`)
and the vendor's own tests assert values are delivered in write order, once
(`tests/test_streaming.py:33-40`) — and it is inconsistent with the *workflow*
write path, which IS deduplicated.

## Evidence

Run `01KXAHB5E71FVTQKJMSMDH2QRH` (seed 1), workflow `stream-...-24858730`:
- `status: SUCCESS`, step `attempts: 2` (failed once, succeeded on retry)
- stream contents `vals: ["v", "v"]`, `count: 2`, `total: 2`
- `INVARIANT ledger_stream-user-1 persona-ledger FAIL 1 stream-count/... acked=1 observed=2`

The workflow is a single step that does `DBOS.write_stream("s", "v")` then raises
on its first attempt (`@DBOS.step(retries_allowed=True, max_attempts=3)`), then
`DBOS.close_stream("s")`. Expected stream = `["v"]`; actual = `["v", "v"]`.

## Suspected seam

`SystemDatabase.write_stream_from_step` (`dbos/_sys_db.py:4229`) inserts the value
at the next unused offset in its **own** transaction, retrying only on an offset
`IntegrityError` — there is **no OAOO / operation-execution record**. Contrast
`write_stream_from_workflow` (`dbos/_sys_db.py:4265`), which is wrapped in
`@db_retry()` and calls `_check_operation_execution_txn` so a replay of the same
`(workflow, function_id)` returns the recorded result instead of re-inserting.
Dispatch to the two paths is in `_core.py:2188-2210` (`is_workflow()` →
from_workflow OAOO; `is_step()` → from_step, no OAOO). So any stream write issued
from step code duplicates whenever the step retries.

## Impact

Correctness (weight 3): a stream reader silently receives duplicated values on a
transient step failure — no error, workflow reports SUCCESS. Streaming progress
from within `@DBOS.step` code is a natural pattern (steps are where the work and
its progress live), so this is reachable in ordinary use.

## Repro (replayable)

```
wio workloads rerun 01KXAHB5E71FVTQKJMSMDH2QRH
# or, minimal shape:
<runner> .workers/lib/run_scenario.py .workers/scenarios/stream-write-dup-on-retry.md --seed 1
# RED: stream "s" contains "v" twice (count=2) though only one logical write occurred.
```

status: held — a plain user-report dossier + standalone repro is the human's call
to send (report skill gates it). No product vocabulary upstream.
