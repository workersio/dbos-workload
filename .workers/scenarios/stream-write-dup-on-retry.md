---
key: stream-write-dup-on-retry
rung: L0
cast: {stream-user: 1}
flows: [stream-write]
depth: 15
status: ready
result: null
replay: null
redproof: null
invariants: [stream-write-once]
story: >-
  A job streams one progress update to a watcher, but the step that writes it
  fails once and retries — the watcher should still see the update exactly once,
  not twice.
---
Real-RED hunt (e7 row-4 refresh, candidate 48). DBOS.write_stream called from a
@DBOS.step uses write_stream_from_step (_sys_db.py:4229), which commits each write
in its own transaction with NO OAOO record — unlike write_stream_from_workflow
(_sys_db.py:4265, @db_retry + _check_operation_execution_txn). The driver's step
writes value "v" to the stream, then raises on its first attempt and succeeds on
retry (max_attempts=3). If the from_step path is not exactly-once, the failed
attempt's write persists AND the retry writes again -> the stream contains "v"
twice -> stream-write-once RED (acked count 1, observed 2). Single actor (inline,
no interleave), so a red is a clean DBOS finding, not a harness artifact.
