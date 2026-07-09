# SQLite datasource OAOO pre-check read is not covered by the lock retry loop

Status: `ready`

Disposition: reproduced locally with a standalone SQLite-backed DBOS script (no
external services), including a passing unlocked control. Root cause pinpointed.
Ready to file upstream.

## Summary

A SQLite-backed datasource transaction records the workflow as terminal `ERROR`
when the one-and-only-once (OAOO) **pre-check read** hits `database is locked`,
even though DBOS already classifies that error as retryable and retries it
inside the transaction body. The lock can be released well within a normal retry
budget, yet the workflow never reaches even its first body attempt.

The original `E-023` workload framed this as "terminal error before the body
reaches a retry attempt". The local repro narrows it precisely: the unprotected
call is `_check_execution` (the OAOO bookkeeping `SELECT` on
`datasource_outputs`), which runs before the retry loop.

## Root Cause (target ref `3df88c4`)

- `dbos/_datasource.py`: the OAOO pre-check `self._check_execution(workflow_id,
  step_id)` runs **before** the `while True:` retry loop (sync ~line 571, async
  ~line 274).
- Inside that loop, a `database is locked` `DBAPIError` is classified retryable
  by `_is_sqlite_serialization_error` (`dbos/_datasource_sqlite.py:14`) and
  retried with backoff.
- The pre-check read is outside the loop, so a transient lock there propagates
  as a terminal datasource error and marks the workflow `ERROR`. The error SQL
  observed locally is exactly the pre-check:
  `SELECT datasource_outputs.output, datasource_outputs.error,
  datasource_outputs.serialization FROM datasource_outputs WHERE workflow_id=?
  AND step_id=?`.

## Environment

- DBOS source: `3df88c4bcc3a2b73d91b765b459f4c7beb6a690c`
- Evidence: `.workers/runs/E-023.md`
- Work item: `.workers/work-items/e-023.md`
- Workload: `.workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py`
- Backend under failing case: SQLite datasource
- Upstream related PR: `dbos-inc/dbos-transact-py#680`, merged

## Reproduction Story

Standalone local repro (no external services; SQLite system DB + SQLite
datasource file):

```bash
.workers/vendor/dbos-venv/bin/python \
  .workers/issues/repros/e023_sqlite_locked_precheck.py
```

The script runs the same datasource workflow twice: once with no contention
(positive control) and once while a separate connection holds a
`BEGIN EXCLUSIVE` lock during the OAOO pre-check, releasing it after ~1s (well
within any retry budget).

Observed output (target ref `3df88c4`, DBOS `2.24.0-12-g3df88c4`,
CPython 3.14.6):

```text
CTRL  status=SUCCESS  body_attempts=1  result={...'attempt': 1}  error=None
LOCK  status=ERROR    body_attempts=0  result=None  error=OperationalError: (sqlite3.OperationalError) database is locked
[SQL: SELECT datasource_outputs.output, datasource_outputs.error, datasource_outputs.serialization
FROM datasource_outputs
WHERE datasource_outputs.workflow_id = ? AND datasource_outputs.step_id = ?]

control succeeded (workflow works unlocked)     : True
locked case became terminal ERROR              : True
terminal error is a SQLite locked error        : True
body never reached first attempt (pre-check)   : True
```

Original cloud workload command (matrix evidence):

```bash
.workers/run-with-postgres.sh .workers/python-runtime.sh \
  .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py \
  --rung rung-006-datasource-dbapi-retry-liveness \
  --case case-004 \
  --seed 6803
```

## Expected Behavior

SQLite locked-database/table errors that DBOS classifies as retryable datasource
concurrency failures should be transient. DBOS should retry the datasource body
after the lock is released, record one successful `datasource_outputs` row, and
preserve replay without duplicate app side effects.

## Actual Behavior

The focused WIO run reported:

- invariant failure:
  `sqlite_locked_retry_reaches_second_attempt_before_lock_release`;
- workflow `wio-ds-6803-case-004` reached status `ERROR`;
- DBOS logged `sqlite3.OperationalError: database is locked` from
  `_datasource.py` while selecting from `main.datasource_outputs`;
- the modeled datasource body attempt ledger stayed empty.

## Impact

If confirmed, SQLite-backed datasource users can see a retryable lock window
become a durable workflow failure before the application code has a chance to
retry. That weakens datasource retry liveness and exactly-once replay semantics
for local/dev SQLite-backed datasource workflows.

## Evidence

- Full matrix run: `01KVYPQAVEF7THH5HQP6H5M9BW`
- Focused run: `01KVYPYX18QKZP34RAJGX2CY1X`
- Run record: `.workers/runs/E-023.md`
- Work item: `.workers/work-items/e-023.md`

## Controls And Non-Claims

- Local positive control: the same workflow with no external lock succeeds, the
  body runs once, and the datasource output is recorded. So the workflow itself
  is correct; only the locked pre-check path fails.
- Passing controls in the original cloud matrix:
  - Postgres serializable retry committed exactly once.
  - Postgres async deadlock retry committed exactly once.
  - Async non-retryable DBAPI error recorded and replayed as an error.
- The repro uses a short SQLite busy timeout (`connect_args={"timeout": 0.05}`)
  to make contention deterministic. The bug is not the timeout value: any lock
  held past the busy timeout during the pre-check becomes terminal because the
  pre-check is not retried. A longer busy timeout only widens the window before
  the same terminal outcome.
- This draft does not claim a Postgres datasource retry bug; the Postgres path
  is a passing control.
- This draft does not claim the in-body retry logic is wrong; it correctly
  retries the same locked error. The gap is the unprotected pre-check read.

## Upstream Duplicate/Fix Check

Checked on 2026-06-25:

- PR `#680` is merged and covers datasource retry work.
- No existing upstream issue was found for SQLite datasource locked retry
  terminal errors with searches around `SQLite datasource retry database
  locked`.
