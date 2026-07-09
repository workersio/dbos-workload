"""Standalone local repro for E-023.

Claim under test: a SQLite-backed datasource transaction records the workflow as
terminal ERROR when the OAOO pre-check read (`_check_execution`) hits
`database is locked`, instead of retrying the transient lock the way the
transaction body already does. The lock is released well within a normal retry
budget, yet the workflow never reaches even its first body attempt.

Root cause (target ref 3df88c4):
  dbos/_datasource.py  the OAOO pre-check `self._check_execution(...)`
  (sync line ~571, async line ~274) runs BEFORE the `while True:` retry loop.
  Inside that loop a `database is locked` DBAPIError is classified retryable by
  `_is_sqlite_serialization_error` (dbos/_datasource_sqlite.py:14) and retried
  with backoff. The pre-check read is NOT inside the loop, so a transient lock
  there propagates as a terminal datasource error.

No external services required: SQLite system DB + a SQLite datasource file.

Run:
    .workers/vendor/dbos-venv/bin/python \
        .workers/issues/repros/e023_sqlite_locked_precheck.py

Exit 0 => locked pre-check was retried / did not become terminal (no repro).
Exit 1 => locked pre-check became a terminal ERROR (E-023 reproduces).
"""

import os
import tempfile
import threading
import time

import sqlalchemy as sa

from dbos import DBOS, DBOSConfig, SQLAlchemyDatasource, SetWorkflowID

LEDGER_TABLE = "e023_ledger"

# Module-level body-attempt counter, keyed by intent id. The datasource body
# increments it on every real attempt; the OAOO pre-check does not.
STEP_CALLS: dict[str, int] = {}

tmpdir = tempfile.mkdtemp(prefix="e023-")
ds_path = os.path.join(tmpdir, "e023_ds.sqlite")
ds_url = f"sqlite:///{ds_path}"

# A short busy timeout makes the lock contention deterministic (matches the
# workload's connect_args timeout). Without it SQLite would block up to 5s.
ds = SQLAlchemyDatasource.create(
    ds_url, engine_kwargs={"connect_args": {"timeout": 0.05}}
)


def init_app_tables() -> None:
    with ds.engine.begin() as conn:
        conn.execute(
            sa.text(
                f"""
                CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    intent_id TEXT NOT NULL,
                    workflow_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    attempt INT NOT NULL
                )
                """
            )
        )


def note_step_call(intent_id: str) -> int:
    STEP_CALLS[intent_id] = STEP_CALLS.get(intent_id, 0) + 1
    return STEP_CALLS[intent_id]


@DBOS.workflow()
def locked_retry_workflow(intent_id: str, payload: str) -> dict:
    def locked_step() -> dict:
        attempt = note_step_call(intent_id)
        session = ds.sql_session()
        session.execute(
            sa.text(
                f"INSERT INTO {LEDGER_TABLE} (intent_id, workflow_id, payload, attempt) "
                f"VALUES (:i, :w, :p, :a)"
            ),
            {"i": intent_id, "w": DBOS.workflow_id, "p": payload, "a": attempt},
        )
        return {"intent_id": intent_id, "payload": payload, "attempt": attempt}

    return ds.run_tx_step(
        {"name": "e023_locked_retry", "isolation_level": "SERIALIZABLE"},
        locked_step,
    )


def hold_exclusive_lock_briefly(hold_seconds: float) -> "sa.Connection":
    """Open a separate connection and take an EXCLUSIVE lock (blocks readers)."""
    lock_engine = sa.create_engine(ds_url, connect_args={"timeout": 0.05})
    conn = lock_engine.connect()
    conn.exec_driver_sql("BEGIN EXCLUSIVE")
    return conn


def run_case(intent_id: str, workflow_id: str, with_lock: bool) -> dict:
    payload = f"payload-{intent_id}"
    lock_conn = None
    if with_lock:
        lock_conn = hold_exclusive_lock_briefly(hold_seconds=1.0)

    with SetWorkflowID(workflow_id):
        handle = DBOS.start_workflow(locked_retry_workflow, intent_id, payload)

    # Give the datasource time to hit the pre-check under the held lock, then
    # release the lock well within any reasonable retry budget.
    if with_lock:
        time.sleep(1.0)
        assert lock_conn is not None
        lock_conn.rollback()
        lock_conn.close()

    error = None
    result = None
    try:
        result = handle.get_result()
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"

    status = DBOS.retrieve_workflow(workflow_id).get_status()
    return {
        "intent_id": intent_id,
        "with_lock": with_lock,
        "status": status.status,
        "body_attempts": STEP_CALLS.get(intent_id, 0),
        "result": result,
        "error": error,
    }


def main() -> int:
    sysdb = os.path.join(tmpdir, "e023_sys.sqlite")
    config: DBOSConfig = {
        "name": "e023repro",
        "system_database_url": f"sqlite:///{sysdb}",
    }
    init_app_tables()
    DBOS(config=config)
    DBOS.launch()
    try:
        # Positive control: no external lock -> body runs, workflow succeeds.
        control = run_case("ctl", "e023-control", with_lock=False)
        # Bug case: external EXCLUSIVE lock held during the pre-check, released
        # after 1s (well within retry budget).
        locked = run_case("lock", "e023-locked", with_lock=True)

        for row in (control, locked):
            print(
                f"{'LOCK' if row['with_lock'] else 'CTRL'}  "
                f"status={row['status']:<8} body_attempts={row['body_attempts']}  "
                f"result={row['result']}  error={row['error']}"
            )
        print()

        control_ok = control["status"] == "SUCCESS" and control["body_attempts"] >= 1
        # The signature: terminal ERROR with a locked-database error, and the
        # body never ran (pre-check failed before the first attempt).
        locked_terminal = locked["status"] == "ERROR"
        locked_msg = (locked["error"] or "").lower()
        is_locked_error = "database is locked" in locked_msg or "locked" in locked_msg
        body_never_ran = locked["body_attempts"] == 0

        print(f"control succeeded (workflow works unlocked)     : {control_ok}")
        print(f"locked case became terminal ERROR              : {locked_terminal}")
        print(f"terminal error is a SQLite locked error        : {is_locked_error}")
        print(f"body never reached first attempt (pre-check)   : {body_never_ran}")
        print()

        if control_ok and locked_terminal and is_locked_error and body_never_ran:
            print("E-023 REPRODUCES: a transient SQLite lock during the OAOO "
                  "pre-check became a terminal workflow ERROR before the "
                  "datasource body could retry.")
            return 1
        if control_ok and locked_terminal:
            print("Locked case failed terminally but not with the exact "
                  "pre-check signature; inspect the error above.")
            return 1
        print("E-023 did NOT reproduce: locked pre-check did not become terminal.")
        return 0
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    raise SystemExit(main())
