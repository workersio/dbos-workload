# [recovery] Stale get_pending_workflows result can execute queued work after ownership is cleared

Filed upstream: https://github.com/dbos-inc/dbos-transact-py/issues/742

When two recovery attempts observe the same queued workflow as `PENDING` on a
dead executor, the first recovery attempt can correctly clear the queue
assignment and return the row to normal queue ownership. A second recovery
attempt can then reuse the earlier `get_pending_workflows(...)` result, see that
`clear_queue_assignment(...)` no longer updates the row, and still fall through
to executing the workflow body.

In other words: a recovery attempt that did not successfully claim or clear the
current queued row can still run queue-owned work.

## Environment observed

- DBOS source: checkout at `3df88c4bcc3a2b73d91b765b459f4c7beb6a690c`
- Runtime observed: DBOS `2.24.0-12-g3df88c4`, CPython `3.14.6`
- Backend: Postgres-backed DBOS queue

## Minimal repro

Run the standalone script in the details section:

```bash
python3 repro_stale_queued_recovery.py
```

The repro drives this ordering:

1. A queued workflow row starts as `ENQUEUED`.
2. The row is moved to `PENDING` under a dead executor.
3. Recovery attempt B uses the pending row and successfully calls
   `clear_queue_assignment(workflow_id)`, returning the row to queue ownership.
4. Recovery attempt C reuses an earlier `get_pending_workflows(...)` result that
   still says the workflow is `PENDING` on the dead executor.
5. Recovery attempt C calls `clear_queue_assignment(workflow_id)`, gets `false`
   because the durable row has already changed, and still executes the workflow.

The standalone script prints the final row and exits nonzero when the stale
recoverer executes the workflow after `clear_queue_assignment(...)` returned
`false`:

```text
REPRODUCED: stale recovery executed queued workflow after clear_queue_assignment returned false
```

<details>
<summary><code>repro_stale_queued_recovery.py</code> - standalone local reproducer</summary>

This script starts a temporary local PostgreSQL cluster, creates one queued workflow row, forces it to `PENDING` under a dead executor, then reuses the same earlier `get_pending_workflows(...)` result for two recovery attempts. It exits nonzero when the stale recovery execution is observed.

Run with `python3 repro_*.py` from a Python environment where `dbos`, `sqlalchemy`, and `psycopg` are installed. If PostgreSQL binaries are not on `PATH`, set `PGBIN` to the directory containing `initdb`, `pg_ctl`, and `pg_isready`.

```python
#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

import sqlalchemy as sa

from dbos import DBOS
from dbos._utils import GlobalParams


PG_BIN = Path(os.environ["PGBIN"]) if "PGBIN" in os.environ else None
PGDATA = Path(os.environ.get("PGDATA", "/tmp/dbos-recovery-repro-pg"))
PGPORT = int(os.environ.get("PGPORT", "55432"))
PGPASSWORD = os.environ.get("PGPASSWORD", "dbos")
APP_DB = "dbos_recovery_repro_app"
SYS_DB = "dbos_recovery_repro_sys"
APP_VERSION = "dbos-recovery-repro-v1"
WORKFLOW_ID = "dbos-stale-queued-recovery-repro"
QUEUE = "dbos_recovery_repro_queue"
DEAD_EXECUTOR = "dead-executor"
CURRENT_ACTOR = "unset"


def pg_bin(name: str) -> str:
    if PG_BIN is not None:
        candidate = PG_BIN / name
        if candidate.exists():
            return str(candidate)
    found = shutil.which(name)
    if not found:
        raise RuntimeError(f"Could not find {name}; set PGBIN to your PostgreSQL bin directory")
    return found


def run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=True, text=True, **kwargs)


def start_postgres() -> None:
    if PGDATA.exists():
        shutil.rmtree(PGDATA)
    init_log = Path("/tmp/dbos-recovery-repro-initdb.log")
    pg_log = Path("/tmp/dbos-recovery-repro-postgres.log")
    run(
        [
            pg_bin("initdb"),
            "-D",
            str(PGDATA),
            "-A",
            "trust",
            "--encoding=UTF8",
            "--no-locale",
            "--username=postgres",
        ],
        stdout=init_log.open("w"),
        stderr=subprocess.STDOUT,
    )
    run(
        [
            pg_bin("pg_ctl"),
            "-D",
            str(PGDATA),
            "-l",
            str(pg_log),
            "-o",
            f"-k /tmp -h 127.0.0.1 -p {PGPORT}",
            "start",
        ],
        stdout=init_log.open("a"),
        stderr=subprocess.STDOUT,
    )
    for _ in range(20):
        ready = subprocess.run(
            [
                pg_bin("pg_isready"),
                "-h",
                "127.0.0.1",
                "-p",
                str(PGPORT),
                "-U",
                "postgres",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if ready.returncode == 0:
            return
        time.sleep(0.25)
    raise RuntimeError("Postgres did not become ready")


def stop_postgres() -> None:
    subprocess.run(
        [pg_bin("pg_ctl"), "-D", str(PGDATA), "-m", "fast", "stop"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def admin_url(database: str = "postgres") -> str:
    return f"postgresql+psycopg://postgres:{PGPASSWORD}@127.0.0.1:{PGPORT}/{database}"


def app_url() -> str:
    return f"postgresql+psycopg://postgres:{PGPASSWORD}@127.0.0.1:{PGPORT}/{APP_DB}"


def sys_url() -> str:
    return f"postgresql+psycopg://postgres:{PGPASSWORD}@127.0.0.1:{PGPORT}/{SYS_DB}"


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def create_databases() -> None:
    engine = sa.create_engine(admin_url(), connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as raw:
            conn = raw.execution_options(isolation_level="AUTOCOMMIT")
            for db in (APP_DB, SYS_DB):
                conn.execute(sa.text(f"DROP DATABASE IF EXISTS {quote_ident(db)} WITH (FORCE)"))
                conn.execute(sa.text(f"CREATE DATABASE {quote_ident(db)}"))
    finally:
        engine.dispose()


def create_effects_table() -> None:
    engine = sa.create_engine(app_url())
    try:
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    """
                    CREATE TABLE recovery_effects (
                        id BIGSERIAL PRIMARY KEY,
                        workflow_id TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        created_at DOUBLE PRECISION NOT NULL
                    )
                    """
                )
            )
    finally:
        engine.dispose()


@DBOS.workflow()
def queued_workflow() -> str:
    engine = sa.create_engine(app_url())
    try:
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    """
                    INSERT INTO recovery_effects (workflow_id, actor, created_at)
                    VALUES (:workflow_id, :actor, :created_at)
                    """
                ),
                {
                    "workflow_id": DBOS.workflow_id,
                    "actor": CURRENT_ACTOR,
                    "created_at": time.time(),
                },
            )
    finally:
        engine.dispose()
    return CURRENT_ACTOR


def enqueue_workflow(sys_engine: sa.Engine) -> None:
    with sys_engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                SELECT dbos.enqueue_workflow(
                    workflow_name => 'queued_workflow',
                    queue_name => :queue_name,
                    workflow_id => :workflow_id,
                    app_version => :app_version
                )
                """
            ),
            {
                "queue_name": QUEUE,
                "workflow_id": WORKFLOW_ID,
                "app_version": APP_VERSION,
            },
        )


def force_pending(sys_engine: sa.Engine) -> None:
    with sys_engine.begin() as conn:
        result = conn.execute(
            sa.text(
                """
                UPDATE dbos.workflow_status
                SET status = 'PENDING', executor_id = :executor_id
                WHERE workflow_uuid = :workflow_id
                """
            ),
            {"executor_id": DEAD_EXECUTOR, "workflow_id": WORKFLOW_ID},
        )
        if result.rowcount != 1:
            raise RuntimeError(f"Expected to force one row pending, updated {result.rowcount}")


def row_status(sys_engine: sa.Engine) -> dict[str, object]:
    with sys_engine.connect() as conn:
        row = conn.execute(
            sa.text(
                """
                SELECT workflow_uuid, status, executor_id, queue_name, output, error
                FROM dbos.workflow_status
                WHERE workflow_uuid = :workflow_id
                """
            ),
            {"workflow_id": WORKFLOW_ID},
        ).fetchone()
    if row is None:
        raise RuntimeError("Missing workflow row")
    return dict(row._mapping)


def effects() -> list[dict[str, object]]:
    engine = sa.create_engine(app_url())
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                sa.text(
                    """
                    SELECT workflow_id, actor
                    FROM recovery_effects
                    ORDER BY id
                    """
                )
            ).fetchall()
            return [dict(row._mapping) for row in rows]
    finally:
        engine.dispose()


def recover_from_stale_result(dbos: DBOS, snapshot: list[object], actor: str) -> tuple[list[str], list[dict[str, object]]]:
    global CURRENT_ACTOR
    CURRENT_ACTOR = actor
    original_get_pending = dbos._sys_db.get_pending_workflows
    original_clear = dbos._sys_db.clear_queue_assignment
    clear_attempts: list[dict[str, object]] = []

    def stale_get_pending(executor_id: str, app_version: str) -> list[object]:
        return list(snapshot)

    def recording_clear(workflow_id: str) -> bool:
        cleared = original_clear(workflow_id)
        clear_attempts.append({"workflow_id": workflow_id, "cleared": cleared})
        return cleared

    dbos._sys_db.get_pending_workflows = stale_get_pending  # type: ignore[method-assign]
    dbos._sys_db.clear_queue_assignment = recording_clear  # type: ignore[method-assign]
    try:
        handles = DBOS._recover_pending_workflows([DEAD_EXECUTOR])
        return [handle.workflow_id for handle in handles], clear_attempts
    finally:
        dbos._sys_db.get_pending_workflows = original_get_pending  # type: ignore[method-assign]
        dbos._sys_db.clear_queue_assignment = original_clear  # type: ignore[method-assign]


def main() -> int:
    start_postgres()
    try:
        create_databases()
        create_effects_table()
        dbos = DBOS(
            config={
                "name": "recovery-repro",
                "application_database_url": app_url(),
                "system_database_url": sys_url(),
                "application_version": APP_VERSION,
                "executor_id": "recovery-worker",
                "enable_otlp": False,
                "run_admin_server": False,
            }
        )
        DBOS.launch()
        sys_engine = sa.create_engine(sys_url())
        try:
            enqueue_workflow(sys_engine)
            force_pending(sys_engine)
            snapshot = dbos._sys_db.get_pending_workflows(DEAD_EXECUTOR, GlobalParams.app_version)
            if len(snapshot) != 1:
                raise RuntimeError(f"Expected one pending workflow, got {snapshot!r}")

            first_handles, first_clears = recover_from_stale_result(dbos, snapshot, "recoverer-b")
            row_after_first = row_status(sys_engine)
            effects_after_first = effects()

            second_handles, second_clears = recover_from_stale_result(dbos, snapshot, "recoverer-c")
            deadline = time.monotonic() + 5
            observed_effects = effects()
            while time.monotonic() < deadline and not observed_effects:
                time.sleep(0.1)
                observed_effects = effects()
            final_row = row_status(sys_engine)

            result = {
                "first_handles": first_handles,
                "first_clears": first_clears,
                "row_after_first": row_after_first,
                "effects_after_first": effects_after_first,
                "second_handles": second_handles,
                "second_clears": second_clears,
                "final_row": final_row,
                "effects": observed_effects,
            }
            print(json.dumps(result, indent=2, sort_keys=True, default=str))
            stale_effects = [effect for effect in observed_effects if effect["actor"] == "recoverer-c"]
            second_clear_false = any(clear["cleared"] is False for clear in second_clears)
            if second_clear_false and stale_effects:
                print("REPRODUCED: stale recovery executed queued workflow after clear_queue_assignment returned false")
                return 1
            print("NOT REPRODUCED")
            return 0
        finally:
            sys_engine.dispose()
            DBOS.destroy()
    finally:
        stop_postgres()


if __name__ == "__main__":
    raise SystemExit(main())
```

Observed local result:

```text
REPRODUCED: stale recovery executed queued workflow after clear_queue_assignment returned false
```

</details>

## Expected behavior

For a queued workflow, if recovery calls `clear_queue_assignment(...)` and it
returns `false`, that recovery attempt should not execute the workflow body. It
should return a polling handle, skip the row, or otherwise let the current queue
owner handle execution.

## Actual behavior

The stale recovery attempt falls through to `execute_workflow_by_id(...)` after
`clear_queue_assignment(...)` returns `false`, so it can execute a workflow that
has already been returned to queue ownership.

## Relevant implementation path

In `dbos/_recovery.py`, queued workflow recovery does this:

```python
if workflow.queue_name:
    cleared = dbos._sys_db.clear_queue_assignment(workflow.workflow_id)
    if cleared:
        return WorkflowHandlePolling(workflow.workflow_id, dbos)
return execute_workflow_by_id(dbos, workflow.workflow_id, True, False)
```

For queued workflows, `clear_queue_assignment(...) == false` means this recovery
attempt did not update the current durable row. Treating that case the same as a
normal non-queued recovery lets a stale `get_pending_workflows(...)` result
execute work it no longer owns.

Ownership invariant: for queued workflow recovery, only the actor that
successfully clears or claims the current durable row executes the workflow
body.
