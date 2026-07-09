# [auth] SQL-enqueued workflow denied by required roles stays PENDING

Filed upstream: https://github.com/dbos-inc/dbos-transact-py/issues/743

The SQL `dbos.enqueue_workflow(...)` helper can persist
`authenticated_user` and `authenticated_roles` on workflow rows. That works for
allowed users. But when a SQL-enqueued workflow requires a role the stored user
does not have, DBOS dequeues the workflow and leaves it stuck `PENDING` instead
of recording a terminal authorization error.

The stored row has `status = PENDING`, `output = null`, and `error = null`.

## Environment observed

- DBOS source: checkout at `3df88c4bcc3a2b73d91b765b459f4c7beb6a690c`
- Runtime observed: DBOS `2.24.0-12-g3df88c4`, CPython `3.14.6`
- Backend: Postgres-backed SQL enqueue and DBOS queue

## Minimal repro

Run the standalone script in the details section:

```bash
python3 repro_sql_auth_denied_pending.py
```

The repro covers both an allowed control case and a denied case:

- Allowed control: user `alice`, roles `["reader", "admin"]`, required role
  `admin`, reaches `SUCCESS`.
- Denied case: user `bob`, roles `["reader"]`, required role `admin`, is
  dequeued but remains `PENDING`.

Observed denied row:

```text
status = PENDING
authenticated_user = "bob"
authenticated_roles = "[\"reader\"]"
output = null
error = null
```

Observed result:

```text
INVARIANT workflow_reached_expected_status ... FAIL
```

<details>
<summary><code>repro_sql_auth_denied_pending.py</code> - standalone local reproducer</summary>

This script starts a temporary local PostgreSQL cluster, defines a DBOS workflow that requires the `admin` role, SQL-enqueues one allowed control row as `alice`, SQL-enqueues one denied row as `bob`, and exits nonzero when the denied row remains `PENDING` with null output and error.

Run with `python3 repro_*.py` from a Python environment where `dbos`, `sqlalchemy`, and `psycopg` are installed. If PostgreSQL binaries are not on `PATH`, set `PGBIN` to the directory containing `initdb`, `pg_ctl`, and `pg_isready`.

```python
#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import sqlalchemy as sa

from dbos import DBOS


PG_BIN = Path(os.environ["PGBIN"]) if "PGBIN" in os.environ else None
PGDATA = Path(os.environ.get("PGDATA", "/tmp/dbos-sql-auth-repro-pg"))
PGPORT = int(os.environ.get("PGPORT", "55434"))
PGPASSWORD = os.environ.get("PGPASSWORD", "dbos")
APP_DB = "dbos_sql_auth_repro_app"
SYS_DB = "dbos_sql_auth_repro_sys"
QUEUE = "dbos_sql_auth_repro_queue"
ALLOWED_WORKFLOW_ID = "dbos-sql-auth-allowed-repro"
DENIED_WORKFLOW_ID = "dbos-sql-auth-denied-repro"
APP_VERSION = "dbos-sql-auth-repro-v1"


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
    init_log = Path("/tmp/dbos-sql-auth-repro-initdb.log")
    pg_log = Path("/tmp/dbos-sql-auth-repro-postgres.log")
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


@DBOS.required_roles(["admin"])
@DBOS.workflow()
def admin_only(label: str) -> str:
    return f"{label}:{DBOS.authenticated_user}:{DBOS.assumed_role}"


def workflow_status(engine: sa.Engine, workflow_id: str) -> dict[str, object] | None:
    with engine.connect() as conn:
        row = conn.execute(
            sa.text(
                """
                SELECT workflow_uuid, status, name, queue_name, output, error,
                       authenticated_user, authenticated_roles
                FROM dbos.workflow_status
                WHERE workflow_uuid = :workflow_id
                """
            ),
            {"workflow_id": workflow_id},
        ).fetchone()
    if row is None:
        return None
    return dict(row._mapping)


def enqueue_workflow(engine: sa.Engine, workflow_id: str, label: str, user: str, roles: list[str]) -> None:
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                SELECT dbos.enqueue_workflow(
                    workflow_name => 'admin_only',
                    queue_name => :queue_name,
                    positional_args => ARRAY[:label]::json[],
                    workflow_id => :workflow_id,
                    app_version => :app_version,
                    authenticated_user => :user,
                    authenticated_roles => :roles
                )
                """
            ),
            {
                "queue_name": QUEUE,
                "label": json.dumps(label),
                "workflow_id": workflow_id,
                "app_version": APP_VERSION,
                "user": user,
                "roles": json.dumps(roles),
            },
        )


def main() -> int:
    start_postgres()
    try:
        create_databases()
        DBOS(
            config={
                "name": "sql-auth-repro",
                "application_database_url": app_url(),
                "system_database_url": sys_url(),
                "application_version": APP_VERSION,
                "executor_id": "sql-auth-worker",
                "enable_otlp": False,
                "run_admin_server": False,
                "notification_listener_polling_interval_sec": 0.01,
            }
        )
        DBOS.launch()
        DBOS.register_queue(QUEUE, concurrency=4, polling_interval_sec=0.05, on_conflict="always_update")

        engine = sa.create_engine(sys_url())
        try:
            enqueue_workflow(
                engine,
                ALLOWED_WORKFLOW_ID,
                "allowed-sql-auth",
                "alice",
                ["reader", "admin"],
            )
            enqueue_workflow(
                engine,
                DENIED_WORKFLOW_ID,
                "denied-sql-auth",
                "bob",
                ["reader"],
            )

            deadline = time.monotonic() + 12
            allowed = None
            denied = None
            while time.monotonic() < deadline:
                allowed = workflow_status(engine, ALLOWED_WORKFLOW_ID)
                denied = workflow_status(engine, DENIED_WORKFLOW_ID)
                if (
                    allowed
                    and allowed["status"] == "SUCCESS"
                    and denied
                    and denied["status"] in {"SUCCESS", "ERROR"}
                ):
                    break
                time.sleep(0.25)
            result = {"allowed": allowed, "denied": denied}
            print(json.dumps(result, indent=2, sort_keys=True, default=str))
            allowed_success = allowed and allowed["status"] == "SUCCESS"
            denied_pending = (
                denied
                and denied["status"] == "PENDING"
                and denied["output"] is None
                and denied["error"] is None
            )
            if allowed_success and denied_pending:
                print("REPRODUCED: denied SQL-enqueued workflow stayed PENDING")
                return 1
            if not allowed_success:
                print("CONTROL FAILED: allowed SQL-enqueued workflow did not reach SUCCESS")
                return 2
            print("NOT REPRODUCED")
            return 0
        finally:
            engine.dispose()
            DBOS.destroy()
    finally:
        stop_postgres()


if __name__ == "__main__":
    raise SystemExit(main())
```

Observed local result:

```text
REPRODUCED: denied SQL-enqueued workflow stayed PENDING
```

</details>

## Expected behavior

For a SQL-enqueued workflow whose persisted roles do not satisfy
`@DBOS.required_roles(["admin"])`, DBOS should restore the persisted auth
context, fail the required-role check, and persist a terminal `ERROR` with an
authorization error.

## Actual behavior

The workflow is dequeued into `PENDING`, the required-role check fails, and the
row remains `PENDING` with null output and null error.

## Relevant implementation path

Relevant source path:

1. `start_queued_workflows(...)` marks selected queued rows `PENDING`.
2. `execute_workflow_by_id(...)` restores auth metadata from the workflow row.
3. `_execute_workflow_wthread(...)` calls `check_required_roles(...)`.
4. That required-role check happens before the wrapper that persists workflow
   success/error outcomes.
5. If `check_required_roles(...)` raises `DBOSNotAuthorizedError`, the exception
   can escape without recording `update_workflow_outcome(ERROR)`.

Required-role failures during queued workflow execution should finalize the row
as `ERROR`, the same way workflow body exceptions are persisted.
