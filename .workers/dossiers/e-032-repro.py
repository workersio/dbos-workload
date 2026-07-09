#!/usr/bin/env python3
"""Standalone repro: DBOS.send from inside a step is not exactly-once.

Claim under test: DBOS.send() from a workflow context is recorded and guarded, so
a re-execution delivers the message once. From a step context it is NOT: each
execution inserts a fresh notification, so a @DBOS.step(max_attempts>1) that sends
then fails re-delivers the same message on every retry. A recipient using
DBOS.recv() then receives one logical message multiple times.

Root cause (dbos/_core.py send_bulk dispatch): `if cur_ctx.is_workflow()` records
the send as a guarded step (workflow_id + function_id); the else branch handles
BOTH client and step contexts and passes workflow_id=None, function_id=None. In
_send_bulk_txn the once-and-only-once block is gated on `workflow_id is not None`,
so it is skipped for the step path, and each call mints a fresh message_uuid — the
on_conflict_do_nothing never dedups. (Contrast DBOS.write_stream, which has the
same workflow-vs-step asymmetry.)

Requirements:
    pip install dbos sqlalchemy "psycopg[binary]"
    a local Postgres (a superuser that can CREATE DATABASE).
    export DBOS_PG="postgresql+psycopg://postgres:dbos@localhost:5432/postgres"

Run:
    python e032_send_step_repro.py

Exit 1 + "BUG REPRODUCED" if the recipient receives the step-sent message more
than once while the workflow-sent control is received exactly once.
"""
from __future__ import annotations

import os
import sys
import uuid

import sqlalchemy as sa
from sqlalchemy.engine import make_url

from dbos import DBOS, DBOSConfig, SetWorkflowID

ADMIN = os.environ.get("DBOS_PG",
                       "postgresql+psycopg://postgres:dbos@localhost:5432/postgres")
TOPIC = "orders"
VALUE = "order-1"
ATTEMPTS_BEFORE_SUCCESS = 3


def make_databases(prefix: str) -> tuple[str, str]:
    base = make_url(ADMIN)
    if base.drivername == "postgresql":
        base = base.set(drivername="postgresql+psycopg")
    admin = base.set(database="postgres")
    engine = sa.create_engine(admin.render_as_string(hide_password=False),
                              connect_args={"connect_timeout": 5})
    with engine.connect() as raw:
        conn = raw.execution_options(isolation_level="AUTOCOMMIT")
        for db in (f"{prefix}_app", f"{prefix}_sys"):
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)'))
            conn.execute(sa.text(f'CREATE DATABASE "{db}"'))
    engine.dispose()
    app = base.set(drivername="postgresql", database=f"{prefix}_app")
    sysu = base.set(drivername="postgresql+psycopg", database=f"{prefix}_sys")
    return (app.render_as_string(hide_password=False),
            sysu.render_as_string(hide_password=False))


prefix = f"dbos_send_repro_{uuid.uuid4().hex[:8]}"
app_url, sys_url = make_databases(prefix)
config: DBOSConfig = {
    "name": "send-step-repro",
    "application_database_url": app_url,
    "system_database_url": sys_url,
    "enable_otlp": False,
}
DBOS.destroy(destroy_registry=False)
DBOS(config=config)

_attempts: dict[str, int] = {}


@DBOS.step(retries_allowed=True, max_attempts=ATTEMPTS_BEFORE_SUCCESS,
           interval_seconds=0.0)
def send_from_step(dest: str, value: str) -> None:
    _attempts[dest] = _attempts.get(dest, 0) + 1
    DBOS.send(dest, value, TOPIC)                      # STEP-context send
    if _attempts[dest] < ATTEMPTS_BEFORE_SUCCESS:      # fail, forcing a retry
        raise RuntimeError(f"transient failure attempt {_attempts[dest]}")


@DBOS.workflow()
def sender_via_step(dest: str, value: str) -> None:
    send_from_step(dest, value)


@DBOS.workflow()
def sender_via_workflow(dest: str, value: str) -> None:
    DBOS.send(dest, value, TOPIC)                      # WORKFLOW-context send


@DBOS.workflow()
def recipient() -> list:
    """Drain every message on the topic until a short timeout returns None."""
    got = []
    while True:
        m = DBOS.recv(TOPIC, timeout_seconds=2)
        if m is None:
            break
        got.append(m)
    return got


DBOS.launch()


def deliver(sender, label: str) -> list:
    dest = f"recipient-{label}-{uuid.uuid4().hex[:8]}"
    with SetWorkflowID(dest):
        handle = DBOS.start_workflow(recipient)        # recipient starts recv-waiting
    sid = f"sender-{label}-{uuid.uuid4().hex[:8]}"
    with SetWorkflowID(sid):
        sender(dest, VALUE)                            # send once (logically)
    return handle.get_result()                         # what the recipient received


from_step = deliver(sender_via_step, "step")
from_workflow = deliver(sender_via_workflow, "workflow")

DBOS.destroy(destroy_registry=False)

print()
print(f"one send from a retrying step  -> recipient received {from_step}  ({len(from_step)} copies)")
print(f"one send from a workflow        -> recipient received {from_workflow}  ({len(from_workflow)} copies)")
print()

if len(from_step) > 1:
    print(f"BUG REPRODUCED: a single logical DBOS.send from a step was delivered "
          f"{len(from_step)} times; the same send from a workflow was delivered "
          f"{len(from_workflow)} time.")
    sys.exit(1)
else:
    print("Not reproduced on this version.")
    sys.exit(0)
