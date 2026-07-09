#!/usr/bin/env python3
"""Standalone repro/observation: DBOSClient.get_event() waits the full timeout even
after the target workflow has already finished without ever setting the key.

DBOSClient.get_event delegates to the system-db get_event wait loop, which exits
only when the in-memory event fires or the deadline passes. Its fallback poll
(get_event_check) queries only the workflow_events row, never the target workflow's
status. So once a workflow has reached a terminal state without setting the key —
at which point the value can no longer appear — the client still blocks until the
whole timeout elapses, then returns None.

NOTE: this may be intended API semantics (get_event is documented as a
timeout-bounded wait, not lifecycle-coupled). It is reported here as a latency /
contract observation, not asserted as a defect.

Requirements:
    pip install dbos sqlalchemy "psycopg[binary]"
    a local Postgres. export DBOS_PG="postgresql+psycopg://postgres:dbos@localhost:5432/postgres"

Run:
    python e025_client_get_event_terminal_miss.py
"""
from __future__ import annotations

import os
import threading
import time
import uuid

import sqlalchemy as sa
from sqlalchemy.engine import make_url

from dbos import DBOS, DBOSClient, DBOSConfig, SetWorkflowID

ADMIN = os.environ.get("DBOS_PG",
                       "postgresql+psycopg://postgres:dbos@localhost:5432/postgres")
TIMEOUT = 12.0
PROMPT_BOUND = 5.0   # a completed-without-set workflow should resolve well under this


def make_databases(prefix: str) -> tuple[str, str]:
    base = make_url(ADMIN)
    if base.drivername == "postgresql":
        base = base.set(drivername="postgresql+psycopg")
    engine = sa.create_engine(base.set(database="postgres").render_as_string(hide_password=False),
                              connect_args={"connect_timeout": 5})
    with engine.connect() as raw:
        conn = raw.execution_options(isolation_level="AUTOCOMMIT")
        for db in (f"{prefix}_app", f"{prefix}_sys"):
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)'))
            conn.execute(sa.text(f'CREATE DATABASE "{db}"'))
    engine.dispose()
    return (base.set(drivername="postgresql", database=f"{prefix}_app").render_as_string(hide_password=False),
            base.set(drivername="postgresql+psycopg", database=f"{prefix}_sys").render_as_string(hide_password=False))


prefix = f"dbos_getevent_repro_{uuid.uuid4().hex[:8]}"
app_url, sys_url = make_databases(prefix)
config: DBOSConfig = {
    "name": "getevent-repro",
    "application_database_url": app_url,
    "system_database_url": sys_url,
    "enable_otlp": False,
}
DBOS.destroy(destroy_registry=False)
DBOS(config=config)


@DBOS.workflow()
def no_event_wf() -> str:
    # Completes without ever calling DBOS.set_event(...).
    return "done"


DBOS.launch()

wid = f"no-event-{uuid.uuid4().hex[:8]}"
client = DBOSClient(system_database_url=sys_url)   # a client has no in-process listener

captured: dict = {}


def waiter() -> None:
    t0 = time.time()
    val = client.get_event(wid, "missing_key", timeout_seconds=TIMEOUT)
    captured["value"] = val
    captured["t_return"] = time.time()
    captured["t_start"] = t0


th = threading.Thread(target=waiter)
th.start()

# Start the target workflow and force it durably terminal.
with SetWorkflowID(wid):
    handle = DBOS.start_workflow(no_event_wf)
handle.get_result()
t_terminal = time.time()

th.join(timeout=TIMEOUT + 5)
DBOS.destroy(destroy_registry=False)

wait_after_terminal = captured["t_return"] - t_terminal
print()
print(f"get_event returned value : {captured['value']!r}")
print(f"waited after workflow was already terminal : {wait_after_terminal:.2f}s "
      f"(prompt bound {PROMPT_BOUND}s, timeout {TIMEOUT}s)")
print()

if captured["value"] is None and wait_after_terminal > PROMPT_BOUND:
    print(f"OBSERVED: get_event blocked ~{wait_after_terminal:.1f}s after the target "
          f"workflow was already terminal without the key — effectively the full "
          f"timeout — before returning None. (Contract question, not asserted as a bug.)")
    raise SystemExit(1)
else:
    print("Not observed (returned promptly after terminal).")
    raise SystemExit(0)
