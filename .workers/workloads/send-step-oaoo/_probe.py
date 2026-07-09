#!/usr/bin/env python3
"""Probe: is DBOS.send from a STEP context exactly-once? (e-031 analog on notifications)

_core.py send_bulk dispatch: `if cur_ctx.is_workflow()` records a guarded step;
the else branch (step OR client) passes workflow_id=None,function_id=None, so the
OAOO block in _send_bulk_txn (gated on workflow_id is not None) is skipped and each
call mints a fresh message_uuid -> a step retry re-inserts a duplicate notification.
Control: DBOS.send from the workflow body (guarded) -> exactly one message.
"""
from __future__ import annotations
import os, sys, uuid
from pathlib import Path

# NOTE: run as a script from a neutral dir so `import dbos` = installed release.
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from dbos import DBOS, DBOSConfig, SetWorkflowID

ADMIN = os.environ.get("DBOS_PG", "postgresql+psycopg://postgres:dbos@127.0.0.1:5459/postgres")
TOPIC = "t"

def make_dbs(prefix):
    base = make_url(ADMIN)
    if base.drivername == "postgresql":
        base = base.set(drivername="postgresql+psycopg")
    eng = sa.create_engine(base.set(database="postgres").render_as_string(hide_password=False),
                           connect_args={"connect_timeout": 5})
    with eng.connect() as raw:
        c = raw.execution_options(isolation_level="AUTOCOMMIT")
        for db in (f"{prefix}_app", f"{prefix}_sys"):
            c.execute(sa.text(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)'))
            c.execute(sa.text(f'CREATE DATABASE "{db}"'))
    eng.dispose()
    return (base.set(drivername="postgresql", database=f"{prefix}_app").render_as_string(hide_password=False),
            base.set(drivername="postgresql+psycopg", database=f"{prefix}_sys").render_as_string(hide_password=False))

prefix = "wio_send_probe"
app_url, sys_url = make_dbs(prefix)
cfg: DBOSConfig = {"name": "send-probe", "application_database_url": app_url,
                   "system_database_url": sys_url, "enable_otlp": False}
DBOS.destroy(destroy_registry=False)
DBOS(config=cfg)

attempts = {"n": 0}

@DBOS.step(retries_allowed=True, max_attempts=3, interval_seconds=0.0)
def flaky_send(dest: str, val: str):
    attempts["n"] += 1
    DBOS.send(dest, val, TOPIC)              # STEP-context send
    if attempts["n"] < 3:
        raise RuntimeError(f"transient {attempts['n']}")

@DBOS.workflow()
def sender_step(dest: str, val: str):
    flaky_send(dest, val)

@DBOS.workflow()
def sender_wf(dest: str, val: str):
    DBOS.send(dest, val, TOPIC)              # WORKFLOW-context send (guarded)

# a destination workflow that receives (must exist for send)
@DBOS.workflow()
def receiver():
    msgs = []
    while True:
        m = DBOS.recv(TOPIC, timeout_seconds=1)
        if m is None:
            break
        msgs.append(m)
    return msgs

DBOS.launch()

def count_notifications(dest_id: str) -> int:
    eng = sa.create_engine(sys_url)
    try:
        with eng.connect() as c:
            # count notification rows queued for this destination + topic
            return c.execute(sa.text(
                "SELECT count(*) FROM dbos.notifications WHERE destination_uuid=:d AND topic=:t"
            ), {"d": dest_id, "t": TOPIC}).scalar() or 0
    finally:
        eng.dispose()

# --- step-context send under retry ---
dest_step = f"dest-step-{uuid.uuid4().hex[:8]}"
with SetWorkflowID(dest_step):
    DBOS.start_workflow(receiver)            # register the destination workflow
sid = f"send-step-{uuid.uuid4().hex[:8]}"
with SetWorkflowID(sid):
    sender_step(dest_step, "V")
n_step = count_notifications(dest_step)

# --- workflow-context send (control) ---
attempts["n"] = 0
dest_wf = f"dest-wf-{uuid.uuid4().hex[:8]}"
with SetWorkflowID(dest_wf):
    DBOS.start_workflow(receiver)
sid2 = f"send-wf-{uuid.uuid4().hex[:8]}"
with SetWorkflowID(sid2):
    sender_wf(dest_wf, "V")
n_wf = count_notifications(dest_wf)

print(f"STEP-CONTEXT send: attempts={attempts['n']}  notifications={n_step}")
print(f"WF-CONTEXT   send: notifications={n_wf}")
print(f"RESULT step={n_step} wf={n_wf} DUPLICATE={'YES' if n_step > 1 else 'no'}")
DBOS.destroy(destroy_registry=False)
sys.exit(1 if n_step > 1 else 0)
