#!/usr/bin/env python3
"""
Standalone reproduction: DBOS Transact (Python) — DBOS.write_stream called from
inside a step is NOT exactly-once. When the step is retried, the same value is
written to the stream again on every attempt, so a consumer of DBOS.read_stream
sees one logical write delivered multiple times.

What happens
------------
write_stream() dispatches on the calling context (dbos/_core.py):

  * from a WORKFLOW  -> SystemDatabase.write_stream_from_workflow : records an
    operation output and guards re-execution with
    _check_operation_execution_txn("DBOS.writeStream"). Exactly-once on replay.
  * from a STEP      -> SystemDatabase.write_stream_from_step : inserts the value
    at offset max(offset)+1 with NO recorded operation and NO execution guard;
    it only retries on an offset IntegrityError.

The `streams` table primary key is (workflow_uuid, key, offset) — it does not
include function_id. A @DBOS.step(retries_allowed=True, max_attempts=N) re-runs
its body under the SAME step function_id on each attempt. So a step that writes
to a stream and then fails re-inserts the same value at a new offset on every
retry. The workflow still completes successfully; the duplication is silent.

The public API is identical (DBOS.write_stream / DBOS.write_stream_async) and the
docs don't distinguish the two contexts, so a value written once from a step
appears N times to readers, while the same value written from a workflow appears
once.

Expected vs actual
------------------
  Expected: one DBOS.write_stream call contributes one value to the stream,
            regardless of retries (matching the workflow-context behavior).
  Actual:   from a retrying step, the value appears once per attempt.

Requirements
------------
    pip install dbos sqlalchemy "psycopg[binary]"
    a running local Postgres (a superuser that can CREATE DATABASE — DBOS creates
    its system and application databases automatically).

    # point this at your Postgres if it is not the default below
    export DBOS_PG="postgresql+psycopg://postgres:dbos@localhost:5432/postgres"

    python e031_repro.py

Exit code 1 and "BUG REPRODUCED" if the retrying step duplicates the value.
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

STREAM_KEY = "progress"
VALUE = "event-1"
# How many attempts the step makes before succeeding (>=2 means at least one retry).
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


prefix = f"dbos_stream_repro_{uuid.uuid4().hex[:8]}"
app_url, sys_url = make_databases(prefix)

config: DBOSConfig = {
    "name": "stream-repro",
    "application_database_url": app_url,
    "system_database_url": sys_url,
    "enable_otlp": False,
}

DBOS.destroy(destroy_registry=False)
DBOS(config=config)

_attempts: dict[str, int] = {}


@DBOS.step(retries_allowed=True, max_attempts=ATTEMPTS_BEFORE_SUCCESS,
           interval_seconds=0.0)
def emit_from_step(value: str, wid: str) -> str:
    _attempts[wid] = _attempts.get(wid, 0) + 1
    DBOS.write_stream(STREAM_KEY, value)
    # Simulate a transient failure so the step is retried by DBOS.
    if _attempts[wid] < ATTEMPTS_BEFORE_SUCCESS:
        raise RuntimeError(f"transient failure on attempt {_attempts[wid]}")
    return value


@DBOS.workflow()
def workflow_writes_from_step(value: str, wid: str) -> str:
    r = emit_from_step(value, wid)
    DBOS.close_stream(STREAM_KEY)
    return r


@DBOS.workflow()
def workflow_writes_directly(value: str, wid: str) -> str:
    DBOS.write_stream(STREAM_KEY, value)      # written from the workflow itself
    DBOS.close_stream(STREAM_KEY)
    return value


DBOS.launch()


def read_all(wid: str) -> list:
    return list(DBOS.read_stream(wid, STREAM_KEY))


# 1) Write the value ONCE from inside a retrying step.
wid_step = f"from-step-{uuid.uuid4().hex[:8]}"
with SetWorkflowID(wid_step):
    workflow_writes_from_step(VALUE, wid_step)
from_step = read_all(wid_step)

# 2) Write the same value ONCE from a workflow context (the comparison).
wid_wf = f"from-workflow-{uuid.uuid4().hex[:8]}"
with SetWorkflowID(wid_wf):
    workflow_writes_directly(VALUE, wid_wf)
from_workflow = read_all(wid_wf)

DBOS.destroy(destroy_registry=False)

print()
print(f"single write from a retrying step  -> stream = {from_step}  ({len(from_step)} values)")
print(f"single write from a workflow        -> stream = {from_workflow}  ({len(from_workflow)} values)")
print()

if len(from_step) > 1:
    print("BUG REPRODUCED: one DBOS.write_stream call from a step produced "
          f"{len(from_step)} stream entries; the same call from a workflow produced "
          f"{len(from_workflow)}.")
    sys.exit(1)
else:
    print("Not reproduced on this version.")
    sys.exit(0)
