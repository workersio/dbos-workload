"""
Reproduction: with the portable JSON serializer, a workflow argument of
float('nan') (or float('inf') / float('-inf')) is persisted to the durable
workflow_status.inputs column as the non-standard tokens NaN / Infinity /
-Infinity. That column is not valid JSON, so any standard/other-language JSON
reader rejects it -- yet the workflow completes with status SUCCESS and DBOS
raises no error.

The portable format is documented as "straightforward use of JSON that all SDKs
can read and write" and data that "can even be read and written from the
database without any DBOS code at all"
(https://docs.dbos.dev/explanations/portable-workflows). NaN / Infinity are not
part of JSON (RFC 8259), so a conforming reader in another language (JS
JSON.parse, Go encoding/json, etc.) cannot read the stored input.

Requires a reachable Postgres. Set PG_URL or use the default below.

    pip install "dbos==2.26.0" "psycopg[binary]" "sqlalchemy"
    python repro.py

Exit code 1 and a "REPRODUCES" line when the defect fires; exit 0 otherwise.
"""
from __future__ import annotations

import json
import os
import uuid

import sqlalchemy as sa
from sqlalchemy.engine import make_url

from dbos import DBOS, DBOSConfig, SetWorkflowID
from dbos._serialization import WorkflowSerializationFormat

PG_URL = os.environ.get("PG_URL", "postgresql://postgres:dbos@localhost:5459/postgres")


def strict_json_ok(text: str) -> bool:
    """True iff text is valid JSON per RFC 8259 (NaN / Infinity rejected)."""
    def reject(tok: str) -> object:
        raise ValueError(f"non-standard token {tok!r}")
    try:
        json.loads(text, parse_constant=reject)
        return True
    except ValueError:
        return False


def make_dbs(prefix: str) -> tuple[str, str]:
    base = make_url(PG_URL).set(drivername="postgresql+psycopg")
    app_db, sys_db = f"{prefix}_app", f"{prefix}_sys"
    admin = base.set(database="postgres").render_as_string(hide_password=False)
    eng = sa.create_engine(admin, connect_args={"connect_timeout": 5})
    with eng.connect() as raw:
        c = raw.execution_options(isolation_level="AUTOCOMMIT")
        for db in (app_db, sys_db):
            c.execute(sa.text(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)'))
            c.execute(sa.text(f'CREATE DATABASE "{db}"'))
    eng.dispose()
    return (
        base.set(drivername="postgresql", database=app_db).render_as_string(hide_password=False),
        base.set(drivername="postgresql+psycopg", database=sys_db).render_as_string(hide_password=False),
    )


@DBOS.workflow(name="echo", serialization_type=WorkflowSerializationFormat.PORTABLE)
def echo(value):
    return value


def stored_inputs(sys_url: str, wid: str) -> str:
    eng = sa.create_engine(sys_url, connect_args={"connect_timeout": 5})
    with eng.connect() as c:
        return c.execute(
            sa.text("SELECT inputs FROM dbos.workflow_status WHERE workflow_uuid = :w"),
            {"w": wid},
        ).scalar()


def main() -> int:
    app_url, sys_url = make_dbs(f"portable_repro_{uuid.uuid4().hex[:8]}")
    config: DBOSConfig = {
        "name": "portable-repro",
        "application_database_url": app_url,
        "system_database_url": sys_url,
        "serializer": None,  # per-workflow PORTABLE below
        "enable_otlp": False,
    }
    DBOS.destroy(destroy_registry=False)
    DBOS(config=config)
    DBOS.launch()

    reproduced = False
    try:
        for label, value in [("control float 1.5", 1.5), ("float('nan')", float("nan"))]:
            wid = f"echo-{uuid.uuid4().hex[:8]}"
            with SetWorkflowID(wid):
                echo(value)
            status = DBOS.retrieve_workflow(wid).get_status().status
            stored = stored_inputs(sys_url, wid)
            ok = strict_json_ok(stored)
            print(f"\n{label}:")
            print(f"  workflow status       : {status}")
            print(f"  stored inputs column  : {stored}")
            print(f"  valid JSON (RFC 8259) : {ok}")
            if status == "SUCCESS" and not ok:
                reproduced = True
    finally:
        DBOS.destroy(destroy_registry=False)

    print()
    if reproduced:
        print("REPRODUCES: workflow SUCCEEDED but its durable inputs column is not valid JSON")
        return 1
    print("did not reproduce")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
