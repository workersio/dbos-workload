# SURFACE: External side-effect idempotency (P3ext)
# MODELS:  step records external API attempt (outside DBOS tx) then may crash
# ORACLE:  external_attempts / processed_external (parent SQL)
# ISSUES:  retry envelope, duplicate side effects
# VARIANCE: op_id from parent scenario

from __future__ import annotations

import os

import sqlalchemy as sa
from dbos import DBOS
from sqlalchemy.pool import NullPool

from dbos_workload_common import dbos_config

_APP_ENGINE = None


def _app_engine():
    global _APP_ENGINE
    if _APP_ENGINE is None:
        url = str(dbos_config()["application_database_url"]).replace(
            "postgresql://", "postgresql+psycopg://", 1
        )
        _APP_ENGINE = sa.create_engine(
            url,
            connect_args={"application_name": "dbos_workload_oracle"},
            poolclass=NullPool,
        )
    return _APP_ENGINE


def _record_external_attempt(op_id: str, run_id: str) -> None:
    executor = os.environ.get("DBOS__VMID", "local")
    with _app_engine().begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO external_attempts(op_id, run_id, executor) "
                "VALUES (:op_id, :run_id, :executor)"
            ),
            {"op_id": op_id, "run_id": run_id, "executor": executor},
        )


@DBOS.dbos_class()
class ExternalWF:
    @staticmethod
    @DBOS.step()
    def external_call(op_id: str, run_id: str) -> str:
        _record_external_attempt(op_id, run_id)
        if os.environ.get("DBOS_CRASH_NOW") == "1":
            os._exit(99)
        return "called"

    @staticmethod
    @DBOS.transaction()
    def record_processed(op_id: str, run_id: str) -> None:
        session = DBOS.sql_session
        session.execute(
            sa.text(
                "INSERT INTO processed_external(op_id, run_id) "
                "VALUES (:op_id, :run_id)"
            ),
            {"op_id": op_id, "run_id": run_id},
        )

    @staticmethod
    @DBOS.workflow()
    def idempotency_workflow(run_id: str, op_id: str) -> str:
        ExternalWF.external_call(op_id, run_id)
        ExternalWF.record_processed(op_id, run_id)
        return op_id
