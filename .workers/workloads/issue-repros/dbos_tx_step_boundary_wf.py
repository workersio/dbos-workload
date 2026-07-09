# SURFACE: Transaction-step boundary (P2tx)
# MODELS:  async steps interleaved with @DBOS.transaction; crash at seed boundary
# ORACLE:  boundary_audit rows (parent SQL)
# ISSUES:  session lifecycle, duplicate writes across retry
# VARIANCE: crash_point from parent scenario

from __future__ import annotations

import asyncio
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


def _record_async(run_id: str, tag: str) -> None:
    executor = os.environ.get("DBOS__VMID", "local")
    with _app_engine().begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO boundary_audit(run_id, tag, kind, executor) "
                "VALUES (:run_id, :tag, 'async', :executor)"
            ),
            {"run_id": run_id, "tag": tag, "executor": executor},
        )


def _maybe_crash(crash_point: str, expected: str) -> None:
    if crash_point == expected and os.environ.get("DBOS_CRASH_NOW") == "1":
        os._exit(99)


@DBOS.dbos_class()
class TxBoundaryWF:
    @staticmethod
    @DBOS.step()
    async def async_mark(run_id: str, tag: str, crash_point: str, crash_after: str) -> str:
        _record_async(run_id, tag)
        await asyncio.sleep(0)
        _maybe_crash(crash_point, crash_after)
        return tag

    @staticmethod
    @DBOS.transaction()
    def tx_mark(run_id: str, tag: str) -> str:
        session = DBOS.sql_session
        session.execute(
            sa.text(
                "INSERT INTO boundary_audit(run_id, tag, kind, executor) "
                "VALUES (:run_id, :tag, 'tx', :executor)"
            ),
            {
                "run_id": run_id,
                "tag": tag,
                "executor": os.environ.get("DBOS__VMID", "local"),
            },
        )
        session.execute(
            sa.text(
                "INSERT INTO boundary_counter(run_id, tag, n) VALUES (:run_id, :tag, 1) "
                "ON CONFLICT (run_id, tag) DO UPDATE SET n = boundary_counter.n + 1"
            ),
            {"run_id": run_id, "tag": tag},
        )
        return tag

    @staticmethod
    @DBOS.workflow()
    async def boundary_workflow(run_id: str, crash_point: str) -> int:
        await TxBoundaryWF.async_mark(run_id, "async_pre", crash_point, "after_async_pre")

        TxBoundaryWF.tx_mark(run_id, "tx_pre")
        _maybe_crash(crash_point, "after_tx_pre")

        await TxBoundaryWF.async_mark(run_id, "async_gather", crash_point, "__never__")
        TxBoundaryWF.tx_mark(run_id, "tx_gather")
        _maybe_crash(crash_point, "after_gather")

        TxBoundaryWF.tx_mark(run_id, "tx_post")
        _maybe_crash(crash_point, "after_tx_post")

        await TxBoundaryWF.async_mark(run_id, "async_post", crash_point, "__never__")
        return 5
