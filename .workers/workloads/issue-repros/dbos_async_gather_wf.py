# SURFACE: Async gather + multi-worker handoff (P4)
# MODELS:  asyncio.gather of distinct async steps; crash mid-gather on worker-a
# ORACLE:  step completion counts, recovery_attempts bound
# ISSUES:  #688
# VARIANCE: which step crashes (a/b/c) from WORKLOAD_SEED

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


def _maybe_crash(step_name: str, crash_step: str) -> None:
    if crash_step == step_name and os.environ.get("DBOS_CRASH_NOW") == "1":
        os._exit(99)


def _record_step(step_name: str, run_id: str) -> None:
    # Keep oracle writes independent of DBOS transaction context; async steps do
    # not expose DBOS.sql_session. Rows intentionally count attempts, so recovery
    # retries remain visible to the bounded-step invariant.
    executor = os.environ.get("DBOS__VMID", "local")
    with _app_engine().begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO gather_steps(run_id, step_name, executor) "
                "VALUES (:run_id, :step_name, :executor)"
            ),
            {"run_id": run_id, "step_name": step_name, "executor": executor},
        )


@DBOS.dbos_class()
class GatherWF:
    @staticmethod
    @DBOS.step()
    async def step_a(run_id: str, crash_step: str) -> str:
        _record_step("step_a", run_id)
        await asyncio.sleep(0)
        _maybe_crash("step_a", crash_step)
        return "a"

    @staticmethod
    @DBOS.step()
    async def step_b(run_id: str, crash_step: str) -> str:
        _record_step("step_b", run_id)
        await asyncio.sleep(0)
        _maybe_crash("step_b", crash_step)
        return "b"

    @staticmethod
    @DBOS.step()
    async def step_c(run_id: str, crash_step: str) -> str:
        _record_step("step_c", run_id)
        await asyncio.sleep(0)
        _maybe_crash("step_c", crash_step)
        return "c"

    @staticmethod
    @DBOS.workflow()
    async def gather_workflow(run_id: str, crash_step: str) -> dict[str, str]:
        a, b, c = await asyncio.gather(
            GatherWF.step_a(run_id, crash_step),
            GatherWF.step_b(run_id, crash_step),
            GatherWF.step_c(run_id, crash_step),
        )
        return {"a": a, "b": b, "c": c}
