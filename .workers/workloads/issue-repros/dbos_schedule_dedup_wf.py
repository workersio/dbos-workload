"""Scheduled tick parent/child workflows for schedule dedup (P718)."""

from __future__ import annotations

import asyncio
import os

import sqlalchemy as sa
from dbos import DBOS
from dbos._context import SetEnqueueOptions

from dbos_workload_common import APP_DB, dbos_config  # noqa: F401

QUEUE_NAME = "formal_schedule_dedup"
DEDUP_SUFFIX = "schedule-dedup"

_APP_ENGINE = None


def _app_engine():
    global _APP_ENGINE
    if _APP_ENGINE is None:
        url = str(dbos_config()["application_database_url"]).replace(
            "postgresql://", "postgresql+psycopg://", 1
        )
        from sqlalchemy.pool import NullPool

        _APP_ENGINE = sa.create_engine(
            url,
            connect_args={"application_name": "dbos_workload_oracle"},
            poolclass=NullPool,
        )
    return _APP_ENGINE


def _record_child_completion(run_id: str, scheduled_time: str) -> None:
    with _app_engine().begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO schedule_completions(run_id, scheduled_time, executor) "
                "VALUES (:run_id, :scheduled_time, :executor)"
            ),
            {
                "run_id": run_id,
                "scheduled_time": scheduled_time,
                "executor": os.environ.get("DBOS__VMID", "local"),
            },
        )


@DBOS.workflow()
async def tick_child(scheduled_time: str, run_id: str) -> str:
    await asyncio.sleep(0.12)
    _record_child_completion(run_id, scheduled_time)
    return scheduled_time


@DBOS.workflow()
async def tick_parent(scheduled_time: str, run_id: str) -> str:
    from dbos._error import DBOSQueueDeduplicatedError

    dedup_id = f"{run_id}-{DEDUP_SUFFIX}"
    try:
        with SetEnqueueOptions(deduplication_id=dedup_id):
            handle = await DBOS.enqueue_workflow_async(
                QUEUE_NAME, tick_child, scheduled_time, run_id
            )
        result = await handle.get_result()
        return f"{scheduled_time}:{result}"
    except DBOSQueueDeduplicatedError:
        return f"{scheduled_time}:deduped"
