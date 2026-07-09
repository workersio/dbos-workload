#!/usr/bin/env python3
"""
P718: schedule-style dedup across overlapping ticks (#718).

# SURFACE: parent tick workflow enqueues deduplicated child on a queue
# MODELS:  concurrent parent ticks with shared deduplication_id
# ORACLE:  bounded_child_completions; at_most_one_live_child
# ISSUES:  #718
"""

from __future__ import annotations

import asyncio
import os

from dbos_workload_common import (
    APP_DB,
    SYS_DB,
    invariant,
    progress,
    psql,
    workload_main,
    workload_seed_raw,
)

QUEUE_NAME = "formal_schedule_dedup"
TICK_COUNT = int(os.environ.get("P718_TICK_COUNT", "3"))


def init_schema() -> None:
    psql(
        """
        CREATE TABLE IF NOT EXISTS schedule_completions(
          id SERIAL PRIMARY KEY,
          run_id TEXT NOT NULL,
          scheduled_time TEXT NOT NULL,
          executor TEXT NOT NULL
        );
        """,
        database=APP_DB,
    )
    psql("TRUNCATE schedule_completions RESTART IDENTITY;", database=APP_DB)


def sql_scalar(sql: str, database: str = APP_DB) -> str:
    return psql(sql, database=database).splitlines()[-1].strip()


async def _run_parent_ticks(tick_times: list[str], run_id: str) -> list[object]:
    from dbos import DBOS
    from dbos_schedule_dedup_wf import tick_parent

    handles = await asyncio.gather(
        *[
            DBOS.start_workflow_async(tick_parent, scheduled_time, run_id)
            for scheduled_time in tick_times
        ]
    )
    await asyncio.gather(*[handle.get_result() for handle in handles])
    return list(handles)


def scenario_schedule_dedup(root_seed: int) -> None:
    from dbos import DBOS
    from dbos_workload_common import dbos_config

    init_schema()
    run_id = f"sched-{workload_seed_raw()[:12]}"
    tick_times = [f"tick-{index}-{run_id}" for index in range(TICK_COUNT)]

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.launch()
    DBOS.register_queue(
        QUEUE_NAME,
        concurrency=2,
        worker_concurrency=2,
        polling_interval_sec=0.05,
    )
    from dbos_schedule_dedup_wf import tick_child, tick_parent  # noqa: F401

    progress("schedule_ticks_start", f"run_id={run_id} ticks={TICK_COUNT}")
    handles = asyncio.run(_run_parent_ticks(tick_times, run_id))
    parent_ok = all(
        DBOS.retrieve_workflow(handle.workflow_id).get_status().status == "SUCCESS"
        for handle in handles
    )
    results = [
        DBOS.retrieve_workflow(handle.workflow_id).get_result() for handle in handles
    ]

    dedup_id = f"{run_id}-schedule-dedup"
    live_children = int(
        sql_scalar(
            f"""
            SELECT COUNT(*) FROM dbos.workflow_status
            WHERE deduplication_id = '{dedup_id}'
              AND status IN ('PENDING', 'ENQUEUED');
            """,
            database=SYS_DB,
        )
    )
    success_children = int(
        sql_scalar(
            f"""
            SELECT COUNT(*) FROM dbos.workflow_status
            WHERE deduplication_id = '{dedup_id}'
              AND status = 'SUCCESS';
            """,
            database=SYS_DB,
        )
    )
    completions = int(
        sql_scalar(f"SELECT COUNT(*) FROM schedule_completions WHERE run_id = '{run_id}';")
    )

    invariant(
        "P718",
        "parent_ticks_success",
        parent_ok,
        f"results={results}",
    )
    invariant(
        "P718",
        "bounded_child_completions",
        completions <= TICK_COUNT and success_children <= TICK_COUNT,
        f"completions={completions} success_children={success_children} ticks={TICK_COUNT}",
    )
    invariant(
        "P718",
        "at_most_one_live_child",
        live_children <= 1,
        f"live_children={live_children} dedup_id={dedup_id}",
    )

    DBOS.destroy(destroy_registry=True)
    progress("scenario_done", f"run_id={run_id} completions={completions}")


def main() -> int:
    return workload_main("dbos_schedule_dedup", scenario_schedule_dedup)


if __name__ == "__main__":
    raise SystemExit(main())
