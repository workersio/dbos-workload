#!/usr/bin/env python3
"""
P714: patch_async concurrency under asyncio.gather (#714).

Concurrent patch_async calls inside one async workflow race checkpoint
reservation (to_thread off-loop). Losers raise DBOSWorkflowConflictIDError;
if that escapes, persist() polls await_workflow_result forever.

# SURFACE: patch_async + asyncio.gather
# MODELS:  gather of tasks each calling patch_async then async steps
# ORACLE:  bounded_terminal; no_persist_poll_storm
# ISSUES:  #714
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid

from dbos_workload_common import (
    SYS_DB,
    invariant,
    progress,
    psql,
    seed_int,
    vendor_ready,
    workload_main,
    workload_seed_raw,
)

from dbos import DBOS, SetWorkflowID
from dbos._error import DBOSWorkflowConflictIDError
from dbos._sys_db import WorkflowStatusString

MAX_RECOVERY_ATTEMPTS = int(os.environ.get("PATCH_ASYNC_MAX_RECOVERY_ATTEMPTS", "2"))
WALL_BUDGET_SEC = float(os.environ.get("PATCH_ASYNC_WALL_BUDGET_SEC", "180"))


def build_scenario(root_seed: int) -> dict[str, object]:
    task_count = 3 + (root_seed % 2)
    steps_per_task = 3 + (root_seed % 5)
    tags = [chr(ord("a") + index) for index in range(task_count)]
    return {
        "task_tags": tags,
        "steps_per_task": steps_per_task,
        "workflow_id": f"patch-gather-{workload_seed_raw()[:16]}-{uuid.uuid4().hex[:8]}",
    }


def sql_scalar(sql: str, database: str = SYS_DB) -> str:
    return psql(sql, database=database).splitlines()[-1].strip()


def assert_no_duplicate_checkpoints(workflow_id: str) -> None:
    dupes = int(
        sql_scalar(
            "SELECT COUNT(*) FROM ("
            "SELECT function_id FROM dbos.operation_outputs "
            f"WHERE workflow_uuid = '{workflow_id}' "
            "GROUP BY function_id HAVING COUNT(*) > 1"
            ") AS dupes;"
        )
    )
    invariant(
        "P714a",
        "no_duplicate_function_ids",
        dupes == 0,
        f"duplicate_function_id_groups={dupes}",
    )


def assert_sysdb_terminal(workflow_id: str) -> tuple[str, int]:
    status = sql_scalar(
        "SELECT status FROM dbos.workflow_status "
        f"WHERE workflow_uuid = '{workflow_id}';"
    )
    recovery_attempts = int(
        sql_scalar(
            "SELECT COALESCE(recovery_attempts, 0) FROM dbos.workflow_status "
            f"WHERE workflow_uuid = '{workflow_id}';"
        )
        or "0"
    )
    return status, recovery_attempts


async def run_parallel_patch_gather(scenario: dict[str, object]) -> None:
    from dbos_workload_common import dbos_config

    config = dbos_config()
    config["enable_patching"] = True

    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.launch()

    from dbos_patch_async_wf import parallel_patch_gather_wf

    task_tags = scenario["task_tags"]
    steps_per_task = int(scenario["steps_per_task"])
    workflow_id = str(scenario["workflow_id"])

    progress(
        "patch_gather_start",
        f"workflow_id={workflow_id} tasks={len(task_tags)} steps={steps_per_task}",
    )

    started = time.monotonic()
    error_summary = ""
    timed_out = False
    result: list[str] | None = None
    handle = None

    try:
        with SetWorkflowID(workflow_id):
            handle = await DBOS.start_workflow_async(
                parallel_patch_gather_wf, task_tags, steps_per_task
            )
            result = await asyncio.wait_for(
                handle.get_result(), timeout=WALL_BUDGET_SEC
            )
    except asyncio.TimeoutError:
        timed_out = True
        error_summary = (
            f"get_result timed out after {WALL_BUDGET_SEC}s "
            "(likely persist() zombie poll after ConflictIDError)"
        )
    except DBOSWorkflowConflictIDError as exc:
        error_summary = str(exc)
    except BaseException as exc:
        error_summary = f"{type(exc).__name__}: {exc}"

    elapsed = time.monotonic() - started
    progress("patch_gather_done", f"elapsed_sec={elapsed:.2f} timed_out={timed_out}")

    sys_status, recovery_attempts = assert_sysdb_terminal(workflow_id)
    progress(
        "patch_gather_status",
        f"status={sys_status} recovery_attempts={recovery_attempts}",
    )

    invariant(
        "bounded_terminal",
        "workflow_terminal_within_budget",
        not timed_out
        and error_summary == ""
        and sys_status == WorkflowStatusString.SUCCESS.value,
        error_summary
        or f"status={sys_status} elapsed_sec={elapsed:.2f} timed_out={timed_out}",
    )

    invariant(
        "no_persist_poll_storm",
        "recovery_attempts_bounded",
        recovery_attempts <= MAX_RECOVERY_ATTEMPTS,
        f"recovery_attempts={recovery_attempts} max={MAX_RECOVERY_ATTEMPTS}",
    )

    if result is not None and error_summary == "" and not timed_out:
        invariant(
            "P714b",
            "gather_result_complete",
            sorted(result) == sorted(task_tags),
            f"expected={task_tags} got={result}",
        )

    assert_no_duplicate_checkpoints(workflow_id)

    DBOS.destroy(destroy_registry=True)


def scenario_patch_async_gather(root_seed: int) -> None:
    scenario = build_scenario(root_seed)
    asyncio.run(run_parallel_patch_gather(scenario))


def main() -> int:
    return workload_main("dbos_patch_async_gather", scenario_patch_async_gather)


if __name__ == "__main__":
    raise SystemExit(main())
