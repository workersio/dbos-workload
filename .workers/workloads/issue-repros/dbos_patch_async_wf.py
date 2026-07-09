# SURFACE: patch_async concurrency under asyncio.gather (P714)
# MODELS:  parallel tasks each call DBOS.patch_async then async steps
# ORACLE:  bounded terminal status; recovery_attempts bounded
# ISSUES:  #714
# VARIANCE: steps_per_task from WORKLOAD_SEED

from __future__ import annotations

import asyncio

from dbos_workload_common import dbos_config  # noqa: F401 — sets vendor PYTHONPATH

from dbos import DBOS


@DBOS.step()
async def patch_small_step(tag: str, step_index: int) -> str:
    await asyncio.sleep(0)
    return f"{tag}:{step_index}"


async def patch_task(tag: str, steps_per_task: int) -> str:
    await DBOS.patch_async(f"patch-{tag}")
    for step_index in range(steps_per_task):
        await patch_small_step(tag, step_index)
        await asyncio.sleep(0)
    return tag


@DBOS.workflow()
async def parallel_patch_gather_wf(task_tags: list[str], steps_per_task: int) -> list[str]:
    return list(
        await asyncio.gather(*(patch_task(tag, steps_per_task) for tag in task_tags))
    )
