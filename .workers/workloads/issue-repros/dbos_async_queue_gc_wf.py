# SURFACE: Async queue dequeue GC soak (P710)
# MODELS:  dequeued async workflow suspended on frame-local asyncio.Future
# ORACLE:  terminal SUCCESS; no "Task was destroyed but it is pending" in logs
# ISSUES:  #710
# VARIANCE: job_count and suspend_ms from WORKLOAD_SEED

from __future__ import annotations

import asyncio

from dbos_workload_common import dbos_config  # noqa: F401 — vendor PYTHONPATH

from dbos import DBOS


@DBOS.dbos_class()
class GcSoakWF:
    @staticmethod
    @DBOS.workflow()
    async def gc_soak_job(job_id: str, suspend_ms: int) -> str:
        """Mimics #710 repro: await a future rooted only in this coroutine frame."""
        loop = asyncio.get_running_loop()
        frame_future = loop.create_future()

        async def release_after_delay() -> None:
            await asyncio.sleep(suspend_ms / 1000.0)
            if not frame_future.done():
                frame_future.set_result(True)

        releaser = asyncio.create_task(release_after_delay())
        try:
            await frame_future
        finally:
            await asyncio.sleep(0)
            if not releaser.done():
                releaser.cancel()
        return job_id
