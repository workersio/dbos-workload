# SURFACE: Queue saturation (P2q)
# MODELS:  multi-step long jobs on a saturated queue, crash mid-drain, recover
# ORACLE:  processed_jobs / step_runs via parent SQL checks
# ISSUES:  #546, #508, starvation, capacity leak
# VARIANCE: job_count, crash index, work_steps from WORKLOAD_SEED

from __future__ import annotations

import os

import sqlalchemy as sa
from dbos import DBOS


@DBOS.dbos_class()
class SaturationWF:
    @staticmethod
    @DBOS.transaction()
    def begin_job(job_id: str) -> None:
        executor = os.environ.get("DBOS__VMID", "local")
        session = DBOS.sql_session
        session.execute(
            sa.text(
                "INSERT INTO saturation_runs(job_id, step_name, executor) "
                "VALUES (:job_id, 'begin', :executor)"
            ),
            {"job_id": job_id, "executor": executor},
        )
        session.execute(
            sa.text(
                "INSERT INTO saturation_state(job_id, state) VALUES (:job_id, 'inflight') "
                "ON CONFLICT (job_id) DO NOTHING"
            ),
            {"job_id": job_id},
        )

    @staticmethod
    @DBOS.transaction()
    def work_tick(job_id: str, tick: int) -> int:
        executor = os.environ.get("DBOS__VMID", "local")
        session = DBOS.sql_session
        session.execute(
            sa.text(
                "INSERT INTO saturation_runs(job_id, step_name, executor) "
                "VALUES (:job_id, :step_name, :executor)"
            ),
            {
                "job_id": job_id,
                "step_name": f"work_{tick}",
                "executor": executor,
            },
        )
        return tick

    @staticmethod
    @DBOS.transaction()
    def complete_job(job_id: str) -> None:
        executor = os.environ.get("DBOS__VMID", "local")
        session = DBOS.sql_session
        session.execute(
            sa.text(
                "INSERT INTO saturation_runs(job_id, step_name, executor) "
                "VALUES (:job_id, 'complete', :executor)"
            ),
            {"job_id": job_id, "executor": executor},
        )
        session.execute(
            sa.text(
                "INSERT INTO saturation_done(job_id, executor) VALUES (:job_id, :executor) "
                "ON CONFLICT (job_id) DO NOTHING"
            ),
            {"job_id": job_id, "executor": executor},
        )
        session.execute(
            sa.text("UPDATE saturation_state SET state = 'done' WHERE job_id = :job_id"),
            {"job_id": job_id},
        )

    @staticmethod
    @DBOS.workflow()
    def long_job(job_id: str, work_steps: int, crash_at_tick: int) -> str:
        SaturationWF.begin_job(job_id)
        for tick in range(work_steps):
            SaturationWF.work_tick(job_id, tick)
            if crash_at_tick == tick and os.environ.get("DBOS_CRASH_NOW") == "1":
                os._exit(99)
        SaturationWF.complete_job(job_id)
        return job_id
