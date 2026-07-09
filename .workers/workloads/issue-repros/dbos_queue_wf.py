# SURFACE: Multi-worker queue (P2)
# MODELS:  queued job workflow, two-step commit, crash mid-job or after complete
# ORACLE:  processed_jobs, step_runs (via parent SQL checks)
# ISSUES:  #546, #541, #453
# VARIANCE: crash_at_step per job from WORKLOAD_SEED / WENV_SEED

from __future__ import annotations

import os

import sqlalchemy as sa
from dbos import DBOS


@DBOS.dbos_class()
class JobWF:
    @staticmethod
    @DBOS.transaction()
    def begin_job(job_id: str) -> None:
        executor = os.environ.get("DBOS__VMID", "local")
        session = DBOS.sql_session
        session.execute(
            sa.text(
                "INSERT INTO step_runs(job_id, step_name, executor) "
                "VALUES (:job_id, 'begin', :executor)"
            ),
            {"job_id": job_id, "executor": executor},
        )
        session.execute(
            sa.text(
                "INSERT INTO job_state(job_id, state) VALUES (:job_id, 'inflight') "
                "ON CONFLICT (job_id) DO NOTHING"
            ),
            {"job_id": job_id},
        )

    @staticmethod
    @DBOS.transaction()
    def complete_job(job_id: str) -> None:
        executor = os.environ.get("DBOS__VMID", "local")
        session = DBOS.sql_session
        session.execute(
            sa.text(
                "INSERT INTO step_runs(job_id, step_name, executor) "
                "VALUES (:job_id, 'complete', :executor)"
            ),
            {"job_id": job_id, "executor": executor},
        )
        session.execute(
            sa.text(
                "INSERT INTO processed_jobs(job_id, executor) VALUES (:job_id, :executor) "
                "ON CONFLICT (job_id) DO NOTHING"
            ),
            {"job_id": job_id, "executor": executor},
        )
        session.execute(
            sa.text("UPDATE job_state SET state = 'done' WHERE job_id = :job_id"),
            {"job_id": job_id},
        )

    @staticmethod
    @DBOS.workflow()
    def process_job(job_id: str, crash_at_step: int) -> str:
        JobWF.begin_job(job_id)
        if crash_at_step == 1 and os.environ.get("DBOS_CRASH_NOW") == "1":
            os._exit(99)
        JobWF.complete_job(job_id)
        if crash_at_step == 2 and os.environ.get("DBOS_CRASH_NOW") == "1":
            os._exit(99)
        return job_id
