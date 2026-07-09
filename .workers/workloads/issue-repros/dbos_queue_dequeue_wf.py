# SURFACE: Queue dequeue crash window (P9)
# MODELS:  queued job workflow; optional crash before any @DBOS.step
# ORACLE:  processed_jobs, step_runs (via parent SQL checks)
# ISSUES:  #546, #541 (stuck PENDING / double execution at dequeue boundary)
# VARIANCE: crash_before_step per job from parent scenario

from __future__ import annotations

import os

import sqlalchemy as sa
from dbos import DBOS


@DBOS.dbos_class()
class DequeueJobWF:
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
    def process_job(job_id: str, crash_before_step: bool) -> str:
        if crash_before_step and os.environ.get("DBOS_CRASH_NOW") == "1":
            progress_marker(job_id, "pre_step_exit")
            os._exit(99)
        DequeueJobWF.begin_job(job_id)
        DequeueJobWF.complete_job(job_id)
        return job_id


def progress_marker(job_id: str, stage: str) -> None:
    print(f"PROGRESS dequeue_wf job={job_id} stage={stage}", flush=True)
