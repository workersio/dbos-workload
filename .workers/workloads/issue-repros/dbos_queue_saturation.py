#!/usr/bin/env python3
"""
P2q: long-running queue saturation — fill a concurrent queue, crash mid-drain, recover.

# SURFACE: Queue fairness, worker concurrency, backpressure
# MODELS:  worker-a/worker-b, concurrency=2, 18–26 multi-step jobs, crash mid saturation;
#          worker-b recovers worker-a and drains remaining jobs
# ORACLE:  Q1 all_jobs_done, Q2 no_inflight, Q3 no_stuck_queue_rows, Q4 bounded_work_ticks
# ISSUES:  #546, #508, starvation, capacity leak
# VARIANCE: job_count, work_steps, crash job/tick from WORKLOAD_SEED
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
from pathlib import Path

from dbos_workload_common import (
    APP_DB,
    RUN_DIR,
    dbos_config,
    invariant,
    progress,
    psql,
    subphase_env,
    workload_main,
    workload_seed_raw,
)

QUEUE_NAME = "formal_saturation"
WORKER_A = "worker-a"
WORKER_B = "worker-b"
NO_DEQUEUE = "__formal_no_dequeue__"
META_PATH = RUN_DIR / "meta.json"

QUEUE_OPTS = {
    "concurrency": 2,
    "worker_concurrency": 2,
    "polling_interval_sec": 0.05,
}


def init_app_schema() -> None:
    psql(
        """
        CREATE TABLE IF NOT EXISTS saturation_done(
          job_id TEXT PRIMARY KEY,
          executor TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS saturation_runs(
          id SERIAL PRIMARY KEY,
          job_id TEXT NOT NULL,
          step_name TEXT NOT NULL,
          executor TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS saturation_state(
          job_id TEXT PRIMARY KEY,
          state TEXT NOT NULL CHECK(state IN ('inflight', 'done'))
        );
        """,
        database=APP_DB,
    )


def reset_app_tables() -> None:
    psql(
        "TRUNCATE saturation_runs, saturation_state, saturation_done RESTART IDENTITY;",
        database=APP_DB,
    )


def build_jobs(root_seed: int) -> tuple[list[dict[str, object]], int, int, str]:
    job_count = 18 + (root_seed % 9)
    work_steps = 3 + (root_seed % 2)
    crash_index = root_seed % job_count
    crash_at_tick = root_seed % work_steps
    jobs: list[dict[str, object]] = []
    for index in range(job_count):
        job_id = f"sat_{index:03d}"
        jobs.append(
            {
                "job_id": job_id,
                "work_steps": work_steps,
                "crash_at_tick": crash_at_tick if index == crash_index else -1,
            }
        )
    return jobs, job_count, work_steps, jobs[crash_index]["job_id"]


def register_queue(dbos) -> None:
    dbos.register_queue(
        QUEUE_NAME,
        concurrency=QUEUE_OPTS["concurrency"],
        worker_concurrency=QUEUE_OPTS["worker_concurrency"],
        polling_interval_sec=QUEUE_OPTS["polling_interval_sec"],
    )


def sql_scalar(sql: str, database: str = APP_DB) -> str:
    return psql(sql, database=database).splitlines()[-1].strip()


def run_phase_enqueue(jobs: list[dict[str, object]]) -> list[str]:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_queue_saturation_wf import SaturationWF

    workflow_ids: list[str] = []
    for job in jobs:
        handle = DBOS.enqueue_workflow(
            QUEUE_NAME,
            SaturationWF.long_job,
            str(job["job_id"]),
            int(job["work_steps"]),
            int(job["crash_at_tick"]),
        )
        workflow_ids.append(handle.workflow_id)
        progress("enqueued", str(job["job_id"]))

    DBOS.destroy(destroy_registry=True)
    return workflow_ids


def run_phase_worker_a() -> None:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([QUEUE_NAME])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_queue_saturation_wf import SaturationWF  # noqa: F401

    os.environ["DBOS_CRASH_NOW"] = "1"
    if not threading.Event().wait(timeout=300):
        os._exit(1)


def run_phase_worker_b(workflow_ids: list[str], job_count: int, work_steps: int) -> None:
    from dbos import DBOS
    from dbos._sys_db import WorkflowStatusString

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([QUEUE_NAME])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_queue_saturation_wf import SaturationWF  # noqa: F401

    progress("recover_worker_a")
    DBOS._recover_pending_workflows([WORKER_A])

    for workflow_id in workflow_ids:
        handle = DBOS.retrieve_workflow(workflow_id)
        result = handle.get_result()
        status = handle.get_status().status
        invariant(
            "Q1b",
            "workflow_terminal_success",
            status == WorkflowStatusString.SUCCESS.value,
            f"workflow_id={workflow_id} status={status} result={result}",
        )

    pending = DBOS.list_workflows(
        status=WorkflowStatusString.PENDING.value,
        queue_name=QUEUE_NAME,
    )
    enqueued = DBOS.list_workflows(
        status=WorkflowStatusString.ENQUEUED.value,
        queue_name=QUEUE_NAME,
    )
    invariant(
        "Q3",
        "no_stuck_queue_rows",
        len(pending) == 0 and len(enqueued) == 0,
        f"pending={len(pending)} enqueued={len(enqueued)}",
    )
    DBOS.destroy(destroy_registry=True)

    done_jobs = int(sql_scalar("SELECT COUNT(*) FROM saturation_done;"))
    inflight = int(
        sql_scalar("SELECT COUNT(*) FROM saturation_state WHERE state = 'inflight';")
    )
    invariant(
        "Q1",
        "all_jobs_done",
        done_jobs == job_count,
        f"saturation_done={done_jobs} expected={job_count} inflight={inflight}",
    )
    invariant(
        "Q2",
        "no_inflight",
        inflight == 0,
        f"inflight={inflight}",
    )
    max_ticks = int(
        sql_scalar(
            "SELECT COALESCE(MAX(c), 0) FROM ("
            "  SELECT COUNT(*) AS c FROM saturation_runs "
            "  WHERE step_name LIKE 'work_%' GROUP BY job_id"
            ") s;"
        )
    )
    invariant(
        "Q4",
        "bounded_work_ticks",
        max_ticks <= work_steps,
        f"max_work_ticks={max_ticks} expected<={work_steps}",
    )


def _run_subphase(phase: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    progress(f"subphase_{phase}_start")
    proc = subprocess.Popen(
        [sys.executable, __file__, "--phase", phase],
        env=subphase_env(__file__, env),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    captured: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        captured.append(line)
        print(line, end="", flush=True)
    rc = proc.wait()
    progress(f"subphase_{phase}_done", f"rc={rc}")
    return subprocess.CompletedProcess(
        args=[__file__, "--phase", phase],
        returncode=rc,
        stdout="".join(captured),
        stderr="",
    )


def scenario_queue_saturation(root_seed: int) -> None:
    jobs, job_count, work_steps, crash_job_id = build_jobs(root_seed)
    (RUN_DIR / "jobs.json").write_text(json.dumps(jobs))
    META_PATH.write_text(
        json.dumps(
            {
                "job_count": job_count,
                "work_steps": work_steps,
                "crash_job_id": crash_job_id,
                "workflow_ids": [],
            }
        )
    )

    progress("schema_init")
    init_app_schema()
    reset_app_tables()

    base_env = {"WORKLOAD_SEED": workload_seed_raw()}
    enqueue = _run_subphase("enqueue", base_env)
    if enqueue.returncode != 0:
        raise RuntimeError(f"enqueue failed rc={enqueue.returncode}")

    meta = json.loads(META_PATH.read_text())
    workflow_ids = meta.get("workflow_ids") or []
    if len(workflow_ids) != job_count:
        raise RuntimeError(
            f"enqueue produced {len(workflow_ids)} workflow ids, expected {job_count}"
        )

    worker_a = _run_subphase("worker_a", {**base_env, "DBOS__VMID": WORKER_A})
    if worker_a.returncode != 99:
        raise RuntimeError(f"worker_a expected rc=99 got {worker_a.returncode}")

    worker_b = _run_subphase("worker_b", {**base_env, "DBOS__VMID": WORKER_B})
    if worker_b.returncode != 0:
        raise RuntimeError(f"worker_b failed rc={worker_b.returncode}")

    progress(
        "scenario_done",
        f"jobs={job_count} work_steps={work_steps} crash_job={crash_job_id}",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--phase",
        choices=["enqueue", "worker_a", "worker_b"],
        default="",
    )
    args = parser.parse_args()

    if args.phase == "enqueue":
        if not META_PATH.exists():
            return 2
        jobs = json.loads((RUN_DIR / "jobs.json").read_text())
        progress("dbos_enqueue_launch")
        workflow_ids = run_phase_enqueue(jobs)
        meta = json.loads(META_PATH.read_text())
        meta["workflow_ids"] = workflow_ids
        META_PATH.write_text(json.dumps(meta))
        return 0

    if args.phase == "worker_a":
        progress("dbos_worker_a_launch")
        run_phase_worker_a()
        return 0

    if args.phase == "worker_b":
        if not META_PATH.exists():
            return 2
        meta = json.loads(META_PATH.read_text())
        workflow_ids = meta.get("workflow_ids") or []
        job_count = int(meta["job_count"])
        work_steps = int(meta["work_steps"])
        progress("dbos_worker_b_launch")
        run_phase_worker_b(workflow_ids, job_count, work_steps)
        return 0

    return workload_main("dbos_queue_saturation", scenario_queue_saturation)


if __name__ == "__main__":
    raise SystemExit(main())
