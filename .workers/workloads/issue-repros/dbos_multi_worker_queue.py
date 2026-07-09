#!/usr/bin/env python3
"""
P2: multi-worker queue — two executors, shared queue, crash + handoff.

# SURFACE: Multi-worker queue (P2 proper)
# MODELS:  worker-a/worker-b (DBOS__VMID), concurrency=2, 12–16 jobs, mid-job or post-job crash
# ORACLE:  I4 jobs_all_completed, I5 global_exactly_once, I5b max_one_begin, I6 no_stuck_pending
# ISSUES:  #546, #541, #453, #508
# VARIANCE: job_count, crash job index, crash mode (mid vs after-complete); WENV_SEED for schedule
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from dbos_workload_common import (
    APP_DB,
    RUN_DIR,
    InvariantFailure,
    dbos_config,
    invariant,
    progress,
    psql,
    reset_run_dir,
    seed_int,
    start_postgres,
    stop_postgres,
    vendor_ready,
    workload_main,
    workload_seed_raw,
)

QUEUE_NAME = "formal_jobs"
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
        CREATE TABLE IF NOT EXISTS processed_jobs(
          job_id TEXT PRIMARY KEY,
          executor TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS step_runs(
          id SERIAL PRIMARY KEY,
          job_id TEXT NOT NULL,
          step_name TEXT NOT NULL,
          executor TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS job_state(
          job_id TEXT PRIMARY KEY,
          state TEXT NOT NULL CHECK(state IN ('inflight', 'done'))
        );
        """,
        database=APP_DB,
    )


def reset_app_tables() -> None:
    psql(
        "TRUNCATE step_runs, job_state, processed_jobs RESTART IDENTITY;",
        database=APP_DB,
    )


def build_jobs(root_seed: int) -> tuple[list[dict[str, object]], int, str, str]:
    job_count = 12 + (root_seed % 5)
    crash_index = root_seed % job_count
    crash_mode = "mid_job" if (root_seed % 2) == 0 else "after_complete"
    crash_at_step = 1 if crash_mode == "mid_job" else 2

    jobs: list[dict[str, object]] = []
    for i in range(job_count):
        job_id = f"job_{i:03d}"
        jobs.append(
            {
                "job_id": job_id,
                "crash_at_step": crash_at_step if i == crash_index else 0,
            }
        )
    return jobs, job_count, crash_mode, jobs[crash_index]["job_id"]


def register_queue(dbos) -> None:
    dbos.register_queue(
        QUEUE_NAME,
        concurrency=QUEUE_OPTS["concurrency"],
        worker_concurrency=QUEUE_OPTS["worker_concurrency"],
        polling_interval_sec=QUEUE_OPTS["polling_interval_sec"],
    )


def run_phase_enqueue(jobs: list[dict[str, object]]) -> list[str]:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_queue_wf import JobWF

    workflow_ids: list[str] = []
    for job in jobs:
        job_id = str(job["job_id"])
        crash_at = int(job["crash_at_step"])
        handle = DBOS.enqueue_workflow(QUEUE_NAME, JobWF.process_job, job_id, crash_at)
        workflow_ids.append(handle.workflow_id)
        progress("enqueued", job_id)

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

    from dbos_queue_wf import JobWF  # noqa: F401

    os.environ["DBOS_CRASH_NOW"] = "1"
    # Block until a dequeued workflow calls os._exit(99).
    import threading

    threading.Event().wait()


def run_phase_worker_b(workflow_ids: list[str], job_count: int) -> None:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([QUEUE_NAME])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_queue_wf import JobWF  # noqa: F401

    progress("recover_worker_a")
    DBOS._recover_pending_workflows([WORKER_A])

    progress("await_results", f"count={len(workflow_ids)}")
    for wfid in workflow_ids:
        handle = DBOS.retrieve_workflow(wfid)
        result = handle.get_result()
        status = handle.get_status().status
        if status != "SUCCESS":
            raise RuntimeError(
                f"workflow {wfid} status={status} result={result} (expected SUCCESS)"
            )

    assert_queue_invariants(DBOS, workflow_ids, job_count)
    DBOS.destroy(destroy_registry=True)


def sql_scalar(sql: str) -> str:
    return psql(sql, database=APP_DB).splitlines()[-1].strip()


def assert_sql_invariants(job_count: int) -> None:
    done_jobs = sql_scalar("SELECT COUNT(*) FROM processed_jobs;")
    inflight = sql_scalar("SELECT COUNT(*) FROM job_state WHERE state = 'inflight';")
    duplicate_begins = sql_scalar(
        "SELECT COUNT(*) FROM ("
        "  SELECT job_id FROM step_runs WHERE step_name = 'begin' "
        "  GROUP BY job_id HAVING COUNT(*) > 1"
        ") d;"
    )
    max_begins = sql_scalar(
        "SELECT COALESCE(MAX(c), 0) FROM ("
        "  SELECT COUNT(*) AS c FROM step_runs WHERE step_name = 'begin' "
        "  GROUP BY job_id"
        ") s;"
    )

    invariant(
        "I4",
        "jobs_all_completed",
        done_jobs == str(job_count),
        f"processed_jobs={done_jobs} expected={job_count} inflight={inflight}",
    )
    invariant(
        "I5",
        "global_exactly_once",
        done_jobs == str(job_count) and inflight == "0",
        f"processed_jobs={done_jobs} inflight={inflight}",
    )
    invariant(
        "I5b",
        "max_one_begin_per_job",
        duplicate_begins == "0" and int(max_begins) <= 1,
        f"duplicate_begin_jobs={duplicate_begins} max_begins={max_begins}",
    )


def assert_queue_invariants(dbos, workflow_ids: list[str], job_count: int) -> None:
    from dbos._sys_db import WorkflowStatusString

    pending = dbos.list_workflows(
        status=WorkflowStatusString.PENDING.value,
        queue_name=QUEUE_NAME,
    )
    enqueued = dbos.list_workflows(
        status=WorkflowStatusString.ENQUEUED.value,
        queue_name=QUEUE_NAME,
    )
    invariant(
        "I6",
        "no_stuck_pending",
        len(pending) == 0 and len(enqueued) == 0,
        f"pending={len(pending)} enqueued={len(enqueued)}",
    )

    for wfid in workflow_ids:
        status = dbos.retrieve_workflow(wfid).get_status().status
        invariant(
            "I6b",
            "workflow_terminal_success",
            status == WorkflowStatusString.SUCCESS.value,
            f"workflow_id={wfid} status={status}",
        )

    assert_sql_invariants(job_count)


def _run_subphase(phase: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        **env,
        "DBOS_WORKLOAD_RUN_DIR": str(RUN_DIR.parent),
        "PYTHONPATH": os.pathsep.join(
            [
                str(Path(__file__).resolve().parent),
                str(Path(__file__).resolve().parents[2] / ".workers" / "vendor" / "py"),
            ]
        ),
    }
    progress(f"subphase_{phase}_start")
    proc = subprocess.Popen(
        [sys.executable, __file__, "--phase", phase],
        env=env,
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


def scenario_multi_worker(root_seed: int) -> None:
    jobs, job_count, crash_mode, crash_job_id = build_jobs(root_seed)

    (RUN_DIR / "jobs.json").write_text(json.dumps(jobs))
    META_PATH.write_text(
        json.dumps(
            {
                "job_count": job_count,
                "crash_mode": crash_mode,
                "crash_job_id": crash_job_id,
                "queue_name": QUEUE_NAME,
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
        raise RuntimeError(f"enqueue phase failed rc={enqueue.returncode}")

    meta = json.loads(META_PATH.read_text())
    workflow_ids = meta.get("workflow_ids") or []
    if len(workflow_ids) != job_count:
        raise RuntimeError(
            f"enqueue produced {len(workflow_ids)} workflow ids, expected {job_count}"
        )

    worker_a = _run_subphase("worker_a", {**base_env, "DBOS__VMID": WORKER_A})
    if worker_a.returncode != 99:
        detail = (worker_a.stdout or "")[-2000:]
        raise RuntimeError(
            f"worker_a expected rc=99 (crash), got {worker_a.returncode}; tail={detail}"
        )

    worker_b = _run_subphase("worker_b", {**base_env, "DBOS__VMID": WORKER_B})
    if worker_b.returncode != 0:
        raise RuntimeError(f"worker_b failed rc={worker_b.returncode}")

    assert_sql_invariants(job_count)
    progress(
        "scenario_done",
        f"jobs={job_count} crash_mode={crash_mode} crash_job={crash_job_id}",
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
            print("missing meta.json", file=sys.stderr)
            return 2
        meta = json.loads(META_PATH.read_text())
        jobs_path = RUN_DIR / "jobs.json"
        if not jobs_path.exists():
            print("missing jobs.json", file=sys.stderr)
            return 2
        jobs = json.loads(jobs_path.read_text())
        job_count = int(meta["job_count"])
        progress("dbos_enqueue_launch")
        workflow_ids = run_phase_enqueue(jobs)
        meta["workflow_ids"] = workflow_ids
        META_PATH.write_text(json.dumps(meta))
        (RUN_DIR / "jobs.json").write_text(json.dumps(jobs))
        return 0

    if args.phase == "worker_a":
        progress("dbos_worker_a_launch")
        try:
            run_phase_worker_a()
        except SystemExit as e:
            return int(e.code) if isinstance(e.code, int) else 1
        return 0

    if args.phase == "worker_b":
        if not META_PATH.exists():
            print("missing meta", file=sys.stderr)
            return 2
        meta = json.loads(META_PATH.read_text())
        workflow_ids = meta.get("workflow_ids") or []
        job_count = int(meta["job_count"])
        progress("dbos_worker_b_launch")
        run_phase_worker_b(workflow_ids, job_count)
        return 0

    return workload_main("dbos_multi_worker_queue", scenario_multi_worker)


if __name__ == "__main__":
    raise SystemExit(main())
