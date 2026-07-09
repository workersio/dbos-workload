#!/usr/bin/env python3
"""
P710: async queue dequeue + GC soak (#710).

Dequeued async workflows can be killed when cyclic GC collects the shielded
task because execute_workflow_by_id drops the WorkflowHandleAsyncTask ref.

# SURFACE: Queue + async dequeued workflow
# MODELS:  N jobs on a queue; executor gc.collect() under load
# ORACLE:  no_destroyed_pending_task; workflow_terminal_success
# ISSUES:  #710
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from dbos_workload_common import (
    RUN_DIR,
    invariant,
    progress,
    reset_run_dir,
    seed_int,
    vendor_ready,
    workload_main,
    workload_seed_raw,
)

QUEUE_NAME = "formal_gc_soak"
EXECUTOR = "gc-executor"
META_PATH = RUN_DIR / "meta.json"
SOAK_TIMEOUT_SEC = float(os.environ.get("P710_SOAK_TIMEOUT_SEC", "120"))
GC_PATTERNS = (
    "Task was destroyed but it is pending",
    "coroutine ignored GeneratorExit",
)

QUEUE_OPTS = {
    "concurrency": 4,
    "worker_concurrency": 4,
    "polling_interval_sec": 0.05,
}


def build_scenario(root_seed: int) -> dict[str, object]:
    job_count = 10 + (root_seed % 11)
    suspend_ms = 50 + (root_seed % 150)
    jobs = [
        {"job_id": f"gcjob_{index:03d}", "suspend_ms": suspend_ms + (index % 7) * 10}
        for index in range(job_count)
    ]
    return {"jobs": jobs, "job_count": job_count, "suspend_ms": suspend_ms}


def register_queue(dbos) -> None:
    dbos.register_queue(
        QUEUE_NAME,
        concurrency=QUEUE_OPTS["concurrency"],
        worker_concurrency=QUEUE_OPTS["worker_concurrency"],
        polling_interval_sec=QUEUE_OPTS["polling_interval_sec"],
    )


def run_phase_enqueue(jobs: list[dict[str, object]]) -> list[str]:
    from dbos import DBOS
    from dbos_workload_common import dbos_config

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues(["__formal_no_dequeue__"])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_async_queue_gc_wf import GcSoakWF

    workflow_ids: list[str] = []
    for job in jobs:
        job_id = str(job["job_id"])
        suspend_ms = int(job["suspend_ms"])
        handle = DBOS.enqueue_workflow(
            QUEUE_NAME, GcSoakWF.gc_soak_job, job_id, suspend_ms
        )
        workflow_ids.append(handle.workflow_id)
        progress("enqueued", f"job_id={job_id} workflow_id={handle.workflow_id}")

    DBOS.destroy(destroy_registry=True)
    return workflow_ids


def run_phase_executor(workflow_ids: list[str], job_count: int) -> None:
    from dbos import DBOS
    from dbos._sys_db import WorkflowStatusString
    from dbos_workload_common import dbos_config

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([QUEUE_NAME])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_async_queue_gc_wf import GcSoakWF  # noqa: F401

    progress("executor_start", f"jobs={job_count} soak_timeout_sec={SOAK_TIMEOUT_SEC}")
    deadline = time.monotonic() + SOAK_TIMEOUT_SEC
    while time.monotonic() < deadline:
        gc.collect()
        pending = 0
        for workflow_id in workflow_ids:
            status = DBOS.retrieve_workflow(workflow_id).get_status().status
            if status not in (
                WorkflowStatusString.SUCCESS.value,
                WorkflowStatusString.ERROR.value,
            ):
                pending += 1
        if pending == 0:
            break
        time.sleep(0.05)
    else:
        raise RuntimeError(
            f"executor soak timed out after {SOAK_TIMEOUT_SEC}s with jobs still pending"
        )

    errors: list[str] = []
    for workflow_id in workflow_ids:
        handle = DBOS.retrieve_workflow(workflow_id)
        status = handle.get_status()
        if status.status != WorkflowStatusString.SUCCESS.value:
            errors.append(
                f"{workflow_id} status={status.status} error={(status.error or '')[:200]}"
            )

    invariant(
        "G710",
        "workflow_terminal_success",
        not errors,
        "; ".join(errors) if errors else f"all {job_count} jobs SUCCESS",
    )

    DBOS.destroy(destroy_registry=True)


def scan_logs_for_gc_failure(captured: str) -> list[str]:
    hits = [pattern for pattern in GC_PATTERNS if pattern in captured]
    return hits


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


def scenario_async_queue_gc(root_seed: int) -> None:
    scenario = build_scenario(root_seed)
    jobs = scenario["jobs"]
    job_count = int(scenario["job_count"])
    META_PATH.write_text(json.dumps({"jobs": jobs, "workflow_ids": []}))

    base_env = {"WORKLOAD_SEED": workload_seed_raw(), "DBOS__VMID": EXECUTOR}
    enqueue = _run_subphase("enqueue", base_env)
    if enqueue.returncode != 0:
        raise RuntimeError(f"enqueue failed rc={enqueue.returncode}")

    meta = json.loads(META_PATH.read_text())
    workflow_ids = meta.get("workflow_ids") or []
    if len(workflow_ids) != job_count:
        raise RuntimeError(
            f"expected {job_count} workflow_ids after enqueue, got {len(workflow_ids)}"
        )

    executor = _run_subphase("executor", base_env)
    combined_logs = enqueue.stdout + executor.stdout
    gc_hits = scan_logs_for_gc_failure(combined_logs)
    invariant(
        "no_destroyed_pending_task",
        "no_gc_destroyed_pending_task_log",
        not gc_hits,
        f"matched={gc_hits}",
    )

    if executor.returncode != 0:
        raise RuntimeError(f"executor failed rc={executor.returncode}")

    progress("scenario_done", f"jobs={job_count} suspend_ms~={scenario['suspend_ms']}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["enqueue", "executor"], default="")
    args = parser.parse_args()

    if args.phase == "enqueue":
        if not META_PATH.exists():
            return 2
        meta = json.loads(META_PATH.read_text())
        jobs = meta["jobs"]
        progress("dbos_enqueue_launch")
        workflow_ids = run_phase_enqueue(jobs)
        meta["workflow_ids"] = workflow_ids
        META_PATH.write_text(json.dumps(meta))
        return 0

    if args.phase == "executor":
        if not META_PATH.exists():
            return 2
        meta = json.loads(META_PATH.read_text())
        workflow_ids = meta.get("workflow_ids") or []
        if not workflow_ids:
            return 2
        progress("dbos_executor_launch")
        run_phase_executor(workflow_ids, len(workflow_ids))
        return 0

    return workload_main("dbos_async_queue_gc", scenario_async_queue_gc)


if __name__ == "__main__":
    raise SystemExit(main())
