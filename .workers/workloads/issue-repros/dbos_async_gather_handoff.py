#!/usr/bin/env python3
"""
P4: async gather + two-worker recovery (#688).

# SURFACE: Async gather + handoff (P4)
# MODELS:  gather(step_a, step_b, step_c); worker-a crash mid-step; worker-b recovers
# ORACLE:  I7 success, I8 recovery bounded, I9 steps once each, I10 no unexpected_step storm
# ISSUES:  #688
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

QUEUE_NAME = "formal_gather"
WORKER_A = "worker-a"
WORKER_B = "worker-b"
NO_DEQUEUE = "__formal_no_dequeue__"
MAX_RECOVERY_ATTEMPTS = 8

META_PATH = RUN_DIR / "meta.json"

QUEUE_OPTS = {
    "concurrency": 2,
    "worker_concurrency": 2,
    "polling_interval_sec": 0.05,
}


def init_app_schema() -> None:
    psql(
        """
        CREATE TABLE IF NOT EXISTS gather_steps(
          id SERIAL PRIMARY KEY,
          run_id TEXT NOT NULL,
          step_name TEXT NOT NULL,
          executor TEXT NOT NULL
        );
        """,
        database=APP_DB,
    )


def reset_app_tables() -> None:
    psql("TRUNCATE gather_steps RESTART IDENTITY;", database=APP_DB)


def build_scenario(root_seed: int) -> dict[str, str]:
    crash_steps = ("step_a", "step_b", "step_c")
    crash_step = crash_steps[root_seed % 3]
    run_id = f"gather-{workload_seed_raw()[:16]}"
    return {"run_id": run_id, "crash_step": crash_step}


def register_queue(dbos) -> None:
    dbos.register_queue(
        QUEUE_NAME,
        concurrency=QUEUE_OPTS["concurrency"],
        worker_concurrency=QUEUE_OPTS["worker_concurrency"],
        polling_interval_sec=QUEUE_OPTS["polling_interval_sec"],
    )


def run_phase_enqueue(run_id: str, crash_step: str) -> str:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_async_gather_wf import GatherWF

    handle = DBOS.enqueue_workflow(
        QUEUE_NAME, GatherWF.gather_workflow, run_id, crash_step
    )
    workflow_id = handle.workflow_id
    progress("enqueued", f"workflow_id={workflow_id} crash_step={crash_step}")
    DBOS.destroy(destroy_registry=True)
    return workflow_id


def run_phase_worker_a() -> None:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([QUEUE_NAME])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_async_gather_wf import GatherWF  # noqa: F401

    os.environ["DBOS_CRASH_NOW"] = "1"
    # Stay alive for queue dequeue; bounded wait avoids hanging until VM timeout
    # if the workflow errors instead of hitting os._exit(99).
    if not threading.Event().wait(timeout=180):
        os._exit(1)


def run_phase_worker_b(workflow_id: str, run_id: str, crash_step: str) -> None:
    from dbos import DBOS
    from dbos._sys_db import WorkflowStatusString

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([QUEUE_NAME])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_async_gather_wf import GatherWF  # noqa: F401

    progress("recover_worker_a")
    DBOS._recover_pending_workflows([WORKER_A])

    handle = DBOS.retrieve_workflow(workflow_id)
    result = handle.get_result()
    status = handle.get_status()
    recovery_attempts = status.recovery_attempts or 0
    wf_status = status.status
    error = status.error or ""

    invariant(
        "I7",
        "workflow_terminal_success",
        wf_status == WorkflowStatusString.SUCCESS.value,
        f"workflow_id={workflow_id} status={wf_status} result={result} error={error[:200]}",
    )
    invariant(
        "I8",
        "recovery_attempts_bounded",
        recovery_attempts <= MAX_RECOVERY_ATTEMPTS,
        f"recovery_attempts={recovery_attempts} max={MAX_RECOVERY_ATTEMPTS}",
    )
    invariant(
        "I10",
        "no_unexpected_step_error",
        "DBOSUnexpectedStepError" not in (error or ""),
        f"error={error[:500]}",
    )

    if isinstance(result, dict):
        invariant(
            "I7b",
            "gather_result_complete",
            result.get("a") == "a" and result.get("b") == "b" and result.get("c") == "c",
            f"result={result}",
        )

    assert_step_invariants(run_id)
    assert_queue_invariants(DBOS, workflow_id)
    DBOS.destroy(destroy_registry=True)


def sql_scalar(sql: str) -> str:
    return psql(sql, database=APP_DB).splitlines()[-1].strip()


def assert_step_invariants(run_id: str) -> None:
    total = 0
    for step in ("step_a", "step_b", "step_c"):
        count = int(
            sql_scalar(
                f"SELECT COUNT(*) FROM gather_steps "
                f"WHERE run_id = '{run_id}' AND step_name = '{step}';"
            )
        )
        total += count
        invariant(
            "I9",
            f"{step}_execution_bounded",
            1 <= count <= 4,
            f"step={step} executions={count} (retry storm if >4)",
        )
    invariant(
        "I9b",
        "total_step_records_bounded",
        total <= 12,
        f"total_step_rows={total} (gather has 3 steps; storm if >>12)",
    )


def assert_queue_invariants(dbos, workflow_id: str) -> None:
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
        "no_stuck_queue_rows",
        len(pending) == 0 and len(enqueued) == 0,
        f"pending={len(pending)} enqueued={len(enqueued)}",
    )
    status = dbos.retrieve_workflow(workflow_id).get_status().status
    invariant(
        "I6b",
        "workflow_not_pending",
        status != WorkflowStatusString.PENDING.value,
        f"workflow_id={workflow_id} status={status}",
    )


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


def scenario_async_gather(root_seed: int) -> None:
    scenario = build_scenario(root_seed)
    run_id = scenario["run_id"]
    crash_step = scenario["crash_step"]

    META_PATH.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "crash_step": crash_step,
                "workflow_id": "",
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
    workflow_id = meta.get("workflow_id") or ""
    if not workflow_id:
        raise RuntimeError("missing workflow_id after enqueue")

    worker_a = _run_subphase("worker_a", {**base_env, "DBOS__VMID": WORKER_A})
    if worker_a.returncode != 99:
        raise RuntimeError(f"worker_a expected rc=99 got {worker_a.returncode}")

    worker_b = _run_subphase("worker_b", {**base_env, "DBOS__VMID": WORKER_B})
    if worker_b.returncode != 0:
        raise RuntimeError(f"worker_b failed rc={worker_b.returncode}")

    progress("scenario_done", f"run_id={run_id} crash_step={crash_step}")


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
        meta = json.loads(META_PATH.read_text())
        run_id = meta["run_id"]
        crash_step = meta["crash_step"]
        progress("dbos_enqueue_launch")
        workflow_id = run_phase_enqueue(run_id, crash_step)
        meta["workflow_id"] = workflow_id
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
        workflow_id = meta.get("workflow_id") or ""
        if not workflow_id:
            return 2
        progress("dbos_worker_b_launch")
        run_phase_worker_b(workflow_id, meta["run_id"], meta["crash_step"])
        return 0

    return workload_main("dbos_async_gather_handoff", scenario_async_gather)


if __name__ == "__main__":
    raise SystemExit(main())
