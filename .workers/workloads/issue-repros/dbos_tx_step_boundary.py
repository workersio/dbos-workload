#!/usr/bin/env python3
"""
P2tx: transaction-step boundary — async steps + transactions + gather + crash.

# SURFACE: Workflow transaction rules, async steps, DB session lifecycle
# MODELS:  queued async workflow interleaves async @DBOS.step and @DBOS.transaction;
#          worker-a crashes at seed-chosen boundary; worker-b recovers
# ORACLE:  T1 terminal_success, T2 tag_writes_bounded, T3 counter_exactly_once,
#          T4 recovery_bounded, T5 no_session_leak_error
# ISSUES:  duplicate app writes, assertion leaks across retry
# VARIANCE: crash_point after_async_pre|after_tx_pre|after_gather|after_tx_post
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
    workload_main,
    workload_seed_raw,
)

QUEUE_NAME = "formal_tx_boundary"
WORKER_A = "worker-a"
WORKER_B = "worker-b"
NO_DEQUEUE = "__formal_no_dequeue__"
MAX_RECOVERY_ATTEMPTS = 8
MAX_TAG_EXECUTIONS = 4

META_PATH = RUN_DIR / "meta.json"

QUEUE_OPTS = {
    "concurrency": 2,
    "worker_concurrency": 2,
    "polling_interval_sec": 0.05,
}

CRASH_POINTS = (
    "after_async_pre",
    "after_tx_pre",
    "after_gather",
    "after_tx_post",
)


def init_app_schema() -> None:
    psql(
        """
        CREATE TABLE IF NOT EXISTS boundary_audit(
          id SERIAL PRIMARY KEY,
          run_id TEXT NOT NULL,
          tag TEXT NOT NULL,
          kind TEXT NOT NULL,
          executor TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS boundary_counter(
          run_id TEXT NOT NULL,
          tag TEXT NOT NULL,
          n INTEGER NOT NULL,
          PRIMARY KEY (run_id, tag)
        );
        """,
        database=APP_DB,
    )


def reset_app_tables() -> None:
    psql(
        "TRUNCATE boundary_audit, boundary_counter RESTART IDENTITY;",
        database=APP_DB,
    )


def build_scenario(root_seed: int) -> dict[str, str]:
    return {
        "run_id": f"tx-boundary-{workload_seed_raw()[:16]}",
        "crash_point": CRASH_POINTS[root_seed % len(CRASH_POINTS)],
        "workflow_id": "",
    }


def register_queue(dbos) -> None:
    dbos.register_queue(
        QUEUE_NAME,
        concurrency=QUEUE_OPTS["concurrency"],
        worker_concurrency=QUEUE_OPTS["worker_concurrency"],
        polling_interval_sec=QUEUE_OPTS["polling_interval_sec"],
    )


def sql_scalar(sql: str) -> str:
    return psql(sql, database=APP_DB).splitlines()[-1].strip()


def run_phase_enqueue(run_id: str, crash_point: str) -> str:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_tx_step_boundary_wf import TxBoundaryWF

    handle = DBOS.enqueue_workflow(
        QUEUE_NAME, TxBoundaryWF.boundary_workflow, run_id, crash_point
    )
    workflow_id = handle.workflow_id
    progress("enqueued", f"workflow_id={workflow_id} crash_point={crash_point}")
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

    from dbos_tx_step_boundary_wf import TxBoundaryWF  # noqa: F401

    os.environ["DBOS_CRASH_NOW"] = "1"
    if not threading.Event().wait(timeout=240):
        os._exit(1)


def assert_oracles(
    dbos, workflow_id: str, run_id: str, crash_point: str
) -> None:
    from dbos._sys_db import WorkflowStatusString

    handle = dbos.retrieve_workflow(workflow_id)
    status = handle.get_status()
    wf_status = status.status
    recovery_attempts = status.recovery_attempts or 0
    error = status.error or ""

    invariant(
        "T1",
        "workflow_terminal_success",
        wf_status == WorkflowStatusString.SUCCESS.value,
        f"workflow_id={workflow_id} status={wf_status} error={error[:200]}",
    )
    invariant(
        "T4",
        "recovery_attempts_bounded",
        recovery_attempts <= MAX_RECOVERY_ATTEMPTS,
        f"recovery_attempts={recovery_attempts} max={MAX_RECOVERY_ATTEMPTS}",
    )
    invariant(
        "T5",
        "no_session_leak_error",
        not error
        or (
            "session" not in error.lower()
            and "illegalstate" not in error.lower()
        ),
        f"error={error[:300]}",
    )

    expected_tags = (
        "async_pre",
        "tx_pre",
        "async_gather",
        "tx_gather",
        "tx_post",
        "async_post",
    )
    for tag in expected_tags:
        count = int(
            sql_scalar(
                f"SELECT COUNT(*) FROM boundary_audit "
                f"WHERE run_id = '{run_id.replace(chr(39), chr(39)+chr(39))}' "
                f"AND tag = '{tag}';"
            )
        )
        invariant(
            "T2",
            f"{tag}_writes_bounded",
            1 <= count <= MAX_TAG_EXECUTIONS,
            f"tag={tag} audit_rows={count} crash_point={crash_point}",
        )

    for tag in ("tx_pre", "tx_gather", "tx_post"):
        counter = int(
            sql_scalar(
                f"SELECT COALESCE(MAX(n), 0) FROM boundary_counter "
                f"WHERE run_id = '{run_id.replace(chr(39), chr(39)+chr(39))}' "
                f"AND tag = '{tag}';"
            )
        )
        invariant(
            "T3",
            f"{tag}_counter_exactly_once",
            counter == 1,
            f"tag={tag} counter_n={counter} expected=1",
        )

    pending = dbos.list_workflows(
        status=WorkflowStatusString.PENDING.value,
        queue_name=QUEUE_NAME,
    )
    enqueued = dbos.list_workflows(
        status=WorkflowStatusString.ENQUEUED.value,
        queue_name=QUEUE_NAME,
    )
    invariant(
        "T6",
        "no_stuck_queue_rows",
        len(pending) == 0 and len(enqueued) == 0,
        f"pending={len(pending)} enqueued={len(enqueued)}",
    )


def run_phase_worker_b(workflow_id: str, run_id: str, crash_point: str) -> None:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([QUEUE_NAME])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_tx_step_boundary_wf import TxBoundaryWF  # noqa: F401

    progress("recover_worker_a")
    DBOS._recover_pending_workflows([WORKER_A])

    handle = DBOS.retrieve_workflow(workflow_id)
    result = handle.get_result()
    progress("workflow_result", f"result={result}")

    assert_oracles(DBOS, workflow_id, run_id, crash_point)
    DBOS.destroy(destroy_registry=True)


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


def scenario_tx_boundary(root_seed: int) -> None:
    meta = build_scenario(root_seed)
    META_PATH.write_text(json.dumps(meta))

    progress("schema_init")
    init_app_schema()
    reset_app_tables()

    base_env = {"WORKLOAD_SEED": workload_seed_raw()}
    enqueue = _run_subphase("enqueue", base_env)
    if enqueue.returncode != 0:
        raise RuntimeError(f"enqueue failed rc={enqueue.returncode}")

    meta = json.loads(META_PATH.read_text())
    workflow_id = str(meta.get("workflow_id") or "")
    if not workflow_id:
        raise RuntimeError("missing workflow_id after enqueue")

    worker_a = _run_subphase("worker_a", {**base_env, "DBOS__VMID": WORKER_A})
    if worker_a.returncode != 99:
        raise RuntimeError(f"worker_a expected rc=99 got {worker_a.returncode}")

    worker_b = _run_subphase("worker_b", {**base_env, "DBOS__VMID": WORKER_B})
    if worker_b.returncode != 0:
        raise RuntimeError(f"worker_b failed rc={worker_b.returncode}")

    progress(
        "scenario_done",
        f"run_id={meta['run_id']} crash_point={meta['crash_point']}",
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
        meta = json.loads(META_PATH.read_text())
        progress("dbos_enqueue_launch")
        workflow_id = run_phase_enqueue(meta["run_id"], meta["crash_point"])
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
        workflow_id = str(meta.get("workflow_id") or "")
        if not workflow_id:
            return 2
        progress("dbos_worker_b_launch")
        run_phase_worker_b(workflow_id, meta["run_id"], meta["crash_point"])
        return 0

    return workload_main("dbos_tx_step_boundary", scenario_tx_boundary)


if __name__ == "__main__":
    raise SystemExit(main())
