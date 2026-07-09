#!/usr/bin/env python3
"""
P7f: stream / fork — write_stream under crash + fork_workflow stream copy.

# SURFACE: Stream / fork writers (Family 7)
# MODELS:  queued parent writes stream (workflow + step), worker-a crashes mid-stream;
#          worker-b recovers parent, forks from seed-chosen step, drains fork child
# ORACLE:  S1 parent_stream_complete, S2 fork_stream_complete, S3 no_duplicate_offsets,
#          S4 fork_prefix_before_rerun, S5 terminal_success, S1b/S2b terminal records,
#          F1 no_terminal_before_recovery
# ISSUES:  #577, #421
# VARIANCE: crash_after_writes (2|3), fork_step (3|4|5), stream_key
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
    SYS_DB,
    dbos_config,
    invariant,
    progress,
    psql,
    workload_main,
    workload_seed_raw,
)

QUEUE_NAME = "formal_stream_fork"
WORKER_A = "worker-a"
WORKER_B = "worker-b"
NO_DEQUEUE = "__formal_no_dequeue__"
MAX_RECOVERY_ATTEMPTS = 8

META_PATH = RUN_DIR / "meta.json"

QUEUE_OPTS = {
    "concurrency": 1,
    "worker_concurrency": 1,
    "polling_interval_sec": 0.05,
}


def init_app_schema() -> None:
    psql(
        """
        CREATE TABLE IF NOT EXISTS stream_results(
          id SERIAL PRIMARY KEY,
          run_id TEXT NOT NULL,
          workflow_id TEXT NOT NULL,
          stream_key TEXT NOT NULL,
          values_json TEXT NOT NULL,
          executor TEXT NOT NULL
        );
        """,
        database=APP_DB,
    )


def reset_app_tables() -> None:
    psql("TRUNCATE stream_results RESTART IDENTITY;", database=APP_DB)


def build_scenario(root_seed: int) -> dict[str, object]:
    run_id = f"stream-{workload_seed_raw()[:16]}"
    return {
        "run_id": run_id,
        "stream_key": f"key_{root_seed % 13}",
        "crash_after_writes": 2 + (root_seed % 2),
        "fork_step": 3 + (root_seed % 3),
        "workflow_id": "",
        "fork_workflow_id": "",
    }


def register_queue(dbos) -> None:
    dbos.register_queue(
        QUEUE_NAME,
        concurrency=QUEUE_OPTS["concurrency"],
        worker_concurrency=QUEUE_OPTS["worker_concurrency"],
        polling_interval_sec=QUEUE_OPTS["polling_interval_sec"],
    )


def sql_scalar(sql: str, database: str = APP_DB) -> str:
    return psql(sql, database=database).splitlines()[-1].strip()


def terminal_record_count(run_id: str) -> int:
    return int(
        sql_scalar(
            "SELECT COUNT(*) FROM stream_results "
            f"WHERE run_id = '{run_id.replace(chr(39), chr(39)+chr(39))}';"
        )
    )


def terminal_record_count_for_workflow(run_id: str, workflow_id: str) -> int:
    rid = run_id.replace("'", "''")
    wid = workflow_id.replace("'", "''")
    return int(
        sql_scalar(
            "SELECT COUNT(*) FROM stream_results "
            f"WHERE run_id = '{rid}' AND workflow_id = '{wid}';"
        )
    )


def expected_fork_prefix(fork_step: int) -> list[int]:
    """Copied stream rows use function_id < start_step (see test_fork_streams)."""
    if fork_step <= 2:
        return []
    return list(range(min(3, fork_step - 1)))


def duplicate_stream_offsets(workflow_ids: list[str]) -> int:
    if not workflow_ids:
        return 0
    quoted = ",".join(
        f"'{wid.replace(chr(39), chr(39)+chr(39))}'" for wid in workflow_ids
    )
    return int(
        sql_scalar(
            "SELECT COUNT(*) FROM ("
            '  SELECT workflow_uuid, key, "offset" FROM dbos.streams '
            f"  WHERE workflow_uuid IN ({quoted}) "
            '  GROUP BY workflow_uuid, key, "offset" HAVING COUNT(*) > 1'
            ") d;",
            database=SYS_DB,
        )
    )


def run_phase_enqueue(
    run_id: str, stream_key: str, crash_after_writes: int
) -> str:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_stream_fork_wf import StreamForkWF

    handle = DBOS.enqueue_workflow(
        QUEUE_NAME,
        StreamForkWF.stream_parent,
        run_id,
        stream_key,
        crash_after_writes,
    )
    workflow_id = handle.workflow_id
    progress("enqueued_stream_parent", f"workflow_id={workflow_id}")
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

    from dbos_stream_fork_wf import StreamForkWF  # noqa: F401

    os.environ["DBOS_CRASH_NOW"] = "1"
    if not threading.Event().wait(timeout=180):
        os._exit(1)


def read_stream_values(dbos, workflow_id: str, stream_key: str) -> list[int]:
    return list(dbos.read_stream(workflow_id, stream_key))


def read_stream_prefix(
    dbos, workflow_id: str, stream_key: str, count: int
) -> list[int]:
    """Read the first `count` stream values (DBOS test_fork_streams pattern)."""
    if count <= 0:
        return []
    gen = dbos.read_stream(workflow_id, stream_key)
    return [next(gen) for _ in range(count)]


def run_phase_worker_b(meta: dict[str, object]) -> None:
    from dbos import DBOS
    from dbos._sys_db import WorkflowStatusString

    run_id = str(meta["run_id"])
    stream_key = str(meta["stream_key"])
    fork_step = int(meta["fork_step"])
    parent_id = str(meta["workflow_id"])

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([QUEUE_NAME])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_stream_fork_wf import StreamForkWF  # noqa: F401

    progress("recover_worker_a")
    DBOS._recover_pending_workflows([WORKER_A])

    parent_handle = DBOS.retrieve_workflow(parent_id)
    parent_result = parent_handle.get_result()
    parent_status = parent_handle.get_status()
    recovery_attempts = parent_status.recovery_attempts or 0

    invariant(
        "S5",
        "parent_terminal_success",
        parent_status.status == WorkflowStatusString.SUCCESS.value,
        f"workflow_id={parent_id} status={parent_status.status} result={parent_result}",
    )
    invariant(
        "S5b",
        "recovery_attempts_bounded",
        recovery_attempts <= MAX_RECOVERY_ATTEMPTS,
        f"recovery_attempts={recovery_attempts} max={MAX_RECOVERY_ATTEMPTS}",
    )

    parent_stream = read_stream_values(DBOS, parent_id, stream_key)
    invariant(
        "S1",
        "parent_stream_complete",
        parent_stream == [0, 1, 2],
        f"workflow_id={parent_id} stream={parent_stream}",
    )

    progress("fork_workflow", f"parent={parent_id} start_step={fork_step}")
    # Fork without queue_name so the listening worker cannot drain the child
    # before we snapshot the copied stream prefix (matches test_fork_streams).
    fork_handle = DBOS.fork_workflow(parent_id, fork_step)
    fork_id = fork_handle.workflow_id
    meta["fork_workflow_id"] = fork_id
    META_PATH.write_text(json.dumps(meta))

    fork_prefix = read_stream_prefix(
        DBOS, fork_id, stream_key, min(3, fork_step - 1)
    )
    expected_prefix = expected_fork_prefix(fork_step)
    invariant(
        "S4",
        "fork_prefix_matches",
        fork_prefix == expected_prefix,
        f"fork_id={fork_id} fork_step={fork_step} expected={expected_prefix} got={fork_prefix}",
    )

    fork_result = fork_handle.get_result()
    fork_status = fork_handle.get_status()
    invariant(
        "S5c",
        "fork_terminal_success",
        fork_status.status == WorkflowStatusString.SUCCESS.value,
        f"fork_id={fork_id} status={fork_status.status} result={fork_result}",
    )

    fork_stream = read_stream_values(DBOS, fork_id, stream_key)
    invariant(
        "S2",
        "fork_stream_complete",
        fork_stream == [0, 1, 2],
        f"fork_id={fork_id} stream={fork_stream}",
    )

    dupes = duplicate_stream_offsets([parent_id, fork_id])
    invariant(
        "S3",
        "no_duplicate_stream_offsets",
        dupes == 0,
        f"duplicate_groups={dupes} workflows={parent_id},{fork_id}",
    )

    parent_terminal_rows = terminal_record_count_for_workflow(run_id, parent_id)
    invariant(
        "S1b",
        "parent_terminal_record_once",
        parent_terminal_rows == 1,
        f"workflow_id={parent_id} stream_results={parent_terminal_rows}",
    )
    fork_terminal_rows = terminal_record_count_for_workflow(run_id, fork_id)
    invariant(
        "S2b",
        "fork_terminal_record_once",
        fork_terminal_rows == 1,
        f"workflow_id={fork_id} stream_results={fork_terminal_rows}",
    )

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


def scenario_stream_fork(root_seed: int) -> None:
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

    meta = json.loads(META_PATH.read_text())
    run_id = str(meta["run_id"])
    invariant(
        "F1",
        "no_terminal_before_recovery",
        terminal_record_count(run_id) == 0,
        f"stream_results={terminal_record_count(run_id)}",
    )

    worker_b = _run_subphase("worker_b", {**base_env, "DBOS__VMID": WORKER_B})
    if worker_b.returncode != 0:
        raise RuntimeError(f"worker_b failed rc={worker_b.returncode}")

    progress(
        "scenario_done",
        f"run_id={run_id} crash_after={meta['crash_after_writes']} fork_step={meta['fork_step']}",
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
        workflow_id = run_phase_enqueue(
            str(meta["run_id"]),
            str(meta["stream_key"]),
            int(meta["crash_after_writes"]),
        )
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
        if not meta.get("workflow_id"):
            return 2
        progress("dbos_worker_b_launch")
        run_phase_worker_b(meta)
        return 0

    return workload_main("dbos_stream_fork", scenario_stream_fork)


if __name__ == "__main__":
    raise SystemExit(main())
