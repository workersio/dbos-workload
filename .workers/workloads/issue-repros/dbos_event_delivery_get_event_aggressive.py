#!/usr/bin/env python3
"""
P6fag: get_event-only aggressive flood — strengthen F2 duplicate-delivery signal.

# SURFACE: set_event / get_event ack under multi-enqueue + crash + recovery
# MODELS:  12–24 same-key enqueued set/get workflows, all crash at set_event;
#          concurrent enqueue; worker-b recovers worker-a
# ORACLE:  F1 no_delivery_before_recovery, E1 delivery_exactly_once,
#          E2 payload_matches, E3 terminal_success, E4 recovery_bounded,
#          E5 all_workflows_terminal_success
# ISSUES:  #562, #588, F2 class (duplicate get_event deliveries)
# VARIANCE: burst_count (12–24), event_key from WORKLOAD_SEED
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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

QUEUE_NAME = "formal_events_get_aggr"
WORKER_A = "worker-a"
WORKER_B = "worker-b"
NO_DEQUEUE = "__formal_no_dequeue__"
MAX_RECOVERY_ATTEMPTS = 8

META_PATH = RUN_DIR / "meta.json"

QUEUE_OPTS = {
    "concurrency": 4,
    "worker_concurrency": 4,
    "polling_interval_sec": 0.02,
}


def init_app_schema() -> None:
    psql(
        """
        CREATE TABLE IF NOT EXISTS event_deliveries(
          id SERIAL PRIMARY KEY,
          run_id TEXT NOT NULL,
          channel TEXT NOT NULL,
          payload TEXT NOT NULL,
          executor TEXT NOT NULL
        );
        """,
        database=APP_DB,
    )


def reset_app_tables() -> None:
    psql("TRUNCATE event_deliveries RESTART IDENTITY;", database=APP_DB)


def build_scenario(root_seed: int) -> dict[str, object]:
    burst_count = 12 + (root_seed % 13)
    return {
        "run_id": f"event-get-aggr-{workload_seed_raw()[:16]}",
        "mode": "get_event",
        "event_key": f"key_{root_seed % 17}",
        "event_value": f"value_{root_seed % 1000}",
        "burst_count": burst_count,
        "workflow_id": "",
        "workflow_ids": [],
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


def delivery_count(run_id: str, channel: str) -> int:
    rid = run_id.replace("'", "''")
    ch = channel.replace("'", "''")
    return int(
        sql_scalar(
            f"SELECT COUNT(*) FROM event_deliveries "
            f"WHERE run_id = '{rid}' AND channel = '{ch}';"
        )
    )


def run_phase_enqueue(
    run_id: str, event_key: str, event_value: str, burst_count: int
) -> list[str]:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_event_delivery_wf import EventWF

    workflow_ids: list[str | None] = [None] * burst_count

    def enqueue_one(index: int) -> tuple[int, str]:
        handle = DBOS.enqueue_workflow(
            QUEUE_NAME,
            EventWF.set_get_event,
            run_id,
            event_key,
            event_value,
            True,
        )
        progress("enqueued_get_event", f"workflow_id={handle.workflow_id} index={index}")
        return index, handle.workflow_id

    with ThreadPoolExecutor(max_workers=min(6, burst_count)) as pool:
        futures = [pool.submit(enqueue_one, i) for i in range(burst_count)]
        for future in as_completed(futures):
            index, workflow_id = future.result()
            workflow_ids[index] = workflow_id

    DBOS.destroy(destroy_registry=True)
    return [wid for wid in workflow_ids if wid is not None]


def run_phase_worker_a() -> None:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([QUEUE_NAME])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_event_delivery_wf import EventWF  # noqa: F401

    os.environ["DBOS_CRASH_NOW"] = "1"
    if not threading.Event().wait(timeout=240):
        os._exit(1)


def assert_fault_no_delivery(meta: dict[str, object]) -> None:
    run_id = str(meta["run_id"])
    channel = f"event:{meta['event_key']}"
    count = delivery_count(run_id, channel)
    invariant(
        "F1",
        "no_delivery_before_recovery",
        count == 0,
        f"channel={channel} deliveries={count}",
    )


def assert_delivery_oracles(meta: dict[str, object], dbos) -> None:
    from dbos._sys_db import WorkflowStatusString

    run_id = str(meta["run_id"])
    channel = f"event:{meta['event_key']}"
    expected = str(meta["event_value"])
    burst_count = int(meta["burst_count"])
    workflow_ids = list(meta.get("workflow_ids") or [])
    probe_workflow_id = str(meta["workflow_id"])

    count = delivery_count(run_id, channel)
    invariant(
        "E1",
        "delivery_exactly_once",
        count == 1,
        f"channel={channel} deliveries={count} burst_count={burst_count} "
        f"enqueued={len(workflow_ids)}",
    )

    payload = sql_scalar(
        "SELECT payload FROM event_deliveries "
        f"WHERE run_id = '{run_id.replace(chr(39), chr(39)+chr(39))}' "
        f"AND channel = '{channel.replace(chr(39), chr(39)+chr(39))}' "
        "ORDER BY id LIMIT 1;"
    )
    invariant(
        "E2",
        "payload_matches",
        payload == expected,
        f"expected={expected} got={payload}",
    )

    handle = dbos.retrieve_workflow(probe_workflow_id)
    status = handle.get_status()
    wf_status = status.status
    recovery_attempts = status.recovery_attempts or 0
    error = status.error or ""

    invariant(
        "E3",
        "terminal_success",
        wf_status == WorkflowStatusString.SUCCESS.value,
        f"workflow_id={probe_workflow_id} status={wf_status} error={error[:200]}",
    )
    invariant(
        "E4",
        "recovery_attempts_bounded",
        recovery_attempts <= MAX_RECOVERY_ATTEMPTS,
        f"recovery_attempts={recovery_attempts} max={MAX_RECOVERY_ATTEMPTS}",
    )

    bad_terminal: list[str] = []
    for workflow_id in workflow_ids:
        wf_status = dbos.retrieve_workflow(workflow_id).get_status().status
        if wf_status != WorkflowStatusString.SUCCESS.value:
            bad_terminal.append(f"{workflow_id}={wf_status}")
    invariant(
        "E5",
        "all_workflows_terminal_success",
        len(bad_terminal) == 0,
        f"failures={bad_terminal[:5]} total={len(workflow_ids)}",
    )


def run_phase_worker_b(meta: dict[str, object]) -> None:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([QUEUE_NAME])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_event_delivery_wf import EventWF  # noqa: F401

    progress("recover_worker_a")
    DBOS._recover_pending_workflows([WORKER_A])

    workflow_id = str(meta["workflow_id"])
    handle = DBOS.retrieve_workflow(workflow_id)
    result = handle.get_result()
    progress("get_event_result", f"workflow_id={workflow_id} result={result}")

    assert_delivery_oracles(meta, DBOS)
    DBOS.destroy(destroy_registry=True)


def _run_subphase(phase: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        **env,
        "DBOS_WORKLOAD_RUN_DIR": str(RUN_DIR.parent),
        "DBOS_WORKLOAD_STEM": Path(__file__).stem,
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


def scenario_get_event_aggressive(root_seed: int) -> None:
    meta = build_scenario(root_seed)
    META_PATH.write_text(json.dumps(meta))

    progress("schema_init")
    init_app_schema()
    reset_app_tables()

    base_env = {"WORKLOAD_SEED": workload_seed_raw()}
    progress("scenario_mode", "get_event")
    progress("burst_count", str(meta["burst_count"]))

    enqueue = _run_subphase("enqueue", base_env)
    if enqueue.returncode != 0:
        raise RuntimeError(f"enqueue failed rc={enqueue.returncode}")

    meta = json.loads(META_PATH.read_text())
    workflow_ids = list(meta.get("workflow_ids") or [])
    if not workflow_ids:
        raise RuntimeError("missing workflow_ids after enqueue")
    meta["workflow_id"] = workflow_ids[-1]
    META_PATH.write_text(json.dumps(meta))

    worker_a = _run_subphase("worker_a", {**base_env, "DBOS__VMID": WORKER_A})
    if worker_a.returncode != 99:
        raise RuntimeError(f"worker_a expected rc=99 got {worker_a.returncode}")

    meta = json.loads(META_PATH.read_text())
    assert_fault_no_delivery(meta)

    worker_b = _run_subphase("worker_b", {**base_env, "DBOS__VMID": WORKER_B})
    if worker_b.returncode != 0:
        raise RuntimeError(f"worker_b failed rc={worker_b.returncode}")

    progress("scenario_done", f"run_id={meta['run_id']} burst={meta['burst_count']}")


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
        workflow_ids = run_phase_enqueue(
            str(meta["run_id"]),
            str(meta["event_key"]),
            str(meta["event_value"]),
            int(meta["burst_count"]),
        )
        meta["workflow_ids"] = workflow_ids
        meta["workflow_id"] = workflow_ids[-1]
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
        progress("dbos_worker_b_launch")
        run_phase_worker_b(meta)
        return 0

    return workload_main(
        "dbos_event_delivery_get_event_aggressive", scenario_get_event_aggressive
    )


if __name__ == "__main__":
    raise SystemExit(main())
