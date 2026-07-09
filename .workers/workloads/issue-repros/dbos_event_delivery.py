#!/usr/bin/env python3
"""
P6f: event delivery — send/recv and set_event/get_event under executor crash + recovery.

# SURFACE: Send / recv / ack (Family 6)
# MODELS:  send_recv: receiver blocks on recv, sender delivers, worker-a crashes after send;
#          get_event: queued workflow sets event then crashes before get_event; worker-b recovers
# ORACLE:  E1 delivery_exactly_once, E2 payload_matches, E3 terminal_success,
#          E4 recovery_bounded, F1 fault landed (no delivery row mid-crash)
# ISSUES:  #562, #588
# VARIANCE: mode send_recv|get_event from WORKLOAD_SEED; topic/key/payload values
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from dbos_workload_common import (
    APP_DB,
    RUN_DIR,
    InvariantFailure,
    dbos_config,
    invariant,
    progress,
    psql,
    workload_main,
    workload_seed_raw,
)

QUEUE_NAME = "formal_events"
WORKER_A = "worker-a"
WORKER_B = "worker-b"
NO_DEQUEUE = "__formal_no_dequeue__"
MAX_RECOVERY_ATTEMPTS = 8

META_PATH = RUN_DIR / "meta.json"
RECV_READY_PATH = RUN_DIR / "recv_ready.marker"

QUEUE_OPTS = {
    "concurrency": 1,
    "worker_concurrency": 1,
    "polling_interval_sec": 0.05,
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


def deterministic_uuid(label: str) -> str:
    digest = hashlib.sha256(f"{workload_seed_raw()}:{label}".encode()).digest()
    return str(uuid.UUID(bytes=digest[:16]))


def build_scenario(root_seed: int) -> dict[str, object]:
    run_id = f"event-{workload_seed_raw()[:16]}"
    mode = "send_recv" if (root_seed % 2) == 0 else "get_event"
    return {
        "run_id": run_id,
        "mode": mode,
        "topic": f"topic_{root_seed % 7}",
        "payload": f"payload_{root_seed % 1000}",
        "event_key": f"key_{root_seed % 11}",
        "event_value": f"value_{root_seed % 1000}",
        "receiver_wfid": deterministic_uuid("receiver"),
        "sender_wfid": deterministic_uuid("sender"),
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


def delivery_count(run_id: str, channel: str | None = None) -> int:
    if channel is None:
        sql = (
            "SELECT COUNT(*) FROM event_deliveries "
            f"WHERE run_id = '{run_id.replace(chr(39), chr(39)+chr(39))}';"
        )
    else:
        sql = (
            "SELECT COUNT(*) FROM event_deliveries "
            f"WHERE run_id = '{run_id.replace(chr(39), chr(39)+chr(39))}' "
            f"AND channel = '{channel.replace(chr(39), chr(39)+chr(39))}';"
        )
    return int(sql_scalar(sql))


def run_phase_enqueue_get_event(run_id: str, event_key: str, event_value: str) -> str:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_event_delivery_wf import EventWF

    handle = DBOS.enqueue_workflow(
        QUEUE_NAME,
        EventWF.set_get_event,
        run_id,
        event_key,
        event_value,
        True,
    )
    workflow_id = handle.workflow_id
    progress("enqueued_get_event", f"workflow_id={workflow_id}")
    DBOS.destroy(destroy_registry=True)
    return workflow_id


def run_phase_worker_a_get_event() -> None:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([QUEUE_NAME])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_event_delivery_wf import EventWF  # noqa: F401

    os.environ["DBOS_CRASH_NOW"] = "1"
    if not threading.Event().wait(timeout=180):
        os._exit(1)


def run_phase_worker_a_send_recv(meta: dict[str, object]) -> None:
    from dbos import DBOS, SetWorkflowID

    receiver_wfid = str(meta["receiver_wfid"])
    sender_wfid = str(meta["sender_wfid"])
    topic = str(meta["topic"])
    payload = str(meta["payload"])
    run_id = str(meta["run_id"])

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.launch()

    from dbos_event_delivery_wf import EventWF

    if RECV_READY_PATH.exists():
        RECV_READY_PATH.unlink()

    recv_error: list[BaseException] = []

    def receiver_thread() -> None:
        try:
            with SetWorkflowID(receiver_wfid):
                handle = DBOS.start_workflow(EventWF.receiver, topic, run_id)
                handle.get_result()
        except BaseException as exc:
            recv_error.append(exc)

    thread = threading.Thread(target=receiver_thread, daemon=True)
    thread.start()

    progress("wait_recv_ready")
    for _ in range(1200):
        if RECV_READY_PATH.exists():
            break
        time.sleep(0.1)
    else:
        raise RuntimeError("receiver never reached recv (no recv_ready.marker)")

    progress("sender_start")
    with SetWorkflowID(sender_wfid):
        sender_handle = DBOS.start_workflow(
            EventWF.sender, receiver_wfid, topic, payload
        )
        sender_handle.get_result()
    progress("sender_done")

    os._exit(99)


def assert_fault_no_delivery(meta: dict[str, object]) -> None:
    run_id = str(meta["run_id"])
    mode = str(meta["mode"])
    if mode == "send_recv":
        channel = f"recv:{meta['topic']}"
        expected = str(meta["payload"])
    else:
        channel = f"event:{meta['event_key']}"
        expected = str(meta["event_value"])

    count = delivery_count(run_id, channel)
    invariant(
        "F1",
        "no_delivery_before_recovery",
        count == 0,
        f"mode={mode} channel={channel} deliveries={count}",
    )


def assert_delivery_oracles(meta: dict[str, object], dbos) -> None:
    from dbos._sys_db import WorkflowStatusString

    run_id = str(meta["run_id"])
    mode = str(meta["mode"])

    if mode == "send_recv":
        channel = f"recv:{meta['topic']}"
        expected = str(meta["payload"])
        workflow_id = str(meta["receiver_wfid"])
    else:
        channel = f"event:{meta['event_key']}"
        expected = str(meta["event_value"])
        workflow_id = str(meta["workflow_id"])

    count = delivery_count(run_id, channel)
    invariant(
        "E1",
        "delivery_exactly_once",
        count == 1,
        f"channel={channel} deliveries={count}",
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

    handle = dbos.retrieve_workflow(workflow_id)
    status = handle.get_status()
    wf_status = status.status
    recovery_attempts = status.recovery_attempts or 0
    error = status.error or ""

    invariant(
        "E3",
        "terminal_success",
        wf_status == WorkflowStatusString.SUCCESS.value,
        f"workflow_id={workflow_id} status={wf_status} error={error[:200]}",
    )
    invariant(
        "E4",
        "recovery_attempts_bounded",
        recovery_attempts <= MAX_RECOVERY_ATTEMPTS,
        f"recovery_attempts={recovery_attempts} max={MAX_RECOVERY_ATTEMPTS}",
    )


def run_phase_worker_b(meta: dict[str, object]) -> None:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    if str(meta["mode"]) == "get_event":
        DBOS.listen_queues([QUEUE_NAME])
    DBOS.launch()
    if str(meta["mode"]) == "get_event":
        register_queue(DBOS)

    from dbos_event_delivery_wf import EventWF  # noqa: F401

    progress("recover_worker_a")
    DBOS._recover_pending_workflows([WORKER_A])

    mode = str(meta["mode"])
    if mode == "send_recv":
        receiver_wfid = str(meta["receiver_wfid"])
        handle = DBOS.retrieve_workflow(receiver_wfid)
        result = handle.get_result()
        progress("receiver_result", f"result={result}")
    else:
        workflow_id = str(meta["workflow_id"])
        handle = DBOS.retrieve_workflow(workflow_id)
        result = handle.get_result()
        progress("get_event_result", f"result={result}")

    assert_delivery_oracles(meta, DBOS)
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


def scenario_event_delivery(root_seed: int) -> None:
    meta = build_scenario(root_seed)
    META_PATH.write_text(json.dumps(meta))

    progress("schema_init")
    init_app_schema()
    reset_app_tables()

    base_env = {"WORKLOAD_SEED": workload_seed_raw()}
    mode = str(meta["mode"])
    progress("scenario_mode", mode)

    if mode == "get_event":
        enqueue = _run_subphase("enqueue", base_env)
        if enqueue.returncode != 0:
            raise RuntimeError(f"enqueue failed rc={enqueue.returncode}")
        meta = json.loads(META_PATH.read_text())
        workflow_id = str(meta.get("workflow_id") or "")
        if not workflow_id:
            raise RuntimeError("missing workflow_id after enqueue")

        worker_a = _run_subphase("worker_a_get_event", {**base_env, "DBOS__VMID": WORKER_A})
        if worker_a.returncode != 99:
            raise RuntimeError(f"worker_a expected rc=99 got {worker_a.returncode}")
    else:
        worker_a = _run_subphase(
            "worker_a_send_recv", {**base_env, "DBOS__VMID": WORKER_A}
        )
        if worker_a.returncode != 99:
            raise RuntimeError(f"worker_a expected rc=99 got {worker_a.returncode}")

    meta = json.loads(META_PATH.read_text())
    assert_fault_no_delivery(meta)

    worker_b = _run_subphase("worker_b", {**base_env, "DBOS__VMID": WORKER_B})
    if worker_b.returncode != 0:
        raise RuntimeError(f"worker_b failed rc={worker_b.returncode}")

    progress("scenario_done", f"mode={mode} run_id={meta['run_id']}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--phase",
        choices=["enqueue", "worker_a_get_event", "worker_a_send_recv", "worker_b"],
        default="",
    )
    args = parser.parse_args()

    if args.phase == "enqueue":
        if not META_PATH.exists():
            return 2
        meta = json.loads(META_PATH.read_text())
        progress("dbos_enqueue_launch")
        workflow_id = run_phase_enqueue_get_event(
            str(meta["run_id"]),
            str(meta["event_key"]),
            str(meta["event_value"]),
        )
        meta["workflow_id"] = workflow_id
        META_PATH.write_text(json.dumps(meta))
        return 0

    if args.phase == "worker_a_get_event":
        progress("dbos_worker_a_get_event_launch")
        run_phase_worker_a_get_event()
        return 0

    if args.phase == "worker_a_send_recv":
        if not META_PATH.exists():
            return 2
        meta = json.loads(META_PATH.read_text())
        progress("dbos_worker_a_send_recv_launch")
        run_phase_worker_a_send_recv(meta)
        return 0

    if args.phase == "worker_b":
        if not META_PATH.exists():
            return 2
        meta = json.loads(META_PATH.read_text())
        progress("dbos_worker_b_launch")
        run_phase_worker_b(meta)
        return 0

    return workload_main("dbos_event_delivery", scenario_event_delivery)


if __name__ == "__main__":
    raise SystemExit(main())
