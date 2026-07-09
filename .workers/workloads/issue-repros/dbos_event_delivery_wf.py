# SURFACE: Event delivery (P6f)
# MODELS:  send/recv across workflows; set_event/get_event same workflow; crash + recovery
# ORACLE:  event_deliveries table (via parent SQL checks)
# ISSUES:  #562, #588, #702 (get_event ack path)
# VARIANCE: mode/topic/payload from parent scenario

from __future__ import annotations

import os
from pathlib import Path

import sqlalchemy as sa
from dbos import DBOS


def _run_dir() -> Path:
    base = Path(os.environ.get("DBOS_WORKLOAD_RUN_DIR", os.environ.get("TMPDIR", "/tmp")))
    stem = os.environ.get("DBOS_WORKLOAD_STEM", "dbos_event_delivery")
    return base / f"dbos-workload-{stem}"


@DBOS.dbos_class()
class EventWF:
    @staticmethod
    @DBOS.transaction()
    def record_delivery(run_id: str, channel: str, payload: str) -> None:
        session = DBOS.sql_session
        session.execute(
            sa.text(
                "INSERT INTO event_deliveries(run_id, channel, payload, executor) "
                "VALUES (:run_id, :channel, :payload, :executor)"
            ),
            {
                "run_id": run_id,
                "channel": channel,
                "payload": payload,
                "executor": os.environ.get("DBOS__VMID", "local"),
            },
        )

    @staticmethod
    @DBOS.workflow()
    def receiver(topic: str, run_id: str) -> str:
        ready = _run_dir() / "recv_ready.marker"
        ready.parent.mkdir(parents=True, exist_ok=True)
        ready.write_text("1", encoding="utf-8")
        print(f"PROGRESS recv_waiting topic={topic}", flush=True)
        msg = DBOS.recv(topic=topic, timeout_seconds=120.0)
        text = str(msg)
        EventWF.record_delivery(run_id, f"recv:{topic}", text)
        return text

    @staticmethod
    @DBOS.workflow()
    def sender(dest_id: str, topic: str, payload: str) -> str:
        DBOS.send(dest_id, payload, topic=topic)
        print(f"PROGRESS send_done dest={dest_id} topic={topic}", flush=True)
        return "sent"

    @staticmethod
    @DBOS.workflow()
    def set_get_event(run_id: str, key: str, value: str, crash_after_set: bool) -> str:
        DBOS.set_event(key, value)
        print(f"PROGRESS set_event_done key={key}", flush=True)
        if crash_after_set and os.environ.get("DBOS_CRASH_NOW") == "1":
            os._exit(99)
        workflow_id = DBOS.workflow_id
        if workflow_id is None:
            raise RuntimeError("missing workflow_id after set_event")
        got = DBOS.get_event(workflow_id, key, timeout_seconds=60.0)
        text = str(got)
        EventWF.record_delivery(run_id, f"event:{key}", text)
        return text
