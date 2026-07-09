#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import json
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import sqlalchemy as sa

try:
    from dbos import DBOS, DBOSClient, DBOSConfig, SendMessage, SetWorkflowID, WorkflowSerializationFormat
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "message-event-cancellation"
RUNG_001_ID = "rung-001-duplicate-timeout-cancel"
RUNG_002_ID = "rung-002-listener-fallback-fork-stream"
RUNG_003_ID = "rung-003-recovery-replay-cancellation"
RUNG_004_ID = "rung-004-bounded-seed-sweep"
RUNG_005_ID = "rung-005-live-stream-resume-listener-offsets"
RUNG_006_ID = "rung-006-client-get-event-prompt-polling"
APP_ID = "wio-message-event-cancellation"
APP_VERSION = "wio-message-rungs-001-006"


class WorkloadFailure(Exception):
    pass


class SetupBlock(Exception):
    pass


@dataclass(frozen=True)
class CasePlan:
    rung_id: str
    case_id: str
    seed: int
    schedule: str
    workflow_id: str
    app_db: str
    sys_db: str
    topic: str
    event_key: str
    idempotency_key: str
    timeout_seconds: float
    late_send_offset_ms: int
    cancel_offset_ms: int
    fallback_interval_seconds: float = 0.1


_GATES: dict[str, threading.Event] = {}
_GATE_PREFIX_READY: dict[str, threading.Event] = {}


def event(name: str, **fields: Any) -> None:
    parts = [f"WIO-EVENT {name}"]
    parts.extend(f"{key}={json.dumps(value, sort_keys=True)}" for key, value in fields.items())
    print(" ".join(parts), flush=True)


def invariant(name: str, ok: bool, **fields: Any) -> None:
    status = "PASS" if ok else "FAIL"
    summary = json.dumps(fields, sort_keys=True) if fields else "ok"
    print(f"INVARIANT {name} {name} {status} {summary}", flush=True)
    if not ok:
        raise WorkloadFailure(f"{name} failed: {summary}")


async def run_blocking(func: Any, *args: Any, **kwargs: Any) -> Any:
    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))


def password() -> str:
    return os.environ.get("PGPASSWORD", "dbos")


def db_url(database: str, *, driver: str = "postgresql+psycopg") -> str:
    return f"{driver}://postgres:{quote(password(), safe='')}@localhost:{os.environ.get('PGPORT', '5432')}/{database}"


def admin_engine() -> sa.Engine:
    return sa.create_engine(db_url("postgres"), connect_args={"connect_timeout": 10})


def cleanup_databases(plan: CasePlan) -> None:
    engine = admin_engine()
    try:
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            for database in (plan.app_db, plan.sys_db):
                conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'))
    finally:
        engine.dispose()


def config(plan: CasePlan, *, executor_id: str | None = None) -> DBOSConfig:
    return {
        "name": APP_ID,
        "application_version": APP_VERSION,
        "executor_id": executor_id,
        "application_database_url": db_url(plan.app_db, driver="postgresql"),
        "system_database_url": db_url(plan.sys_db),
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
    }


def make_plan(rung: str, case_id: str, seed_override: int | None = None) -> CasePlan:
    normalized_rung = normalize_rung(rung)
    rung_001_cases: dict[str, tuple[int, str, float, int, int]] = {
        "case-001": (3101, "cancel-recv-registered-then-duplicate-send", 0.2, 0, 10),
        "case-002": (3103, "cancel-event-registered-then-set-event", 1.0, 0, 10),
        "case-003": (3107, "timeout-before-late-send-and-bulk-reject", 0.1, 50, 0),
    }
    rung_002_cases: dict[str, tuple[int, str, float, int, int]] = {
        "case-001": (3111, "fallback-recv-listener-stopped-before-send", 10.0, 0, 0),
        "case-002": (3113, "fallback-get-event-listener-stopped-before-set", 10.0, 0, 0),
        "case-003": (3117, "fork-fanout-duplicate-key", 1.0, 0, 0),
        "case-004": (3119, "fork-event-step-boundaries", 1.0, 0, 0),
        "case-005": (3121, "interleaved-stream-offsets", 1.0, 0, 0),
    }
    rung_003_cases: dict[str, tuple[int, str, float, int, int]] = {
        "case-001": (3131, "relaunch-after-cancelled-recv-reuse", 120.0, 0, 10),
        "case-002": (3133, "relaunch-after-cancelled-event-reuse", 120.0, 0, 10),
        "case-003": (3137, "timeout-replay-after-relaunch", 0.1, 50, 0),
        "case-004": (3139, "fallback-waiter-relaunch", 120.0, 0, 0),
        "case-005": (3141, "fork-tree-relaunch-before-fanout", 1.0, 0, 0),
        "case-006": (3143, "stream-relaunch-before-close", 120.0, 0, 0),
        "case-007": (3147, "repeated-cancel-reuse-across-relaunch", 120.0, 0, 10),
        "case-008": (3149, "relaunch-then-bulk-reject-valid-send", 120.0, 0, 0),
    }
    rung_004_cases: dict[str, tuple[int, str, float, int, int]] = {
        "case-001": (3151, "cancel-recv-registered-then-duplicate-send", 0.2, 0, 0),
        "case-002": (3152, "cancel-recv-registered-then-duplicate-send", 0.2, 0, 25),
        "case-003": (3153, "cancel-recv-registered-then-duplicate-send", 0.2, 0, 100),
        "case-004": (3154, "cancel-event-registered-then-set-event", 1.0, 0, 0),
        "case-005": (3155, "cancel-event-registered-then-set-event", 1.0, 0, 25),
        "case-006": (3156, "cancel-event-registered-then-set-event", 1.0, 0, 100),
        "case-007": (3157, "duplicate-send-before-timeout", 1.0, 0, 0),
        "case-008": (3158, "duplicate-send-after-timeout", 0.1, 50, 0),
        "case-009": (3159, "bulk-duplicate-key-reject", 10.0, 0, 0),
        "case-010": (3160, "fallback-recv-listener-stopped-before-send", 10.0, 0, 0),
        "case-011": (3161, "fallback-recv-listener-stopped-before-send", 10.0, 0, 0),
        "case-012": (3162, "fallback-get-event-listener-stopped-before-set", 10.0, 0, 0),
        "case-013": (3163, "fallback-get-event-listener-stopped-before-set", 10.0, 0, 0),
        "case-014": (3164, "fork-fanout-two-descendants", 1.0, 0, 0),
        "case-015": (3165, "fork-fanout-four-descendants", 1.0, 0, 0),
        "case-016": (3166, "fork-event-early-step", 1.0, 0, 0),
        "case-017": (3167, "fork-event-late-step", 1.0, 0, 0),
        "case-018": (3168, "stream-two-keys-two-writers", 1.0, 0, 0),
        "case-019": (3169, "stream-three-keys-three-writers", 1.0, 0, 0),
        "case-020": (3170, "stream-hot-key-five-writes", 1.0, 0, 0),
        "case-021": (3171, "replay-cancel-recv", 120.0, 0, 10),
        "case-022": (3172, "replay-timeout-late-send", 0.1, 50, 0),
        "case-023": (3173, "replay-stream-before-close", 120.0, 0, 0),
        "case-024": (3174, "mixed-small-session", 10.0, 0, 0),
    }
    rung_005_cases: dict[str, tuple[int, str, float, int, int]] = {
        "case-001": (3181, "runtime-live-reconnect-offsets", 45.0, 0, 0),
        "case-002": (3182, "client-and-fallback-resume", 45.0, 0, 0),
        "case-003": (3183, "blocked-reader-termination-relaunch", 45.0, 0, 0),
    }
    rung_006_cases: dict[str, tuple[int, str, float, int, int]] = {
        "case-001": (7130, "client-sync-delayed-event", 12.0, 0, 0),
        "case-002": (7131, "client-async-delayed-event", 12.0, 0, 0),
        "case-003": (7132, "client-terminal-missing-event", 12.0, 0, 0),
        "case-004": (7133, "client-event-update-race", 12.0, 0, 0),
    }
    cases_by_rung = {
        RUNG_001_ID: rung_001_cases,
        RUNG_002_ID: rung_002_cases,
        RUNG_003_ID: rung_003_cases,
        RUNG_004_ID: rung_004_cases,
        RUNG_005_ID: rung_005_cases,
        RUNG_006_ID: rung_006_cases,
    }
    cases = cases_by_rung[normalized_rung]
    if case_id not in cases:
        raise SetupBlock(f"unknown case {case_id} for rung {normalized_rung}")
    seed, schedule, timeout_seconds, late_send_offset_ms, cancel_offset_ms = cases[case_id]
    if seed_override is not None:
        seed = seed_override
    suffix = f"{normalized_rung.split('-')[1]}_{case_id.replace('-', '_')}_{seed}_{uuid.uuid5(uuid.NAMESPACE_URL, f'{normalized_rung}:{case_id}:{seed}').hex[:8]}"
    fallback_interval_seconds = 0.1
    if normalized_rung == RUNG_004_ID and case_id in {"case-010", "case-012"}:
        fallback_interval_seconds = 0.05
    elif normalized_rung == RUNG_004_ID and case_id in {"case-011", "case-013"}:
        fallback_interval_seconds = 0.25
    elif normalized_rung == RUNG_005_ID and case_id in {"case-002", "case-003"}:
        fallback_interval_seconds = 0.25
    elif normalized_rung == RUNG_006_ID:
        fallback_interval_seconds = 0.1
    return CasePlan(
        rung_id=normalized_rung,
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        workflow_id=f"wio-message-{normalized_rung.split('-')[1]}-{case_id}-{seed}-{uuid.uuid5(uuid.NAMESPACE_DNS, f'wf:{normalized_rung}:{case_id}:{seed}').hex[:8]}",
        app_db=f"wio_message_app_{suffix}",
        sys_db=f"wio_message_sys_{suffix}",
        topic=f"topic-{case_id}-{seed}",
        event_key=f"event-{case_id}-{seed}",
        idempotency_key=f"idem-{case_id}-{seed}",
        timeout_seconds=timeout_seconds,
        late_send_offset_ms=late_send_offset_ms,
        cancel_offset_ms=cancel_offset_ms,
        fallback_interval_seconds=fallback_interval_seconds,
    )


def normalize_rung(rung: str) -> str:
    if rung in {"rung-001", RUNG_001_ID, "rung-001-duplicate-timeout-cancel"}:
        return RUNG_001_ID
    if rung in {"rung-002", RUNG_002_ID, "rung-002-listener-fallback-fork-stream"}:
        return RUNG_002_ID
    if rung in {"rung-003", RUNG_003_ID, "rung-003-recovery-replay-cancellation"}:
        return RUNG_003_ID
    if rung in {"rung-004", RUNG_004_ID, "rung-004-bounded-seed-sweep"}:
        return RUNG_004_ID
    if rung in {"rung-005", RUNG_005_ID, "rung-005-live-stream-resume-listener-offsets"}:
        return RUNG_005_ID
    if rung in {"rung-006", RUNG_006_ID, "rung-006-client-get-event-prompt-polling"}:
        return RUNG_006_ID
    raise SetupBlock(f"unsupported rung {rung}")


@DBOS.workflow()
def noop_workflow() -> str:
    return "noop"


@DBOS.workflow()
async def set_event_workflow(key: str, value: str) -> str:
    await DBOS.set_event_async(key, value)
    return value


@DBOS.workflow()
def set_event_sync_workflow(key: str, value: str) -> str:
    DBOS.set_event(key, value)
    return value


@DBOS.workflow()
def client_delayed_event_workflow(key: str, label: str, delay_seconds: float) -> dict[str, Any]:
    DBOS.sleep(delay_seconds)
    value = {"label": label, "written_at": time.time()}
    DBOS.set_event(key, value)
    return value


@DBOS.workflow()
async def client_async_delayed_event_workflow(key: str, label: str, delay_seconds: float) -> dict[str, Any]:
    await DBOS.sleep_async(delay_seconds)
    value = {"label": label, "written_at": time.time()}
    await DBOS.set_event_async(key, value)
    return value


@DBOS.workflow()
def client_terminal_no_event_workflow(key: str, delay_seconds: float) -> str:
    DBOS.sleep(delay_seconds)
    return f"no-event:{key}"


@DBOS.workflow()
def client_event_update_workflow(key: str, initial: str, updated: str, delay_seconds: float) -> str:
    DBOS.set_event(key, initial)
    DBOS.sleep(delay_seconds)
    DBOS.set_event(key, updated)
    return updated


@DBOS.workflow()
def recv_timeout_workflow(topic: str, timeout_seconds: float) -> Any:
    return DBOS.recv(topic, timeout_seconds=timeout_seconds)


@DBOS.workflow()
def recv_one_workflow(topic: str, timeout_seconds: float) -> str:
    return str(DBOS.recv(topic, timeout_seconds=timeout_seconds))


@DBOS.workflow()
def recv_string_workflow(topic: str, timeout_seconds: float) -> str:
    return str(DBOS.recv(topic, timeout_seconds=timeout_seconds))


@DBOS.workflow()
def get_event_string_workflow(target_workflow_id: str, key: str, timeout_seconds: float) -> str:
    return str(DBOS.get_event(target_workflow_id, key, timeout_seconds=timeout_seconds))


@DBOS.step()
def forkable_step() -> int:
    return 1


@DBOS.workflow()
def forkable_workflow() -> int:
    return forkable_step()


@DBOS.step()
def set_named_event_step(key: str, value: str) -> str:
    DBOS.set_event(key, value)
    return value


@DBOS.workflow()
def event_boundary_workflow(prefix: str) -> str:
    DBOS.set_event(f"{prefix}-root", "root-start")
    set_named_event_step(f"{prefix}-step-1", "one")
    set_named_event_step(f"{prefix}-step-2", "two")
    set_named_event_step(f"{prefix}-step-3", "three")
    DBOS.set_event(f"{prefix}-final", "final")
    return "event-boundary-done"


@DBOS.workflow()
def stream_writer_workflow(sequences: dict[str, list[str]]) -> str:
    max_len = max(len(values) for values in sequences.values())
    for index in range(max_len):
        for key in sorted(sequences):
            values = sequences[key]
            if index < len(values):
                DBOS.write_stream(key, values[index])
    for key in sorted(sequences):
        DBOS.close_stream(key)
    return "stream-writer-done"


@DBOS.workflow()
def gated_stream_writer_workflow(workflow_id: str, sequences: dict[str, list[str]]) -> str:
    gate = _GATES.setdefault(workflow_id, threading.Event())
    prefix_ready = _GATE_PREFIX_READY.setdefault(workflow_id, threading.Event())
    for key in sorted(sequences):
        DBOS.write_stream(key, sequences[key][0])
    prefix_ready.set()
    if not gate.wait(timeout=120):
        raise TimeoutError("stream relaunch gate did not open")
    max_len = max(len(values) for values in sequences.values())
    for index in range(1, max_len):
        for key in sorted(sequences):
            values = sequences[key]
            if index < len(values):
                DBOS.write_stream(key, values[index])
    for key in sorted(sequences):
        DBOS.close_stream(key)
    return workflow_id


@DBOS.workflow()
def live_stream_writer_workflow(stream_key: str, count: int, seed: int, label: str, delay_seconds: float, close_stream: bool) -> dict[str, Any]:
    for ordinal in range(count):
        value = {
            "seed": seed,
            "label": label,
            "ordinal": ordinal,
            "written_at": time.time(),
        }
        DBOS.write_stream(stream_key, value)
        if ordinal != count - 1 and delay_seconds > 0:
            DBOS.sleep(delay_seconds)
    if close_stream:
        DBOS.close_stream(stream_key)
    return {"stream_key": stream_key, "count": count, "closed": close_stream}


@DBOS.workflow()
def gated_live_stream_writer_workflow(
    workflow_id: str,
    stream_key: str,
    count: int,
    prefix_count: int,
    seed: int,
    label: str,
    delay_seconds: float,
    close_stream: bool,
) -> dict[str, Any]:
    gate = _GATES.setdefault(workflow_id, threading.Event())
    prefix_ready = _GATE_PREFIX_READY.setdefault(workflow_id, threading.Event())
    for ordinal in range(prefix_count):
        DBOS.write_stream(
            stream_key,
            {
                "seed": seed,
                "label": label,
                "ordinal": ordinal,
                "written_at": time.time(),
            },
        )
    prefix_ready.set()
    if not gate.wait(timeout=120):
        raise TimeoutError("live stream gate did not open")
    for ordinal in range(prefix_count, count):
        DBOS.write_stream(
            stream_key,
            {
                "seed": seed,
                "label": label,
                "ordinal": ordinal,
                "written_at": time.time(),
            },
        )
        if ordinal != count - 1 and delay_seconds > 0:
            DBOS.sleep(delay_seconds)
    if close_stream:
        DBOS.close_stream(stream_key)
    return {"stream_key": stream_key, "count": count, "prefix_count": prefix_count, "closed": close_stream}


@DBOS.workflow()
def unclosed_live_stream_writer_workflow(stream_key: str, count: int, seed: int, label: str, delay_seconds: float, tail_sleep_seconds: float) -> dict[str, Any]:
    for ordinal in range(count):
        value = {
            "seed": seed,
            "label": label,
            "ordinal": ordinal,
            "written_at": time.time(),
        }
        DBOS.write_stream(stream_key, value)
        if ordinal != count - 1 and delay_seconds > 0:
            DBOS.sleep(delay_seconds)
    DBOS.sleep(tail_sleep_seconds)
    return {"stream_key": stream_key, "count": count, "closed": False, "tail_sleep_seconds": tail_sleep_seconds}


def executor_a(plan: CasePlan) -> str:
    return f"{plan.workflow_id}-executor-a"


def executor_b(plan: CasePlan) -> str:
    return f"{plan.workflow_id}-executor-b"


def executor_c(plan: CasePlan) -> str:
    return f"{plan.workflow_id}-executor-c"


def launch(plan: CasePlan, *, executor_id: str | None = None, clean: bool = True) -> DBOS:
    if clean:
        cleanup_databases(plan)
        _GATES.clear()
        _GATE_PREFIX_READY.clear()
    os.environ["DBOS__APPID"] = APP_ID
    os.environ["DBOS__APPVERSION"] = APP_VERSION
    if executor_id is not None:
        os.environ["DBOS__VMID"] = executor_id
    else:
        os.environ.pop("DBOS__VMID", None)
    DBOS.destroy(destroy_registry=False)
    dbos = DBOS(config=config(plan, executor_id=executor_id))
    DBOS.launch()
    event(
        "case_start" if clean else "case_relaunch",
        frontier=FRONTIER_ID,
        rung=plan.rung_id,
        executor_id=executor_id,
        **asdict(plan),
    )
    return dbos


def relaunch(plan: CasePlan, *, from_executor_id: str, to_executor_id: str | None = None) -> tuple[DBOS, list[Any]]:
    DBOS.destroy(destroy_registry=False)
    dbos = launch(plan, executor_id=to_executor_id or executor_b(plan), clean=False)
    handles = DBOS._recover_pending_workflows([from_executor_id])
    event("recovered_pending_workflows", from_executor_id=from_executor_id, handles=[handle.workflow_id for handle in handles])
    return dbos, handles


def status_snapshot(workflow_id: str) -> dict[str, Any] | None:
    status = DBOS.get_workflow_status(workflow_id)
    if status is None:
        return None
    return {
        "workflow_id": status.workflow_id,
        "status": status.status,
        "queue_name": status.queue_name,
        "executor_id": getattr(status, "executor_id", None),
        "created_at": status.created_at,
        "updated_at": status.updated_at,
        "completed_at": getattr(status, "completed_at", None),
    }


def wait_for_status(workflow_id: str, expected: str, *, timeout_seconds: float = 10.0) -> dict[str, Any] | None:
    deadline = time.monotonic() + timeout_seconds
    observed = None
    while time.monotonic() < deadline:
        observed = status_snapshot(workflow_id)
        if observed and observed["status"] == expected:
            return observed
        time.sleep(0.05)
    return observed


def wait_for_condition(name: str, predicate: Any, *, timeout_seconds: float = 10.0) -> Any:
    deadline = time.monotonic() + timeout_seconds
    observed = None
    while time.monotonic() < deadline:
        observed = predicate()
        if observed:
            return observed
        time.sleep(0.05)
    invariant(name, bool(observed), observed=observed)
    return observed


def stream_listener_snapshot(sys_db: Any, *, workflow_id: str | None = None, key: str | None = None) -> list[dict[str, Any]]:
    rows = []
    for payload, components, event_obj in sys_db.streams_map.snapshot():
        workflow_component, key_component = components
        if workflow_id is not None and workflow_component != workflow_id:
            continue
        if key is not None and key_component != key:
            continue
        rows.append(
            {
                "payload": payload,
                "workflow_id": workflow_component,
                "key": key_component,
                "event_set": event_obj.is_set(),
            }
        )
    return rows


def assert_no_stream_listener(name: str, sys_db: Any, workflow_id: str, key: str) -> None:
    snapshot = stream_listener_snapshot(sys_db, workflow_id=workflow_id, key=key)
    invariant(name, not snapshot, workflow_id=workflow_id, key=key, streams_map=snapshot)


def wait_for_stream_prefix(dbos: DBOS, workflow_id: str, key: str, count: int, *, timeout_seconds: float = 20.0) -> list[Any]:
    def current_prefix() -> list[Any] | None:
        values = []
        for offset in range(count):
            try:
                values.append(dbos._sys_db.read_stream(workflow_id, key, offset))
            except ValueError:
                return None
        return values

    observed = wait_for_condition(
        "stream_prefix_rows_visible",
        current_prefix,
        timeout_seconds=timeout_seconds,
    )
    invariant(
        "stream_prefix_count_matches",
        len(observed) == count,
        workflow_id=workflow_id,
        key=key,
        observed_count=len(observed),
        expected_count=count,
        ordinals=stream_ordinals(observed),
    )
    return observed


def stream_ordinals(values: list[Any]) -> list[int]:
    return [int(value["ordinal"]) for value in values]


def assert_stream_suffix(name: str, values: list[Any], *, offset: int, count: int, seed: int, label: str) -> None:
    expected_ordinals = list(range(offset, count))
    observed_ordinals = stream_ordinals(values)
    labels_match = all(value.get("label") == label and int(value.get("seed")) == seed for value in values)
    invariant(
        name,
        observed_ordinals == expected_ordinals and labels_match,
        observed_ordinals=observed_ordinals,
        expected_ordinals=expected_ordinals,
        label=label,
        labels_match=labels_match,
        seed=seed,
    )


def collect_sync_stream(reader_name: str, iterator: Any) -> dict[str, Any]:
    started_at = time.time()
    values = []
    latencies = []
    live_latencies = []
    live_ordinals = []
    for value in iterator:
        observed_at = time.time()
        values.append(value)
        if isinstance(value, dict) and isinstance(value.get("written_at"), (int, float)):
            written_at = float(value["written_at"])
            latencies.append(observed_at - written_at)
            if written_at >= started_at:
                live_latencies.append(observed_at - written_at)
                live_ordinals.append(int(value["ordinal"]))
    completed_at = time.time()
    return {
        "reader": reader_name,
        "values": values,
        "ordinals": stream_ordinals(values),
        "started_at": started_at,
        "completed_at": completed_at,
        "duration": completed_at - started_at,
        "latencies": latencies,
        "max_latency": max(latencies) if latencies else 0.0,
        "live_latencies": live_latencies,
        "live_ordinals": live_ordinals,
        "max_live_latency": max(live_latencies) if live_latencies else 0.0,
    }


async def collect_async_stream(reader_name: str, async_iterator: Any) -> dict[str, Any]:
    started_at = time.time()
    values = []
    latencies = []
    live_latencies = []
    live_ordinals = []
    async for value in async_iterator:
        observed_at = time.time()
        values.append(value)
        if isinstance(value, dict) and isinstance(value.get("written_at"), (int, float)):
            written_at = float(value["written_at"])
            latencies.append(observed_at - written_at)
            if written_at >= started_at:
                live_latencies.append(observed_at - written_at)
                live_ordinals.append(int(value["ordinal"]))
    completed_at = time.time()
    return {
        "reader": reader_name,
        "values": values,
        "ordinals": stream_ordinals(values),
        "started_at": started_at,
        "completed_at": completed_at,
        "duration": completed_at - started_at,
        "latencies": latencies,
        "max_latency": max(latencies) if latencies else 0.0,
        "live_latencies": live_latencies,
        "live_ordinals": live_ordinals,
        "max_live_latency": max(live_latencies) if live_latencies else 0.0,
    }


def run_collector_thread(name: str, func: Any, *, timeout_seconds: float) -> dict[str, Any]:
    holder, thread, started_at = start_collector_thread(name, func)
    return finish_collector_thread(name, holder, thread, started_at, timeout_seconds=timeout_seconds)


def start_collector_thread(name: str, func: Any) -> tuple[dict[str, Any], threading.Thread, float]:
    holder: dict[str, Any] = {}

    def target() -> None:
        try:
            holder["result"] = func()
        except BaseException as exc:
            holder["error"] = f"{type(exc).__name__}: {exc}"

    thread = threading.Thread(target=target, name=f"wio-{name}", daemon=True)
    started_at = time.time()
    thread.start()
    return holder, thread, started_at


def finish_collector_thread(name: str, holder: dict[str, Any], thread: threading.Thread, started_at: float, *, timeout_seconds: float) -> dict[str, Any]:
    thread.join(timeout=timeout_seconds)
    elapsed = time.time() - started_at
    invariant(
        f"{name}_reader_completed",
        not thread.is_alive() and "error" not in holder,
        elapsed=elapsed,
        timeout_seconds=timeout_seconds,
        error=holder.get("error"),
    )
    result = holder["result"]
    result["thread_elapsed"] = elapsed
    return result


def client_event_payload(workflow_id: str, key: str) -> str:
    return f"{workflow_id}::{key}"


def make_client(plan: CasePlan) -> DBOSClient:
    client = DBOSClient(system_database_url=db_url(plan.sys_db))
    client._sys_db._notification_listener_polling_interval_sec = plan.fallback_interval_seconds
    return client


def client_prompt_bound_seconds(plan: CasePlan, *, delay_seconds: float = 0.0) -> float:
    return delay_seconds + max(5.0, plan.fallback_interval_seconds * 5.0 + 1.5)


def assert_client_polling_contract(client: DBOSClient, plan: CasePlan, prefix: str) -> dict[str, Any]:
    sys_db = client._sys_db
    recheck_interval = sys_db._event_recheck_interval()
    listener_running = getattr(sys_db, "_listener_running", None)
    use_listen_notify = getattr(sys_db, "use_listen_notify", None)
    invariant(
        f"{prefix}_client_has_no_listener",
        listener_running is False and use_listen_notify is False,
        listener_running=listener_running,
        use_listen_notify=use_listen_notify,
    )
    invariant(
        f"{prefix}_client_recheck_uses_polling_interval",
        recheck_interval <= plan.fallback_interval_seconds * 1.5,
        recheck_interval=recheck_interval,
        modeled_polling_interval=plan.fallback_interval_seconds,
    )
    return {
        "listener_running": listener_running,
        "use_listen_notify": use_listen_notify,
        "recheck_interval": recheck_interval,
        "modeled_polling_interval": plan.fallback_interval_seconds,
    }


def start_workflow_with_id(workflow_id: str, workflow: Any, *args: Any) -> Any:
    with SetWorkflowID(workflow_id):
        return DBOS.start_workflow(workflow, *args)


def start_client_get_thread(client: DBOSClient, workflow_id: str, key: str, timeout_seconds: float) -> tuple[dict[str, Any], threading.Thread, float]:
    return start_collector_thread(
        "client_get_event",
        lambda: {
            "value": client.get_event(workflow_id, key, timeout_seconds),
            "workflow_id": workflow_id,
            "key": key,
            "timeout_seconds": timeout_seconds,
        },
    )


def finish_client_get_thread(name: str, holder: dict[str, Any], thread: threading.Thread, started_at: float, *, timeout_seconds: float) -> dict[str, Any]:
    result = finish_collector_thread(name, holder, thread, started_at, timeout_seconds=timeout_seconds)
    result["duration"] = result["thread_elapsed"]
    result["completed_at"] = started_at + result["thread_elapsed"]
    return result


async def wait_for_async_condition(name: str, predicate: Any, *, timeout_seconds: float = 10.0) -> Any:
    deadline = time.monotonic() + timeout_seconds
    observed = None
    while time.monotonic() < deadline:
        observed = predicate()
        if observed:
            return observed
        await asyncio.sleep(0.05)
    invariant(name, bool(observed), observed=observed)
    return observed


def consume_stream_prefix(iterator: Any, count: int) -> list[Any]:
    values = []
    try:
        for _ in range(count):
            values.append(next(iterator))
    finally:
        close = getattr(iterator, "close", None)
        if close is not None:
            close()
    return values


async def cancel_recv_setup_window(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    sys_db = dbos._sys_db
    payload = f"{plan.workflow_id}::{plan.topic}"
    with SetWorkflowID(plan.workflow_id):
        noop_workflow()
    in_setup = threading.Event()
    release_setup = threading.Event()
    original_recv_check = sys_db.recv_check

    def blocking_recv_check(*args: Any, **kwargs: Any) -> None:
        in_setup.set()
        if not release_setup.wait(timeout=10):
            raise TimeoutError("recv setup gate did not release")
        original_recv_check(*args, **kwargs)

    sys_db.recv_check = blocking_recv_check  # type: ignore[method-assign]
    try:
        task = asyncio.create_task(
            sys_db.recv_async(plan.workflow_id, 100, 101, plan.topic, timeout_seconds=10)
        )
        reached = await run_blocking(in_setup.wait, 10)
        invariant("recv_cancel_target_window_reached", reached, payload=payload)
        if plan.cancel_offset_ms:
            await asyncio.sleep(plan.cancel_offset_ms / 1000)
        task.cancel()
        release_setup.set()
        cancelled = False
        try:
            await task
        except asyncio.CancelledError:
            cancelled = True
        invariant("recv_cancel_observed", cancelled, payload=payload)
        stale_after_cancel = sys_db.notifications_map.get(payload)
        invariant("recv_cancel_no_stale_waiter", stale_after_cancel is None, payload=payload, stale=str(stale_after_cancel))
    finally:
        sys_db.recv_check = original_recv_check  # type: ignore[method-assign]

    await DBOS.send_async(plan.workflow_id, "first", plan.topic, idempotency_key=plan.idempotency_key)
    await DBOS.send_async(plan.workflow_id, "duplicate", plan.topic, idempotency_key=plan.idempotency_key)
    first = await sys_db.recv_async(plan.workflow_id, 102, 103, plan.topic, timeout_seconds=2)
    second = await sys_db.recv_async(plan.workflow_id, 104, 105, plan.topic, timeout_seconds=0.2)
    invariant("duplicate_send_delivered_once", first == "first" and second is None, first=first, second=second)
    invariant("recv_reuse_no_stale_waiter", sys_db.notifications_map.get(payload) is None, payload=payload)
    return {"payload": payload, "first": first, "second": second}


async def cancel_event_setup_window(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    sys_db = dbos._sys_db
    payload = f"{plan.workflow_id}::{plan.event_key}"
    with SetWorkflowID(plan.workflow_id):
        noop_workflow()
    in_setup = threading.Event()
    release_setup = threading.Event()
    original_get_event_check = sys_db.get_event_check

    def blocking_get_event_check(*args: Any, **kwargs: Any) -> None:
        in_setup.set()
        if not release_setup.wait(timeout=10):
            raise TimeoutError("get_event setup gate did not release")
        original_get_event_check(*args, **kwargs)

    sys_db.get_event_check = blocking_get_event_check  # type: ignore[method-assign]
    try:
        task = asyncio.create_task(sys_db.get_event_async(plan.workflow_id, plan.event_key, timeout_seconds=10))
        reached = await run_blocking(in_setup.wait, 10)
        invariant("event_cancel_target_window_reached", reached, payload=payload)
        if plan.cancel_offset_ms:
            await asyncio.sleep(plan.cancel_offset_ms / 1000)
        task.cancel()
        release_setup.set()
        cancelled = False
        try:
            await task
        except asyncio.CancelledError:
            cancelled = True
        invariant("event_cancel_observed", cancelled, payload=payload)
        stale_after_cancel = sys_db.workflow_events_map.get(payload)
        invariant("event_cancel_no_stale_waiter", stale_after_cancel is None, payload=payload, stale=str(stale_after_cancel))
    finally:
        sys_db.get_event_check = original_get_event_check  # type: ignore[method-assign]

    expected = f"value-{plan.seed}"
    dbos._sys_db.set_event_from_workflow(
        plan.workflow_id,
        9001,
        plan.event_key,
        expected,
        serialization_type=WorkflowSerializationFormat.DEFAULT,
    )
    set_result = expected
    event_value = await DBOS.get_event_async(plan.workflow_id, plan.event_key, timeout_seconds=1)
    all_events = await run_blocking(DBOS.get_all_events, plan.workflow_id)
    invariant("event_later_get_matches_model", event_value == expected, event_value=event_value, expected=expected, set_result=set_result)
    invariant("event_all_events_matches_model", all_events.get(plan.event_key) == expected, all_events=all_events, expected=expected)
    invariant("event_reuse_no_stale_waiter", sys_db.workflow_events_map.get(payload) is None, payload=payload)
    return {"payload": payload, "event_value": event_value, "all_events": all_events}


def timeout_and_bulk_reject(plan: CasePlan) -> dict[str, Any]:
    with SetWorkflowID(plan.workflow_id):
        timeout_handle = DBOS.start_workflow(recv_timeout_workflow, plan.topic, plan.timeout_seconds)
    first_timeout = timeout_handle.get_result()
    if plan.late_send_offset_ms:
        time.sleep(plan.late_send_offset_ms / 1000)
    DBOS.send(plan.workflow_id, "late", plan.topic, idempotency_key=plan.idempotency_key)
    with SetWorkflowID(plan.workflow_id):
        replay_result = recv_timeout_workflow(plan.topic, plan.timeout_seconds)
    invariant("timeout_replay_stable_none", first_timeout is None and replay_result is None, first=first_timeout, replay=replay_result)

    bulk_dest = f"{plan.workflow_id}-bulk"
    with SetWorkflowID(bulk_dest):
        bulk_handle = DBOS.start_workflow(recv_one_workflow, plan.topic, 0.5)
    duplicate_error = ""
    try:
        DBOS.send_bulk(
            [
                SendMessage(bulk_dest, "first", plan.topic, idempotency_key=plan.idempotency_key),
                SendMessage(bulk_dest, "second", plan.topic, idempotency_key=plan.idempotency_key),
            ]
        )
    except Exception as exc:
        duplicate_error = f"{type(exc).__name__}: {exc}"
    bulk_result = bulk_handle.get_result()
    invariant("bulk_duplicate_key_rejected", "duplicate idempotency keys" in duplicate_error and plan.idempotency_key in duplicate_error, error=duplicate_error)
    invariant("bulk_duplicate_no_partial_delivery", bulk_result == "None", result=bulk_result)
    return {"first_timeout": first_timeout, "replay_result": replay_result, "bulk_result": bulk_result, "duplicate_error": duplicate_error}


def stop_listener_for_fallback(dbos: DBOS, plan: CasePlan) -> None:
    sys_db = dbos._sys_db
    sys_db._notification_fallback_polling_interval = plan.fallback_interval_seconds
    sys_db._notification_listener_polling_interval_sec = plan.fallback_interval_seconds
    sys_db._run_background_processes = False
    sys_db._cleanup_connections()
    event(
        "listener_stopped",
        case=plan.case_id,
        use_listen_notify=getattr(sys_db, "use_listen_notify", None),
        fallback_interval_seconds=plan.fallback_interval_seconds,
    )


def fallback_bound_seconds(plan: CasePlan) -> float:
    return max(8.0, min(plan.timeout_seconds, 10.0))


def fallback_recv_listener_stopped(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    with SetWorkflowID(plan.workflow_id):
        handle = DBOS.start_workflow(recv_string_workflow, plan.topic, plan.timeout_seconds)
    stop_listener_for_fallback(dbos, plan)
    listener_stopped = not dbos._sys_db._run_background_processes
    invariant("fallback_recv_listener_stopped_before_send", listener_stopped, listener_stopped=listener_stopped)

    DBOS.send(plan.workflow_id, f"value-{plan.seed}", plan.topic, idempotency_key=plan.idempotency_key)
    started = time.time()
    result = handle.get_result()
    duration = time.time() - started
    expected = f"value-{plan.seed}"
    invariant("fallback_recv_delivered_model_value", result == expected, result=result, expected=expected, duration=duration)
    bound = fallback_bound_seconds(plan)
    invariant("fallback_recv_within_bound", duration < bound, duration=duration, bound_seconds=bound)
    payload = f"{plan.workflow_id}::{plan.topic}"
    invariant("fallback_recv_no_stale_waiter", dbos._sys_db.notifications_map.get(payload) is None, payload=payload)
    return {"result": result, "duration": duration, "payload": payload}


def fallback_get_event_listener_stopped(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    target_workflow_id = plan.workflow_id
    getter_workflow_id = f"{plan.workflow_id}-getter"
    expected = f"value-{plan.seed}"
    with SetWorkflowID(getter_workflow_id):
        getter_handle = DBOS.start_workflow(get_event_string_workflow, target_workflow_id, plan.event_key, plan.timeout_seconds)
    stop_listener_for_fallback(dbos, plan)
    listener_stopped = not dbos._sys_db._run_background_processes
    invariant("fallback_event_listener_stopped_before_set", listener_stopped, listener_stopped=listener_stopped)

    with SetWorkflowID(target_workflow_id):
        set_result = set_event_sync_workflow(plan.event_key, expected)
    started = time.time()
    result = getter_handle.get_result()
    duration = time.time() - started
    all_events = DBOS.get_all_events(target_workflow_id)
    invariant("fallback_event_delivered_model_value", result == expected, result=result, expected=expected, duration=duration, set_result=set_result)
    bound = fallback_bound_seconds(plan)
    invariant("fallback_event_within_bound", duration < bound, duration=duration, bound_seconds=bound)
    invariant("fallback_event_all_events_matches", all_events.get(plan.event_key) == expected, all_events=all_events, expected=expected)
    payload = f"{target_workflow_id}::{plan.event_key}"
    invariant("fallback_event_no_stale_waiter", dbos._sys_db.workflow_events_map.get(payload) is None, payload=payload)
    return {"result": result, "duration": duration, "all_events": all_events, "getter_workflow_id": getter_workflow_id}


def notification_destinations(dbos: DBOS, *, message_uuids: bool = False) -> set[str]:
    from dbos._sys_db import SystemSchema

    col = SystemSchema.notifications.c.message_uuid if message_uuids else SystemSchema.notifications.c.destination_uuid
    with dbos._sys_db.engine.begin() as conn:
        return {row[0] for row in conn.execute(sa.select(col)).all()}


def fork_fanout_duplicate_key(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    def run_root(label: str) -> str:
        workflow_id = f"{plan.workflow_id}-{label}"
        with SetWorkflowID(workflow_id):
            result = forkable_workflow()
        invariant(f"fork_fanout_{label}_root_ran", result == 1, workflow_id=workflow_id, result=result)
        return workflow_id

    def fork(parent_id: str, label: str) -> str:
        fork_id = f"{parent_id}-{label}"
        with SetWorkflowID(fork_id):
            handle = DBOS.fork_workflow(parent_id, 1)
        result = handle.get_result()
        status = handle.get_status()
        invariant(f"fork_fanout_{label}_fork_ran", result == 1 and status.forked_from == parent_id, workflow_id=handle.workflow_id, result=result, forked_from=status.forked_from, parent=parent_id)
        return handle.workflow_id

    root = run_root("root")
    child = fork(root, "child")
    grandchild = fork(child, "grandchild")
    unrelated = run_root("unrelated")
    unrelated_child = fork(unrelated, "unrelated-child")

    DBOS.send(root, "fanout", plan.topic, idempotency_key=plan.idempotency_key, send_to_forks=True)
    DBOS.send(root, "duplicate", plan.topic, idempotency_key=plan.idempotency_key, send_to_forks=True)
    destinations = notification_destinations(dbos)
    message_uuids = notification_destinations(dbos, message_uuids=True)
    expected_destinations = {root, child, grandchild}
    expected_uuids = {f"{plan.idempotency_key}::{destination}" for destination in expected_destinations}
    invariant("fork_fanout_delivery_set_matches", expected_destinations <= destinations and unrelated not in destinations and unrelated_child not in destinations, expected=sorted(expected_destinations), destinations=sorted(destinations), unrelated=unrelated, unrelated_child=unrelated_child)
    invariant("fork_fanout_duplicate_key_once_per_destination", expected_uuids <= message_uuids, expected=sorted(expected_uuids), message_uuids=sorted(message_uuids))
    return {"root": root, "child": child, "grandchild": grandchild, "destinations": sorted(destinations), "message_uuids": sorted(message_uuids)}


def fork_fanout_chain(dbos: DBOS, plan: CasePlan, *, descendant_count: int) -> dict[str, Any]:
    root = f"{plan.workflow_id}-root"
    with SetWorkflowID(root):
        root_result = forkable_workflow()
    invariant("fork_chain_root_ran", root_result == 1, workflow_id=root, result=root_result)

    chain = [root]
    parent = root
    for index in range(1, descendant_count + 1):
        fork_id = f"{plan.workflow_id}-child-{index}"
        with SetWorkflowID(fork_id):
            handle = DBOS.fork_workflow(parent, 1)
        result = handle.get_result()
        status = handle.get_status()
        invariant(
            f"fork_chain_child_{index}_ran",
            result == 1 and status.forked_from == parent,
            workflow_id=handle.workflow_id,
            result=result,
            forked_from=status.forked_from,
            parent=parent,
        )
        chain.append(handle.workflow_id)
        parent = handle.workflow_id

    unrelated = f"{plan.workflow_id}-unrelated"
    with SetWorkflowID(unrelated):
        unrelated_result = forkable_workflow()
    invariant("fork_chain_unrelated_ran", unrelated_result == 1, workflow_id=unrelated, result=unrelated_result)

    DBOS.send(root, "chain-fanout", plan.topic, idempotency_key=plan.idempotency_key, send_to_forks=True)
    DBOS.send(root, "chain-duplicate", plan.topic, idempotency_key=plan.idempotency_key, send_to_forks=True)
    destinations = notification_destinations(dbos)
    message_uuids = notification_destinations(dbos, message_uuids=True)
    expected_destinations = set(chain)
    expected_uuids = {f"{plan.idempotency_key}::{destination}" for destination in expected_destinations}
    invariant(
        "fork_chain_delivery_set_matches",
        expected_destinations <= destinations and unrelated not in destinations,
        expected=sorted(expected_destinations),
        destinations=sorted(destinations),
        unrelated=unrelated,
    )
    invariant("fork_chain_duplicate_key_once_per_destination", expected_uuids <= message_uuids, expected=sorted(expected_uuids), message_uuids=sorted(message_uuids))
    return {"root": root, "chain": chain, "unrelated": unrelated, "destinations": sorted(destinations), "message_uuids": sorted(message_uuids)}


def fork_event_step_boundaries(plan: CasePlan) -> dict[str, Any]:
    prefix = f"event-{plan.seed}"
    with SetWorkflowID(plan.workflow_id):
        root_result = event_boundary_workflow(prefix)
    invariant("fork_event_root_completed", root_result == "event-boundary-done", result=root_result)

    forks: dict[str, dict[str, Any]] = {}
    for start_step in (2, 3):
        fork_id = f"{plan.workflow_id}-fork-step-{start_step}"
        with SetWorkflowID(fork_id):
            handle = DBOS.fork_workflow(plan.workflow_id, start_step)
        pre_events = DBOS.get_all_events(handle.workflow_id)
        result = handle.get_result()
        post_events = DBOS.get_all_events(handle.workflow_id)
        forks[str(start_step)] = {"workflow_id": handle.workflow_id, "pre": pre_events, "post": post_events, "result": result}

    root_events = DBOS.get_all_events(plan.workflow_id)
    expected_keys = {f"{prefix}-root", f"{prefix}-step-1", f"{prefix}-step-2", f"{prefix}-step-3", f"{prefix}-final"}
    invariant("fork_event_root_events_complete", expected_keys <= set(root_events), expected=sorted(expected_keys), root_events=root_events)
    for step, data in forks.items():
        post_events = data["post"]
        invariant(f"fork_event_step_{step}_post_converges", expected_keys <= set(post_events) and post_events.get(f"{prefix}-final") == "final", workflow_id=data["workflow_id"], post_events=post_events)
    invariant("fork_event_pre_state_captured", all(data["pre"] for data in forks.values()), forks=forks)
    return {"root_events": root_events, "forks": forks}


def fork_event_single_step(plan: CasePlan, *, start_step: int) -> dict[str, Any]:
    prefix = f"event-{plan.seed}"
    with SetWorkflowID(plan.workflow_id):
        root_result = event_boundary_workflow(prefix)
    invariant("fork_event_root_completed", root_result == "event-boundary-done", result=root_result)

    fork_id = f"{plan.workflow_id}-fork-step-{start_step}"
    with SetWorkflowID(fork_id):
        handle = DBOS.fork_workflow(plan.workflow_id, start_step)
    pre_events = DBOS.get_all_events(handle.workflow_id)
    result = handle.get_result()
    post_events = DBOS.get_all_events(handle.workflow_id)
    expected_keys = {f"{prefix}-root", f"{prefix}-step-1", f"{prefix}-step-2", f"{prefix}-step-3", f"{prefix}-final"}
    invariant("fork_event_single_result", result == "event-boundary-done", workflow_id=handle.workflow_id, result=result, start_step=start_step)
    invariant("fork_event_single_post_converges", expected_keys <= set(post_events) and post_events.get(f"{prefix}-final") == "final", workflow_id=handle.workflow_id, post_events=post_events, expected=sorted(expected_keys))
    invariant("fork_event_single_pre_state_captured", bool(pre_events), workflow_id=handle.workflow_id, pre_events=pre_events, start_step=start_step)
    return {"workflow_id": handle.workflow_id, "start_step": start_step, "pre": pre_events, "post": post_events, "result": result}


def interleaved_stream_offsets(plan: CasePlan) -> dict[str, Any]:
    sequences = {
        f"{plan.topic}-hot": [f"hot-{i}-{plan.seed}" for i in range(5)],
        f"{plan.topic}-warm": [f"warm-{i}-{plan.seed}" for i in range(3)],
        f"{plan.topic}-cold": [f"cold-{i}-{plan.seed}" for i in range(2)],
    }
    with SetWorkflowID(plan.workflow_id):
        result = stream_writer_workflow(sequences)
    invariant("stream_writer_completed", result == "stream-writer-done", result=result)
    observed = {key: list(DBOS.read_stream(plan.workflow_id, key)) for key in sequences}
    invariant("stream_values_match_model", observed == sequences, observed=observed, expected=sequences)
    offset_reads = {key: list(DBOS.read_stream(plan.workflow_id, key, offset=min(2, len(values)))) for key, values in sequences.items()}
    expected_offsets = {key: values[min(2, len(values)):] for key, values in sequences.items()}
    invariant("stream_offset_reads_match_model", offset_reads == expected_offsets, observed=offset_reads, expected=expected_offsets)
    return {"observed": observed, "offset_reads": offset_reads}


def stream_matrix(plan: CasePlan, *, key_count: int, hot_length: int) -> dict[str, Any]:
    sequences: dict[str, list[str]] = {}
    for index in range(key_count):
        label = "hot" if index == 0 else f"key-{index}"
        length = hot_length if index == 0 else max(2, hot_length - index)
        sequences[f"{plan.topic}-{label}"] = [f"{label}-{item}-{plan.seed}" for item in range(length)]
    with SetWorkflowID(plan.workflow_id):
        result = stream_writer_workflow(sequences)
    invariant("stream_matrix_writer_completed", result == "stream-writer-done", result=result, key_count=key_count, hot_length=hot_length)
    observed = {key: list(DBOS.read_stream(plan.workflow_id, key)) for key in sequences}
    invariant("stream_matrix_values_match_model", observed == sequences, observed=observed, expected=sequences)
    offset_reads = {key: list(DBOS.read_stream(plan.workflow_id, key, offset=1)) for key in sequences}
    expected_offsets = {key: values[1:] for key, values in sequences.items()}
    invariant("stream_matrix_offset_reads_match_model", offset_reads == expected_offsets, observed=offset_reads, expected=expected_offsets)
    return {"observed": observed, "offset_reads": offset_reads, "key_count": key_count, "hot_length": hot_length}


def duplicate_send_before_timeout(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    with SetWorkflowID(plan.workflow_id):
        handle = DBOS.start_workflow(recv_one_workflow, plan.topic, plan.timeout_seconds)
    pending = wait_for_status(plan.workflow_id, "PENDING")
    invariant("duplicate_before_timeout_receiver_pending", pending is not None and pending["status"] == "PENDING", status=pending)
    DBOS.send(plan.workflow_id, "first", plan.topic, idempotency_key=plan.idempotency_key)
    DBOS.send(plan.workflow_id, "duplicate", plan.topic, idempotency_key=plan.idempotency_key)
    result = handle.get_result()
    message_uuids = notification_destinations(dbos, message_uuids=True)
    expected_uuid = f"{plan.idempotency_key}::{plan.workflow_id}"
    invariant("duplicate_before_timeout_delivered_once", result == "first", result=result, expected="first")
    invariant("duplicate_before_timeout_one_message_uuid", expected_uuid in message_uuids, expected=expected_uuid, message_uuids=sorted(message_uuids))
    return {"result": result, "message_uuids": sorted(message_uuids), "status": status_snapshot(plan.workflow_id)}


def duplicate_send_after_timeout(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    with SetWorkflowID(plan.workflow_id):
        handle = DBOS.start_workflow(recv_timeout_workflow, plan.topic, plan.timeout_seconds)
    first = handle.get_result()
    if plan.late_send_offset_ms:
        time.sleep(plan.late_send_offset_ms / 1000)
    DBOS.send(plan.workflow_id, "late", plan.topic, idempotency_key=plan.idempotency_key)
    DBOS.send(plan.workflow_id, "late-duplicate", plan.topic, idempotency_key=plan.idempotency_key)
    replay = DBOS.retrieve_workflow(plan.workflow_id).get_result()
    message_uuids = notification_destinations(dbos, message_uuids=True)
    expected_uuid = f"{plan.idempotency_key}::{plan.workflow_id}"
    invariant("duplicate_after_timeout_result_stable_none", first is None and replay is None, first=first, replay=replay, status=status_snapshot(plan.workflow_id))
    invariant("duplicate_after_timeout_one_message_uuid", expected_uuid in message_uuids, expected=expected_uuid, message_uuids=sorted(message_uuids))
    return {"first": first, "replay": replay, "message_uuids": sorted(message_uuids), "status": status_snapshot(plan.workflow_id)}


def bulk_duplicate_reject_valid_send(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    receiver_id = f"{plan.workflow_id}-bulk"
    with SetWorkflowID(receiver_id):
        handle = DBOS.start_workflow(recv_one_workflow, plan.topic, plan.timeout_seconds)
    pending = wait_for_status(receiver_id, "PENDING")
    invariant("bulk_duplicate_receiver_pending", pending is not None and pending["status"] == "PENDING", status=pending)
    duplicate_error = ""
    try:
        DBOS.send_bulk(
            [
                SendMessage(receiver_id, "first", plan.topic, idempotency_key=plan.idempotency_key),
                SendMessage(receiver_id, "second", plan.topic, idempotency_key=plan.idempotency_key),
            ]
        )
    except Exception as exc:
        duplicate_error = f"{type(exc).__name__}: {exc}"
    message_uuids_after_reject = notification_destinations(dbos, message_uuids=True)
    invariant("bulk_duplicate_key_rejected", "duplicate idempotency keys" in duplicate_error and plan.idempotency_key in duplicate_error, error=duplicate_error)
    invariant("bulk_duplicate_no_partial_delivery", not message_uuids_after_reject, message_uuids=sorted(message_uuids_after_reject))
    expected = f"valid-{plan.seed}"
    DBOS.send(receiver_id, expected, plan.topic, idempotency_key=f"{plan.idempotency_key}-valid")
    result = handle.get_result()
    invariant("bulk_valid_send_after_reject_delivered_once", result == expected, result=result, expected=expected, status=status_snapshot(receiver_id))
    return {"receiver_id": receiver_id, "duplicate_error": duplicate_error, "result": result, "message_uuids_after_reject": sorted(message_uuids_after_reject)}


def mixed_small_session(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    receiver_id = f"{plan.workflow_id}-mixed-receiver"
    with SetWorkflowID(receiver_id):
        receiver_handle = DBOS.start_workflow(recv_one_workflow, plan.topic, plan.timeout_seconds)
    stop_listener_for_fallback(dbos, plan)
    DBOS.send(receiver_id, "mixed-message", plan.topic, idempotency_key=plan.idempotency_key)
    DBOS.send(receiver_id, "mixed-duplicate", plan.topic, idempotency_key=plan.idempotency_key)
    recv_result = receiver_handle.get_result()
    invariant("mixed_recv_duplicate_delivered_once", recv_result == "mixed-message", result=recv_result)

    target_workflow_id = f"{plan.workflow_id}-mixed-event-target"
    with SetWorkflowID(target_workflow_id):
        event_result = set_event_sync_workflow(plan.event_key, f"mixed-event-{plan.seed}")
    all_events = DBOS.get_all_events(target_workflow_id)
    invariant("mixed_event_get_all_matches", all_events.get(plan.event_key) == event_result, all_events=all_events, expected=event_result)

    stream_id = f"{plan.workflow_id}-mixed-stream"
    sequences = {f"{plan.topic}-mixed": [f"mixed-stream-{index}-{plan.seed}" for index in range(3)]}
    with SetWorkflowID(stream_id):
        stream_result = stream_writer_workflow(sequences)
    observed_stream = {key: list(DBOS.read_stream(stream_id, key)) for key in sequences}
    invariant("mixed_stream_values_match", stream_result == "stream-writer-done" and observed_stream == sequences, result=stream_result, observed=observed_stream, expected=sequences)

    message_uuids = notification_destinations(dbos, message_uuids=True)
    expected_uuid = f"{plan.idempotency_key}::{receiver_id}"
    invariant("mixed_message_uuid_once", expected_uuid in message_uuids, expected=expected_uuid, message_uuids=sorted(message_uuids))
    return {"recv_result": recv_result, "event_result": event_result, "all_events": all_events, "observed_stream": observed_stream, "message_uuids": sorted(message_uuids)}


async def relaunch_after_cancelled_recv_reuse(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    cancel_result = await cancel_recv_setup_window(dbos, plan)
    first_executor = executor_a(plan)
    dbos, handles = relaunch(plan, from_executor_id=first_executor)
    invariant("relaunch_executed_after_recv_cancel", not handles, handles=[handle.workflow_id for handle in handles])

    reuse_workflow_id = f"{plan.workflow_id}-reuse"
    with SetWorkflowID(reuse_workflow_id):
        handle = DBOS.start_workflow(recv_string_workflow, plan.topic, plan.timeout_seconds)
    pending = await run_blocking(wait_for_status, reuse_workflow_id, "PENDING")
    invariant("recv_reuse_pending_after_relaunch", pending is not None and pending["status"] == "PENDING", status=pending)
    expected = f"reuse-{plan.seed}"
    await DBOS.send_async(reuse_workflow_id, expected, plan.topic, idempotency_key=f"{plan.idempotency_key}-reuse")
    result = await run_blocking(handle.get_result)
    status = await run_blocking(status_snapshot, reuse_workflow_id)
    payload = f"{reuse_workflow_id}::{plan.topic}"
    invariant("recv_reuse_after_relaunch_delivered", result == expected, result=result, expected=expected, status=status)
    invariant("recv_reuse_after_relaunch_no_stale_waiter", dbos._sys_db.notifications_map.get(payload) is None, payload=payload)
    return {"cancel": cancel_result, "reuse_workflow_id": reuse_workflow_id, "result": result, "status": status}


async def relaunch_after_cancelled_event_reuse(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    cancel_result = await cancel_event_setup_window(dbos, plan)
    first_executor = executor_a(plan)
    dbos, handles = relaunch(plan, from_executor_id=first_executor)
    invariant("relaunch_executed_after_event_cancel", not handles, handles=[handle.workflow_id for handle in handles])

    target_workflow_id = f"{plan.workflow_id}-target"
    getter_workflow_id = f"{plan.workflow_id}-getter"
    expected = f"event-reuse-{plan.seed}"
    with SetWorkflowID(target_workflow_id):
        noop_workflow()
    with SetWorkflowID(getter_workflow_id):
        getter_handle = DBOS.start_workflow(get_event_string_workflow, target_workflow_id, plan.event_key, plan.timeout_seconds)
    pending = await run_blocking(wait_for_status, getter_workflow_id, "PENDING")
    invariant("event_reuse_getter_pending_after_relaunch", pending is not None and pending["status"] == "PENDING", status=pending)
    def set_event_after_relaunch() -> str:
        dbos._sys_db.set_event_from_workflow(
            target_workflow_id,
            9002,
            plan.event_key,
            expected,
            serialization_type=WorkflowSerializationFormat.DEFAULT,
        )
        return expected

    set_result = await run_blocking(set_event_after_relaunch)
    result = await run_blocking(getter_handle.get_result)
    events = await run_blocking(DBOS.get_all_events, target_workflow_id)
    payload = f"{target_workflow_id}::{plan.event_key}"
    invariant("event_reuse_after_relaunch_delivered", result == expected and set_result == expected, result=result, expected=expected, set_result=set_result)
    invariant("event_reuse_after_relaunch_events_match", events.get(plan.event_key) == expected, events=events, expected=expected)
    invariant("event_reuse_after_relaunch_no_stale_waiter", dbos._sys_db.workflow_events_map.get(payload) is None, payload=payload)
    return {"cancel": cancel_result, "getter_workflow_id": getter_workflow_id, "target_workflow_id": target_workflow_id, "result": result, "events": events}


def timeout_replay_after_relaunch(plan: CasePlan) -> dict[str, Any]:
    with SetWorkflowID(plan.workflow_id):
        timeout_handle = DBOS.start_workflow(recv_timeout_workflow, plan.topic, plan.timeout_seconds)
    first = timeout_handle.get_result()
    invariant("timeout_before_relaunch_none", first is None, result=first, status=status_snapshot(plan.workflow_id))

    relaunch(plan, from_executor_id=executor_a(plan))
    recovered = DBOS.retrieve_workflow(plan.workflow_id).get_result()
    if plan.late_send_offset_ms:
        time.sleep(plan.late_send_offset_ms / 1000)
    DBOS.send(plan.workflow_id, "late-after-relaunch", plan.topic, idempotency_key=plan.idempotency_key)
    replay = DBOS.retrieve_workflow(plan.workflow_id).get_result()
    invariant("timeout_relaunch_retrieval_stable_none", recovered is None and replay is None, recovered=recovered, replay=replay, status=status_snapshot(plan.workflow_id))
    return {"first": first, "recovered": recovered, "replay": replay, "status": status_snapshot(plan.workflow_id)}


def fallback_waiter_relaunch(plan: CasePlan) -> dict[str, Any]:
    with SetWorkflowID(plan.workflow_id):
        handle = DBOS.start_workflow(recv_string_workflow, plan.topic, plan.timeout_seconds)
    pending = wait_for_status(plan.workflow_id, "PENDING")
    invariant("fallback_relaunch_waiter_pending_before_destroy", pending is not None and pending["status"] == "PENDING", status=pending)

    dbos, handles = relaunch(plan, from_executor_id=executor_a(plan))
    recovered_ids = [recovered_handle.workflow_id for recovered_handle in handles]
    invariant("fallback_relaunch_recovered_waiter", plan.workflow_id in recovered_ids, recovered_ids=recovered_ids)
    stop_listener_for_fallback(dbos, plan)
    expected = f"fallback-relaunch-{plan.seed}"
    DBOS.send(plan.workflow_id, expected, plan.topic, idempotency_key=plan.idempotency_key)
    recovered_handle = next(recovered_handle for recovered_handle in handles if recovered_handle.workflow_id == plan.workflow_id)
    result = recovered_handle.get_result()
    status = status_snapshot(plan.workflow_id)
    payload = f"{plan.workflow_id}::{plan.topic}"
    invariant("fallback_relaunch_delivered_model_value", result == expected, result=result, expected=expected, status=status)
    invariant("fallback_relaunch_no_stale_waiter", dbos._sys_db.notifications_map.get(payload) is None, payload=payload)
    return {"pending_before": pending, "recovered_ids": recovered_ids, "result": result, "status": status}


def fork_tree_relaunch_before_fanout(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    built = fork_fanout_duplicate_key(dbos, plan)
    dbos, _ = relaunch(plan, from_executor_id=executor_a(plan))
    DBOS.send(built["root"], "after-relaunch", f"{plan.topic}-after", idempotency_key=f"{plan.idempotency_key}-after", send_to_forks=True)
    destinations = notification_destinations(dbos)
    expected = {built["root"], built["child"], built["grandchild"]}
    invariant("fork_tree_after_relaunch_delivery_set_matches", expected <= destinations, expected=sorted(expected), destinations=sorted(destinations))
    return {"before": built, "after_destinations": sorted(destinations)}


def stream_relaunch_before_close(plan: CasePlan) -> dict[str, Any]:
    sequences = {
        f"{plan.topic}-hot": [f"hot-{i}-{plan.seed}" for i in range(6)],
        f"{plan.topic}-warm": [f"warm-{i}-{plan.seed}" for i in range(4)],
    }
    _GATES[plan.workflow_id] = threading.Event()
    _GATE_PREFIX_READY[plan.workflow_id] = threading.Event()
    with SetWorkflowID(plan.workflow_id):
        handle = DBOS.start_workflow(gated_stream_writer_workflow, plan.workflow_id, sequences)
    prefix_ready = wait_for_condition(
        "stream_relaunch_prefix_attempted_before_destroy",
        lambda: _GATE_PREFIX_READY[plan.workflow_id].is_set(),
    )
    pending = status_snapshot(plan.workflow_id)
    invariant("stream_relaunch_workflow_pending_before_destroy", pending is not None and pending["status"] == "PENDING", status=pending)

    old_gate = _GATES[plan.workflow_id]
    _GATES[plan.workflow_id] = threading.Event()
    _, handles = relaunch(plan, from_executor_id=executor_a(plan))
    recovered_ids = [recovered_handle.workflow_id for recovered_handle in handles]
    invariant("stream_relaunch_recovered_writer", plan.workflow_id in recovered_ids, recovered_ids=recovered_ids)
    _GATES[plan.workflow_id].set()
    recovered_handle = next(recovered_handle for recovered_handle in handles if recovered_handle.workflow_id == plan.workflow_id)
    result = recovered_handle.get_result()
    observed = {key: list(DBOS.read_stream(plan.workflow_id, key)) for key in sequences}
    invariant("stream_relaunch_result_matches_workflow", result == plan.workflow_id, result=result, workflow_id=plan.workflow_id)
    invariant("stream_relaunch_sequences_exact", observed == sequences, observed=observed, expected=sequences)
    old_gate.set()
    return {"prefix_ready": prefix_ready, "recovered_ids": recovered_ids, "observed": observed}


def repeated_cancel_reuse_across_relaunch(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    recv_result = asyncio.run(cancel_recv_setup_window(dbos, plan))
    dbos, _ = relaunch(plan, from_executor_id=executor_a(plan))
    event_result = asyncio.run(cancel_event_setup_window(dbos, plan))
    relaunch(plan, from_executor_id=executor_b(plan), to_executor_id=executor_c(plan))
    final_recv_id = f"{plan.workflow_id}-final-recv"
    with SetWorkflowID(final_recv_id):
        handle = DBOS.start_workflow(recv_string_workflow, plan.topic, plan.timeout_seconds)
    wait_for_status(final_recv_id, "PENDING")
    expected = f"final-{plan.seed}"
    DBOS.send(final_recv_id, expected, plan.topic, idempotency_key=f"{plan.idempotency_key}-final")
    result = handle.get_result()
    status = status_snapshot(final_recv_id)
    invariant("repeated_cancel_final_reuse_delivered", result == expected, result=result, expected=expected, status=status)
    return {"recv": recv_result, "event": event_result, "final_recv_id": final_recv_id, "result": result}


def relaunch_then_bulk_reject_valid_send(plan: CasePlan) -> dict[str, Any]:
    receiver_id = f"{plan.workflow_id}-receiver"
    with SetWorkflowID(receiver_id):
        receiver_handle = DBOS.start_workflow(recv_one_workflow, plan.topic, plan.timeout_seconds)
    pending = wait_for_status(receiver_id, "PENDING")
    invariant("bulk_relaunch_receiver_pending_before_destroy", pending is not None and pending["status"] == "PENDING", status=pending)
    dbos, handles = relaunch(plan, from_executor_id=executor_a(plan))
    recovered_ids = [handle.workflow_id for handle in handles]
    invariant("bulk_relaunch_receiver_recovered", receiver_id in recovered_ids, recovered_ids=recovered_ids)

    duplicate_error = ""
    try:
        DBOS.send_bulk(
            [
                SendMessage(receiver_id, "first", plan.topic, idempotency_key=plan.idempotency_key),
                SendMessage(receiver_id, "second", plan.topic, idempotency_key=plan.idempotency_key),
            ]
        )
    except Exception as exc:
        duplicate_error = f"{type(exc).__name__}: {exc}"
    invariant("bulk_relaunch_duplicate_key_rejected", "duplicate idempotency keys" in duplicate_error and plan.idempotency_key in duplicate_error, error=duplicate_error)
    notification_message_ids = notification_destinations(dbos, message_uuids=True)
    duplicate_message_ids = {
        f"{plan.idempotency_key}::{receiver_id}",
    }
    invariant(
        "bulk_relaunch_duplicate_no_partial_delivery",
        duplicate_message_ids.isdisjoint(notification_message_ids),
        duplicate_message_ids=sorted(duplicate_message_ids),
        notification_message_ids=sorted(notification_message_ids),
    )
    expected = f"valid-{plan.seed}"
    DBOS.send(receiver_id, expected, plan.topic, idempotency_key=f"{plan.idempotency_key}-valid")
    recovered_handle = next(handle for handle in handles if handle.workflow_id == receiver_id)
    result = recovered_handle.get_result()
    invariant("bulk_relaunch_valid_send_delivered_once", result == expected, result=result, expected=expected, status=status_snapshot(receiver_id))
    return {"receiver_id": receiver_id, "duplicate_error": duplicate_error, "notification_message_ids": sorted(notification_message_ids), "result": result}


def gated_live_resume(
    *,
    dbos: DBOS,
    plan: CasePlan,
    reader_name: str,
    workflow_suffix: str,
    stream_key: str,
    label: str,
    listener_sys_db: Any,
    prefix_iterator_factory: Any,
    resumed_reader_factory: Any,
    timeout_seconds: float,
    latency_bound_seconds: float,
) -> dict[str, Any]:
    count = 6
    prefix_count = 2
    workflow_id = f"{plan.workflow_id}-{workflow_suffix}"
    _GATES[workflow_id] = threading.Event()
    _GATE_PREFIX_READY[workflow_id] = threading.Event()
    with SetWorkflowID(workflow_id):
        handle = DBOS.start_workflow(
            gated_live_stream_writer_workflow,
            workflow_id,
            stream_key,
            count,
            prefix_count,
            plan.seed,
            label,
            0.2,
            True,
        )
    prefix_ready = wait_for_condition(
        f"{reader_name}_writer_prefix_ready",
        lambda: _GATE_PREFIX_READY[workflow_id].is_set(),
        timeout_seconds=20.0,
    )
    stored_prefix = wait_for_stream_prefix(dbos, workflow_id, stream_key, prefix_count, timeout_seconds=20.0)
    consumed_prefix = consume_stream_prefix(prefix_iterator_factory(workflow_id, stream_key), prefix_count)
    assert_stream_suffix(
        f"{reader_name}_prefix_matches_model",
        consumed_prefix,
        offset=0,
        count=prefix_count,
        seed=plan.seed,
        label=label,
    )
    assert_stream_suffix(
        f"{reader_name}_stored_prefix_matches_model",
        stored_prefix,
        offset=0,
        count=prefix_count,
        seed=plan.seed,
        label=label,
    )
    assert_no_stream_listener(f"{reader_name}_prefix_reader_unregistered", listener_sys_db, workflow_id, stream_key)

    holder, thread, started_at = start_collector_thread(
        reader_name,
        lambda: resumed_reader_factory(workflow_id, stream_key, prefix_count),
    )
    registration = wait_for_condition(
        f"{reader_name}_resumed_reader_registered",
        lambda: stream_listener_snapshot(listener_sys_db, workflow_id=workflow_id, key=stream_key),
        timeout_seconds=20.0,
    )
    event(f"{reader_name}_reader_registered", snapshot=registration)
    _GATES[workflow_id].set()
    reader_result = finish_collector_thread(
        reader_name,
        holder,
        thread,
        started_at,
        timeout_seconds=timeout_seconds,
    )
    writer_result = handle.get_result()
    assert_stream_suffix(
        f"{reader_name}_resumed_suffix_matches_model",
        reader_result["values"],
        offset=prefix_count,
        count=count,
        seed=plan.seed,
        label=label,
    )
    invariant(
        f"{reader_name}_live_delivery_within_bound",
        reader_result["live_ordinals"] and reader_result["max_live_latency"] < latency_bound_seconds,
        live_ordinals=reader_result["live_ordinals"],
        max_live_latency=reader_result["max_live_latency"],
        latency_bound_seconds=latency_bound_seconds,
        polling_interval_seconds=getattr(listener_sys_db, "_notification_listener_polling_interval_sec", None),
    )
    invariant(
        f"{reader_name}_writer_closed_stream",
        writer_result["closed"] and writer_result["count"] == count,
        writer_result=writer_result,
        status=status_snapshot(workflow_id),
    )
    assert_no_stream_listener(f"{reader_name}_resumed_reader_unregistered", listener_sys_db, workflow_id, stream_key)
    return {
        "workflow_id": workflow_id,
        "stream_key": stream_key,
        "prefix_ready": prefix_ready,
        "stored_prefix": stored_prefix,
        "consumed_prefix": consumed_prefix,
        "reader": reader_result,
        "writer_result": writer_result,
        "status": status_snapshot(workflow_id),
    }


def runtime_live_reconnect_offsets(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    dbos._sys_db._notification_listener_polling_interval_sec = 8.0
    sync_result = gated_live_resume(
        dbos=dbos,
        plan=plan,
        reader_name="runtime_sync_live_resume",
        workflow_suffix="runtime-sync",
        stream_key=f"{plan.topic}-runtime-sync",
        label="runtime-sync",
        listener_sys_db=dbos._sys_db,
        prefix_iterator_factory=lambda workflow_id, key: DBOS.read_stream(workflow_id, key),
        resumed_reader_factory=lambda workflow_id, key, offset: collect_sync_stream(
            "runtime_sync_live_resume",
            DBOS.read_stream(workflow_id, key, offset=offset),
        ),
        timeout_seconds=30.0,
        latency_bound_seconds=4.0,
    )
    async_result = gated_live_resume(
        dbos=dbos,
        plan=plan,
        reader_name="runtime_async_live_resume",
        workflow_suffix="runtime-async",
        stream_key=f"{plan.topic}-runtime-async",
        label="runtime-async",
        listener_sys_db=dbos._sys_db,
        prefix_iterator_factory=lambda workflow_id, key: DBOS.read_stream(workflow_id, key),
        resumed_reader_factory=lambda workflow_id, key, offset: asyncio.run(
            collect_async_stream(
                "runtime_async_live_resume",
                DBOS.read_stream_async(workflow_id, key, offset=offset, polling_interval_sec=30.0),
            )
        ),
        timeout_seconds=30.0,
        latency_bound_seconds=4.0,
    )
    return {"runtime_sync": sync_result, "runtime_async": async_result}


def client_and_fallback_resume(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    results: dict[str, Any] = {}
    client = DBOSClient(system_database_url=db_url(plan.sys_db))
    client._sys_db._notification_listener_polling_interval_sec = plan.fallback_interval_seconds
    try:
        results["client_sync"] = gated_live_resume(
            dbos=dbos,
            plan=plan,
            reader_name="client_sync_live_resume",
            workflow_suffix="client-sync",
            stream_key=f"{plan.topic}-client-sync",
            label="client-sync",
            listener_sys_db=client._sys_db,
            prefix_iterator_factory=lambda workflow_id, key: client.read_stream(workflow_id, key),
            resumed_reader_factory=lambda workflow_id, key, offset: collect_sync_stream(
                "client_sync_live_resume",
                client.read_stream(workflow_id, key, offset=offset),
            ),
            timeout_seconds=30.0,
            latency_bound_seconds=8.0,
        )
        results["client_async"] = gated_live_resume(
            dbos=dbos,
            plan=plan,
            reader_name="client_async_live_resume",
            workflow_suffix="client-async",
            stream_key=f"{plan.topic}-client-async",
            label="client-async",
            listener_sys_db=client._sys_db,
            prefix_iterator_factory=lambda workflow_id, key: client.read_stream(workflow_id, key),
            resumed_reader_factory=lambda workflow_id, key, offset: asyncio.run(
                collect_async_stream(
                    "client_async_live_resume",
                    client.read_stream_async(workflow_id, key, offset=offset),
                )
            ),
            timeout_seconds=30.0,
            latency_bound_seconds=8.0,
        )
    finally:
        client.destroy()

    stop_listener_for_fallback(dbos, plan)
    results["runtime_fallback"] = gated_live_resume(
        dbos=dbos,
        plan=plan,
        reader_name="runtime_fallback_live_resume",
        workflow_suffix="runtime-fallback",
        stream_key=f"{plan.topic}-runtime-fallback",
        label="runtime-fallback",
        listener_sys_db=dbos._sys_db,
        prefix_iterator_factory=lambda workflow_id, key: DBOS.read_stream(workflow_id, key),
        resumed_reader_factory=lambda workflow_id, key, offset: collect_sync_stream(
            "runtime_fallback_live_resume",
            DBOS.read_stream(workflow_id, key, offset=offset),
        ),
        timeout_seconds=30.0,
        latency_bound_seconds=8.0,
    )
    return results


def blocked_reader_termination_relaunch(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    workflow_id = f"{plan.workflow_id}-unclosed"
    stream_key = f"{plan.topic}-unclosed"
    count = 2
    dbos._sys_db._notification_listener_polling_interval_sec = plan.fallback_interval_seconds
    with SetWorkflowID(workflow_id):
        handle = DBOS.start_workflow(
            unclosed_live_stream_writer_workflow,
            stream_key,
            count,
            plan.seed,
            "blocked-termination",
            0.1,
            2.0,
        )
    wait_for_stream_prefix(dbos, workflow_id, stream_key, count, timeout_seconds=20.0)
    holder, thread, started_at = start_collector_thread(
        "blocked_reader_termination",
        lambda: collect_sync_stream(
            "blocked_reader_termination",
            DBOS.read_stream(workflow_id, stream_key, offset=1),
        ),
    )
    registration = wait_for_condition(
        "blocked_reader_registered_before_workflow_terminal",
        lambda: stream_listener_snapshot(dbos._sys_db, workflow_id=workflow_id, key=stream_key),
        timeout_seconds=20.0,
    )
    status_while_blocked = status_snapshot(workflow_id)
    invariant(
        "blocked_reader_observed_pending_workflow",
        status_while_blocked is not None and status_while_blocked["status"] == "PENDING",
        status=status_while_blocked,
        registration=registration,
    )
    reader_result = finish_collector_thread(
        "blocked_reader_termination",
        holder,
        thread,
        started_at,
        timeout_seconds=20.0,
    )
    writer_result = handle.get_result()
    status_after = status_snapshot(workflow_id)
    assert_stream_suffix(
        "blocked_reader_terminal_suffix_matches_model",
        reader_result["values"],
        offset=1,
        count=count,
        seed=plan.seed,
        label="blocked-termination",
    )
    invariant(
        "blocked_reader_exited_after_workflow_terminal",
        status_after is not None and status_after["status"] == "SUCCESS" and reader_result["duration"] < 8.0,
        status=status_after,
        reader_duration=reader_result["duration"],
        writer_result=writer_result,
    )
    assert_no_stream_listener("blocked_reader_unregistered_after_terminal", dbos._sys_db, workflow_id, stream_key)

    DBOS.destroy(destroy_registry=False)
    dbos = launch(plan, executor_id=executor_b(plan), clean=False)
    dbos._sys_db._notification_listener_polling_interval_sec = plan.fallback_interval_seconds
    relaunched_reader = collect_sync_stream(
        "blocked_reader_post_relaunch_resume",
        DBOS.read_stream(workflow_id, stream_key, offset=1),
    )
    assert_stream_suffix(
        "blocked_reader_post_relaunch_suffix_matches_model",
        relaunched_reader["values"],
        offset=1,
        count=count,
        seed=plan.seed,
        label="blocked-termination",
    )
    assert_no_stream_listener("blocked_reader_post_relaunch_unregistered", dbos._sys_db, workflow_id, stream_key)
    return {
        "workflow_id": workflow_id,
        "stream_key": stream_key,
        "reader": reader_result,
        "writer_result": writer_result,
        "status_after": status_after,
        "relaunched_reader": relaunched_reader,
        "post_relaunch_status": status_snapshot(workflow_id),
    }


def client_sync_delayed_event(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    client = make_client(plan)
    target_workflow_id = f"{plan.workflow_id}-client-sync"
    key = f"{plan.event_key}-sync"
    expected_label = f"sync-value-{plan.seed}"
    delay_seconds = 0.4
    payload = client_event_payload(target_workflow_id, key)
    try:
        polling = assert_client_polling_contract(client, plan, "client_sync_event")
        holder, thread, started_at = start_client_get_thread(client, target_workflow_id, key, plan.timeout_seconds)
        registration = wait_for_condition(
            "client_sync_waiter_registered_before_set",
            lambda: client._sys_db.workflow_events_map.get(payload) is not None,
            timeout_seconds=5.0,
        )
        handle = start_workflow_with_id(target_workflow_id, client_delayed_event_workflow, key, expected_label, delay_seconds)
        wait_result = finish_client_get_thread(
            "client_sync_get_event_prompt",
            holder,
            thread,
            started_at,
            timeout_seconds=plan.timeout_seconds + 2.0,
        )
        handle_result = handle.get_result()
        events = DBOS.get_all_events(target_workflow_id)
        status = status_snapshot(target_workflow_id)
        delivered_value = wait_result["value"]
        final_event = events.get(key)
        live_latency = wait_result["completed_at"] - delivered_value.get("written_at", wait_result["completed_at"]) if isinstance(delivered_value, dict) else plan.timeout_seconds
        bound = client_prompt_bound_seconds(plan)
        invariant(
            "client_sync_get_event_delivered_model_value",
            isinstance(delivered_value, dict)
            and isinstance(handle_result, dict)
            and isinstance(final_event, dict)
            and delivered_value.get("label") == expected_label
            and handle_result.get("label") == expected_label
            and final_event.get("label") == expected_label,
            result=wait_result,
            expected_label=expected_label,
            handle_result=handle_result,
            events=events,
            status=status,
        )
        invariant(
            "client_sync_get_event_within_prompt_bound",
            live_latency < bound,
            live_latency=live_latency,
            total_wait_duration=wait_result["duration"],
            bound_seconds=bound,
            timeout_seconds=plan.timeout_seconds,
            polling=polling,
        )
        invariant(
            "client_sync_event_table_matches_model",
            isinstance(final_event, dict) and final_event.get("label") == expected_label,
            workflow_id=target_workflow_id,
            key=key,
            events=events,
            expected_label=expected_label,
        )
        invariant(
            "client_sync_get_event_no_stale_waiter",
            client._sys_db.workflow_events_map.get(payload) is None,
            payload=payload,
            registration=registration,
        )
        return {
            "workflow_id": target_workflow_id,
            "key": key,
            "payload": payload,
            "expected_label": expected_label,
            "wait_result": wait_result,
            "live_latency": live_latency,
            "handle_result": handle_result,
            "events": events,
            "status": status,
            "bound_seconds": bound,
            "polling": polling,
        }
    finally:
        client.destroy()


async def client_async_delayed_event(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    client = make_client(plan)
    target_workflow_id = f"{plan.workflow_id}-client-async"
    key = f"{plan.event_key}-async"
    expected_label = f"async-value-{plan.seed}"
    delay_seconds = 0.4
    payload = client_event_payload(target_workflow_id, key)
    ticker_stop = asyncio.Event()
    ticker_count = 0

    async def ticker() -> None:
        nonlocal ticker_count
        while not ticker_stop.is_set():
            ticker_count += 1
            await asyncio.sleep(0.05)

    try:
        polling = assert_client_polling_contract(client, plan, "client_async_event")
        started_at = time.time()
        wait_task = asyncio.create_task(client.get_event_async(target_workflow_id, key, plan.timeout_seconds))
        ticker_task = asyncio.create_task(ticker())
        registration = await wait_for_async_condition(
            "client_async_waiter_registered_before_set",
            lambda: client._sys_db.workflow_events_map.get(payload) is not None,
            timeout_seconds=5.0,
        )
        handle = await run_blocking(
            start_workflow_with_id,
            target_workflow_id,
            client_async_delayed_event_workflow,
            key,
            expected_label,
            delay_seconds,
        )
        value = await asyncio.wait_for(wait_task, timeout=plan.timeout_seconds + 2.0)
        completed_at = time.time()
        duration = completed_at - started_at
        ticker_stop.set()
        await ticker_task
        handle_result = await run_blocking(handle.get_result)
        events = await run_blocking(DBOS.get_all_events, target_workflow_id)
        status = await run_blocking(status_snapshot, target_workflow_id)
        final_event = events.get(key)
        live_latency = completed_at - value.get("written_at", completed_at) if isinstance(value, dict) else plan.timeout_seconds
        bound = client_prompt_bound_seconds(plan)
        invariant(
            "client_async_get_event_delivered_model_value",
            isinstance(value, dict)
            and isinstance(handle_result, dict)
            and isinstance(final_event, dict)
            and value.get("label") == expected_label
            and handle_result.get("label") == expected_label
            and final_event.get("label") == expected_label,
            result=value,
            expected_label=expected_label,
            handle_result=handle_result,
            events=events,
            status=status,
        )
        invariant(
            "client_async_get_event_within_prompt_bound",
            live_latency < bound,
            live_latency=live_latency,
            total_wait_duration=duration,
            bound_seconds=bound,
            timeout_seconds=plan.timeout_seconds,
            polling=polling,
        )
        invariant(
            "client_async_get_event_loop_kept_ticking",
            ticker_count >= 3,
            ticker_count=ticker_count,
            duration=duration,
        )
        invariant(
            "client_async_event_table_matches_model",
            isinstance(final_event, dict) and final_event.get("label") == expected_label,
            workflow_id=target_workflow_id,
            key=key,
            events=events,
            expected_label=expected_label,
        )
        invariant(
            "client_async_get_event_no_stale_waiter",
            client._sys_db.workflow_events_map.get(payload) is None,
            payload=payload,
            registration=registration,
        )
        return {
            "workflow_id": target_workflow_id,
            "key": key,
            "payload": payload,
            "expected_label": expected_label,
            "result": value,
            "duration": duration,
            "live_latency": live_latency,
            "ticker_count": ticker_count,
            "handle_result": handle_result,
            "events": events,
            "status": status,
            "bound_seconds": bound,
            "polling": polling,
        }
    finally:
        ticker_stop.set()
        client.destroy()


def client_terminal_missing_event(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    client = make_client(plan)
    target_workflow_id = f"{plan.workflow_id}-terminal-miss"
    key = f"{plan.event_key}-missing"
    delay_seconds = 0.3
    payload = client_event_payload(target_workflow_id, key)
    try:
        polling = assert_client_polling_contract(client, plan, "client_terminal_miss")
        holder, thread, started_at = start_client_get_thread(client, target_workflow_id, key, plan.timeout_seconds)
        registration = wait_for_condition(
            "client_terminal_miss_waiter_registered_before_terminal",
            lambda: client._sys_db.workflow_events_map.get(payload) is not None,
            timeout_seconds=5.0,
        )
        handle = start_workflow_with_id(target_workflow_id, client_terminal_no_event_workflow, key, delay_seconds)
        handle_result = handle.get_result()
        status_after_terminal = status_snapshot(target_workflow_id)
        terminal_observed_at = time.time()
        wait_result = finish_client_get_thread(
            "client_terminal_miss_get_event_prompt",
            holder,
            thread,
            started_at,
            timeout_seconds=plan.timeout_seconds + 2.0,
        )
        status_after_wait = status_snapshot(target_workflow_id)
        events = DBOS.get_all_events(target_workflow_id)
        terminal_latency = wait_result["completed_at"] - terminal_observed_at
        bound = client_prompt_bound_seconds(plan)
        invariant(
            "client_terminal_miss_returns_none",
            wait_result["value"] is None,
            result=wait_result,
            handle_result=handle_result,
            status_after_terminal=status_after_terminal,
            events=events,
        )
        invariant(
            "client_terminal_miss_within_prompt_bound",
            terminal_latency < bound,
            terminal_latency=terminal_latency,
            total_wait_duration=wait_result["duration"],
            bound_seconds=bound,
            timeout_seconds=plan.timeout_seconds,
            polling=polling,
        )
        invariant(
            "client_terminal_miss_status_unchanged",
            status_after_terminal is not None
            and status_after_wait is not None
            and status_after_terminal["status"] == "SUCCESS"
            and status_after_wait["status"] == "SUCCESS",
            status_after_terminal=status_after_terminal,
            status_after_wait=status_after_wait,
        )
        invariant(
            "client_terminal_miss_event_table_empty",
            key not in events,
            workflow_id=target_workflow_id,
            key=key,
            events=events,
        )
        invariant(
            "client_terminal_miss_no_stale_waiter",
            client._sys_db.workflow_events_map.get(payload) is None,
            payload=payload,
            registration=registration,
        )
        return {
            "workflow_id": target_workflow_id,
            "key": key,
            "payload": payload,
            "wait_result": wait_result,
            "terminal_latency": terminal_latency,
            "handle_result": handle_result,
            "status_after_terminal": status_after_terminal,
            "status_after_wait": status_after_wait,
            "events": events,
            "bound_seconds": bound,
            "polling": polling,
        }
    finally:
        client.destroy()


def client_event_update_race(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    client = make_client(plan)
    target_workflow_id = f"{plan.workflow_id}-update"
    key = f"{plan.event_key}-update"
    initial = f"initial-{plan.seed}"
    updated = f"updated-{plan.seed}"
    delay_seconds = 2.0
    payload = client_event_payload(target_workflow_id, key)
    try:
        polling = assert_client_polling_contract(client, plan, "client_update_event")
        handle = start_workflow_with_id(target_workflow_id, client_event_update_workflow, key, initial, updated, delay_seconds)
        initial_visible = wait_for_condition(
            "client_update_initial_event_visible_before_update",
            lambda: client.get_event(target_workflow_id, key, 0) == initial,
            timeout_seconds=5.0,
        )
        first_started = time.time()
        first = client.get_event(target_workflow_id, key, plan.timeout_seconds)
        first_duration = time.time() - first_started
        handle_result = handle.get_result()
        second_started = time.time()
        second = client.get_event(target_workflow_id, key, plan.timeout_seconds)
        second_duration = time.time() - second_started
        events = DBOS.get_all_events(target_workflow_id)
        status = status_snapshot(target_workflow_id)
        immediate_bound = client_prompt_bound_seconds(plan, delay_seconds=0.0)
        invariant(
            "client_update_first_read_observes_initial",
            first == initial and first_duration < immediate_bound,
            first=first,
            initial=initial,
            first_duration=first_duration,
            bound_seconds=immediate_bound,
            initial_visible=initial_visible,
        )
        invariant(
            "client_update_later_read_observes_updated",
            second == updated and handle_result == updated and second_duration < immediate_bound,
            second=second,
            updated=updated,
            second_duration=second_duration,
            handle_result=handle_result,
            bound_seconds=immediate_bound,
        )
        invariant(
            "client_update_event_table_matches_updated",
            events.get(key) == updated,
            workflow_id=target_workflow_id,
            key=key,
            events=events,
            expected=updated,
            status=status,
        )
        invariant(
            "client_update_no_stale_waiter",
            client._sys_db.workflow_events_map.get(payload) is None,
            payload=payload,
        )
        return {
            "workflow_id": target_workflow_id,
            "key": key,
            "payload": payload,
            "initial": initial,
            "updated": updated,
            "first": first,
            "second": second,
            "first_duration": first_duration,
            "second_duration": second_duration,
            "handle_result": handle_result,
            "events": events,
            "status": status,
            "bound_seconds": immediate_bound,
            "polling": polling,
        }
    finally:
        client.destroy()


def write_artifact(artifact_dir: Path, plan: CasePlan, result: dict[str, Any]) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    data = {"plan": asdict(plan), "result": result}
    (artifact_dir / f"{plan.case_id}.json").write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_plan_artifact(artifact_dir: Path, plan: CasePlan) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    data = {
        "frontier_id": FRONTIER_ID,
        "rung_id": plan.rung_id,
        "case_id": plan.case_id,
        "seed": plan.seed,
        "schedule": plan.schedule,
        "derived_plan": asdict(plan),
    }
    (artifact_dir / f"{plan.case_id}-plan.json").write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def needs_named_executor(plan: CasePlan) -> bool:
    return plan.schedule in {
        "relaunch-after-cancelled-recv-reuse",
        "relaunch-after-cancelled-event-reuse",
        "timeout-replay-after-relaunch",
        "fallback-waiter-relaunch",
        "fork-tree-relaunch-before-fanout",
        "stream-relaunch-before-close",
        "repeated-cancel-reuse-across-relaunch",
        "relaunch-then-bulk-reject-valid-send",
        "replay-cancel-recv",
        "replay-timeout-late-send",
        "replay-stream-before-close",
    }


def case_ids_for_rung(rung: str) -> list[str]:
    if rung == RUNG_001_ID:
        return ["case-001", "case-002", "case-003"]
    if rung == RUNG_002_ID:
        return ["case-001", "case-002", "case-003", "case-004", "case-005"]
    if rung == RUNG_003_ID:
        return ["case-001", "case-002", "case-003", "case-004", "case-005", "case-006", "case-007", "case-008"]
    if rung == RUNG_004_ID:
        return [f"case-{index:03d}" for index in range(1, 25)]
    if rung == RUNG_005_ID:
        return ["case-001", "case-002", "case-003"]
    if rung == RUNG_006_ID:
        return ["case-001", "case-002", "case-003", "case-004"]
    raise SetupBlock(f"unsupported rung {rung}")


def run_case(plan: CasePlan, artifact_dir: Path) -> int:
    write_plan_artifact(artifact_dir, plan)
    dbos = launch(plan, executor_id=executor_a(plan) if needs_named_executor(plan) else None)
    try:
        if plan.schedule == "cancel-recv-registered-then-duplicate-send":
            with SetWorkflowID(plan.workflow_id):
                noop_workflow()
            result = asyncio.run(cancel_recv_setup_window(dbos, plan))
        elif plan.schedule == "cancel-event-registered-then-set-event":
            result = asyncio.run(cancel_event_setup_window(dbos, plan))
        elif plan.schedule == "timeout-before-late-send-and-bulk-reject":
            result = timeout_and_bulk_reject(plan)
        elif plan.schedule == "fallback-recv-listener-stopped-before-send":
            result = fallback_recv_listener_stopped(dbos, plan)
        elif plan.schedule == "fallback-get-event-listener-stopped-before-set":
            result = fallback_get_event_listener_stopped(dbos, plan)
        elif plan.schedule == "fork-fanout-duplicate-key":
            result = fork_fanout_duplicate_key(dbos, plan)
        elif plan.schedule == "fork-event-step-boundaries":
            result = fork_event_step_boundaries(plan)
        elif plan.schedule == "interleaved-stream-offsets":
            result = interleaved_stream_offsets(plan)
        elif plan.schedule == "duplicate-send-before-timeout":
            result = duplicate_send_before_timeout(dbos, plan)
        elif plan.schedule == "duplicate-send-after-timeout":
            result = duplicate_send_after_timeout(dbos, plan)
        elif plan.schedule == "bulk-duplicate-key-reject":
            result = bulk_duplicate_reject_valid_send(dbos, plan)
        elif plan.schedule == "fork-fanout-two-descendants":
            result = fork_fanout_chain(dbos, plan, descendant_count=2)
        elif plan.schedule == "fork-fanout-four-descendants":
            result = fork_fanout_chain(dbos, plan, descendant_count=4)
        elif plan.schedule == "fork-event-early-step":
            result = fork_event_single_step(plan, start_step=2)
        elif plan.schedule == "fork-event-late-step":
            result = fork_event_single_step(plan, start_step=3)
        elif plan.schedule == "stream-two-keys-two-writers":
            result = stream_matrix(plan, key_count=2, hot_length=4)
        elif plan.schedule == "stream-three-keys-three-writers":
            result = stream_matrix(plan, key_count=3, hot_length=4)
        elif plan.schedule == "stream-hot-key-five-writes":
            result = stream_matrix(plan, key_count=1, hot_length=5)
        elif plan.schedule == "relaunch-after-cancelled-recv-reuse":
            result = asyncio.run(relaunch_after_cancelled_recv_reuse(dbos, plan))
        elif plan.schedule == "relaunch-after-cancelled-event-reuse":
            result = asyncio.run(relaunch_after_cancelled_event_reuse(dbos, plan))
        elif plan.schedule == "timeout-replay-after-relaunch":
            result = timeout_replay_after_relaunch(plan)
        elif plan.schedule == "fallback-waiter-relaunch":
            result = fallback_waiter_relaunch(plan)
        elif plan.schedule == "fork-tree-relaunch-before-fanout":
            result = fork_tree_relaunch_before_fanout(dbos, plan)
        elif plan.schedule == "stream-relaunch-before-close":
            result = stream_relaunch_before_close(plan)
        elif plan.schedule == "repeated-cancel-reuse-across-relaunch":
            result = repeated_cancel_reuse_across_relaunch(dbos, plan)
        elif plan.schedule == "relaunch-then-bulk-reject-valid-send":
            result = relaunch_then_bulk_reject_valid_send(plan)
        elif plan.schedule == "replay-cancel-recv":
            result = asyncio.run(relaunch_after_cancelled_recv_reuse(dbos, plan))
        elif plan.schedule == "replay-timeout-late-send":
            result = timeout_replay_after_relaunch(plan)
        elif plan.schedule == "replay-stream-before-close":
            result = stream_relaunch_before_close(plan)
        elif plan.schedule == "mixed-small-session":
            result = mixed_small_session(dbos, plan)
        elif plan.schedule == "runtime-live-reconnect-offsets":
            result = runtime_live_reconnect_offsets(dbos, plan)
        elif plan.schedule == "client-and-fallback-resume":
            result = client_and_fallback_resume(dbos, plan)
        elif plan.schedule == "blocked-reader-termination-relaunch":
            result = blocked_reader_termination_relaunch(dbos, plan)
        elif plan.schedule == "client-sync-delayed-event":
            result = client_sync_delayed_event(dbos, plan)
        elif plan.schedule == "client-async-delayed-event":
            result = asyncio.run(client_async_delayed_event(dbos, plan))
        elif plan.schedule == "client-terminal-missing-event":
            result = client_terminal_missing_event(dbos, plan)
        elif plan.schedule == "client-event-update-race":
            result = client_event_update_race(dbos, plan)
        else:
            raise SetupBlock(f"unsupported schedule {plan.schedule}")
        write_artifact(artifact_dir, plan, result)
        event("case_passed", case=plan.case_id, result=result)
        return 0
    finally:
        DBOS.destroy(destroy_registry=False)
        if os.environ.get("WIO_CLEANUP_AFTER") == "1":
            cleanup_databases(plan)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DBOS message/event cancellation workload")
    parser.add_argument("--rung", default="rung-001")
    parser.add_argument("--case", choices=[f"case-{index:03d}" for index in range(1, 25)])
    parser.add_argument("--seed", type=int)
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument("--artifact-dir", default="/tmp/wio-artifacts/message-event-cancellation")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rung = normalize_rung(args.rung)
    if args.all_cases:
        cases = case_ids_for_rung(rung)
    elif args.case:
        cases = [args.case]
    else:
        raise SetupBlock("--case or --all-cases is required")
    if args.all_cases and not args.sequential:
        raise SetupBlock("--all-cases currently requires --sequential to keep DBOS global state isolated")
    if args.all_cases and args.seed is not None:
        raise SetupBlock("--seed is only supported with --case")
    try:
        for case_id in cases:
            run_case(make_plan(rung, case_id, args.seed), Path(args.artifact_dir))
        return 0
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 42
    except WorkloadFailure as exc:
        print(f"WORKLOAD-FAIL {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
