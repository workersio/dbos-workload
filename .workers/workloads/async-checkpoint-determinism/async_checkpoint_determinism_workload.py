#!/usr/bin/env python3
"""Fresh WIO workload for DBOS async checkpoint determinism.

Frontier: async-checkpoint-determinism
Rung:
  - rung-001-async-checkpoint-recovery-cancel-compose
  - rung-002-queued-async-task-retention-gc-pressure
  - rung-003-preemptible-step-cancel-resume-isolation
Protected product promise:
  Async DBOS operations reserve deterministic checkpoint positions and preserve
  workflow/child context across concurrent scheduling, recovery, cancellation,
  and replay. Queued async workflows remain strongly reachable under GC
  pressure until terminal state and release task references afterward.
  Preemptible async steps cancel mid-await, bypass retries, avoid poisoned step
  outputs, and resume to one successful durable result.
Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py \
    --rung rung-002-queued-async-task-retention-gc-pressure --case case-001 --seed 7110
Seed policy:
  Exact rung-001 seeds are 7310, 7311, 7312. Exact rung-002 seeds are 7110,
  7111, 7112, 7113. Exact rung-003 seeds are 6600, 6601, 6602, 6603. Every
  case writes the derived plan and observed DBOS state under the artifact
  directory.
Invariant oracle:
  Public handle result/error, terminal workflow status, workflow step ordering,
  child workflow lineage, patch error classification, task weakrefs,
  `_workflow_tasks` snapshots, and modeled application effects must agree
  within bounded timeouts.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import os
import random
import sys
import threading
import time
import weakref
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
VENV_SITE_PACKAGES = (
    REPO_ROOT
    / ".workers"
    / "vendor"
    / "dbos-venv"
    / "lib"
    / "python3.12"
    / "site-packages"
)
if VENV_SITE_PACKAGES.exists():
    sys.path.insert(0, str(VENV_SITE_PACKAGES))

for target in [
    REPO_ROOT / ".workers" / "vendor" / "dbos-transact-py",
    Path("/Users/viswa/code/workers/dbos-transact-py"),
]:
    if target.exists():
        sys.path.insert(0, str(target))
        break

try:
    import sqlalchemy as sa
    from sqlalchemy.engine import make_url

    from dbos import DBOS, DBOSConfig, SetWorkflowID
    from dbos._dbos import _get_dbos_instance
    from dbos._schemas.system_database import SystemSchema
    from dbos._sys_db import WorkflowStatusString
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "async-checkpoint-determinism"
RUNG_001_ID = "rung-001-async-checkpoint-recovery-cancel-compose"
RUNG_002_ID = "rung-002-queued-async-task-retention-gc-pressure"
RUNG_003_ID = "rung-003-preemptible-step-cancel-resume-isolation"
APP_ID = "wio-async-checkpoint"
APP_VERSION = "wio-async-checkpoint"

TERMINAL_STATUSES = {
    WorkflowStatusString.SUCCESS.value,
    WorkflowStatusString.ERROR.value,
    WorkflowStatusString.CANCELLED.value,
    WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value,
}

RUNG_001_CASE_MATRIX = {
    "case-001": (7310, "gather-distinct-steps-recover"),
    "case-002": (7311, "concurrent-patch-plus-steps"),
    "case-003": (7312, "cancel-during-async-child-start"),
}

RUNG_002_CASE_MATRIX = {
    "case-001": (7110, "single-queued-async-gc-pin"),
    "case-002": (7111, "six-queued-async-gc-pressure"),
    "case-003": (7112, "cancel-half-after-gc"),
    "case-004": (7113, "modeled-error-after-gc"),
}

RUNG_003_CASE_MATRIX = {
    "case-001": (6600, "decorated-preemptible-retry-cancel-resume"),
    "case-002": (6601, "run-step-async-preemptible-option-cancel-resume"),
    "case-003": (6602, "preemptible-vs-nonpreemptible-control"),
    "case-004": (6603, "preempted-resume-after-runtime-boundary"),
}

CASE_MATRICES = {
    RUNG_001_ID: RUNG_001_CASE_MATRIX,
    RUNG_002_ID: RUNG_002_CASE_MATRIX,
    RUNG_003_ID: RUNG_003_CASE_MATRIX,
}

_ledger_lock = threading.Lock()
_ledger_rows: list[dict[str, Any]] = []
_thread_events: dict[str, threading.Event] = {}
_gc_state_lock = threading.Lock()
_gc_future_refs: dict[str, weakref.ReferenceType[asyncio.Future[str]]] = {}
_gc_future_loops: dict[str, asyncio.AbstractEventLoop] = {}
_preempt_events: dict[str, asyncio.Event] = {}


class WorkloadFailure(Exception):
    pass


class SetupBlock(Exception):
    pass


class ModeledAsyncRetentionError(Exception):
    pass


@dataclass(frozen=True)
class CasePlan:
    rung_id: str
    case_id: str
    seed: int
    schedule: str
    database_prefix: str
    workflow_id: str
    gather_delays_ms: dict[str, int]
    patch_tags: list[str]
    child_count: int
    timeout_sec: float
    queue_name: str
    queued_workflow_count: int
    queue_concurrency: int
    cancel_indices: list[int]
    error_index: int | None
    gc_cycles: int
    preemptible_path: str
    preemptible_control: bool
    runtime_boundary: bool


def now_ms() -> int:
    return int(time.time() * 1000)


def event(name: str, **fields: Any) -> None:
    parts = [f"WIO-EVENT {name}"]
    parts.extend(f"{key}={json.dumps(value, sort_keys=True, default=str)}" for key, value in fields.items())
    print(" ".join(parts), flush=True)


def invariant(name: str, ok: bool, **fields: Any) -> None:
    status = "PASS" if ok else "FAIL"
    summary = json.dumps(fields, sort_keys=True, default=str) if fields else "ok"
    print(f"INVARIANT {name} {name} {status} {summary}", flush=True)
    if not ok:
        raise WorkloadFailure(f"{name} failed: {summary}")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def append_ledger(**row: Any) -> None:
    row.setdefault("observed_at_ms", now_ms())
    with _ledger_lock:
        _ledger_rows.append(row)


def ledger_rows() -> list[dict[str, Any]]:
    with _ledger_lock:
        return list(_ledger_rows)


def ledger_count(*, workflow_id: str, logical_op: str, event_name: str) -> int:
    return len(
        [
            row
            for row in ledger_rows()
            if row.get("workflow_id") == workflow_id
            and row.get("logical_op") == logical_op
            and row.get("event") == event_name
        ]
    )


def reset_case_state() -> None:
    with _ledger_lock:
        _ledger_rows.clear()
    _thread_events.clear()
    with _gc_state_lock:
        _gc_future_refs.clear()
        _gc_future_loops.clear()
    _preempt_events.clear()


def thread_event(name: str) -> threading.Event:
    if name not in _thread_events:
        _thread_events[name] = threading.Event()
    return _thread_events[name]


def preempt_event(name: str) -> asyncio.Event:
    if name not in _preempt_events:
        _preempt_events[name] = asyncio.Event()
    return _preempt_events[name]


def admin_url() -> sa.URL:
    raw = os.environ.get(
        "DBOS_POSTGRES_ADMIN_URL",
        "postgresql+psycopg://postgres:dbos@localhost:5432/postgres",
    )
    url = make_url(raw)
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+psycopg")
    return url


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def prepare_databases(prefix: str, artifacts: Path) -> tuple[str, str, str]:
    base = admin_url()
    app_db = f"{prefix}_app"
    sys_db = f"{prefix}_sys"
    admin = str(base.set(database=base.database or "postgres"))
    masked = str(base.set(password="***" if base.password else None))
    event("postgres_preflight", admin_url=masked, app_db=app_db, sys_db=sys_db)
    try:
        engine = sa.create_engine(admin, connect_args={"connect_timeout": 5})
        with engine.connect() as raw_connection:
            connection = raw_connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(sa.text("SET statement_timeout = '8000ms'"))
            for database in (app_db, sys_db):
                connection.execute(sa.text(f"DROP DATABASE IF EXISTS {quote_ident(database)} WITH (FORCE)"))
                connection.execute(sa.text(f"CREATE DATABASE {quote_ident(database)}"))
        engine.dispose()
    except Exception as exc:
        write_json(
            artifacts / "setup-block.json",
            {
                "kind": "postgres_unavailable_or_database_create_failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "admin_url": masked,
            },
        )
        raise SetupBlock(f"postgres setup failed: {type(exc).__name__}: {exc}") from exc
    return (
        str(base.set(drivername="postgresql", database=app_db)),
        str(base.set(drivername="postgresql+psycopg", database=sys_db)),
        masked,
    )


def drop_databases(prefix: str) -> None:
    if os.environ.get("WIO_ASYNC_CHECKPOINT_KEEP_DATABASES") == "1":
        return
    base = admin_url()
    admin = str(base.set(database=base.database or "postgres"))
    engine = sa.create_engine(admin, connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as raw_connection:
            connection = raw_connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(sa.text("SET statement_timeout = '5000ms'"))
            connection.execute(sa.text("SET lock_timeout = '3000ms'"))
            for suffix in ("app", "sys"):
                connection.execute(
                    sa.text(f"DROP DATABASE IF EXISTS {quote_ident(prefix + '_' + suffix)} WITH (FORCE)")
                )
    except Exception as exc:
        event("database_cleanup_best_effort_failed", prefix=prefix, error_type=type(exc).__name__, error=str(exc))
    finally:
        engine.dispose()


def make_config(plan: CasePlan, app_url: str, sys_url: str) -> DBOSConfig:
    return {
        "name": APP_ID,
        "application_database_url": app_url,
        "system_database_url": sys_url,
        "application_version": f"{APP_VERSION}-{plan.case_id}",
        "executor_id": f"wio-async-{plan.case_id}",
        "enable_otlp": False,
        "enable_patching": True,
        "max_executor_threads": 16,
    }


def canonical_rung_id(rung_id: str) -> str:
    if rung_id in {RUNG_001_ID, "rung-001"}:
        return RUNG_001_ID
    if rung_id in {RUNG_002_ID, "rung-002"}:
        return RUNG_002_ID
    if rung_id in {RUNG_003_ID, "rung-003"}:
        return RUNG_003_ID
    return rung_id


def make_plan(rung_id: str, case_id: str, seed_override: int | None = None) -> CasePlan:
    rung_id = canonical_rung_id(rung_id)
    if rung_id not in CASE_MATRICES:
        raise SetupBlock(f"unsupported rung: {rung_id}")
    if case_id not in CASE_MATRICES[rung_id]:
        raise SetupBlock(f"unsupported case: {case_id}")
    seed, schedule = CASE_MATRICES[rung_id][case_id]
    if seed_override is not None and seed_override != seed:
        raise SetupBlock(f"{case_id} for {rung_id} requires seed {seed}, got {seed_override}")
    rng = random.Random(seed)
    rung_slug = "r1" if rung_id == RUNG_001_ID else "r2"
    digest = f"{rung_slug}_{seed:x}_{case_id.replace('-', '_')}"
    queued_workflow_count = 0
    queue_concurrency = 1
    cancel_indices: list[int] = []
    error_index: int | None = None
    gc_cycles = 0
    preemptible_path = ""
    preemptible_control = False
    runtime_boundary = False
    if rung_id == RUNG_002_ID:
        if case_id == "case-001":
            queued_workflow_count = 1
            queue_concurrency = 1
        elif case_id == "case-002":
            queued_workflow_count = 6
            queue_concurrency = 2
        elif case_id == "case-003":
            queued_workflow_count = 6
            queue_concurrency = 6
            cancel_indices = [0, 2, 4]
        elif case_id == "case-004":
            queued_workflow_count = 4
            queue_concurrency = 4
            error_index = 1
        gc_cycles = 8 + rng.randint(0, 4)
    if rung_id == RUNG_003_ID:
        if case_id == "case-001":
            preemptible_path = "decorator"
        elif case_id == "case-002":
            preemptible_path = "run_step_async"
        elif case_id == "case-003":
            preemptible_path = "decorator"
            preemptible_control = True
        elif case_id == "case-004":
            preemptible_path = "decorator"
            runtime_boundary = True
    return CasePlan(
        rung_id=rung_id,
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        database_prefix=f"wio_async_{digest}",
        workflow_id=f"{FRONTIER_ID}-{rung_id}-{case_id}-{seed}",
        gather_delays_ms={
            "step_a": 45 + rng.randint(0, 8),
            "step_b": 1 + rng.randint(0, 3),
            "step_c": 18 + rng.randint(0, 6),
        },
        patch_tags=[f"{case_id}-{tag}-{seed}" for tag in ("a", "b", "c")],
        child_count=4,
        timeout_sec=45.0,
        queue_name=f"wio_async_gc_{case_id.replace('-', '_')}_{seed}",
        queued_workflow_count=queued_workflow_count,
        queue_concurrency=queue_concurrency,
        cancel_indices=cancel_indices,
        error_index=error_index,
        gc_cycles=gc_cycles,
        preemptible_path=preemptible_path,
        preemptible_control=preemptible_control,
        runtime_boundary=runtime_boundary,
    )


def status_value(status: Any, key: str) -> Any:
    if status is None:
        return None
    if isinstance(status, dict):
        return status.get(key)
    return getattr(status, key)


def error_signature(exc: BaseException | None) -> dict[str, Any]:
    if exc is None:
        return {"type": None, "message": None, "repr": None}
    return {
        "type": type(exc).__name__,
        "message": str(exc),
        "repr": repr(exc),
        "cause": error_signature(exc.__cause__) if exc.__cause__ is not None else None,
        "context": error_signature(exc.__context__) if exc.__context__ is not None else None,
    }


def error_text(signature: dict[str, Any]) -> str:
    pieces = []
    stack = [signature]
    while stack:
        item = stack.pop()
        for key in ("type", "message", "repr"):
            value = item.get(key)
            if value is not None:
                pieces.append(str(value))
        for key in ("cause", "context"):
            if item.get(key) is not None:
                stack.append(item[key])
    return "\n".join(pieces)


def launch_dbos(config: DBOSConfig) -> None:
    DBOS(config=config)
    DBOS.launch()


async def wait_for_status(workflow_id: str, allowed: set[str], timeout_sec: float) -> Any:
    deadline = time.time() + timeout_sec
    last = None
    while time.time() < deadline:
        status = await DBOS.get_workflow_status_async(workflow_id)
        last = status
        if status is not None and status_value(status, "status") in allowed:
            return status
        await asyncio.sleep(0.1)
    return last


def raw_rows(sys_url: str, workflow_id: str) -> dict[str, Any]:
    engine = sa.create_engine(sys_url, connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as conn:
            statuses = (
                conn.execute(
                    sa.text(
                        """
                        SELECT workflow_uuid, status, name, parent_workflow_id,
                               executor_id, error
                        FROM dbos.workflow_status
                        WHERE workflow_uuid = :workflow_id
                           OR parent_workflow_id = :workflow_id
                           OR workflow_uuid LIKE :prefix
                        ORDER BY created_at, workflow_uuid
                        """
                    ),
                    {"workflow_id": workflow_id, "prefix": workflow_id + "-%"},
                )
                .mappings()
                .all()
            )
            steps = (
                conn.execute(
                    sa.text(
                        """
                        SELECT workflow_uuid, function_id, function_name, output,
                               error, child_workflow_id
                        FROM dbos.operation_outputs
                        WHERE workflow_uuid = :workflow_id
                           OR workflow_uuid LIKE :prefix
                        ORDER BY workflow_uuid, function_id
                        """
                    ),
                    {"workflow_id": workflow_id, "prefix": workflow_id + "-%"},
                )
                .mappings()
                .all()
            )
    finally:
        engine.dispose()
    return {
        "workflow_status": [dict(row) for row in statuses],
        "operation_outputs": [dict(row) for row in steps],
    }


async def wait_for_child_rows(
    sys_url: str,
    workflow_id: str,
    child_count: int,
    timeout_sec: float,
) -> dict[str, Any]:
    deadline = time.time() + timeout_sec
    last_rows = raw_rows(sys_url, workflow_id)
    while time.time() < deadline:
        child_rows = [
            row
            for row in last_rows["workflow_status"]
            if row["workflow_uuid"] != workflow_id and row["name"].endswith("cancellable_child_workflow")
        ]
        if len(child_rows) == child_count and {row["status"] for row in child_rows} <= TERMINAL_STATUSES:
            return last_rows
        await asyncio.sleep(0.1)
        last_rows = raw_rows(sys_url, workflow_id)
    return last_rows


def force_recovery_status(dbos: DBOS, workflow_id: str) -> None:
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": WorkflowStatusString.PENDING.value})
            .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
        )


@DBOS.step()
async def deterministic_gather_step(plan_json: str, label: str, delay_ms: int) -> str:
    plan = CasePlan(**json.loads(plan_json))
    append_ledger(
        workflow_id=plan.workflow_id,
        case_id=plan.case_id,
        logical_op=label,
        event="step_body_start",
    )
    await asyncio.sleep(delay_ms / 1000)
    append_ledger(
        workflow_id=plan.workflow_id,
        case_id=plan.case_id,
        logical_op=label,
        event="step_body_finish",
        delay_ms=delay_ms,
    )
    return label


@DBOS.step()
async def patch_side_step(plan_json: str, tag: str, index: int) -> str:
    plan = CasePlan(**json.loads(plan_json))
    await asyncio.sleep(0)
    append_ledger(
        workflow_id=plan.workflow_id,
        case_id=plan.case_id,
        logical_op=f"{tag}:{index}",
        event="patch_side_step",
    )
    return f"{tag}:{index}"


@DBOS.workflow()
async def gather_recovery_workflow(plan_json: str) -> dict[str, Any]:
    plan = CasePlan(**json.loads(plan_json))
    a, b, c = await asyncio.gather(
        deterministic_gather_step(plan_json, "step_a", plan.gather_delays_ms["step_a"]),
        deterministic_gather_step(plan_json, "step_b", plan.gather_delays_ms["step_b"]),
        deterministic_gather_step(plan_json, "step_c", plan.gather_delays_ms["step_c"]),
    )
    return {
        "frontier": FRONTIER_ID,
        "case": plan.case_id,
        "seed": plan.seed,
        "result_order": [a, b, c],
    }


async def patch_then_steps(plan_json: str, tag: str) -> str:
    await DBOS.patch_async(tag)
    for index in range(3):
        await patch_side_step(plan_json, tag, index)
        await asyncio.sleep(0)
    return tag


@DBOS.workflow()
async def concurrent_patch_workflow(plan_json: str) -> list[str]:
    plan = CasePlan(**json.loads(plan_json))
    return list(await asyncio.gather(*(patch_then_steps(plan_json, tag) for tag in plan.patch_tags)))


@DBOS.workflow()
async def cancellable_child_workflow(plan_json: str, index: int) -> str:
    plan = CasePlan(**json.loads(plan_json))
    append_ledger(
        workflow_id=plan.workflow_id,
        case_id=plan.case_id,
        logical_op=f"child-{index}",
        event="child_started",
    )
    thread_event(f"{plan.workflow_id}:child-{index}-started").set()
    await DBOS.sleep_async(30.0)
    append_ledger(
        workflow_id=plan.workflow_id,
        case_id=plan.case_id,
        logical_op=f"child-{index}",
        event="child_finished_unexpectedly",
    )
    return f"child-{index}"


async def start_child_for_parent(plan_json: str, index: int) -> str:
    plan = CasePlan(**json.loads(plan_json))
    thread_event(f"{plan.workflow_id}:child-launch-started").set()
    handle = await DBOS.start_workflow_async(cancellable_child_workflow, plan_json, index)
    append_ledger(
        workflow_id=plan.workflow_id,
        case_id=plan.case_id,
        logical_op=f"child-{index}",
        event="child_handle_created",
        child_workflow_id=handle.get_workflow_id(),
    )
    return handle.get_workflow_id()


@DBOS.workflow()
async def cancel_child_parent_workflow(plan_json: str) -> list[str]:
    plan = CasePlan(**json.loads(plan_json))
    tasks = [asyncio.create_task(start_child_for_parent(plan_json, i)) for i in range(plan.child_count)]
    child_ids = list(await asyncio.gather(*tasks))
    append_ledger(
        workflow_id=plan.workflow_id,
        case_id=plan.case_id,
        logical_op="parent",
        event="parent_child_handles_created",
        child_workflow_ids=child_ids,
    )
    thread_event(f"{plan.workflow_id}:children-created").set()
    await DBOS.sleep_async(30.0)
    return child_ids


@DBOS.workflow()
async def queued_gc_retention_workflow(plan_json: str, index: int) -> str:
    plan = CasePlan(**json.loads(plan_json))
    workflow_id = DBOS.workflow_id
    loop = asyncio.get_running_loop()
    future: asyncio.Future[str] = loop.create_future()
    with _gc_state_lock:
        _gc_future_refs[workflow_id] = weakref.ref(future)
        _gc_future_loops[workflow_id] = loop
    append_ledger(
        workflow_id=workflow_id,
        case_id=plan.case_id,
        logical_op=f"queued-{index}",
        event="queued_gc_suspended",
        loop_id=id(loop),
    )
    thread_event(f"{plan.workflow_id}:queued-{index}-suspended").set()
    try:
        action = await future
        if action == "modeled-error":
            raise ModeledAsyncRetentionError(f"modeled async retention error {index}")
        append_ledger(
            workflow_id=workflow_id,
            case_id=plan.case_id,
            logical_op=f"queued-{index}",
            event="queued_gc_returning",
            action=action,
        )
        return f"done:{index}:{plan.seed}"
    except BaseException as exc:
        append_ledger(
            workflow_id=workflow_id,
            case_id=plan.case_id,
            logical_op=f"queued-{index}",
            event="queued_gc_interrupted",
            error_type=type(exc).__name__,
            error=str(exc),
        )
        raise


def preempt_retry_validator(exc: BaseException) -> bool:
    workflow_id = DBOS.workflow_id or "unknown"
    append_ledger(
        workflow_id=workflow_id,
        case_id="preemptible",
        logical_op="retry-validator",
        event="preempt_retry_validator_called",
        error_type=type(exc).__name__,
        error=str(exc),
    )
    return True


async def preemptible_step_body(plan_json: str, branch: str, result_label: str) -> str:
    plan = CasePlan(**json.loads(plan_json))
    workflow_id = DBOS.workflow_id or plan.workflow_id
    key = f"{workflow_id}:{branch}"
    invocation = ledger_count(
        workflow_id=workflow_id,
        logical_op=branch,
        event_name="preempt_step_invocation_start",
    ) + 1
    release = preempt_event(f"{key}:release")
    append_ledger(
        workflow_id=workflow_id,
        case_id=plan.case_id,
        logical_op=branch,
        event="preempt_step_invocation_start",
        invocation=invocation,
        gate_released=release.is_set(),
    )
    if invocation == 1:
        preempt_event(f"{key}:started").set()
        try:
            await release.wait()
            append_ledger(
                workflow_id=workflow_id,
                case_id=plan.case_id,
                logical_op=branch,
                event="preempt_step_first_invocation_released",
                invocation=invocation,
            )
            return f"{result_label}:first-release:{plan.seed}"
        except asyncio.CancelledError:
            append_ledger(
                workflow_id=workflow_id,
                case_id=plan.case_id,
                logical_op=branch,
                event="preempt_step_cancelled",
                invocation=invocation,
                gate_released=release.is_set(),
            )
            raise
    append_ledger(
        workflow_id=workflow_id,
        case_id=plan.case_id,
        logical_op=branch,
        event="preempt_step_resume_success",
        invocation=invocation,
    )
    return f"{result_label}:resumed:{plan.seed}"


@DBOS.step(
    preemptible=True,
    retries_allowed=True,
    max_attempts=5,
    interval_seconds=0.01,
    should_retry=preempt_retry_validator,
)
async def decorated_preemptible_step(plan_json: str, branch: str) -> str:
    return await preemptible_step_body(plan_json, branch, "decorated")


async def run_step_preemptible_body(plan_json: str, branch: str) -> str:
    return await preemptible_step_body(plan_json, branch, "run-step")


@DBOS.step()
async def non_preemptible_control_step(plan_json: str, branch: str) -> str:
    return await preemptible_step_body(plan_json, branch, "control")


@DBOS.workflow()
async def decorated_preemptible_workflow(plan_json: str, branch: str) -> str:
    result = await decorated_preemptible_step(plan_json, branch)
    append_ledger(
        workflow_id=DBOS.workflow_id,
        case_id=CasePlan(**json.loads(plan_json)).case_id,
        logical_op=branch,
        event="preempt_workflow_returning",
        result=result,
    )
    return result


@DBOS.workflow()
async def run_step_preemptible_workflow(plan_json: str, branch: str) -> str:
    result = await DBOS.run_step_async(
        {
            "name": "run_step_preemptible_body",
            "preemptible": True,
            "retries_allowed": True,
            "max_attempts": 5,
            "interval_seconds": 0.01,
            "should_retry": preempt_retry_validator,
        },
        run_step_preemptible_body,
        plan_json,
        branch,
    )
    append_ledger(
        workflow_id=DBOS.workflow_id,
        case_id=CasePlan(**json.loads(plan_json)).case_id,
        logical_op=branch,
        event="preempt_workflow_returning",
        result=result,
    )
    return result


@DBOS.workflow()
async def non_preemptible_control_workflow(plan_json: str, branch: str) -> str:
    result = await non_preemptible_control_step(plan_json, branch)
    append_ledger(
        workflow_id=DBOS.workflow_id,
        case_id=CasePlan(**json.loads(plan_json)).case_id,
        logical_op=branch,
        event="preempt_workflow_returning",
        result=result,
    )
    return result


async def run_gather_recovery_case(plan: CasePlan, sys_url: str) -> dict[str, Any]:
    plan_json = json.dumps(asdict(plan), sort_keys=True)
    with SetWorkflowID(plan.workflow_id):
        first_result = await asyncio.wait_for(gather_recovery_workflow(plan_json), timeout=plan.timeout_sec)
    invariant(
        "gather_initial_result_matches_model",
        first_result["result_order"] == ["step_a", "step_b", "step_c"],
        result=first_result,
    )
    first_steps = await DBOS.list_workflow_steps_async(plan.workflow_id)
    first_names = [step["function_name"].rsplit(".", 1)[-1] for step in first_steps]
    invariant(
        "gather_step_order_uses_declaration_order",
        first_names == ["deterministic_gather_step"] * 3,
        step_names=first_names,
        steps=first_steps,
    )
    first_outputs = [step["output"] for step in first_steps]
    invariant(
        "gather_step_outputs_preserve_logical_order",
        first_outputs == ["step_a", "step_b", "step_c"],
        outputs=first_outputs,
    )
    start_rows = [row for row in ledger_rows() if row["event"] == "step_body_start"]
    finish_rows = [row for row in ledger_rows() if row["event"] == "step_body_finish"]
    invariant(
        "gather_effect_ledger_once_per_step_before_recovery",
        sorted(row["logical_op"] for row in start_rows) == ["step_a", "step_b", "step_c"]
        and sorted(row["logical_op"] for row in finish_rows) == ["step_a", "step_b", "step_c"],
        ledger=ledger_rows(),
    )

    dbos = _get_dbos_instance()
    force_recovery_status(dbos, plan.workflow_id)

    recovered_handles = await asyncio.wait_for(
        asyncio.to_thread(DBOS._recover_pending_workflows, [f"wio-async-{plan.case_id}"]),
        timeout=plan.timeout_sec,
    )
    invariant(
        "gather_recovery_returned_one_handle",
        len(recovered_handles) == 1,
        recovered_handle_count=len(recovered_handles),
        executor_id=f"wio-async-{plan.case_id}",
    )
    recovered_result = await asyncio.wait_for(
        asyncio.to_thread(recovered_handles[0].get_result),
        timeout=plan.timeout_sec,
    )
    invariant(
        "gather_recovery_result_matches_model",
        recovered_result == first_result,
        first_result=first_result,
        recovered_result=recovered_result,
    )
    recovered_status = await DBOS.get_workflow_status_async(plan.workflow_id)
    invariant(
        "gather_recovery_terminal_success",
        status_value(recovered_status, "status") == WorkflowStatusString.SUCCESS.value,
        status=getattr(recovered_status, "__dict__", str(recovered_status)),
    )
    recovered_steps = await DBOS.list_workflow_steps_async(plan.workflow_id)
    invariant(
        "gather_recovery_did_not_duplicate_steps",
        len(recovered_steps) == len(first_steps)
        and [step["function_id"] for step in recovered_steps] == [1, 2, 3],
        first_steps=first_steps,
        recovered_steps=recovered_steps,
    )
    finish_after = [row for row in ledger_rows() if row["event"] == "step_body_finish"]
    invariant(
        "gather_recovery_did_not_duplicate_effects",
        len(finish_after) == 3,
        ledger=ledger_rows(),
    )
    return {
        "first_result": first_result,
        "recovered_result": recovered_result,
        "steps": recovered_steps,
        "raw_rows": raw_rows(sys_url, plan.workflow_id),
    }


async def run_patch_case(plan: CasePlan, sys_url: str) -> dict[str, Any]:
    plan_json = json.dumps(asdict(plan), sort_keys=True)
    signature: dict[str, Any] | None = None
    with SetWorkflowID(plan.workflow_id):
        try:
            await asyncio.wait_for(concurrent_patch_workflow(plan_json), timeout=plan.timeout_sec)
        except Exception as exc:
            signature = error_signature(exc)
    invariant("patch_workflow_raised_bounded_error", signature is not None, workflow_id=plan.workflow_id)
    text = error_text(signature or {})
    invariant(
        "patch_error_is_nondeterminism_class",
        "DBOSPatchNondeterminismError" in text or "called concurrently with other operations" in text,
        signature=signature,
    )
    status = await wait_for_status(plan.workflow_id, TERMINAL_STATUSES, plan.timeout_sec)
    invariant(
        "patch_terminal_status_is_error",
        status_value(status, "status") == WorkflowStatusString.ERROR.value,
        status=getattr(status, "__dict__", str(status)),
    )
    steps = await DBOS.list_workflow_steps_async(plan.workflow_id)
    function_ids = [step["function_id"] for step in steps]
    invariant(
        "patch_steps_have_unique_function_ids",
        len(function_ids) == len(set(function_ids)),
        steps=steps,
    )
    invariant(
        "patch_failure_did_not_record_successful_full_ledger",
        len([row for row in ledger_rows() if row["event"] == "patch_side_step"]) < len(plan.patch_tags) * 3,
        ledger=ledger_rows(),
    )
    return {
        "exception": signature,
        "status": getattr(status, "__dict__", str(status)),
        "steps": steps,
        "raw_rows": raw_rows(sys_url, plan.workflow_id),
    }


async def run_child_cancel_case(plan: CasePlan, sys_url: str) -> dict[str, Any]:
    plan_json = json.dumps(asdict(plan), sort_keys=True)
    with SetWorkflowID(plan.workflow_id):
        handle = await DBOS.start_workflow_async(cancel_child_parent_workflow, plan_json)

    launch_started = await asyncio.to_thread(
        thread_event(f"{plan.workflow_id}:child-launch-started").wait,
        plan.timeout_sec,
    )
    invariant("child_launch_started_before_cancel", launch_started, workflow_id=plan.workflow_id)

    children_created = await asyncio.to_thread(
        thread_event(f"{plan.workflow_id}:children-created").wait,
        plan.timeout_sec,
    )
    invariant("child_handles_created_before_cancel", children_created, workflow_id=plan.workflow_id)

    await DBOS.cancel_workflow_async(plan.workflow_id, cancel_children=True)
    signature: dict[str, Any] | None = None
    try:
        await asyncio.wait_for(handle.get_result(), timeout=plan.timeout_sec)
    except Exception as exc:
        signature = error_signature(exc)
    invariant(
        "parent_handle_observed_cancellation",
        signature is not None and "cancelled" in error_text(signature).lower(),
        signature=signature,
    )
    parent_status = await wait_for_status(plan.workflow_id, TERMINAL_STATUSES, plan.timeout_sec)
    invariant(
        "parent_reaches_terminal_after_cancel",
        status_value(parent_status, "status") in {
            WorkflowStatusString.CANCELLED.value,
            WorkflowStatusString.ERROR.value,
        },
        status=getattr(parent_status, "__dict__", str(parent_status)),
    )

    rows = await wait_for_child_rows(sys_url, plan.workflow_id, plan.child_count, plan.timeout_sec)
    child_rows = [
        row
        for row in rows["workflow_status"]
        if row["workflow_uuid"] != plan.workflow_id and row["name"].endswith("cancellable_child_workflow")
    ]
    invariant(
        "cancel_created_modeled_children",
        len(child_rows) == plan.child_count,
        expected_child_count=plan.child_count,
        child_rows=child_rows,
        all_rows=rows["workflow_status"],
    )
    bad_parent_rows = [
        row
        for row in child_rows
        if not row.get("workflow_uuid") or row.get("parent_workflow_id") != plan.workflow_id
    ]
    invariant(
        "cancel_children_preserve_parent_context",
        not bad_parent_rows,
        child_rows=child_rows,
        bad_parent_rows=bad_parent_rows,
    )
    child_statuses = {row["status"] for row in child_rows}
    invariant(
        "cancel_child_statuses_are_bounded",
        child_statuses <= TERMINAL_STATUSES,
        child_statuses=sorted(child_statuses),
        child_rows=child_rows,
    )
    operation_child_ids = [
        row.get("child_workflow_id")
        for row in rows["operation_outputs"]
        if row.get("child_workflow_id")
    ]
    invariant(
        "cancel_operation_rows_reference_children",
        sorted(operation_child_ids) == sorted(row["workflow_uuid"] for row in child_rows),
        operation_child_ids=operation_child_ids,
        child_rows=child_rows,
        operation_outputs=rows["operation_outputs"],
    )
    return {
        "parent_exception": signature,
        "parent_status": getattr(parent_status, "__dict__", str(parent_status)),
        "raw_rows": rows,
        "ledger": ledger_rows(),
    }


def workflow_task_snapshot() -> dict[str, Any]:
    dbos = _get_dbos_instance()
    tasks = list(dbos._workflow_tasks)
    return {
        "count": len(tasks),
        "tasks": [
            {
                "id": id(task),
                "done": task.done(),
                "cancelled": task.cancelled(),
                "coro": repr(task.get_coro())[:180],
            }
            for task in tasks
        ],
    }


def future_liveness(workflow_ids: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with _gc_state_lock:
        refs = {workflow_id: _gc_future_refs.get(workflow_id) for workflow_id in workflow_ids}
    for workflow_id, ref in refs.items():
        future = ref() if ref is not None else None
        rows.append({"workflow_id": workflow_id, "has_ref": ref is not None, "alive": future is not None})
        del future
    return rows


async def wait_for_suspended(plan: CasePlan, indices: list[int]) -> None:
    waits = [
        asyncio.to_thread(
            thread_event(f"{plan.workflow_id}:queued-{index}-suspended").wait,
            plan.timeout_sec,
        )
        for index in indices
    ]
    results = await asyncio.gather(*waits)
    invariant(
        "queued_gc_workflows_reached_suspended_state",
        all(results),
        indices=indices,
        results=results,
        ledger=ledger_rows(),
        task_snapshot=workflow_task_snapshot(),
    )
    await asyncio.sleep(0.2)


async def force_gc_pressure(plan: CasePlan, active_workflow_ids: list[str], label: str) -> dict[str, Any]:
    before = workflow_task_snapshot()
    weak_before = future_liveness(active_workflow_ids)
    invariant(
        "queued_gc_task_pin_present_before_gc",
        before["count"] >= len(active_workflow_ids),
        active_workflow_ids=active_workflow_ids,
        snapshot=before,
    )
    invariant(
        "queued_gc_pending_futures_alive_before_gc",
        all(row["alive"] for row in weak_before),
        active_workflow_ids=active_workflow_ids,
        weakrefs=weak_before,
    )
    collections: list[int] = []
    for _ in range(plan.gc_cycles):
        allocation_pressure = [bytearray(1024) for _ in range(256)]
        collections.append(gc.collect())
        del allocation_pressure
        await asyncio.sleep(0.05)
    after = workflow_task_snapshot()
    weak_after = future_liveness(active_workflow_ids)
    interrupted = [
        row
        for row in ledger_rows()
        if row.get("event") == "queued_gc_interrupted"
        and (
            row.get("error_type") == "GeneratorExit"
            or "GeneratorExit" in row.get("error", "")
            or "coroutine ignored GeneratorExit" in row.get("error", "")
        )
    ]
    invariant(
        "queued_gc_task_pin_survives_gc",
        after["count"] >= len(active_workflow_ids),
        active_workflow_ids=active_workflow_ids,
        before=before,
        after=after,
        collections=collections,
        label=label,
    )
    invariant(
        "queued_gc_pending_futures_alive_after_gc",
        all(row["alive"] for row in weak_after),
        active_workflow_ids=active_workflow_ids,
        weakrefs=weak_after,
        label=label,
    )
    invariant(
        "queued_gc_no_generator_exit_signature",
        not interrupted,
        interrupted=interrupted,
        ledger=ledger_rows(),
        label=label,
    )
    return {
        "label": label,
        "before": before,
        "after": after,
        "weak_before": weak_before,
        "weak_after": weak_after,
        "collections": collections,
    }


def complete_suspended_future(workflow_id: str, action: str) -> bool:
    with _gc_state_lock:
        ref = _gc_future_refs.get(workflow_id)
        loop = _gc_future_loops.get(workflow_id)
    future = ref() if ref is not None else None
    if future is None or loop is None:
        return False

    def complete() -> None:
        if not future.done():
            future.set_result(action)

    loop.call_soon_threadsafe(complete)
    return True


async def wait_for_task_release(timeout_sec: float) -> dict[str, Any]:
    deadline = time.time() + timeout_sec
    last = workflow_task_snapshot()
    while time.time() < deadline:
        gc.collect()
        last = workflow_task_snapshot()
        if last["count"] == 0:
            return last
        await asyncio.sleep(0.1)
    return last


async def terminal_status(workflow_id: str, timeout_sec: float) -> Any:
    return await wait_for_status(workflow_id, TERMINAL_STATUSES, timeout_sec)


async def run_gc_retention_case(plan: CasePlan, sys_url: str) -> dict[str, Any]:
    plan_json = json.dumps(asdict(plan), sort_keys=True)
    await DBOS.register_queue_async(
        plan.queue_name,
        concurrency=plan.queue_concurrency,
        worker_concurrency=plan.queue_concurrency,
        polling_interval_sec=0.1,
        on_conflict="always_update",
    )
    handles: dict[int, Any] = {}
    workflow_ids = [f"{plan.workflow_id}-{index}" for index in range(plan.queued_workflow_count)]
    for index, workflow_id in enumerate(workflow_ids):
        with SetWorkflowID(workflow_id):
            handles[index] = await DBOS.enqueue_workflow_async(
                plan.queue_name,
                queued_gc_retention_workflow,
                plan_json,
                index,
            )

    observations: dict[str, Any] = {
        "workflow_ids": workflow_ids,
        "queue_name": plan.queue_name,
        "queue_concurrency": plan.queue_concurrency,
        "gc_phases": [],
        "terminal": {},
    }
    processed: set[int] = set()
    while len(processed) < plan.queued_workflow_count:
        remaining = [index for index in range(plan.queued_workflow_count) if index not in processed]
        active = remaining[: plan.queue_concurrency]
        active_ids = [workflow_ids[index] for index in active]
        await wait_for_suspended(plan, active)
        observations["gc_phases"].append(await force_gc_pressure(plan, active_ids, f"active-{active}"))

        to_cancel = [index for index in active if index in plan.cancel_indices]
        to_error = [index for index in active if plan.error_index == index]
        to_release = [index for index in reversed(active) if index not in to_cancel and index not in to_error]
        for index in to_cancel:
            await DBOS.cancel_workflow_async(workflow_ids[index])
        for index in to_error:
            invariant(
                "queued_gc_error_future_released",
                complete_suspended_future(workflow_ids[index], "modeled-error"),
                workflow_id=workflow_ids[index],
            )
        for index in to_release:
            invariant(
                "queued_gc_success_future_released",
                complete_suspended_future(workflow_ids[index], "success"),
                workflow_id=workflow_ids[index],
            )
        processed.update(active)

    for index, workflow_id in enumerate(workflow_ids):
        signature: dict[str, Any] | None = None
        result: Any = None
        try:
            result = await asyncio.wait_for(handles[index].get_result(), timeout=plan.timeout_sec)
        except Exception as exc:
            signature = error_signature(exc)
        status = await terminal_status(workflow_id, plan.timeout_sec)
        status_name = status_value(status, "status")
        observations["terminal"][workflow_id] = {
            "index": index,
            "result": result,
            "exception": signature,
            "status": getattr(status, "__dict__", str(status)),
        }
        if index in plan.cancel_indices:
            invariant(
                "queued_gc_cancelled_terminal_matches_model",
                signature is not None
                and status_name in {WorkflowStatusString.CANCELLED.value, WorkflowStatusString.ERROR.value}
                and "GeneratorExit" not in error_text(signature),
                workflow_id=workflow_id,
                terminal=observations["terminal"][workflow_id],
            )
        elif plan.error_index == index:
            text = error_text(signature or {})
            invariant(
                "queued_gc_modeled_error_terminal_matches_model",
                signature is not None
                and status_name == WorkflowStatusString.ERROR.value
                and "ModeledAsyncRetentionError" in text
                and "GeneratorExit" not in text,
                workflow_id=workflow_id,
                terminal=observations["terminal"][workflow_id],
            )
        else:
            invariant(
                "queued_gc_success_terminal_matches_model",
                result == f"done:{index}:{plan.seed}"
                and signature is None
                and status_name == WorkflowStatusString.SUCCESS.value,
                workflow_id=workflow_id,
                terminal=observations["terminal"][workflow_id],
            )

    interrupted = [
        row
        for row in ledger_rows()
        if row.get("event") == "queued_gc_interrupted"
        and (
            row.get("error_type") == "GeneratorExit"
            or "GeneratorExit" in row.get("error", "")
            or "coroutine ignored GeneratorExit" in row.get("error", "")
        )
    ]
    invariant("queued_gc_no_generator_exit_terminal", not interrupted, interrupted=interrupted, ledger=ledger_rows())
    released_snapshot = await wait_for_task_release(plan.timeout_sec)
    invariant(
        "queued_gc_workflow_tasks_released_after_terminal",
        released_snapshot["count"] == 0,
        released_snapshot=released_snapshot,
        ledger=ledger_rows(),
    )
    observations["released_snapshot"] = released_snapshot
    observations["raw_rows"] = {
        workflow_id: raw_rows(sys_url, workflow_id) for workflow_id in workflow_ids
    }
    observations["ledger"] = ledger_rows()
    return observations


def preempt_workflow_fn(path: str) -> Any:
    if path == "decorator":
        return decorated_preemptible_workflow
    if path == "run_step_async":
        return run_step_preemptible_workflow
    raise SetupBlock(f"unsupported preemptible path: {path}")


def public_step_rows(steps: list[dict[str, Any]], step_name: str) -> list[dict[str, Any]]:
    return [step for step in steps if step_name in step["function_name"]]


def raw_step_rows(rows: dict[str, Any], step_name: str) -> list[dict[str, Any]]:
    return [row for row in rows["operation_outputs"] if step_name in row["function_name"]]


async def wait_for_preempt_event(name: str, timeout_sec: float) -> bool:
    try:
        await asyncio.wait_for(preempt_event(name).wait(), timeout=timeout_sec)
        return True
    except asyncio.TimeoutError:
        return False


async def wait_for_cancel_ledger(
    workflow_id: str,
    branch: str,
    timeout_sec: float,
) -> list[dict[str, Any]]:
    deadline = time.time() + timeout_sec
    rows: list[dict[str, Any]] = []
    while time.time() < deadline:
        rows = [
            row
            for row in ledger_rows()
            if row.get("workflow_id") == workflow_id
            and row.get("logical_op") == branch
            and row.get("event") == "preempt_step_cancelled"
        ]
        if rows:
            return rows
        await asyncio.sleep(0.05)
    return rows


async def observe_cancelled_handle(handle: Any, timeout_sec: float) -> dict[str, Any]:
    try:
        result = await asyncio.wait_for(handle.get_result(), timeout=timeout_sec)
        return {"result": result, "exception": None}
    except BaseException as exc:
        signature = error_signature(exc)
        return {"result": None, "exception": signature}


async def run_one_preemptible_branch(
    plan: CasePlan,
    sys_url: str,
    *,
    workflow_id: str,
    branch: str,
    path: str,
    runtime_boundary: bool = False,
    config: DBOSConfig | None = None,
) -> dict[str, Any]:
    plan_json = json.dumps(asdict(plan), sort_keys=True)
    step_name = "decorated_preemptible_step" if path == "decorator" else "run_step_preemptible_body"
    workflow_fn = preempt_workflow_fn(path)
    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(workflow_fn, plan_json, branch)

    started = await wait_for_preempt_event(f"{workflow_id}:{branch}:started", plan.timeout_sec)
    invariant(
        "preemptible_step_reached_blocked_state_before_cancel",
        started,
        workflow_id=workflow_id,
        branch=branch,
        path=path,
        ledger=ledger_rows(),
    )

    await DBOS.cancel_workflow_async(workflow_id)
    cancel_rows = await wait_for_cancel_ledger(workflow_id, branch, plan.timeout_sec)
    invariant(
        "preemptible_step_observed_cancellation_before_gate_release",
        len(cancel_rows) == 1 and cancel_rows[0].get("gate_released") is False,
        workflow_id=workflow_id,
        branch=branch,
        cancel_rows=cancel_rows,
        ledger=ledger_rows(),
    )
    cancelled_handle = await observe_cancelled_handle(handle, plan.timeout_sec)
    cancelled_text = error_text(cancelled_handle["exception"] or {})
    cancelled_status = await terminal_status(workflow_id, plan.timeout_sec)
    pre_resume_steps = await DBOS.list_workflow_steps_async(workflow_id)
    pre_resume_raw = raw_rows(sys_url, workflow_id)
    retry_rows = [
        row
        for row in ledger_rows()
        if row.get("event") == "preempt_retry_validator_called"
        and row.get("workflow_id") == workflow_id
    ]
    invariant(
        "preemptible_cancelled_public_handle_and_status_match",
        isinstance(cancelled_handle["exception"], dict)
        and (
            "DBOSAwaitedWorkflowCancelledError" in cancelled_text
            or "cancelled" in cancelled_text.lower()
        )
        and status_value(cancelled_status, "status") == WorkflowStatusString.CANCELLED.value,
        workflow_id=workflow_id,
        handle=cancelled_handle,
        status=getattr(cancelled_status, "__dict__", str(cancelled_status)),
    )
    invariant(
        "preemptible_cancel_bypasses_retry_validator",
        retry_rows == [],
        workflow_id=workflow_id,
        retry_rows=retry_rows,
        ledger=ledger_rows(),
    )
    invariant(
        "preemptible_pre_resume_has_no_step_output_or_error",
        public_step_rows(pre_resume_steps, step_name) == []
        and raw_step_rows(pre_resume_raw, step_name) == [],
        workflow_id=workflow_id,
        step_name=step_name,
        public_steps=pre_resume_steps,
        raw_rows=pre_resume_raw,
    )

    if runtime_boundary:
        if config is None:
            raise SetupBlock("runtime boundary case requires config")
        DBOS.destroy(destroy_registry=False, workflow_completion_timeout_sec=2)
        launch_dbos(config)

    resumed_handle = await DBOS.resume_workflow_async(workflow_id)
    resumed_result = await asyncio.wait_for(resumed_handle.get_result(), timeout=plan.timeout_sec)
    final_status = await terminal_status(workflow_id, plan.timeout_sec)
    final_steps = await DBOS.list_workflow_steps_async(workflow_id)
    final_raw = raw_rows(sys_url, workflow_id)
    step_rows = public_step_rows(final_steps, step_name)
    final_snapshot = await wait_for_task_release(plan.timeout_sec)
    expected_result = "decorated:resumed:" + str(plan.seed) if path == "decorator" else "run-step:resumed:" + str(plan.seed)
    invocation_rows = [
        row
        for row in ledger_rows()
        if row.get("workflow_id") == workflow_id
        and row.get("logical_op") == branch
        and row.get("event") == "preempt_step_invocation_start"
    ]
    invariant(
        "preemptible_resume_records_one_successful_step_row",
        resumed_result == expected_result
        and status_value(final_status, "status") == WorkflowStatusString.SUCCESS.value
        and len(step_rows) == 1
        and step_rows[0]["output"] == expected_result
        and step_rows[0]["error"] is None
        and len(invocation_rows) == 2
        and final_snapshot["count"] == 0,
        workflow_id=workflow_id,
        expected_result=expected_result,
        resumed_result=resumed_result,
        final_status=getattr(final_status, "__dict__", str(final_status)),
        final_steps=final_steps,
        raw_rows=final_raw,
        invocation_rows=invocation_rows,
        task_snapshot=final_snapshot,
    )
    return {
        "workflow_id": workflow_id,
        "branch": branch,
        "path": path,
        "cancelled_handle": cancelled_handle,
        "cancelled_status": getattr(cancelled_status, "__dict__", str(cancelled_status)),
        "pre_resume_steps": pre_resume_steps,
        "pre_resume_raw": pre_resume_raw,
        "resumed_result": resumed_result,
        "final_status": getattr(final_status, "__dict__", str(final_status)),
        "final_steps": final_steps,
        "final_raw": final_raw,
        "task_snapshot": final_snapshot,
    }


async def run_preemptible_control_branch(plan: CasePlan, sys_url: str) -> dict[str, Any]:
    plan_json = json.dumps(asdict(plan), sort_keys=True)
    workflow_id = f"{plan.workflow_id}-control"
    branch = "control"
    with SetWorkflowID(workflow_id):
        handle = await DBOS.start_workflow_async(non_preemptible_control_workflow, plan_json, branch)
    started = await wait_for_preempt_event(f"{workflow_id}:{branch}:started", plan.timeout_sec)
    invariant(
        "nonpreemptible_control_reached_blocked_state_before_cancel",
        started,
        workflow_id=workflow_id,
        ledger=ledger_rows(),
    )
    await DBOS.cancel_workflow_async(workflow_id)
    await asyncio.sleep(2.2)
    premature_cancel_rows = [
        row
        for row in ledger_rows()
        if row.get("workflow_id") == workflow_id
        and row.get("logical_op") == branch
        and row.get("event") == "preempt_step_cancelled"
    ]
    invariant(
        "nonpreemptible_control_not_interrupted_before_gate_release",
        premature_cancel_rows == [],
        workflow_id=workflow_id,
        premature_cancel_rows=premature_cancel_rows,
        ledger=ledger_rows(),
    )
    preempt_event(f"{workflow_id}:{branch}:release").set()
    terminal = await observe_cancelled_handle(handle, plan.timeout_sec)
    status = await terminal_status(workflow_id, plan.timeout_sec)
    final_snapshot = await wait_for_task_release(plan.timeout_sec)
    invariant(
        "nonpreemptible_control_released_without_task_leak",
        status_value(status, "status") in TERMINAL_STATUSES and final_snapshot["count"] == 0,
        workflow_id=workflow_id,
        terminal=terminal,
        status=getattr(status, "__dict__", str(status)),
        task_snapshot=final_snapshot,
        raw_rows=raw_rows(sys_url, workflow_id),
    )
    return {
        "workflow_id": workflow_id,
        "terminal": terminal,
        "status": getattr(status, "__dict__", str(status)),
        "raw_rows": raw_rows(sys_url, workflow_id),
        "task_snapshot": final_snapshot,
    }


async def run_preemptible_case(
    plan: CasePlan,
    sys_url: str,
    config: DBOSConfig,
) -> dict[str, Any]:
    observations: dict[str, Any] = {}
    workflow_id = f"{plan.workflow_id}-{plan.preemptible_path}"
    observations["preemptible"] = await run_one_preemptible_branch(
        plan,
        sys_url,
        workflow_id=workflow_id,
        branch=plan.preemptible_path,
        path=plan.preemptible_path,
        runtime_boundary=plan.runtime_boundary,
        config=config,
    )
    if plan.preemptible_control:
        observations["control"] = await run_preemptible_control_branch(plan, sys_url)
    observations["ledger"] = ledger_rows()
    return observations


async def run_case_async(plan: CasePlan, artifacts_root: Path) -> dict[str, Any]:
    reset_case_state()
    artifacts = artifacts_root / plan.rung_id / plan.case_id
    artifacts.mkdir(parents=True, exist_ok=True)
    app_url, sys_url, admin_masked = prepare_databases(plan.database_prefix, artifacts)
    case_contract = {
        **asdict(plan),
        "frontier": FRONTIER_ID,
        "protected_product_promise": (
            "queued async workflow tasks remain reachable under GC pressure until terminal "
            "state and release task references afterward"
            if plan.rung_id == RUNG_002_ID
            else "preemptible async steps cancel mid-await, bypass retries, avoid poisoned "
            "step outputs, and resume to one successful durable result"
            if plan.rung_id == RUNG_003_ID
            else "async checkpoint positions and child context remain deterministic across "
            "concurrent scheduling, recovery, cancellation, and replay"
        ),
        "replay_command": (
            ".workers/run-with-postgres.sh .workers/python-runtime.sh "
            ".workers/workloads/async-checkpoint-determinism/async_checkpoint_determinism_workload.py "
            f"--rung {plan.rung_id} --case {plan.case_id} --seed {plan.seed}"
        ),
        "admin_url": admin_masked,
        "system_database_url": sys_url.replace(str(make_url(sys_url).password or ""), "***"),
    }
    write_json(artifacts / "case.json", case_contract)
    config = make_config(plan, app_url, sys_url)
    result: dict[str, Any]
    try:
        event("case_start", frontier=FRONTIER_ID, rung=plan.rung_id, case=plan.case_id, seed=plan.seed)
        DBOS.destroy(destroy_registry=False)
        launch_dbos(config)
        if plan.rung_id == RUNG_002_ID:
            result = await run_gc_retention_case(plan, sys_url)
        elif plan.rung_id == RUNG_003_ID:
            result = await run_preemptible_case(plan, sys_url, config)
        elif plan.case_id == "case-001":
            result = await run_gather_recovery_case(plan, sys_url)
        elif plan.case_id == "case-002":
            result = await run_patch_case(plan, sys_url)
        elif plan.case_id == "case-003":
            result = await run_child_cancel_case(plan, sys_url)
        else:
            raise SetupBlock(f"unsupported case: {plan.case_id}")
        result["ledger"] = ledger_rows()
        write_json(artifacts / "observations.json", result)
        event("case_complete", rung=plan.rung_id, case=plan.case_id, status="passed")
        return {"case": asdict(plan), "workflow_id": plan.workflow_id, "status": "passed"}
    finally:
        try:
            DBOS.destroy(destroy_registry=False, workflow_completion_timeout_sec=2)
        except Exception as exc:
            event("dbos_destroy_best_effort_failed", error_type=type(exc).__name__, error=str(exc))
        drop_databases(plan.database_prefix)


async def run_selected_async(args: argparse.Namespace) -> int:
    rung_id = canonical_rung_id(args.rung)
    if rung_id not in CASE_MATRICES:
        raise SetupBlock(f"unsupported rung: {args.rung}")
    case_ids = list(CASE_MATRICES[rung_id]) if args.all_cases else [args.case]
    if not args.all_cases and args.case is None:
        raise SetupBlock("--case is required unless --all-cases is set")
    if args.seed is not None and len(case_ids) != 1:
        raise SetupBlock("--seed may only be used with a single --case")
    artifacts_root = Path(args.artifacts_dir)
    results = []
    for case_id in case_ids:
        plan = make_plan(rung_id, case_id, args.seed)
        results.append(await run_case_async(plan, artifacts_root))
    write_json(artifacts_root / rung_id / "summary.json", results)
    event("rung_complete", rung=rung_id, cases=len(results), status="passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rung", required=True)
    parser.add_argument("--case")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true", help="accepted for WIO command compatibility")
    parser.add_argument(
        "--artifacts-dir",
        default="/tmp/wio-artifacts/async-checkpoint-determinism",
    )
    args = parser.parse_args()
    try:
        return asyncio.run(run_selected_async(args))
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 44
    except WorkloadFailure as exc:
        print(f"WORKLOAD-FAIL {exc}", flush=True)
        return 10


if __name__ == "__main__":
    raise SystemExit(main())
