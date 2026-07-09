#!/usr/bin/env python3
"""Fresh WIO workload for DBOS recovery under Postgres faults.

Frontier: recovery-db-faults
Rung: rung-001-recovery-db-restart-single-window
Protected product promise: Postgres-checkpointed DBOS workflows resume from
completed steps after executor interruption and transient database failure.
Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/recovery-db-faults/recovery_db_faults_workload.py --rung rung-001 --case case-001
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote


REPO_ROOT = Path(__file__).resolve().parents[3]
VENV_SITE_PACKAGES = REPO_ROOT / ".workers" / "vendor" / "dbos-venv" / "lib" / "python3.12" / "site-packages"
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

    import dbos._recovery as recovery_module
    from dbos import DBOS, DBOSConfig, SetWorkflowID
    from dbos._sys_db import GetPendingWorkflowsOutput, WorkflowStatusString
    from dbos._utils import GlobalParams
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "recovery-db-faults"
RUNG_ID = "rung-001-recovery-db-restart-single-window"
EXECUTOR_A = "wio-worker-a"
EXECUTOR_B = "wio-worker-b"
EXECUTOR_C = "wio-recoverer-c"
EXECUTOR_D = "wio-worker-d"
APP_VERSION = "wio-recovery-rung-001"
APP_ID = "wio-recovery-db-faults"
QUEUE_NAME = "wio_recovery_queue"
TERMINAL_STATUSES = {
    WorkflowStatusString.SUCCESS.value,
    WorkflowStatusString.ERROR.value,
    WorkflowStatusString.CANCELLED.value,
    WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value,
}
STEP_TWO_RESTART_REQUESTED = threading.Event()
STEP_TWO_RESTART_DONE = threading.Event()
STEP_TWO_RESTART_ERRORS: list[str] = []


class WorkloadFailure(Exception):
    pass


class SetupBlock(Exception):
    pass


@dataclass(frozen=True)
class CasePlan:
    case_id: str
    seed: int
    schedule: str
    restart_down_ms: int
    workflow_id: str
    app_db: str
    sys_db: str
    max_recovery_attempts: int | None = None
    restart_offset_ms: int = 0


def event(name: str, **fields: Any) -> None:
    parts = [f"WIO-EVENT {name}"]
    parts.extend(f"{key}={json.dumps(value, sort_keys=True)}" for key, value in fields.items())
    print(" ".join(parts), flush=True)


def invariant(name: str, ok: bool, **fields: Any) -> None:
    status = "PASS" if ok else "FAIL"
    summary = json.dumps(fields, sort_keys=True) if fields else "ok"
    print(f"INVARIANT {name} {name} {status} {summary}", flush=True)
    if not ok:
        raise WorkloadFailure(f"{name} failed: {json.dumps(fields, sort_keys=True)}")


def make_plan(case_id: str) -> CasePlan:
    cases = {
        "case-001": (101, "restart-before-scan", 750),
        "case-002": (103, "restart-after-scan-before-execute", 750),
        "case-003": (107, "restart-during-recovered-execute", 1500),
    }
    if case_id not in cases:
        raise SetupBlock(f"unknown case {case_id}")
    seed, schedule, down_ms = cases[case_id]
    suffix = f"{case_id.replace('-', '_')}_{seed}_{uuid.uuid4().hex[:8]}"
    return CasePlan(
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        restart_down_ms=down_ms,
        workflow_id=f"wio-recovery-{case_id}-{seed}-{uuid.uuid4().hex[:12]}",
        app_db=f"wio_recovery_app_{suffix}",
        sys_db=f"wio_recovery_sys_{suffix}",
    )


def make_rung2_plans(case_id: str) -> list[CasePlan]:
    cases = {
        "case-001": (111, "restart-before-scan", 750, 3),
        "case-002": (113, "restart-after-first-id", 750, 3),
        "case-003": (127, "restart-during-direct-execute", 1500, 2),
        "case-004": (131, "restart-during-queue-clear", 750, 2),
        "case-005": (137, "restart-before-result-retrieve", 750, 1),
        "case-006": (139, "restart-longer-transient", 1500, 1),
    }
    if case_id not in cases:
        raise SetupBlock(f"unknown rung-002 case {case_id}")
    seed, schedule, down_ms, workflow_count = cases[case_id]
    suffix = f"rung2_{case_id.replace('-', '_')}_{seed}_{uuid.uuid4().hex[:8]}"
    app_db = f"wio_recovery_app_{suffix}"
    sys_db = f"wio_recovery_sys_{suffix}"
    return [
        CasePlan(
            case_id=f"{case_id}-wf-{index + 1}",
            seed=seed + index,
            schedule=schedule,
            restart_down_ms=down_ms,
            workflow_id=f"wio-recovery-rung2-{case_id}-{seed}-{index + 1}-{uuid.uuid4().hex[:8]}",
            app_db=app_db,
            sys_db=sys_db,
        )
        for index in range(workflow_count)
    ]


def make_rung3_plans(case_id: str) -> list[CasePlan]:
    cases = {
        "case-001": (149, "one-outage-then-healthy", 750, [3]),
        "case-002": (151, "two-outages-then-healthy", 750, [5]),
        "case-003": (157, "exceed-recovery-budget", 750, [1]),
        "case-004": (163, "duplicate-completed-invocations", 750, [3]),
        "case-005": (167, "restart-after-dlq-update", 750, [1]),
        "case-006": (173, "resume-after-dlq", 750, [1]),
        "case-007": (179, "outage-beyond-attempt-timeout", 3000, [3]),
        "case-008": (181, "mixed-success-dlq", 750, [3, 1, 5]),
    }
    if case_id not in cases:
        raise SetupBlock(f"unknown rung-003 case {case_id}")
    seed, schedule, down_ms, budgets = cases[case_id]
    suffix = f"rung3_{case_id.replace('-', '_')}_{seed}_{uuid.uuid4().hex[:8]}"
    app_db = f"wio_recovery_app_{suffix}"
    sys_db = f"wio_recovery_sys_{suffix}"
    return [
        CasePlan(
            case_id=f"{case_id}-wf-{index + 1}",
            seed=seed + index,
            schedule=schedule,
            restart_down_ms=down_ms,
            workflow_id=f"wio-recovery-rung3-{case_id}-{seed}-{index + 1}-{uuid.uuid4().hex[:8]}",
            app_db=app_db,
            sys_db=sys_db,
            max_recovery_attempts=budget,
        )
        for index, budget in enumerate(budgets)
    ]


def make_rung4_plans(case_id: str) -> list[CasePlan]:
    cases: dict[str, tuple[int, str, int, int, int]] = {}
    case_no = 1
    for index, (offset_ms, down_ms) in enumerate((offset, down) for offset in [0, 25] for down in [250, 750, 1500]):
        cases[f"case-{case_no:03d}"] = (200 + index, "restart-before-scan", down_ms, 1 + (index % 3), offset_ms)
        case_no += 1
    for index, (offset_ms, down_ms) in enumerate((offset, down) for offset in [0, 25] for down in [250, 750, 1500]):
        cases[f"case-{case_no:03d}"] = (206 + index, "restart-after-first-id", down_ms, max(2, 1 + (index % 5)), offset_ms)
        case_no += 1
    for index, (offset_ms, down_ms) in enumerate((offset, down) for offset in [0, 50] for down in [250, 750, 1500]):
        cases[f"case-{case_no:03d}"] = (212 + index, "restart-during-direct-execute", down_ms, 1 + (index % 3), offset_ms)
        case_no += 1
    for index, (offset_ms, down_ms) in enumerate((offset, down) for offset in [0, 50] for down in [250, 750, 1500]):
        schedule = "restart-during-queue-clear" if index < 3 else "restart-before-result-retrieve"
        cases[f"case-{case_no:03d}"] = (218 + index, schedule, down_ms, 1 + (index % 3), offset_ms)
        case_no += 1

    if case_id not in cases:
        raise SetupBlock(f"unknown rung-004 case {case_id}")
    seed, schedule, down_ms, workflow_count, offset_ms = cases[case_id]
    suffix = f"rung4_{case_id.replace('-', '_')}_{seed}_{uuid.uuid4().hex[:8]}"
    app_db = f"wio_recovery_app_{suffix}"
    sys_db = f"wio_recovery_sys_{suffix}"
    return [
        CasePlan(
            case_id=f"{case_id}-wf-{index + 1}",
            seed=seed + index,
            schedule=schedule,
            restart_down_ms=down_ms,
            workflow_id=f"wio-recovery-rung4-{case_id}-{seed}-{index + 1}-{uuid.uuid4().hex[:8]}",
            app_db=app_db,
            sys_db=sys_db,
            restart_offset_ms=offset_ms,
        )
        for index in range(workflow_count)
    ]


RUNG5_SOURCE_WORKFLOW_IDS = [
    "wio-recovery-rung4-case-011-210-1-834a0421",
    "wio-recovery-rung4-case-011-210-2-4323598a",
    "wio-recovery-rung4-case-011-210-3-a7625b6b",
    "wio-recovery-rung4-case-011-210-4-57606676",
    "wio-recovery-rung4-case-011-210-5-ab80ddd8",
]


def make_rung5_plans(case_id: str) -> list[CasePlan]:
    cases: dict[str, tuple[int, str, int, int, int]] = {
        # Source finding: rung-004 case-011, five workflows, 25ms offset, 750ms outage.
        "case-001": (210, "restart-after-first-id", 750, 5, 25),
        # Shrink attempt: preserve the same timing but reduce to the smallest workflow count.
        "case-002": (210, "restart-after-first-id", 750, 1, 25),
        # Shrink attempt: preserve the same fanout but reduce the outage duration bucket.
        "case-003": (210, "restart-after-first-id", 250, 5, 25),
        # Boundary probes: find the smallest workflow count at the smaller outage bucket.
        "case-004": (210, "restart-after-first-id", 250, 2, 25),
        "case-005": (210, "restart-after-first-id", 250, 3, 25),
        "case-006": (210, "restart-after-first-id", 250, 4, 25),
        # Minimized reproducer: five workflows are required, but the offset shrinks to zero.
        "case-007": (210, "restart-after-first-id", 250, 5, 0),
    }
    if case_id not in cases:
        raise SetupBlock(f"unknown rung-005 case {case_id}")

    seed, schedule, down_ms, workflow_count, offset_ms = cases[case_id]
    suffix = f"rung5_{case_id.replace('-', '_')}_{seed}"
    app_db = f"wio_recovery_app_{suffix}"
    sys_db = f"wio_recovery_sys_{suffix}"
    return [
        CasePlan(
            case_id=f"{case_id}-wf-{index + 1}",
            seed=seed + index,
            schedule=schedule,
            restart_down_ms=down_ms,
            workflow_id=RUNG5_SOURCE_WORKFLOW_IDS[index],
            app_db=app_db,
            sys_db=sys_db,
            restart_offset_ms=offset_ms,
        )
        for index in range(workflow_count)
    ]


def make_rung6_plans(case_id: str) -> list[CasePlan]:
    cases = {
        "case-001": (224, "dual-recoverer-one-queued-row", 1, 0),
        "case-002": (225, "dual-recoverer-batch-concurrency", 3, 0),
        "case-003": (226, "db-reconnect-after-clear-before-drain", 2, 750),
    }
    if case_id not in cases:
        raise SetupBlock(f"unknown rung-006 case {case_id}")
    seed, schedule, workflow_count, down_ms = cases[case_id]
    suffix = f"rung6_{case_id.replace('-', '_')}_{seed}_{uuid.uuid4().hex[:8]}"
    app_db = f"wio_recovery_app_{suffix}"
    sys_db = f"wio_recovery_sys_{suffix}"
    return [
        CasePlan(
            case_id=f"{case_id}-wf-{index + 1}",
            seed=seed + index,
            schedule=schedule,
            restart_down_ms=down_ms,
            workflow_id=f"wio-recovery-rung6-{case_id}-{seed}-{index + 1}-{uuid.uuid4().hex[:8]}",
            app_db=app_db,
            sys_db=sys_db,
        )
        for index in range(workflow_count)
    ]


def load_replay_plans(replay_case: str) -> list[CasePlan]:
    candidate = Path(replay_case)
    raw = candidate.read_text() if candidate.exists() else replay_case
    data = json.loads(raw)
    plans = data.get("plans", data) if isinstance(data, dict) else data
    if not isinstance(plans, list):
        raise SetupBlock("--replay-case must contain a list of plans or an object with a plans list")
    return [CasePlan(**item) for item in plans]


def password() -> str:
    return os.environ.get("PGPASSWORD", "dbos")


def db_url(database: str, *, driver: str = "postgresql+psycopg") -> str:
    return f"{driver}://postgres:{quote(password(), safe='')}@localhost:{os.environ.get('PGPORT', '5432')}/{database}"


def admin_engine() -> sa.Engine:
    return sa.create_engine(db_url("postgres", driver="postgresql+psycopg"), connect_args={"connect_timeout": 10})


def app_engine(plan: CasePlan) -> sa.Engine:
    return sa.create_engine(db_url(plan.app_db), connect_args={"connect_timeout": 5}, pool_pre_ping=True)


def sys_engine(plan: CasePlan) -> sa.Engine:
    return sa.create_engine(db_url(plan.sys_db, driver="postgresql+psycopg"), connect_args={"connect_timeout": 5}, pool_pre_ping=True)


def config(plan: CasePlan) -> DBOSConfig:
    return {
        "name": "wio-recovery-db-faults",
        "application_database_url": db_url(plan.app_db),
        "system_database_url": db_url(plan.sys_db, driver="postgresql+psycopg"),
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
    }


def configure_executor(executor_id: str) -> None:
    os.environ["DBOS__VMID"] = executor_id
    os.environ["DBOS__APPVERSION"] = APP_VERSION
    os.environ["DBOS__APPID"] = APP_ID


def transient_db_unavailable(exc: BaseException) -> bool:
    message = str(exc).lower()
    return any(
        token in message
        for token in [
            "connection refused",
            "connection failed",
            "server closed the connection",
            "terminating connection",
            "the database system is starting up",
            "could not connect to server",
            "could not receive data from server",
            "connection timeout expired",
            "connection is lost",
        ]
    )


def retry_transient_db(label: str, fn: Any, timeout_sec: float = 30.0) -> Any:
    deadline = time.monotonic() + timeout_sec
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn()
        except Exception as exc:
            if not transient_db_unavailable(exc) or time.monotonic() >= deadline:
                raise
            event("harness_transient_db_retry", label=label, attempt=attempt, error=f"{type(exc).__name__}: {exc}")
            time.sleep(0.25)


def cleanup_databases(plan: CasePlan) -> None:
    engine = admin_engine()
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        for name in [plan.app_db, plan.sys_db]:
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
    engine.dispose()


def ensure_harness_tables(plan: CasePlan) -> None:
    engine = app_engine(plan)
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                CREATE TABLE IF NOT EXISTS wio_recovery_ledger (
                  id BIGSERIAL PRIMARY KEY,
                  workflow_id TEXT NOT NULL,
                  step_name TEXT NOT NULL,
                  executor_id TEXT NOT NULL,
                  created_at_ms BIGINT NOT NULL
                )
                """
            )
        )
        conn.execute(
            sa.text(
                """
                CREATE TABLE IF NOT EXISTS wio_recovery_gates (
                  workflow_id TEXT PRIMARY KEY,
                  open BOOLEAN NOT NULL DEFAULT FALSE
                )
                """
            )
        )
        conn.execute(
            sa.text(
                "INSERT INTO wio_recovery_gates(workflow_id, open) VALUES (:wid, FALSE) "
                "ON CONFLICT (workflow_id) DO NOTHING"
            ),
            {"wid": plan.workflow_id},
        )
    engine.dispose()


def ledger_count(plan: CasePlan, step_name: str) -> int:
    engine = app_engine(plan)
    try:
        with engine.connect() as conn:
            return int(
                conn.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM wio_recovery_ledger "
                        "WHERE workflow_id = :wid AND step_name = :step"
                    ),
                    {"wid": plan.workflow_id, "step": step_name},
                ).scalar_one()
            )
    except Exception as exc:
        if "does not exist" in str(exc):
            return 0
        raise
    finally:
        engine.dispose()


def open_gate(plan: CasePlan) -> None:
    def write_gate() -> int:
        engine = app_engine(plan)
        try:
            with engine.begin() as conn:
                result = conn.execute(
                    sa.text("UPDATE wio_recovery_gates SET open = TRUE WHERE workflow_id = :wid"),
                    {"wid": plan.workflow_id},
                )
                return int(result.rowcount or 0)
        finally:
            engine.dispose()

    updated = retry_transient_db(f"open_gate:{plan.case_id}", write_gate, timeout_sec=60.0)
    event("gate_opened", case=plan.case_id, workflow_id=plan.workflow_id, updated_rows=updated)
    if updated != 1:
        raise WorkloadFailure(f"gate row missing for {plan.workflow_id}; updated_rows={updated}")


def gate_is_open(plan: CasePlan) -> bool:
    def read_gate() -> bool:
        engine = app_engine(plan)
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    sa.text("SELECT open FROM wio_recovery_gates WHERE workflow_id = :wid"),
                    {"wid": plan.workflow_id},
                ).scalar_one_or_none()
                return bool(row)
        finally:
            engine.dispose()

    try:
        return bool(retry_transient_db("gate_is_open", read_gate, timeout_sec=15.0))
    except Exception as exc:
        if transient_db_unavailable(exc):
            event("gate_poll_db_unavailable", case=plan.case_id, error=f"{type(exc).__name__}: {exc}")
            return False
        raise


def workflow_row(plan: CasePlan) -> dict[str, Any]:
    engine = sys_engine(plan)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                sa.text(
                    "SELECT workflow_uuid, status, executor_id, application_version, recovery_attempts, queue_name "
                    "FROM dbos.workflow_status WHERE workflow_uuid = :wid"
                ),
                {"wid": plan.workflow_id},
            ).mappings().one_or_none()
            return dict(row) if row is not None else {}
    except Exception as exc:
        if "does not exist" in str(exc):
            return {}
        raise
    finally:
        engine.dispose()


def workflow_rows(plans: list[CasePlan]) -> dict[str, dict[str, Any]]:
    return {plan.workflow_id: workflow_row(plan) for plan in plans}


def queue_name_for_rung6(plan: CasePlan) -> str:
    case_id = plan.case_id.split("-wf-")[0].replace("-", "_")
    return f"wio_recovery_race_{case_id}_{plan.seed}"


def active_queue_rows(plan: CasePlan, queue_name: str) -> list[dict[str, Any]]:
    engine = sys_engine(plan)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                sa.text(
                    """
                    SELECT workflow_uuid, status, executor_id, queue_name, started_at_epoch_ms
                    FROM dbos.workflow_status
                    WHERE queue_name = :queue_name
                      AND status IN ('ENQUEUED', 'DELAYED', 'PENDING')
                    ORDER BY created_at, workflow_uuid
                    """
                ),
                {"queue_name": queue_name},
            ).mappings().all()
            return [dict(row) for row in rows]
    finally:
        engine.dispose()


def workflow_status_to_dict(status: Any) -> dict[str, Any]:
    fields = [
        "workflow_id",
        "status",
        "name",
        "class_name",
        "config_name",
        "authenticated_user",
        "assumed_role",
        "authenticated_roles",
        "created_at",
        "updated_at",
        "queue_name",
        "executor_id",
        "app_version",
        "workflow_timeout_ms",
        "workflow_deadline_epoch_ms",
        "deduplication_id",
        "priority",
        "queue_partition_key",
        "forked_from",
        "was_forked_from",
        "parent_workflow_id",
    ]
    return {field: getattr(status, field, None) for field in fields}


def queued_list_snapshot(queue_name: str, label: str, executor_id: str | None = None) -> dict[str, Any]:
    try:
        rows = DBOS.list_queued_workflows(
            queue_name=queue_name,
            executor_id=executor_id,
            load_input=False,
            load_output=False,
            limit=100,
        )
        return {
            "label": label,
            "queue_name": queue_name,
            "executor_id": executor_id,
            "rows": [workflow_status_to_dict(row) for row in rows],
            "error": None,
        }
    except Exception as exc:
        return {
            "label": label,
            "queue_name": queue_name,
            "executor_id": executor_id,
            "rows": [],
            "error": f"{type(exc).__name__}: {exc}",
        }


def queued_list_snapshots(queue_name: str, label: str) -> dict[str, Any]:
    return {
        "all": queued_list_snapshot(queue_name, label),
        "dead_executor": queued_list_snapshot(queue_name, label, EXECUTOR_A),
        "drain_executor": queued_list_snapshot(queue_name, label, EXECUTOR_D),
    }


def ledger_rows(plan: CasePlan) -> list[dict[str, Any]]:
    engine = app_engine(plan)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                sa.text(
                    """
                    SELECT workflow_id, step_name, executor_id, created_at_ms
                    FROM wio_recovery_ledger
                    WHERE workflow_id = :wid
                    ORDER BY id
                    """
                ),
                {"wid": plan.workflow_id},
            ).mappings().all()
            return [dict(row) for row in rows]
    except Exception as exc:
        if "does not exist" in str(exc):
            return []
        raise
    finally:
        engine.dispose()


def ledger_rows_for_plans(plans: list[CasePlan]) -> dict[str, list[dict[str, Any]]]:
    return {plan.workflow_id: ledger_rows(plan) for plan in plans}


def force_rows_pending_for_executor(plans: list[CasePlan], executor_id: str, queue_name: str) -> list[dict[str, Any]]:
    base = plans[0]
    engine = sys_engine(base)
    try:
        with engine.begin() as conn:
            for plan in plans:
                conn.execute(
                    sa.text(
                        """
                        UPDATE dbos.workflow_status
                        SET status = 'PENDING',
                            executor_id = :executor_id,
                            queue_name = :queue_name,
                            application_version = :app_version
                        WHERE workflow_uuid = :workflow_id
                        """
                    ),
                    {
                        "executor_id": executor_id,
                        "queue_name": queue_name,
                        "app_version": APP_VERSION,
                        "workflow_id": plan.workflow_id,
                    },
                )
    finally:
        engine.dispose()
    rows = workflow_rows(plans)
    event("rung6_forced_pending_rows", executor_id=executor_id, queue_name=queue_name, rows=rows)
    return list(rows.values())


def recover_stale_snapshot(dbos: DBOS, snapshot: list[GetPendingWorkflowsOutput], label: str) -> dict[str, Any]:
    original = dbos._sys_db.get_pending_workflows
    original_clear = dbos._sys_db.clear_queue_assignment
    clear_attempts: list[dict[str, Any]] = []

    def stale_get_pending(executor_id: str, app_version: str) -> list[GetPendingWorkflowsOutput]:
        event(
            "rung6_stale_snapshot_used",
            label=label,
            requested_executor_id=executor_id,
            requested_app_version=app_version,
            workflow_ids=[row.workflow_id for row in snapshot],
        )
        return list(snapshot)

    def recording_clear_queue_assignment(workflow_id: str) -> bool:
        try:
            cleared = original_clear(workflow_id)
            clear_attempts.append({"workflow_id": workflow_id, "cleared": cleared, "error": None})
            return cleared
        except Exception as exc:
            clear_attempts.append({"workflow_id": workflow_id, "cleared": None, "error": f"{type(exc).__name__}: {exc}"})
            raise

    dbos._sys_db.get_pending_workflows = stale_get_pending  # type: ignore[method-assign]
    dbos._sys_db.clear_queue_assignment = recording_clear_queue_assignment  # type: ignore[method-assign]
    try:
        handles = DBOS._recover_pending_workflows([EXECUTOR_A])
        return {
            "label": label,
            "handle_ids": [handle.get_workflow_id() for handle in handles],
            "clear_attempts": clear_attempts,
            "error": None,
        }
    except Exception as exc:
        return {
            "label": label,
            "handle_ids": [],
            "clear_attempts": clear_attempts,
            "error": f"{type(exc).__name__}: {exc}",
        }
    finally:
        dbos._sys_db.get_pending_workflows = original  # type: ignore[method-assign]
        dbos._sys_db.clear_queue_assignment = original_clear  # type: ignore[method-assign]


def collect_stale_recoverer(
    dbos: DBOS,
    snapshot: list[GetPendingWorkflowsOutput],
    label: str,
    results: list[dict[str, Any]],
) -> None:
    GlobalParams.executor_id = EXECUTOR_C
    os.environ["DBOS__VMID"] = EXECUTOR_C
    results.append(recover_stale_snapshot(dbos, snapshot, label))


def get_handle_result(handle: Any, timeout_seconds: float = 30.0) -> Any:
    try:
        return handle.get_result(timeout_seconds=timeout_seconds)
    except TypeError:
        return handle.get_result()


def wait_for_rung6_drain_turn(current: CasePlan, future: list[CasePlan], queue_name: str) -> dict[str, Any]:
    deadline = time.monotonic() + 90
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        current_row = workflow_row(current)
        future_rows = workflow_rows(future) if future else {}
        future_step_one_counts = {plan.workflow_id: ledger_count(plan, "step_one") for plan in future}
        last = {
            "current_row": current_row,
            "future_rows": future_rows,
            "future_step_one_counts": future_step_one_counts,
            "active_queue_rows": active_queue_rows(current, queue_name),
        }
        if (
            current_row.get("status") == WorkflowStatusString.PENDING.value
            and current_row.get("executor_id") == EXECUTOR_D
            and all(count == 0 for count in future_step_one_counts.values())
        ):
            event("rung6_drain_turn_ready", workflow_id=current.workflow_id, snapshot=last)
            return last
        time.sleep(0.1)
    raise WorkloadFailure(f"queue drain did not reach serialized turn for {current.workflow_id}; last={last}")


def wait_for_rung6_terminal(plan: CasePlan) -> dict[str, Any]:
    deadline = time.monotonic() + 90
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        row = workflow_row(plan)
        last = {
            "row": row,
            "step_two_count": ledger_count(plan, "step_two"),
        }
        if row.get("status") in TERMINAL_STATUSES and last["step_two_count"] == 1:
            event("rung6_drain_turn_completed", workflow_id=plan.workflow_id, snapshot=last)
            return last
        time.sleep(0.1)
    raise WorkloadFailure(f"queue drain did not terminally complete {plan.workflow_id}; last={last}")


def operation_output_count(plan: CasePlan, function_name_fragment: str) -> int:
    engine = sys_engine(plan)
    try:
        with engine.connect() as conn:
            return int(
                conn.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM dbos.operation_outputs "
                        "WHERE workflow_uuid = :wid AND function_name LIKE :name"
                    ),
                    {"wid": plan.workflow_id, "name": f"%{function_name_fragment}%"},
                ).scalar_one()
            )
    except Exception as exc:
        if "does not exist" in str(exc):
            return 0
        raise
    finally:
        engine.dispose()


def pg_ready() -> bool:
    cmd = [
        "pg_isready",
        "-h",
        os.environ.get("WIO_PGHOST_ADDR", "127.0.0.1"),
        "-p",
        os.environ.get("PGPORT", "5432"),
        "-U",
        "postgres",
    ]
    return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False).returncode == 0


def wait_for_pg_ready(timeout_sec: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if pg_ready():
            return
        time.sleep(0.25)
    raise SetupBlock("postgres did not become ready after restart")


def restart_postgres(plan: CasePlan, label: str) -> None:
    pgdata = os.environ.get("WIO_PGDATA", "/tmp/wio-postgres-data")
    pglog = os.environ.get("WIO_PGLOG", "/tmp/wio-postgres.log")
    host = os.environ.get("WIO_PGHOST_ADDR", "127.0.0.1")
    port = os.environ.get("PGPORT", "5432")
    run_prefix = [] if os.environ.get("WIO_PG_NO_SU") == "1" else ["su", "postgres", "-c"]

    def run_pg_ctl(command: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if run_prefix:
            return subprocess.run([*run_prefix, command], text=True, **kwargs)
        return subprocess.run(command, shell=True, text=True, **kwargs)

    if plan.restart_offset_ms > 0:
        event("postgres_restart_offset_wait", case=plan.case_id, label=label, offset_ms=plan.restart_offset_ms)
        time.sleep(plan.restart_offset_ms / 1000)
    event("postgres_restart_begin", case=plan.case_id, label=label, down_ms=plan.restart_down_ms, offset_ms=plan.restart_offset_ms)
    run_pg_ctl(f"pg_ctl -D '{pgdata}' -m fast stop", check=False, timeout=20)
    time.sleep(plan.restart_down_ms / 1000)
    start = run_pg_ctl(
        f"pg_ctl -D '{pgdata}' -l '{pglog}' -o \"-k /tmp -h {host} -p {port}\" start",
        capture_output=True,
        check=False,
        timeout=20,
    )
    if start.returncode != 0:
        raise SetupBlock(
            f"postgres restart failed label={label} rc={start.returncode} "
            f"stdout={start.stdout[-500:]} stderr={start.stderr[-500:]}"
        )
    wait_for_pg_ready()
    event("postgres_restart_end", case=plan.case_id, label=label)


def reset_step_two_restart_coordination() -> None:
    STEP_TWO_RESTART_REQUESTED.clear()
    STEP_TWO_RESTART_DONE.clear()
    STEP_TWO_RESTART_ERRORS.clear()


def restart_when_step_two_requests(plan: CasePlan) -> None:
    if not STEP_TWO_RESTART_REQUESTED.wait(timeout=120):
        STEP_TWO_RESTART_ERRORS.append("step_two did not request restart")
        STEP_TWO_RESTART_DONE.set()
        return
    try:
        restart_postgres(plan, "during-recovered-execute")
    except Exception as exc:
        STEP_TWO_RESTART_ERRORS.append(f"{type(exc).__name__}: {exc}")
        event("step_two_external_restart_failed", case=plan.case_id, error=STEP_TWO_RESTART_ERRORS[-1])
    finally:
        STEP_TWO_RESTART_DONE.set()


def insert_ledger(plan: CasePlan, step_name: str) -> None:
    def write_ledger() -> None:
        ensure_harness_tables(plan)
        engine = app_engine(plan)
        try:
            with engine.begin() as conn:
                conn.execute(
                    sa.text(
                        "INSERT INTO wio_recovery_ledger(workflow_id, step_name, executor_id, created_at_ms) "
                        "VALUES (:wid, :step, :executor, :created)"
                    ),
                    {
                        "wid": plan.workflow_id,
                        "step": step_name,
                        "executor": os.environ.get("DBOS__VMID", "unknown"),
                        "created": int(time.time() * 1000),
                    },
                )
        finally:
            engine.dispose()

    retry_transient_db(f"insert_ledger:{step_name}", write_ledger)


def recovery_step_one(plan_json: str) -> str:
    plan = CasePlan(**json.loads(plan_json))
    insert_ledger(plan, "step_one")
    event("step_one_completed", case=plan.case_id, workflow_id=plan.workflow_id)
    return "step-one-ok"


def recovery_step_two(plan_json: str) -> str:
    plan = CasePlan(**json.loads(plan_json))
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        if gate_is_open(plan):
            break
        time.sleep(0.25)
    else:
        raise TimeoutError("gate did not open")

    if os.environ.get("WIO_RECOVERY_RESTART_IN_STEP_TWO") == "1":
        os.environ["WIO_RECOVERY_RESTART_IN_STEP_TWO"] = "0"
        if os.environ.get("WIO_RECOVERY_EXTERNAL_STEP_TWO_RESTART") == "1":
            event("step_two_restart_requested", case=plan.case_id)
            STEP_TWO_RESTART_REQUESTED.set()
            if not STEP_TWO_RESTART_DONE.wait(timeout=120):
                raise TimeoutError("external step_two restart did not finish")
            if STEP_TWO_RESTART_ERRORS:
                raise RuntimeError(f"external step_two restart failed: {STEP_TWO_RESTART_ERRORS[-1]}")
        else:
            restart_postgres(plan, "during-recovered-execute")

    insert_ledger(plan, "step_two")
    return f"result-{plan.workflow_id}"


@DBOS.step()
def step_one(plan_json: str) -> str:
    return recovery_step_one(plan_json)


@DBOS.step()
def step_two(plan_json: str) -> str:
    return recovery_step_two(plan_json)


@DBOS.workflow()
def recovery_workflow(plan_json: str) -> str:
    step_one(plan_json)
    return step_two(plan_json)


@DBOS.workflow(max_recovery_attempts=1)
def recovery_workflow_max1(plan_json: str) -> str:
    step_one(plan_json)
    return step_two(plan_json)


@DBOS.workflow(max_recovery_attempts=3)
def recovery_workflow_max3(plan_json: str) -> str:
    step_one(plan_json)
    return step_two(plan_json)


@DBOS.workflow(max_recovery_attempts=5)
def recovery_workflow_max5(plan_json: str) -> str:
    step_one(plan_json)
    return step_two(plan_json)


def workflow_for_plan(plan: CasePlan) -> Any:
    if plan.max_recovery_attempts == 1:
        return recovery_workflow_max1
    if plan.max_recovery_attempts == 3:
        return recovery_workflow_max3
    if plan.max_recovery_attempts == 5:
        return recovery_workflow_max5
    return recovery_workflow


def child_main(plan: CasePlan) -> int:
    configure_executor(EXECUTOR_A)
    DBOS.destroy(destroy_registry=False)
    DBOS(config=config(plan))
    DBOS.launch()
    ensure_harness_tables(plan)
    with SetWorkflowID(plan.workflow_id):
        handle = DBOS.start_workflow(workflow_for_plan(plan), json.dumps(asdict(plan), sort_keys=True))
    event("child_started_workflow", case=plan.case_id, workflow_id=plan.workflow_id)
    handle.get_result()
    return 0


def wait_for_child_pending(plan: CasePlan, child: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 90
    last_row: dict[str, Any] = {}
    while time.monotonic() < deadline:
        if child.poll() is not None:
            raise WorkloadFailure(f"child exited before pending state: {child.returncode}")
        count = ledger_count(plan, "step_one")
        row = workflow_row(plan)
        last_row = row
        if count == 1 and row.get("status") == WorkflowStatusString.PENDING.value:
            event("pending_state_reached", case=plan.case_id, row=row)
            return
        time.sleep(0.5)
    raise WorkloadFailure(f"workflow did not reach pending state; last_row={last_row}")


def stop_child(child: subprocess.Popen[str]) -> None:
    if child.poll() is not None:
        return
    child.send_signal(signal.SIGTERM)
    try:
        child.wait(timeout=10)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=10)


def launch_child(plan: CasePlan) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["DBOS__VMID"] = EXECUTOR_A
    env["DBOS__APPVERSION"] = APP_VERSION
    env["DBOS__APPID"] = APP_ID
    cmd = [sys.executable, str(Path(__file__).resolve()), "--child", "--case", plan.case_id, "--plan-json", json.dumps(asdict(plan))]
    return subprocess.Popen(cmd, env=env, text=True)


def recover_with_optional_fault(plan: CasePlan, dbos: DBOS) -> list[Any]:
    if plan.schedule == "restart-before-scan":
        restart_postgres(plan, "before-scan")
        return DBOS._recover_pending_workflows([EXECUTOR_A])

    if plan.schedule == "restart-after-scan-before-execute":
        original = dbos._sys_db.get_pending_workflows

        def wrapped_get_pending(executor_id: str, app_version: str) -> Any:
            rows = original(executor_id, app_version)
            event("pending_ids_captured", case=plan.case_id, ids=[row.workflow_id for row in rows])
            restart_postgres(plan, "after-scan-before-execute")
            return rows

        dbos._sys_db.get_pending_workflows = wrapped_get_pending  # type: ignore[method-assign]
        try:
            return DBOS._recover_pending_workflows([EXECUTOR_A])
        finally:
            dbos._sys_db.get_pending_workflows = original  # type: ignore[method-assign]

    if plan.schedule == "restart-during-recovered-execute":
        os.environ["WIO_RECOVERY_RESTART_IN_STEP_TWO"] = "1"
        return DBOS._recover_pending_workflows([EXECUTOR_A])

    raise SetupBlock(f"unsupported schedule {plan.schedule}")


def recover_matrix_with_optional_fault(plans: list[CasePlan], dbos: DBOS) -> list[Any]:
    schedule = plans[0].schedule
    if schedule in {"restart-before-scan", "restart-longer-transient"}:
        restart_postgres(plans[0], schedule)
        return DBOS._recover_pending_workflows([EXECUTOR_A])

    if schedule == "restart-after-first-id":
        original = recovery_module._recover_workflow
        seen = {"count": 0}

        def wrapped_recover(inner_dbos: DBOS, workflow: Any) -> Any:
            handle = original(inner_dbos, workflow)
            seen["count"] += 1
            if seen["count"] == 1:
                event("first_pending_id_recovered", case="case-002", workflow_id=workflow.workflow_id)
                restart_postgres(plans[0], "after-first-id")
            return handle

        recovery_module._recover_workflow = wrapped_recover  # type: ignore[assignment]
        try:
            return DBOS._recover_pending_workflows([EXECUTOR_A])
        finally:
            recovery_module._recover_workflow = original  # type: ignore[assignment]

    if schedule == "restart-during-direct-execute":
        reset_step_two_restart_coordination()
        os.environ["WIO_RECOVERY_EXTERNAL_STEP_TWO_RESTART"] = "1"
        thread = threading.Thread(target=restart_when_step_two_requests, args=(plans[0],), daemon=True)
        thread.start()
        os.environ["WIO_RECOVERY_RESTART_IN_STEP_TWO"] = "1"
        handles = DBOS._recover_pending_workflows([EXECUTOR_A])
        thread.join(timeout=5)
        return handles

    if schedule == "restart-during-queue-clear":
        original = dbos._sys_db.clear_queue_assignment
        seen = {"done": False}

        def wrapped_clear(workflow_id: str) -> bool:
            cleared = original(workflow_id)
            if not seen["done"]:
                seen["done"] = True
                event("queue_assignment_clear_observed", case="case-004", workflow_id=workflow_id, cleared=cleared)
                restart_postgres(plans[0], "during-queue-clear")
            return cleared

        dbos._sys_db.clear_queue_assignment = wrapped_clear  # type: ignore[method-assign]
        try:
            return DBOS._recover_pending_workflows([EXECUTOR_A])
        finally:
            dbos._sys_db.clear_queue_assignment = original  # type: ignore[method-assign]

    if schedule == "restart-before-result-retrieve":
        handles = DBOS._recover_pending_workflows([EXECUTOR_A])
        return handles

    raise SetupBlock(f"unsupported rung-002 schedule {schedule}")


def start_plan_workflow(plan: CasePlan, *, queued: bool) -> Any:
    with SetWorkflowID(plan.workflow_id):
        if queued:
            return DBOS.enqueue_workflow(QUEUE_NAME, workflow_for_plan(plan), json.dumps(asdict(plan), sort_keys=True))
        return DBOS.start_workflow(workflow_for_plan(plan), json.dumps(asdict(plan), sort_keys=True))


def wait_for_all_pending(plans: list[CasePlan]) -> list[dict[str, Any]]:
    deadline = time.monotonic() + 180
    rows: list[dict[str, Any]] = []
    last_event_at = 0.0
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            rows = [workflow_row(plan) for plan in plans]
            step_one_counts = {plan.workflow_id: ledger_count(plan, "step_one") for plan in plans}
        except Exception as exc:
            if not transient_db_unavailable(exc):
                raise
            last_error = f"{type(exc).__name__}: {exc}"
            event("matrix_pending_poll_db_unavailable", expected=len(plans), error=last_error)
            time.sleep(0.25)
            continue
        if all(step_one_counts[plan.workflow_id] == 1 and row.get("status") == WorkflowStatusString.PENDING.value for plan, row in zip(plans, rows)):
            event("matrix_pending_state_reached", count=len(plans), rows=rows)
            return rows
        now = time.monotonic()
        if now - last_event_at >= 10.0:
            last_event_at = now
            event(
                "matrix_pending_wait",
                expected=len(plans),
                step_one_counts=step_one_counts,
                rows=rows,
                last_error=last_error,
            )
        time.sleep(0.25)
    raise WorkloadFailure(f"matrix workflows did not all reach pending state; rows={rows}; last_error={last_error}")


def parent_main_rung2(plans: list[CasePlan], artifact_dir: Path, rung_label: str = "rung-002-recovery-window-matrix") -> int:
    base = plans[0]
    case_id = base.case_id.split("-wf-")[0]
    queued = base.schedule == "restart-during-queue-clear"
    event("case_start", frontier=FRONTIER_ID, rung=rung_label, case_id=case_id, workflow_count=len(plans), schedule=base.schedule, restart_down_ms=base.restart_down_ms, restart_offset_ms=base.restart_offset_ms)
    cleanup_databases(base)
    configure_executor(EXECUTOR_A)
    DBOS.destroy(destroy_registry=False)
    dbos = DBOS(config=config(base))
    DBOS.launch()
    if queued:
        DBOS.register_queue(QUEUE_NAME, worker_concurrency=max(1, len(plans)))
    for plan in plans:
        ensure_harness_tables(plan)

    original_handles = [start_plan_workflow(plan, queued=queued) for plan in plans]
    rows_after_start = wait_for_all_pending(plans)
    invariant("matrix_all_pending_on_executor_a", all(row.get("executor_id") == EXECUTOR_A for row in rows_after_start), rows=rows_after_start)
    invariant("matrix_step_one_once_before_recovery", all(ledger_count(plan, "step_one") == 1 for plan in plans), workflow_ids=[plan.workflow_id for plan in plans])

    handles = recover_matrix_with_optional_fault(plans, dbos)
    invariant("matrix_recovery_returned_expected_handles", len(handles) == len(plans), handle_count=len(handles), expected=len(plans))
    for plan in plans:
        open_gate(plan)

    if base.schedule == "restart-before-result-retrieve":
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and not all(workflow_row(plan).get("status") in TERMINAL_STATUSES for plan in plans):
            time.sleep(0.25)
        restart_postgres(base, "before-result-retrieve")

    results: dict[str, Any] = {}
    last_errors: dict[str, str] = {}
    handles_by_id = {handle.get_workflow_id(): handle for handle in handles}
    for plan, original_handle in zip(plans, original_handles):
        handle = handles_by_id.get(plan.workflow_id, original_handle)
        try:
            results[plan.workflow_id] = handle.get_result()
        except Exception as exc:
            last_errors[plan.workflow_id] = f"{type(exc).__name__}: {exc}"
            try:
                results[plan.workflow_id] = original_handle.get_result()
            except Exception as original_exc:
                last_errors[plan.workflow_id] = f"{type(original_exc).__name__}: {original_exc}"

    final_rows = {plan.workflow_id: workflow_row(plan) for plan in plans}
    step_one_counts = {plan.workflow_id: ledger_count(plan, "step_one") for plan in plans}
    step_two_counts = {plan.workflow_id: ledger_count(plan, "step_two") for plan in plans}
    step_one_outputs = {plan.workflow_id: operation_output_count(plan, "step_one") for plan in plans}
    write_matrix_artifacts(artifact_dir, case_id, plans, final_rows, results, step_one_counts, step_two_counts, step_one_outputs, last_errors)

    invariant("matrix_all_terminal", all(row.get("status") in TERMINAL_STATUSES for row in final_rows.values()), rows=final_rows)
    invariant("matrix_all_succeeded", all(row.get("status") == WorkflowStatusString.SUCCESS.value for row in final_rows.values()), rows=final_rows)
    invariant("matrix_no_dead_executor_pending_rows", not any(row.get("status") == WorkflowStatusString.PENDING.value and row.get("executor_id") == EXECUTOR_A for row in final_rows.values()), rows=final_rows)
    invariant("matrix_completed_step_ledger_once", all(count == 1 for count in step_one_counts.values()), counts=step_one_counts)
    invariant("matrix_completed_step_checkpoint_once", all(count == 1 for count in step_one_outputs.values()), counts=step_one_outputs)
    invariant("matrix_recovered_step_completed_once", all(count == 1 for count in step_two_counts.values()), counts=step_two_counts)
    invariant("matrix_handle_results_match_model", all(results.get(plan.workflow_id) == f"result-{plan.workflow_id}" for plan in plans), results=results, errors=last_errors)
    event("case_passed", case=case_id, workflow_ids=[plan.workflow_id for plan in plans])
    DBOS.destroy(destroy_registry=False)
    cleanup_databases(base)
    return 0


def force_pending_for_recovery(dbos: DBOS, plan: CasePlan, label: str) -> dict[str, Any]:
    dbos._sys_db.update_workflow_outcome(plan.workflow_id, WorkflowStatusString.PENDING.value)
    row = workflow_row(plan)
    event("rung3_forced_pending", case=plan.case_id, label=label, row=row)
    return row


def recover_rung3_cycle(dbos: DBOS, plan: CasePlan, cycle: int, *, restart: bool) -> dict[str, Any]:
    force_pending_for_recovery(dbos, plan, f"cycle-{cycle}")
    if restart:
        restart_postgres(plan, f"rung3-cycle-{cycle}")
    handles = DBOS._recover_pending_workflows([EXECUTOR_A])
    results: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for handle in handles:
        workflow_id = handle.get_workflow_id()
        try:
            results[workflow_id] = handle.get_result()
        except Exception as exc:
            errors[workflow_id] = f"{type(exc).__name__}: {exc}"
    row = workflow_row(plan)
    record = {
        "cycle": cycle,
        "restart": restart,
        "handle_count": len(handles),
        "handle_ids": [handle.get_workflow_id() for handle in handles],
        "row": row,
        "results": results,
        "errors": errors,
    }
    event("rung3_recovery_cycle", case=plan.case_id, cycle=cycle, record=record)
    return record


def run_until_dlq(dbos: DBOS, plan: CasePlan, *, restart_each: bool, max_cycles: int = 8) -> list[dict[str, Any]]:
    timeline = []
    for cycle in range(1, max_cycles + 1):
        record = recover_rung3_cycle(dbos, plan, cycle, restart=restart_each)
        timeline.append(record)
        if record["row"].get("status") == WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value:
            return timeline
    return timeline


def invoke_completed_workflow(plan: CasePlan, count: int) -> list[dict[str, Any]]:
    invocations = []
    for attempt in range(1, count + 1):
        with SetWorkflowID(plan.workflow_id):
            try:
                result = workflow_for_plan(plan)(json.dumps(asdict(plan), sort_keys=True))
                error = None
            except Exception as exc:
                result = None
                error = f"{type(exc).__name__}: {exc}"
        row = workflow_row(plan)
        record = {"attempt": attempt, "result": result, "error": error, "row": row}
        event("rung3_duplicate_completed_invocation", case=plan.case_id, record=record)
        invocations.append(record)
    return invocations


def parent_main_rung3(plans: list[CasePlan], artifact_dir: Path) -> int:
    base = plans[0]
    case_id = base.case_id.split("-wf-")[0]
    event(
        "case_start",
        frontier=FRONTIER_ID,
        rung="rung-003-replay-dlq-liveness",
        case_id=case_id,
        workflow_count=len(plans),
        schedule=base.schedule,
        restart_down_ms=base.restart_down_ms,
        max_recovery_attempts=[plan.max_recovery_attempts for plan in plans],
    )
    cleanup_databases(base)
    configure_executor(EXECUTOR_A)
    DBOS.destroy(destroy_registry=False)
    dbos = DBOS(config=config(base))
    DBOS.launch()
    for plan in plans:
        ensure_harness_tables(plan)

    original_handles = [start_plan_workflow(plan, queued=False) for plan in plans]
    rows_after_start = wait_for_all_pending(plans)
    invariant("rung3_all_pending_on_executor_a", all(row.get("executor_id") == EXECUTOR_A for row in rows_after_start), rows=rows_after_start)
    invariant("rung3_step_one_once_before_replay", all(ledger_count(plan, "step_one") == 1 for plan in plans), workflow_ids=[plan.workflow_id for plan in plans])

    for plan in plans:
        open_gate(plan)

    initial_results: dict[str, Any] = {}
    initial_errors: dict[str, str] = {}
    for plan, handle in zip(plans, original_handles):
        try:
            initial_results[plan.workflow_id] = handle.get_result()
        except Exception as exc:
            initial_errors[plan.workflow_id] = f"{type(exc).__name__}: {exc}"
    invariant("rung3_initial_completion_succeeded", not initial_errors, results=initial_results, errors=initial_errors)

    timeline: dict[str, list[dict[str, Any]]] = {plan.workflow_id: [] for plan in plans}
    duplicate_invocations: dict[str, list[dict[str, Any]]] = {}
    resume_results: dict[str, Any] = {}
    resume_errors: dict[str, str] = {}
    expected_statuses: dict[str, str] = {plan.workflow_id: WorkflowStatusString.SUCCESS.value for plan in plans}

    if base.schedule == "one-outage-then-healthy":
        timeline[base.workflow_id] = [recover_rung3_cycle(dbos, base, 1, restart=True)]
    elif base.schedule == "two-outages-then-healthy":
        timeline[base.workflow_id] = [recover_rung3_cycle(dbos, base, cycle, restart=True) for cycle in range(1, 3)]
    elif base.schedule == "exceed-recovery-budget":
        timeline[base.workflow_id] = run_until_dlq(dbos, base, restart_each=False)
        expected_statuses[base.workflow_id] = WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value
    elif base.schedule == "duplicate-completed-invocations":
        duplicate_invocations[base.workflow_id] = invoke_completed_workflow(base, 6)
    elif base.schedule == "restart-after-dlq-update":
        timeline[base.workflow_id] = run_until_dlq(dbos, base, restart_each=False)
        expected_statuses[base.workflow_id] = WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value
        restart_postgres(base, "after-dlq-update")
        event("rung3_post_dlq_restart_row", case=base.case_id, row=workflow_row(base))
    elif base.schedule == "resume-after-dlq":
        timeline[base.workflow_id] = run_until_dlq(dbos, base, restart_each=False)
        resumed_handle = DBOS.resume_workflow(base.workflow_id)
        recovered_resume_handles = DBOS._recover_pending_workflows([EXECUTOR_A])
        try:
            handle = recovered_resume_handles[0] if recovered_resume_handles else resumed_handle
            resume_results[base.workflow_id] = handle.get_result()
        except Exception as exc:
            resume_errors[base.workflow_id] = f"{type(exc).__name__}: {exc}"
            recovered_resume_handles = DBOS._recover_pending_workflows([EXECUTOR_A])
            for handle in recovered_resume_handles:
                try:
                    resume_results[handle.get_workflow_id()] = handle.get_result()
                except Exception as handle_exc:
                    resume_errors[handle.get_workflow_id()] = f"{type(handle_exc).__name__}: {handle_exc}"
        expected_statuses[base.workflow_id] = WorkflowStatusString.SUCCESS.value
    elif base.schedule == "outage-beyond-attempt-timeout":
        timeline[base.workflow_id] = [recover_rung3_cycle(dbos, base, 1, restart=True)]
    elif base.schedule == "mixed-success-dlq":
        timeline[plans[0].workflow_id] = [recover_rung3_cycle(dbos, plans[0], 1, restart=True)]
        timeline[plans[1].workflow_id] = run_until_dlq(dbos, plans[1], restart_each=False)
        expected_statuses[plans[1].workflow_id] = WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value
        timeline[plans[2].workflow_id] = [recover_rung3_cycle(dbos, plans[2], cycle, restart=True) for cycle in range(1, 3)]
    else:
        raise SetupBlock(f"unsupported rung-003 schedule {base.schedule}")

    final_rows = {plan.workflow_id: workflow_row(plan) for plan in plans}
    step_one_counts = {plan.workflow_id: ledger_count(plan, "step_one") for plan in plans}
    step_two_counts = {plan.workflow_id: ledger_count(plan, "step_two") for plan in plans}
    step_one_outputs = {plan.workflow_id: operation_output_count(plan, "step_one") for plan in plans}
    step_two_outputs = {plan.workflow_id: operation_output_count(plan, "step_two") for plan in plans}
    write_rung3_artifacts(
        artifact_dir,
        case_id,
        plans,
        expected_statuses,
        final_rows,
        timeline,
        initial_results,
        resume_results,
        resume_errors,
        duplicate_invocations,
        step_one_counts,
        step_two_counts,
        step_one_outputs,
        step_two_outputs,
    )

    invariant("rung3_all_modeled_terminal", all(row.get("status") in TERMINAL_STATUSES for row in final_rows.values()), rows=final_rows)
    invariant("rung3_terminal_matches_model", all(final_rows[wid].get("status") == expected for wid, expected in expected_statuses.items()), rows=final_rows, expected=expected_statuses)
    invariant("rung3_no_dead_executor_pending_rows", not any(row.get("status") == WorkflowStatusString.PENDING.value and row.get("executor_id") == EXECUTOR_A for row in final_rows.values()), rows=final_rows)
    invariant("rung3_completed_step_ledger_once", all(count == 1 for count in step_one_counts.values()), counts=step_one_counts)
    invariant("rung3_completed_step_checkpoint_once", all(count == 1 for count in step_one_outputs.values()), counts=step_one_outputs)
    invariant("rung3_recovered_step_ledger_once", all(count == 1 for count in step_two_counts.values()), counts=step_two_counts)
    invariant("rung3_recovered_step_checkpoint_once", all(count == 1 for count in step_two_outputs.values()), counts=step_two_outputs)
    invariant("rung3_attempt_timeline_recorded", all(records or base.schedule == "duplicate-completed-invocations" for records in timeline.values()), timeline=timeline)
    if base.schedule == "restart-after-dlq-update":
        invariant("rung3_dlq_stable_after_restart", final_rows[base.workflow_id].get("status") == WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value, row=final_rows[base.workflow_id])
    if base.schedule == "resume-after-dlq":
        invariant("rung3_resume_after_dlq_succeeded", final_rows[base.workflow_id].get("status") == WorkflowStatusString.SUCCESS.value and not resume_errors, row=final_rows[base.workflow_id], results=resume_results, errors=resume_errors)
    if base.schedule == "duplicate-completed-invocations":
        invariant("rung3_duplicate_completed_invocations_stable", all(item.get("error") is None and item["row"].get("status") == WorkflowStatusString.SUCCESS.value for item in duplicate_invocations[base.workflow_id]), invocations=duplicate_invocations[base.workflow_id])

    event("case_passed", case=case_id, workflow_ids=[plan.workflow_id for plan in plans])
    DBOS.destroy(destroy_registry=False)
    cleanup_databases(base)
    return 0


def parent_main_rung6(plans: list[CasePlan], artifact_dir: Path) -> int:
    base = plans[0]
    case_id = base.case_id.split("-wf-")[0]
    queue_name = queue_name_for_rung6(base)
    event(
        "case_start",
        frontier=FRONTIER_ID,
        rung="rung-006-concurrent-queued-recovery-ownership",
        case_id=case_id,
        workflow_count=len(plans),
        schedule=base.schedule,
        queue_name=queue_name,
    )
    cleanup_databases(base)

    configure_executor(EXECUTOR_A)
    DBOS.destroy(destroy_registry=False)
    DBOS(config={**config(base), "executor_id": EXECUTOR_A})
    DBOS.listen_queues([])
    DBOS.launch()
    DBOS.register_queue(
        queue_name,
        concurrency=1,
        worker_concurrency=1,
        polling_interval_sec=0.05,
        on_conflict="always_update",
    )
    for plan in plans:
        ensure_harness_tables(plan)
    original_handles = []
    for plan in plans:
        with SetWorkflowID(plan.workflow_id):
            original_handles.append(DBOS.enqueue_workflow(queue_name, workflow_for_plan(plan), json.dumps(asdict(plan), sort_keys=True)))

    rows_after_enqueue = workflow_rows(plans)
    invariant(
        "rung6_all_modeled_rows_enqueued_before_force",
        all(row.get("status") == WorkflowStatusString.ENQUEUED.value and row.get("queue_name") == queue_name for row in rows_after_enqueue.values()),
        rows=rows_after_enqueue,
    )
    rows_after_force = force_rows_pending_for_executor(plans, EXECUTOR_A, queue_name)
    DBOS.destroy(destroy_registry=False, workflow_completion_timeout_sec=1)

    original_handle_ids = [handle.get_workflow_id() for handle in original_handles]
    invariant(
        "rung6_all_modeled_rows_pending_on_dead_executor",
        all(row.get("status") == WorkflowStatusString.PENDING.value and row.get("executor_id") == EXECUTOR_A for row in rows_after_force),
        rows=rows_after_force,
    )
    snapshot = [
        GetPendingWorkflowsOutput(workflow_id=plan.workflow_id, queue_name=queue_name)
        for plan in plans
    ]
    event("rung6_captured_stale_snapshot", workflow_ids=[row.workflow_id for row in snapshot], queue_name=queue_name)

    configure_executor(EXECUTOR_B)
    DBOS.destroy(destroy_registry=False)
    dbos_b = DBOS(config={**config(base), "executor_id": EXECUTOR_B})
    DBOS.listen_queues([])
    DBOS.launch()
    clear_result = recover_stale_snapshot(dbos_b, snapshot, "recoverer-b-clear")
    rows_after_clear = workflow_rows(plans)
    queued_after_clear = queued_list_snapshots(queue_name, "after-clear")
    invariant(
        "rung6_recoverer_b_returned_polling_handles",
        sorted(clear_result["handle_ids"]) == sorted(plan.workflow_id for plan in plans) and clear_result["error"] is None,
        clear_result=clear_result,
        rows_after_clear=rows_after_clear,
    )
    invariant(
        "rung6_clear_removed_dead_executor_pending",
        not any(row.get("status") == WorkflowStatusString.PENDING.value and row.get("executor_id") == EXECUTOR_A for row in rows_after_clear.values()),
        rows_after_clear=rows_after_clear,
    )

    if base.schedule == "db-reconnect-after-clear-before-drain":
        restart_postgres(base, "after-clear-before-drain")

    stale_results: list[dict[str, Any]] = []
    stale_thread = threading.Thread(
        target=collect_stale_recoverer,
        args=(dbos_b, snapshot, "recoverer-c-stale", stale_results),
        daemon=True,
    )
    stale_thread.start()
    stale_thread.join(timeout=1.0)
    stale_thread_alive_before_gate = stale_thread.is_alive()
    event("rung6_stale_recoverer_probe", alive_before_gate=stale_thread_alive_before_gate, stale_results=stale_results)

    if not stale_thread_alive_before_gate:
        DBOS.destroy(destroy_registry=False, workflow_completion_timeout_sec=1)
        configure_executor(EXECUTOR_D)
        DBOS.destroy(destroy_registry=False)
        dbos_d = DBOS(config={**config(base), "executor_id": EXECUTOR_D})
        DBOS.listen_queues([queue_name])
        DBOS.launch()
    else:
        dbos_d = None

    drain_turns: list[dict[str, Any]] = []
    queued_after_redequeue: dict[str, Any] = {}
    if stale_thread_alive_before_gate:
        for plan in plans:
            open_gate(plan)
    else:
        queued_after_redequeue = queued_list_snapshots(queue_name, "after-redequeue-before-gates")
        for index, plan in enumerate(plans):
            turn_ready = wait_for_rung6_drain_turn(plan, plans[index + 1 :], queue_name)
            open_gate(plan)
            turn_done = wait_for_rung6_terminal(plan)
            drain_turns.append(
                {
                    "workflow_id": plan.workflow_id,
                    "before_gate": turn_ready,
                    "after_terminal": turn_done,
                    "queued_snapshot_after_terminal": queued_list_snapshots(queue_name, f"after-terminal-{index + 1}"),
                }
            )

    if stale_thread_alive_before_gate:
        stale_thread.join(timeout=60)
        if stale_thread.is_alive():
            raise WorkloadFailure("stale recoverer remained blocked after gates opened")

    final_results: dict[str, Any] = {}
    result_errors: dict[str, str] = {}
    for plan in plans:
        try:
            final_results[plan.workflow_id] = DBOS.retrieve_workflow(plan.workflow_id).get_result()
        except Exception as exc:
            result_errors[plan.workflow_id] = f"retrieved:{type(exc).__name__}: {exc}"

    final_rows = workflow_rows(plans)
    ledgers = ledger_rows_for_plans(plans)
    step_one_counts = {plan.workflow_id: ledger_count(plan, "step_one") for plan in plans}
    step_two_counts = {plan.workflow_id: ledger_count(plan, "step_two") for plan in plans}
    active_rows_after_terminal = active_queue_rows(base, queue_name)
    write_rung6_artifacts(
        artifact_dir,
        case_id,
        plans,
        queue_name,
        rows_after_enqueue,
        rows_after_force,
        rows_after_clear,
        original_handle_ids,
        clear_result,
        stale_results,
        stale_thread_alive_before_gate,
        drain_turns,
        queued_after_clear,
        queued_after_redequeue,
        final_rows,
        ledgers,
        final_results,
        result_errors,
        active_rows_after_terminal,
    )

    stale_executor_effects = {
        workflow_id: [row for row in rows if row["step_name"] == "step_two" and row["executor_id"] == EXECUTOR_C]
        for workflow_id, rows in ledgers.items()
    }
    stale_executor_effects = {workflow_id: rows for workflow_id, rows in stale_executor_effects.items() if rows}
    drain_executor_effects = {
        workflow_id: [row for row in rows if row["step_name"] == "step_two" and row["executor_id"] == EXECUTOR_D]
        for workflow_id, rows in ledgers.items()
    }
    direct_recovery_effects = {
        workflow_id: [row for row in rows if row["step_name"] == "step_two" and row["executor_id"] in {EXECUTOR_B, EXECUTOR_C}]
        for workflow_id, rows in ledgers.items()
    }
    direct_recovery_effects = {workflow_id: rows for workflow_id, rows in direct_recovery_effects.items() if rows}

    invariant("rung6_no_stale_recoverer_body_effect", not stale_executor_effects, stale_executor_effects=stale_executor_effects, ledgers=ledgers, stale_results=stale_results)
    invariant("rung6_no_recovery_executor_queue_bypass", not direct_recovery_effects, direct_recovery_effects=direct_recovery_effects, ledgers=ledgers)
    invariant("rung6_queue_drain_executor_ran_recovered_step_once", all(len(drain_executor_effects.get(plan.workflow_id, [])) == 1 for plan in plans), drain_executor_effects=drain_executor_effects, ledgers=ledgers)
    invariant("rung6_queue_concurrency_one_serialized_drain", stale_thread_alive_before_gate or len(drain_turns) == len(plans), drain_turns=drain_turns, stale_thread_alive_before_gate=stale_thread_alive_before_gate)
    invariant("rung6_all_terminal_success", all(row.get("status") == WorkflowStatusString.SUCCESS.value for row in final_rows.values()), rows=final_rows)
    invariant("rung6_no_dead_executor_pending_rows", not any(row.get("status") == WorkflowStatusString.PENDING.value and row.get("executor_id") == EXECUTOR_A for row in final_rows.values()), rows=final_rows)
    invariant("rung6_step_one_ledger_once", all(count == 1 for count in step_one_counts.values()), counts=step_one_counts)
    invariant("rung6_step_two_ledger_once", all(count == 1 for count in step_two_counts.values()), counts=step_two_counts)
    invariant("rung6_handle_results_match_terminal_model", not result_errors and all(final_results.get(plan.workflow_id) == f"result-{plan.workflow_id}" for plan in plans), results=final_results, errors=result_errors)
    invariant("rung6_no_active_queue_rows_after_terminal", not active_rows_after_terminal, active_rows=active_rows_after_terminal)

    event("case_passed", case=case_id, workflow_ids=[plan.workflow_id for plan in plans])
    DBOS.destroy(destroy_registry=False, workflow_completion_timeout_sec=2)
    cleanup_databases(base)
    return 0


def parent_main(plan: CasePlan, artifact_dir: Path) -> int:
    event("case_start", frontier=FRONTIER_ID, rung=RUNG_ID, **asdict(plan))
    cleanup_databases(plan)
    configure_executor(EXECUTOR_A)
    DBOS.destroy(destroy_registry=False)
    dbos = DBOS(config=config(plan))
    DBOS.launch()
    ensure_harness_tables(plan)

    with SetWorkflowID(plan.workflow_id):
        original_handle = DBOS.start_workflow(workflow_for_plan(plan), json.dumps(asdict(plan), sort_keys=True))

    deadline = time.monotonic() + 120
    row_after_start: dict[str, Any] = {}
    while time.monotonic() < deadline:
        row_after_start = workflow_row(plan)
        if ledger_count(plan, "step_one") == 1 and row_after_start.get("status") == WorkflowStatusString.PENDING.value:
            break
        time.sleep(0.25)
    else:
        raise WorkloadFailure(f"workflow did not reach pending state; last_row={row_after_start}")

    event("pending_state_reached", case=plan.case_id, row=row_after_start)
    invariant("accepted_workflow_pending_on_executor_a", row_after_start.get("executor_id") == EXECUTOR_A, row=row_after_start)
    invariant("step_one_ledger_once_before_recovery", ledger_count(plan, "step_one") == 1)

    restart_thread: threading.Thread | None = None
    if plan.schedule == "restart-during-recovered-execute":
        reset_step_two_restart_coordination()
        os.environ["WIO_RECOVERY_EXTERNAL_STEP_TWO_RESTART"] = "1"
        restart_thread = threading.Thread(target=restart_when_step_two_requests, args=(plan,), daemon=True)
        restart_thread.start()

    handles = recover_with_optional_fault(plan, dbos)
    invariant("recovery_returned_handle", len(handles) == 1, handle_count=len(handles))
    open_gate(plan)

    result = None
    last_error = None
    for attempt in range(1, 4):
        try:
            result = handles[0].get_result()
            break
        except Exception as exc:  # transient DB restart during step result persistence.
            last_error = f"{type(exc).__name__}: {exc}"
            event("recovery_handle_attempt_failed", case=plan.case_id, attempt=attempt, error=last_error)
            wait_for_pg_ready()
            handles = DBOS._recover_pending_workflows([EXECUTOR_A])
            if not handles:
                break

    if result is None:
        try:
            result = original_handle.get_result()
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"

    if restart_thread is not None:
        restart_thread.join(timeout=5)
        if restart_thread.is_alive():
            last_error = "step_two external restart thread still alive after result wait"

    expected_result = f"result-{plan.workflow_id}"
    final_row = workflow_row(plan)
    step_one_count = ledger_count(plan, "step_one")
    step_two_count = ledger_count(plan, "step_two")
    step_one_outputs = operation_output_count(plan, "step_one")

    write_artifacts(artifact_dir, plan, final_row, result, step_one_count, step_two_count, step_one_outputs, last_error)

    invariant("workflow_eventually_terminal", final_row.get("status") in TERMINAL_STATUSES, row=final_row)
    invariant("workflow_succeeded", final_row.get("status") == WorkflowStatusString.SUCCESS.value, row=final_row)
    invariant("no_dead_executor_pending_row", not (final_row.get("status") == WorkflowStatusString.PENDING.value and final_row.get("executor_id") == EXECUTOR_A), row=final_row)
    invariant("completed_step_ledger_once", step_one_count == 1, step_one_count=step_one_count)
    invariant("completed_step_checkpoint_once", step_one_outputs == 1, step_one_outputs=step_one_outputs)
    invariant("recovered_step_completed_once", step_two_count == 1, step_two_count=step_two_count)
    invariant("handle_result_matches_model", result == expected_result, result=result, expected=expected_result, last_error=last_error)
    event("case_passed", case=plan.case_id, workflow_id=plan.workflow_id)
    DBOS.destroy(destroy_registry=False)
    cleanup_databases(plan)
    return 0


def write_artifacts(
    artifact_dir: Path,
    plan: CasePlan,
    final_row: dict[str, Any],
    result: Any,
    step_one_count: int,
    step_two_count: int,
    step_one_outputs: int,
    last_error: str | None,
) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    data = {
        "plan": asdict(plan),
        "final_row": final_row,
        "result": result,
        "step_one_count": step_one_count,
        "step_two_count": step_two_count,
        "step_one_outputs": step_one_outputs,
        "last_error": last_error,
    }
    (artifact_dir / f"{plan.case_id}.json").write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_matrix_artifacts(
    artifact_dir: Path,
    case_id: str,
    plans: list[CasePlan],
    final_rows: dict[str, dict[str, Any]],
    results: dict[str, Any],
    step_one_counts: dict[str, int],
    step_two_counts: dict[str, int],
    step_one_outputs: dict[str, int],
    last_errors: dict[str, str],
) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    data = {
        "case_id": case_id,
        "plans": [asdict(plan) for plan in plans],
        "final_rows": final_rows,
        "results": results,
        "step_one_counts": step_one_counts,
        "step_two_counts": step_two_counts,
        "step_one_outputs": step_one_outputs,
        "last_errors": last_errors,
    }
    (artifact_dir / f"{case_id}.json").write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_rung3_artifacts(
    artifact_dir: Path,
    case_id: str,
    plans: list[CasePlan],
    expected_statuses: dict[str, str],
    final_rows: dict[str, dict[str, Any]],
    timeline: dict[str, list[dict[str, Any]]],
    initial_results: dict[str, Any],
    resume_results: dict[str, Any],
    resume_errors: dict[str, str],
    duplicate_invocations: dict[str, list[dict[str, Any]]],
    step_one_counts: dict[str, int],
    step_two_counts: dict[str, int],
    step_one_outputs: dict[str, int],
    step_two_outputs: dict[str, int],
) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    data = {
        "case_id": case_id,
        "plans": [asdict(plan) for plan in plans],
        "expected_statuses": expected_statuses,
        "final_rows": final_rows,
        "timeline": timeline,
        "initial_results": initial_results,
        "resume_results": resume_results,
        "resume_errors": resume_errors,
        "duplicate_invocations": duplicate_invocations,
        "step_one_counts": step_one_counts,
        "step_two_counts": step_two_counts,
        "step_one_outputs": step_one_outputs,
        "step_two_outputs": step_two_outputs,
    }
    (artifact_dir / f"{case_id}.json").write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_rung6_artifacts(
    artifact_dir: Path,
    case_id: str,
    plans: list[CasePlan],
    queue_name: str,
    rows_after_enqueue: dict[str, dict[str, Any]],
    rows_after_force: list[dict[str, Any]],
    rows_after_clear: dict[str, dict[str, Any]],
    original_handle_ids: list[str],
    clear_result: dict[str, Any],
    stale_results: list[dict[str, Any]],
    stale_thread_alive_before_gate: bool,
    drain_turns: list[dict[str, Any]],
    queued_after_clear: dict[str, Any],
    queued_after_redequeue: dict[str, Any],
    final_rows: dict[str, dict[str, Any]],
    ledgers: dict[str, list[dict[str, Any]]],
    final_results: dict[str, Any],
    result_errors: dict[str, str],
    active_rows_after_terminal: list[dict[str, Any]],
) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    data = {
        "case_id": case_id,
        "frontier_id": FRONTIER_ID,
        "rung_id": "rung-006-concurrent-queued-recovery-ownership",
        "protected_product_promise": (
            "Queued recovery may clear assignment or observe that another recoverer already cleared it, "
            "but stale recoverers must not execute queue-owned work outside queue ownership."
        ),
        "replay_command": (
            ".workers/run-with-postgres.sh .workers/python-runtime.sh "
            ".workers/workloads/recovery-db-faults/recovery_db_faults_workload.py "
            f"--rung rung-006-concurrent-queued-recovery-ownership --case {case_id}"
        ),
        "seed_policy": {plan.workflow_id: plan.seed for plan in plans},
        "invariant_oracle": [
            "no stale recoverer step_two ledger rows",
            "no recovery executor queue bypass",
            "drain executor runs recovered step once per workflow",
            "queue concurrency one serialized drain",
            "terminal success and matching public handle results",
            "no active modeled queue rows after terminal completion",
        ],
        "queue_configuration": {
            "queue_name": queue_name,
            "concurrency": 1,
            "worker_concurrency": 1,
            "polling_interval_sec": 0.05,
        },
        "executors": {
            "dead_executor": EXECUTOR_A,
            "recoverer_b": EXECUTOR_B,
            "stale_recoverer_c": EXECUTOR_C,
            "drain_executor": EXECUTOR_D,
        },
        "plans": [asdict(plan) for plan in plans],
        "captured_pending_snapshot": [
            {"workflow_id": plan.workflow_id, "queue_name": queue_name}
            for plan in plans
        ],
        "rows_after_enqueue": rows_after_enqueue,
        "rows_after_force": rows_after_force,
        "rows_after_clear": rows_after_clear,
        "original_handle_ids": original_handle_ids,
        "queued_after_clear": queued_after_clear,
        "queued_after_redequeue": queued_after_redequeue,
        "recoverer_b_result": clear_result,
        "stale_recoverer_results": stale_results,
        "stale_thread_alive_before_gate": stale_thread_alive_before_gate,
        "drain_turns": drain_turns,
        "final_rows": final_rows,
        "ledgers": ledgers,
        "final_results": final_results,
        "result_errors": result_errors,
        "active_rows_after_terminal": active_rows_after_terminal,
        "target_ref": "0c41e6dfb46440184d19a52cdecc64a8c5f40d60",
    }
    (artifact_dir / f"{case_id}.json").write_text(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DBOS recovery DB faults workload")
    parser.add_argument("--rung", default="rung-001")
    parser.add_argument(
        "--case",
        choices=[f"case-{index:03d}" for index in range(1, 25)],
    )
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--plan-json")
    parser.add_argument("--replay-case")
    parser.add_argument("--artifact-dir", default="/tmp/wio-artifacts/recovery-db-faults-rung-001")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.rung not in {
        "rung-001",
        RUNG_ID,
        "rung-002",
        "rung-002-recovery-window-matrix",
        "rung-003",
        "rung-003-replay-dlq-liveness",
        "rung-004",
        "rung-004-bounded-seed-sweep",
        "rung-005",
        "rung-005-finding-minimization",
        "rung-006",
        "rung-006-concurrent-queued-recovery-ownership",
    }:
        raise SetupBlock(f"unsupported rung {args.rung}")
    try:
        if args.rung in {"rung-006", "rung-006-concurrent-queued-recovery-ownership"}:
            if args.all_cases:
                if not args.sequential:
                    raise SetupBlock("--all-cases requires --sequential for rung-006")
                for case_id in ["case-001", "case-002", "case-003"]:
                    parent_main_rung6(make_rung6_plans(case_id), Path(args.artifact_dir) / case_id)
                return 0
            if not args.case:
                raise SetupBlock("--case or --all-cases is required for rung-006")
            return parent_main_rung6(make_rung6_plans(args.case), Path(args.artifact_dir))
        if args.rung in {"rung-005", "rung-005-finding-minimization"}:
            if args.replay_case:
                plans = load_replay_plans(args.replay_case)
            elif args.plan_json:
                plans = [CasePlan(**item) for item in json.loads(args.plan_json)]
            elif args.case:
                plans = make_rung5_plans(args.case)
            else:
                raise SetupBlock("--case, --plan-json, or --replay-case is required for rung-005")
            return parent_main_rung2(plans, Path(args.artifact_dir), rung_label="rung-005-finding-minimization")
        if args.rung in {"rung-004", "rung-004-bounded-seed-sweep"}:
            if args.plan_json:
                plans = [CasePlan(**item) for item in json.loads(args.plan_json)]
            elif args.case:
                plans = make_rung4_plans(args.case)
            else:
                raise SetupBlock("--case or --plan-json is required for rung-004")
            return parent_main_rung2(plans, Path(args.artifact_dir), rung_label="rung-004-bounded-seed-sweep")
        if args.rung in {"rung-003", "rung-003-replay-dlq-liveness"}:
            if args.plan_json:
                plans = [CasePlan(**item) for item in json.loads(args.plan_json)]
            elif args.case:
                plans = make_rung3_plans(args.case)
            else:
                raise SetupBlock("--case or --plan-json is required for rung-003")
            return parent_main_rung3(plans, Path(args.artifact_dir))
        if args.rung in {"rung-002", "rung-002-recovery-window-matrix"}:
            if args.plan_json:
                plans = [CasePlan(**item) for item in json.loads(args.plan_json)]
            elif args.case:
                plans = make_rung2_plans(args.case)
            else:
                raise SetupBlock("--case or --plan-json is required for rung-002")
            return parent_main_rung2(plans, Path(args.artifact_dir))
        if not args.case and not args.plan_json:
            raise SetupBlock("--case or --plan-json is required for rung-001")
        plan = CasePlan(**json.loads(args.plan_json)) if args.plan_json else make_plan(args.case)
        if args.child:
            return child_main(plan)
        return parent_main(plan, Path(args.artifact_dir))
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 42
    except WorkloadFailure as exc:
        print(f"WORKLOAD-FAIL {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
