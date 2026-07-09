#!/usr/bin/env python3
"""WIO workload for committed DBOS system-database retry idempotence.

Frontier: system-db-retry-idempotence
Rung:
  - rung-001-committed-sysdb-retry-reentry
Protected product promise:
  DBOS system database retry loops preserve exactly-once durable semantics when
  a retry re-enters the same logical operation after durable state may already
  have committed.
Replay:
  python .workers/workloads/system-db-retry-idempotence/system_db_retry_idempotence_workload.py \
    --rung rung-001-committed-sysdb-retry-reentry --case case-001 --seed 7400
Seed policy:
  Exact case seeds are 7400, 7401, 7402, and 7403. Each case writes its plan,
  replay method, logical ledger, public observations, and raw system rows under
  the artifact directory.
Invariant oracle:
  An independent logical-operation ledger is compared with DBOS public handles,
  workflow statuses, operation_outputs rows, and notification consumed flags.
"""

from __future__ import annotations

import argparse
import inspect
import json
import os
import random
import sys
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
VENDOR_ROOT = REPO_ROOT / ".workers" / "vendor" / "dbos-transact-py"
LOCAL_TARGET = REPO_ROOT / "target"
VENV_ROOT = REPO_ROOT / ".workers" / "vendor" / "dbos-venv"

site_packages = sorted(VENV_ROOT.glob("lib/python*/site-packages"))
if site_packages:
    sys.path.insert(0, str(site_packages[-1]))

for target in [
    VENDOR_ROOT,
    LOCAL_TARGET,
    Path("/Users/viswa/code/workers/dbos-transact-py"),
]:
    if target.exists():
        sys.path.insert(0, str(target))
        break

try:
    import sqlalchemy as sa
    from sqlalchemy.engine import make_url

    from dbos import DBOS, DBOSConfig, SetWorkflowID
    from dbos._context import DBOSContext, _set_local_dbos_context, get_local_dbos_context
    from dbos._error import DBOSException, DBOSWorkflowConflictIDError
    from dbos._schemas.system_database import SystemSchema
    from dbos._serialization import deserialize_value
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "system-db-retry-idempotence"
RUNG_ID = "rung-001-committed-sysdb-retry-reentry"
APP_ID = "wio-sysdb-retry"
APP_VERSION = "wio-system-db-retry-rung-001"

CASE_MATRIX: dict[str, tuple[int, str]] = {
    "case-001": (7400, "receive-two-messages-committed-retry"),
    "case-002": (7401, "receive-timeout-then-late-message"),
    "case-003": (7402, "child-edge-committed-retry"),
    "case-004": (7403, "implicit-get-result-function-id-retry"),
}


class WorkloadFailure(Exception):
    pass


class SetupBlock(Exception):
    pass


@dataclass(frozen=True)
class CasePlan:
    rung_id: str
    case_id: str
    seed: int
    scenario: str
    database_prefix: str
    workflow_id: str
    function_id: int
    topic: str
    payloads: list[str]
    child_id: str
    alt_child_id: str


def now_ms() -> int:
    return int(time.time() * 1000)


def event(name: str, **fields: Any) -> None:
    parts = [f"WIO-EVENT {name}"]
    parts.extend(
        f"{key}={json.dumps(value, sort_keys=True, default=str)}"
        for key, value in fields.items()
    )
    print(" ".join(parts), flush=True)


def invariant(name: str, ok: bool, **fields: Any) -> None:
    status = "PASS" if ok else "FAIL"
    summary = json.dumps(fields, sort_keys=True, default=str) if fields else "ok"
    print(f"INVARIANT {name} {status} {summary}", flush=True)
    if not ok:
        raise WorkloadFailure(f"{name} failed: {summary}")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


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
    admin = base.set(database=base.database or "postgres").render_as_string(
        hide_password=False
    )
    masked = base.set(password="***" if base.password else None).render_as_string(
        hide_password=False
    )
    event("postgres_preflight", admin_url=masked, app_db=app_db, sys_db=sys_db)
    try:
        engine = sa.create_engine(admin, connect_args={"connect_timeout": 5})
        with engine.connect() as raw_connection:
            connection = raw_connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(sa.text("SET statement_timeout = '8000ms'"))
            for database in (app_db, sys_db):
                connection.execute(
                    sa.text(f"DROP DATABASE IF EXISTS {quote_ident(database)} WITH (FORCE)")
                )
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
        base.set(drivername="postgresql", database=app_db).render_as_string(
            hide_password=False
        ),
        base.set(drivername="postgresql+psycopg", database=sys_db).render_as_string(
            hide_password=False
        ),
        masked,
    )


def drop_databases(prefix: str) -> None:
    if os.environ.get("WIO_SYSTEM_DB_RETRY_KEEP_DATABASES") == "1":
        return
    base = admin_url()
    admin = base.set(database=base.database or "postgres").render_as_string(
        hide_password=False
    )
    engine = sa.create_engine(admin, connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as raw_connection:
            connection = raw_connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(sa.text("SET statement_timeout = '5000ms'"))
            connection.execute(sa.text("SET lock_timeout = '3000ms'"))
            for suffix in ("app", "sys"):
                connection.execute(
                    sa.text(
                        f"DROP DATABASE IF EXISTS {quote_ident(prefix + '_' + suffix)} WITH (FORCE)"
                    )
                )
    except Exception as exc:
        event(
            "database_cleanup_best_effort_failed",
            prefix=prefix,
            error_type=type(exc).__name__,
            error=str(exc),
        )
    finally:
        engine.dispose()


def make_config(plan: CasePlan, app_url: str, sys_url: str) -> DBOSConfig:
    return {
        "name": APP_ID,
        "application_database_url": app_url,
        "system_database_url": sys_url,
        "application_version": f"{APP_VERSION}-{plan.case_id}",
        "executor_id": f"wio-sysdb-retry-{plan.case_id}",
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
    }


def make_plan(rung_id: str, case_id: str, seed_override: int | None = None) -> CasePlan:
    if rung_id not in {RUNG_ID, "rung-001"}:
        raise SetupBlock(f"unsupported rung: {rung_id}")
    if case_id not in CASE_MATRIX:
        raise SetupBlock(f"unsupported case: {case_id}")
    expected_seed, scenario = CASE_MATRIX[case_id]
    seed = seed_override if seed_override is not None else expected_seed
    if seed != expected_seed:
        raise SetupBlock(f"{case_id} requires seed {expected_seed}, got {seed}")
    rng = random.Random(seed)
    digest = f"{seed}_{case_id.replace('-', '_')}"
    return CasePlan(
        rung_id=RUNG_ID,
        case_id=case_id,
        seed=seed,
        scenario=scenario,
        database_prefix=f"wio_sysdb_retry_{digest}",
        workflow_id=f"{FRONTIER_ID}-{case_id}-{seed}-{rng.randint(1000, 9999)}",
        function_id=1 + rng.randint(0, 3),
        topic=f"topic-{case_id}-{seed}",
        payloads=[f"{case_id}-message-a-{seed}", f"{case_id}-message-b-{seed}"],
        child_id=f"{FRONTIER_ID}-{case_id}-child-{seed}",
        alt_child_id=f"{FRONTIER_ID}-{case_id}-alt-child-{seed}",
    )


@DBOS.workflow()
def _noop_status_workflow() -> str:
    return "status-row-created"


@DBOS.workflow()
def _retry_child_workflow(payload: str) -> dict[str, str]:
    return {"child_payload": payload}


@DBOS.workflow()
def _retry_parent_workflow(child_id: str, payload: str) -> dict[str, Any]:
    with SetWorkflowID(child_id):
        child_result = _retry_child_workflow(payload)
    return {"parent_payload": payload, "child_result": child_result}


class RecordGetResultRetryEngine:
    """Proxy that fails exactly once inside SystemDatabase.record_get_result."""

    def __init__(self, real: Any) -> None:
        self._real = real
        self.fired = 0
        self.thread_id: int | None = None

    def begin(self) -> Any:
        if self.fired == 0 and self._inside_record_get_result():
            self.fired += 1
            self.thread_id = threading.get_ident()
            raise Exception("database is locked")
        return self._real.begin()

    def _inside_record_get_result(self) -> bool:
        return any(frame.function == "record_get_result" for frame in inspect.stack())

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


def launch_dbos(config: DBOSConfig) -> Any:
    DBOS.destroy(destroy_registry=False)
    dbos = DBOS(config=config)
    DBOS.launch()
    return dbos


def make_status_row(workflow_id: str) -> None:
    with SetWorkflowID(workflow_id):
        result = _noop_status_workflow()
    invariant("status-row-workflow-completed", result == "status-row-created", workflow_id=workflow_id)


def deserialize_row_value(dbos: Any, value: str | None, serialization: str | None) -> Any:
    return deserialize_value(value, serialization, dbos._serializer)


def operation_rows(dbos: Any, workflow_id: str) -> list[dict[str, Any]]:
    with dbos._sys_db.engine.connect() as conn:
        rows = conn.execute(
            sa.select(
                SystemSchema.operation_outputs.c.workflow_uuid,
                SystemSchema.operation_outputs.c.function_id,
                SystemSchema.operation_outputs.c.function_name,
                SystemSchema.operation_outputs.c.output,
                SystemSchema.operation_outputs.c.error,
                SystemSchema.operation_outputs.c.child_workflow_id,
                SystemSchema.operation_outputs.c.serialization,
                SystemSchema.operation_outputs.c.started_at_epoch_ms,
                SystemSchema.operation_outputs.c.completed_at_epoch_ms,
            )
            .where(SystemSchema.operation_outputs.c.workflow_uuid == workflow_id)
            .order_by(SystemSchema.operation_outputs.c.function_id)
        ).mappings()
        return [dict(row) for row in rows]


def notification_rows(dbos: Any, workflow_id: str) -> list[dict[str, Any]]:
    with dbos._sys_db.engine.connect() as conn:
        rows = conn.execute(
            sa.select(
                SystemSchema.notifications.c.destination_uuid,
                SystemSchema.notifications.c.topic,
                SystemSchema.notifications.c.message,
                SystemSchema.notifications.c.serialization,
                SystemSchema.notifications.c.consumed,
                SystemSchema.notifications.c.message_uuid,
                SystemSchema.notifications.c.created_at_epoch_ms,
            )
            .where(SystemSchema.notifications.c.destination_uuid == workflow_id)
            .order_by(SystemSchema.notifications.c.created_at_epoch_ms)
        ).mappings()
        return [dict(row) for row in rows]


def workflow_status(dbos: Any, workflow_id: str) -> dict[str, Any] | None:
    status = dbos.get_workflow_status(workflow_id)
    if status is None:
        return None
    return {
        "workflow_id": status.workflow_id,
        "status": status.status,
        "name": status.name,
        "executor_id": getattr(status, "executor_id", None),
        "created_at": getattr(status, "created_at", None),
        "updated_at": getattr(status, "updated_at", None),
        "completed_at": getattr(status, "completed_at", None),
    }


def decoded_notifications(dbos: Any, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    decoded: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["decoded_message"] = deserialize_row_value(
            dbos, row["message"], row["serialization"]
        )
        decoded.append(item)
    return decoded


def recv_operation_rows(rows: list[dict[str, Any]], function_id: int) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if row["function_id"] == function_id and row["function_name"] == "DBOS.recv"
    ]


def case_receive_two_messages(dbos: Any, plan: CasePlan) -> dict[str, Any]:
    make_status_row(plan.workflow_id)
    DBOS.send(plan.workflow_id, plan.payloads[0], plan.topic, idempotency_key=f"{plan.case_id}-a")
    DBOS.send(plan.workflow_id, plan.payloads[1], plan.topic, idempotency_key=f"{plan.case_id}-b")
    before_rows = decoded_notifications(dbos, notification_rows(dbos, plan.workflow_id))
    invariant("receive-precondition-two-unconsumed", len(before_rows) == 2 and not any(row["consumed"] for row in before_rows), rows=before_rows)

    first = dbos._sys_db.recv_consume(plan.workflow_id, plan.function_id, plan.topic, now_ms())
    rows_after_first = operation_rows(dbos, plan.workflow_id)
    durable_recv_rows = recv_operation_rows(rows_after_first, plan.function_id)
    invariant("receive-durable-row-before-replay", len(durable_recv_rows) == 1, rows=durable_recv_rows)

    replay = dbos._sys_db.recv_consume(plan.workflow_id, plan.function_id, plan.topic, now_ms())
    after_notifications = decoded_notifications(dbos, notification_rows(dbos, plan.workflow_id))
    consumed = [row for row in after_notifications if row["consumed"]]
    unconsumed = [row for row in after_notifications if not row["consumed"]]
    rows_after_replay = operation_rows(dbos, plan.workflow_id)
    recv_rows = recv_operation_rows(rows_after_replay, plan.function_id)

    invariant("receive-replay-returned-recorded-message", replay == first and first in plan.payloads, first=first, replay=replay)
    invariant("receive-replay-did-not-consume-second-message", len(consumed) == 1 and len(unconsumed) == 1 and unconsumed[0]["decoded_message"] != first, notifications=after_notifications)
    invariant("receive-replay-single-operation-row", len(recv_rows) == 1, rows=recv_rows)
    return {
        "replay_method": "explicit_committed_state_replay_surrogate",
        "ledger": {
            "operation": "DBOS.recv",
            "workflow_id": plan.workflow_id,
            "function_id": plan.function_id,
            "recorded_output": first,
            "replay_output": replay,
            "expected_consumed_count": 1,
        },
        "notifications_before": before_rows,
        "notifications_after": after_notifications,
        "operation_rows": rows_after_replay,
        "status": workflow_status(dbos, plan.workflow_id),
    }


def case_receive_timeout_then_late_message(dbos: Any, plan: CasePlan) -> dict[str, Any]:
    make_status_row(plan.workflow_id)
    first = dbos._sys_db.recv_consume(plan.workflow_id, plan.function_id, plan.topic, now_ms())
    rows_after_first = operation_rows(dbos, plan.workflow_id)
    durable_recv_rows = recv_operation_rows(rows_after_first, plan.function_id)
    invariant("timeout-durable-none-row-before-replay", first is None and len(durable_recv_rows) == 1, first=first, rows=durable_recv_rows)

    late_payload = f"{plan.case_id}-late-{plan.seed}"
    DBOS.send(plan.workflow_id, late_payload, plan.topic, idempotency_key=f"{plan.case_id}-late")
    replay = dbos._sys_db.recv_consume(plan.workflow_id, plan.function_id, plan.topic, now_ms())
    after_notifications = decoded_notifications(dbos, notification_rows(dbos, plan.workflow_id))
    rows_after_replay = operation_rows(dbos, plan.workflow_id)
    recv_rows = recv_operation_rows(rows_after_replay, plan.function_id)

    invariant("timeout-replay-returned-recorded-none", replay is None, replay=replay)
    invariant("timeout-late-message-remained-unconsumed", len(after_notifications) == 1 and after_notifications[0]["decoded_message"] == late_payload and not after_notifications[0]["consumed"], notifications=after_notifications)
    invariant("timeout-replay-single-operation-row", len(recv_rows) == 1, rows=recv_rows)
    return {
        "replay_method": "explicit_committed_state_replay_surrogate",
        "ledger": {
            "operation": "DBOS.recv",
            "workflow_id": plan.workflow_id,
            "function_id": plan.function_id,
            "recorded_output": None,
            "late_message": late_payload,
            "expected_consumed_count": 0,
        },
        "notifications_after": after_notifications,
        "operation_rows": rows_after_replay,
        "status": workflow_status(dbos, plan.workflow_id),
    }


def case_child_edge_replay(dbos: Any, plan: CasePlan) -> dict[str, Any]:
    make_status_row(plan.workflow_id)
    function_name = "wio_child_edge_case_003"
    dbos._sys_db.record_child_workflow(
        plan.workflow_id, plan.child_id, plan.function_id, function_name
    )
    rows_after_first = operation_rows(dbos, plan.workflow_id)
    target_rows = [row for row in rows_after_first if row["function_id"] == plan.function_id]
    invariant("child-edge-durable-row-before-replay", len(target_rows) == 1 and target_rows[0]["child_workflow_id"] == plan.child_id, rows=target_rows)

    dbos._sys_db.record_child_workflow(
        plan.workflow_id, plan.child_id, plan.function_id, function_name
    )
    same_child_rows = operation_rows(dbos, plan.workflow_id)
    matching = [row for row in same_child_rows if row["function_id"] == plan.function_id]
    invariant("child-edge-same-child-idempotent", len(matching) == 1 and matching[0]["child_workflow_id"] == plan.child_id, rows=matching)

    conflict_type = None
    try:
        dbos._sys_db.record_child_workflow(
            plan.workflow_id, plan.alt_child_id, plan.function_id, function_name
        )
    except DBOSWorkflowConflictIDError as exc:
        conflict_type = type(exc).__name__
    invariant("child-edge-different-child-conflicts", conflict_type == "DBOSWorkflowConflictIDError", conflict_type=conflict_type)

    empty_error_type = None
    try:
        dbos._sys_db.record_child_workflow(plan.workflow_id, "", plan.function_id + 1, function_name)
    except DBOSException as exc:
        empty_error_type = type(exc).__name__
    rows_after_empty = operation_rows(dbos, plan.workflow_id)
    empty_rows = [row for row in rows_after_empty if row["function_id"] == plan.function_id + 1]
    invariant("child-edge-empty-child-rejected-before-write", empty_error_type == "DBOSException" and len(empty_rows) == 0, empty_error_type=empty_error_type, empty_rows=empty_rows)
    return {
        "replay_method": "explicit_committed_state_replay_surrogate",
        "ledger": {
            "operation": "record_child_workflow",
            "workflow_id": plan.workflow_id,
            "function_id": plan.function_id,
            "child_id": plan.child_id,
            "different_child_conflict": conflict_type,
            "empty_child_error": empty_error_type,
        },
        "operation_rows": rows_after_empty,
        "status": workflow_status(dbos, plan.workflow_id),
    }


def case_implicit_get_result_retry(dbos: Any, plan: CasePlan, config: DBOSConfig) -> dict[str, Any]:
    real_engine = dbos._sys_db.engine
    proxy = RecordGetResultRetryEngine(real_engine)
    payload = f"{plan.case_id}-child-payload-{plan.seed}"
    try:
        dbos._sys_db.engine = proxy  # type: ignore[assignment]
        with SetWorkflowID(plan.workflow_id):
            result = _retry_parent_workflow(plan.child_id, payload)
    finally:
        dbos._sys_db.engine = real_engine

    rows = operation_rows(dbos, plan.workflow_id)
    child_rows = [row for row in rows if row["child_workflow_id"] == plan.child_id and row["function_name"] != "DBOS.getResult"]
    get_result_rows = [row for row in rows if row["function_name"] == "DBOS.getResult"]
    parent_status = workflow_status(dbos, plan.workflow_id)
    child_status = workflow_status(dbos, plan.child_id)

    invariant("get-result-retry-fired-once", proxy.fired == 1, fired=proxy.fired, thread_id=proxy.thread_id)
    invariant("get-result-public-result-agrees", result["child_result"]["child_payload"] == payload, result=result, payload=payload)
    invariant("get-result-parent-child-success", parent_status is not None and parent_status["status"] == "SUCCESS" and child_status is not None and child_status["status"] == "SUCCESS", parent_status=parent_status, child_status=child_status)
    invariant("get-result-single-child-edge", len(child_rows) == 1, child_rows=child_rows)
    invariant("get-result-single-row", len(get_result_rows) == 1 and get_result_rows[0]["child_workflow_id"] == plan.child_id, get_result_rows=get_result_rows)
    invariant("get-result-function-id-advanced-once", get_result_rows[0]["function_id"] == child_rows[0]["function_id"] + 1, rows=rows)

    DBOS.destroy(destroy_registry=False)
    relaunched = launch_dbos(config)
    try:
        parent_retrieved = DBOS.retrieve_workflow(plan.workflow_id).get_result(polling_interval_sec=0.1)
        child_retrieved = DBOS.retrieve_workflow(plan.child_id).get_result(polling_interval_sec=0.1)
    finally:
        dbos = relaunched
    invariant("get-result-relaunch-retrieval-agrees", parent_retrieved == result and child_retrieved == {"child_payload": payload}, parent_retrieved=parent_retrieved, child_retrieved=child_retrieved)
    rows_after_relaunch = operation_rows(dbos, plan.workflow_id)
    return {
        "replay_method": "stack_scoped_record_get_result_retry_injection",
        "retry_injection": {
            "fired": proxy.fired,
            "thread_id": proxy.thread_id,
            "exception": "database is locked",
        },
        "ledger": {
            "operation": "DBOS.getResult",
            "workflow_id": plan.workflow_id,
            "child_workflow_id": plan.child_id,
            "expected_get_result_rows": 1,
            "expected_function_id_delta_after_child_edge": 1,
        },
        "parent_result": result,
        "parent_retrieved_after_relaunch": parent_retrieved,
        "child_retrieved_after_relaunch": child_retrieved,
        "operation_rows_before_relaunch": rows,
        "operation_rows_after_relaunch": rows_after_relaunch,
        "parent_status": workflow_status(dbos, plan.workflow_id),
        "child_status": workflow_status(dbos, plan.child_id),
    }


def run_case(plan: CasePlan, artifact_dir: Path) -> int:
    case_artifacts = artifact_dir / plan.case_id
    case_artifacts.mkdir(parents=True, exist_ok=True)
    write_json(case_artifacts / "plan.json", asdict(plan))
    app_url, sys_url, admin_masked = prepare_databases(plan.database_prefix, case_artifacts)
    config = make_config(plan, app_url, sys_url)
    dbos = launch_dbos(config)
    event(
        "case_start",
        frontier=FRONTIER_ID,
        rung=plan.rung_id,
        admin_url=admin_masked,
        **asdict(plan),
    )
    try:
        if plan.scenario == "receive-two-messages-committed-retry":
            result = case_receive_two_messages(dbos, plan)
        elif plan.scenario == "receive-timeout-then-late-message":
            result = case_receive_timeout_then_late_message(dbos, plan)
        elif plan.scenario == "child-edge-committed-retry":
            result = case_child_edge_replay(dbos, plan)
        elif plan.scenario == "implicit-get-result-function-id-retry":
            result = case_implicit_get_result_retry(dbos, plan, config)
        else:
            raise SetupBlock(f"unsupported scenario {plan.scenario}")
        write_json(case_artifacts / "result.json", result)
        event("case_passed", case=plan.case_id, result_summary=result["ledger"])
        return 0
    finally:
        DBOS.destroy(destroy_registry=False)
        if os.environ.get("WIO_CLEANUP_AFTER", "1") == "1":
            drop_databases(plan.database_prefix)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DBOS system DB retry idempotence workload")
    parser.add_argument("--rung", default=RUNG_ID)
    parser.add_argument("--case", choices=sorted(CASE_MATRIX))
    parser.add_argument("--seed", type=int)
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument(
        "--artifact-dir",
        default="/tmp/wio-artifacts/system-db-retry-idempotence",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.all_cases:
        cases = sorted(CASE_MATRIX)
    elif args.case:
        cases = [args.case]
    else:
        raise SetupBlock("--case or --all-cases is required")
    if args.all_cases and not args.sequential:
        raise SetupBlock("--all-cases requires --sequential to keep DBOS global state isolated")
    try:
        for case_id in cases:
            seed = args.seed if len(cases) == 1 else None
            run_case(make_plan(args.rung, case_id, seed), Path(args.artifact_dir))
        return 0
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 42
    except WorkloadFailure as exc:
        print(f"WORKLOAD-FAIL {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
