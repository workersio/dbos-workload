#!/usr/bin/env python3
"""WIO workload for DBOS custom decorator composition fidelity.

Frontier: decorator-composition-fidelity
Rung:
  - rung-001-custom-decorator-entrypoint-matrix
Protected product promise:
  DBOS-decorated functions remain discoverable, durable, replayable, and
  operator-visible when composed with ordinary decorators that preserve
  __wrapped__.
Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/decorator-composition-fidelity/decorator_composition_fidelity_workload.py \
    --rung rung-001-custom-decorator-entrypoint-matrix --case case-001 --seed 7060
Seed policy:
  Exact seeds are 7060, 7061, 7062, and 7063.
Invariant oracle:
  Independent hook ledger, DBOS public results/errors, workflow_status rows,
  operation_outputs rows, class metadata, and replay/recovery counts agree with
  the modeled decorator composition.
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import hashlib
import inspect
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, TypeVar, cast

REPO_ROOT = Path(__file__).resolve().parents[3]
VENV_ROOT = REPO_ROOT / ".workers" / "vendor" / "dbos-venv"
site_packages = sorted(VENV_ROOT.glob("lib/python*/site-packages"))
if site_packages:
    sys.path.insert(0, str(site_packages[-1]))

for target in [
    REPO_ROOT / ".workers" / "vendor" / "dbos-transact-py",
    REPO_ROOT / "target",
    Path("/Users/viswa/code/workers/dbos-transact-py"),
]:
    if target.exists():
        sys.path.insert(0, str(target))
        break

try:
    import sqlalchemy as sa
    from sqlalchemy.engine import make_url

    from dbos import (
        DBOS,
        DBOSClient,
        DBOSConfig,
        DBOSConfiguredInstance,
        Queue,
        SetWorkflowID,
    )
    from dbos._schemas.system_database import SystemSchema
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "decorator-composition-fidelity"
RUNG_ID = "rung-001-custom-decorator-entrypoint-matrix"
APP_ID = "wio-decorator-composition"
APP_VERSION = "wio-decorator-composition-rung-001"
HOOK_TABLE = "decorator_hook_ledger"

CASE_MATRIX: dict[str, tuple[int, str]] = {
    "case-001": (7060, "dbos-outer-async-entrypoints"),
    "case-002": (7061, "custom-outer-function-lookup"),
    "case-003": (7062, "sync-failure-replay-boundary"),
    "case-004": (7063, "class-method-metadata"),
}

CASE001_QUEUE = Queue("e024_case001_queue")
CASE002_QUEUE = Queue("e024_case002_queue")
CASE004_QUEUE = Queue("e024_case004_queue")

F = TypeVar("F", bound=Callable[..., Any])
_hook_events: list[dict[str, Any]] = []
_app_side_effect_counts: dict[str, int] = {}


class WorkloadFailure(Exception):
    pass


class SetupBlock(Exception):
    pass


class ModeledApplicationError(Exception):
    pass


@dataclass(frozen=True)
class CasePlan:
    rung_id: str
    case_id: str
    seed: int
    scenario: str
    database_prefix: str
    workflow_id: str


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
    print(f"INVARIANT {name} {name} {status} {summary}", flush=True)
    if not ok:
        raise WorkloadFailure(f"{name} failed: {summary}")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def hook_event(label: str, phase: str) -> None:
    _hook_events.append(
        {
            "label": label,
            "phase": phase,
            "workflow_id": DBOS.workflow_id,
            "step_id": DBOS.step_id,
            "at_ms": now_ms(),
        }
    )


def app_hook(label: str) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                hook_event(label, "before")
                try:
                    result = await func(*args, **kwargs)
                except Exception:
                    hook_event(label, "error")
                    raise
                hook_event(label, "after")
                return result

            return cast(F, async_wrapper)

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            hook_event(label, "before")
            try:
                result = func(*args, **kwargs)
            except Exception:
                hook_event(label, "error")
                raise
            hook_event(label, "after")
            return result

        return cast(F, sync_wrapper)

    return decorator


def hook_counts() -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for row in _hook_events:
        label_counts = counts.setdefault(row["label"], {"before": 0, "after": 0, "error": 0})
        label_counts[row["phase"]] = label_counts.get(row["phase"], 0) + 1
    return counts


@DBOS.step(name="e024_case001_async_step")
async def e024_case001_async_step(payload: str) -> str:
    return payload + ":step"


@DBOS.workflow(name="e024_case001_async_workflow")
@app_hook("case001_workflow")
async def e024_case001_async_workflow(payload: str) -> str:
    return await e024_case001_async_step(payload)


@app_hook("case002_step")
@DBOS.step(name="e024_case002_sync_step")
def e024_case002_sync_step(payload: str) -> str:
    return payload + ":step"


@app_hook("case002_workflow")
@DBOS.workflow(name="e024_case002_sync_workflow")
def e024_case002_sync_workflow(payload: str) -> str:
    return e024_case002_sync_step(payload)


@DBOS.step(name="e024_case003_step")
@app_hook("case003_step")
def e024_case003_step(intent_id: str) -> str:
    return "step:" + intent_id


@DBOS.transaction(name="e024_case003_transaction")
@app_hook("case003_transaction")
def e024_case003_transaction(intent_id: str) -> str:
    _app_side_effect_counts[intent_id] = _app_side_effect_counts.get(intent_id, 0) + 1
    DBOS.sql_session.execute(
        sa.text(
            f"""
            INSERT INTO {HOOK_TABLE} (intent_id, workflow_id, payload, created_at_ms)
            VALUES (:intent_id, :workflow_id, :payload, :created_at_ms)
            """
        ),
        {
            "intent_id": intent_id,
            "workflow_id": DBOS.workflow_id,
            "payload": "transaction:" + intent_id,
            "created_at_ms": now_ms(),
        },
    )
    return "transaction:" + intent_id


@DBOS.workflow(name="e024_case003_failure_workflow")
@app_hook("case003_workflow")
def e024_case003_failure_workflow(intent_id: str) -> str:
    e024_case003_step(intent_id)
    e024_case003_transaction(intent_id)
    raise ModeledApplicationError("modeled decorator failure: " + intent_id)


@DBOS.dbos_class("E024DecoratedClass")
class E024DecoratedClass(DBOSConfiguredInstance):
    def __init__(self) -> None:
        super().__init__("case004-instance")

    @app_hook("case004_instance_workflow")
    @DBOS.workflow(name="e024_case004_instance_workflow")
    def instance_workflow(self, payload: str) -> str:
        return "instance:" + self.config_name + ":" + payload

    @staticmethod
    @app_hook("case004_static_workflow")
    @DBOS.workflow(name="e024_case004_static_workflow")
    def static_workflow(payload: str) -> str:
        return "static:" + payload

    @classmethod
    @app_hook("case004_class_workflow")
    @DBOS.workflow(name="e024_case004_class_workflow")
    def class_workflow(cls, payload: str) -> str:
        return "class:" + cls.__name__ + ":" + payload


CASE004_INSTANCE = E024DecoratedClass()


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
    if os.environ.get("WIO_DECORATOR_KEEP_DATABASES") == "1":
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
            for suffix in ("app", "sys"):
                connection.execute(
                    sa.text(
                        f"DROP DATABASE IF EXISTS {quote_ident(prefix + '_' + suffix)} WITH (FORCE)"
                    )
                )
    except Exception as exc:
        event("database_cleanup_best_effort_failed", prefix=prefix, error=str(exc))
    finally:
        engine.dispose()


def make_config(plan: CasePlan, app_url: str, sys_url: str) -> DBOSConfig:
    return {
        "name": APP_ID,
        "application_database_url": app_url,
        "system_database_url": sys_url,
        "application_version": f"{APP_VERSION}-{plan.case_id}",
        "executor_id": f"wio-decorator-{plan.case_id}",
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        "run_admin_server": False,
        "runtimeConfig": {"max_executor_threads": 16},
    }


def make_plan(rung_id: str, case_id: str, seed: int | None = None) -> CasePlan:
    if rung_id != RUNG_ID:
        raise SetupBlock(f"unsupported rung {rung_id}")
    if case_id not in CASE_MATRIX:
        raise SetupBlock(f"unknown case {case_id}")
    expected_seed, scenario = CASE_MATRIX[case_id]
    if seed is not None and seed != expected_seed:
        raise SetupBlock(f"seed {seed} does not match {case_id} seed {expected_seed}")
    suffix = hashlib.sha1(f"{case_id}:{expected_seed}".encode("utf-8")).hexdigest()[:8]
    return CasePlan(
        rung_id=RUNG_ID,
        case_id=case_id,
        seed=expected_seed,
        scenario=scenario,
        database_prefix=f"wio_decorator_{expected_seed}_{case_id.replace('-', '_')}_{suffix}",
        workflow_id=f"wio-e024-{expected_seed}-{case_id}",
    )


def init_app_table(app_url: str) -> None:
    engine = sa.create_engine(app_url.replace("postgresql://", "postgresql+psycopg://"))
    try:
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {HOOK_TABLE} (
                        id BIGSERIAL PRIMARY KEY,
                        intent_id TEXT NOT NULL,
                        workflow_id TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        created_at_ms BIGINT NOT NULL
                    )
                    """
                )
            )
    finally:
        engine.dispose()


def workflow_status_rows(dbos: DBOS, workflow_ids: list[str]) -> list[dict[str, Any]]:
    with dbos._sys_db.engine.begin() as conn:
        rows = conn.execute(
            sa.select(
                SystemSchema.workflow_status.c.workflow_uuid,
                SystemSchema.workflow_status.c.status,
                SystemSchema.workflow_status.c.name,
                SystemSchema.workflow_status.c.class_name,
                SystemSchema.workflow_status.c.config_name,
                SystemSchema.workflow_status.c.queue_name,
            )
            .where(SystemSchema.workflow_status.c.workflow_uuid.in_(workflow_ids))
            .order_by(SystemSchema.workflow_status.c.workflow_uuid)
        ).mappings().all()
    return [dict(row) for row in rows]


def operation_output_rows(dbos: DBOS, workflow_ids: list[str]) -> list[dict[str, Any]]:
    with dbos._sys_db.engine.begin() as conn:
        rows = conn.execute(
            sa.select(
                SystemSchema.operation_outputs.c.workflow_uuid,
                SystemSchema.operation_outputs.c.function_id,
                SystemSchema.operation_outputs.c.function_name,
                SystemSchema.operation_outputs.c.error,
            )
            .where(SystemSchema.operation_outputs.c.workflow_uuid.in_(workflow_ids))
            .order_by(
                SystemSchema.operation_outputs.c.workflow_uuid,
                SystemSchema.operation_outputs.c.function_id,
            )
        ).mappings().all()
    return [dict(row) for row in rows]


def app_rows(app_url: str, intent_id: str) -> list[dict[str, Any]]:
    engine = sa.create_engine(app_url.replace("postgresql://", "postgresql+psycopg://"))
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                sa.text(
                    f"""
                    SELECT intent_id, workflow_id, payload
                    FROM {HOOK_TABLE}
                    WHERE intent_id = :intent_id
                    ORDER BY id
                    """
                ),
                {"intent_id": intent_id},
            ).mappings().all()
            return [dict(row) for row in rows]
    finally:
        engine.dispose()


def label_total(label: str, phase: str) -> int:
    return hook_counts().get(label, {}).get(phase, 0)


async def run_case001(plan: CasePlan, dbos: DBOS, sys_url: str) -> dict[str, Any]:
    direct_id = plan.workflow_id + "-direct"
    queue_id = plan.workflow_id + "-queue"
    client_id = plan.workflow_id + "-client"
    with SetWorkflowID(direct_id):
        first = await e024_case001_async_workflow("direct")
    counts_after_first = hook_counts()
    with SetWorkflowID(direct_id):
        replay = await e024_case001_async_workflow("mutated")
    counts_after_replay = hook_counts()
    invariant(
        "dbos_outer_completed_replay_does_not_rerun_inner_hook",
        replay == "direct:step" and counts_after_replay == counts_after_first,
        first=first,
        replay=replay,
        counts_after_first=counts_after_first,
        counts_after_replay=counts_after_replay,
        direct_status=workflow_status_rows(dbos, [direct_id]),
        direct_operations=operation_output_rows(dbos, [direct_id]),
    )
    with SetWorkflowID(queue_id):
        queue_handle = await CASE001_QUEUE.enqueue_async(e024_case001_async_workflow, "queue")
    queue_result = await queue_handle.get_result()
    client = DBOSClient(system_database_url=sys_url)
    try:
        client_handle = await client.enqueue_async(
            {
                "queue_name": CASE001_QUEUE.name,
                "workflow_name": "e024_case001_async_workflow",
                "workflow_id": client_id,
            },
            "client",
        )
        client_result = await client_handle.get_result()
    finally:
        client.destroy()
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING", "name": "e024_case001_async_workflow"})
            .where(SystemSchema.workflow_status.c.workflow_uuid == direct_id)
        )

    def recover_in_thread() -> Any:
        return DBOS._execute_workflow_id(direct_id).get_result()

    recovery_result = await asyncio.to_thread(recover_in_thread)
    workflow_ids = [direct_id, queue_id, client_id]
    statuses = workflow_status_rows(dbos, workflow_ids)
    operations = operation_output_rows(dbos, workflow_ids)
    counts = hook_counts()
    invariant(
        "dbos_outer_async_entrypoints_preserve_names_and_replay_hooks",
        first == "direct:step"
        and replay == "direct:step"
        and queue_result == "queue:step"
        and client_result == "client:step"
        and recovery_result == "direct:step"
        and counts_after_first.get("case001_workflow", {}).get("before") == 1
        and counts.get("case001_workflow", {}).get("before") == 4
        and counts.get("case001_workflow", {}).get("after") == 4
        and all(row["name"] == "e024_case001_async_workflow" for row in statuses)
        and {row["function_name"] for row in operations} == {"e024_case001_async_step"},
        results={
            "first": first,
            "replay": replay,
            "queue": queue_result,
            "client": client_result,
            "recovery": recovery_result,
        },
        counts=counts,
        statuses=statuses,
        operations=operations,
    )
    return {
        "case": plan.case_id,
        "results": [first, replay, queue_result, client_result, recovery_result],
        "hook_counts": counts,
        "workflow_status_rows": statuses,
        "operation_output_rows": operations,
    }


def run_case002(plan: CasePlan, dbos: DBOS, sys_url: str) -> dict[str, Any]:
    start_id = plan.workflow_id + "-start"
    queue_id = plan.workflow_id + "-queue"
    client_id = plan.workflow_id + "-client"
    with SetWorkflowID(start_id):
        start_result = DBOS.start_workflow(e024_case002_sync_workflow, "start").get_result()
    with SetWorkflowID(queue_id):
        queue_result = CASE002_QUEUE.enqueue(e024_case002_sync_workflow, "queue").get_result()
    client = DBOSClient(system_database_url=sys_url)
    try:
        client_result = client.enqueue(
            {
                "queue_name": CASE002_QUEUE.name,
                "workflow_name": "e024_case002_sync_workflow",
                "workflow_id": client_id,
            },
            "client",
        ).get_result()
    finally:
        client.destroy()
    workflow_ids = [start_id, queue_id, client_id]
    statuses = workflow_status_rows(dbos, workflow_ids)
    operations = operation_output_rows(dbos, workflow_ids)
    counts = hook_counts()
    invariant(
        "custom_outer_lookup_preserves_workflow_and_step_names",
        start_result == "start:step"
        and queue_result == "queue:step"
        and client_result == "client:step"
        and counts.get("case002_workflow", {}).get("before") == 3
        and counts.get("case002_step", {}).get("before") == 3
        and all(row["name"] == "e024_case002_sync_workflow" for row in statuses)
        and {row["function_name"] for row in operations} == {"e024_case002_sync_step"},
        results={"start": start_result, "queue": queue_result, "client": client_result},
        counts=counts,
        statuses=statuses,
        operations=operations,
    )
    return {
        "case": plan.case_id,
        "results": [start_result, queue_result, client_result],
        "hook_counts": counts,
        "workflow_status_rows": statuses,
        "operation_output_rows": operations,
    }


def run_case003(plan: CasePlan, dbos: DBOS, app_url: str) -> dict[str, Any]:
    intent_id = "intent-" + plan.case_id
    first_error = ""
    second_error = ""
    try:
        with SetWorkflowID(plan.workflow_id):
            e024_case003_failure_workflow(intent_id)
    except Exception as exc:
        first_error = str(exc)
    counts_after_first = hook_counts()
    try:
        with SetWorkflowID(plan.workflow_id):
            e024_case003_failure_workflow(intent_id)
    except Exception as exc:
        second_error = str(exc)
    statuses = workflow_status_rows(dbos, [plan.workflow_id])
    operations = operation_output_rows(dbos, [plan.workflow_id])
    rows = app_rows(app_url, intent_id)
    counts = hook_counts()
    invariant(
        "sync_failure_replay_preserves_app_error_without_rerun",
        "modeled decorator failure" in first_error
        and second_error == first_error
        and counts == counts_after_first
        and counts.get("case003_workflow", {}).get("before") == 1
        and counts.get("case003_workflow", {}).get("error") == 1
        and counts.get("case003_step", {}).get("before") == 1
        and counts.get("case003_transaction", {}).get("before") == 1
        and len(rows) == 1
        and statuses == [
            {
                "workflow_uuid": plan.workflow_id,
                "status": "ERROR",
                "name": "e024_case003_failure_workflow",
                "class_name": None,
                "config_name": None,
                "queue_name": None,
            }
        ]
        and {row["function_name"] for row in operations}
        == {"e024_case003_step", "e024_case003_transaction"},
        first_error=first_error,
        second_error=second_error,
        counts_after_first=counts_after_first,
        counts=counts,
        app_rows=rows,
        statuses=statuses,
        operations=operations,
    )
    return {
        "case": plan.case_id,
        "first_error": first_error,
        "second_error": second_error,
        "hook_counts": counts,
        "app_rows": rows,
        "workflow_status_rows": statuses,
        "operation_output_rows": operations,
    }


def run_case004(plan: CasePlan, dbos: DBOS, sys_url: str) -> dict[str, Any]:
    instance_id = plan.workflow_id + "-instance"
    static_id = plan.workflow_id + "-static"
    class_id = plan.workflow_id + "-class"
    with SetWorkflowID(instance_id):
        instance_result = DBOS.start_workflow(
            CASE004_INSTANCE.instance_workflow, "direct"
        ).get_result()
    with SetWorkflowID(static_id):
        static_result = DBOS.start_workflow(
            E024DecoratedClass.static_workflow, "static"
        ).get_result()
    with SetWorkflowID(class_id):
        class_result = CASE004_QUEUE.enqueue(
            E024DecoratedClass.class_workflow, "class"
        ).get_result()
    client = DBOSClient(system_database_url=sys_url)
    client_static_id = plan.workflow_id + "-client-static"
    try:
        client_static_result = client.enqueue(
            {
                "queue_name": CASE004_QUEUE.name,
                "workflow_name": "e024_case004_static_workflow",
                "class_name": "E024DecoratedClass",
                "workflow_id": client_static_id,
            },
            "client-static",
        ).get_result()
    finally:
        client.destroy()
    workflow_ids = [instance_id, static_id, class_id, client_static_id]
    statuses = workflow_status_rows(dbos, workflow_ids)
    counts = hook_counts()
    expected_names = {
        instance_id: ("e024_case004_instance_workflow", "E024DecoratedClass", "case004-instance"),
        static_id: ("e024_case004_static_workflow", "E024DecoratedClass", None),
        class_id: ("e024_case004_class_workflow", "E024DecoratedClass", None),
        client_static_id: ("e024_case004_static_workflow", "E024DecoratedClass", None),
    }
    status_ok = all(
        (
            row["name"],
            row["class_name"],
            row["config_name"],
        )
        == expected_names[row["workflow_uuid"]]
        for row in statuses
    )
    invariant(
        "class_method_metadata_survives_wrapped_entrypoints",
        instance_result == "instance:case004-instance:direct"
        and static_result == "static:static"
        and class_result == "class:E024DecoratedClass:class"
        and client_static_result == "static:client-static"
        and status_ok
        and counts.get("case004_instance_workflow", {}).get("before") == 1
        and counts.get("case004_static_workflow", {}).get("before") == 2
        and counts.get("case004_class_workflow", {}).get("before") == 1,
        results={
            "instance": instance_result,
            "static": static_result,
            "class": class_result,
            "client_static": client_static_result,
        },
        counts=counts,
        statuses=statuses,
    )
    return {
        "case": plan.case_id,
        "results": {
            "instance": instance_result,
            "static": static_result,
            "class": class_result,
            "client_static": client_static_result,
        },
        "hook_counts": counts,
        "workflow_status_rows": statuses,
    }


def run_one(plan: CasePlan, artifact_dir: Path) -> dict[str, Any]:
    _hook_events.clear()
    _app_side_effect_counts.clear()
    app_url, sys_url, masked = prepare_databases(plan.database_prefix, artifact_dir)
    init_app_table(app_url)
    dbos: DBOS | None = None
    try:
        DBOS.destroy(destroy_registry=False)
        dbos = DBOS(config=make_config(plan, app_url, sys_url))
        dbos.launch()
        event(
            "decorator_case_started",
            rung=plan.rung_id,
            case_id=plan.case_id,
            seed=plan.seed,
            scenario=plan.scenario,
        )
        if plan.case_id == "case-001":
            result = asyncio.run(run_case001(plan, dbos, sys_url))
        elif plan.case_id == "case-002":
            result = run_case002(plan, dbos, sys_url)
        elif plan.case_id == "case-003":
            result = run_case003(plan, dbos, app_url)
        elif plan.case_id == "case-004":
            result = run_case004(plan, dbos, sys_url)
        else:
            raise SetupBlock(f"unsupported case {plan.case_id}")
        result.update(
            {
                "status": "passed",
                "frontier": FRONTIER_ID,
                "rung": plan.rung_id,
                "seed": plan.seed,
                "scenario": plan.scenario,
                "app_db": plan.database_prefix + "_app",
                "sys_db": plan.database_prefix + "_sys",
                "admin_url": masked,
            }
        )
        write_json(artifact_dir / "result.json", result)
        event("decorator_case_passed", rung=plan.rung_id, case_id=plan.case_id)
        return result
    finally:
        if dbos is not None:
            DBOS.destroy(destroy_registry=False)
        drop_databases(plan.database_prefix)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rung", required=True)
    parser.add_argument("--case", dest="case_id")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument("--artifact-dir", default="/tmp/wio-artifacts/decorator-composition-fidelity")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.rung != RUNG_ID:
            raise SetupBlock(f"unsupported rung {args.rung}")
        cases = list(CASE_MATRIX) if args.all_cases else [args.case_id or ""]
        if not cases or not all(cases):
            raise SetupBlock("--case is required unless --all-cases is set")
        if args.all_cases and not args.sequential:
            raise SetupBlock("--all-cases requires --sequential")
        artifact_root = Path(args.artifact_dir)
        summaries: list[dict[str, Any]] = []
        for case_id in cases:
            plan = make_plan(args.rung, case_id, args.seed if not args.all_cases else None)
            case_artifacts = artifact_root / case_id
            write_json(
                case_artifacts / "case.json",
                {
                    **asdict(plan),
                    "frontier": FRONTIER_ID,
                    "protected_product_promise": (
                        "DBOS-decorated functions remain discoverable, durable, replayable, "
                        "and operator-visible when composed with ordinary functools.wraps decorators."
                    ),
                    "replay_command": (
                        ".workers/run-with-postgres.sh .workers/python-runtime.sh "
                        ".workers/workloads/decorator-composition-fidelity/decorator_composition_fidelity_workload.py "
                        f"--rung {plan.rung_id} --case {plan.case_id} --seed {plan.seed}"
                    ),
                    "seed_policy": "Exact seeds from E-024 parameter matrix.",
                    "invariant_oracle": (
                        "Hook ledger plus workflow_status, operation_outputs, public results, "
                        "class metadata, and replay/recovery counts agree with the model."
                    ),
                },
            )
            summaries.append(run_one(plan, case_artifacts))
        write_json(artifact_root / "summary.json", summaries)
        return 0
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 42
    except WorkloadFailure as exc:
        print(f"FINDING-CANDIDATE {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
