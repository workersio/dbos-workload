#!/usr/bin/env python3
"""WIO workload for DBOS SQL enqueue auth-context preservation.

Frontier: auth-context-sql-enqueue
Rung:
  - rung-001-sql-auth-context-recovery-query
Protected product promise:
  PostgreSQL SQL-origin workflow auth metadata survives queue execution,
  required-role checks, delay/duplicate/relaunch, export/import, and
  runtime/client/direct-SQL observations.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
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

    from dbos import DBOS, DBOSClient, DBOSConfig
    from dbos._error import DBOSNotAuthorizedError
    from dbos._registrations import get_dbos_func_name
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "auth-context-sql-enqueue"
RUNG_001_ID = "rung-001-sql-auth-context-recovery-query"
APP_ID = "wio-auth-sql"
APP_VERSION = "wio-auth-sql-rungs-001"
PROMPT_PATH = "evidence-key:frontier-auth-context-sql-enqueue"


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
    focus: str
    database_prefix: str


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
                "kind": "postgres_setup_failed",
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
    if os.environ.get("WIO_AUTH_SQL_KEEP_DATABASES") == "1":
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
        "application_version": f"{APP_VERSION}-{plan.rung_id}-{plan.case_id}",
        "executor_id": f"wio-auth-sql-{plan.case_id}",
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        "run_admin_server": False,
        "runtimeConfig": {"max_executor_threads": 16},
    }


def normalize_rung(rung: str) -> str:
    aliases = {
        "rung-001": RUNG_001_ID,
        RUNG_001_ID: RUNG_001_ID,
    }
    if rung not in aliases:
        raise SetupBlock(f"unsupported rung {rung}")
    return aliases[rung]


def case_ids_for_rung(rung_id: str) -> list[str]:
    if rung_id == RUNG_001_ID:
        return ["case-001", "case-002", "case-003", "case-004"]
    raise SetupBlock(f"unsupported rung {rung_id}")


def make_plan(rung: str, case_id: str) -> CasePlan:
    rung_id = normalize_rung(rung)
    if case_id not in case_ids_for_rung(rung_id):
        raise SetupBlock(f"unknown case {case_id} for {rung_id}")
    specs = {
        "case-001": (
            6830,
            "sql-role-allowed",
            "SQL enqueue with admin role completes under modeled auth context",
        ),
        "case-002": (
            6831,
            "sql-role-denied",
            "SQL enqueue missing admin role fails authorization without losing auth metadata",
        ),
        "case-003": (
            6832,
            "delay-duplicate-relaunch",
            "Delayed SQL enqueue survives duplicate attempt and relaunch with original auth",
        ),
        "case-004": (
            6833,
            "export-import-client-parity",
            "Export/import preserves SQL-origin auth metadata and result",
        ),
    }
    seed, schedule, focus = specs[case_id]
    suffix = hashlib.sha1(f"{rung_id}:{case_id}:{seed}".encode()).hexdigest()[:8]
    return CasePlan(
        rung_id=rung_id,
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        focus=focus,
        database_prefix=f"wio_auth_{seed}_{case_id.replace('-', '_')}_{suffix}",
    )


@DBOS.required_roles(["admin"])
@DBOS.workflow()
def sql_auth_required_workflow(label: str) -> dict[str, Any]:
    return {
        "kind": "sql_auth_required",
        "label": label,
        "workflow_id": DBOS.workflow_id,
        "authenticated_user": DBOS.authenticated_user,
        "authenticated_roles": DBOS.authenticated_roles,
        "assumed_role": DBOS.assumed_role,
    }


@DBOS.workflow()
def sql_auth_open_workflow(label: str) -> dict[str, Any]:
    return {
        "kind": "sql_auth_open",
        "label": label,
        "workflow_id": DBOS.workflow_id,
        "authenticated_user": DBOS.authenticated_user,
        "authenticated_roles": DBOS.authenticated_roles,
        "assumed_role": DBOS.assumed_role,
    }


def workflow_name(fn: Any) -> str:
    return get_dbos_func_name(fn)


def launch_dbos(plan: CasePlan, app_url: str, sys_url: str) -> DBOS:
    DBOS.destroy(destroy_registry=False)
    dbos = DBOS(config=make_config(plan, app_url, sys_url))
    dbos.launch()
    return dbos


def register_case_queue(queue_name: str) -> None:
    DBOS.register_queue(
        queue_name,
        concurrency=4,
        polling_interval_sec=0.05,
        on_conflict="always_update",
    )


def sql_engine(sys_url: str) -> sa.Engine:
    return sa.create_engine(sys_url, connect_args={"connect_timeout": 5})


def execute_sql(engine: sa.Engine, sql: str, params: dict[str, Any] | None = None) -> Any:
    with engine.begin() as connection:
        result = connection.execute(sa.text(sql), params or {})
        return result.fetchone()


def workflow_status_row(engine: sa.Engine, workflow_id: str) -> dict[str, Any] | None:
    sql = """
    SELECT workflow_uuid, status, name, queue_name, deduplication_id,
           inputs, output, error, authenticated_user, authenticated_roles,
           assumed_role, application_version, delay_until_epoch_ms
    FROM "dbos".workflow_status
    WHERE workflow_uuid = :workflow_id
    """
    row = execute_sql(engine, sql, {"workflow_id": workflow_id})
    if row is None:
        return None
    return {
        "workflow_id": row[0],
        "status": row[1],
        "name": row[2],
        "queue_name": row[3],
        "deduplication_id": row[4],
        "inputs": row[5],
        "output": row[6],
        "error": row[7],
        "authenticated_user": row[8],
        "authenticated_roles": row[9],
        "assumed_role": row[10],
        "app_version": row[11],
        "delay_until_epoch_ms": row[12],
    }


def enqueue_sql_workflow(
    engine: sa.Engine,
    *,
    workflow_name_value: str,
    queue_name: str,
    workflow_id: str,
    label: str,
    authenticated_user: str,
    authenticated_roles: list[str],
    deduplication_id: str | None = None,
    delay_until_epoch_ms: int | None = None,
) -> str:
    sql = """
    SELECT "dbos".enqueue_workflow(
        workflow_name => :workflow_name,
        queue_name => :queue_name,
        positional_args => ARRAY[:label]::json[],
        workflow_id => :workflow_id,
        app_version => :app_version,
        deduplication_id => :deduplication_id,
        authenticated_user => :authenticated_user,
        authenticated_roles => :authenticated_roles,
        delay_until_epoch_ms => :delay_until_epoch_ms
    )
    """
    row = execute_sql(
        engine,
        sql,
        {
            "workflow_name": workflow_name_value,
            "queue_name": queue_name,
            "label": json.dumps(label),
            "workflow_id": workflow_id,
            "app_version": DBOS.application_version,
            "deduplication_id": deduplication_id,
            "authenticated_user": authenticated_user,
            "authenticated_roles": json.dumps(authenticated_roles),
            "delay_until_epoch_ms": delay_until_epoch_ms,
        },
    )
    if row is None:
        raise SetupBlock("SQL enqueue returned no row")
    return str(row[0])


def wait_for_status(
    engine: sa.Engine,
    workflow_id: str,
    statuses: set[str],
    *,
    timeout_seconds: float = 45.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        last = workflow_status_row(engine, workflow_id)
        if last is not None and last["status"] in statuses:
            return last
        time.sleep(0.25)
    invariant(
        "workflow_reached_expected_status",
        False,
        workflow_id=workflow_id,
        expected_statuses=sorted(statuses),
        last=last,
        timeout_seconds=timeout_seconds,
    )
    raise AssertionError("unreachable")


def public_status_summary(status: Any) -> dict[str, Any] | None:
    if status is None:
        return None
    return {
        "workflow_id": status.workflow_id,
        "status": status.status,
        "name": status.name,
        "authenticated_user": status.authenticated_user,
        "authenticated_roles": status.authenticated_roles,
        "assumed_role": status.assumed_role,
        "queue_name": status.queue_name,
        "app_version": status.app_version,
    }


def list_ids(rows: list[Any]) -> list[str]:
    return sorted(row.workflow_id for row in rows)


def assert_auth_observations(
    label: str,
    *,
    workflow_id: str,
    expected_user: str,
    expected_roles: list[str],
    expected_status: str | None,
    engine: sa.Engine,
    client: DBOSClient,
) -> dict[str, Any]:
    runtime_status = DBOS.get_workflow_status(workflow_id)
    client_status = client.retrieve_workflow(workflow_id).get_status()
    sql_status = workflow_status_row(engine, workflow_id)
    runtime_user_rows = DBOS.list_workflows(
        user=expected_user,
        workflow_ids=[workflow_id],
        load_input=False,
        load_output=False,
    )
    client_user_rows = client.list_workflows(
        user=expected_user,
        workflow_ids=[workflow_id],
        load_input=False,
        load_output=False,
    )
    wrong_runtime_rows = DBOS.list_workflows(
        user=f"{expected_user}-wrong",
        workflow_ids=[workflow_id],
        load_input=False,
        load_output=False,
    )
    sql_roles = json.loads(sql_status["authenticated_roles"]) if sql_status else None
    ok = (
        runtime_status is not None
        and client_status is not None
        and sql_status is not None
        and runtime_status.authenticated_user == expected_user
        and runtime_status.authenticated_roles == expected_roles
        and client_status.authenticated_user == expected_user
        and client_status.authenticated_roles == expected_roles
        and sql_status["authenticated_user"] == expected_user
        and sql_roles == expected_roles
        and list_ids(runtime_user_rows) == [workflow_id]
        and list_ids(client_user_rows) == [workflow_id]
        and wrong_runtime_rows == []
        and (expected_status is None or runtime_status.status == expected_status)
    )
    invariant(
        label,
        ok,
        workflow_id=workflow_id,
        expected_user=expected_user,
        expected_roles=expected_roles,
        expected_status=expected_status,
        runtime=public_status_summary(runtime_status),
        client=public_status_summary(client_status),
        sql=sql_status,
        runtime_user_rows=list_ids(runtime_user_rows),
        client_user_rows=list_ids(client_user_rows),
        wrong_runtime_rows=list_ids(wrong_runtime_rows),
    )
    return {
        "runtime": public_status_summary(runtime_status),
        "client": public_status_summary(client_status),
        "sql": sql_status,
        "runtime_user_rows": list_ids(runtime_user_rows),
        "client_user_rows": list_ids(client_user_rows),
    }


def assert_required_result(
    label: str,
    *,
    workflow_id: str,
    label_value: str,
    expected_user: str,
    expected_roles: list[str],
) -> dict[str, Any]:
    result = DBOS.retrieve_workflow(workflow_id).get_result()
    invariant(
        label,
        result == {
            "kind": "sql_auth_required",
            "label": label_value,
            "workflow_id": workflow_id,
            "authenticated_user": expected_user,
            "authenticated_roles": expected_roles,
            "assumed_role": "admin",
        },
        workflow_id=workflow_id,
        expected_user=expected_user,
        expected_roles=expected_roles,
        result=result,
    )
    return result


def run_auth_case(plan: CasePlan, artifacts: Path) -> dict[str, Any]:
    app_url, sys_url, masked = prepare_databases(plan.database_prefix, artifacts)
    import_prefix: str | None = None
    dbos: DBOS | None = None
    client: DBOSClient | None = None
    engine: sa.Engine | None = None
    try:
        dbos = launch_dbos(plan, app_url, sys_url)
        queue_name = f"wio_auth_queue_{plan.seed}_{plan.case_id.replace('-', '_')}"
        register_case_queue(queue_name)
        engine = sql_engine(sys_url)
        client = DBOSClient(system_database_url=sys_url)
        observations: dict[str, Any] = {}
        event(
            "auth_sql_case_started",
            rung=plan.rung_id,
            case_id=plan.case_id,
            seed=plan.seed,
            schedule=plan.schedule,
        )

        if plan.case_id == "case-001":
            workflow_id = f"wio-auth-{plan.seed}-allowed"
            user = "alice"
            roles = ["reader", "admin"]
            label_value = "allowed-sql-auth"
            returned = enqueue_sql_workflow(
                engine,
                workflow_name_value=workflow_name(sql_auth_required_workflow),
                queue_name=queue_name,
                workflow_id=workflow_id,
                label=label_value,
                authenticated_user=user,
                authenticated_roles=roles,
            )
            wait_for_status(engine, workflow_id, {"SUCCESS"})
            observations["auth"] = assert_auth_observations(
                "sql_allowed_auth_runtime_client_sql",
                workflow_id=workflow_id,
                expected_user=user,
                expected_roles=roles,
                expected_status="SUCCESS",
                engine=engine,
                client=client,
            )
            observations["result"] = assert_required_result(
                "sql_allowed_required_role_body_observed_auth",
                workflow_id=workflow_id,
                label_value=label_value,
                expected_user=user,
                expected_roles=roles,
            )
            observations["returned_workflow_id"] = returned

        elif plan.case_id == "case-002":
            workflow_id = f"wio-auth-{plan.seed}-denied"
            user = "bob"
            roles = ["reader"]
            returned = enqueue_sql_workflow(
                engine,
                workflow_name_value=workflow_name(sql_auth_required_workflow),
                queue_name=queue_name,
                workflow_id=workflow_id,
                label="denied-sql-auth",
                authenticated_user=user,
                authenticated_roles=roles,
            )
            wait_for_status(engine, workflow_id, {"ERROR"})
            result_error = None
            try:
                DBOS.retrieve_workflow(workflow_id).get_result()
            except Exception as exc:  # expected authorization path
                result_error = {"type": type(exc).__name__, "message": str(exc)}
            observations["auth"] = assert_auth_observations(
                "sql_denied_auth_preserved_runtime_client_sql",
                workflow_id=workflow_id,
                expected_user=user,
                expected_roles=roles,
                expected_status="ERROR",
                engine=engine,
                client=client,
            )
            sql_status = workflow_status_row(engine, workflow_id)
            invariant(
                "sql_denied_terminal_error_is_authorization_specific",
                result_error is not None
                and (
                    result_error["type"] == DBOSNotAuthorizedError.__name__
                    or "not authenticated" in result_error["message"]
                    or "required roles" in result_error["message"]
                )
                and sql_status is not None
                and "DBOSNotAuthorizedError" in str(sql_status["error"]),
                workflow_id=workflow_id,
                result_error=result_error,
                sql_status=sql_status,
            )
            observations["result_error"] = result_error
            observations["returned_workflow_id"] = returned

        elif plan.case_id == "case-003":
            workflow_id = f"wio-auth-{plan.seed}-delay"
            user = "carol"
            roles = ["reader", "admin"]
            original_label = "original-delayed-auth"
            duplicate_label = "duplicate-should-not-win"
            delay_until = int(time.time() * 1000) + 3_000
            returned = enqueue_sql_workflow(
                engine,
                workflow_name_value=workflow_name(sql_auth_required_workflow),
                queue_name=queue_name,
                workflow_id=workflow_id,
                label=original_label,
                authenticated_user=user,
                authenticated_roles=roles,
                deduplication_id=f"dedup-{plan.seed}",
                delay_until_epoch_ms=delay_until,
            )
            duplicate_returned = enqueue_sql_workflow(
                engine,
                workflow_name_value=workflow_name(sql_auth_required_workflow),
                queue_name=queue_name,
                workflow_id=workflow_id,
                label=duplicate_label,
                authenticated_user="mallory",
                authenticated_roles=["reader"],
                deduplication_id=f"dedup-{plan.seed}",
                delay_until_epoch_ms=delay_until + 30_000,
            )
            delayed_before = workflow_status_row(engine, workflow_id)
            client.destroy()
            client = None
            DBOS.destroy(destroy_registry=False)
            dbos = launch_dbos(plan, app_url, sys_url)
            register_case_queue(queue_name)
            client = DBOSClient(system_database_url=sys_url)
            wait_for_status(engine, workflow_id, {"SUCCESS"}, timeout_seconds=60.0)
            observations["auth"] = assert_auth_observations(
                "sql_delay_duplicate_relaunch_auth_preserved",
                workflow_id=workflow_id,
                expected_user=user,
                expected_roles=roles,
                expected_status="SUCCESS",
                engine=engine,
                client=client,
            )
            observations["result"] = assert_required_result(
                "sql_delay_duplicate_relaunch_result_uses_original_input",
                workflow_id=workflow_id,
                label_value=original_label,
                expected_user=user,
                expected_roles=roles,
            )
            final_sql = workflow_status_row(engine, workflow_id)
            invariant(
                "sql_duplicate_did_not_overwrite_inputs_or_auth",
                returned == workflow_id
                and duplicate_returned == workflow_id
                and delayed_before is not None
                and final_sql is not None
                and original_label in str(final_sql["inputs"])
                and duplicate_label not in str(final_sql["inputs"])
                and final_sql["authenticated_user"] == user
                and json.loads(final_sql["authenticated_roles"]) == roles,
                workflow_id=workflow_id,
                returned=returned,
                duplicate_returned=duplicate_returned,
                delayed_before=delayed_before,
                final_sql=final_sql,
            )

        elif plan.case_id == "case-004":
            workflow_id = f"wio-auth-{plan.seed}-export"
            user = "dora"
            roles = ["auditor", "admin"]
            label_value = "export-import-auth"
            enqueue_sql_workflow(
                engine,
                workflow_name_value=workflow_name(sql_auth_required_workflow),
                queue_name=queue_name,
                workflow_id=workflow_id,
                label=label_value,
                authenticated_user=user,
                authenticated_roles=roles,
            )
            wait_for_status(engine, workflow_id, {"SUCCESS"})
            source_result = assert_required_result(
                "sql_export_source_result_observed_auth",
                workflow_id=workflow_id,
                label_value=label_value,
                expected_user=user,
                expected_roles=roles,
            )
            exported = dbos._sys_db.export_workflow(workflow_id, export_children=True)
            exported_status = exported[0]["workflow_status"]
            import_prefix = plan.database_prefix + "_imp"
            import_app_url, import_sys_url, _ = prepare_databases(import_prefix, artifacts / "import")
            client.destroy()
            client = None
            DBOS.destroy(destroy_registry=False)
            dbos = DBOS(config=make_config(plan, import_app_url, import_sys_url))
            dbos.launch()
            dbos._sys_db.import_workflow(exported)
            client = DBOSClient(system_database_url=import_sys_url)
            import_engine = sql_engine(import_sys_url)
            try:
                observations["import_auth"] = assert_auth_observations(
                    "sql_export_import_auth_runtime_client_sql",
                    workflow_id=workflow_id,
                    expected_user=user,
                    expected_roles=roles,
                    expected_status="SUCCESS",
                    engine=import_engine,
                    client=client,
                )
                imported_result = DBOS.retrieve_workflow(workflow_id).get_result()
                user_ids = list_ids(
                    client.list_workflows(
                        user=user,
                        workflow_ids=[workflow_id],
                        load_input=False,
                        load_output=False,
                    )
                )
                invariant(
                    "sql_export_import_payload_preserves_auth_and_result",
                    exported_status["authenticated_user"] == user
                    and json.loads(exported_status["authenticated_roles"]) == roles
                    and imported_result == source_result
                    and user_ids == [workflow_id],
                    workflow_id=workflow_id,
                    exported_auth={
                        "user": exported_status["authenticated_user"],
                        "roles": exported_status["authenticated_roles"],
                    },
                    source_result=source_result,
                    imported_result=imported_result,
                    user_ids=user_ids,
                )
                observations["exported_auth"] = {
                    "user": exported_status["authenticated_user"],
                    "roles": exported_status["authenticated_roles"],
                }
                observations["imported_result"] = imported_result
            finally:
                import_engine.dispose()

        else:
            raise SetupBlock(f"unsupported case {plan.case_id}")

        result = {
            "status": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case_id": plan.case_id,
            "seed": plan.seed,
            "schedule": plan.schedule,
            "observations": observations,
            "dbos_product_source": str(next((p for p in sys.path if p.endswith("dbos-transact-py")), "")),
            "app_db": plan.database_prefix + "_app",
            "sys_db": plan.database_prefix + "_sys",
            "admin_url": masked,
        }
        write_json(artifacts / "result.json", result)
        event("case_passed", rung=plan.rung_id, case_id=plan.case_id)
        return result
    finally:
        if client is not None:
            client.destroy()
        if dbos is not None:
            DBOS.destroy(destroy_registry=False)
        if engine is not None:
            engine.dispose()
        drop_databases(plan.database_prefix)
        if import_prefix is not None:
            drop_databases(import_prefix)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rung", required=True)
    parser.add_argument("--case", dest="case_id")
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument("--artifact-dir", default="/tmp/wio-artifacts/auth-context-sql-enqueue")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        rung_id = normalize_rung(args.rung)
        cases = case_ids_for_rung(rung_id) if args.all_cases else [args.case_id or ""]
        if not cases or not all(cases):
            raise SetupBlock("--case is required unless --all-cases is set")
        if args.all_cases and not args.sequential:
            raise SetupBlock("--all-cases requires --sequential for exclusive database setup")
        artifact_root = Path(args.artifact_dir)
        summaries: list[dict[str, Any]] = []
        for case_id in cases:
            plan = make_plan(rung_id, case_id)
            case_artifacts = artifact_root / case_id
            write_json(
                case_artifacts / "case.json",
                {
                    **asdict(plan),
                    "frontier": FRONTIER_ID,
                    "prompt_path": PROMPT_PATH,
                    "protected_product_promise": (
                        "SQL-origin workflows preserve authenticated user and roles "
                        "through queue execution, required-role checks, relaunch, "
                        "export/import, and runtime/client/direct SQL observations."
                    ),
                    "replay_command": (
                        ".workers/run-with-postgres.sh .workers/python-runtime.sh "
                        f".workers/workloads/auth-context-sql-enqueue/auth_context_sql_enqueue_workload.py --rung {plan.rung_id} --case {plan.case_id}"
                    ),
                    "seed_policy": "Exact rung seeds 6830, 6831, 6832, and 6833.",
                    "invariant_oracle": (
                        "Independent owner/roles/input model compared against "
                        "DBOS.get_workflow_status, DBOSClient list/status, direct "
                        "SQL workflow_status rows, required-role workflow results, "
                        "duplicate SQL enqueue behavior, relaunch, and export/import."
                    ),
                },
            )
            summaries.append(run_auth_case(plan, case_artifacts))
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
