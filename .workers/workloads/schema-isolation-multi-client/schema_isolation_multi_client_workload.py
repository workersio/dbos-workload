#!/usr/bin/env python3
"""WIO workload for DBOS multi-schema object isolation.

Frontier: schema-isolation-multi-client
Rung:
  - rung-001-two-schema-client-datasource-isolation
Protected product promise:
  A schema-bound DBOS object keeps targeting its own Postgres schema after a
  second object initializes a different schema in the same Python process.
Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/schema-isolation-multi-client/schema_isolation_multi_client_workload.py \
    --rung rung-001-two-schema-client-datasource-isolation --case case-001
Seed policy:
  Exact seeds 7420, 7421, 7422. Each case writes generated schema names,
  workflow IDs, and physical SQL observations under the artifact directory.
Invariant oracle:
  Public DBOS/DBOSClient/datasource observations and independent quoted
  schema-qualified SQL must agree with the per-schema ledger. The oracle does
  not use DBOS SystemSchema/ApplicationSchema/DatasourceSchema objects.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

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
    from sqlalchemy.orm import Session

    from dbos import DBOS, DBOSClient, DBOSConfig, SQLAlchemyDatasource, SetWorkflowID
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "schema-isolation-multi-client"
RUNG_001_ID = "rung-001-two-schema-client-datasource-isolation"
APP_ID = "wio-schema-isolation"
APP_VERSION = "wio-schema-isolation-rung-001"

CASE_MATRIX = {
    "case-001": (7420, "dbos-client-a-after-client-b"),
    "case-002": (7421, "caller-owned-transaction-after-schema-switch"),
    "case-003": (7422, "datasource-a-after-datasource-b"),
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
    schedule: str
    database_prefix: str
    schema_a: str
    schema_b: str
    workflow_a: str
    workflow_b: str
    tx_commit_workflow: str
    tx_rollback_workflow: str
    queue_name: str
    topic: str
    datasource_workflow_a: str
    datasource_workflow_b: str


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


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def admin_url() -> sa.URL:
    raw = os.environ.get(
        "DBOS_POSTGRES_ADMIN_URL",
        "postgresql+psycopg://postgres:dbos@localhost:5432/postgres",
    )
    url = make_url(raw)
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+psycopg")
    return url


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


def psycopg_url(url: str) -> str:
    parsed = make_url(url)
    if parsed.drivername == "postgresql":
        parsed = parsed.set(drivername="postgresql+psycopg")
    return parsed.render_as_string(hide_password=False)


def drop_databases(prefix: str) -> None:
    if os.environ.get("WIO_SCHEMA_ISOLATION_KEEP_DATABASES") == "1":
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


def make_config(plan: CasePlan, app_url: str, sys_url: str, schema: str, suffix: str) -> DBOSConfig:
    return {
        "name": APP_ID,
        "application_database_url": app_url,
        "system_database_url": sys_url,
        "dbos_system_schema": schema,
        "application_version": f"{APP_VERSION}-{plan.case_id}-{suffix}",
        "executor_id": f"wio-schema-{plan.case_id}-{suffix}",
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        "run_admin_server": False,
        "max_executor_threads": 8,
    }


def make_plan(rung_id: str, case_id: str) -> CasePlan:
    aliases = {"rung-001": RUNG_001_ID, RUNG_001_ID: RUNG_001_ID}
    if rung_id not in aliases:
        raise SetupBlock(f"unsupported rung: {rung_id}")
    if case_id not in CASE_MATRIX:
        raise SetupBlock(f"unsupported case: {case_id}")
    seed, schedule = CASE_MATRIX[case_id]
    rng = random.Random(seed)
    token = f"{seed:x}_{case_id.replace('-', '_')}_{rng.randrange(10_000):04d}"
    return CasePlan(
        rung_id=RUNG_001_ID,
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        database_prefix=f"wio_schema_{token}",
        schema_a=f"wio_schema_a_{seed}",
        schema_b=f"wio_schema_b_{seed}",
        workflow_a=f"{FRONTIER_ID}-{case_id}-workflow-a-{seed}",
        workflow_b=f"{FRONTIER_ID}-{case_id}-workflow-b-{seed}",
        tx_commit_workflow=f"{FRONTIER_ID}-{case_id}-tx-commit-{seed}",
        tx_rollback_workflow=f"{FRONTIER_ID}-{case_id}-tx-rollback-{seed}",
        queue_name=f"wio_schema_queue_{seed}",
        topic=f"wio_schema_topic_{seed}",
        datasource_workflow_a=f"{FRONTIER_ID}-{case_id}-ds-a-{seed}",
        datasource_workflow_b=f"{FRONTIER_ID}-{case_id}-ds-b-{seed}",
    )


def scalar_count(engine: sa.Engine, schema: str, table: str, where: str, params: dict[str, Any]) -> int:
    query = sa.text(f"SELECT count(*) FROM {quote_ident(schema)}.{quote_ident(table)} WHERE {where}")
    with engine.connect() as conn:
        return int(conn.execute(query, params).scalar_one())


def row_dicts(engine: sa.Engine, schema: str, table: str, where: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    query = sa.text(f"SELECT * FROM {quote_ident(schema)}.{quote_ident(table)} WHERE {where}")
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().all()
    return [dict(row) for row in rows]


def physical_workflow_counts(engine: sa.Engine, plan: CasePlan, workflow_id: str) -> dict[str, int]:
    return {
        plan.schema_a: scalar_count(
            engine,
            plan.schema_a,
            "workflow_status",
            "workflow_uuid = :workflow_id",
            {"workflow_id": workflow_id},
        ),
        plan.schema_b: scalar_count(
            engine,
            plan.schema_b,
            "workflow_status",
            "workflow_uuid = :workflow_id",
            {"workflow_id": workflow_id},
        ),
    }


def physical_operation_counts(engine: sa.Engine, plan: CasePlan, workflow_id: str) -> dict[str, int]:
    return {
        plan.schema_a: scalar_count(
            engine,
            plan.schema_a,
            "operation_outputs",
            "workflow_uuid = :workflow_id",
            {"workflow_id": workflow_id},
        ),
        plan.schema_b: scalar_count(
            engine,
            plan.schema_b,
            "operation_outputs",
            "workflow_uuid = :workflow_id",
            {"workflow_id": workflow_id},
        ),
    }


def physical_transaction_counts(engine: sa.Engine, plan: CasePlan, workflow_id: str) -> dict[str, int]:
    return {
        plan.schema_a: scalar_count(
            engine,
            plan.schema_a,
            "transaction_outputs",
            "workflow_uuid = :workflow_id",
            {"workflow_id": workflow_id},
        ),
        plan.schema_b: scalar_count(
            engine,
            plan.schema_b,
            "transaction_outputs",
            "workflow_uuid = :workflow_id",
            {"workflow_id": workflow_id},
        ),
    }


def physical_datasource_counts(engine: sa.Engine, plan: CasePlan, workflow_id: str) -> dict[str, int]:
    return {
        plan.schema_a: scalar_count(
            engine,
            plan.schema_a,
            "datasource_outputs",
            "workflow_id = :workflow_id",
            {"workflow_id": workflow_id},
        ),
        plan.schema_b: scalar_count(
            engine,
            plan.schema_b,
            "datasource_outputs",
            "workflow_id = :workflow_id",
            {"workflow_id": workflow_id},
        ),
    }


def destroy_dbos() -> None:
    try:
        DBOS.destroy(destroy_registry=True)
    except Exception as exc:
        event("dbos_destroy_best_effort_failed", error_type=type(exc).__name__, error=str(exc))


def launch_schema_runtime(
    plan: CasePlan,
    app_url: str,
    sys_url: str,
    schema: str,
    suffix: str,
    body: Callable[[], Any],
) -> Any:
    destroy_dbos()
    DBOS(config=make_config(plan, app_url, sys_url, schema, suffix))
    DBOS.launch()
    try:
        return body()
    finally:
        destroy_dbos()


def create_runtime_workflow_row(
    plan: CasePlan,
    app_url: str,
    sys_url: str,
    schema: str,
    suffix: str,
    workflow_id: str,
) -> str:
    def body() -> str:
        def step_body(label: str) -> str:
            return f"step:{label}:{workflow_id}"

        @DBOS.transaction(name=f"schema_tx_{suffix}")
        def schema_transaction(label: str) -> str:
            return f"tx:{label}:{workflow_id}"

        @DBOS.workflow(name=f"schema_workflow_{suffix}")
        def schema_workflow(label: str) -> str:
            step_result = DBOS.run_step({"name": f"schema_step_{suffix}"}, step_body, label)
            tx_result = schema_transaction(label)
            return f"{step_result}|{tx_result}"

        with SetWorkflowID(workflow_id):
            result = schema_workflow(suffix)
        expected = f"step:{suffix}:{workflow_id}|tx:{suffix}:{workflow_id}"
        invariant(
            f"{plan.case_id}_runtime_{suffix}_workflow_result",
            result == expected,
            observed=result,
            expected=expected,
        )
        return result

    return launch_schema_runtime(plan, app_url, sys_url, schema, suffix, body)


def migrate_schema_pair(plan: CasePlan, app_url: str, sys_url: str) -> None:
    create_runtime_workflow_row(plan, app_url, sys_url, plan.schema_a, "a", plan.workflow_a)
    create_runtime_workflow_row(plan, app_url, sys_url, plan.schema_b, "b", plan.workflow_b)


def public_ids(rows: list[Any]) -> list[str]:
    ids = []
    for row in rows:
        row_id = getattr(row, "workflow_id", None) or getattr(row, "workflow_uuid", None)
        if row_id is None and isinstance(row, dict):
            row_id = row.get("workflow_id") or row.get("workflow_uuid")
        ids.append(row_id)
    return sorted(str(value) for value in ids if value is not None)


def assert_schema_exclusive_counts(
    name: str,
    counts: dict[str, int],
    expected_schema: str,
    other_schema: str,
) -> None:
    invariant(
        name,
        counts.get(expected_schema) == 1 and counts.get(other_schema) == 0,
        counts=counts,
        expected_schema=expected_schema,
        other_schema=other_schema,
    )


def run_case_001(plan: CasePlan, app_url: str, sys_url: str, artifacts: Path) -> dict[str, Any]:
    migrate_schema_pair(plan, app_url, sys_url)
    sys_engine = sa.create_engine(sys_url)
    app_engine = sa.create_engine(psycopg_url(app_url))
    client_a: DBOSClient | None = None
    client_b: DBOSClient | None = None
    try:
        client_a = DBOSClient(
            system_database_url=sys_url,
            application_database_url=app_url,
            dbos_system_schema=plan.schema_a,
        )
        client_b = DBOSClient(
            system_database_url=sys_url,
            application_database_url=app_url,
            dbos_system_schema=plan.schema_b,
        )
        public_a_for_a = public_ids(client_a.list_workflows(workflow_ids=[plan.workflow_a]))
        public_a_for_b = public_ids(client_a.list_workflows(workflow_ids=[plan.workflow_b]))
        public_b_for_a = public_ids(client_b.list_workflows(workflow_ids=[plan.workflow_a]))
        public_b_for_b = public_ids(client_b.list_workflows(workflow_ids=[plan.workflow_b]))
        steps_a = client_a.list_workflow_steps(plan.workflow_a)
        steps_b = client_b.list_workflow_steps(plan.workflow_b)
        observations = {
            "public_a_for_a": public_a_for_a,
            "public_a_for_b": public_a_for_b,
            "public_b_for_a": public_b_for_a,
            "public_b_for_b": public_b_for_b,
            "steps_a": [dict(step) for step in steps_a],
            "steps_b": [dict(step) for step in steps_b],
            "physical_workflow_a": physical_workflow_counts(sys_engine, plan, plan.workflow_a),
            "physical_workflow_b": physical_workflow_counts(sys_engine, plan, plan.workflow_b),
            "physical_operation_a": physical_operation_counts(sys_engine, plan, plan.workflow_a),
            "physical_operation_b": physical_operation_counts(sys_engine, plan, plan.workflow_b),
            "physical_transaction_a": physical_transaction_counts(app_engine, plan, plan.workflow_a),
            "physical_transaction_b": physical_transaction_counts(app_engine, plan, plan.workflow_b),
        }
        write_json(artifacts / "observations.json", observations)

        assert_schema_exclusive_counts(
            f"{plan.case_id}_workflow_a_physical_schema",
            observations["physical_workflow_a"],
            plan.schema_a,
            plan.schema_b,
        )
        assert_schema_exclusive_counts(
            f"{plan.case_id}_workflow_b_physical_schema",
            observations["physical_workflow_b"],
            plan.schema_b,
            plan.schema_a,
        )
        invariant(
            f"{plan.case_id}_client_a_public_isolation",
            public_a_for_a == [plan.workflow_a] and public_a_for_b == [],
            public_a_for_a=public_a_for_a,
            public_a_for_b=public_a_for_b,
        )
        invariant(
            f"{plan.case_id}_client_b_public_isolation",
            public_b_for_b == [plan.workflow_b] and public_b_for_a == [],
            public_b_for_b=public_b_for_b,
            public_b_for_a=public_b_for_a,
        )
        invariant(
            f"{plan.case_id}_client_a_steps_are_schema_a",
            len(steps_a) >= 2 and all(str(step["function_name"]).startswith(("schema_step_a", "schema_tx_a")) for step in steps_a),
            steps=[dict(step) for step in steps_a],
        )
        invariant(
            f"{plan.case_id}_client_b_steps_are_schema_b",
            len(steps_b) >= 2 and all(str(step["function_name"]).startswith(("schema_step_b", "schema_tx_b")) for step in steps_b),
            steps=[dict(step) for step in steps_b],
        )
        assert_schema_exclusive_counts(
            f"{plan.case_id}_operation_a_physical_schema",
            observations["physical_operation_a"],
            plan.schema_a,
            plan.schema_b,
        )
        assert_schema_exclusive_counts(
            f"{plan.case_id}_operation_b_physical_schema",
            observations["physical_operation_b"],
            plan.schema_b,
            plan.schema_a,
        )
        assert_schema_exclusive_counts(
            f"{plan.case_id}_transaction_a_physical_schema",
            observations["physical_transaction_a"],
            plan.schema_a,
            plan.schema_b,
        )
        assert_schema_exclusive_counts(
            f"{plan.case_id}_transaction_b_physical_schema",
            observations["physical_transaction_b"],
            plan.schema_b,
            plan.schema_a,
        )
        return observations
    finally:
        if client_a is not None:
            client_a.destroy()
        if client_b is not None:
            client_b.destroy()
        sys_engine.dispose()
        app_engine.dispose()


def run_case_002(plan: CasePlan, app_url: str, sys_url: str, artifacts: Path) -> dict[str, Any]:
    migrate_schema_pair(plan, app_url, sys_url)
    sys_engine = sa.create_engine(sys_url)
    client_a: DBOSClient | None = None
    client_b: DBOSClient | None = None
    try:
        client_a = DBOSClient(
            system_database_url=sys_url,
            application_database_url=app_url,
            dbos_system_schema=plan.schema_a,
        )
        client_b = DBOSClient(
            system_database_url=sys_url,
            application_database_url=app_url,
            dbos_system_schema=plan.schema_b,
        )
        commit_options = {
            "queue_name": plan.queue_name,
            "workflow_name": "schema_transaction_target",
            "workflow_id": plan.tx_commit_workflow,
        }
        rollback_options = {
            "queue_name": plan.queue_name,
            "workflow_name": "schema_transaction_target",
            "workflow_id": plan.tx_rollback_workflow,
        }

        with Session(client_a._sys_db.engine) as session:
            session.execute(sa.text("SELECT 1"))
            handle = client_a.enqueue_in_transaction(session, commit_options, "commit")
            invariant(
                f"{plan.case_id}_commit_handle_id",
                handle.get_workflow_id() == plan.tx_commit_workflow,
                observed=handle.get_workflow_id(),
                expected=plan.tx_commit_workflow,
            )
            precommit = physical_workflow_counts(sys_engine, plan, plan.tx_commit_workflow)
            invariant(
                f"{plan.case_id}_commit_precommit_invisible",
                precommit[plan.schema_a] == 0 and precommit[plan.schema_b] == 0,
                precommit=precommit,
            )
            session.commit()

        with Session(client_a._sys_db.engine) as session:
            session.execute(sa.text("SELECT 1"))
            client_a.enqueue_in_transaction(session, rollback_options, "rollback")
            session.rollback()

        public_a_commit = public_ids(client_a.list_workflows(workflow_ids=[plan.tx_commit_workflow]))
        public_b_commit = public_ids(client_b.list_workflows(workflow_ids=[plan.tx_commit_workflow]))
        commit_counts = physical_workflow_counts(sys_engine, plan, plan.tx_commit_workflow)
        rollback_counts = physical_workflow_counts(sys_engine, plan, plan.tx_rollback_workflow)
        observations = {
            "public_a_commit": public_a_commit,
            "public_b_commit": public_b_commit,
            "commit_counts": commit_counts,
            "rollback_counts": rollback_counts,
            "commit_rows_a": row_dicts(sys_engine, plan.schema_a, "workflow_status", "workflow_uuid = :workflow_id", {"workflow_id": plan.tx_commit_workflow}),
            "commit_rows_b": row_dicts(sys_engine, plan.schema_b, "workflow_status", "workflow_uuid = :workflow_id", {"workflow_id": plan.tx_commit_workflow}),
        }
        write_json(artifacts / "observations.json", observations)

        assert_schema_exclusive_counts(
            f"{plan.case_id}_commit_schema_a_only",
            commit_counts,
            plan.schema_a,
            plan.schema_b,
        )
        invariant(
            f"{plan.case_id}_rollback_left_no_rows",
            rollback_counts[plan.schema_a] == 0 and rollback_counts[plan.schema_b] == 0,
            rollback_counts=rollback_counts,
        )
        invariant(
            f"{plan.case_id}_client_a_public_commit_visible",
            public_a_commit == [plan.tx_commit_workflow],
            public_a_commit=public_a_commit,
        )
        invariant(
            f"{plan.case_id}_client_b_public_commit_isolated",
            public_b_commit == [],
            public_b_commit=public_b_commit,
        )
        return observations
    finally:
        if client_a is not None:
            client_a.destroy()
        if client_b is not None:
            client_b.destroy()
        sys_engine.dispose()


def run_datasource_workflow(
    plan: CasePlan,
    app_url: str,
    sys_url: str,
    runtime_schema: str,
    suffix: str,
    workflow_id: str,
    target_first: bool,
) -> str:
    def body() -> str:
        if target_first:
            target_ds = SQLAlchemyDatasource.create(app_url, schema=runtime_schema)
            other_schema = plan.schema_b if runtime_schema == plan.schema_a else plan.schema_a
            SQLAlchemyDatasource.create(app_url, schema=other_schema)
        else:
            other_schema = plan.schema_b if runtime_schema == plan.schema_a else plan.schema_a
            SQLAlchemyDatasource.create(app_url, schema=other_schema)
            target_ds = SQLAlchemyDatasource.create(app_url, schema=runtime_schema)

        def ds_step() -> str:
            return f"ds:{suffix}:{workflow_id}"

        @DBOS.workflow(name=f"schema_ds_workflow_{suffix}")
        def ds_workflow() -> str:
            return target_ds.run_tx_step({"name": f"schema_ds_step_{suffix}"}, ds_step)

        with SetWorkflowID(workflow_id):
            result = ds_workflow()
        expected = f"ds:{suffix}:{workflow_id}"
        invariant(
            f"{plan.case_id}_datasource_{suffix}_workflow_result",
            result == expected,
            observed=result,
            expected=expected,
        )
        return result

    return launch_schema_runtime(plan, app_url, sys_url, runtime_schema, f"ds-{suffix}", body)


def run_case_003(plan: CasePlan, app_url: str, sys_url: str, artifacts: Path) -> dict[str, Any]:
    migrate_schema_pair(plan, app_url, sys_url)
    run_datasource_workflow(
        plan,
        app_url,
        sys_url,
        plan.schema_a,
        "a_after_b",
        plan.datasource_workflow_a,
        target_first=True,
    )
    run_datasource_workflow(
        plan,
        app_url,
        sys_url,
        plan.schema_b,
        "b_after_a",
        plan.datasource_workflow_b,
        target_first=True,
    )
    sys_engine = sa.create_engine(sys_url)
    app_engine = sa.create_engine(psycopg_url(app_url))
    try:
        observations = {
            "datasource_a": physical_datasource_counts(app_engine, plan, plan.datasource_workflow_a),
            "datasource_b": physical_datasource_counts(app_engine, plan, plan.datasource_workflow_b),
            "operation_a": physical_operation_counts(sys_engine, plan, plan.datasource_workflow_a),
            "operation_b": physical_operation_counts(sys_engine, plan, plan.datasource_workflow_b),
            "workflow_a": physical_workflow_counts(sys_engine, plan, plan.datasource_workflow_a),
            "workflow_b": physical_workflow_counts(sys_engine, plan, plan.datasource_workflow_b),
        }
        write_json(artifacts / "observations.json", observations)
        assert_schema_exclusive_counts(
            f"{plan.case_id}_datasource_a_physical_schema",
            observations["datasource_a"],
            plan.schema_a,
            plan.schema_b,
        )
        assert_schema_exclusive_counts(
            f"{plan.case_id}_datasource_b_physical_schema",
            observations["datasource_b"],
            plan.schema_b,
            plan.schema_a,
        )
        assert_schema_exclusive_counts(
            f"{plan.case_id}_operation_a_physical_schema",
            observations["operation_a"],
            plan.schema_a,
            plan.schema_b,
        )
        assert_schema_exclusive_counts(
            f"{plan.case_id}_operation_b_physical_schema",
            observations["operation_b"],
            plan.schema_b,
            plan.schema_a,
        )
        return observations
    finally:
        sys_engine.dispose()
        app_engine.dispose()


def run_case(plan: CasePlan, artifacts_root: Path) -> dict[str, Any]:
    artifacts = artifacts_root / plan.case_id
    artifacts.mkdir(parents=True, exist_ok=True)
    write_json(artifacts / "plan.json", asdict(plan))
    app_url, sys_url, masked_admin = prepare_databases(plan.database_prefix, artifacts)
    event(
        "case_start",
        frontier=FRONTIER_ID,
        rung=plan.rung_id,
        case_id=plan.case_id,
        seed=plan.seed,
        schedule=plan.schedule,
        schema_a=plan.schema_a,
        schema_b=plan.schema_b,
        admin_url=masked_admin,
    )
    try:
        if plan.case_id == "case-001":
            observations = run_case_001(plan, app_url, sys_url, artifacts)
        elif plan.case_id == "case-002":
            observations = run_case_002(plan, app_url, sys_url, artifacts)
        elif plan.case_id == "case-003":
            observations = run_case_003(plan, app_url, sys_url, artifacts)
        else:
            raise SetupBlock(f"unsupported case: {plan.case_id}")
        event("case_complete", case_id=plan.case_id, seed=plan.seed)
        return observations
    finally:
        destroy_dbos()
        drop_databases(plan.database_prefix)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rung", default=RUNG_001_ID)
    parser.add_argument("--case", dest="case_id", choices=sorted(CASE_MATRIX))
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true", help="accepted for executor parity")
    parser.add_argument(
        "--artifact-dir",
        default="/tmp/wio-artifacts/schema-isolation-multi-client",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.all_cases:
        case_ids = sorted(CASE_MATRIX)
    elif args.case_id:
        case_ids = [args.case_id]
    else:
        raise SetupBlock("choose --case <case-id> or --all-cases")

    artifacts_root = Path(args.artifact_dir)
    artifacts_root.mkdir(parents=True, exist_ok=True)
    run_summary: list[dict[str, Any]] = []
    for case_id in case_ids:
        plan = make_plan(args.rung, case_id)
        observations = run_case(plan, artifacts_root)
        run_summary.append({"plan": asdict(plan), "observations": observations})

    write_json(artifacts_root / "summary.json", run_summary)
    event("workload_complete", cases=case_ids, artifacts=str(artifacts_root))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        raise SystemExit(44)
    except WorkloadFailure as exc:
        print(f"WORKLOAD-FAIL {exc}", flush=True)
        raise SystemExit(10)
