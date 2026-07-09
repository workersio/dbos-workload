#!/usr/bin/env python3
"""Fresh WIO workload for DBOS workflow attribute query behavior.

Frontier: workflow-attributes-query
Rungs:
  - rung-000-attribute-smoke
  - rung-001-attribute-query-postgres
  - rung-002-replay-fork-attribute-history
  - rung-003-cross-backend-negative-contract
  - rung-004-bounded-seed-sweep
  - rung-005-scheduled-workflow-identity-query
Evidence key:
  evidence-key:events/frontier_designer-20260620T072711428752000Z.prompt.md
Protected product promise:
  Workflow attributes are queryable, mutable, and visible through public/client
  APIs consistently across creation, update, lifecycle state, and Postgres
  filtering.
Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py \
    --rung rung-001-attribute-query-postgres --case case-001
Seed policy:
  Exact rung seeds are encoded below; each case writes its derived case JSON and
  observed query results under the artifact directory.
Invariant oracle:
  Independent expected workflow-id sets and latest attribute dictionaries must
  match public DBOS/DBOSClient list results exactly.
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
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
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

    from dbos import (
        DBOS,
        DBOSClient,
        DBOSConfig,
        Queue,
        SetEnqueueOptions,
        SetWorkflowAttributes,
        SetWorkflowID,
    )
    from dbos._error import DBOSException
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "workflow-attributes-query"
RUNG_000_ID = "rung-000-attribute-smoke"
RUNG_001_ID = "rung-001-attribute-query-postgres"
RUNG_002_ID = "rung-002-replay-fork-attribute-history"
RUNG_003_ID = "rung-003-cross-backend-negative-contract"
RUNG_004_ID = "rung-004-bounded-seed-sweep"
RUNG_005_ID = "rung-005-scheduled-workflow-identity-query"
RUNG_006_ID = "rung-006-legacy-scheduler-latest-app-version"
RUNG_007_ID = "rung-007-temporal-introspection-windows"
APP_ID = "wio-workflow-attributes"
APP_VERSION = "wio-workflow-attributes-rungs-000-007"
PROMPT_PATH = "evidence-key:events/frontier_designer-20260620T072711428752000Z.prompt.md"
TEMPORAL_RELEASE_EVENTS: dict[str, threading.Event] = {}


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
    admin = str(base.set(database=base.database or "postgres"))
    masked = str(base.set(password="***" if base.password else None))
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
        str(base.set(drivername="postgresql", database=app_db)),
        str(base.set(drivername="postgresql+psycopg", database=sys_db)),
        masked,
    )


def drop_databases(prefix: str) -> None:
    if os.environ.get("WIO_ATTRIBUTES_KEEP_DATABASES") == "1":
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
        "executor_id": f"wio-attributes-{plan.case_id}",
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        "run_admin_server": False,
        "runtimeConfig": {"max_executor_threads": 16},
    }


def normalize_rung(rung: str) -> str:
    aliases = {
        "rung-000": RUNG_000_ID,
        RUNG_000_ID: RUNG_000_ID,
        "rung-001": RUNG_001_ID,
        RUNG_001_ID: RUNG_001_ID,
        "rung-002": RUNG_002_ID,
        RUNG_002_ID: RUNG_002_ID,
        "rung-003": RUNG_003_ID,
        RUNG_003_ID: RUNG_003_ID,
        "rung-004": RUNG_004_ID,
        RUNG_004_ID: RUNG_004_ID,
        "rung-005": RUNG_005_ID,
        RUNG_005_ID: RUNG_005_ID,
        "rung-006": RUNG_006_ID,
        RUNG_006_ID: RUNG_006_ID,
        "rung-007": RUNG_007_ID,
        RUNG_007_ID: RUNG_007_ID,
    }
    if rung not in aliases:
        raise SetupBlock(f"unsupported rung {rung}")
    return aliases[rung]


def case_ids_for_rung(rung_id: str) -> list[str]:
    if rung_id == RUNG_000_ID:
        return ["case-001"]
    if rung_id == RUNG_001_ID:
        return ["case-001", "case-002", "case-003"]
    if rung_id == RUNG_002_ID:
        return ["case-001", "case-002", "case-003"]
    if rung_id == RUNG_003_ID:
        return ["case-001", "case-002", "case-003"]
    if rung_id == RUNG_004_ID:
        return [f"case-{i:03d}" for i in range(1, 25)]
    if rung_id == RUNG_005_ID:
        return ["case-001", "case-002", "case-003"]
    if rung_id == RUNG_006_ID:
        return ["case-001", "case-002", "case-003"]
    if rung_id == RUNG_007_ID:
        return ["case-001", "case-002", "case-003", "case-004"]
    raise SetupBlock(f"unsupported rung {rung_id}")


def make_plan(rung: str, case_id: str, seed_override: int | None = None) -> CasePlan:
    rung_id = normalize_rung(rung)
    if case_id not in case_ids_for_rung(rung_id):
        raise SetupBlock(f"unknown case {case_id} for {rung_id}")

    specs = {
        (RUNG_000_ID, "case-001"): (
            3800,
            "create-workflow-with-attributes-update-query",
            "attribute create/list/update/query APIs run",
        ),
        (RUNG_001_ID, "case-001"): (
            3810,
            "create-workflows-with-string-int-bool-attrs",
            "Postgres equality and type predicates return exact modeled IDs",
        ),
        (RUNG_001_ID, "case-002"): (
            3811,
            "replace-dict-then-query-old-and-new-keys",
            "attribute replacement removes stale keys",
        ),
        (RUNG_001_ID, "case-003"): (
            3812,
            "mix-success-error-enqueued-workflows-with-attrs",
            "attribute predicate composes with lifecycle status filters",
        ),
        (RUNG_002_ID, "case-001"): (
            3820,
            "recover-replay-workflow-that-updates-attrs",
            "attribute update inside workflow is checkpointed once",
        ),
        (RUNG_002_ID, "case-002"): (
            3821,
            "update-attrs-before-and-after-fork",
            "fork inherits modeled attributes at fork point",
        ),
        (RUNG_002_ID, "case-003"): (
            3822,
            "client-updates-attrs-while-workflow-blocked-resumed",
            "client update during lifecycle transition is visible once",
        ),
        (RUNG_003_ID, "case-001"): (
            3830,
            "run-same-attr-filter-against-sqlite-if-configured",
            "SQLite unsupported filtering is explicit",
        ),
        (RUNG_003_ID, "case-002"): (
            3831,
            "compare-postgres-expected-set-to-sqlite-behavior",
            "Postgres path remains authoritative",
        ),
        (RUNG_003_ID, "case-003"): (
            3832,
            "request-unsupported-predicate",
            "unsupported backend does not return silently wrong set",
        ),
        (RUNG_005_ID, "case-001"): (
            3820,
            "trigger-two-schedules-sharing-one-workflow",
            "schedule_name distinguishes trigger rows from manual rows with the same workflow function",
        ),
        (RUNG_005_ID, "case-002"): (
            3821,
            "trigger-backfill-compose-name-status-queue-filters",
            "schedule identity is populated across trigger and backfill and composes with existing filters",
        ),
        (RUNG_005_ID, "case-003"): (
            3822,
            "export-delete-import-preserves-schedule-name",
            "schedule identity is persisted workflow state, not derived from the schedule table",
        ),
        (RUNG_006_ID, "case-001"): (
            6990,
            "sync-decorator-latest-version",
            "legacy sync scheduled rows use the modeled latest application version",
        ),
        (RUNG_006_ID, "case-002"): (
            6991,
            "async-decorator-version-rollover",
            "legacy async scheduled rows use the version that was latest when enqueued",
        ),
        (RUNG_006_ID, "case-003"): (
            6992,
            "relaunch-scheduled-version-parity",
            "legacy scheduled app-version metadata survives relaunch and client/runtime queries",
        ),
        (RUNG_007_ID, "case-001"): (
            6810,
            "success-error-cancel-resume-completed-windows",
            "completed_at transitions and runtime/client filters match the temporal ledger",
        ),
        (RUNG_007_ID, "case-002"): (
            6811,
            "queued-delayed-direct-dequeue-windows",
            "dequeued_at windows include only started queued workflows and never direct workflows",
        ),
        (RUNG_007_ID, "case-003"): (
            6812,
            "relaunch-export-import-temporal-preservation",
            "workflow temporal fields survive relaunch and export/delete/import",
        ),
        (RUNG_007_ID, "case-004"): (
            6813,
            "step-timing-workflow-aggregate-buckets",
            "workflow and operation-output timing aggregates match the modeled rows",
        ),
    }
    if rung_id == RUNG_004_ID:
        case_number = int(case_id.split("-")[1])
        variants = [
            "predicate-equality",
            "predicate-type",
            "latest-replace",
            "status-compose",
            "fork-history",
            "backend-negative",
        ]
        variant = variants[(case_number - 1) % len(variants)]
        specs[(rung_id, case_id)] = (
            3839 + case_number,
            f"generate-bounded-{variant}-variant-from-seed",
            f"{variant} preserves the frontier oracle",
        )
    seed, schedule, focus = specs[(rung_id, case_id)]
    if seed_override is not None:
        seed = seed_override
    suffix = hashlib.sha1(f"{rung_id}:{case_id}:{seed}".encode()).hexdigest()[:8]
    return CasePlan(
        rung_id=rung_id,
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        focus=focus,
        database_prefix=f"wio_attr_{seed}_{case_id.replace('-', '_')}_{suffix}",
    )


@DBOS.workflow()
def attr_success_workflow(label: str) -> str:
    return label


@DBOS.workflow()
def attr_error_workflow(label: str) -> str:
    raise RuntimeError(f"modeled attribute error {label}")


@DBOS.workflow()
def attr_update_inside_workflow(label: str, attrs: dict[str, Any]) -> str:
    if DBOS.workflow_id is None:
        raise RuntimeError("workflow id missing inside attribute update workflow")
    DBOS.update_workflow_attributes(DBOS.workflow_id, attrs)
    return label


@DBOS.step()
def attr_marker_step(label: str) -> str:
    return label


@DBOS.workflow()
def attr_forkable_history_workflow(label: str) -> str:
    first = attr_marker_step(f"{label}-first")
    second = attr_marker_step(f"{label}-second")
    return f"{first}:{second}"


@DBOS.workflow()
def schedule_identity_workflow(scheduled_at: datetime, ctx: Any) -> dict[str, Any]:
    return {
        "kind": "schedule_identity",
        "workflow_id": DBOS.workflow_id,
        "scheduled_at": scheduled_at.isoformat(),
        "ctx": ctx,
    }


@DBOS.scheduled("* * * * * *")
@DBOS.workflow()
def legacy_sync_scheduled_version_workflow(scheduled_at: datetime, actual_at: datetime) -> dict[str, Any]:
    return {
        "kind": "legacy_sync",
        "workflow_id": DBOS.workflow_id,
        "scheduled_at": scheduled_at.isoformat(),
        "actual_at": actual_at.isoformat(),
    }


@DBOS.scheduled("* * * * * *")
@DBOS.workflow()
async def legacy_async_scheduled_version_workflow(scheduled_at: datetime, actual_at: datetime) -> dict[str, Any]:
    return {
        "kind": "legacy_async",
        "workflow_id": DBOS.workflow_id,
        "scheduled_at": scheduled_at.isoformat(),
        "actual_at": actual_at.isoformat(),
    }


@DBOS.scheduled("* * * * * *")
@DBOS.workflow()
def legacy_relaunch_scheduled_version_workflow(scheduled_at: datetime, actual_at: datetime) -> dict[str, Any]:
    return {
        "kind": "legacy_relaunch",
        "workflow_id": DBOS.workflow_id,
        "scheduled_at": scheduled_at.isoformat(),
        "actual_at": actual_at.isoformat(),
    }


@DBOS.workflow()
def temporal_success_workflow(label: str) -> str:
    return label


@DBOS.workflow()
def temporal_error_workflow(label: str) -> str:
    raise RuntimeError(f"modeled temporal error {label}")


@DBOS.workflow()
def temporal_blocking_workflow(event_key: str, label: str) -> str:
    release = TEMPORAL_RELEASE_EVENTS[event_key]
    if not release.wait(timeout=30):
        raise RuntimeError(f"temporal release event timed out for {event_key}")
    return label


@DBOS.workflow()
def temporal_child_workflow(label: str) -> str:
    return f"child:{label}"


@DBOS.step()
def temporal_quick_step(label: str) -> str:
    return f"quick:{label}"


@DBOS.step()
def temporal_slow_step(label: str, sleep_ms: int) -> str:
    time.sleep(sleep_ms / 1000)
    return f"slow:{label}"


@DBOS.workflow()
def temporal_direct_aggregate_workflow(label: str, sleep_ms: int) -> str:
    child = temporal_child_workflow(label)
    quick = temporal_quick_step(label)
    slow = temporal_slow_step(label, sleep_ms)
    return f"direct:{child}:{quick}:{slow}"


@DBOS.workflow()
def temporal_queued_aggregate_workflow(label: str, sleep_ms: int) -> str:
    quick = temporal_quick_step(label)
    slow = temporal_slow_step(label, sleep_ms)
    return f"queued:{quick}:{slow}"


def workflow_name(fn: Any) -> str:
    return getattr(fn, "dbos_function_name", fn.__qualname__)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def iso_from_epoch_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


def time_bucket(epoch_ms: int, bucket_ms: int) -> int:
    return (epoch_ms // bucket_ms) * bucket_ms


def row_to_dict(row: Any) -> dict[str, Any]:
    return {
        "workflow_id": row.workflow_id,
        "status": row.status,
        "name": row.name,
        "queue_name": row.queue_name,
        "created_at": row.created_at,
        "dequeued_at": row.dequeued_at,
        "delay_until_epoch_ms": row.delay_until_epoch_ms,
        "completed_at": row.completed_at,
        "attributes": row.attributes,
        "app_version": row.app_version,
    }


def status_obj(workflow_id: str) -> Any:
    status = DBOS.get_workflow_status(workflow_id)
    if status is None:
        raise WorkloadFailure(f"workflow {workflow_id} missing status")
    return status


def temporal_status_map(workflow_ids: list[str]) -> dict[str, dict[str, Any]]:
    return {workflow_id: row_to_dict(status_obj(workflow_id)) for workflow_id in workflow_ids}


def wait_for_status(
    workflow_id: str,
    predicate: Any,
    *,
    timeout: float = 8.0,
    interval: float = 0.05,
) -> Any:
    deadline = time.time() + timeout
    last_status: Any = None
    while time.time() < deadline:
        last_status = DBOS.get_workflow_status(workflow_id)
        if last_status is not None and predicate(last_status):
            return last_status
        time.sleep(interval)
    raise WorkloadFailure(
        f"timed out waiting for status predicate on {workflow_id}: {row_to_dict(last_status) if last_status else None}"
    )


def assert_temporal_filter(
    label: str,
    *,
    workflow_ids: list[str],
    expected_ids: set[str],
    client: DBOSClient,
    queued: bool = False,
    **filters: Any,
) -> dict[str, Any]:
    list_fn = DBOS.list_queued_workflows if queued else DBOS.list_workflows
    client_list_fn = client.list_queued_workflows if queued else client.list_workflows
    runtime_rows = list_fn(
        workflow_ids=workflow_ids,
        load_input=False,
        load_output=False,
        **filters,
    )
    client_rows = client_list_fn(
        workflow_ids=workflow_ids,
        load_input=False,
        load_output=False,
        **filters,
    )
    runtime_ids = ids_for(runtime_rows)
    client_ids = ids_for(client_rows)
    invariant(
        label,
        runtime_ids == expected_ids and client_ids == expected_ids,
        filters=filters,
        queued=queued,
        expected=sorted_ids(expected_ids),
        runtime=sorted_ids(runtime_ids),
        client=sorted_ids(client_ids),
        runtime_rows=[row_to_dict(row) for row in runtime_rows],
        client_rows=[row_to_dict(row) for row in client_rows],
    )
    return {
        "filters": filters,
        "queued": queued,
        "expected": sorted_ids(expected_ids),
        "runtime": sorted_ids(runtime_ids),
        "client": sorted_ids(client_ids),
    }


def status_map(workflow_ids: list[str]) -> dict[str, dict[str, Any]]:
    rows = DBOS.list_workflows(workflow_ids=workflow_ids, load_input=False, load_output=False)
    return {
        row.workflow_id: {
            "status": row.status,
            "name": row.name,
            "attributes": row.attributes,
            "queue_name": row.queue_name,
            "schedule_name": row.schedule_name,
            "app_version": row.app_version,
        }
        for row in rows
    }


def matched_ids(**kwargs: Any) -> set[str]:
    return {
        row.workflow_id
        for row in DBOS.list_workflows(load_input=False, load_output=False, **kwargs)
    }


def sorted_ids(values: set[str]) -> list[str]:
    return sorted(values)


def status_summary(rows: list[Any]) -> dict[str, dict[str, Any]]:
    return {
        row.workflow_id: {
            "status": row.status,
            "name": row.name,
            "queue_name": row.queue_name,
            "attributes": row.attributes,
            "schedule_name": row.schedule_name,
            "app_version": row.app_version,
        }
        for row in rows
    }


def ids_for(rows: list[Any]) -> set[str]:
    return {row.workflow_id for row in rows}


def assert_status_origin_model(
    label: str,
    *,
    workflow_ids: list[str],
    origin_model: dict[str, str | None],
    client: DBOSClient,
) -> dict[str, Any]:
    runtime_rows = DBOS.list_workflows(
        workflow_ids=workflow_ids,
        load_input=False,
        load_output=False,
    )
    client_rows = client.list_workflows(
        workflow_ids=workflow_ids,
        load_input=False,
        load_output=False,
    )
    runtime_by_id = {row.workflow_id: row for row in runtime_rows}
    client_by_id = {row.workflow_id: row for row in client_rows}
    runtime_origins = {
        workflow_id: runtime_by_id[workflow_id].schedule_name
        for workflow_id in workflow_ids
        if workflow_id in runtime_by_id
    }
    client_origins = {
        workflow_id: client_by_id[workflow_id].schedule_name
        for workflow_id in workflow_ids
        if workflow_id in client_by_id
    }
    status_origins = {
        workflow_id: (DBOS.get_workflow_status(workflow_id).schedule_name if DBOS.get_workflow_status(workflow_id) else None)
        for workflow_id in workflow_ids
    }
    invariant(
        label,
        set(runtime_by_id) == set(workflow_ids)
        and set(client_by_id) == set(workflow_ids)
        and runtime_origins == origin_model
        and client_origins == origin_model
        and status_origins == origin_model,
        workflow_ids=workflow_ids,
        origin_model=origin_model,
        runtime=runtime_origins,
        client=client_origins,
        status=status_origins,
        runtime_rows=status_summary(runtime_rows),
        client_rows=status_summary(client_rows),
    )
    return {
        "runtime_rows": status_summary(runtime_rows),
        "client_rows": status_summary(client_rows),
        "status_origins": status_origins,
    }


def assert_schedule_filter(
    label: str,
    *,
    expected_ids: set[str],
    client: DBOSClient,
    schedule_name: str | list[str] | None = None,
    **filters: Any,
) -> dict[str, list[str]]:
    runtime_ids = ids_for(
        DBOS.list_workflows(
            schedule_name=schedule_name,
            load_input=False,
            load_output=False,
            **filters,
        )
    )
    client_ids = ids_for(
        client.list_workflows(
            schedule_name=schedule_name,
            load_input=False,
            load_output=False,
            **filters,
        )
    )
    invariant(
        label,
        runtime_ids == expected_ids and client_ids == expected_ids,
        schedule_name=schedule_name,
        filters=filters,
        expected=sorted_ids(expected_ids),
        runtime=sorted_ids(runtime_ids),
        client=sorted_ids(client_ids),
    )
    return {
        "expected": sorted_ids(expected_ids),
        "runtime": sorted_ids(runtime_ids),
        "client": sorted_ids(client_ids),
    }


def start_success(workflow_id: str, attrs: dict[str, Any], label: str) -> str:
    with SetWorkflowAttributes(attrs):
        with SetWorkflowID(workflow_id):
            result = attr_success_workflow(label)
    invariant(
        "workflow_public_result_matches_label",
        result == label,
        workflow_id=workflow_id,
        result=result,
        expected=label,
    )
    return workflow_id


def start_error(workflow_id: str, attrs: dict[str, Any], label: str) -> str:
    with SetWorkflowAttributes(attrs):
        with SetWorkflowID(workflow_id):
            try:
                attr_error_workflow(label)
            except RuntimeError as exc:
                event("modeled_error_workflow_raised", workflow_id=workflow_id, error=str(exc))
                return workflow_id
    raise WorkloadFailure("error workflow unexpectedly returned success")


def exact_status(workflow_id: str) -> dict[str, Any]:
    rows = status_map([workflow_id])
    if workflow_id not in rows:
        raise WorkloadFailure(f"workflow {workflow_id} missing from status map")
    return rows[workflow_id]


def assert_sqlite_attribute_filter_rejected(label: str, fn: Any) -> dict[str, str]:
    try:
        fn()
    except DBOSException as exc:
        message = str(exc)
        invariant(
            label,
            "not supported on SQLite" in message
            and "Postgres system database" in message,
            error=message,
        )
        return {"error_type": type(exc).__name__, "message": message}
    raise WorkloadFailure(f"{label} unexpectedly returned a result set")


def sqlite_urls(plan: CasePlan, artifacts: Path) -> tuple[str, str]:
    db_dir = artifacts / "sqlite"
    db_dir.mkdir(parents=True, exist_ok=True)
    return (
        f"sqlite:///{db_dir / (plan.database_prefix + '_app.sqlite')}",
        f"sqlite:///{db_dir / (plan.database_prefix + '_sys.sqlite')}",
    )


def run_sqlite_attribute_case(plan: CasePlan, artifacts: Path) -> dict[str, Any]:
    app_url, sys_url = sqlite_urls(plan, artifacts)
    dbos: DBOS | None = None
    client: DBOSClient | None = None
    try:
        DBOS.destroy(destroy_registry=False)
        dbos = DBOS(config=make_config(plan, app_url, sys_url))
        dbos.launch()
        client = DBOSClient(system_database_url=sys_url)
        event(
            "sqlite_case_started",
            rung=plan.rung_id,
            case_id=plan.case_id,
            seed=plan.seed,
            schedule=plan.schedule,
        )

        prefix = f"wio-attr-{plan.seed}-{plan.case_id}"
        workflow_id = f"{prefix}-sqlite"
        start_success(
            workflow_id,
            {"backend": "sqlite", "seed": plan.seed, "case": plan.case_id},
            "sqlite",
        )
        status_before_filter = exact_status(workflow_id)
        list_error = assert_sqlite_attribute_filter_rejected(
            "sqlite_attribute_filter_rejected",
            lambda: DBOS.list_workflows(attributes={"backend": "sqlite"}),
        )
        client_error: dict[str, str] | None = None
        queued_id: str | None = None
        if plan.case_id == "case-003" or "backend-negative" in plan.schedule:
            options = {
                "queue_name": f"wio-attr-sqlite-unconsumed-{plan.seed}",
                "workflow_name": "attr_success_workflow",
                "attributes": {"backend": "sqlite-client", "seed": plan.seed},
            }
            handle = client.enqueue(options, "sqlite-client")
            queued_id = handle.workflow_id
            client.update_workflow_attributes(
                queued_id, {"backend": "sqlite-client-updated", "seed": plan.seed}
            )
            client_error = assert_sqlite_attribute_filter_rejected(
                "sqlite_client_attribute_filter_rejected",
                lambda: client.list_queued_workflows(
                    attributes={"backend": "sqlite-client-updated"}
                ),
            )
            invariant(
                "sqlite_unfiltered_status_still_exposes_updated_attrs",
                handle.get_status().attributes
                == {"backend": "sqlite-client-updated", "seed": plan.seed},
                queued_id=queued_id,
                status=handle.get_status().attributes,
            )

        result = {
            "status": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case_id": plan.case_id,
            "seed": plan.seed,
            "schedule": plan.schedule,
            "backend": "sqlite",
            "workflow_id": workflow_id,
            "queued_id": queued_id,
            "status_before_filter": status_before_filter,
            "sqlite_filter_error": list_error,
            "sqlite_client_filter_error": client_error,
            "app_db": app_url,
            "sys_db": sys_url,
        }
        write_json(artifacts / "result.json", result)
        event("case_passed", rung=plan.rung_id, case_id=plan.case_id)
        return result
    finally:
        if client is not None:
            client.destroy()
        if dbos is not None:
            DBOS.destroy(destroy_registry=False)


def run_schedule_identity_case(plan: CasePlan, artifacts: Path) -> dict[str, Any]:
    app_url, sys_url, masked = prepare_databases(plan.database_prefix, artifacts)
    dbos: DBOS | None = None
    client: DBOSClient | None = None
    schedule_names: list[str] = []
    try:
        DBOS.destroy(destroy_registry=False)
        dbos = DBOS(config=make_config(plan, app_url, sys_url))
        dbos.launch()
        client = DBOSClient(system_database_url=sys_url)
        event(
            "schedule_identity_case_started",
            rung=plan.rung_id,
            case_id=plan.case_id,
            seed=plan.seed,
            schedule=plan.schedule,
        )

        prefix = f"wio-sched-ident-{plan.seed}-{plan.case_id}"
        wf_name = workflow_name(schedule_identity_workflow)
        origin_model: dict[str, str | None] = {}
        filter_observations: dict[str, Any] = {}
        workflow_ids: list[str] = []
        handle_results: dict[str, Any] = {}

        if plan.case_id == "case-001":
            sched_a = f"{prefix}-a"
            sched_b = f"{prefix}-b"
            schedule_names.extend([sched_a, sched_b])
            for schedule_name in schedule_names:
                DBOS.create_schedule(
                    schedule_name=schedule_name,
                    workflow_fn=schedule_identity_workflow,
                    schedule="0 0 * * *",
                    context={"case": plan.case_id, "schedule_name": schedule_name, "seed": plan.seed},
                )
            manual_handle = DBOS.start_workflow(
                schedule_identity_workflow,
                datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
                {"case": plan.case_id, "manual": True, "seed": plan.seed},
            )
            handle_a = DBOS.trigger_schedule(sched_a)
            handle_b = DBOS.trigger_schedule(sched_b)
            for handle in (manual_handle, handle_a, handle_b):
                handle_results[handle.workflow_id] = handle.get_result()
            origin_model = {
                manual_handle.workflow_id: None,
                handle_a.workflow_id: sched_a,
                handle_b.workflow_id: sched_b,
            }
            workflow_ids = list(origin_model)
            filter_observations["single_a"] = assert_schedule_filter(
                "schedule_identity_single_name_filter_exact",
                expected_ids={handle_a.workflow_id},
                client=client,
                schedule_name=sched_a,
            )
            filter_observations["list_ab"] = assert_schedule_filter(
                "schedule_identity_name_list_filter_exact",
                expected_ids={handle_a.workflow_id, handle_b.workflow_id},
                client=client,
                schedule_name=[sched_a, sched_b],
            )
            filter_observations["manual_excluded"] = assert_schedule_filter(
                "schedule_identity_manual_run_excluded",
                expected_ids=set(),
                client=client,
                schedule_name=f"{prefix}-never",
            )
            filter_observations["composed_name"] = assert_schedule_filter(
                "schedule_identity_composes_with_workflow_name",
                expected_ids={handle_b.workflow_id},
                client=client,
                schedule_name=sched_b,
                name=wf_name,
            )

        elif plan.case_id == "case-002":
            sched_main = f"{prefix}-main"
            sched_other = f"{prefix}-other"
            queue_name = f"wio_sched_ident_queue_{plan.seed}"
            DBOS.register_queue(queue_name, worker_concurrency=1, polling_interval_sec=0.05, on_conflict="always_update")
            schedule_names.extend([sched_main, sched_other])
            DBOS.create_schedule(
                schedule_name=sched_main,
                workflow_fn=schedule_identity_workflow,
                schedule="0 * * * *",
                context={"case": plan.case_id, "schedule_name": sched_main, "seed": plan.seed},
                queue_name=queue_name,
            )
            DBOS.create_schedule(
                schedule_name=sched_other,
                workflow_fn=schedule_identity_workflow,
                schedule="0 * * * *",
                context={"case": plan.case_id, "schedule_name": sched_other, "seed": plan.seed},
                queue_name=queue_name,
            )
            manual_handle = DBOS.start_workflow(
                schedule_identity_workflow,
                datetime(2025, 2, 1, 0, 0, tzinfo=timezone.utc),
                {"case": plan.case_id, "manual": True, "seed": plan.seed},
            )
            trigger_handle = DBOS.trigger_schedule(sched_main)
            start = datetime(2025, 2, 1, 0, 30, 0, tzinfo=timezone.utc)
            end = datetime(2025, 2, 1, 3, 30, 0, tzinfo=timezone.utc)
            backfill_handles = DBOS.backfill_schedule(sched_main, start, end)
            other_handle = DBOS.trigger_schedule(sched_other)
            for handle in [manual_handle, trigger_handle, other_handle, *backfill_handles]:
                handle_results[handle.workflow_id] = handle.get_result()
            origin_model = {manual_handle.workflow_id: None, trigger_handle.workflow_id: sched_main, other_handle.workflow_id: sched_other}
            origin_model.update({handle.workflow_id: sched_main for handle in backfill_handles})
            workflow_ids = list(origin_model)
            main_ids = {workflow_id for workflow_id, origin in origin_model.items() if origin == sched_main}
            other_ids = {workflow_id for workflow_id, origin in origin_model.items() if origin == sched_other}
            filter_observations["single_main"] = assert_schedule_filter(
                "schedule_identity_trigger_backfill_filter_exact",
                expected_ids=main_ids,
                client=client,
                schedule_name=sched_main,
            )
            filter_observations["status_success"] = assert_schedule_filter(
                "schedule_identity_composes_with_status",
                expected_ids=main_ids,
                client=client,
                schedule_name=sched_main,
                status="SUCCESS",
            )
            filter_observations["queue"] = assert_schedule_filter(
                "schedule_identity_composes_with_queue_name",
                expected_ids=main_ids,
                client=client,
                schedule_name=sched_main,
                queue_name=queue_name,
            )
            filter_observations["workflow_name"] = assert_schedule_filter(
                "schedule_identity_composes_with_name",
                expected_ids=main_ids | other_ids,
                client=client,
                schedule_name=[sched_main, sched_other],
                name=wf_name,
            )
            filter_observations["user_attributes_are_separate"] = assert_schedule_filter(
                "schedule_identity_does_not_match_user_attributes",
                expected_ids=set(),
                client=client,
                schedule_name=sched_main,
                attributes={"schedule_name": sched_main},
            )

        elif plan.case_id == "case-003":
            sched_export = f"{prefix}-export"
            schedule_names.append(sched_export)
            DBOS.create_schedule(
                schedule_name=sched_export,
                workflow_fn=schedule_identity_workflow,
                schedule="0 0 * * *",
                context={"case": plan.case_id, "schedule_name": sched_export, "seed": plan.seed},
            )
            handle = DBOS.trigger_schedule(sched_export)
            handle_results[handle.workflow_id] = handle.get_result()
            DBOS.update_workflow_attributes(
                handle.workflow_id,
                {"user_schedule_name": "not-the-schedule-field", "seed": plan.seed},
            )
            exported = dbos._sys_db.export_workflow(handle.workflow_id, export_children=True)
            exported_summary = {
                "count": len(exported),
                "workflow_ids": [item["workflow_status"]["workflow_uuid"] for item in exported],
                "schedule_names": [item["workflow_status"].get("schedule_name") for item in exported],
                "attributes": [item["workflow_status"].get("attributes") for item in exported],
            }
            DBOS.delete_workflow(handle.workflow_id)
            DBOS.delete_schedule(sched_export)
            schedule_names.remove(sched_export)
            invariant(
                "schedule_identity_deleted_workflow_absent_before_import",
                DBOS.list_workflows(workflow_ids=[handle.workflow_id]) == [],
                workflow_id=handle.workflow_id,
            )
            dbos._sys_db.import_workflow(exported)
            imported_status = DBOS.get_workflow_status(handle.workflow_id)
            imported_result = DBOS.retrieve_workflow(handle.workflow_id).get_result()
            origin_model = {handle.workflow_id: sched_export}
            workflow_ids = [handle.workflow_id]
            filter_observations["imported_by_schedule"] = assert_schedule_filter(
                "schedule_identity_imported_workflow_queryable_after_schedule_delete",
                expected_ids={handle.workflow_id},
                client=client,
                schedule_name=sched_export,
            )
            invariant(
                "schedule_identity_export_import_preserves_attrs_and_result",
                imported_status is not None
                and imported_status.schedule_name == sched_export
                and imported_status.attributes == {"user_schedule_name": "not-the-schedule-field", "seed": plan.seed}
                and imported_result == handle_results[handle.workflow_id],
                workflow_id=handle.workflow_id,
                imported_schedule_name=imported_status.schedule_name if imported_status else None,
                imported_attributes=imported_status.attributes if imported_status else None,
                imported_result=imported_result,
                original_result=handle_results[handle.workflow_id],
                exported_summary=exported_summary,
            )
            filter_observations["exported_summary"] = exported_summary
        else:
            raise SetupBlock(f"unsupported schedule identity case {plan.case_id}")

        status_observations = assert_status_origin_model(
            "schedule_identity_status_and_list_match_model",
            workflow_ids=workflow_ids,
            origin_model=origin_model,
            client=client,
        )
        result = {
            "status": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case_id": plan.case_id,
            "seed": plan.seed,
            "schedule": plan.schedule,
            "workflow_name": wf_name,
            "workflow_ids": workflow_ids,
            "origin_model": origin_model,
            "filter_observations": filter_observations,
            "status_observations": status_observations,
            "handle_results": handle_results,
            "dbos_product_source": str(next((p for p in sys.path if p.endswith("dbos-transact-py")), "")),
            "app_db": plan.database_prefix + "_app",
            "sys_db": plan.database_prefix + "_sys",
            "admin_url": masked,
        }
        write_json(artifacts / "result.json", result)
        event("case_passed", rung=plan.rung_id, case_id=plan.case_id)
        return result
    finally:
        for schedule_name in schedule_names:
            try:
                DBOS.delete_schedule(schedule_name)
            except Exception as exc:
                event("schedule_cleanup_best_effort_failed", schedule_name=schedule_name, error_type=type(exc).__name__, error=str(exc))
        if client is not None:
            client.destroy()
        if dbos is not None:
            DBOS.destroy(destroy_registry=False)
        drop_databases(plan.database_prefix)


def scheduled_workflow_prefix(fn: Any) -> str:
    return f"sched-{workflow_name(fn)}-"


def scheduled_rows_matching(
    rows: list[Any],
    *,
    status: str | None = None,
    app_version: str | None = None,
) -> list[Any]:
    return [
        row
        for row in rows
        if (status is None or row.status == status)
        and (app_version is None or row.app_version == app_version)
    ]


def scheduled_success_rows(
    rows: list[Any],
    *,
    app_version: str | None = None,
) -> list[Any]:
    return scheduled_rows_matching(
        rows,
        status="SUCCESS",
        app_version=app_version,
    )


def set_application_version_timestamp(dbos: DBOS, version_name: str, timestamp_ms: int) -> None:
    dbos._sys_db.create_application_version(version_name)
    dbos._sys_db.update_application_version_timestamp(version_name, timestamp_ms)
    event("application_version_modeled", version_name=version_name, timestamp_ms=timestamp_ms)


def versioned_rows(prefix: str) -> list[Any]:
    return DBOS.list_workflows(
        workflow_id_prefix=prefix,
        load_input=False,
        load_output=False,
        sort_desc=False,
    )


def wait_for_versioned_rows(
    label: str,
    *,
    prefix: str,
    predicate: Any,
    timeout_seconds: float = 25.0,
) -> list[Any]:
    deadline = time.monotonic() + timeout_seconds
    last_rows: list[Any] = []
    while time.monotonic() < deadline:
        last_rows = versioned_rows(prefix)
        if predicate(last_rows):
            return last_rows
        time.sleep(0.25)
    invariant(
        label,
        False,
        prefix=prefix,
        rows=status_summary(last_rows),
        timeout_seconds=timeout_seconds,
    )
    return last_rows


def assert_version_rows(
    label: str,
    *,
    workflow_ids: list[str],
    expected_versions: dict[str, str],
    client: DBOSClient,
) -> dict[str, Any]:
    runtime_rows = DBOS.list_workflows(
        workflow_ids=workflow_ids,
        load_input=False,
        load_output=False,
    )
    client_rows = client.list_workflows(
        workflow_ids=workflow_ids,
        load_input=False,
        load_output=False,
    )
    runtime_by_id = {row.workflow_id: row for row in runtime_rows}
    client_by_id = {row.workflow_id: row for row in client_rows}
    durable_by_id = {
        workflow_id: DBOS.get_workflow_status(workflow_id)
        for workflow_id in workflow_ids
    }
    runtime_versions = {
        workflow_id: runtime_by_id[workflow_id].app_version
        for workflow_id in workflow_ids
        if workflow_id in runtime_by_id
    }
    client_versions = {
        workflow_id: client_by_id[workflow_id].app_version
        for workflow_id in workflow_ids
        if workflow_id in client_by_id
    }
    durable_versions = {
        workflow_id: durable_by_id[workflow_id].app_version
        for workflow_id in workflow_ids
        if durable_by_id[workflow_id] is not None
    }
    invariant(
        label,
        set(runtime_by_id) == set(workflow_ids)
        and set(client_by_id) == set(workflow_ids)
        and set(durable_versions) == set(workflow_ids)
        and runtime_versions == expected_versions
        and client_versions == expected_versions
        and durable_versions == expected_versions,
        workflow_ids=workflow_ids,
        expected_versions=expected_versions,
        runtime_versions=runtime_versions,
        client_versions=client_versions,
        durable_versions=durable_versions,
        runtime_rows=status_summary(runtime_rows),
        client_rows=status_summary(client_rows),
    )
    return {
        "runtime_rows": status_summary(runtime_rows),
        "client_rows": status_summary(client_rows),
        "durable_versions": durable_versions,
    }


def assert_version_results(label: str, workflow_ids: list[str], expected_kind: str) -> dict[str, Any]:
    results = {}
    for workflow_id in workflow_ids:
        results[workflow_id] = DBOS.retrieve_workflow(workflow_id).get_result()
    invariant(
        label,
        all(
            result["kind"] == expected_kind and result["workflow_id"] == workflow_id
            for workflow_id, result in results.items()
        ),
        expected_kind=expected_kind,
        results=results,
    )
    return results


def relaunch_for_version_case(plan: CasePlan, app_url: str, sys_url: str) -> DBOS:
    DBOS.destroy(destroy_registry=False)
    dbos = DBOS(config=make_config(plan, app_url, sys_url))
    dbos.launch()
    event("legacy_version_case_relaunched", rung=plan.rung_id, case_id=plan.case_id)
    return dbos


def run_legacy_scheduler_version_case(plan: CasePlan, artifacts: Path) -> dict[str, Any]:
    app_url, sys_url, masked = prepare_databases(plan.database_prefix, artifacts)
    dbos: DBOS | None = None
    client: DBOSClient | None = None
    try:
        DBOS.destroy(destroy_registry=False)
        dbos = DBOS(config=make_config(plan, app_url, sys_url))
        initial_versions: dict[str, str] = {}
        if plan.case_id == "case-001":
            initial_versions = {
                "older": f"legacy-old-{plan.seed}",
                "newer": f"legacy-new-{plan.seed}",
            }
        elif plan.case_id == "case-002":
            initial_versions = {
                "before": f"legacy-before-{plan.seed}",
                "after": f"legacy-after-{plan.seed}",
            }
        elif plan.case_id == "case-003":
            initial_versions = {"latest": f"legacy-relaunch-{plan.seed}"}
        dbos.launch()
        current_version = DBOS.application_version
        now_ms = int(time.time() * 1000)
        if plan.case_id == "case-001":
            initial_versions["newer"] = current_version
            set_application_version_timestamp(dbos, initial_versions["older"], now_ms + 10_000)
            set_application_version_timestamp(dbos, initial_versions["newer"], now_ms + 20_000)
        elif plan.case_id == "case-002":
            initial_versions["before"] = current_version
            set_application_version_timestamp(dbos, initial_versions["before"], now_ms + 10_000)
        elif plan.case_id == "case-003":
            initial_versions["latest"] = current_version
            set_application_version_timestamp(dbos, initial_versions["latest"], now_ms + 10_000)
        client = DBOSClient(system_database_url=sys_url)
        event(
            "legacy_scheduler_version_case_started",
            rung=plan.rung_id,
            case_id=plan.case_id,
            seed=plan.seed,
            schedule=plan.schedule,
        )

        observations: dict[str, Any] = {}

        if plan.case_id == "case-001":
            newer_version = initial_versions["newer"]
            prefix = scheduled_workflow_prefix(legacy_sync_scheduled_version_workflow)
            rows = wait_for_versioned_rows(
                "legacy_sync_rows_enqueued",
                prefix=prefix,
                predicate=lambda found: len(
                    scheduled_success_rows(
                        found,
                        app_version=newer_version,
                    )
                )
                >= 1,
            )
            workflow_ids = [row.workflow_id for row in rows]
            terminal_ids = [
                row.workflow_id
                for row in scheduled_success_rows(
                    rows,
                    app_version=newer_version,
                )
            ]
            expected_versions = {workflow_id: newer_version for workflow_id in workflow_ids}
            observations["version_rows"] = assert_version_rows(
                "legacy_sync_latest_version_runtime_client_durable",
                workflow_ids=workflow_ids,
                expected_versions=expected_versions,
                client=client,
            )
            observations["results"] = assert_version_results(
                "legacy_sync_terminal_results_retrievable",
                terminal_ids,
                "legacy_sync",
            )
            observations["latest_versions"] = [dict(row) for row in dbos._sys_db.list_application_versions()[:3]]

        elif plan.case_id == "case-002":
            before_version = initial_versions["before"]
            after_version = initial_versions["after"]
            prefix = scheduled_workflow_prefix(legacy_async_scheduled_version_workflow)
            before_rows = wait_for_versioned_rows(
                "legacy_async_before_version_rows_enqueued",
                prefix=prefix,
                predicate=lambda found: any(
                    scheduled_rows_matching(
                        found,
                        app_version=before_version,
                    )
                ),
            )
            before_ids = [
                row.workflow_id
                for row in scheduled_success_rows(
                    before_rows,
                    app_version=before_version,
                )
            ]
            set_application_version_timestamp(dbos, after_version, int(time.time() * 1000) + 20_000)
            rows = wait_for_versioned_rows(
                "legacy_async_after_version_rows_enqueued",
                prefix=prefix,
                predicate=lambda found: bool(
                    scheduled_rows_matching(
                        found,
                        app_version=after_version,
                    )
                )
                and any(
                    scheduled_rows_matching(
                        found,
                        app_version=before_version,
                    )
                ),
            )
            candidate_rows = [
                row
                for row in rows
                if row.app_version in {before_version, after_version}
            ]
            workflow_ids = [row.workflow_id for row in candidate_rows]
            expected_versions = {
                row.workflow_id: row.app_version
                for row in candidate_rows
            }
            observed_versions = {
                row.workflow_id: row.app_version
                for row in rows
            }
            invariant(
                "legacy_async_version_rollover_has_no_unexpected_versions",
                set(workflow_ids) == set(expected_versions)
                and before_version in set(expected_versions.values())
                and after_version in set(expected_versions.values())
                and set(observed_versions.values()) <= {before_version, after_version},
                workflow_ids=workflow_ids,
                expected_versions=expected_versions,
                before_ids=before_ids,
                allowed_versions=sorted([before_version, after_version]),
                observed_versions=observed_versions,
                rows=status_summary(rows),
            )
            observations["version_rows"] = assert_version_rows(
                "legacy_async_rollover_runtime_client_durable",
                workflow_ids=workflow_ids,
                expected_versions=expected_versions,
                client=client,
            )
            if before_ids:
                observations["results"] = assert_version_results(
                    "legacy_async_current_version_terminal_results_retrievable",
                    before_ids,
                    "legacy_async",
                )

        elif plan.case_id == "case-003":
            latest_version = initial_versions["latest"]
            prefix = scheduled_workflow_prefix(legacy_relaunch_scheduled_version_workflow)
            rows_before = wait_for_versioned_rows(
                "legacy_relaunch_rows_enqueued_before_relaunch",
                prefix=prefix,
                predicate=lambda found: len(
                    scheduled_rows_matching(
                        found,
                        app_version=latest_version,
                    )
                )
                >= 1,
            )
            workflow_ids = [row.workflow_id for row in rows_before]
            terminal_ids = [
                row.workflow_id
                for row in scheduled_success_rows(
                    rows_before,
                    app_version=latest_version,
                )
            ]
            expected_versions = {workflow_id: latest_version for workflow_id in workflow_ids}
            observations["before_relaunch"] = assert_version_rows(
                "legacy_relaunch_before_runtime_client_durable",
                workflow_ids=workflow_ids,
                expected_versions=expected_versions,
                client=client,
            )
            client.destroy()
            client = None
            dbos = relaunch_for_version_case(plan, app_url, sys_url)
            client = DBOSClient(system_database_url=sys_url)
            observations["after_relaunch"] = assert_version_rows(
                "legacy_relaunch_after_runtime_client_durable",
                workflow_ids=workflow_ids,
                expected_versions=expected_versions,
                client=client,
            )
            if terminal_ids:
                observations["results"] = assert_version_results(
                    "legacy_relaunch_terminal_results_retrievable",
                    terminal_ids,
                    "legacy_relaunch",
                )

        else:
            raise SetupBlock(f"unsupported legacy scheduler version case {plan.case_id}")

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
        drop_databases(plan.database_prefix)


def run_temporal_introspection_case(plan: CasePlan, artifacts: Path) -> dict[str, Any]:
    app_url, sys_url, masked = prepare_databases(plan.database_prefix, artifacts)
    dbos: DBOS | None = None
    client: DBOSClient | None = None
    try:
        DBOS.destroy(destroy_registry=False)
        dbos = DBOS(config=make_config(plan, app_url, sys_url))
        dbos.launch()
        client = DBOSClient(system_database_url=sys_url)
        event(
            "temporal_case_started",
            rung=plan.rung_id,
            case_id=plan.case_id,
            seed=plan.seed,
            schedule=plan.schedule,
        )

        prefix = f"wio-temporal-{plan.seed}-{plan.case_id}"
        observations: dict[str, Any] = {}

        if plan.case_id == "case-001":
            success_id = f"{prefix}-success"
            error_id = f"{prefix}-error"
            resume_id = f"{prefix}-cancel-resume"
            event_key = f"{prefix}-release"
            TEMPORAL_RELEASE_EVENTS[event_key] = threading.Event()

            before_all = now_iso()
            with SetWorkflowID(success_id):
                success_result = temporal_success_workflow("success")
            before_error = now_iso()
            with SetWorkflowID(error_id):
                try:
                    temporal_error_workflow("error")
                except RuntimeError as exc:
                    observations["modeled_error"] = str(exc)
                else:
                    raise WorkloadFailure("temporal error workflow unexpectedly succeeded")
            with SetWorkflowID(resume_id):
                DBOS.start_workflow(temporal_blocking_workflow, event_key, "resumed-success")
            wait_for_status(resume_id, lambda status: status.status == "PENDING")
            DBOS.cancel_workflow(resume_id)
            cancelled = status_obj(resume_id)
            resumed_handle = DBOS.resume_workflow(resume_id)
            resumed_active = status_obj(resume_id)
            before_release = now_iso()

            observations["pre_release_completed_window"] = assert_temporal_filter(
                "temporal_completed_window_excludes_resumed_active",
                workflow_ids=[success_id, error_id, resume_id],
                expected_ids={success_id, error_id},
                client=client,
                completed_after=before_all,
                completed_before=before_release,
            )
            TEMPORAL_RELEASE_EVENTS[event_key].set()
            final_result = resumed_handle.get_result()
            after_final = now_iso()
            final_status = status_obj(resume_id)
            statuses = temporal_status_map([success_id, error_id, resume_id])

            invariant(
                "temporal_terminal_resume_completed_at_transitions",
                success_result == "success"
                and final_result == "resumed-success"
                and statuses[success_id]["status"] == "SUCCESS"
                and statuses[error_id]["status"] == "ERROR"
                and cancelled.status == "CANCELLED"
                and cancelled.completed_at is not None
                and resumed_active.completed_at is None
                and final_status.status == "SUCCESS"
                and final_status.completed_at is not None
                and final_status.completed_at >= (cancelled.completed_at or 0)
                and all(
                    row["completed_at"] is None or row["created_at"] <= row["completed_at"]
                    for row in statuses.values()
                ),
                cancelled=row_to_dict(cancelled),
                resumed_active=row_to_dict(resumed_active),
                final=row_to_dict(final_status),
                statuses=statuses,
            )
            observations["post_release_completed_window"] = assert_temporal_filter(
                "temporal_completed_window_includes_fresh_final_completion",
                workflow_ids=[success_id, error_id, resume_id],
                expected_ids={success_id, error_id, resume_id},
                client=client,
                completed_after=before_all,
                completed_before=after_final,
            )
            observations["completed_after_error"] = assert_temporal_filter(
                "temporal_completed_after_matches_independent_model",
                workflow_ids=[success_id, error_id, resume_id],
                expected_ids={error_id, resume_id},
                client=client,
                completed_after=before_error,
                completed_before=after_final,
            )
            observations["statuses"] = statuses

        elif plan.case_id == "case-002":
            queue = Queue(
                f"wio_temporal_queue_{plan.seed}",
                worker_concurrency=1,
                polling_interval_sec=0.02,
            )
            block_id = f"{prefix}-queued-block"
            delayed_id = f"{prefix}-queued-delayed"
            direct_id = f"{prefix}-direct"
            event_key = f"{prefix}-queue-release"
            TEMPORAL_RELEASE_EVENTS[event_key] = threading.Event()

            before_all = now_iso()
            with SetWorkflowID(block_id):
                block_handle = queue.enqueue(
                    temporal_blocking_workflow, event_key, "queued-block"
                )
            block_started = wait_for_status(
                block_id,
                lambda status: status.status == "PENDING" and status.dequeued_at is not None,
            )
            with SetWorkflowID(direct_id):
                direct_result = temporal_success_workflow("direct")
            with SetEnqueueOptions(delay_seconds=0.25):
                with SetWorkflowID(delayed_id):
                    delayed_handle = queue.enqueue(temporal_success_workflow, "delayed")
            delayed_initial = status_obj(delayed_id)
            before_release = now_iso()
            observations["pre_release_dequeued_window"] = assert_temporal_filter(
                "temporal_dequeued_window_matches_started_queue_only_before_release",
                workflow_ids=[block_id, delayed_id, direct_id],
                expected_ids={block_id},
                client=client,
                dequeued_after=before_all,
                dequeued_before=before_release,
            )
            TEMPORAL_RELEASE_EVENTS[event_key].set()
            block_result = block_handle.get_result()
            delayed_result = delayed_handle.get_result()
            after_all = now_iso()
            statuses = temporal_status_map([block_id, delayed_id, direct_id])

            invariant(
                "temporal_queued_dequeued_at_ordering_and_direct_exclusion",
                block_result == "queued-block"
                and delayed_result == "delayed"
                and direct_result == "direct"
                and statuses[block_id]["dequeued_at"] is not None
                and statuses[delayed_id]["dequeued_at"] is not None
                and statuses[direct_id]["dequeued_at"] is None
                and statuses[block_id]["dequeued_at"] >= statuses[block_id]["created_at"]
                and statuses[delayed_id]["dequeued_at"] >= statuses[delayed_id]["created_at"]
                and delayed_initial.delay_until_epoch_ms is not None
                and statuses[delayed_id]["dequeued_at"] >= delayed_initial.delay_until_epoch_ms,
                block_started=row_to_dict(block_started),
                delayed_initial=row_to_dict(delayed_initial),
                statuses=statuses,
            )
            observations["post_release_dequeued_window"] = assert_temporal_filter(
                "temporal_dequeued_window_matches_all_started_queued_workflows",
                workflow_ids=[block_id, delayed_id, direct_id],
                expected_ids={block_id, delayed_id},
                client=client,
                dequeued_after=before_all,
                dequeued_before=after_all,
            )
            observations["post_release_queued_list_window"] = assert_temporal_filter(
                "temporal_queued_list_dequeued_window_matches_started_queue_only",
                workflow_ids=[block_id, delayed_id, direct_id],
                expected_ids={block_id, delayed_id},
                client=client,
                queued=True,
                status="SUCCESS",
                dequeued_after=before_all,
                dequeued_before=after_all,
            )
            observations["statuses"] = statuses

        elif plan.case_id == "case-003":
            queue = Queue(
                f"wio_temporal_relaunch_queue_{plan.seed}",
                worker_concurrency=1,
                polling_interval_sec=0.02,
            )
            queued_id = f"{prefix}-queued-export"
            resumed_id = f"{prefix}-resumed-export"
            event_key = f"{prefix}-resume-release"
            TEMPORAL_RELEASE_EVENTS[event_key] = threading.Event()

            before_all = now_iso()
            with SetWorkflowID(queued_id):
                queued_handle = queue.enqueue(temporal_success_workflow, "queued-export")
            queued_result = queued_handle.get_result()
            with SetWorkflowID(resumed_id):
                DBOS.start_workflow(temporal_blocking_workflow, event_key, "resumed-export")
            wait_for_status(resumed_id, lambda status: status.status == "PENDING")
            DBOS.cancel_workflow(resumed_id)
            cancelled = status_obj(resumed_id)
            resumed_handle = DBOS.resume_workflow(resumed_id)
            active_after_resume = status_obj(resumed_id)
            TEMPORAL_RELEASE_EVENTS[event_key].set()
            resumed_result = resumed_handle.get_result()
            after_all = now_iso()
            before_relaunch = temporal_status_map([queued_id, resumed_id])

            DBOS.destroy(destroy_registry=False)
            dbos = DBOS(config=make_config(plan, app_url, sys_url))
            dbos.launch()
            after_relaunch = temporal_status_map([queued_id, resumed_id])
            exported: list[dict[str, Any]] = []
            for workflow_id in [queued_id, resumed_id]:
                exported.extend(dbos._sys_db.export_workflow(workflow_id, export_children=True))
                DBOS.delete_workflow(workflow_id)
            deleted_rows = DBOS.list_workflows(
                workflow_ids=[queued_id, resumed_id],
                load_input=False,
                load_output=False,
            )
            dbos._sys_db.import_workflow(exported)
            after_import = temporal_status_map([queued_id, resumed_id])

            temporal_fields = ["created_at", "dequeued_at", "delay_until_epoch_ms", "completed_at"]
            invariant(
                "temporal_relaunch_export_import_preserves_timestamp_fields",
                queued_result == "queued-export"
                and resumed_result == "resumed-export"
                and cancelled.completed_at is not None
                and active_after_resume.completed_at is None
                and deleted_rows == []
                and {
                    workflow_id: {field: before_relaunch[workflow_id][field] for field in temporal_fields}
                    for workflow_id in before_relaunch
                }
                == {
                    workflow_id: {field: after_relaunch[workflow_id][field] for field in temporal_fields}
                    for workflow_id in after_relaunch
                }
                == {
                    workflow_id: {field: after_import[workflow_id][field] for field in temporal_fields}
                    for workflow_id in after_import
                },
                cancelled=row_to_dict(cancelled),
                active_after_resume=row_to_dict(active_after_resume),
                before_relaunch=before_relaunch,
                after_relaunch=after_relaunch,
                after_import=after_import,
                exported_count=len(exported),
            )
            observations["completed_after_import"] = assert_temporal_filter(
                "temporal_imported_completed_window_membership_preserved",
                workflow_ids=[queued_id, resumed_id],
                expected_ids={queued_id, resumed_id},
                client=client,
                completed_after=before_all,
                completed_before=after_all,
            )
            observations["dequeued_after_import"] = assert_temporal_filter(
                "temporal_imported_dequeued_window_membership_preserved",
                workflow_ids=[queued_id, resumed_id],
                expected_ids={
                    workflow_id
                    for workflow_id, row in after_import.items()
                    if row["dequeued_at"] is not None
                },
                client=client,
                dequeued_after=before_all,
                dequeued_before=after_all,
            )
            observations["before_relaunch"] = before_relaunch
            observations["after_relaunch"] = after_relaunch
            observations["after_import"] = after_import

        elif plan.case_id == "case-004":
            queue = Queue(
                f"wio_temporal_aggregate_queue_{plan.seed}",
                worker_concurrency=1,
                polling_interval_sec=0.02,
            )
            workflow_ids = [
                f"{prefix}-direct-a",
                f"{prefix}-direct-b",
                f"{prefix}-queued-a",
                f"{prefix}-queued-b",
            ]
            before_all = now_iso()
            direct_results: list[str] = []
            for workflow_id, label in zip(workflow_ids[:2], ["direct-a", "direct-b"]):
                with SetWorkflowID(workflow_id):
                    handle = DBOS.start_workflow(
                        temporal_direct_aggregate_workflow, label, 70
                    )
                direct_results.append(handle.get_result())
            queued_results: list[str] = []
            for workflow_id, label in zip(workflow_ids[2:], ["queued-a", "queued-b"]):
                with SetWorkflowID(workflow_id):
                    handle = queue.enqueue(temporal_queued_aggregate_workflow, label, 70)
                queued_results.append(handle.get_result())
            after_all = now_iso()
            statuses = temporal_status_map(workflow_ids)
            aggregate_status_rows = [
                row_to_dict(row)
                for row in DBOS.list_workflows(
                    workflow_id_prefix=[prefix],
                    load_input=False,
                    load_output=False,
                )
            ]
            bucket_ms = 60 * 60 * 1000

            aggregate_rows = dbos._sys_db.get_workflow_aggregates(
                group_by_name=True,
                time_bucket_size_ms=bucket_ms,
                workflow_id_prefix=[prefix],
                select_count=True,
                select_min_created_at=True,
                select_max_queue_wait_ms=True,
                select_max_total_latency_ms=True,
            )
            aggregate_by_key = {
                (row["group"]["name"], int(row["group"]["time_bucket"])): row
                for row in aggregate_rows
            }
            expected_aggregates: dict[tuple[str, int], dict[str, Any]] = {}
            for row in aggregate_status_rows:
                key = (row["name"], time_bucket(row["created_at"], bucket_ms))
                expected = expected_aggregates.setdefault(
                    key,
                    {"count": 0, "min_created_at": row["created_at"], "rows": []},
                )
                expected["count"] += 1
                expected["min_created_at"] = min(expected["min_created_at"], row["created_at"])
                expected["rows"].append(row)

            aggregate_model_ok = set(aggregate_by_key) == set(expected_aggregates)
            for key, expected in expected_aggregates.items():
                row = aggregate_by_key.get(key)
                if row is None:
                    aggregate_model_ok = False
                    continue
                has_queue = any(item["dequeued_at"] is not None for item in expected["rows"])
                aggregate_model_ok = aggregate_model_ok and row["count"] == expected["count"]
                aggregate_model_ok = (
                    aggregate_model_ok
                    and row["min_created_at"] == expected["min_created_at"]
                    and row["max_total_latency_ms"] is not None
                    and row["max_total_latency_ms"] >= 0
                    and (
                        (has_queue and row["max_queue_wait_ms"] is not None and row["max_queue_wait_ms"] >= 0)
                        or (not has_queue and row["max_queue_wait_ms"] is None)
                    )
                )

            direct_steps = DBOS.list_workflow_steps(workflow_ids[0])
            direct_steps_by_name = {step["function_name"]: step for step in direct_steps}
            quick_name = workflow_name(temporal_quick_step)
            slow_name = workflow_name(temporal_slow_step)
            child_name = workflow_name(temporal_child_workflow)
            step_timing_ok = True
            for step_name in [quick_name, slow_name]:
                step = direct_steps_by_name.get(step_name)
                step_timing_ok = (
                    step_timing_ok
                    and step is not None
                    and step["started_at_epoch_ms"] is not None
                    and step["completed_at_epoch_ms"] is not None
                    and step["completed_at_epoch_ms"] >= step["started_at_epoch_ms"]
                )
            if slow_name in direct_steps_by_name:
                step_timing_ok = (
                    step_timing_ok
                    and direct_steps_by_name[slow_name]["completed_at_epoch_ms"]
                    - direct_steps_by_name[slow_name]["started_at_epoch_ms"]
                    >= 40
                )

            step_aggregate_rows = dbos._sys_db.get_step_aggregates(
                group_by_function_name=True,
                workflow_id_prefix=[prefix],
                completed_after=before_all,
                completed_before=after_all,
                select_count=True,
                select_max_duration_ms=True,
            )
            completed_step_by_fn = {
                row["group"]["function_name"]: row for row in step_aggregate_rows
            }
            unfiltered_step_rows = dbos._sys_db.get_step_aggregates(
                group_by_function_name=True,
                workflow_id_prefix=[prefix],
                select_count=True,
                select_max_duration_ms=True,
            )
            unfiltered_by_fn = {
                row["group"]["function_name"]: row for row in unfiltered_step_rows
            }
            bookkeeping_names = {child_name, "DBOS.getResult"}
            bookkeeping_excluded = all(
                name not in completed_step_by_fn for name in bookkeeping_names
            )
            bookkeeping_null_when_unfiltered = all(
                name not in unfiltered_by_fn
                or unfiltered_by_fn[name]["max_duration_ms"] is None
                for name in bookkeeping_names
            )

            invariant(
                "temporal_workflow_aggregates_and_step_timing_match_model",
                all(result.startswith("direct:") for result in direct_results)
                and all(result.startswith("queued:") for result in queued_results)
                and all(
                    row["completed_at"] is not None
                    and row["created_at"] <= row["completed_at"]
                    for row in aggregate_status_rows
                )
                and statuses[workflow_ids[0]]["dequeued_at"] is None
                and statuses[workflow_ids[2]]["dequeued_at"] is not None
                and aggregate_model_ok
                and step_timing_ok
                and completed_step_by_fn.get(quick_name, {}).get("count") == 4
                and completed_step_by_fn.get(slow_name, {}).get("count") == 4
                and completed_step_by_fn.get(slow_name, {}).get("max_duration_ms", 0) >= 40
                and bookkeeping_excluded
                and bookkeeping_null_when_unfiltered,
                statuses=statuses,
                aggregate_rows=[dict(row) for row in aggregate_rows],
                expected_aggregates={
                    f"{key[0]}:{key[1]}": value for key, value in expected_aggregates.items()
                },
                direct_steps=direct_steps,
                aggregate_status_rows=aggregate_status_rows,
                completed_step_aggregates=[dict(row) for row in step_aggregate_rows],
                unfiltered_step_aggregates=[dict(row) for row in unfiltered_step_rows],
            )
            observations["statuses"] = statuses
            observations["aggregate_status_rows"] = aggregate_status_rows
            observations["workflow_aggregates"] = [dict(row) for row in aggregate_rows]
            observations["step_aggregates_completed_window"] = [
                dict(row) for row in step_aggregate_rows
            ]
            observations["step_aggregates_unfiltered"] = [
                dict(row) for row in unfiltered_step_rows
            ]

        else:
            raise SetupBlock(f"unsupported temporal case {plan.case_id}")

        result = {
            "status": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case_id": plan.case_id,
            "seed": plan.seed,
            "schedule": plan.schedule,
            "focus": plan.focus,
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
        drop_databases(plan.database_prefix)


def run_attribute_case(plan: CasePlan, artifacts: Path) -> dict[str, Any]:
    if plan.rung_id == RUNG_005_ID:
        return run_schedule_identity_case(plan, artifacts)
    if plan.rung_id == RUNG_006_ID:
        return run_legacy_scheduler_version_case(plan, artifacts)
    if plan.rung_id == RUNG_007_ID:
        return run_temporal_introspection_case(plan, artifacts)

    if (
        plan.rung_id == RUNG_003_ID
        and plan.case_id in {"case-001", "case-003"}
    ) or (plan.rung_id == RUNG_004_ID and "backend-negative" in plan.schedule):
        return run_sqlite_attribute_case(plan, artifacts)

    app_url, sys_url, masked = prepare_databases(plan.database_prefix, artifacts)
    dbos: DBOS | None = None
    client: DBOSClient | None = None
    try:
        DBOS.destroy(destroy_registry=False)
        dbos = DBOS(config=make_config(plan, app_url, sys_url))
        dbos.launch()
        client = DBOSClient(system_database_url=sys_url)
        event(
            "case_started",
            rung=plan.rung_id,
            case_id=plan.case_id,
            seed=plan.seed,
            schedule=plan.schedule,
        )

        prefix = f"wio-attr-{plan.seed}-{plan.case_id}"
        query_observations: dict[str, list[str]] = {}

        latest_replace_case = (
            plan.rung_id == RUNG_000_ID
            or (plan.rung_id == RUNG_001_ID and plan.case_id == "case-002")
            or (plan.rung_id == RUNG_004_ID and "latest-replace" in plan.schedule)
        )
        predicate_case = (
            (plan.rung_id == RUNG_001_ID and plan.case_id == "case-001")
            or (plan.rung_id == RUNG_003_ID and plan.case_id == "case-002")
            or (
                plan.rung_id == RUNG_004_ID
                and (
                    "predicate-equality" in plan.schedule
                    or "predicate-type" in plan.schedule
                )
            )
        )
        status_compose_case = (
            (plan.rung_id == RUNG_001_ID and plan.case_id == "case-003")
            or (plan.rung_id == RUNG_004_ID and "status-compose" in plan.schedule)
        )
        fork_history_case = (
            (plan.rung_id == RUNG_002_ID and plan.case_id == "case-002")
            or (plan.rung_id == RUNG_004_ID and "fork-history" in plan.schedule)
        )

        if latest_replace_case:
            workflow_id = f"{prefix}-replace"
            initial = {"customer": "acme", "tier": 1, "old": True, "seed": plan.seed}
            replacement = {
                "customer": "bigco",
                "tier": 2,
                "region": "us-east-1",
                "seed": plan.seed,
            }
            start_success(workflow_id, initial, "replace")
            DBOS.update_workflow_attributes(workflow_id, replacement)
            query_observations["old"] = sorted_ids(matched_ids(attributes={"old": True}))
            query_observations["new"] = sorted_ids(
                matched_ids(attributes={"customer": "bigco", "tier": 2, "seed": plan.seed})
            )
            query_observations["stale_combo"] = sorted_ids(
                matched_ids(attributes={"customer": "acme", "tier": 2})
            )
            invariant(
                "attribute_replace_is_whole_dict",
                query_observations["old"] == []
                and query_observations["new"] == [workflow_id]
                and query_observations["stale_combo"] == [],
                observations=query_observations,
                expected_workflow=workflow_id,
            )
            if plan.rung_id == RUNG_000_ID:
                DBOS.update_workflow_attributes(workflow_id, None)
                query_observations["cleared"] = sorted_ids(
                    matched_ids(attributes={"customer": "bigco"})
                )
                invariant(
                    "attribute_clear_removes_from_filtered_results",
                    query_observations["cleared"] == [],
                    observations=query_observations,
                )

        if predicate_case:
            w1 = start_success(
                f"{prefix}-typed-a",
                {
                    "customer": "acme",
                    "tier": 1,
                    "beta": True,
                    "note": None,
                    "seed": plan.seed,
                },
                "typed-a",
            )
            w2 = start_success(
                f"{prefix}-typed-b",
                {
                    "customer": "bigco",
                    "tier": 2,
                    "meta": {"region": "us-east-1", "seed": plan.seed},
                    "seed": plan.seed,
                },
                "typed-b",
            )
            w3 = start_success(f"{prefix}-no-attrs", {}, "no-attrs")
            queries = {
                "customer_acme": matched_ids(
                    attributes={"customer": "acme", "seed": plan.seed}
                ),
                "bigco_tier": matched_ids(
                    attributes={"customer": "bigco", "tier": 2, "seed": plan.seed}
                ),
                "wrong_combo": matched_ids(attributes={"customer": "acme", "tier": 2}),
                "tier_int": matched_ids(attributes={"tier": 1}),
                "bool": matched_ids(attributes={"beta": True}),
                "null": matched_ids(attributes={"note": None}),
                "nested": matched_ids(
                    attributes={"meta": {"region": "us-east-1", "seed": plan.seed}}
                ),
                "workflow_id_filter": matched_ids(attributes={"tier": 1}, workflow_ids=[w2]),
                "missing": matched_ids(attributes={"missing": "key"}),
            }
            query_observations = {key: sorted_ids(value) for key, value in queries.items()}
            invariant(
                "postgres_attribute_predicates_match_exact_model",
                queries["customer_acme"] == {w1}
                and queries["bigco_tier"] == {w2}
                and queries["wrong_combo"] == set()
                and queries["tier_int"] == {w1}
                and queries["bool"] == {w1}
                and queries["null"] == {w1}
                and queries["nested"] == {w2}
                and queries["workflow_id_filter"] == set()
                and queries["missing"] == set()
                and w3 not in set().union(*queries.values()),
                observations=query_observations,
                workflows=[w1, w2, w3],
            )

        if status_compose_case:
            success_id = start_success(
                f"{prefix}-success",
                {"group": "status-compose", "kind": "success", "seed": plan.seed},
                "success",
            )
            error_id = start_error(
                f"{prefix}-error",
                {"group": "status-compose", "kind": "error", "seed": plan.seed},
                "error",
            )
            options = {
                "queue_name": f"wio-attr-unconsumed-{plan.seed}",
                "workflow_name": "attr_success_workflow",
                "attributes": {
                    "group": "status-compose",
                    "kind": "enqueued",
                    "seed": plan.seed,
                },
            }
            enqueued_handle = client.enqueue(options, "enqueued")
            enqueued_id = enqueued_handle.workflow_id
            all_group = matched_ids(attributes={"group": "status-compose", "seed": plan.seed})
            success_group = matched_ids(
                attributes={"group": "status-compose", "seed": plan.seed}, status="SUCCESS"
            )
            error_group = matched_ids(
                attributes={"group": "status-compose", "seed": plan.seed},
                status="ERROR",
            )
            enqueued_group = matched_ids(
                attributes={"group": "status-compose", "seed": plan.seed},
                status="ENQUEUED",
            )
            query_observations = {
                "all_group": sorted_ids(all_group),
                "success_group": sorted_ids(success_group),
                "error_group": sorted_ids(error_group),
                "enqueued_group": sorted_ids(enqueued_group),
            }
            invariant(
                "attribute_predicate_composes_with_status",
                all_group == {success_id, error_id, enqueued_id}
                and success_group == {success_id}
                and error_group == {error_id}
                and enqueued_group == {enqueued_id},
                observations=query_observations,
            )

        if plan.rung_id == RUNG_002_ID and plan.case_id == "case-001":
            workflow_id = f"{prefix}-inside-update"
            final_attrs = {"phase": "inside", "attempt": 1, "seed": plan.seed}
            attempted_replay_attrs = {"phase": "replayed", "attempt": 2, "seed": plan.seed}
            with SetWorkflowAttributes({"phase": "start", "seed": plan.seed}):
                with SetWorkflowID(workflow_id):
                    first_result = attr_update_inside_workflow("inside-update", final_attrs)
            with SetWorkflowAttributes({"phase": "start-replay", "seed": plan.seed}):
                with SetWorkflowID(workflow_id):
                    second_result = attr_update_inside_workflow(
                        "inside-update", attempted_replay_attrs
                    )
            steps = DBOS.list_workflow_steps(workflow_id)
            step_names = [step["function_name"] for step in steps]
            current_attrs = exact_status(workflow_id)["attributes"]
            query_observations["inside_final"] = sorted_ids(
                matched_ids(attributes=final_attrs)
            )
            query_observations["inside_attempted_replay"] = sorted_ids(
                matched_ids(attributes=attempted_replay_attrs)
            )
            invariant(
                "workflow_attribute_update_inside_replay_checkpointed_once",
                first_result == "inside-update"
                and second_result == "inside-update"
                and current_attrs == final_attrs
                and step_names == ["DBOS.updateWorkflowAttributes"]
                and query_observations["inside_final"] == [workflow_id]
                and query_observations["inside_attempted_replay"] == [],
                workflow_id=workflow_id,
                current_attrs=current_attrs,
                step_names=step_names,
                observations=query_observations,
            )

        if fork_history_case:
            original_id = f"{prefix}-fork-original"
            initial_attrs = {"phase": "initial", "seed": plan.seed}
            before_fork_attrs = {"phase": "before-fork", "seed": plan.seed}
            after_fork_attrs = {"phase": "after-fork", "seed": plan.seed}
            with SetWorkflowAttributes(initial_attrs):
                with SetWorkflowID(original_id):
                    original_result = attr_forkable_history_workflow("forkable")
            DBOS.update_workflow_attributes(original_id, before_fork_attrs)
            forked_handle = DBOS.fork_workflow(original_id, 1)
            forked_result = forked_handle.get_result()
            forked_id = forked_handle.workflow_id
            DBOS.update_workflow_attributes(original_id, after_fork_attrs)
            original_attrs = exact_status(original_id)["attributes"]
            forked_attrs = exact_status(forked_id)["attributes"]
            query_observations["original_after_fork"] = sorted_ids(
                matched_ids(attributes=after_fork_attrs)
            )
            query_observations["fork_inherited"] = sorted_ids(
                matched_ids(attributes=before_fork_attrs)
            )
            query_observations["stale_initial"] = sorted_ids(
                matched_ids(attributes=initial_attrs)
            )
            invariant(
                "fork_attribute_history_matches_update_point",
                original_result == "forkable-first:forkable-second"
                and forked_result == "forkable-first:forkable-second"
                and original_attrs == after_fork_attrs
                and forked_attrs == before_fork_attrs
                and query_observations["original_after_fork"] == [original_id]
                and query_observations["fork_inherited"] == [forked_id]
                and query_observations["stale_initial"] == [],
                original_id=original_id,
                forked_id=forked_id,
                original_attrs=original_attrs,
                forked_attrs=forked_attrs,
                observations=query_observations,
            )

        if plan.rung_id == RUNG_002_ID and plan.case_id == "case-003":
            options = {
                "queue_name": f"wio-attr-client-transition-{plan.seed}",
                "workflow_name": "attr_success_workflow",
                "attributes": {"phase": "queued", "seed": plan.seed},
            }
            handle = client.enqueue(options, "client-transition")
            queued_id = handle.workflow_id
            updated_attrs = {"phase": "client-updated", "seed": plan.seed}
            client.update_workflow_attributes(queued_id, updated_attrs)
            query_observations["queued_old"] = sorted_ids(
                matched_ids(attributes={"phase": "queued", "seed": plan.seed})
            )
            query_observations["queued_updated"] = sorted_ids(
                matched_ids(attributes=updated_attrs)
            )
            client_queued_updated = {
                row.workflow_id
                for row in client.list_queued_workflows(attributes=updated_attrs)
            }
            invariant(
                "client_lifecycle_attribute_update_visible_once",
                handle.get_status().attributes == updated_attrs
                and query_observations["queued_old"] == []
                and query_observations["queued_updated"] == [queued_id]
                and client_queued_updated == {queued_id},
                queued_id=queued_id,
                status=handle.get_status().attributes,
                observations=query_observations,
                client_queued_updated=sorted_ids(client_queued_updated),
            )

        workflow_ids = sorted({wid for ids in query_observations.values() for wid in ids})
        status_rows = status_map(workflow_ids) if workflow_ids else {}
        result = {
            "status": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case_id": plan.case_id,
            "seed": plan.seed,
            "schedule": plan.schedule,
            "query_observations": query_observations,
            "status_rows": status_rows,
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
        drop_databases(plan.database_prefix)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rung", required=True)
    parser.add_argument("--case", dest="case_id")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument("--artifact-dir", default="/tmp/wio-artifacts/workflow-attributes-query")
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
        if args.all_cases and rung_id == RUNG_006_ID:
            for case_id in cases:
                event("case_subprocess_start", rung=rung_id, case_id=case_id)
                completed = subprocess.run(
                    [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "--rung",
                        rung_id,
                        "--case",
                        case_id,
                        "--artifact-dir",
                        str(artifact_root),
                    ],
                    cwd=str(REPO_ROOT),
                )
                if completed.returncode != 0:
                    return completed.returncode
                summaries.append(json.loads((artifact_root / case_id / "result.json").read_text()))
            write_json(artifact_root / "summary.json", summaries)
            return 0
        for case_id in cases:
            plan = make_plan(rung_id, case_id, args.seed)
            case_artifacts = artifact_root / case_id
            if plan.rung_id == RUNG_005_ID:
                protected_product_promise = (
                    "DBOS-owned scheduler identity (schedule_name) is queryable through "
                    "runtime/client workflow status APIs, composes with existing filters, "
                    "excludes manual workflow runs, and survives export/import."
                )
                invariant_oracle = (
                    "Independent schedule-origin model compared against DBOS.list_workflows, "
                    "DBOSClient.list_workflows, get_workflow_status, composed filters, and "
                    "export/delete/import observations."
                )
            elif plan.rung_id == RUNG_006_ID:
                protected_product_promise = (
                    "Legacy @DBOS.scheduled workflow rows carry the latest modeled "
                    "application version through internal scheduler enqueue, terminal "
                    "result retrieval, relaunch, and runtime/client status observations."
                )
                invariant_oracle = (
                    "Modeled application-version timeline compared against durable "
                    "workflow status, DBOS.list_workflows, DBOSClient.list_workflows, "
                    "and retrieved terminal scheduled workflow results."
                )
            elif plan.rung_id == RUNG_007_ID:
                protected_product_promise = (
                    "Workflow temporal introspection exposes durable completion/dequeue "
                    "timestamps consistently across runtime, client, queued/direct "
                    "workflows, terminal transitions, resume, filtering, aggregation, "
                    "and export/import."
                )
                invariant_oracle = (
                    "Independent temporal ledger compared against DBOS.list_workflows, "
                    "DBOSClient.list_workflows, durable status rows, relaunch/import "
                    "snapshots, workflow aggregates, and step aggregate windows."
                )
            else:
                protected_product_promise = (
                    "Workflow attributes are queryable and mutable through public/client APIs."
                )
                invariant_oracle = (
                    "Independent expected workflow-id sets and latest attribute dictionaries "
                    "exactly match public list/filter results."
                )
            write_json(
                case_artifacts / "case.json",
                {
                    **asdict(plan),
                    "frontier": FRONTIER_ID,
                    "prompt_path": PROMPT_PATH,
                    "protected_product_promise": protected_product_promise,
                    "replay_command": (
                        ".workers/run-with-postgres.sh .workers/python-runtime.sh "
                        f".workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung {plan.rung_id} --case {plan.case_id} --seed {plan.seed}"
                    ),
                    "seed_policy": "Exact rung seeds from workflow-attributes-query rung records.",
                    "invariant_oracle": invariant_oracle,
                },
            )
            summaries.append(run_attribute_case(plan, case_artifacts))
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
