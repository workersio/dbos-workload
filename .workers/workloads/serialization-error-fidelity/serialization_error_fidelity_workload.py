#!/usr/bin/env python3
"""Fresh WIO workload for DBOS durable serialization error fidelity.

Frontier: serialization-error-fidelity
Rungs:
  - rung-000-serializer-smoke
  - rung-001-default-serializer-error
  - rung-002-retry-recovery-error-records
  - rung-003-config-matrix
  - rung-004-portable-structured-error-metadata
  - rung-005-retry-class-stored-error-result-liveness
Evidence key:
  evidence-key:events/frontier_designer-20260620T072712859935000Z.prompt.md
Protected product promise:
  Failed durable workflows preserve the original actionable application error in
  durable status and retrieval paths instead of replacing it with serializer or
  configuration infrastructure errors.
Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py \
    --rung rung-001-default-serializer-error --case case-001
Seed policy:
  Exact rung seeds are encoded below; each case writes the derived case JSON,
  workflow id, observed error signatures, and invariant results.
Invariant oracle:
  Status, handle retrieval, client retrieval, and step rows must preserve the
  modeled application marker and must not report only serializer/config noise.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import queue
import sys
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[3]
VENV_SITE_PACKAGES = (
    next(
        (
            REPO_ROOT / ".workers" / "vendor" / "dbos-venv" / "lib"
        ).glob("python*/site-packages"),
        REPO_ROOT
        / ".workers"
        / "vendor"
        / "dbos-venv"
        / "lib"
        / "python3.12"
        / "site-packages",
    )
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

    from dbos import DBOS, DBOSClient, DBOSConfig, SetWorkflowID
    from dbos._serialization import (
        DBOSDefaultSerializer,
        DBOSPortableJSONSerializer,
        deserialize_exception,
        PortableWorkflowError,
        Serializer,
        WorkflowSerializationFormat,
    )
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "serialization-error-fidelity"
RUNG_000_ID = "rung-000-serializer-smoke"
RUNG_001_ID = "rung-001-default-serializer-error"
RUNG_002_ID = "rung-002-retry-recovery-error-records"
RUNG_003_ID = "rung-003-config-matrix"
RUNG_004_ID = "rung-004-portable-structured-error-metadata"
RUNG_005_ID = "rung-005-retry-class-stored-error-result-liveness"
APP_ID = "wio-ser-error-fidelity"
APP_VERSION = "wio-serialization-error-fidelity-rungs-000-005"
PROMPT_PATH = "evidence-key:events/frontier_designer-20260620T072712859935000Z.prompt.md"
ERROR_PREFIX = "WIO_SERIALIZATION_APP_ERROR"
DBAPI_ERROR_PREFIX = "WIO_DBAPI_STORED_ERROR"
PARENT_DBAPI_PREFIX = "WIO_PARENT_OBSERVED_CHILD_DBAPI"
STRUCTURED_ERROR_NAME = "WIOPortableStructuredError"
STRUCTURED_ERROR_PREFIX = "WIO_STRUCTURED_ERROR"
PORTABLE_JSON_SERIALIZATION = DBOSPortableJSONSerializer().name()
FORBIDDEN_NOISE = (
    "not portable json serializable",
    "not json serializable",
    "serialization is not available",
    "error serializing object",
    "pickle data was truncated",
    "could not deserialize",
    "failed to deserialize",
)
RESULT_TIMEOUT_SECONDS = 15.0
PARENT_RESULT_TIMEOUT_SECONDS = 30.0


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
    workflow_kind: str
    serializer_mode: str
    database_prefix: str
    expect_retry_wrapper: bool = False
    relaunch_before_read: bool = False
    late_read_seconds: float = 0.0


RETRY_COUNTS: dict[str, int] = {}


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
    if os.environ.get("WIO_SERIALIZATION_KEEP_DATABASES") == "1":
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


def serializer_for_mode(mode: str) -> Serializer:
    if mode == "portable_json_config":
        return DBOSPortableJSONSerializer()
    return DBOSDefaultSerializer


def make_config(plan: CasePlan, app_url: str, sys_url: str) -> DBOSConfig:
    config: DBOSConfig = {
        "name": APP_ID,
        "application_database_url": app_url,
        "system_database_url": sys_url,
        "application_version": f"{APP_VERSION}-{plan.rung_id}-{plan.case_id}",
        "executor_id": f"wio-serialization-{plan.case_id}",
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        "run_admin_server": False,
        "runtimeConfig": {"max_executor_threads": 8},
    }
    if plan.serializer_mode == "portable_json_config":
        config["serializer"] = DBOSPortableJSONSerializer()
    return config


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
    }
    if rung not in aliases:
        raise SetupBlock(f"unsupported rung {rung}")
    return aliases[rung]


def case_ids_for_rung(rung_id: str) -> list[str]:
    if rung_id == RUNG_000_ID:
        return ["case-001"]
    if rung_id in (RUNG_001_ID, RUNG_002_ID, RUNG_003_ID, RUNG_004_ID, RUNG_005_ID):
        return ["case-001", "case-002", "case-003"]
    raise SetupBlock(f"unsupported rung {rung_id}")


def make_plan(rung: str, case_id: str) -> CasePlan:
    rung_id = normalize_rung(rung)
    if case_id not in case_ids_for_rung(rung_id):
        raise SetupBlock(f"unknown case {case_id} for {rung_id}")

    specs: dict[tuple[str, str], tuple[int, str, str, str, str, bool, bool, float]] = {
        (RUNG_000_ID, "case-001"): (
            3500,
            "run-one-failing-workflow-under-baseline-config",
            "serializer modes and durable error retrieval run",
            "default",
            "default_pickle",
            False,
            False,
            0.0,
        ),
        (RUNG_001_ID, "case-001"): (
            3510,
            "failing-step-raises-valueerror-with-seed-marker",
            "default serializer preserves application ValueError",
            "default",
            "default_pickle",
            False,
            False,
            0.0,
        ),
        (RUNG_001_ID, "case-002"): (
            3511,
            "workflow-carries-nested-payload-then-raises-app-error",
            "non-json-ish payload does not mask application failure",
            "default_non_json_payload",
            "default_pickle",
            False,
            False,
            0.0,
        ),
        (RUNG_001_ID, "case-003"): (
            3512,
            "read-error-through-all-public-paths",
            "client/status/handle retrieval agree on same durable error",
            "default",
            "default_pickle",
            False,
            False,
            0.0,
        ),
        (RUNG_002_ID, "case-001"): (
            3520,
            "fail-after-retryable-step-attempts",
            "retries preserve final application error",
            "retry",
            "default_pickle",
            True,
            False,
            0.0,
        ),
        (RUNG_002_ID, "case-002"): (
            3521,
            "relaunch-dbosisolated-process-then-read-status",
            "relaunch recovery/read path does not rewrite terminal error",
            "default",
            "default_pickle",
            False,
            True,
            0.0,
        ),
        (RUNG_002_ID, "case-003"): (
            3522,
            "read-after-delay-cleanup-window",
            "late handle read does not lose error detail",
            "default",
            "default_pickle",
            False,
            False,
            0.25,
        ),
        (RUNG_003_ID, "case-001"): (
            3530,
            "run-failing-workflow-under-pickle-serializer",
            "native/pickle mode preserves app error",
            "native",
            "default_pickle",
            False,
            False,
            0.0,
        ),
        (RUNG_003_ID, "case-002"): (
            3531,
            "run-failing-workflow-under-portable-serializer",
            "portable mode preserves app error",
            "portable",
            "default_pickle",
            False,
            False,
            0.0,
        ),
        (RUNG_003_ID, "case-003"): (
            3532,
            "set-serializer-via-env-config-conflict",
            "explicit workflow serializer choice is recorded and app error preserved",
            "native",
            "portable_json_config",
            False,
            False,
            0.0,
        ),
        (RUNG_004_ID, "case-001"): (
            3540,
            "portable-workflow-raises-structured-error",
            "portable structured metadata survives status, handle, client, and step reads",
            "structured_portable",
            "portable_json_config",
            False,
            False,
            0.0,
        ),
        (RUNG_004_ID, "case-002"): (
            3541,
            "portable-error-after-nested-cause",
            "modeled cause marker carried in portable data survives public reads",
            "structured_portable",
            "portable_json_config",
            False,
            False,
            0.0,
        ),
        (RUNG_004_ID, "case-003"): (
            3542,
            "relaunch-before-structured-error-read",
            "relaunch does not flatten portable structured metadata",
            "structured_portable",
            "portable_json_config",
            False,
            True,
            0.0,
        ),
        (RUNG_005_ID, "case-001"): (
            3550,
            "direct-and-retrieved-handle-dbapi-error",
            "runtime and retrieved handles raise stored connection-invalidated DBAPI error promptly",
            "dbapi_direct",
            "default_pickle",
            False,
            False,
            0.0,
        ),
        (RUNG_005_ID, "case-002"): (
            3551,
            "parent-child-get-result-dbapi-error",
            "parent-child get_result records and propagates stored retry-class DB error without hanging",
            "dbapi_parent_child",
            "default_pickle",
            False,
            False,
            0.0,
        ),
        (RUNG_005_ID, "case-003"): (
            3552,
            "relaunch-and-client-dbapi-error-read",
            "runtime and client reads after relaunch raise the same stored retry-class DB error",
            "dbapi_relaunch_client",
            "default_pickle",
            False,
            True,
            0.0,
        ),
    }
    seed, schedule, focus, workflow_kind, serializer_mode, retry_wrapper, relaunch, late = specs[
        (rung_id, case_id)
    ]
    digest = hashlib.sha1(f"{FRONTIER_ID}:{rung_id}:{case_id}:{seed}".encode()).hexdigest()[
        :10
    ]
    return CasePlan(
        rung_id=rung_id,
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        focus=focus,
        workflow_kind=workflow_kind,
        serializer_mode=serializer_mode,
        database_prefix=f"wio_ser_{digest}",
        expect_retry_wrapper=retry_wrapper,
        relaunch_before_read=relaunch,
        late_read_seconds=late,
    )


def derived_payload(plan: CasePlan) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "frontier": FRONTIER_ID,
        "rung": plan.rung_id,
        "case": plan.case_id,
        "seed": plan.seed,
        "schedule": plan.schedule,
        "nested": {
            "numbers": [plan.seed, plan.seed + 1, plan.seed + 2],
            "flags": {"portable": plan.workflow_kind == "portable"},
        },
    }
    if plan.workflow_kind == "default_non_json_payload":
        payload["non_json_pickle_only"] = {
            "bytes": b"wio-serialization-bytes",
            "tuple": ("tuple", plan.seed),
            "set": {"alpha", f"seed-{plan.seed}"},
        }
    return payload


def marker_for(plan: CasePlan) -> str:
    return f"{ERROR_PREFIX} frontier={FRONTIER_ID} rung={plan.rung_id} case={plan.case_id} seed={plan.seed}"


def modeled_message(plan: CasePlan, *, attempt: int | None = None) -> str:
    suffix = "" if attempt is None else f" attempt={attempt}"
    return f"{marker_for(plan)} kind={plan.workflow_kind}{suffix}"


def structured_message(plan: CasePlan) -> str:
    return (
        f"{STRUCTURED_ERROR_PREFIX} frontier={FRONTIER_ID} "
        f"rung={plan.rung_id} case={plan.case_id} seed={plan.seed}"
    )


def structured_code(plan: CasePlan) -> str:
    return f"WIO_CODE_{plan.seed}_{plan.case_id.replace('-', '_')}"


def structured_cause_marker(plan: CasePlan) -> str:
    return f"WIO_STRUCTURED_CAUSE case={plan.case_id} seed={plan.seed}"


def structured_data(plan: CasePlan) -> dict[str, Any]:
    return {
        "frontier": FRONTIER_ID,
        "rung": plan.rung_id,
        "case": plan.case_id,
        "seed": plan.seed,
        "phase": plan.schedule,
        "modeled_cause": structured_cause_marker(plan),
        "relaunch": plan.relaunch_before_read,
        "nested": {
            "numbers": [plan.seed, plan.seed + 7],
            "labels": ["portable", "structured", plan.case_id],
            "flags": {"portable_error": True, "diagnostic_cause": plan.case_id == "case-002"},
        },
    }


def dbapi_marker(plan: CasePlan) -> str:
    return f"{DBAPI_ERROR_PREFIX}_{plan.seed}_{plan.case_id.replace('-', '_')}"


def parent_dbapi_marker(plan: CasePlan) -> str:
    return f"{PARENT_DBAPI_PREFIX}_{plan.seed}_{plan.case_id.replace('-', '_')}"


def structured_model(plan: CasePlan) -> dict[str, Any]:
    return {
        "name": STRUCTURED_ERROR_NAME,
        "message": structured_message(plan),
        "code": structured_code(plan),
        "data": structured_data(plan),
    }


@DBOS.step()
def modeled_failing_step(message: str, payload: dict[str, Any]) -> None:
    if payload.get("frontier") != FRONTIER_ID:
        raise AssertionError("workload payload lost frontier marker before app failure")
    raise ValueError(message)


@DBOS.step()
def structured_failing_step(plan_json: str) -> None:
    plan = CasePlan(**json.loads(plan_json))
    model = structured_model(plan)
    error = PortableWorkflowError(
        model["message"],
        model["name"],
        model["code"],
        model["data"],
    )
    if plan.case_id == "case-002":
        raise error from ValueError(structured_cause_marker(plan))
    raise error


@DBOS.step()
def dbapi_connection_invalidated_step(plan_json: str, sys_url: str) -> None:
    plan = CasePlan(**json.loads(plan_json))
    marker = dbapi_marker(plan)
    engine = sa.create_engine(sys_url, connect_args={"connect_timeout": 5})
    try:
        with engine.begin() as connection:
            connection.execute(sa.text("SET LOCAL idle_in_transaction_session_timeout = '400'"))
            connection.execute(sa.text(f"SELECT 1 /* {marker}_pre */"))
            time.sleep(1.0)
            connection.execute(sa.text(f"SELECT 1 /* {marker}_post */"))
    finally:
        engine.dispose()


@DBOS.step(retries_allowed=True, interval_seconds=0, max_attempts=3, backoff_rate=1)
def retry_failing_step(marker: str) -> None:
    attempt = RETRY_COUNTS.get(marker, 0) + 1
    RETRY_COUNTS[marker] = attempt
    raise ValueError(f"{marker} kind=retry attempt={attempt}")


@DBOS.workflow()
def default_error_workflow(message: str, payload: dict[str, Any]) -> None:
    modeled_failing_step(message, payload)


@DBOS.workflow(serialization_type=WorkflowSerializationFormat.NATIVE)
def native_error_workflow(message: str, payload: dict[str, Any]) -> None:
    modeled_failing_step(message, payload)


@DBOS.workflow(serialization_type=WorkflowSerializationFormat.PORTABLE)
def portable_error_workflow(message: str, payload: dict[str, Any]) -> None:
    modeled_failing_step(message, payload)


@DBOS.workflow(serialization_type=WorkflowSerializationFormat.PORTABLE)
def structured_portable_error_workflow(plan_json: str) -> None:
    structured_failing_step(plan_json)


@DBOS.workflow()
def dbapi_error_workflow(plan_json: str, sys_url: str) -> None:
    dbapi_connection_invalidated_step(plan_json, sys_url)


@DBOS.workflow()
def dbapi_parent_get_result_workflow(plan_json: str, sys_url: str, child_workflow_id: str) -> None:
    plan = CasePlan(**json.loads(plan_json))
    with SetWorkflowID(child_workflow_id):
        child_handle = DBOS.start_workflow(dbapi_error_workflow, plan_json, sys_url)
    try:
        child_handle.get_result(polling_interval_sec=0.05)
    except Exception as exc:
        raise RuntimeError(parent_dbapi_marker(plan)) from exc
    raise AssertionError("child workflow unexpectedly returned without DBAPI error")


@DBOS.workflow()
def retry_error_workflow(marker: str) -> None:
    retry_failing_step(marker)


def workflow_for(plan: CasePlan) -> Callable[..., None]:
    if plan.workflow_kind == "native":
        return native_error_workflow
    if plan.workflow_kind == "portable":
        return portable_error_workflow
    if plan.workflow_kind == "retry":
        return retry_error_workflow
    if plan.workflow_kind == "structured_portable":
        return structured_portable_error_workflow
    return default_error_workflow


def error_signature(exc: BaseException | None) -> dict[str, Any]:
    if exc is None:
        return {"type": None, "message": None, "repr": None, "children": []}
    children = []
    for child in getattr(exc, "errors", []) or []:
        children.append(error_signature(child))
    return {
        "type": type(exc).__name__,
        "module": type(exc).__module__,
        "base_types": [cls.__name__ for cls in type(exc).mro()],
        "message": str(exc),
        "repr": repr(exc),
        "portable_name": getattr(exc, "name", None),
        "portable_code": getattr(exc, "code", None),
        "portable_data": getattr(exc, "data", None),
        "cause": error_signature(exc.__cause__) if getattr(exc, "__cause__", None) is not None else None,
        "context": error_signature(exc.__context__) if getattr(exc, "__context__", None) is not None else None,
        "children": children,
    }


def flatten_signature(signature: dict[str, Any]) -> list[dict[str, Any]]:
    out = [signature]
    for child in signature.get("children", []) or []:
        out.extend(flatten_signature(child))
    if signature.get("cause") is not None:
        out.extend(flatten_signature(signature["cause"]))
    if signature.get("context") is not None:
        out.extend(flatten_signature(signature["context"]))
    return out


def extract_error_text(signature: dict[str, Any]) -> str:
    pieces = []
    for item in flatten_signature(signature):
        for key in ("type", "message", "repr", "portable_name", "portable_code"):
            value = item.get(key)
            if value is not None:
                pieces.append(str(value))
    return "\n".join(pieces)


def check_error_signature(
    name: str,
    signature: dict[str, Any],
    marker: str,
    *,
    allow_retry_wrapper: bool,
) -> None:
    text = extract_error_text(signature)
    lowered = text.lower()
    type_names = {str(item.get("type")) for item in flatten_signature(signature)}
    portable_names = {
        str(item.get("portable_name"))
        for item in flatten_signature(signature)
        if item.get("portable_name") is not None
    }
    has_modeled_error = (
        marker in text
        and (
            "ValueError" in type_names
            or "ValueError" in portable_names
            or isinstance(signature, PortableWorkflowError)
            or "valueerror" in lowered
        )
    )
    if allow_retry_wrapper:
        has_modeled_error = has_modeled_error and (
            "DBOSMaxStepRetriesExceeded" in type_names
            or "maximum of 3 retries" in text
            or any("attempt=3" in str(item.get("message")) for item in flatten_signature(signature))
        )
    noise_hits = [noise for noise in FORBIDDEN_NOISE if noise in lowered]
    invariant(
        name,
        has_modeled_error and not noise_hits,
        marker=marker,
        signature=signature,
        forbidden_noise=noise_hits,
        allow_retry_wrapper=allow_retry_wrapper,
    )


def structured_signature_matches(signature: dict[str, Any], model: dict[str, Any]) -> bool:
    for item in flatten_signature(signature):
        if (
            item.get("portable_name") == model["name"]
            and item.get("message") == model["message"]
            and item.get("portable_code") == model["code"]
            and item.get("portable_data") == model["data"]
        ):
            return True
    return False


def check_structured_signature(name: str, signature: dict[str, Any], model: dict[str, Any]) -> None:
    invariant(
        name,
        structured_signature_matches(signature, model),
        expected=model,
        signature=signature,
    )


def raw_error_rows(sys_url: str, workflow_id: str) -> dict[str, Any]:
    engine = sa.create_engine(sys_url, connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as conn:
            status = conn.execute(
                sa.text(
                    """
                    SELECT workflow_uuid, status, error, serialization
                    FROM dbos.workflow_status
                    WHERE workflow_uuid = :workflow_id
                    """
                ),
                {"workflow_id": workflow_id},
            ).mappings().one()
            steps = conn.execute(
                sa.text(
                    """
                    SELECT function_id, function_name, error, serialization
                    FROM dbos.operation_outputs
                    WHERE workflow_uuid = :workflow_id
                    ORDER BY function_id
                    """
                ),
                {"workflow_id": workflow_id},
            ).mappings().all()
    finally:
        engine.dispose()

    def decode(row: Any) -> dict[str, Any]:
        raw = dict(row)
        raw_error = raw.get("error")
        raw["decoded_error"] = None
        if raw_error is not None and raw.get("serialization") == PORTABLE_JSON_SERIALIZATION:
            try:
                raw["decoded_error"] = json.loads(raw_error)
            except Exception as exc:
                raw["decoded_error"] = {"decode_error": f"{type(exc).__name__}: {exc}"}
        elif raw_error is not None:
            try:
                raw["decoded_error"] = error_signature(
                    deserialize_exception(
                        raw_error,
                        raw.get("serialization"),
                        DBOSDefaultSerializer,
                    )
                )
            except Exception as exc:
                raw["decoded_error"] = {"decode_error": f"{type(exc).__name__}: {exc}"}
        return raw

    return {
        "workflow_status": decode(status),
        "operation_outputs": [decode(row) for row in steps],
    }


def check_raw_structured_rows(raw_rows: dict[str, Any], model: dict[str, Any]) -> None:
    status_row = raw_rows["workflow_status"]
    invariant(
        "structured_raw_workflow_error_is_portable_json",
        status_row.get("serialization") == PORTABLE_JSON_SERIALIZATION,
        status_row=status_row,
    )
    invariant(
        "structured_raw_workflow_error_preserves_metadata",
        status_row.get("decoded_error") == model,
        expected=model,
        status_row=status_row,
    )
    step_error_rows = [
        row
        for row in raw_rows["operation_outputs"]
        if row.get("error") is not None
    ]
    invariant(
        "structured_failed_step_error_row_exists",
        bool(step_error_rows),
        raw_rows=raw_rows,
    )
    invariant(
        "structured_raw_step_error_preserves_metadata",
        any(
            row.get("serialization") == PORTABLE_JSON_SERIALIZATION
            and row.get("decoded_error") == model
            for row in step_error_rows
        ),
        expected=model,
        step_error_rows=step_error_rows,
    )


def capture_exception(fn: Callable[[], Any]) -> tuple[Any, BaseException | None]:
    try:
        return fn(), None
    except BaseException as exc:
        return None, exc


def bounded_capture(
    name: str,
    fn: Callable[[], Any],
    timeout_seconds: float = RESULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    result_queue: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=1)

    def runner() -> None:
        try:
            result_queue.put(("returned", fn()), block=False)
        except BaseException as exc:
            result_queue.put(("exception", exc), block=False)

    started_at = time.monotonic()
    thread = threading.Thread(target=runner, name=f"wio-{name}", daemon=True)
    thread.start()
    thread.join(timeout_seconds)
    elapsed = time.monotonic() - started_at
    if thread.is_alive():
        return {
            "name": name,
            "timed_out": True,
            "elapsed_seconds": elapsed,
            "timeout_seconds": timeout_seconds,
            "exception": None,
            "returned": None,
        }
    kind, value = result_queue.get_nowait()
    return {
        "name": name,
        "timed_out": False,
        "elapsed_seconds": elapsed,
        "timeout_seconds": timeout_seconds,
        "exception": error_signature(value) if kind == "exception" else None,
        "returned": repr(value) if kind == "returned" else None,
    }


def dbapi_signature_matches(signature: dict[str, Any], marker: str) -> bool:
    text = extract_error_text(signature)
    lowered = text.lower()
    for item in flatten_signature(signature):
        base_types = {str(value) for value in item.get("base_types", [])}
        type_name = str(item.get("type"))
        if (
            "DBAPIError" in base_types
            or type_name in {"DBAPIError", "InternalError", "OperationalError"}
        ) and ("idle-in-transaction" in lowered or marker in text):
            return True
    return False


def check_dbapi_outcome(name: str, outcome: dict[str, Any], marker: str) -> None:
    invariant(
        f"{name}_completes_within_bound",
        not outcome["timed_out"],
        outcome=outcome,
        marker=marker,
    )
    signature = outcome.get("exception")
    invariant(
        f"{name}_raises_modeled_dbapi_error",
        signature is not None and dbapi_signature_matches(signature, marker),
        outcome=outcome,
        marker=marker,
    )


def check_parent_outcome(
    name: str,
    outcome: dict[str, Any],
    plan: CasePlan,
    *,
    require_child_dbapi_chain: bool,
) -> None:
    invariant(
        f"{name}_completes_within_bound",
        not outcome["timed_out"],
        outcome=outcome,
    )
    signature = outcome.get("exception")
    text = extract_error_text(signature or {})
    invariant(
        f"{name}_raises_modeled_parent_wrapper",
        signature is not None
        and parent_dbapi_marker(plan) in text
        and (
            not require_child_dbapi_chain
            or dbapi_signature_matches(signature, dbapi_marker(plan))
        ),
        outcome=outcome,
        expected_parent_marker=parent_dbapi_marker(plan),
        expected_dbapi_marker=dbapi_marker(plan),
        require_child_dbapi_chain=require_child_dbapi_chain,
    )


def wait_for_terminal_status(
    workflow_id: str,
    timeout_seconds: float = RESULT_TIMEOUT_SECONDS,
) -> Any:
    deadline = time.monotonic() + timeout_seconds
    last_status = None
    while time.monotonic() < deadline:
        last_status = DBOS.get_workflow_status(workflow_id)
        if last_status is not None and last_status.status in {"SUCCESS", "ERROR", "CANCELLED"}:
            return last_status
        time.sleep(0.05)
    invariant(
        "workflow_reaches_terminal_status",
        False,
        workflow_id=workflow_id,
        last_status=repr(last_status),
        timeout_seconds=timeout_seconds,
    )


def check_error_status(name: str, status: Any, marker: str) -> dict[str, Any]:
    signature = error_signature(status.error)
    invariant(
        f"{name}_is_terminal_error",
        status.status == "ERROR",
        workflow_id=status.workflow_id,
        status=status.status,
        error=signature,
    )
    invariant(
        f"{name}_error_is_modeled_dbapi",
        dbapi_signature_matches(signature, marker),
        workflow_id=status.workflow_id,
        error=signature,
        marker=marker,
    )
    return {
        "workflow_id": status.workflow_id,
        "status": status.status,
        "name": status.name,
        "app_version": status.app_version,
        "error": signature,
    }


def check_parent_error_status(name: str, status: Any, plan: CasePlan) -> dict[str, Any]:
    signature = error_signature(status.error)
    text = extract_error_text(signature)
    invariant(
        f"{name}_is_terminal_error",
        status.status == "ERROR",
        workflow_id=status.workflow_id,
        status=status.status,
        error=signature,
    )
    invariant(
        f"{name}_preserves_parent_marker",
        parent_dbapi_marker(plan) in text,
        workflow_id=status.workflow_id,
        error=signature,
        marker=parent_dbapi_marker(plan),
    )
    return {
        "workflow_id": status.workflow_id,
        "status": status.status,
        "name": status.name,
        "app_version": status.app_version,
        "error": signature,
    }


def get_single_status(workflow_id: str) -> Any:
    statuses = DBOS.list_workflows(workflow_ids=[workflow_id])
    invariant(
        "status_row_exists",
        len(statuses) == 1,
        workflow_id=workflow_id,
        observed_count=len(statuses),
    )
    return statuses[0]


def run_modeled_workflow(plan: CasePlan, workflow_id: str) -> BaseException:
    wf = workflow_for(plan)
    marker = marker_for(plan)
    if plan.workflow_kind == "retry":
        RETRY_COUNTS.pop(marker, None)
        _, exc = capture_exception(lambda: wf(marker))
    else:
        _, exc = capture_exception(lambda: wf(modeled_message(plan), derived_payload(plan)))
    signature = error_signature(exc)
    invariant(
        "modeled_application_exception_observed",
        exc is not None and marker in extract_error_text(signature),
        workflow_id=workflow_id,
        exception=signature,
        marker=marker,
    )
    return exc


def run_structured_workflow(plan: CasePlan, workflow_id: str) -> BaseException:
    wf = workflow_for(plan)
    _, exc = capture_exception(lambda: wf(json.dumps(asdict(plan), sort_keys=True)))
    signature = error_signature(exc)
    model = structured_model(plan)
    invariant(
        "structured_application_exception_observed",
        exc is not None and structured_signature_matches(signature, model),
        workflow_id=workflow_id,
        exception=signature,
        expected=model,
    )
    return exc


def collect_observations(
    plan: CasePlan,
    workflow_id: str,
    sys_url: str,
) -> dict[str, Any]:
    marker = marker_for(plan)
    serializer = serializer_for_mode(plan.serializer_mode)
    status = get_single_status(workflow_id)
    status_signature = error_signature(status.error)
    check_error_signature(
        "status_error_preserves_application_error",
        status_signature,
        marker,
        allow_retry_wrapper=plan.expect_retry_wrapper,
    )

    _, handle_exc = capture_exception(lambda: DBOS.retrieve_workflow(workflow_id).get_result())
    handle_signature = error_signature(handle_exc)
    check_error_signature(
        "handle_error_preserves_application_error",
        handle_signature,
        marker,
        allow_retry_wrapper=plan.expect_retry_wrapper,
    )

    client = DBOSClient(system_database_url=sys_url, serializer=serializer)
    try:
        client_statuses = client.list_workflows(workflow_ids=[workflow_id])
        invariant(
            "client_status_row_exists",
            len(client_statuses) == 1,
            workflow_id=workflow_id,
            observed_count=len(client_statuses),
        )
        client_status_signature = error_signature(client_statuses[0].error)
        check_error_signature(
            "client_status_error_preserves_application_error",
            client_status_signature,
            marker,
            allow_retry_wrapper=plan.expect_retry_wrapper,
        )
        _, client_handle_exc = capture_exception(
            lambda: client.retrieve_workflow(workflow_id).get_result()
        )
        client_handle_signature = error_signature(client_handle_exc)
        check_error_signature(
            "client_handle_error_preserves_application_error",
            client_handle_signature,
            marker,
            allow_retry_wrapper=plan.expect_retry_wrapper,
        )
        client_steps = client.list_workflow_steps(workflow_id)
    finally:
        client.destroy()

    steps = DBOS.list_workflow_steps(workflow_id)
    step_sigs = [error_signature(step.get("error")) for step in steps]
    client_step_sigs = [error_signature(step.get("error")) for step in client_steps]
    invariant(
        "failed_step_row_recorded",
        any(marker in extract_error_text(sig) for sig in step_sigs),
        workflow_id=workflow_id,
        step_errors=step_sigs,
        marker=marker,
    )
    invariant(
        "client_failed_step_row_recorded",
        any(marker in extract_error_text(sig) for sig in client_step_sigs),
        workflow_id=workflow_id,
        step_errors=client_step_sigs,
        marker=marker,
    )
    if plan.expect_retry_wrapper:
        invariant(
            "retry_attempt_count_recorded",
            RETRY_COUNTS.get(marker) == 3 or sum(marker in extract_error_text(sig) for sig in step_sigs) >= 1,
            retry_counter=RETRY_COUNTS.get(marker),
            step_errors=step_sigs,
        )

    signature_texts = [
        extract_error_text(status_signature),
        extract_error_text(handle_signature),
        extract_error_text(client_status_signature),
        extract_error_text(client_handle_signature),
    ]
    invariant(
        "public_error_paths_agree_on_marker",
        all(marker in text for text in signature_texts),
        marker=marker,
        signature_texts=signature_texts,
    )

    return {
        "status": {
            "workflow_id": status.workflow_id,
            "status": status.status,
            "name": status.name,
            "app_version": status.app_version,
            "error": status_signature,
        },
        "handle_error": handle_signature,
        "client_status_error": client_status_signature,
        "client_handle_error": client_handle_signature,
        "steps": step_sigs,
        "client_steps": client_step_sigs,
    }


def collect_structured_observations(
    plan: CasePlan,
    workflow_id: str,
    sys_url: str,
) -> dict[str, Any]:
    model = structured_model(plan)
    status = get_single_status(workflow_id)
    status_signature = error_signature(status.error)

    _, handle_exc = capture_exception(lambda: DBOS.retrieve_workflow(workflow_id).get_result())
    handle_signature = error_signature(handle_exc)

    client = DBOSClient(system_database_url=sys_url, serializer=DBOSPortableJSONSerializer())
    try:
        client_statuses = client.list_workflows(workflow_ids=[workflow_id])
        invariant(
            "structured_client_status_row_exists",
            len(client_statuses) == 1,
            workflow_id=workflow_id,
            observed_count=len(client_statuses),
        )
        client_status_signature = error_signature(client_statuses[0].error)
        _, client_handle_exc = capture_exception(
            lambda: client.retrieve_workflow(workflow_id).get_result()
        )
        client_handle_signature = error_signature(client_handle_exc)
        client_steps = client.list_workflow_steps(workflow_id)
    finally:
        client.destroy()

    steps = DBOS.list_workflow_steps(workflow_id)
    step_sigs = [error_signature(step.get("error")) for step in steps]
    client_step_sigs = [error_signature(step.get("error")) for step in client_steps]
    raw_rows = raw_error_rows(sys_url, workflow_id)

    observations = {
        "model": model,
        "status": {
            "workflow_id": status.workflow_id,
            "status": status.status,
            "name": status.name,
            "app_version": status.app_version,
            "error": status_signature,
        },
        "handle_error": handle_signature,
        "client_status_error": client_status_signature,
        "client_handle_error": client_handle_signature,
        "steps": step_sigs,
        "client_steps": client_step_sigs,
        "raw_rows": raw_rows,
        "diagnostic_python_cause_context": {
            "status": {
                "cause": status_signature.get("cause"),
                "context": status_signature.get("context"),
            },
            "handle": {
                "cause": handle_signature.get("cause"),
                "context": handle_signature.get("context"),
            },
        },
    }

    return observations


def check_structured_observations(observations: dict[str, Any]) -> None:
    model = observations["model"]
    status_signature = observations["status"]["error"]
    handle_signature = observations["handle_error"]
    client_status_signature = observations["client_status_error"]
    client_handle_signature = observations["client_handle_error"]
    step_sigs = observations["steps"]
    client_step_sigs = observations["client_steps"]
    raw_rows = observations["raw_rows"]

    check_raw_structured_rows(raw_rows, model)
    check_structured_signature("structured_status_preserves_metadata", status_signature, model)
    check_structured_signature("structured_handle_preserves_metadata", handle_signature, model)
    check_structured_signature("structured_client_status_preserves_metadata", client_status_signature, model)
    check_structured_signature("structured_client_handle_preserves_metadata", client_handle_signature, model)
    invariant(
        "structured_runtime_step_preserves_metadata",
        any(structured_signature_matches(sig, model) for sig in step_sigs),
        expected=model,
        step_errors=step_sigs,
    )
    invariant(
        "structured_client_step_preserves_metadata",
        any(structured_signature_matches(sig, model) for sig in client_step_sigs),
        expected=model,
        step_errors=client_step_sigs,
    )
    invariant(
        "structured_public_paths_agree_on_metadata",
        all(
            structured_signature_matches(sig, model)
            for sig in [
                status_signature,
                handle_signature,
                client_status_signature,
                client_handle_signature,
            ]
        ),
        expected=model,
        signatures=[
            status_signature,
            handle_signature,
            client_status_signature,
            client_handle_signature,
        ],
    )


def check_runtime_and_client_statuses(workflow_id: str, sys_url: str, marker: str) -> dict[str, Any]:
    runtime_status = DBOS.get_workflow_status(workflow_id)
    invariant(
        "runtime_get_status_row_exists",
        runtime_status is not None,
        workflow_id=workflow_id,
    )
    runtime_status_obs = check_error_status("runtime_get_status", runtime_status, marker)

    runtime_list = DBOS.list_workflows(workflow_ids=[workflow_id])
    invariant(
        "runtime_list_status_row_exists",
        len(runtime_list) == 1,
        workflow_id=workflow_id,
        observed_count=len(runtime_list),
    )
    runtime_list_obs = check_error_status("runtime_list_status", runtime_list[0], marker)

    client = DBOSClient(system_database_url=sys_url, serializer=DBOSDefaultSerializer)
    try:
        client_status = client.retrieve_workflow(workflow_id).get_status()
        client_status_obs = check_error_status("client_handle_status", client_status, marker)
        client_list = client.list_workflows(workflow_ids=[workflow_id])
        invariant(
            "client_list_status_row_exists",
            len(client_list) == 1,
            workflow_id=workflow_id,
            observed_count=len(client_list),
        )
        client_list_obs = check_error_status("client_list_status", client_list[0], marker)
    finally:
        client.destroy()

    return {
        "runtime_get_status": runtime_status_obs,
        "runtime_list_status": runtime_list_obs,
        "client_handle_status": client_status_obs,
        "client_list_status": client_list_obs,
    }


def check_raw_dbapi_rows(name: str, raw_rows: dict[str, Any], marker: str) -> None:
    status_row = raw_rows["workflow_status"]
    step_error_rows = [
        row for row in raw_rows["operation_outputs"] if row.get("error") is not None
    ]
    invariant(
        f"{name}_raw_workflow_error_row_exists",
        status_row.get("status") == "ERROR" and status_row.get("error") is not None,
        raw_rows=raw_rows,
    )
    invariant(
        f"{name}_raw_step_error_row_exists",
        bool(step_error_rows),
        raw_rows=raw_rows,
    )
    invariant(
        f"{name}_raw_rows_preserve_dbapi_fault_marker",
        dbapi_signature_matches(status_row.get("decoded_error") or {}, marker)
        and any(
            dbapi_signature_matches(row.get("decoded_error") or {}, marker)
            for row in step_error_rows
        ),
        marker=marker,
        raw_rows=raw_rows,
    )


def collect_dbapi_direct_observations(
    plan: CasePlan,
    workflow_id: str,
    sys_url: str,
    *,
    include_original_handle: bool,
    start_workflow: bool = True,
) -> dict[str, Any]:
    marker = dbapi_marker(plan)
    observations: dict[str, Any] = {
        "marker": marker,
        "timeout_seconds": RESULT_TIMEOUT_SECONDS,
        "retrievals": {},
    }

    if include_original_handle:
        with SetWorkflowID(workflow_id):
            handle = DBOS.start_workflow(dbapi_error_workflow, json.dumps(asdict(plan), sort_keys=True), sys_url)
        original_outcome = bounded_capture(
            "runtime_original_handle_dbapi",
            lambda: handle.get_result(polling_interval_sec=0.05),
        )
        check_dbapi_outcome("runtime_original_handle", original_outcome, marker)
        observations["retrievals"]["runtime_original_handle"] = original_outcome
    elif start_workflow:
        with SetWorkflowID(workflow_id):
            DBOS.start_workflow(dbapi_error_workflow, json.dumps(asdict(plan), sort_keys=True), sys_url)
        terminal_status = wait_for_terminal_status(workflow_id)
        check_error_status("pre_relaunch_terminal_status", terminal_status, marker)

    status_observations = check_runtime_and_client_statuses(workflow_id, sys_url, marker)

    retrieved_outcome = bounded_capture(
        "runtime_retrieved_handle_dbapi",
        lambda: DBOS.retrieve_workflow(workflow_id).get_result(polling_interval_sec=0.05),
    )
    check_dbapi_outcome("runtime_retrieved_handle", retrieved_outcome, marker)

    client = DBOSClient(system_database_url=sys_url, serializer=DBOSDefaultSerializer)
    try:
        client_outcome = bounded_capture(
            "client_retrieved_handle_dbapi",
            lambda: client.retrieve_workflow(workflow_id).get_result(polling_interval_sec=0.05),
        )
        check_dbapi_outcome("client_retrieved_handle", client_outcome, marker)
    finally:
        client.destroy()

    raw_rows = raw_error_rows(sys_url, workflow_id)
    check_raw_dbapi_rows("dbapi_direct", raw_rows, marker)
    observations["statuses"] = status_observations
    observations["retrievals"]["runtime_retrieved_handle"] = retrieved_outcome
    observations["retrievals"]["client_retrieved_handle"] = client_outcome
    observations["raw_rows"] = raw_rows
    return observations


def collect_dbapi_parent_child_observations(
    plan: CasePlan,
    parent_workflow_id: str,
    child_workflow_id: str,
    sys_url: str,
) -> dict[str, Any]:
    with SetWorkflowID(parent_workflow_id):
        parent_handle = DBOS.start_workflow(
            dbapi_parent_get_result_workflow,
            json.dumps(asdict(plan), sort_keys=True),
            sys_url,
            child_workflow_id,
        )
    parent_outcome = bounded_capture(
        "parent_original_handle_dbapi",
        lambda: parent_handle.get_result(polling_interval_sec=0.05),
        timeout_seconds=PARENT_RESULT_TIMEOUT_SECONDS,
    )
    check_parent_outcome(
        "parent_original_handle",
        parent_outcome,
        plan,
        require_child_dbapi_chain=True,
    )

    parent_status = DBOS.get_workflow_status(parent_workflow_id)
    child_status = DBOS.get_workflow_status(child_workflow_id)
    invariant("parent_status_row_exists", parent_status is not None, workflow_id=parent_workflow_id)
    invariant("child_status_row_exists", child_status is not None, workflow_id=child_workflow_id)
    parent_status_obs = check_parent_error_status("parent_status", parent_status, plan)
    child_status_obs = check_error_status("child_status", child_status, dbapi_marker(plan))

    parent_retrieved = bounded_capture(
        "parent_retrieved_handle_dbapi",
        lambda: DBOS.retrieve_workflow(parent_workflow_id).get_result(polling_interval_sec=0.05),
    )
    check_parent_outcome(
        "parent_retrieved_handle",
        parent_retrieved,
        plan,
        require_child_dbapi_chain=False,
    )
    child_retrieved = bounded_capture(
        "child_retrieved_handle_dbapi",
        lambda: DBOS.retrieve_workflow(child_workflow_id).get_result(polling_interval_sec=0.05),
    )
    check_dbapi_outcome("child_retrieved_handle", child_retrieved, dbapi_marker(plan))

    raw_parent = raw_error_rows(sys_url, parent_workflow_id)
    raw_child = raw_error_rows(sys_url, child_workflow_id)
    check_raw_dbapi_rows("dbapi_child", raw_child, dbapi_marker(plan))
    return {
        "parent_workflow_id": parent_workflow_id,
        "child_workflow_id": child_workflow_id,
        "parent_marker": parent_dbapi_marker(plan),
        "child_marker": dbapi_marker(plan),
        "retrievals": {
            "parent_original_handle": parent_outcome,
            "parent_retrieved_handle": parent_retrieved,
            "child_retrieved_handle": child_retrieved,
        },
        "statuses": {
            "parent": parent_status_obs,
            "child": child_status_obs,
        },
        "raw_rows": {
            "parent": raw_parent,
            "child": raw_child,
        },
    }


def collect_dbapi_observations(
    plan: CasePlan,
    workflow_id: str,
    sys_url: str,
) -> dict[str, Any]:
    if plan.case_id == "case-002":
        return collect_dbapi_parent_child_observations(
            plan,
            workflow_id,
            f"{workflow_id}-child",
            sys_url,
        )
    if plan.case_id == "case-003":
        observations = collect_dbapi_direct_observations(
            plan,
            workflow_id,
            sys_url,
            include_original_handle=False,
            start_workflow=True,
        )
        return observations
    return collect_dbapi_direct_observations(
        plan,
        workflow_id,
        sys_url,
        include_original_handle=True,
        start_workflow=True,
    )


def launch_dbos(config: DBOSConfig) -> None:
    DBOS(config=config)
    DBOS.launch()


def relaunch_dbos(config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=False)
    launch_dbos(config)


def run_case(plan: CasePlan, artifacts_root: Path) -> dict[str, Any]:
    artifacts = artifacts_root / plan.rung_id / plan.case_id
    artifacts.mkdir(parents=True, exist_ok=True)
    app_url, sys_url, admin_masked = prepare_databases(plan.database_prefix, artifacts)
    workflow_id = f"{FRONTIER_ID}-{plan.rung_id}-{plan.case_id}-{plan.seed}"
    config = make_config(plan, app_url, sys_url)
    case_json = {
        **asdict(plan),
        "frontier": FRONTIER_ID,
        "prompt_path": PROMPT_PATH,
        "workflow_id": workflow_id,
        "app_url": app_url.replace(str(make_url(app_url).password or ""), "***"),
        "system_database_url": sys_url.replace(str(make_url(sys_url).password or ""), "***"),
        "admin_url": admin_masked,
        "dbos_serializer_env": {
            "WIO_SERIALIZER_OVERRIDE": os.environ.get("WIO_SERIALIZER_OVERRIDE"),
            "DBOS__SERIALIZER": os.environ.get("DBOS__SERIALIZER"),
            "DBOS_SERIALIZER": os.environ.get("DBOS_SERIALIZER"),
        },
        "derived_payload": repr(derived_payload(plan)),
        "expected_marker": marker_for(plan),
        "expected_dbapi_marker": dbapi_marker(plan) if plan.rung_id == RUNG_005_ID else None,
        "expected_parent_marker": parent_dbapi_marker(plan) if plan.rung_id == RUNG_005_ID else None,
        "structured_model": structured_model(plan) if plan.rung_id == RUNG_004_ID else None,
    }
    write_json(artifacts / "case.json", case_json)

    observations: dict[str, Any] = {}
    try:
        event("case_start", frontier=FRONTIER_ID, rung=plan.rung_id, case=plan.case_id, seed=plan.seed)
        launch_dbos(config)
        if plan.rung_id == RUNG_005_ID:
            if plan.case_id == "case-003":
                with SetWorkflowID(workflow_id):
                    DBOS.start_workflow(
                        dbapi_error_workflow,
                        json.dumps(asdict(plan), sort_keys=True),
                        sys_url,
                    )
                before_status = wait_for_terminal_status(workflow_id)
                before = check_error_status(
                    "dbapi_pre_relaunch_terminal_status",
                    before_status,
                    dbapi_marker(plan),
                )
                relaunch_dbos(config)
                after_status = DBOS.get_workflow_status(workflow_id)
                invariant(
                    "dbapi_post_relaunch_status_row_exists",
                    after_status is not None,
                    workflow_id=workflow_id,
                )
                after = check_error_status(
                    "dbapi_post_relaunch_terminal_status",
                    after_status,
                    dbapi_marker(plan),
                )
                write_json(
                    artifacts / "relaunch-status.json",
                    {"before": before, "after": after},
                )
                observations = collect_dbapi_direct_observations(
                    plan,
                    workflow_id,
                    sys_url,
                    include_original_handle=False,
                    start_workflow=False,
                )
            else:
                observations = collect_dbapi_observations(plan, workflow_id, sys_url)
            write_json(artifacts / "observations.json", observations)
        else:
            with SetWorkflowID(workflow_id):
                if plan.rung_id == RUNG_004_ID:
                    first_exc = run_structured_workflow(plan, workflow_id)
                else:
                    first_exc = run_modeled_workflow(plan, workflow_id)
            write_json(artifacts / "initial-exception.json", error_signature(first_exc))

        if plan.relaunch_before_read and plan.rung_id != RUNG_005_ID:
            before = error_signature(get_single_status(workflow_id).error)
            relaunch_dbos(config)
            after = error_signature(get_single_status(workflow_id).error)
            if plan.rung_id == RUNG_004_ID:
                model = structured_model(plan)
                invariant(
                    "structured_relaunch_does_not_rewrite_terminal_error",
                    structured_signature_matches(before, model)
                    and structured_signature_matches(after, model),
                    before=before,
                    after=after,
                    expected=model,
                )
            else:
                invariant(
                    "relaunch_does_not_rewrite_terminal_error",
                    marker_for(plan) in extract_error_text(before)
                    and marker_for(plan) in extract_error_text(after),
                    before=before,
                    after=after,
                )

        if plan.late_read_seconds:
            time.sleep(plan.late_read_seconds)

        if plan.rung_id == RUNG_005_ID:
            pass
        elif plan.rung_id == RUNG_004_ID:
            observations = collect_structured_observations(plan, workflow_id, sys_url)
            write_json(artifacts / "observations.json", observations)
            check_structured_observations(observations)
        else:
            observations = collect_observations(plan, workflow_id, sys_url)
            write_json(artifacts / "observations.json", observations)
        event("case_complete", rung=plan.rung_id, case=plan.case_id, status="passed")
        return {"case": asdict(plan), "workflow_id": workflow_id, "status": "passed"}
    finally:
        try:
            DBOS.destroy(destroy_registry=False)
        except Exception as exc:
            event("dbos_destroy_best_effort_failed", error_type=type(exc).__name__, error=str(exc))
        drop_databases(plan.database_prefix)


def run_selected(args: argparse.Namespace) -> int:
    rung_id = normalize_rung(args.rung)
    case_ids = case_ids_for_rung(rung_id) if args.all_cases else [args.case]
    if not args.all_cases and args.case is None:
        raise SetupBlock("--case is required unless --all-cases is set")

    artifacts_root = Path(args.artifacts_dir)
    results = []
    for case_id in case_ids:
        plan = make_plan(rung_id, case_id)
        results.append(run_case(plan, artifacts_root))
    write_json(artifacts_root / rung_id / "summary.json", results)
    event("rung_complete", rung=rung_id, cases=len(results), status="passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rung", required=True)
    parser.add_argument("--case")
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true", help="accepted for WIO command compatibility")
    parser.add_argument(
        "--artifacts-dir",
        default="/tmp/wio-artifacts/serialization-error-fidelity",
    )
    args = parser.parse_args()
    try:
        return run_selected(args)
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 44
    except WorkloadFailure as exc:
        print(f"FINDING-CANDIDATE {exc}", flush=True)
        return 10


if __name__ == "__main__":
    raise SystemExit(main())
