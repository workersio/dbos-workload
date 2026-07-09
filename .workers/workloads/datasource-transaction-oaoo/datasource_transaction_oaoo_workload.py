#!/usr/bin/env python3
"""Fresh WIO workload for DBOS datasource/transaction OAOO behavior.

Frontier: datasource-transaction-oaoo
Rungs:
  - rung-000-transaction-smoke
  - rung-001-transaction-replay-once
  - rung-002-rollback-enqueue-boundary
  - rung-003-retry-cleanup-failure
  - rung-004-bounded-seed-sweep
  - rung-005-transactional-send-visibility
  - rung-006-datasource-dbapi-retry-liveness
Evidence key:
  evidence-key:events/frontier_designer-20260620T072709969639000Z.prompt.md
Protected product promise:
  Application datasource transactions execute exactly once from the user's
  perspective while DBOS records operation outputs and recovers/replays the
  transaction result without duplicate app side effects. Caller-owned
  transactional sends become visible only on commit and disappear on rollback.
Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py \
    --rung rung-001-transaction-replay-once --case case-001
Seed policy:
  Exact rung seeds are encoded below; each case writes the derived case JSON and
  operation schedule under the artifact directory.
Invariant oracle:
  Independent intent model, app-side ledger rows, datasource_outputs rows,
  DBOS operation_outputs rows, workflow results, and replay call counts agree.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import threading
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

    from dbos import (
        AsyncSQLAlchemyDatasource,
        DBOS,
        DBOSClient,
        DBOSConfig,
        SQLAlchemyDatasource,
        SendMessage,
        SetWorkflowID,
    )
    from dbos._schemas.datasource_database import DatasourceSchema
    from dbos._schemas.system_database import SystemSchema
    from dbos._workflow_commands import garbage_collect
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "datasource-transaction-oaoo"
RUNG_000_ID = "rung-000-transaction-smoke"
RUNG_001_ID = "rung-001-transaction-replay-once"
RUNG_002_ID = "rung-002-rollback-enqueue-boundary"
RUNG_003_ID = "rung-003-retry-cleanup-failure"
RUNG_004_ID = "rung-004-bounded-seed-sweep"
RUNG_005_ID = "rung-005-transactional-send-visibility"
RUNG_006_ID = "rung-006-datasource-dbapi-retry-liveness"
APP_ID = "wio-ds-tx-oaoo"
APP_VERSION = "wio-datasource-transaction-rungs-000-006"
PROMPT_PATH = "evidence-key:events/frontier_designer-20260620T072709969639000Z.prompt.md"

LEDGER_TABLE = "datasource_oaoo_ledger"
ENQUEUE_LEDGER_TABLE = "datasource_enqueue_tx_ledger"
RETRY_PROBE_TABLE = "datasource_retry_probe"


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
    workflow_id: str
    first_payload: str
    second_payload: str | None = None
    delete_sys_workflow_before_replay: bool = False
    variant: str | None = None


_sync_ds: SQLAlchemyDatasource | None = None
_async_ds: AsyncSQLAlchemyDatasource | None = None
_step_call_counts: dict[str, int] = {}
_retry_lock = threading.Lock()
_pg_serializable_first_reads: dict[str, set[str]] = {}
_pg_serializable_barriers: dict[str, threading.Event] = {}
_pg_deadlock_first_locks: dict[str, set[str]] = {}
_pg_deadlock_barriers: dict[str, threading.Event] = {}


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
    if os.environ.get("WIO_DATASOURCE_KEEP_DATABASES") == "1":
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
        "executor_id": f"wio-datasource-{plan.case_id}",
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        "run_admin_server": False,
        "runtimeConfig": {"max_executor_threads": 32},
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
        return ["case-001", "case-002", "case-003", "case-004", "case-005", "case-006"]
    if rung_id == RUNG_004_ID:
        return [f"case-{index:03d}" for index in range(1, 25)]
    if rung_id == RUNG_005_ID:
        return ["case-001", "case-002", "case-003"]
    if rung_id == RUNG_006_ID:
        return ["case-001", "case-002", "case-003", "case-004"]
    raise SetupBlock(f"unsupported rung {rung_id}")


def make_plan(rung: str, case_id: str) -> CasePlan:
    rung_id = normalize_rung(rung)
    if case_id not in case_ids_for_rung(rung_id):
        raise SetupBlock(f"unknown case {case_id} for {rung_id}")

    def plan(
        seed: int,
        schedule: str,
        focus: str,
        second_payload: str | None = None,
        delete_sys: bool = False,
        variant: str | None = None,
    ) -> CasePlan:
        prefix_case = case_id.replace("-", "_")
        suffix_input = f"{rung_id}:{case_id}:{seed}".encode("utf-8")
        stable_suffix = hashlib.sha1(suffix_input).hexdigest()[:8]
        db_prefix = f"wio_ds_{seed}_{prefix_case}_{stable_suffix}"
        return CasePlan(
            rung_id=rung_id,
            case_id=case_id,
            seed=seed,
            schedule=schedule,
            focus=focus,
            database_prefix=db_prefix,
            workflow_id=f"wio-ds-{seed}-{case_id}",
            first_payload=f"payload-{seed}-first",
            second_payload=second_payload,
            delete_sys_workflow_before_replay=delete_sys,
            variant=variant,
        )

    if rung_id == RUNG_000_ID:
        return plan(
            3300,
            "run-one-committed-transaction-and-read-app-system",
            "transaction/datasource harness reaches app and system DB",
        )
    if rung_id == RUNG_001_ID:
        if case_id == "case-001":
            return plan(
                3310,
                "rerun-same-workflow-operation-id-after-committed",
                "committed transaction output is replayed without duplicate side effect",
            )
        if case_id == "case-002":
            return plan(
                3311,
                "reinvoke-workflow-id-with-different-payload-after-commit",
                "same workflow operation id with changed payload does not overwrite committed output",
                second_payload="payload-3311-mutated",
            )
        return plan(
            3312,
            "delete-system-workflow-row-then-rerun-datasource-output",
            "datasource output survives system operation-output loss without second app write",
            delete_sys=True,
        )

    if rung_id == RUNG_002_ID and case_id == "case-001":
        return plan(
            3320,
            "block-before-commit-and-poll-queue-visibility",
            "enqueue_in_transaction is invisible before commit and visible after commit",
        )
    if rung_id == RUNG_002_ID and case_id == "case-002":
        return plan(
            3321,
            "raise-after-enqueue-inside-transaction",
            "rollback removes both transaction ledger row and queued workflow",
        )
    if rung_id == RUNG_002_ID:
        return plan(
            3322,
            "commit-enqueue-then-replay-same-workflow-id",
            "replaying a committed enqueue transaction leaves one downstream queued effect",
        )

    if rung_id == RUNG_004_ID:
        case_number = int(case_id.removeprefix("case-"))
        seed = 3339 + case_number
        variants = [
            (
                "commit-replay",
                "generate-bounded-commit-replay-variant-from-seed",
                "committed transaction replay returns the first result with one app row",
            ),
            (
                "rollback-no-effect",
                "generate-bounded-rollback-no-effect-variant-from-seed",
                "failing datasource transaction rolls back the app row and records one error",
            ),
            (
                "enqueue-commit",
                "generate-bounded-enqueue-commit-variant-from-seed",
                "enqueue_in_transaction commit leaves one visible downstream workflow",
            ),
            (
                "enqueue-rollback",
                "generate-bounded-enqueue-rollback-variant-from-seed",
                "enqueue_in_transaction rollback leaves no ledger or child workflow",
            ),
            (
                "retry-after-commit",
                "generate-bounded-retry-after-commit-variant-from-seed",
                "system-row loss plus replay preserves committed datasource output",
            ),
            (
                "cleanup-after-result",
                "generate-bounded-cleanup-after-result-variant-from-seed",
                "bounded cleanup preserves the committed datasource result",
            ),
        ]
        variant, schedule, focus = variants[(case_number - 1) % len(variants)]
        second_payload = None
        if variant in {"commit-replay", "retry-after-commit"}:
            second_payload = f"payload-{seed}-mutated"
        return plan(
            seed,
            schedule,
            focus,
            second_payload=second_payload,
            variant=variant,
        )

    if rung_id == RUNG_005_ID:
        if case_id == "case-001":
            return plan(
                3370,
                "send-commit-wakes-blocked-receiver",
                "transactional send stays invisible before commit and wakes the blocked receiver after commit",
            )
        if case_id == "case-002":
            return plan(
                3371,
                "send-rollback-no-delivery",
                "rolled-back transactional send leaves no durable notification and fallback send proves receiver liveness",
            )
        return plan(
            3372,
            "enqueue-plus-send-same-transaction",
            "enqueue_in_transaction and send_in_transaction commit or rollback atomically",
        )

    if rung_id == RUNG_006_ID:
        if case_id == "case-001":
            return plan(
                6800,
                "sync-postgres-serializable-write-conflict",
                "two sync datasource workflows update the same Postgres row under SERIALIZABLE so one transaction retries from a real 40001 window",
            )
        if case_id == "case-002":
            return plan(
                6801,
                "async-postgres-deadlock-retry",
                "two async datasource workflows lock two Postgres rows in opposite order so one transaction retries from a real 40P01 window",
            )
        if case_id == "case-003":
            return plan(
                6802,
                "async-postgres-nonretryable-dbapi",
                "an async datasource body raises a real syntax DBAPI error and replays it without retrying",
            )
        return plan(
            6803,
            "sqlite-locked-datasource-retry",
            "an external SQLite writer lock forces a locked-database retry before the datasource records one success",
        )

    rung_003_cases = {
        "case-001": (
            3330,
            "delete-system-row-after-app-commit-before-result-reread",
            "datasource output survives system operation-output loss without duplicate app row",
            None,
        ),
        "case-002": (
            3331,
            "garbage-collect-with-live-result-window",
            "cleanup threshold leaves modeled committed result readable",
            None,
        ),
        "case-003": (
            3332,
            "fail-after-one-side-effect-gate",
            "failing datasource transaction rolls back partial app side effects and replays recorded error",
            None,
        ),
        "case-004": (
            3333,
            "recover-then-replay-same-workflow-id-mutated-payload",
            "recovery replay preserves committed output instead of mutated input",
            "payload-3333-mutated",
        ),
        "case-005": (
            3334,
            "late-result-read-after-idle-delay",
            "late replay/read returns committed output without re-executing the datasource step",
            None,
        ),
        "case-006": (
            3335,
            "run-cleanup-after-committed-transaction",
            "bounded cleanup preserves the newest modeled operation output and app row",
            None,
        ),
    }
    seed, schedule, focus, second_payload = rung_003_cases[case_id]
    return plan(seed, schedule, focus, second_payload=second_payload)


def get_ds() -> SQLAlchemyDatasource:
    if _sync_ds is None:
        raise RuntimeError("datasource not initialized")
    return _sync_ds


def get_async_ds() -> AsyncSQLAlchemyDatasource:
    if _async_ds is None:
        raise RuntimeError("async datasource not initialized")
    return _async_ds


def note_step_call(key: str) -> int:
    _step_call_counts[key] = _step_call_counts.get(key, 0) + 1
    return _step_call_counts[key]


def wait_for_retry_barrier(
    mapping: dict[str, set[str]],
    barriers: dict[str, threading.Event],
    intent_id: str,
    participant: str,
    expected_count: int,
    timeout_sec: float,
) -> None:
    with _retry_lock:
        participants = mapping.setdefault(intent_id, set())
        participants.add(participant)
        barrier = barriers.setdefault(intent_id, threading.Event())
        if len(participants) >= expected_count:
            barrier.set()
    if not barrier.wait(timeout_sec):
        raise RuntimeError(
            "retry barrier timed out: "
            + json.dumps(
                {
                    "intent_id": intent_id,
                    "participant": participant,
                    "participants": sorted(mapping.get(intent_id, set())),
                    "expected_count": expected_count,
                },
                sort_keys=True,
            )
        )


def wait_until_call_count_at_least(key: str, minimum: int, timeout_sec: float) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if _step_call_counts.get(key, 0) >= minimum:
            return
        time.sleep(0.02)
    raise WorkloadFailure(
        "call count window not reached: "
        + json.dumps(
            {
                "key": key,
                "minimum": minimum,
                "observed": _step_call_counts.get(key, 0),
            },
            sort_keys=True,
        )
    )


@DBOS.workflow()
def datasource_oaoo_workflow(intent_id: str, payload: str) -> dict[str, Any]:
    ds = get_ds()

    def write_once() -> dict[str, Any]:
        _step_call_counts[intent_id] = _step_call_counts.get(intent_id, 0) + 1
        session = ds.sql_session()
        session.execute(
            sa.text(
                f"""
                INSERT INTO {LEDGER_TABLE}
                    (intent_id, workflow_id, payload, step_call_count, created_at_ms)
                VALUES
                    (:intent_id, :workflow_id, :payload, :step_call_count, :created_at_ms)
                """
            ),
            {
                "intent_id": intent_id,
                "workflow_id": DBOS.workflow_id,
                "payload": payload,
                "step_call_count": _step_call_counts[intent_id],
                "created_at_ms": now_ms(),
            },
        )
        return {
            "intent_id": intent_id,
            "workflow_id": DBOS.workflow_id,
            "payload": payload,
            "step_call_count": _step_call_counts[intent_id],
        }

    return ds.run_tx_step(None, write_once)


@DBOS.workflow()
def datasource_failing_after_insert_workflow(intent_id: str, payload: str) -> dict[str, Any]:
    ds = get_ds()

    def fail_after_insert() -> dict[str, Any]:
        _step_call_counts[intent_id] = _step_call_counts.get(intent_id, 0) + 1
        session = ds.sql_session()
        session.execute(
            sa.text(
                f"""
                INSERT INTO {LEDGER_TABLE}
                    (intent_id, workflow_id, payload, step_call_count, created_at_ms)
                VALUES
                    (:intent_id, :workflow_id, :payload, :step_call_count, :created_at_ms)
                """
            ),
            {
                "intent_id": intent_id,
                "workflow_id": DBOS.workflow_id,
                "payload": payload,
                "step_call_count": _step_call_counts[intent_id],
                "created_at_ms": now_ms(),
            },
        )
        raise RuntimeError(f"modeled datasource failure after insert for {intent_id}")

    return ds.run_tx_step(None, fail_after_insert)


@DBOS.workflow()
def datasource_pg_serializable_retry_workflow(
    intent_id: str, participant: str, payload: str
) -> dict[str, Any]:
    ds = get_ds()

    def serializable_conflict() -> dict[str, Any]:
        attempt = note_step_call(f"{intent_id}:{participant}")
        session = ds.sql_session()
        backend_pid = session.execute(sa.text("SELECT pg_backend_pid()")).scalar_one()
        before_value = session.execute(
            sa.text(f"SELECT value FROM {RETRY_PROBE_TABLE} WHERE probe_key = 'shared'")
        ).scalar_one()
        if attempt == 1:
            wait_for_retry_barrier(
                _pg_serializable_first_reads,
                _pg_serializable_barriers,
                intent_id,
                participant,
                expected_count=2,
                timeout_sec=8.0,
            )
        session.execute(
            sa.text(
                f"""
                UPDATE {RETRY_PROBE_TABLE}
                SET value = value + 1, updated_by = :participant, updated_at_ms = :updated_at_ms
                WHERE probe_key = 'shared'
                """
            ),
            {"participant": participant, "updated_at_ms": now_ms()},
        )
        session.execute(
            sa.text(
                f"""
                INSERT INTO {LEDGER_TABLE}
                    (intent_id, workflow_id, payload, step_call_count, created_at_ms)
                VALUES
                    (:intent_id, :workflow_id, :payload, :step_call_count, :created_at_ms)
                """
            ),
            {
                "intent_id": intent_id,
                "workflow_id": DBOS.workflow_id,
                "payload": payload,
                "step_call_count": attempt,
                "created_at_ms": now_ms(),
            },
        )
        return {
            "intent_id": intent_id,
            "participant": participant,
            "workflow_id": DBOS.workflow_id,
            "attempt": attempt,
            "backend_pid": backend_pid,
            "before_value": before_value,
            "payload": payload,
        }

    return ds.run_tx_step(
        {"name": "pg_serializable_retry", "isolation_level": "SERIALIZABLE"},
        serializable_conflict,
    )


@DBOS.workflow()
async def datasource_pg_async_deadlock_retry_workflow(
    intent_id: str, participant: str, payload: str
) -> dict[str, Any]:
    ds = get_async_ds()

    async def deadlock_conflict() -> dict[str, Any]:
        attempt = note_step_call(f"{intent_id}:{participant}")
        session = ds.sql_session()
        backend_pid = (
            await session.execute(sa.text("SELECT pg_backend_pid()"))
        ).scalar_one()
        first_key, second_key = ("left", "right") if participant == "left" else ("right", "left")
        await session.execute(
            sa.text(
                f"SELECT value FROM {RETRY_PROBE_TABLE} WHERE probe_key = :key FOR UPDATE"
            ),
            {"key": first_key},
        )
        if attempt == 1:
            await asyncio.to_thread(
                wait_for_retry_barrier,
                _pg_deadlock_first_locks,
                _pg_deadlock_barriers,
                intent_id,
                participant,
                2,
                8.0,
            )
        await session.execute(
            sa.text(
                f"SELECT value FROM {RETRY_PROBE_TABLE} WHERE probe_key = :key FOR UPDATE"
            ),
            {"key": second_key},
        )
        await session.execute(
            sa.text(
                f"""
                UPDATE {RETRY_PROBE_TABLE}
                SET value = value + 1, updated_by = :participant, updated_at_ms = :updated_at_ms
                WHERE probe_key IN ('left', 'right')
                """
            ),
            {"participant": participant, "updated_at_ms": now_ms()},
        )
        await session.execute(
            sa.text(
                f"""
                INSERT INTO {LEDGER_TABLE}
                    (intent_id, workflow_id, payload, step_call_count, created_at_ms)
                VALUES
                    (:intent_id, :workflow_id, :payload, :step_call_count, :created_at_ms)
                """
            ),
            {
                "intent_id": intent_id,
                "workflow_id": DBOS.workflow_id,
                "payload": payload,
                "step_call_count": attempt,
                "created_at_ms": now_ms(),
            },
        )
        return {
            "intent_id": intent_id,
            "participant": participant,
            "workflow_id": DBOS.workflow_id,
            "attempt": attempt,
            "backend_pid": backend_pid,
            "first_key": first_key,
            "second_key": second_key,
            "payload": payload,
        }

    return await ds.run_tx_step_async(
        {"name": "pg_async_deadlock_retry", "isolation_level": "READ COMMITTED"},
        deadlock_conflict,
    )


@DBOS.workflow()
async def datasource_pg_async_nonretryable_workflow(intent_id: str) -> str:
    ds = get_async_ds()

    async def syntax_error_step() -> str:
        note_step_call(intent_id)
        session = ds.sql_session()
        await session.execute(sa.text("selct definitely_not_valid from missing_table"))
        return "unreachable"

    return await ds.run_tx_step_async(
        {"name": "pg_async_nonretryable", "isolation_level": "READ COMMITTED"},
        syntax_error_step,
    )


@DBOS.workflow()
def datasource_sqlite_locked_retry_workflow(intent_id: str, payload: str) -> dict[str, Any]:
    ds = get_ds()

    def locked_step() -> dict[str, Any]:
        attempt = note_step_call(intent_id)
        session = ds.sql_session()
        session.execute(
            sa.text(
                f"""
                INSERT INTO {LEDGER_TABLE}
                    (intent_id, workflow_id, payload, step_call_count, created_at_ms)
                VALUES
                    (:intent_id, :workflow_id, :payload, :step_call_count, :created_at_ms)
                """
            ),
            {
                "intent_id": intent_id,
                "workflow_id": DBOS.workflow_id,
                "payload": payload,
                "step_call_count": attempt,
                "created_at_ms": now_ms(),
            },
        )
        return {
            "intent_id": intent_id,
            "workflow_id": DBOS.workflow_id,
            "payload": payload,
            "attempt": attempt,
        }

    return ds.run_tx_step(
        {"name": "sqlite_locked_retry", "isolation_level": "SERIALIZABLE"},
        locked_step,
    )


@DBOS.workflow()
def transactional_send_receiver(topic: str, timeout_seconds: float = 15.0) -> dict[str, Any]:
    message = DBOS.recv(topic, timeout_seconds)
    return {
        "workflow_id": DBOS.workflow_id,
        "topic": topic,
        "message": message,
    }


def init_app_tables(ds: SQLAlchemyDatasource) -> None:
    id_column = (
        "INTEGER PRIMARY KEY AUTOINCREMENT"
        if ds.engine.dialect.name == "sqlite"
        else "BIGSERIAL PRIMARY KEY"
    )
    with ds.engine.begin() as conn:
        conn.execute(
            sa.text(
                f"""
                CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
                    id {id_column},
                    intent_id TEXT NOT NULL,
                    workflow_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    step_call_count INT NOT NULL,
                    created_at_ms BIGINT NOT NULL
                )
                """
            )
        )


async def init_app_tables_async(ds: AsyncSQLAlchemyDatasource) -> None:
    async with ds.engine.begin() as conn:
        await conn.execute(
            sa.text(
                f"""
                CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    intent_id TEXT NOT NULL,
                    workflow_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    step_call_count INT NOT NULL,
                    created_at_ms BIGINT NOT NULL
                )
                """
            )
        )


def init_retry_probe_rows(ds: SQLAlchemyDatasource, rows: dict[str, int]) -> None:
    with ds.engine.begin() as conn:
        conn.execute(
            sa.text(
                f"""
                CREATE TABLE IF NOT EXISTS {RETRY_PROBE_TABLE} (
                    probe_key TEXT PRIMARY KEY,
                    value INT NOT NULL,
                    updated_by TEXT,
                    updated_at_ms BIGINT
                )
                """
            )
        )
        for key, value in rows.items():
            conn.execute(
                sa.text(
                    f"""
                    INSERT INTO {RETRY_PROBE_TABLE} (probe_key, value, updated_by, updated_at_ms)
                    VALUES (:probe_key, :value, NULL, :updated_at_ms)
                    ON CONFLICT (probe_key) DO UPDATE
                    SET value = EXCLUDED.value,
                        updated_by = EXCLUDED.updated_by,
                        updated_at_ms = EXCLUDED.updated_at_ms
                    """
                ),
                {"probe_key": key, "value": value, "updated_at_ms": now_ms()},
            )


async def init_retry_probe_rows_async(
    ds: AsyncSQLAlchemyDatasource, rows: dict[str, int]
) -> None:
    async with ds.engine.begin() as conn:
        await conn.execute(
            sa.text(
                f"""
                CREATE TABLE IF NOT EXISTS {RETRY_PROBE_TABLE} (
                    probe_key TEXT PRIMARY KEY,
                    value INT NOT NULL,
                    updated_by TEXT,
                    updated_at_ms BIGINT
                )
                """
            )
        )
        for key, value in rows.items():
            await conn.execute(
                sa.text(
                    f"""
                    INSERT INTO {RETRY_PROBE_TABLE} (probe_key, value, updated_by, updated_at_ms)
                    VALUES (:probe_key, :value, NULL, :updated_at_ms)
                    ON CONFLICT (probe_key) DO UPDATE
                    SET value = EXCLUDED.value,
                        updated_by = EXCLUDED.updated_by,
                        updated_at_ms = EXCLUDED.updated_at_ms
                    """
                ),
                {"probe_key": key, "value": value, "updated_at_ms": now_ms()},
            )


def retry_probe_rows(ds: SQLAlchemyDatasource) -> list[dict[str, Any]]:
    with ds.engine.begin() as conn:
        rows = conn.execute(
            sa.text(
                f"""
                SELECT probe_key, value, updated_by, updated_at_ms
                FROM {RETRY_PROBE_TABLE}
                ORDER BY probe_key
                """
            )
        ).mappings().all()
    return [dict(row) for row in rows]


async def retry_probe_rows_async(ds: AsyncSQLAlchemyDatasource) -> list[dict[str, Any]]:
    async with ds.engine.begin() as conn:
        rows = (
            await conn.execute(
                sa.text(
                    f"""
                    SELECT probe_key, value, updated_by, updated_at_ms
                    FROM {RETRY_PROBE_TABLE}
                    ORDER BY probe_key
                    """
                )
            )
        ).mappings().all()
    return [dict(row) for row in rows]


def ledger_rows(ds: SQLAlchemyDatasource, intent_id: str) -> list[dict[str, Any]]:
    with ds.engine.begin() as conn:
        rows = conn.execute(
            sa.text(
                f"""
                SELECT id, intent_id, workflow_id, payload, step_call_count, created_at_ms
                FROM {LEDGER_TABLE}
                WHERE intent_id = :intent_id
                ORDER BY id
                """
            ),
            {"intent_id": intent_id},
        ).mappings().all()
    return [dict(row) for row in rows]


async def ledger_rows_async(
    ds: AsyncSQLAlchemyDatasource, intent_id: str
) -> list[dict[str, Any]]:
    async with ds.engine.begin() as conn:
        rows = (
            await conn.execute(
                sa.text(
                    f"""
                    SELECT id, intent_id, workflow_id, payload, step_call_count, created_at_ms
                    FROM {LEDGER_TABLE}
                    WHERE intent_id = :intent_id
                    ORDER BY id
                    """
                ),
                {"intent_id": intent_id},
            )
        ).mappings().all()
    return [dict(row) for row in rows]


def datasource_output_rows(ds: SQLAlchemyDatasource, workflow_id: str) -> list[dict[str, Any]]:
    with ds.engine.begin() as conn:
        rows = conn.execute(
            sa.select(
                DatasourceSchema.datasource_outputs.c.workflow_id,
                DatasourceSchema.datasource_outputs.c.step_id,
                DatasourceSchema.datasource_outputs.c.output,
                DatasourceSchema.datasource_outputs.c.error,
                DatasourceSchema.datasource_outputs.c.serialization,
            )
            .where(DatasourceSchema.datasource_outputs.c.workflow_id == workflow_id)
            .order_by(DatasourceSchema.datasource_outputs.c.step_id)
        ).mappings().all()
    return [dict(row) for row in rows]


async def datasource_output_rows_async(
    ds: AsyncSQLAlchemyDatasource, workflow_id: str
) -> list[dict[str, Any]]:
    async with ds.engine.begin() as conn:
        rows = (
            await conn.execute(
                sa.select(
                    DatasourceSchema.datasource_outputs.c.workflow_id,
                    DatasourceSchema.datasource_outputs.c.step_id,
                    DatasourceSchema.datasource_outputs.c.output,
                    DatasourceSchema.datasource_outputs.c.error,
                    DatasourceSchema.datasource_outputs.c.serialization,
                )
                .where(DatasourceSchema.datasource_outputs.c.workflow_id == workflow_id)
                .order_by(DatasourceSchema.datasource_outputs.c.step_id)
            )
        ).mappings().all()
    return [dict(row) for row in rows]


def operation_output_rows(dbos: DBOS, workflow_id: str) -> list[dict[str, Any]]:
    with dbos._sys_db.engine.begin() as conn:
        rows = conn.execute(
            sa.select(
                SystemSchema.operation_outputs.c.workflow_uuid,
                SystemSchema.operation_outputs.c.function_id,
                SystemSchema.operation_outputs.c.output,
                SystemSchema.operation_outputs.c.error,
            )
            .where(SystemSchema.operation_outputs.c.workflow_uuid == workflow_id)
            .order_by(SystemSchema.operation_outputs.c.function_id)
        ).mappings().all()
    return [dict(row) for row in rows]


def workflow_status_rows(dbos: DBOS, workflow_id: str) -> list[dict[str, Any]]:
    with dbos._sys_db.engine.begin() as conn:
        rows = conn.execute(
            sa.select(
                SystemSchema.workflow_status.c.workflow_uuid,
                SystemSchema.workflow_status.c.status,
                SystemSchema.workflow_status.c.name,
                SystemSchema.workflow_status.c.queue_name,
            ).where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
        ).mappings().all()
    return [dict(row) for row in rows]


def notification_rows(dbos: DBOS, workflow_id: str, topic: str | None = None) -> list[dict[str, Any]]:
    with dbos._sys_db.engine.begin() as conn:
        return notification_rows_from_connection(conn, workflow_id, topic)


def notification_rows_from_connection(
    conn: sa.Connection, workflow_id: str, topic: str | None = None
) -> list[dict[str, Any]]:
    cols = SystemSchema.notifications.c
    query = (
        sa.select(
            cols.destination_uuid,
            cols.topic,
            cols.message_uuid,
            cols.created_at_epoch_ms,
            cols.consumed,
            cols.serialization,
        )
        .where(cols.destination_uuid == workflow_id)
        .order_by(cols.created_at_epoch_ms.asc(), cols.message_uuid.asc())
    )
    if topic is not None:
        query = query.where(cols.topic == topic)
    rows = conn.execute(query).mappings().all()
    return [dict(row) for row in rows]


def workflow_status_rows_from_connection(conn: sa.Connection, workflow_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        sa.select(
            SystemSchema.workflow_status.c.workflow_uuid,
            SystemSchema.workflow_status.c.status,
            SystemSchema.workflow_status.c.name,
            SystemSchema.workflow_status.c.queue_name,
        ).where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
    ).mappings().all()
    return [dict(row) for row in rows]


def wait_for_workflow_status(
    dbos: DBOS,
    workflow_id: str,
    allowed_statuses: set[str],
    timeout_sec: float = 8.0,
) -> list[dict[str, Any]]:
    deadline = time.time() + timeout_sec
    last_rows: list[dict[str, Any]] = []
    while time.time() < deadline:
        last_rows = workflow_status_rows(dbos, workflow_id)
        if last_rows and last_rows[0]["status"] in allowed_statuses:
            return last_rows
        time.sleep(0.05)
    raise WorkloadFailure(
        "workflow did not reach expected status window: "
        + json.dumps(
            {
                "workflow_id": workflow_id,
                "allowed_statuses": sorted(allowed_statuses),
                "last_rows": last_rows,
            },
            sort_keys=True,
            default=str,
        )
    )


def init_enqueue_ledger(dbos: DBOS) -> None:
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.text(
                f"""
                CREATE TABLE IF NOT EXISTS {ENQUEUE_LEDGER_TABLE} (
                    intent_id TEXT PRIMARY KEY,
                    child_workflow_id TEXT NOT NULL,
                    attempt_count INT NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_at_ms BIGINT NOT NULL,
                    updated_at_ms BIGINT NOT NULL
                )
                """
            )
        )


def enqueue_ledger_rows(dbos: DBOS, intent_id: str) -> list[dict[str, Any]]:
    with dbos._sys_db.engine.begin() as conn:
        rows = conn.execute(
            sa.text(
                f"""
                SELECT intent_id, child_workflow_id, attempt_count, payload, created_at_ms, updated_at_ms
                FROM {ENQUEUE_LEDGER_TABLE}
                WHERE intent_id = :intent_id
                ORDER BY intent_id
                """
            ),
            {"intent_id": intent_id},
        ).mappings().all()
    return [dict(row) for row in rows]


def enqueue_child_status_rows(dbos: DBOS, child_workflow_id: str) -> list[dict[str, Any]]:
    with dbos._sys_db.engine.begin() as conn:
        rows = conn.execute(
            sa.select(
                SystemSchema.workflow_status.c.workflow_uuid,
                SystemSchema.workflow_status.c.status,
                SystemSchema.workflow_status.c.name,
                SystemSchema.workflow_status.c.queue_name,
                SystemSchema.workflow_status.c.inputs,
                SystemSchema.workflow_status.c.deduplication_id,
            ).where(SystemSchema.workflow_status.c.workflow_uuid == child_workflow_id)
        ).mappings().all()
    return [dict(row) for row in rows]


def delete_system_workflow_row(dbos: DBOS, workflow_id: str) -> None:
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.delete(SystemSchema.workflow_status).where(
                SystemSchema.workflow_status.c.workflow_uuid == workflow_id
            )
        )
    event("system_workflow_row_deleted", workflow_id=workflow_id)


def enqueue_options(plan: CasePlan, child_workflow_id: str) -> dict[str, Any]:
    return {
        "workflow_id": child_workflow_id,
        "workflow_name": "datasource_enqueue_child_workflow",
        "queue_name": f"wio-ds-enqueue-{plan.seed}",
        "app_version": f"{APP_VERSION}-{plan.rung_id}-{plan.case_id}",
        "deduplication_id": f"dedupe-{plan.seed}-{plan.case_id}",
        "attributes": {
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case": plan.case_id,
            "seed": plan.seed,
        },
    }


def receiver_enqueue_options(plan: CasePlan, workflow_id: str, queue_name: str) -> dict[str, Any]:
    return {
        "workflow_id": workflow_id,
        "workflow_name": "transactional_send_receiver",
        "queue_name": queue_name,
        "app_version": f"{APP_VERSION}-{plan.rung_id}-{plan.case_id}",
        "attributes": {
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case": plan.case_id,
            "seed": plan.seed,
        },
    }


def write_enqueue_effect(
    conn: sa.Connection,
    intent_id: str,
    child_workflow_id: str,
    payload: str,
) -> None:
    timestamp = now_ms()
    conn.execute(
        sa.text(
            f"""
            INSERT INTO {ENQUEUE_LEDGER_TABLE}
                (intent_id, child_workflow_id, attempt_count, payload, created_at_ms, updated_at_ms)
            VALUES
                (:intent_id, :child_workflow_id, 1, :payload, :created_at_ms, :updated_at_ms)
            ON CONFLICT (intent_id) DO UPDATE
            SET attempt_count = {ENQUEUE_LEDGER_TABLE}.attempt_count + 1,
                updated_at_ms = EXCLUDED.updated_at_ms
            """
        ),
        {
            "intent_id": intent_id,
            "child_workflow_id": child_workflow_id,
            "payload": payload,
            "created_at_ms": timestamp,
            "updated_at_ms": timestamp,
        },
    )


def enqueue_once_in_transaction(
    dbos: DBOS,
    client: DBOSClient,
    plan: CasePlan,
    child_workflow_id: str,
    intent_id: str,
    payload: str,
) -> None:
    with dbos._sys_db.engine.begin() as conn:
        write_enqueue_effect(conn, intent_id, child_workflow_id, payload)
        client.enqueue_in_transaction(
            conn,
            enqueue_options(plan, child_workflow_id),  # type: ignore[arg-type]
            payload,
        )


def run_transactional_send_case(plan: CasePlan, artifacts: Path) -> dict[str, Any]:
    app_url, sys_url, masked = prepare_databases(plan.database_prefix, artifacts)
    dbos: DBOS | None = None
    client: DBOSClient | None = None
    try:
        DBOS.destroy(destroy_registry=False)
        dbos = DBOS(config=make_config(plan, app_url, sys_url))
        dbos.launch()
        client = DBOSClient(system_database_url=sys_url)
        engine = client._sys_db.engine
        topic = f"wio-ds-send-topic-{plan.seed}-{plan.case_id}"
        receiver_id = f"wio-ds-send-{plan.seed}-{plan.case_id}"
        message = f"message-{plan.seed}-{plan.case_id}-commit"
        rolled_back_message = f"message-{plan.seed}-{plan.case_id}-rollback"
        fallback_message = f"message-{plan.seed}-{plan.case_id}-fallback"
        idempotency_key = f"wio-ds-send-idem-{plan.seed}-{plan.case_id}"
        queue_name = f"wio-ds-send-q-{plan.seed}"
        rollback_receiver_id = f"{receiver_id}-rollback"
        event(
            "transactional_send_case_started",
            rung=plan.rung_id,
            case_id=plan.case_id,
            seed=plan.seed,
            schedule=plan.schedule,
            receiver_id=receiver_id,
            topic=topic,
        )

        pre_commit_notifications: list[dict[str, Any]] = []
        pre_commit_status: list[dict[str, Any]] = []
        in_transaction_notifications: list[dict[str, Any]] = []
        rollback_notifications: list[dict[str, Any]] = []
        rollback_status: list[dict[str, Any]] = []
        rollback_branch_status: list[dict[str, Any]] = []
        rollback_branch_notifications: list[dict[str, Any]] = []

        if plan.case_id in {"case-001", "case-002"}:
            with SetWorkflowID(receiver_id):
                handle = DBOS.start_workflow(transactional_send_receiver, topic)
            wait_for_workflow_status(dbos, receiver_id, {"PENDING"})

        if plan.case_id == "case-001":
            conn = engine.connect()
            transaction = conn.begin()
            try:
                client.send_in_transaction(conn, receiver_id, message, topic, idempotency_key)
                client.send_in_transaction(conn, receiver_id, message, topic, idempotency_key)
                in_transaction_notifications = notification_rows_from_connection(
                    conn, receiver_id, topic
                )
                with engine.connect() as other_conn:
                    pre_commit_notifications = notification_rows_from_connection(
                        other_conn, receiver_id, topic
                    )
                    pre_commit_status = workflow_status_rows_from_connection(
                        other_conn, receiver_id
                    )
                invariant(
                    "transactional_send_pre_commit_invisible",
                    len(pre_commit_notifications) == 0
                    and len(pre_commit_status) == 1
                    and pre_commit_status[0]["status"] == "PENDING",
                    pre_commit_notifications=pre_commit_notifications,
                    pre_commit_status=pre_commit_status,
                )
                invariant(
                    "transactional_send_duplicate_key_single_row_inside_transaction",
                    len(in_transaction_notifications) == 1,
                    in_transaction_notifications=in_transaction_notifications,
                )
                transaction.commit()
            except Exception:
                transaction.rollback()
                raise
            finally:
                conn.close()
            receiver_result = handle.get_result()
            status_after = workflow_status_rows(dbos, receiver_id)
            notifications_after = notification_rows(dbos, receiver_id, topic)
            invariant(
                "transactional_send_commit_delivers_once",
                receiver_result["message"] == message
                and len(notifications_after) == 1
                and notifications_after[0]["consumed"] is True
                and len(status_after) == 1
                and status_after[0]["status"] == "SUCCESS",
                receiver_result=receiver_result,
                notifications_after=notifications_after,
                status_after=status_after,
            )
            result = {
                "status": "passed",
                "frontier": FRONTIER_ID,
                "rung": plan.rung_id,
                "case_id": plan.case_id,
                "seed": plan.seed,
                "schedule": plan.schedule,
                "receiver_id": receiver_id,
                "topic": topic,
                "message": message,
                "pre_commit_notifications": pre_commit_notifications,
                "pre_commit_status": pre_commit_status,
                "in_transaction_notifications": in_transaction_notifications,
                "notifications_after": notifications_after,
                "status_after": status_after,
                "receiver_result": receiver_result,
                "app_db": plan.database_prefix + "_app",
                "sys_db": plan.database_prefix + "_sys",
                "admin_url": masked,
            }
            write_json(artifacts / "result.json", result)
            event("transactional_send_case_passed", rung=plan.rung_id, case_id=plan.case_id)
            return result

        if plan.case_id == "case-002":
            conn = engine.connect()
            transaction = conn.begin()
            try:
                client.send_in_transaction(conn, receiver_id, rolled_back_message, topic)
                with engine.connect() as other_conn:
                    pre_commit_notifications = notification_rows_from_connection(
                        other_conn, receiver_id, topic
                    )
                    pre_commit_status = workflow_status_rows_from_connection(
                        other_conn, receiver_id
                    )
                transaction.rollback()
                event("transactional_send_rollback_applied", receiver_id=receiver_id)
            except Exception:
                transaction.rollback()
                raise
            finally:
                conn.close()
            rollback_notifications = notification_rows(dbos, receiver_id, topic)
            rollback_status = workflow_status_rows(dbos, receiver_id)
            invariant(
                "transactional_send_rollback_leaves_no_notification",
                len(pre_commit_notifications) == 0
                and len(rollback_notifications) == 0
                and len(rollback_status) == 1
                and rollback_status[0]["status"] == "PENDING",
                pre_commit_notifications=pre_commit_notifications,
                pre_commit_status=pre_commit_status,
                rollback_notifications=rollback_notifications,
                rollback_status=rollback_status,
            )
            client.send(receiver_id, fallback_message, topic)
            receiver_result = handle.get_result()
            status_after = workflow_status_rows(dbos, receiver_id)
            notifications_after = notification_rows(dbos, receiver_id, topic)
            invariant(
                "transactional_send_rollback_fallback_liveness",
                receiver_result["message"] == fallback_message
                and rolled_back_message not in json.dumps(receiver_result, sort_keys=True)
                and len(notifications_after) == 1
                and notifications_after[0]["consumed"] is True
                and len(status_after) == 1
                and status_after[0]["status"] == "SUCCESS",
                receiver_result=receiver_result,
                notifications_after=notifications_after,
                status_after=status_after,
            )
            result = {
                "status": "passed",
                "frontier": FRONTIER_ID,
                "rung": plan.rung_id,
                "case_id": plan.case_id,
                "seed": plan.seed,
                "schedule": plan.schedule,
                "receiver_id": receiver_id,
                "topic": topic,
                "rolled_back_message": rolled_back_message,
                "fallback_message": fallback_message,
                "pre_commit_notifications": pre_commit_notifications,
                "pre_commit_status": pre_commit_status,
                "rollback_notifications": rollback_notifications,
                "rollback_status": rollback_status,
                "notifications_after": notifications_after,
                "status_after": status_after,
                "receiver_result": receiver_result,
                "app_db": plan.database_prefix + "_app",
                "sys_db": plan.database_prefix + "_sys",
                "admin_url": masked,
            }
            write_json(artifacts / "result.json", result)
            event("transactional_send_case_passed", rung=plan.rung_id, case_id=plan.case_id)
            return result

        DBOS.register_queue(
            queue_name,
            concurrency=4,
            polling_interval_sec=0.05,
            on_conflict="always_update",
        )
        conn = engine.connect()
        transaction = conn.begin()
        try:
            handle = client.enqueue_in_transaction(
                conn,
                receiver_enqueue_options(plan, receiver_id, queue_name),  # type: ignore[arg-type]
                topic,
            )
            client.send_in_transaction(conn, receiver_id, message, topic)
            with engine.connect() as other_conn:
                pre_commit_notifications = notification_rows_from_connection(
                    other_conn, receiver_id, topic
                )
                pre_commit_status = workflow_status_rows_from_connection(
                    other_conn, receiver_id
                )
            invariant(
                "enqueue_plus_send_pre_commit_invisible",
                len(pre_commit_notifications) == 0 and len(pre_commit_status) == 0,
                pre_commit_notifications=pre_commit_notifications,
                pre_commit_status=pre_commit_status,
            )
            transaction.commit()
        except Exception:
            transaction.rollback()
            raise
        finally:
            conn.close()
        receiver_result = handle.get_result()
        status_after = workflow_status_rows(dbos, receiver_id)
        notifications_after = notification_rows(dbos, receiver_id, topic)
        invariant(
            "enqueue_plus_send_commit_atomic_delivery",
            receiver_result["message"] == message
            and len(notifications_after) == 1
            and notifications_after[0]["consumed"] is True
            and len(status_after) == 1
            and status_after[0]["status"] == "SUCCESS"
            and status_after[0]["queue_name"] == queue_name,
            receiver_result=receiver_result,
            notifications_after=notifications_after,
            status_after=status_after,
            queue_name=queue_name,
        )

        conn = engine.connect()
        transaction = conn.begin()
        try:
            client.enqueue_in_transaction(
                conn,
                receiver_enqueue_options(plan, rollback_receiver_id, queue_name),  # type: ignore[arg-type]
                topic,
            )
            client.send_in_transaction(conn, rollback_receiver_id, rolled_back_message, topic)
            transaction.rollback()
            event("enqueue_plus_send_rollback_applied", receiver_id=rollback_receiver_id)
        except Exception:
            transaction.rollback()
            raise
        finally:
            conn.close()
        rollback_branch_status = workflow_status_rows(dbos, rollback_receiver_id)
        rollback_branch_notifications = notification_rows(dbos, rollback_receiver_id, topic)
        invariant(
            "enqueue_plus_send_rollback_removes_both_effects",
            len(rollback_branch_status) == 0 and len(rollback_branch_notifications) == 0,
            rollback_branch_status=rollback_branch_status,
            rollback_branch_notifications=rollback_branch_notifications,
        )

        result = {
            "status": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case_id": plan.case_id,
            "seed": plan.seed,
            "schedule": plan.schedule,
            "receiver_id": receiver_id,
            "rollback_receiver_id": rollback_receiver_id,
            "topic": topic,
            "queue_name": queue_name,
            "message": message,
            "rolled_back_message": rolled_back_message,
            "pre_commit_notifications": pre_commit_notifications,
            "pre_commit_status": pre_commit_status,
            "notifications_after": notifications_after,
            "status_after": status_after,
            "receiver_result": receiver_result,
            "rollback_branch_status": rollback_branch_status,
            "rollback_branch_notifications": rollback_branch_notifications,
            "app_db": plan.database_prefix + "_app",
            "sys_db": plan.database_prefix + "_sys",
            "admin_url": masked,
        }
        write_json(artifacts / "result.json", result)
        event("transactional_send_case_passed", rung=plan.rung_id, case_id=plan.case_id)
        return result
    finally:
        if client is not None:
            client.destroy()
        if dbos is not None:
            DBOS.destroy(destroy_registry=False)
        drop_databases(plan.database_prefix)


def run_enqueue_boundary_case(plan: CasePlan, artifacts: Path) -> dict[str, Any]:
    app_url, sys_url, masked = prepare_databases(plan.database_prefix, artifacts)
    dbos: DBOS | None = None
    client: DBOSClient | None = None
    try:
        DBOS.destroy(destroy_registry=False)
        dbos = DBOS(config=make_config(plan, app_url, sys_url))
        dbos.launch()
        init_enqueue_ledger(dbos)
        client = DBOSClient(system_database_url=sys_url)

        child_workflow_id = f"wio-ds-enq-{plan.seed}-{plan.case_id}"
        intent_id = f"enqueue-intent-{plan.seed}-{plan.case_id}"
        event(
            "enqueue_case_started",
            rung=plan.rung_id,
            case_id=plan.case_id,
            seed=plan.seed,
            schedule=plan.schedule,
            child_workflow_id=child_workflow_id,
        )

        pre_commit_ledger: list[dict[str, Any]] = []
        pre_commit_status: list[dict[str, Any]] = []
        if plan.case_id == "case-001":
            conn = dbos._sys_db.engine.connect()
            transaction = conn.begin()
            try:
                write_enqueue_effect(conn, intent_id, child_workflow_id, plan.first_payload)
                client.enqueue_in_transaction(
                    conn,
                    enqueue_options(plan, child_workflow_id),  # type: ignore[arg-type]
                    plan.first_payload,
                )
                pre_commit_ledger = enqueue_ledger_rows(dbos, intent_id)
                pre_commit_status = enqueue_child_status_rows(dbos, child_workflow_id)
                invariant(
                    "pre_commit_enqueue_invisible",
                    len(pre_commit_ledger) == 0 and len(pre_commit_status) == 0,
                    pre_commit_ledger=pre_commit_ledger,
                    pre_commit_status=pre_commit_status,
                )
                transaction.commit()
            except Exception:
                transaction.rollback()
                raise
            finally:
                conn.close()
        elif plan.case_id == "case-002" or plan.variant == "enqueue-rollback":
            conn = dbos._sys_db.engine.connect()
            transaction = conn.begin()
            try:
                write_enqueue_effect(conn, intent_id, child_workflow_id, plan.first_payload)
                client.enqueue_in_transaction(
                    conn,
                    enqueue_options(plan, child_workflow_id),  # type: ignore[arg-type]
                    plan.first_payload,
                )
                raise RuntimeError("modeled rollback after enqueue_in_transaction")
            except RuntimeError as exc:
                transaction.rollback()
                event("modeled_transaction_rollback", error=str(exc))
            finally:
                conn.close()
        else:
            enqueue_once_in_transaction(
                dbos, client, plan, child_workflow_id, intent_id, plan.first_payload
            )
            if plan.case_id == "case-003":
                enqueue_once_in_transaction(
                    dbos, client, plan, child_workflow_id, intent_id, plan.first_payload
                )

        ledger_after = enqueue_ledger_rows(dbos, intent_id)
        child_status_after = enqueue_child_status_rows(dbos, child_workflow_id)

        if plan.case_id == "case-002" or plan.variant == "enqueue-rollback":
            invariant(
                "rollback_removes_ledger_and_enqueue",
                len(ledger_after) == 0 and len(child_status_after) == 0,
                ledger_after=ledger_after,
                child_status_after=child_status_after,
            )
        else:
            expected_attempt_count = 2 if plan.case_id == "case-003" else 1
            invariant(
                "committed_enqueue_visible_once",
                len(ledger_after) == 1
                and ledger_after[0]["payload"] == plan.first_payload
                and ledger_after[0]["attempt_count"] == expected_attempt_count
                and len(child_status_after) == 1
                and child_status_after[0]["status"] == "ENQUEUED",
                ledger_after=ledger_after,
                child_status_after=child_status_after,
                expected_attempt_count=expected_attempt_count,
            )
            invariant(
                "queued_effect_has_modeled_identity",
                child_status_after[0]["queue_name"] == f"wio-ds-enqueue-{plan.seed}"
                and child_status_after[0]["workflow_uuid"] == child_workflow_id,
                child_status_after=child_status_after,
            )

        result = {
            "status": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case_id": plan.case_id,
            "seed": plan.seed,
            "schedule": plan.schedule,
            "workflow_id": plan.workflow_id,
            "child_workflow_id": child_workflow_id,
            "intent_id": intent_id,
            "pre_commit_ledger": pre_commit_ledger,
            "pre_commit_status": pre_commit_status,
            "ledger_after": ledger_after,
            "child_status_after": child_status_after,
            "send_in_transaction_api": "not present in DBOSClient/DBOS public API for target commit; rung executes enqueue_in_transaction boundary",
            "app_db": plan.database_prefix + "_app",
            "sys_db": plan.database_prefix + "_sys",
            "admin_url": masked,
        }
        write_json(artifacts / "result.json", result)
        event("enqueue_case_passed", rung=plan.rung_id, case_id=plan.case_id)
        return result
    finally:
        if client is not None:
            client.destroy()
        if dbos is not None:
            DBOS.destroy(destroy_registry=False)
        drop_databases(plan.database_prefix)


def invoke_workflow(plan: CasePlan, payload: str) -> dict[str, Any]:
    with SetWorkflowID(plan.workflow_id):
        result = datasource_oaoo_workflow(f"intent-{plan.seed}-{plan.case_id}", payload)
    event(
        "workflow_invoked",
        workflow_id=plan.workflow_id,
        payload=payload,
        result=result,
    )
    return result


def invoke_failing_workflow(plan: CasePlan, payload: str) -> str:
    with SetWorkflowID(plan.workflow_id):
        try:
            datasource_failing_after_insert_workflow(
                f"intent-{plan.seed}-{plan.case_id}", payload
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            event(
                "failing_workflow_invoked",
                workflow_id=plan.workflow_id,
                payload=payload,
                error=error,
            )
            return error
    raise WorkloadFailure("modeled failing workflow unexpectedly returned success")


def run_retry_cleanup_case(plan: CasePlan, artifacts: Path) -> dict[str, Any]:
    global _sync_ds
    _step_call_counts.clear()
    app_url, sys_url, masked = prepare_databases(plan.database_prefix, artifacts)
    ds_schema = f"ds_{plan.seed}_{plan.case_id.replace('-', '_')}"
    dbos: DBOS | None = None
    try:
        ds_url = app_url.replace("postgresql://", "postgresql+psycopg://")
        _sync_ds = SQLAlchemyDatasource.create(ds_url, schema=ds_schema)
        init_app_tables(_sync_ds)
        DBOS.destroy(destroy_registry=False)
        dbos = DBOS(config=make_config(plan, app_url, sys_url))
        dbos.launch()
        event(
            "retry_cleanup_case_started",
            rung=plan.rung_id,
            case_id=plan.case_id,
            seed=plan.seed,
            schedule=plan.schedule,
        )

        intent_id = f"intent-{plan.seed}-{plan.case_id}"
        cleanup_applied = False
        system_row_deleted = False
        idle_delay_ms = 0

        if plan.case_id == "case-003" or plan.variant == "rollback-no-effect":
            first_error = invoke_failing_workflow(plan, plan.first_payload)
            second_error = invoke_failing_workflow(plan, plan.first_payload)
            app_rows = ledger_rows(_sync_ds, intent_id)
            ds_rows = datasource_output_rows(_sync_ds, plan.workflow_id)
            op_rows = operation_output_rows(dbos, plan.workflow_id)
            wf_rows = workflow_status_rows(dbos, plan.workflow_id)
            step_calls = _step_call_counts.get(intent_id, 0)

            invariant(
                "failed_transaction_rolled_back_and_recorded",
                len(app_rows) == 0
                and len(ds_rows) == 1
                and ds_rows[0]["error"] is not None
                and len(wf_rows) == 1
                and wf_rows[0]["status"] == "ERROR"
                and step_calls == 1,
                first_error=first_error,
                second_error=second_error,
                app_rows=app_rows,
                datasource_rows=ds_rows,
                operation_rows=op_rows,
                workflow_rows=wf_rows,
                step_calls=step_calls,
            )

            result = {
                "status": "passed",
                "frontier": FRONTIER_ID,
                "rung": plan.rung_id,
                "case_id": plan.case_id,
                "seed": plan.seed,
                "schedule": plan.schedule,
                "workflow_id": plan.workflow_id,
                "first_error": first_error,
                "second_error": second_error,
                "step_calls": step_calls,
                "app_rows": app_rows,
                "datasource_output_rows": ds_rows,
                "operation_output_rows": op_rows,
                "workflow_status_rows": wf_rows,
                "app_db": plan.database_prefix + "_app",
                "sys_db": plan.database_prefix + "_sys",
                "admin_url": masked,
            }
            write_json(artifacts / "result.json", result)
            event("retry_cleanup_case_passed", rung=plan.rung_id, case_id=plan.case_id)
            return result

        first_result = invoke_workflow(plan, plan.first_payload)
        if plan.case_id in {"case-001", "case-004"} or plan.variant == "retry-after-commit":
            delete_system_workflow_row(dbos, plan.workflow_id)
            system_row_deleted = True
        if plan.case_id in {"case-002", "case-006"} or plan.variant == "cleanup-after-result":
            garbage_collect(dbos, cutoff_epoch_timestamp_ms=None, rows_threshold=1)
            cleanup_applied = True
            event("garbage_collect_applied", rows_threshold=1)
        if plan.case_id == "case-005":
            idle_delay_ms = 50
            time.sleep(idle_delay_ms / 1000)

        second_payload = plan.second_payload or plan.first_payload
        second_result = invoke_workflow(plan, second_payload)

        app_rows = ledger_rows(_sync_ds, intent_id)
        ds_rows = datasource_output_rows(_sync_ds, plan.workflow_id)
        op_rows = operation_output_rows(dbos, plan.workflow_id)
        wf_rows = workflow_status_rows(dbos, plan.workflow_id)
        step_calls = _step_call_counts.get(intent_id, 0)

        invariant(
            "retry_cleanup_replay_preserves_committed_result",
            first_result["payload"] == plan.first_payload
            and second_result["payload"] == plan.first_payload
            and len(app_rows) == 1
            and app_rows[0]["payload"] == plan.first_payload
            and len(ds_rows) == 1
            and ds_rows[0]["error"] is None
            and len(wf_rows) == 1
            and wf_rows[0]["status"] == "SUCCESS"
            and step_calls == 1,
            first_result=first_result,
            second_result=second_result,
            second_payload=second_payload,
            app_rows=app_rows,
            datasource_rows=ds_rows,
            operation_rows=op_rows,
            workflow_rows=wf_rows,
            step_calls=step_calls,
            cleanup_applied=cleanup_applied,
            system_row_deleted=system_row_deleted,
            idle_delay_ms=idle_delay_ms,
        )
        invariant(
            "datasource_and_system_records_modeled_after_retry_cleanup",
            len(op_rows) <= 1
            and all(row["error"] is None for row in op_rows)
            and all(row["function_id"] == 1 for row in op_rows),
            operation_rows=op_rows,
        )

        result = {
            "status": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case_id": plan.case_id,
            "seed": plan.seed,
            "schedule": plan.schedule,
            "workflow_id": plan.workflow_id,
            "first_result": first_result,
            "second_result": second_result,
            "second_payload": second_payload,
            "step_calls": step_calls,
            "cleanup_applied": cleanup_applied,
            "system_row_deleted": system_row_deleted,
            "idle_delay_ms": idle_delay_ms,
            "app_rows": app_rows,
            "datasource_output_rows": ds_rows,
            "operation_output_rows": op_rows,
            "workflow_status_rows": wf_rows,
            "app_db": plan.database_prefix + "_app",
            "sys_db": plan.database_prefix + "_sys",
            "admin_url": masked,
        }
        write_json(artifacts / "result.json", result)
        event("retry_cleanup_case_passed", rung=plan.rung_id, case_id=plan.case_id)
        return result
    finally:
        if dbos is not None:
            DBOS.destroy(destroy_registry=False)
        if _sync_ds is not None and getattr(_sync_ds, "created_engine", False):
            _sync_ds.engine.dispose()
        _sync_ds = None
        drop_databases(plan.database_prefix)


def run_dbapi_retry_liveness_case(plan: CasePlan, artifacts: Path) -> dict[str, Any]:
    global _sync_ds, _async_ds
    _step_call_counts.clear()
    with _retry_lock:
        _pg_serializable_first_reads.clear()
        _pg_serializable_barriers.clear()
        _pg_deadlock_first_locks.clear()
        _pg_deadlock_barriers.clear()

    app_url, sys_url, masked = prepare_databases(plan.database_prefix, artifacts)
    ds_schema = f"ds_{plan.seed}_{plan.case_id.replace('-', '_')}"
    dbos: DBOS | None = None
    sqlite_lock_conn: sa.Connection | None = None
    sqlite_lock_engine: sa.Engine | None = None
    try:
        DBOS.destroy(destroy_registry=False)
        dbos = DBOS(config=make_config(plan, app_url, sys_url))
        dbos.launch()
        intent_id = f"intent-{plan.seed}-{plan.case_id}"
        event(
            "dbapi_retry_liveness_case_started",
            rung=plan.rung_id,
            case_id=plan.case_id,
            seed=plan.seed,
            schedule=plan.schedule,
        )

        if plan.case_id == "case-001":
            ds_url = app_url.replace("postgresql://", "postgresql+psycopg://")
            _sync_ds = SQLAlchemyDatasource.create(ds_url, schema=ds_schema)
            init_app_tables(_sync_ds)
            init_retry_probe_rows(_sync_ds, {"shared": 0})
            left_wfid = f"{plan.workflow_id}-left"
            right_wfid = f"{plan.workflow_id}-right"
            with SetWorkflowID(left_wfid):
                left_handle = DBOS.start_workflow(
                    datasource_pg_serializable_retry_workflow,
                    intent_id,
                    "left",
                    f"{plan.first_payload}-left",
                )
            with SetWorkflowID(right_wfid):
                right_handle = DBOS.start_workflow(
                    datasource_pg_serializable_retry_workflow,
                    intent_id,
                    "right",
                    f"{plan.first_payload}-right",
                )
            first_results = [left_handle.get_result(), right_handle.get_result()]
            call_counts_after_first = dict(_step_call_counts)
            with SetWorkflowID(left_wfid):
                left_replay = datasource_pg_serializable_retry_workflow(
                    intent_id, "left", "mutated-left"
                )
            with SetWorkflowID(right_wfid):
                right_replay = datasource_pg_serializable_retry_workflow(
                    intent_id, "right", "mutated-right"
                )
            app_rows = ledger_rows(_sync_ds, intent_id)
            probe_rows = retry_probe_rows(_sync_ds)
            ds_rows = {
                left_wfid: datasource_output_rows(_sync_ds, left_wfid),
                right_wfid: datasource_output_rows(_sync_ds, right_wfid),
            }
            op_rows = {
                left_wfid: operation_output_rows(dbos, left_wfid),
                right_wfid: operation_output_rows(dbos, right_wfid),
            }
            wf_rows = {
                left_wfid: workflow_status_rows(dbos, left_wfid),
                right_wfid: workflow_status_rows(dbos, right_wfid),
            }
            probe_values = {row["probe_key"]: row["value"] for row in probe_rows}
            total_attempts = sum(
                count for key, count in _step_call_counts.items() if key.startswith(intent_id)
            )
            invariant(
                "postgres_serializable_retry_commits_exactly_once",
                len(app_rows) == 2
                and probe_values == {"shared": 2}
                and total_attempts > 2
                and _step_call_counts == call_counts_after_first
                and left_replay["payload"].endswith("-left")
                and right_replay["payload"].endswith("-right")
                and all(len(rows) == 1 and rows[0]["error"] is None for rows in ds_rows.values())
                and all(len(rows) == 1 and rows[0]["error"] is None for rows in op_rows.values())
                and all(len(rows) == 1 and rows[0]["status"] == "SUCCESS" for rows in wf_rows.values()),
                first_results=first_results,
                left_replay=left_replay,
                right_replay=right_replay,
                call_counts=_step_call_counts,
                app_rows=app_rows,
                probe_rows=probe_rows,
                datasource_rows=ds_rows,
                operation_rows=op_rows,
                workflow_rows=wf_rows,
            )
            result = {
                "status": "passed",
                "frontier": FRONTIER_ID,
                "rung": plan.rung_id,
                "case_id": plan.case_id,
                "seed": plan.seed,
                "workflow_ids": [left_wfid, right_wfid],
                "first_results": first_results,
                "replay_results": [left_replay, right_replay],
                "call_counts": dict(_step_call_counts),
                "app_rows": app_rows,
                "probe_rows": probe_rows,
                "datasource_output_rows": ds_rows,
                "operation_output_rows": op_rows,
                "workflow_status_rows": wf_rows,
                "app_db": plan.database_prefix + "_app",
                "sys_db": plan.database_prefix + "_sys",
                "admin_url": masked,
            }
            write_json(artifacts / "result.json", result)
            event("dbapi_retry_liveness_case_passed", rung=plan.rung_id, case_id=plan.case_id)
            return result

        if plan.case_id in {"case-002", "case-003"}:
            async def run_async_postgres_case() -> dict[str, Any]:
                global _async_ds
                ds_url = app_url.replace("postgresql://", "postgresql+psycopg://")
                _async_ds = await AsyncSQLAlchemyDatasource.create(ds_url, schema=ds_schema)
                await init_app_tables_async(_async_ds)
                if plan.case_id == "case-002":
                    await init_retry_probe_rows_async(_async_ds, {"left": 0, "right": 0})
                    left_wfid = f"{plan.workflow_id}-left"
                    right_wfid = f"{plan.workflow_id}-right"
                    with SetWorkflowID(left_wfid):
                        left_handle = await DBOS.start_workflow_async(
                            datasource_pg_async_deadlock_retry_workflow,
                            intent_id,
                            "left",
                            f"{plan.first_payload}-left",
                        )
                    with SetWorkflowID(right_wfid):
                        right_handle = await DBOS.start_workflow_async(
                            datasource_pg_async_deadlock_retry_workflow,
                            intent_id,
                            "right",
                            f"{plan.first_payload}-right",
                        )
                    first_results = await asyncio.gather(
                        left_handle.get_result(), right_handle.get_result()
                    )
                    call_counts_after_first = dict(_step_call_counts)
                    with SetWorkflowID(left_wfid):
                        left_replay = await datasource_pg_async_deadlock_retry_workflow(
                            intent_id, "left", "mutated-left"
                        )
                    with SetWorkflowID(right_wfid):
                        right_replay = await datasource_pg_async_deadlock_retry_workflow(
                            intent_id, "right", "mutated-right"
                        )
                    app_rows = await ledger_rows_async(_async_ds, intent_id)
                    probe_rows = await retry_probe_rows_async(_async_ds)
                    ds_rows = {
                        left_wfid: await datasource_output_rows_async(_async_ds, left_wfid),
                        right_wfid: await datasource_output_rows_async(_async_ds, right_wfid),
                    }
                    op_rows = {
                        left_wfid: operation_output_rows(dbos, left_wfid),
                        right_wfid: operation_output_rows(dbos, right_wfid),
                    }
                    wf_rows = {
                        left_wfid: workflow_status_rows(dbos, left_wfid),
                        right_wfid: workflow_status_rows(dbos, right_wfid),
                    }
                    probe_values = {row["probe_key"]: row["value"] for row in probe_rows}
                    total_attempts = sum(
                        count
                        for key, count in _step_call_counts.items()
                        if key.startswith(intent_id)
                    )
                    invariant(
                        "postgres_async_deadlock_retry_commits_exactly_once",
                        len(app_rows) == 2
                        and probe_values == {"left": 2, "right": 2}
                        and total_attempts > 2
                        and _step_call_counts == call_counts_after_first
                        and left_replay["payload"].endswith("-left")
                        and right_replay["payload"].endswith("-right")
                        and all(len(rows) == 1 and rows[0]["error"] is None for rows in ds_rows.values())
                        and all(len(rows) == 1 and rows[0]["error"] is None for rows in op_rows.values())
                        and all(len(rows) == 1 and rows[0]["status"] == "SUCCESS" for rows in wf_rows.values()),
                        first_results=first_results,
                        replay_results=[left_replay, right_replay],
                        call_counts=_step_call_counts,
                        app_rows=app_rows,
                        probe_rows=probe_rows,
                        datasource_rows=ds_rows,
                        operation_rows=op_rows,
                        workflow_rows=wf_rows,
                    )
                    return {
                        "status": "passed",
                        "frontier": FRONTIER_ID,
                        "rung": plan.rung_id,
                        "case_id": plan.case_id,
                        "seed": plan.seed,
                        "workflow_ids": [left_wfid, right_wfid],
                        "first_results": first_results,
                        "replay_results": [left_replay, right_replay],
                        "call_counts": dict(_step_call_counts),
                        "app_rows": app_rows,
                        "probe_rows": probe_rows,
                        "datasource_output_rows": ds_rows,
                        "operation_output_rows": op_rows,
                        "workflow_status_rows": wf_rows,
                        "app_db": plan.database_prefix + "_app",
                        "sys_db": plan.database_prefix + "_sys",
                        "admin_url": masked,
                    }

                wfid = plan.workflow_id
                first_error = ""
                replay_error = ""
                with SetWorkflowID(wfid):
                    try:
                        await datasource_pg_async_nonretryable_workflow(intent_id)
                    except Exception as exc:
                        first_error = f"{type(exc).__name__}: {exc}"
                    else:
                        raise WorkloadFailure("nonretryable datasource workflow unexpectedly succeeded")
                call_counts_after_first = dict(_step_call_counts)
                with SetWorkflowID(wfid):
                    try:
                        await datasource_pg_async_nonretryable_workflow(intent_id)
                    except Exception as exc:
                        replay_error = f"{type(exc).__name__}: {exc}"
                    else:
                        raise WorkloadFailure("nonretryable datasource replay unexpectedly succeeded")
                app_rows = await ledger_rows_async(_async_ds, intent_id)
                ds_rows = await datasource_output_rows_async(_async_ds, wfid)
                op_rows = operation_output_rows(dbos, wfid)
                wf_rows = workflow_status_rows(dbos, wfid)
                invariant(
                    "postgres_async_nonretryable_records_and_replays_error",
                    _step_call_counts == call_counts_after_first
                    and _step_call_counts.get(intent_id) == 1
                    and len(app_rows) == 0
                    and len(ds_rows) == 1
                    and ds_rows[0]["error"] is not None
                    and len(wf_rows) == 1
                    and wf_rows[0]["status"] == "ERROR"
                    and "syntax" in (first_error + replay_error).lower(),
                    first_error=first_error,
                    replay_error=replay_error,
                    call_counts=_step_call_counts,
                    app_rows=app_rows,
                    datasource_rows=ds_rows,
                    operation_rows=op_rows,
                    workflow_rows=wf_rows,
                )
                return {
                    "status": "passed",
                    "frontier": FRONTIER_ID,
                    "rung": plan.rung_id,
                    "case_id": plan.case_id,
                    "seed": plan.seed,
                    "workflow_id": wfid,
                    "first_error": first_error,
                    "replay_error": replay_error,
                    "call_counts": dict(_step_call_counts),
                    "app_rows": app_rows,
                    "datasource_output_rows": ds_rows,
                    "operation_output_rows": op_rows,
                    "workflow_status_rows": wf_rows,
                    "app_db": plan.database_prefix + "_app",
                    "sys_db": plan.database_prefix + "_sys",
                    "admin_url": masked,
                }

            result = asyncio.run(run_async_postgres_case())
            write_json(artifacts / "result.json", result)
            event("dbapi_retry_liveness_case_passed", rung=plan.rung_id, case_id=plan.case_id)
            return result

        sqlite_path = artifacts / f"{plan.database_prefix}.sqlite"
        sqlite_url = f"sqlite:///{sqlite_path}"
        _sync_ds = SQLAlchemyDatasource.create(
            sqlite_url,
            engine_kwargs={"connect_args": {"timeout": 0.05}},
        )
        init_app_tables(_sync_ds)
        init_retry_probe_rows(_sync_ds, {"locked": 0})
        sqlite_lock_engine = sa.create_engine(
            sqlite_url,
            connect_args={"timeout": 0.05},
        )
        sqlite_lock_conn = sqlite_lock_engine.connect()
        sqlite_lock_conn.exec_driver_sql("BEGIN EXCLUSIVE")
        sqlite_lock_conn.execute(
            sa.text(
                f"""
                UPDATE {RETRY_PROBE_TABLE}
                SET value = value + 1, updated_by = 'external-lock', updated_at_ms = :updated_at_ms
                WHERE probe_key = 'locked'
                """
            ),
            {"updated_at_ms": now_ms()},
        )
        lock_acquired_at_ms = now_ms()
        with SetWorkflowID(plan.workflow_id):
            handle = DBOS.start_workflow(
                datasource_sqlite_locked_retry_workflow,
                intent_id,
                plan.first_payload,
            )
        try:
            wait_until_call_count_at_least(intent_id, 2, timeout_sec=8.0)
        except WorkloadFailure as exc:
            invariant(
                "sqlite_locked_retry_reaches_second_attempt_before_lock_release",
                False,
                call_counts=dict(_step_call_counts),
                lock_acquired_at_ms=lock_acquired_at_ms,
                wait_error=str(exc),
                workflow_rows=workflow_status_rows(dbos, plan.workflow_id),
            )
        lock_released_at_ms = now_ms()
        sqlite_lock_conn.commit()
        sqlite_lock_conn.close()
        sqlite_lock_conn = None
        sqlite_lock_engine.dispose()
        sqlite_lock_engine = None
        first_result = handle.get_result()
        call_counts_after_first = dict(_step_call_counts)
        with SetWorkflowID(plan.workflow_id):
            replay_result = datasource_sqlite_locked_retry_workflow(
                intent_id, "mutated-sqlite-payload"
            )
        app_rows = ledger_rows(_sync_ds, intent_id)
        ds_rows = datasource_output_rows(_sync_ds, plan.workflow_id)
        op_rows = operation_output_rows(dbos, plan.workflow_id)
        wf_rows = workflow_status_rows(dbos, plan.workflow_id)
        invariant(
            "sqlite_locked_retry_records_one_success",
            _step_call_counts == call_counts_after_first
            and _step_call_counts.get(intent_id, 0) >= 2
            and first_result["payload"] == plan.first_payload
            and replay_result["payload"] == plan.first_payload
            and len(app_rows) == 1
            and app_rows[0]["payload"] == plan.first_payload
            and len(ds_rows) == 1
            and ds_rows[0]["error"] is None
            and len(op_rows) == 1
            and op_rows[0]["error"] is None
            and len(wf_rows) == 1
            and wf_rows[0]["status"] == "SUCCESS",
            first_result=first_result,
            replay_result=replay_result,
            call_counts=_step_call_counts,
            lock_acquired_at_ms=lock_acquired_at_ms,
            lock_released_at_ms=lock_released_at_ms,
            app_rows=app_rows,
            datasource_rows=ds_rows,
            operation_rows=op_rows,
            workflow_rows=wf_rows,
        )
        result = {
            "status": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case_id": plan.case_id,
            "seed": plan.seed,
            "workflow_id": plan.workflow_id,
            "first_result": first_result,
            "replay_result": replay_result,
            "call_counts": dict(_step_call_counts),
            "lock_acquired_at_ms": lock_acquired_at_ms,
            "lock_released_at_ms": lock_released_at_ms,
            "app_rows": app_rows,
            "datasource_output_rows": ds_rows,
            "operation_output_rows": op_rows,
            "workflow_status_rows": wf_rows,
            "sqlite_path": str(sqlite_path),
            "sys_db": plan.database_prefix + "_sys",
            "admin_url": masked,
        }
        write_json(artifacts / "result.json", result)
        event("dbapi_retry_liveness_case_passed", rung=plan.rung_id, case_id=plan.case_id)
        return result
    finally:
        if sqlite_lock_conn is not None:
            try:
                sqlite_lock_conn.rollback()
                sqlite_lock_conn.close()
            except Exception:
                pass
        if sqlite_lock_engine is not None:
            sqlite_lock_engine.dispose()
        if dbos is not None:
            DBOS.destroy(destroy_registry=False)
        if _sync_ds is not None and getattr(_sync_ds, "created_engine", False):
            _sync_ds.engine.dispose()
        if _async_ds is not None and getattr(_async_ds, "created_engine", False):
            asyncio.run(_async_ds.engine.dispose())
        _sync_ds = None
        _async_ds = None
        drop_databases(plan.database_prefix)


def run_case(plan: CasePlan, artifacts: Path) -> dict[str, Any]:
    global _sync_ds
    _step_call_counts.clear()
    app_url, sys_url, masked = prepare_databases(plan.database_prefix, artifacts)
    ds_schema = f"ds_{plan.seed}_{plan.case_id.replace('-', '_')}"
    dbos: DBOS | None = None
    try:
        ds_url = app_url.replace("postgresql://", "postgresql+psycopg://")
        _sync_ds = SQLAlchemyDatasource.create(ds_url, schema=ds_schema)
        init_app_tables(_sync_ds)
        DBOS.destroy(destroy_registry=False)
        dbos = DBOS(config=make_config(plan, app_url, sys_url))
        dbos.launch()
        event(
            "case_started",
            rung=plan.rung_id,
            case_id=plan.case_id,
            seed=plan.seed,
            schedule=plan.schedule,
        )

        first_result = invoke_workflow(plan, plan.first_payload)
        second_payload = plan.second_payload or plan.first_payload

        if plan.delete_sys_workflow_before_replay:
            delete_system_workflow_row(dbos, plan.workflow_id)

        second_result = invoke_workflow(plan, second_payload)

        intent_id = f"intent-{plan.seed}-{plan.case_id}"
        app_rows = ledger_rows(_sync_ds, intent_id)
        ds_rows = datasource_output_rows(_sync_ds, plan.workflow_id)
        op_rows = operation_output_rows(dbos, plan.workflow_id)
        wf_rows = workflow_status_rows(dbos, plan.workflow_id)

        expected_payload = plan.first_payload
        invariant(
            "public_replay_result_matches_first_commit",
            first_result["payload"] == expected_payload and second_result["payload"] == expected_payload,
            first_result=first_result,
            second_result=second_result,
            expected_payload=expected_payload,
            second_payload=second_payload,
        )
        invariant(
            "app_side_effect_occurs_once",
            len(app_rows) == 1 and app_rows[0]["payload"] == expected_payload,
            app_rows=app_rows,
            expected_payload=expected_payload,
        )
        invariant(
            "datasource_output_recorded_once",
            len(ds_rows) == 1 and ds_rows[0]["step_id"] == 1 and ds_rows[0]["error"] is None,
            datasource_rows=ds_rows,
        )
        invariant(
            "system_operation_output_visible",
            len(op_rows) == 1 and op_rows[0]["function_id"] == 1 and op_rows[0]["error"] is None,
            operation_rows=op_rows,
        )
        invariant(
            "workflow_terminal_success",
            len(wf_rows) == 1 and wf_rows[0]["status"] == "SUCCESS",
            workflow_rows=wf_rows,
        )

        result = {
            "status": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case_id": plan.case_id,
            "seed": plan.seed,
            "schedule": plan.schedule,
            "workflow_id": plan.workflow_id,
            "first_result": first_result,
            "second_result": second_result,
            "app_rows": app_rows,
            "datasource_output_rows": ds_rows,
            "operation_output_rows": op_rows,
            "workflow_status_rows": wf_rows,
            "dbos_product_source": str(next((p for p in sys.path if p.endswith("dbos-transact-py")), "")),
            "app_db": plan.database_prefix + "_app",
            "sys_db": plan.database_prefix + "_sys",
            "admin_url": masked,
        }
        write_json(artifacts / "result.json", result)
        event("case_passed", rung=plan.rung_id, case_id=plan.case_id)
        return result
    finally:
        if dbos is not None:
            DBOS.destroy(destroy_registry=False)
        if _sync_ds is not None and getattr(_sync_ds, "created_engine", False):
            _sync_ds.engine.dispose()
        _sync_ds = None
        drop_databases(plan.database_prefix)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rung", required=True)
    parser.add_argument("--case", dest="case_id")
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument("--artifact-dir", default="/tmp/wio-artifacts/datasource-transaction-oaoo")
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
                        "Datasource transactions replay committed outputs without duplicate app side effects; "
                        "transactional sends and enqueues are visible only after caller-owned commit; "
                        "retryable DBAPI concurrency failures retry with one durable success while non-retryable DBAPI errors replay as errors."
                    ),
                    "replay_command": (
                        ".workers/run-with-postgres.sh .workers/python-runtime.sh "
                        f".workers/workloads/datasource-transaction-oaoo/datasource_transaction_oaoo_workload.py --rung {plan.rung_id} --case {plan.case_id}"
                    ),
                    "seed_policy": "Exact rung seeds from datasource-transaction-oaoo rung records.",
                    "invariant_oracle": (
                        "Independent intent model plus app ledger, datasource_outputs, operation_outputs, "
                        "notification rows, retry attempt counts, workflow status rows, and workflow result agreement."
                    ),
                },
            )
            if plan.rung_id == RUNG_005_ID:
                summaries.append(run_transactional_send_case(plan, case_artifacts))
            elif plan.rung_id == RUNG_006_ID:
                summaries.append(run_dbapi_retry_liveness_case(plan, case_artifacts))
            elif plan.rung_id == RUNG_002_ID:
                summaries.append(run_enqueue_boundary_case(plan, case_artifacts))
            elif plan.rung_id == RUNG_003_ID:
                summaries.append(run_retry_cleanup_case(plan, case_artifacts))
            elif plan.rung_id == RUNG_004_ID and plan.variant in {
                "enqueue-commit",
                "enqueue-rollback",
            }:
                summaries.append(run_enqueue_boundary_case(plan, case_artifacts))
            elif plan.rung_id == RUNG_004_ID and plan.variant in {
                "rollback-no-effect",
                "retry-after-commit",
                "cleanup-after-result",
            }:
                summaries.append(run_retry_cleanup_case(plan, case_artifacts))
            else:
                summaries.append(run_case(plan, case_artifacts))
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
