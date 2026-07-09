#!/usr/bin/env python3
"""WIO workload for DBOS Postgres migration startup liveness.

Frontier: migration-startup-liveness
Rung:
  - rung-001-migration-early-exit-advisory-lock
Protected product promise:
  DBOS Postgres startup skips migration advisory-lock waits when the system
  schema is already current, but stale or partial schemas still migrate safely
  after the lock holder releases.
Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/migration-startup-liveness/migration_startup_liveness_workload.py \
    --rung rung-001-migration-early-exit-advisory-lock --case case-001 --seed 6770
Seed policy:
  Exact seeds are 6770, 6771, 6772, and 6773. Each case writes its plan,
  lock timeline, migration worker results, schema rows, required table
  observations, and runtime smoke result under the artifact directory.
Invariant oracle:
  Independent lock timeline and schema-state observations must agree with
  elapsed worker behavior, final dbos_migrations version, required system
  tables, and a minimal DBOS runtime smoke.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor
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
    from dbos._migration import get_dbos_migrations
    from dbos._serialization import DefaultSerializer
    from dbos._sys_db import SystemDatabase
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "migration-startup-liveness"
RUNG_ID = "rung-001-migration-early-exit-advisory-lock"
APP_ID = "wio-migration-startup-liveness"
APP_VERSION = "wio-migration-startup-rung-001"
MIGRATION_LOCK_ID = 1234567890
SCHEMA = "dbos"

CASE_MATRIX: dict[str, tuple[int, str]] = {
    "case-001": (6770, "up-to-date-held-lock-single-start"),
    "case-002": (6771, "stale-version-held-lock-release"),
    "case-003": (6772, "missing-migrations-table-held-lock-release"),
    "case-004": (6773, "up-to-date-held-lock-concurrent-warm-starts"),
}

REQUIRED_TABLES = {
    "application_versions",
    "dbos_migrations",
    "notifications",
    "operation_outputs",
    "streams",
    "workflow_events",
    "workflow_status",
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
    schema: str
    worker_count: int
    early_exit_bound_sec: float
    held_observation_sec: float
    post_release_bound_sec: float


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


def latest_version(schema: str) -> int:
    return len(get_dbos_migrations(schema, use_listen_notify=True, is_cockroach=False))


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
    worker_count = 6 if case_id == "case-004" else 1
    return CasePlan(
        rung_id=RUNG_ID,
        case_id=case_id,
        seed=seed,
        scenario=scenario,
        database_prefix=f"wio_migration_{seed}_{case_id.replace('-', '_')}",
        workflow_id=f"{FRONTIER_ID}-{case_id}-{seed}-{rng.randint(1000, 9999)}",
        schema=SCHEMA,
        worker_count=worker_count,
        early_exit_bound_sec=8.0,
        held_observation_sec=2.5,
        post_release_bound_sec=120.0,
    )


def prepare_database(prefix: str, artifacts: Path) -> tuple[str, str]:
    base = admin_url()
    sys_db = f"{prefix}_sys"
    admin = base.set(database=base.database or "postgres").render_as_string(
        hide_password=False
    )
    masked = base.set(password="***" if base.password else None).render_as_string(
        hide_password=False
    )
    event("postgres_preflight", admin_url=masked, sys_db=sys_db)
    try:
        engine = sa.create_engine(admin, connect_args={"connect_timeout": 5})
        with engine.connect() as raw_connection:
            connection = raw_connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(sa.text("SET statement_timeout = '8000ms'"))
            connection.execute(
                sa.text(f"DROP DATABASE IF EXISTS {quote_ident(sys_db)} WITH (FORCE)")
            )
            connection.execute(sa.text(f"CREATE DATABASE {quote_ident(sys_db)}"))
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
        base.set(drivername="postgresql+psycopg", database=sys_db).render_as_string(
            hide_password=False
        ),
        masked,
    )


def drop_database(prefix: str) -> None:
    if os.environ.get("WIO_MIGRATION_KEEP_DATABASES") == "1":
        return
    base = admin_url()
    sys_db = f"{prefix}_sys"
    admin = base.set(database=base.database or "postgres").render_as_string(
        hide_password=False
    )
    engine = sa.create_engine(admin, connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as raw_connection:
            connection = raw_connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(sa.text("SET statement_timeout = '5000ms'"))
            connection.execute(sa.text("SET lock_timeout = '3000ms'"))
            connection.execute(
                sa.text(f"DROP DATABASE IF EXISTS {quote_ident(sys_db)} WITH (FORCE)")
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


def create_system_database(sys_url: str, schema: str) -> SystemDatabase:
    return SystemDatabase.create(
        system_database_url=sys_url,
        engine_kwargs={"connect_args": {"connect_timeout": 10}},
        engine=None,
        schema=schema,
        serializer=DefaultSerializer(),
        executor_id=None,
        use_listen_notify=True,
    )


def run_migration_worker(sys_url: str, schema: str, worker_id: str) -> dict[str, Any]:
    started_ms = now_ms()
    started_mono = time.monotonic()
    sys_db = create_system_database(sys_url, schema)
    try:
        sys_db.run_migrations()
        return {
            "worker_id": worker_id,
            "started_at_ms": started_ms,
            "ended_at_ms": now_ms(),
            "elapsed_sec": time.monotonic() - started_mono,
            "exception": None,
        }
    except BaseException as exc:
        return {
            "worker_id": worker_id,
            "started_at_ms": started_ms,
            "ended_at_ms": now_ms(),
            "elapsed_sec": time.monotonic() - started_mono,
            "exception": {
                "type": type(exc).__name__,
                "message": str(exc),
                "repr": repr(exc),
            },
        }
    finally:
        sys_db.destroy()


class AdvisoryLockHolder:
    def __init__(self, sys_url: str) -> None:
        self.engine = sa.create_engine(sys_url, connect_args={"connect_timeout": 5})
        self.connection = self.engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        )
        self.released = False
        self.connection.execute(sa.text("SET statement_timeout = '8000ms'"))
        self.pid = self.connection.execute(sa.text("SELECT pg_backend_pid()")).scalar_one()
        self.connection.execute(
            sa.text("SELECT pg_advisory_lock(:lock_id)"),
            {"lock_id": MIGRATION_LOCK_ID},
        )
        self.acquired_at_ms = now_ms()

    def is_held(self) -> bool:
        count = self.connection.execute(
            sa.text(
                "SELECT COUNT(*) FROM pg_locks "
                "WHERE locktype = 'advisory' AND pid = pg_backend_pid()"
            )
        ).scalar_one()
        return count > 0

    def release(self) -> dict[str, Any]:
        released_at_ms = now_ms()
        unlock_result = None
        if not self.released:
            unlock_result = self.connection.execute(
                sa.text("SELECT pg_advisory_unlock(:lock_id)"),
                {"lock_id": MIGRATION_LOCK_ID},
            ).scalar_one()
            self.released = True
        return {
            "pid": self.pid,
            "acquired_at_ms": self.acquired_at_ms,
            "released_at_ms": released_at_ms,
            "unlock_result": unlock_result,
        }

    def close(self) -> None:
        try:
            if not self.released:
                self.release()
        finally:
            self.connection.close()
            self.engine.dispose()


def migrate_to_latest(sys_url: str, schema: str) -> dict[str, Any]:
    result = run_migration_worker(sys_url, schema, "initial")
    if result["exception"] is not None:
        raise SetupBlock(f"initial migration failed: {result['exception']}")
    return result


def migration_rows(sys_url: str, schema: str) -> list[int]:
    engine = sa.create_engine(sys_url, connect_args={"connect_timeout": 5})
    try:
        with engine.begin() as conn:
            exists = conn.execute(
                sa.text(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = :schema AND table_name = 'dbos_migrations'"
                ),
                {"schema": schema},
            ).fetchone()
            if exists is None:
                return []
            rows = conn.execute(
                sa.text(f'SELECT version FROM "{schema}".dbos_migrations ORDER BY version')
            ).fetchall()
            return [int(row[0]) for row in rows]
    finally:
        engine.dispose()


def required_tables(sys_url: str, schema: str) -> list[str]:
    engine = sa.create_engine(sys_url, connect_args={"connect_timeout": 5})
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                sa.text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = :schema ORDER BY table_name"
                ),
                {"schema": schema},
            ).fetchall()
            return [str(row[0]) for row in rows]
    finally:
        engine.dispose()


def rewind_version(sys_url: str, schema: str, version: int) -> None:
    engine = sa.create_engine(sys_url, connect_args={"connect_timeout": 5})
    try:
        with engine.begin() as conn:
            conn.execute(
                sa.text(f'UPDATE "{schema}".dbos_migrations SET version = :version'),
                {"version": version},
            )
    finally:
        engine.dispose()


def create_partial_schema(sys_url: str, schema: str) -> None:
    engine = sa.create_engine(sys_url, connect_args={"connect_timeout": 5})
    try:
        with engine.begin() as conn:
            conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    finally:
        engine.dispose()


@DBOS.workflow()
def migration_smoke_workflow(payload: str) -> str:
    return f"migration-smoke:{payload}"


def make_config(plan: CasePlan, sys_url: str) -> DBOSConfig:
    return {
        "name": APP_ID,
        "application_database_url": None,
        "database_url": None,
        "system_database_url": sys_url,
        "dbos_system_schema": plan.schema,
        "application_version": f"{APP_VERSION}-{plan.case_id}",
        "executor_id": f"wio-migration-{plan.case_id}",
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.05,
        "runtimeConfig": {
            "run_admin_server": False,
            "scheduler_polling_interval_sec": 60.0,
        },
    }


def runtime_smoke(plan: CasePlan, sys_url: str) -> dict[str, Any]:
    workflow_id = f"{plan.workflow_id}-smoke"
    DBOS.destroy(destroy_registry=False)
    try:
        DBOS(config=make_config(plan, sys_url))
        DBOS.launch()
        with SetWorkflowID(workflow_id):
            result = migration_smoke_workflow(plan.case_id)
        status = DBOS.get_workflow_status(workflow_id)
        return {
            "workflow_id": workflow_id,
            "result": result,
            "status": None if status is None else status.status,
        }
    finally:
        DBOS.destroy(destroy_registry=False, workflow_completion_timeout_sec=2)


def collect_futures(
    futures: list[Future[dict[str, Any]]],
    timeout_sec: float,
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout_sec
    results: list[dict[str, Any]] = []
    for future in futures:
        remaining = max(0.1, deadline - time.monotonic())
        results.append(future.result(timeout=remaining))
    return results


def wait_until_all_done(futures: list[Future[dict[str, Any]]], timeout_sec: float) -> bool:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if all(future.done() for future in futures):
            return True
        time.sleep(0.05)
    return all(future.done() for future in futures)


def final_schema_observation(plan: CasePlan, sys_url: str) -> dict[str, Any]:
    return {
        "migration_rows": migration_rows(sys_url, plan.schema),
        "required_tables": required_tables(sys_url, plan.schema),
        "latest_version": latest_version(plan.schema),
    }


def assert_final_schema_current(plan: CasePlan, sys_url: str) -> dict[str, Any]:
    observation = final_schema_observation(plan, sys_url)
    smoke = runtime_smoke(plan, sys_url)
    observation["runtime_smoke"] = smoke
    expected_version = observation["latest_version"]
    invariant(
        "migration_final_version_exactly_latest",
        observation["migration_rows"] == [expected_version],
        case=plan.case_id,
        observation=observation,
    )
    invariant(
        "migration_required_tables_queryable",
        REQUIRED_TABLES.issubset(set(observation["required_tables"])),
        case=plan.case_id,
        required=sorted(REQUIRED_TABLES),
        observation=observation,
    )
    invariant(
        "migration_runtime_smoke_succeeds",
        smoke["result"] == f"migration-smoke:{plan.case_id}"
        and smoke["status"] == "SUCCESS",
        case=plan.case_id,
        smoke=smoke,
    )
    return observation


def run_up_to_date_case(plan: CasePlan, sys_url: str) -> dict[str, Any]:
    initial = migrate_to_latest(sys_url, plan.schema)
    before_rows = migration_rows(sys_url, plan.schema)
    holder = AdvisoryLockHolder(sys_url)
    lock_acquired = {
        "pid": holder.pid,
        "acquired_at_ms": holder.acquired_at_ms,
        "held_after_acquire": holder.is_held(),
    }
    futures: list[Future[dict[str, Any]]] = []
    results: list[dict[str, Any]]
    with ThreadPoolExecutor(max_workers=plan.worker_count) as executor:
        futures = [
            executor.submit(
                run_migration_worker,
                sys_url,
                plan.schema,
                f"warm-start-{index}",
            )
            for index in range(plan.worker_count)
        ]
        all_done_before_release = wait_until_all_done(
            futures, plan.early_exit_bound_sec
        )
        held_before_release = holder.is_held()
        release = holder.release()
        results = collect_futures(futures, plan.post_release_bound_sec)
    holder.close()
    final_observation = assert_final_schema_current(plan, sys_url)
    worker_errors = [result for result in results if result["exception"] is not None]
    max_elapsed = max(result["elapsed_sec"] for result in results)
    invariant(
        "up_to_date_migrations_return_before_lock_release",
        all_done_before_release
        and held_before_release
        and not worker_errors
        and max_elapsed < plan.early_exit_bound_sec,
        case=plan.case_id,
        worker_count=plan.worker_count,
        before_rows=before_rows,
        lock_acquired=lock_acquired,
        release=release,
        results=results,
    )
    invariant(
        "up_to_date_migrations_do_not_mutate_version",
        before_rows == final_observation["migration_rows"],
        case=plan.case_id,
        before_rows=before_rows,
        final=final_observation,
    )
    return {
        "initial_migration": initial,
        "lock_acquired": lock_acquired,
        "release": release,
        "worker_results": results,
        "final": final_observation,
    }


def run_locked_required_migration_case(
    plan: CasePlan,
    sys_url: str,
    *,
    partial: bool,
) -> dict[str, Any]:
    if partial:
        create_partial_schema(sys_url, plan.schema)
        setup = {"kind": "partial_schema", "before_rows": migration_rows(sys_url, plan.schema)}
    else:
        initial = migrate_to_latest(sys_url, plan.schema)
        stale_version = latest_version(plan.schema) - 1
        rewind_version(sys_url, plan.schema, stale_version)
        setup = {
            "kind": "stale_schema",
            "initial_migration": initial,
            "stale_version": stale_version,
            "before_rows": migration_rows(sys_url, plan.schema),
        }

    holder = AdvisoryLockHolder(sys_url)
    lock_acquired = {
        "pid": holder.pid,
        "acquired_at_ms": holder.acquired_at_ms,
        "held_after_acquire": holder.is_held(),
    }
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run_migration_worker, sys_url, plan.schema, "repair")
        time.sleep(plan.held_observation_sec)
        completed_before_release = future.done()
        held_before_release = holder.is_held()
        release = holder.release()
        result = future.result(timeout=plan.post_release_bound_sec)
    holder.close()
    final_observation = assert_final_schema_current(plan, sys_url)
    invariant_name = (
        "partial_schema_migration_waits_for_lock_release"
        if partial
        else "stale_schema_migration_waits_for_lock_release"
    )
    invariant(
        invariant_name,
        not completed_before_release
        and held_before_release
        and result["exception"] is None
        and result["ended_at_ms"] >= release["released_at_ms"],
        case=plan.case_id,
        setup=setup,
        lock_acquired=lock_acquired,
        release=release,
        worker_result=result,
        final=final_observation,
    )
    return {
        "setup": setup,
        "lock_acquired": lock_acquired,
        "release": release,
        "worker_results": [result],
        "final": final_observation,
    }


def run_case(plan: CasePlan, artifacts_root: Path) -> dict[str, Any]:
    artifacts = artifacts_root / plan.rung_id / plan.case_id
    write_json(artifacts / "plan.json", asdict(plan))
    sys_url, admin_masked = prepare_database(plan.database_prefix, artifacts)
    event(
        "case_start",
        frontier=FRONTIER_ID,
        rung=plan.rung_id,
        case=plan.case_id,
        seed=plan.seed,
        scenario=plan.scenario,
        admin_url=admin_masked,
    )
    try:
        if plan.case_id in {"case-001", "case-004"}:
            observations = run_up_to_date_case(plan, sys_url)
        elif plan.case_id == "case-002":
            observations = run_locked_required_migration_case(
                plan,
                sys_url,
                partial=False,
            )
        elif plan.case_id == "case-003":
            observations = run_locked_required_migration_case(
                plan,
                sys_url,
                partial=True,
            )
        else:
            raise SetupBlock(f"unsupported case: {plan.case_id}")
        write_json(artifacts / "observations.json", observations)
        event("case_complete", rung=plan.rung_id, case=plan.case_id, status="passed")
        return {"case": asdict(plan), "status": "passed"}
    finally:
        DBOS.destroy(destroy_registry=False, workflow_completion_timeout_sec=2)
        drop_database(plan.database_prefix)


def run_selected(args: argparse.Namespace) -> int:
    rung_id = RUNG_ID if args.rung in {RUNG_ID, "rung-001"} else args.rung
    if rung_id != RUNG_ID:
        raise SetupBlock(f"unsupported rung: {args.rung}")
    case_ids = list(CASE_MATRIX) if args.all_cases else [args.case]
    if not args.all_cases and args.case is None:
        raise SetupBlock("--case is required unless --all-cases is set")
    if args.seed is not None and len(case_ids) != 1:
        raise SetupBlock("--seed may only be used with a single --case")
    artifacts_root = Path(args.artifacts_dir)
    results = []
    for case_id in case_ids:
        plan = make_plan(rung_id, case_id, args.seed)
        results.append(run_case(plan, artifacts_root))
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
        default="/tmp/wio-artifacts/migration-startup-liveness",
    )
    args = parser.parse_args()
    try:
        return run_selected(args)
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 44
    except WorkloadFailure as exc:
        print(f"WORKLOAD-FAIL {exc}", flush=True)
        return 10


if __name__ == "__main__":
    raise SystemExit(main())
