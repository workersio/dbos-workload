#!/usr/bin/env python3
"""WIO workload for DBOS adopted-loop shutdown liveness.

Frontier: runtime-shutdown-event-loop-liveness
Rung:
  - rung-001-adopted-loop-timeout-destroy-liveness
Protected product promise:
  DBOS.destroy() and BackgroundEventLoop helpers remain bounded when DBOS has
  adopted an application event loop and timeout waiter tasks are pending.
Replay:
  python .workers/workloads/runtime-shutdown-event-loop-liveness/runtime_shutdown_event_loop_liveness_workload.py \
    --rung rung-001-adopted-loop-timeout-destroy-liveness --case case-001 --seed 7220
Seed policy:
  Exact case seeds are 7220, 7221, and 7222. Each case writes a lifecycle
  ledger under the artifact directory.
Invariant oracle:
  Loop identity, timeout-task count, destroy duration/thread join, same-loop
  submit classification, public workflow result/status, and relaunch result
  must agree within bounded time.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import threading
import time
import traceback
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

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

    from dbos import DBOS, DBOSConfig, SetWorkflowID, SetWorkflowTimeout
    from dbos._event_loop import BackgroundEventLoop
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "runtime-shutdown-event-loop-liveness"
RUNG_ID = "rung-001-adopted-loop-timeout-destroy-liveness"
APP_ID = "wio-runtime-shutdown"
APP_VERSION = "wio-runtime-shutdown-rung-001"

CASE_MATRIX: dict[str, tuple[int, str]] = {
    "case-001": (7220, "adopted-loop-destroy-with-timeout-task"),
    "case-002": (7221, "same-loop-submit-coroutine-guard"),
    "case-003": (7222, "destroy-then-relaunch-smoke"),
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
    relaunch_workflow_id: str
    payload: str
    timeout_sec: float
    join_bound_sec: float
    destroy_bound_sec: float


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
    if os.environ.get("WIO_RUNTIME_SHUTDOWN_KEEP_DATABASES") == "1":
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
        "executor_id": f"wio-shutdown-{plan.case_id}",
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
    suffix = f"{seed}_{case_id.replace('-', '_')}"
    return CasePlan(
        rung_id=RUNG_ID,
        case_id=case_id,
        seed=seed,
        scenario=scenario,
        database_prefix=f"wio_shutdown_{suffix}",
        workflow_id=f"{FRONTIER_ID}-{case_id}-{seed}-{rng.randint(1000, 9999)}",
        relaunch_workflow_id=f"{FRONTIER_ID}-{case_id}-relaunch-{seed}",
        payload=f"{case_id}-payload-{seed}",
        timeout_sec=30.0,
        join_bound_sec=120.0,
        destroy_bound_sec=20.0,
    )


def thread_stack(thread: threading.Thread) -> list[str]:
    frames = sys._current_frames()
    if thread.ident is None or thread.ident not in frames:
        return []
    return traceback.format_stack(frames[thread.ident])


def wait_for_timeout_task(dbos: Any, timeout_sec: float) -> dict[str, Any]:
    deadline = time.time() + timeout_sec
    last: dict[str, Any] = {"count": 0, "tasks": []}
    while time.time() < deadline:
        tasks = list(dbos._timeout_tasks)
        last = {
            "count": len(tasks),
            "tasks": [
                {
                    "id": id(task),
                    "done": task.done(),
                    "cancelled": task.cancelled(),
                }
                for task in tasks
            ],
        }
        if tasks:
            return last
        time.sleep(0.05)
    return last


async def wait_for_timeout_task_async(dbos: Any, timeout_sec: float) -> dict[str, Any]:
    deadline = time.time() + timeout_sec
    last: dict[str, Any] = {"count": 0, "tasks": []}
    while time.time() < deadline:
        tasks = list(dbos._timeout_tasks)
        last = {
            "count": len(tasks),
            "tasks": [
                {
                    "id": id(task),
                    "done": task.done(),
                    "cancelled": task.cancelled(),
                }
                for task in tasks
            ],
        }
        if tasks:
            return last
        await asyncio.sleep(0.05)
    return last


def run_adopted_loop_destroy(plan: CasePlan, config: DBOSConfig) -> dict[str, Any]:
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    ledger: dict[str, Any] = {
        "scenario": plan.scenario,
        "worker_thread": "wio-adopted-loop-destroy",
        "events": [],
    }
    errors: list[dict[str, Any]] = []

    @DBOS.workflow()
    def wf_with_timeout(payload: str) -> str:
        return f"done:{payload}"

    def worker_body() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def scenario() -> None:
            running_loop = asyncio.get_running_loop()
            DBOS.launch()
            target_loop = dbos._background_event_loop.target_loop()
            ledger["running_loop_id"] = id(running_loop)
            ledger["target_loop_id"] = id(target_loop)
            ledger["adopted_loop"] = target_loop is running_loop
            with SetWorkflowID(plan.workflow_id):
                with SetWorkflowTimeout(plan.timeout_sec):
                    result = wf_with_timeout(plan.payload)
            ledger["workflow_result"] = result
            ledger["timeout_tasks_before_destroy"] = await wait_for_timeout_task_async(dbos, 5.0)
            destroy_start = time.monotonic()
            DBOS.destroy(destroy_registry=True)
            ledger["destroy_duration_sec"] = time.monotonic() - destroy_start
            ledger["timeout_tasks_after_destroy"] = {
                "count": len(dbos._timeout_tasks),
                "tasks": [
                    {
                        "id": id(task),
                        "done": task.done(),
                        "cancelled": task.cancelled(),
                    }
                    for task in dbos._timeout_tasks
                ],
            }

        try:
            loop.run_until_complete(scenario())
        except BaseException as exc:
            errors.append({"type": type(exc).__name__, "message": str(exc)})
        finally:
            loop.close()

    worker = threading.Thread(target=worker_body, name="wio-adopted-loop-destroy", daemon=True)
    worker.start()
    worker.join(timeout=plan.join_bound_sec)
    ledger["worker_alive_after_join"] = worker.is_alive()
    ledger["worker_stack_after_join"] = thread_stack(worker) if worker.is_alive() else []
    ledger["errors"] = errors

    invariant("adopted-loop-worker-finished", not worker.is_alive(), ledger=ledger)
    invariant("adopted-loop-no-scenario-error", not errors, errors=errors)
    invariant("adopted-loop-target-proven", ledger.get("adopted_loop") is True, ledger=ledger)
    before = ledger.get("timeout_tasks_before_destroy", {})
    invariant("adopted-loop-timeout-task-before-destroy", before.get("count", 0) > 0, ledger=ledger)
    invariant("adopted-loop-destroy-bounded", ledger.get("destroy_duration_sec", plan.destroy_bound_sec + 1) < plan.destroy_bound_sec, ledger=ledger)
    after = ledger.get("timeout_tasks_after_destroy", {})
    invariant("adopted-loop-timeout-tasks-cleared", after.get("count") == 0, ledger=ledger)
    invariant("adopted-loop-workflow-result", ledger.get("workflow_result") == f"done:{plan.payload}", ledger=ledger)
    return ledger


def run_same_loop_submit_guard(plan: CasePlan) -> dict[str, Any]:
    ledger: dict[str, Any] = {"scenario": plan.scenario}

    async def scenario() -> None:
        bg = BackgroundEventLoop()
        bg.start()
        try:
            running_loop = asyncio.get_running_loop()

            async def noop() -> int:
                return 42

            ledger["running_loop_id"] = id(running_loop)
            ledger["target_loop_id"] = id(bg.target_loop())
            ledger["adopted_loop"] = bg.target_loop() is running_loop
            started = time.monotonic()
            try:
                result = bg.submit_coroutine(noop())
                ledger["submit_result"] = result
            except BaseException as exc:
                ledger["submit_error_type"] = type(exc).__name__
                ledger["submit_error_message"] = str(exc)
            ledger["submit_duration_sec"] = time.monotonic() - started
        finally:
            bg.stop()

    asyncio.run(scenario())
    invariant("same-loop-target-proven", ledger.get("adopted_loop") is True, ledger=ledger)
    invariant("same-loop-submit-runtime-error", ledger.get("submit_error_type") == "RuntimeError" and "deadlock" in ledger.get("submit_error_message", ""), ledger=ledger)
    invariant("same-loop-submit-bounded", ledger.get("submit_duration_sec", 999.0) < 1.0, ledger=ledger)
    return ledger


def run_relaunch_smoke(plan: CasePlan, config: DBOSConfig) -> dict[str, Any]:
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def relaunch_workflow(payload: str) -> str:
        return f"relaunch:{payload}"

    DBOS.launch()
    try:
        before_count = len(dbos._timeout_tasks)
        with SetWorkflowID(plan.relaunch_workflow_id):
            with SetWorkflowTimeout(5.0):
                result = relaunch_workflow(plan.payload)
        deadline = time.time() + 5.0
        while dbos._timeout_tasks and time.time() < deadline:
            time.sleep(0.05)
        status = DBOS.get_workflow_status(plan.relaunch_workflow_id)
        ledger = {
            "scenario": plan.scenario,
            "prior_destroy": "explicit DBOS.destroy(destroy_registry=True); matrix case-001 covers adopted-loop teardown",
            "timeout_count_before_relaunch_workflow": before_count,
            "timeout_count_after_relaunch_workflow": len(dbos._timeout_tasks),
            "workflow_result": result,
            "workflow_status": None if status is None else status.status,
        }
    finally:
        DBOS.destroy(destroy_registry=True)
    invariant("relaunch-starts-after-destroy", ledger["timeout_count_before_relaunch_workflow"] == 0, ledger=ledger)
    invariant("relaunch-workflow-result", ledger["workflow_result"] == f"relaunch:{plan.payload}", ledger=ledger)
    invariant("relaunch-workflow-status-success", ledger["workflow_status"] == "SUCCESS", ledger=ledger)
    invariant("relaunch-timeout-tasks-drained", ledger["timeout_count_after_relaunch_workflow"] == 0, ledger=ledger)
    return ledger


def run_case(plan: CasePlan, artifact_dir: Path) -> int:
    case_artifacts = artifact_dir / plan.case_id
    case_artifacts.mkdir(parents=True, exist_ok=True)
    write_json(case_artifacts / "plan.json", asdict(plan))
    app_url, sys_url, admin_masked = prepare_databases(plan.database_prefix, case_artifacts)
    config = make_config(plan, app_url, sys_url)
    event(
        "case_start",
        frontier=FRONTIER_ID,
        rung=plan.rung_id,
        admin_url=admin_masked,
        **asdict(plan),
    )
    try:
        if plan.scenario == "adopted-loop-destroy-with-timeout-task":
            result = run_adopted_loop_destroy(plan, config)
        elif plan.scenario == "same-loop-submit-coroutine-guard":
            result = run_same_loop_submit_guard(plan)
        elif plan.scenario == "destroy-then-relaunch-smoke":
            result = run_relaunch_smoke(plan, config)
        else:
            raise SetupBlock(f"unsupported scenario {plan.scenario}")
        write_json(case_artifacts / "result.json", result)
        event("case_passed", case=plan.case_id, result_summary=result)
        return 0
    finally:
        DBOS.destroy(destroy_registry=True)
        if os.environ.get("WIO_CLEANUP_AFTER", "1") == "1":
            drop_databases(plan.database_prefix)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DBOS runtime shutdown event-loop liveness workload")
    parser.add_argument("--rung", default=RUNG_ID)
    parser.add_argument("--case", choices=sorted(CASE_MATRIX))
    parser.add_argument("--seed", type=int)
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument(
        "--artifact-dir",
        default="/tmp/wio-artifacts/runtime-shutdown-event-loop-liveness",
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
