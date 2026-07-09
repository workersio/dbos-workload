#!/usr/bin/env python3
"""WIO workload for the DBOS #763 workflow-invoke outcome-pipeline restructure.

Frontier: workflow-invoke-outcome-pipeline
Rung:
  - rung-001-concurrent-and-replay-invoke
Protected product promise:
  #763 restructured `_core.workflow_wrapper` from `.wrap(init_wf)` to
  `.wrap(get_wf_invoke).intercept(check_and_init)`. Every workflow invocation
  flows through it. `check_and_init` decides `should_execute`; on the
  not-execute branch it returns the recorded result (or re-raises the recorded
  error) WITHOUT running the body, populating the shared-closure `init_status`
  only on the execute branch. For async, `Pending._intercept`/`_wrap` run
  `check_and_init` and `get_wf_invoke` on separate `asyncio.to_thread` threads.
  Invariant: the body runs exactly once; concurrent same-id and completed-async
  re-invocation callers get the identical recorded outcome; a recorded ERROR
  re-raises the ORIGINAL exception type, never a pipeline internal.
Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/workflow-invoke-outcome-pipeline/workflow_invoke_outcome_pipeline_workload.py \
    --rung rung-001-concurrent-and-replay-invoke --case case-001
Seed policy:
  Exact case seeds are 7631 (concurrent same-id, direct sync path from two
  threads), 7632 (completed-async result replay), 7633 (completed-async ERROR
  re-raise).
Invariant oracle:
  An independent app-DB ledger (one row per REAL body execution, keyed by a
  fresh uuid + DBOS.workflow_id) proves exactly-once independently of DBOS.
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
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
VENDOR_ROOT = REPO_ROOT / ".workers" / "vendor" / "dbos-transact-py"
LOCAL_TARGET = REPO_ROOT / "target"
VENV_ROOT = REPO_ROOT / ".workers" / "vendor" / "dbos-venv"

# Fork-native: prefer the repo-root `dbos/` package (the #763 target source).
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

# Repo-root dbos wins over the vendored copies (all currently md5-identical).
sys.path.insert(0, str(REPO_ROOT))

try:
    import sqlalchemy as sa
    from sqlalchemy.engine import make_url

    from dbos import DBOS, DBOSConfig, SetWorkflowID
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "workflow-invoke-outcome-pipeline"
RUNG_ID = "rung-001-concurrent-and-replay-invoke"
APP_ID = "wio-invoke-pipeline"
APP_VERSION = "wio-workflow-invoke-outcome-pipeline-rung-001"

CASE_MATRIX: dict[str, tuple[int, str]] = {
    "case-001": (7631, "concurrent-same-id"),
    "case-002": (7632, "completed-async-result-replay"),
    "case-003": (7633, "completed-async-error-reraise"),
}

# Pipeline-internal exception types that must NEVER surface to a caller. If a
# recorded-error re-invoke re-raises one of these, the outcome pipeline leaked
# an internal instead of the original app error -> RED.
PIPELINE_LEAK_TYPES = {
    "KeyError",
    "AttributeError",
    "TypeError",
    "NoResult",
    "IndexError",
    "NameError",
    "UnboundLocalError",
}


class WorkloadFailure(Exception):
    pass


class SetupBlock(Exception):
    pass


class ProbeError(Exception):
    """Custom, module-level (picklable) application error for case-003."""


# --- Independent side-effect ledger, plumbed to the workflow bodies -----------
# Set per-case to the case's application-DB engine before any invocation.
_LEDGER_ENGINE: Any = None
# case-001 shared reachability observations, reset per run.
_C1: dict[str, Any] = {}


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


def invariant(id_: str, name: str, ok: bool, **fields: Any) -> None:
    status = "PASS" if ok else "FAIL"
    summary = json.dumps(fields, sort_keys=True, default=str) if fields else "ok"
    print(f"INVARIANT {id_} {name} {status} {summary}", flush=True)
    if not ok:
        raise WorkloadFailure(f"{id_} {name} failed: {summary}")


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
    if os.environ.get("WIO_INVOKE_PIPELINE_KEEP_DATABASES") == "1":
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
        "executor_id": f"wio-invoke-pipeline-{plan.case_id}",
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
        database_prefix=f"wio_invoke_pipeline_{digest}",
        workflow_id=f"{FRONTIER_ID}-{case_id}-{seed}-{rng.randint(1000, 9999)}",
    )


# --- Ledger helpers -----------------------------------------------------------
def ensure_ledger(engine: Any) -> None:
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "CREATE TABLE IF NOT EXISTS wio_invoke_execs ("
                "exec_uuid TEXT PRIMARY KEY, workflow_id TEXT, tag TEXT)"
            )
        )


def record_exec(engine: Any, workflow_id: str | None, tag: str) -> str:
    exec_uuid = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO wio_invoke_execs (exec_uuid, workflow_id, tag) "
                "VALUES (:u, :w, :t)"
            ),
            {"u": exec_uuid, "w": workflow_id, "t": tag},
        )
    return exec_uuid


def ledger_rows(engine: Any, workflow_id: str) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT exec_uuid, workflow_id, tag FROM wio_invoke_execs "
                "WHERE workflow_id = :w ORDER BY exec_uuid"
            ),
            {"w": workflow_id},
        ).mappings()
        return [dict(row) for row in rows]


# --- Workflow bodies (module-level registration) ------------------------------
@DBOS.workflow()
def concurrent_body(sleep_s: float, tag: str) -> dict[str, Any]:
    """case-001: sync body. Both callers race under one id; body runs once."""
    wfid = DBOS.workflow_id
    _C1["body_thread_ident"] = threading.get_ident()
    _C1["body_started_ms"] = now_ms()
    # Stay in-flight so the concurrent caller genuinely overlaps this execution.
    time.sleep(sleep_s)
    exec_uuid = record_exec(_LEDGER_ENGINE, wfid, tag)
    _C1["body_committed_ms"] = now_ms()
    _C1.setdefault("execs", []).append(exec_uuid)
    return {"wfid": wfid, "tag": tag, "value": 42, "exec_uuid": exec_uuid}


@DBOS.workflow()
async def replay_body(tag: str) -> dict[str, Any]:
    """case-002: async body driven to SUCCESS terminal, then re-invoked."""
    wfid = DBOS.workflow_id
    await asyncio.sleep(0.05)
    exec_uuid = await asyncio.to_thread(record_exec, _LEDGER_ENGINE, wfid, tag)
    return {"wfid": wfid, "tag": tag, "value": 4242, "exec_uuid": exec_uuid}


@DBOS.workflow()
async def error_body(tag: str) -> dict[str, Any]:
    """case-003: async body records it ran, then raises the original app error."""
    wfid = DBOS.workflow_id
    await asyncio.sleep(0.02)
    await asyncio.to_thread(record_exec, _LEDGER_ENGINE, wfid, tag)
    raise ProbeError(f"probe-failure-{tag}")


# --- Cases --------------------------------------------------------------------
def case_concurrent_same_id(dbos: Any, plan: CasePlan) -> dict[str, Any]:
    _C1.clear()
    wfid = plan.workflow_id
    sleep_s = 0.4
    barrier = threading.Barrier(2)
    results: dict[int, Any] = {}
    starts: dict[int, int] = {}
    returns: dict[int, int] = {}
    idents: dict[int, int] = {}
    errors: dict[int, str] = {}

    def invoke(idx: int) -> None:
        idents[idx] = threading.get_ident()
        barrier.wait()
        starts[idx] = now_ms()
        try:
            with SetWorkflowID(wfid):
                results[idx] = concurrent_body(sleep_s, f"case-001-caller-{idx}")
        except Exception as exc:  # captured, surfaced as a FAIL below
            errors[idx] = f"{type(exc).__name__}: {exc}"
        finally:
            returns[idx] = now_ms()

    threads = [threading.Thread(target=invoke, args=(i,)) for i in (0, 1)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    invariant(
        "invoke-concurrent-no-caller-error",
        "both-callers-returned-cleanly",
        not errors and len(results) == 2,
        errors=errors,
        results=results,
    )

    rows = ledger_rows(dbos._app_db.engine, wfid)
    body_ident = _C1.get("body_thread_ident")
    body_committed_ms = _C1.get("body_committed_ms")
    # Identify winner (ran the body inline) and loser (awaited the recorded result).
    winner_idx = next((i for i, ident in idents.items() if ident == body_ident), None)
    loser_idx = next((i for i in idents if i != winner_idx), None)

    # Reachability: prove genuine overlap. The loser began before the body
    # committed and only returned at/after the commit -> it was in-flight,
    # blocked on the winner's recorded result, across the whole body execution.
    overlap_ok = (
        winner_idx is not None
        and loser_idx is not None
        and body_committed_ms is not None
        and starts[loser_idx] <= body_committed_ms
        and returns[loser_idx] >= body_committed_ms
    )
    invariant(
        "invoke-concurrent-reachability",
        "both-invocations-in-flight-overlapping-body",
        overlap_ok,
        winner_idx=winner_idx,
        loser_idx=loser_idx,
        starts=starts,
        returns=returns,
        body_started_ms=_C1.get("body_started_ms"),
        body_committed_ms=body_committed_ms,
    )

    # body-runs-once: independent ledger says the body executed exactly once.
    invariant(
        "invoke-concurrent-body-runs-once",
        "ledger-rows-equal-one",
        len(rows) == 1,
        ledger_rows=rows,
        ledger_count=len(rows),
    )

    # callers-agree: both callers observed the identical recorded result.
    both = results.get(0), results.get(1)
    invariant(
        "invoke-concurrent-callers-agree",
        "both-callers-identical-result",
        both[0] == both[1] and both[0] is not None,
        caller0=both[0],
        caller1=both[1],
    )

    return {
        "scenario": plan.scenario,
        "workflow_id": wfid,
        "ledger_rows": rows,
        "ledger_count": len(rows),
        "winner_idx": winner_idx,
        "loser_idx": loser_idx,
        "starts": starts,
        "returns": returns,
        "body_started_ms": _C1.get("body_started_ms"),
        "body_committed_ms": body_committed_ms,
        "caller_results": results,
        "status": workflow_status(dbos, wfid),
    }


def case_completed_async_result_replay(dbos: Any, plan: CasePlan) -> dict[str, Any]:
    wfid = plan.workflow_id
    engine = dbos._app_db.engine
    observations: dict[str, Any] = {}

    async def drive() -> None:
        with SetWorkflowID(wfid):
            observations["first"] = await replay_body("case-002-first")
        observations["status_after_first"] = await workflow_status_async(dbos, wfid)
        # Re-invoke the SAME id with a DIFFERENT tag: a wrongful re-run would
        # return / record the mutated tag.
        with SetWorkflowID(wfid):
            observations["second"] = await replay_body("case-002-second-MUTATED")

    asyncio.run(drive())

    rows = ledger_rows(engine, wfid)
    status_after_first = observations["status_after_first"]

    # Reachability: the workflow reached a SUCCESS terminal before re-invoke.
    invariant(
        "invoke-replay-reachability",
        "terminal-success-before-reinvoke",
        status_after_first is not None and status_after_first["status"] == "SUCCESS",
        status_after_first=status_after_first,
    )

    invariant(
        "invoke-replay-body-runs-once",
        "ledger-rows-equal-one",
        len(rows) == 1 and rows and rows[0]["tag"] == "case-002-first",
        ledger_rows=rows,
        ledger_count=len(rows),
    )

    first = observations["first"]
    second = observations["second"]
    invariant(
        "invoke-replay-callers-agree",
        "reinvoke-returns-recorded-result",
        second == first and second.get("tag") == "case-002-first",
        first=first,
        second=second,
    )

    return {
        "scenario": plan.scenario,
        "workflow_id": wfid,
        "ledger_rows": rows,
        "ledger_count": len(rows),
        "first": first,
        "second": second,
        "status_after_first": status_after_first,
        "status_final": workflow_status(dbos, wfid),
    }


def case_completed_async_error_reraise(dbos: Any, plan: CasePlan) -> dict[str, Any]:
    wfid = plan.workflow_id
    engine = dbos._app_db.engine
    observations: dict[str, Any] = {}

    async def drive() -> None:
        with SetWorkflowID(wfid):
            try:
                await error_body("case-003-first")
                observations["first_raised"] = None
            except BaseException as exc:  # noqa: BLE001 - we classify below
                observations["first_type"] = type(exc).__name__
                observations["first_msg"] = str(exc)
                observations["first_raised"] = f"{type(exc).__name__}: {exc}"
        observations["status_after_first"] = await workflow_status_async(dbos, wfid)
        # Re-invoke the completed-ERROR workflow under the same id.
        with SetWorkflowID(wfid):
            try:
                await error_body("case-003-second-MUTATED")
                observations["reraise_type"] = None
                observations["reraise_msg"] = None
                observations["reraised"] = None
            except BaseException as exc:  # noqa: BLE001
                observations["reraise_type"] = type(exc).__name__
                observations["reraise_msg"] = str(exc)
                observations["reraised"] = f"{type(exc).__name__}: {exc}"

    asyncio.run(drive())

    rows = ledger_rows(engine, wfid)
    status_after_first = observations["status_after_first"]

    # Reachability: first invocation raised (its intended app error) and the
    # workflow reached an ERROR terminal before re-invoke.
    first_type = observations.get("first_type")
    invariant(
        "invoke-error-reachability",
        "terminal-error-before-reinvoke",
        status_after_first is not None
        and status_after_first["status"] == "ERROR"
        and first_type == "ProbeError",
        first_raised=observations.get("first_raised"),
        status_after_first=status_after_first,
    )

    invariant(
        "invoke-error-body-runs-once",
        "ledger-rows-equal-one",
        len(rows) == 1 and rows and rows[0]["tag"] == "case-003-first",
        ledger_rows=rows,
        ledger_count=len(rows),
    )

    # error-fidelity: the re-invoke re-raised the ORIGINAL app error type/message,
    # NOT a pipeline-internal leak (KeyError/AttributeError/TypeError/NoResult...).
    reraise_type = observations.get("reraise_type")
    reraise_msg = observations.get("reraise_msg")
    original_msg = observations.get("first_msg")
    fidelity_ok = (
        reraise_type is not None
        and reraise_type not in PIPELINE_LEAK_TYPES
        and (reraise_type == "ProbeError" or "ProbeError" in (reraise_msg or ""))
        and reraise_msg == original_msg
    )
    invariant(
        "invoke-error-fidelity",
        "reinvoke-reraises-original-not-pipeline-internal",
        fidelity_ok,
        reraise_type=reraise_type,
        reraise_msg=reraise_msg,
        original_type=first_type,
        original_msg=original_msg,
        pipeline_leak_types=sorted(PIPELINE_LEAK_TYPES),
    )

    return {
        "scenario": plan.scenario,
        "workflow_id": wfid,
        "ledger_rows": rows,
        "ledger_count": len(rows),
        "first_raised": observations.get("first_raised"),
        "reraised": observations.get("reraised"),
        "status_after_first": status_after_first,
        "status_final": workflow_status(dbos, wfid),
    }


def _status_dict(status: Any) -> dict[str, Any] | None:
    if status is None:
        return None
    return {
        "workflow_id": status.workflow_id,
        "status": status.status,
        "name": status.name,
        "recovery_attempts": getattr(status, "recovery_attempts", None),
    }


def workflow_status(dbos: Any, workflow_id: str) -> dict[str, Any] | None:
    return _status_dict(dbos.get_workflow_status(workflow_id))


async def workflow_status_async(dbos: Any, workflow_id: str) -> dict[str, Any] | None:
    return _status_dict(await dbos.get_workflow_status_async(workflow_id))


def launch_dbos(config: DBOSConfig) -> Any:
    DBOS.destroy(destroy_registry=False)
    dbos = DBOS(config=config)
    DBOS.launch()
    return dbos


def run_case(plan: CasePlan, artifact_dir: Path) -> int:
    global _LEDGER_ENGINE
    case_artifacts = artifact_dir / plan.case_id
    case_artifacts.mkdir(parents=True, exist_ok=True)
    write_json(case_artifacts / "plan.json", asdict(plan))
    app_url, sys_url, admin_masked = prepare_databases(plan.database_prefix, case_artifacts)
    config = make_config(plan, app_url, sys_url)
    dbos = launch_dbos(config)
    _LEDGER_ENGINE = dbos._app_db.engine
    ensure_ledger(_LEDGER_ENGINE)
    event(
        "case_start",
        frontier=FRONTIER_ID,
        rung=plan.rung_id,
        admin_url=admin_masked,
        **asdict(plan),
    )
    try:
        if plan.scenario == "concurrent-same-id":
            result = case_concurrent_same_id(dbos, plan)
        elif plan.scenario == "completed-async-result-replay":
            result = case_completed_async_result_replay(dbos, plan)
        elif plan.scenario == "completed-async-error-reraise":
            result = case_completed_async_error_reraise(dbos, plan)
        else:
            raise SetupBlock(f"unsupported scenario {plan.scenario}")
        write_json(case_artifacts / "result.json", result)
        event(
            "case_passed",
            case=plan.case_id,
            workflow_id=plan.workflow_id,
            ledger_count=result.get("ledger_count"),
        )
        return 0
    finally:
        _LEDGER_ENGINE = None
        DBOS.destroy(destroy_registry=False)
        if os.environ.get("WIO_CLEANUP_AFTER", "1") == "1":
            drop_databases(plan.database_prefix)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="DBOS workflow-invoke outcome-pipeline (#763) workload"
    )
    parser.add_argument("--rung", default=RUNG_ID)
    parser.add_argument("--case", choices=sorted(CASE_MATRIX))
    parser.add_argument("--seed", type=int)
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument(
        "--artifact-dir",
        default="/tmp/wio-artifacts/workflow-invoke-outcome-pipeline",
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
