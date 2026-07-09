#!/usr/bin/env python3
"""WIO workload for DBOS incremental garbage-collection OAOO / orphan reuse.

Frontier: gc-orphan-oaoo
Rung:
  - rung-001-gc-orphan-oaoo
Protected product promise:
  DBOS incremental garbage collection (PR #751) preserves exactly-once
  correctness: once a workflow_id's durable state is (partly or fully) GC'd,
  reusing that workflow_id (a normal idempotency-key pattern) executes the new
  workflow FRESH -- it must never replay a dead workflow's stale, orphaned
  transaction output.

Mechanism under test:
  garbage_collect() runs in two phases across two databases with no shared
  transaction: sys-db deletes workflow_status, then app-db deletes
  transaction_outputs. If a process dies between phases, transaction_outputs
  rows are orphaned (their workflow_status is gone). transaction_outputs is
  keyed by (workflow_uuid, function_id); check_transaction_execution replays
  ANY matching row and SKIPS the step body. A NEW workflow that reuses the GC'd
  workflow_id whose first transaction lands on the same function_id collides
  with the orphan and replays the dead workflow's stale output = OAOO
  violation.

Replay:
  python .workers/workloads/gc-orphan-oaoo/gc_orphan_oaoo_workload.py \
    --rung rung-001-gc-orphan-oaoo --case case-001
  python .workers/workloads/gc-orphan-oaoo/gc_orphan_oaoo_workload.py \
    --rung rung-001-gc-orphan-oaoo --case case-002

Cases:
  case-001 baseline-full-gc-reuse (seed 8101) -- CONTROL, all PASS.
  case-002 partial-gc-orphan-reuse (seed 8102) -- the finding; p3 FAILs RED if
    the orphan is replayed / body skipped / a conflict is raised.

Invariant oracle:
  An independent app-side side-effect ledger (wio_gc_effects: one row per real
  body execution) is compared with the workflow's public return value and the
  raw workflow_status / transaction_outputs rows.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
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
    from dbos._dbos import _get_dbos_instance
    from dbos._schemas.application_database import ApplicationSchema
    from dbos._schemas.system_database import SystemSchema
    from dbos._workflow_commands import garbage_collect
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "gc-orphan-oaoo"
RUNG_ID = "rung-001-gc-orphan-oaoo"
APP_ID = "wio-gc-orphan-oaoo"
APP_VERSION = "wio-gc-orphan-oaoo-rung-001"

CASE_MATRIX: dict[str, tuple[int, str]] = {
    "case-001": (8101, "baseline-full-gc-reuse"),
    "case-002": (8102, "partial-gc-orphan-reuse"),
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


# --------------------------------------------------------------------------- #
# DBOS application code under test.
# --------------------------------------------------------------------------- #
@DBOS.transaction()
def stamp(n: int) -> str:
    # A real, observable side effect: one fresh row per genuine body execution.
    DBOS.sql_session.execute(
        sa.text("INSERT INTO wio_gc_effects(exec_uuid, workflow_id, n) VALUES (:u, :w, :n)"),
        {"u": str(uuid.uuid4()), "w": DBOS.workflow_id, "n": n},
    )
    return f"result-{n}"


@DBOS.workflow()
def gc_probe(n: int) -> str:
    return stamp(n)


# --------------------------------------------------------------------------- #
# Emission helpers.
# --------------------------------------------------------------------------- #
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


# --------------------------------------------------------------------------- #
# Postgres setup.
# --------------------------------------------------------------------------- #
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
    if os.environ.get("WIO_GC_ORPHAN_KEEP_DATABASES") == "1":
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
        "executor_id": f"wio-gc-orphan-{plan.case_id}",
        "enable_otlp": False,
    }


def launch_dbos(config: DBOSConfig) -> Any:
    DBOS.destroy(destroy_registry=False)
    dbos = DBOS(config=config)
    DBOS.launch()
    return dbos


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
        database_prefix=f"wio_gc_orphan_{digest}",
        workflow_id=f"{FRONTIER_ID}-{case_id}-{seed}-{rng.randint(1000, 9999)}",
    )


# --------------------------------------------------------------------------- #
# Raw-row observers (independent of the DBOS public API).
# --------------------------------------------------------------------------- #
def create_effects_table(dbos: Any) -> None:
    with dbos._app_db.engine.begin() as conn:
        conn.execute(
            sa.text(
                "CREATE TABLE IF NOT EXISTS wio_gc_effects("
                "exec_uuid TEXT PRIMARY KEY, workflow_id TEXT NOT NULL, n INT NOT NULL)"
            )
        )


def effects_count(dbos: Any, workflow_id: str, n: int) -> int:
    with dbos._app_db.engine.connect() as conn:
        return int(
            conn.execute(
                sa.text(
                    "SELECT count(*) FROM wio_gc_effects WHERE workflow_id = :w AND n = :n"
                ),
                {"w": workflow_id, "n": n},
            ).scalar()
        )


def sys_status_count(dbos: Any, workflow_id: str) -> int:
    with dbos._sys_db.engine.connect() as conn:
        return int(
            conn.execute(
                sa.select(sa.func.count())
                .select_from(SystemSchema.workflow_status)
                .where(SystemSchema.workflow_status.c.workflow_uuid == workflow_id)
            ).scalar()
        )


def app_txn_output_rows(dbos: Any, workflow_id: str) -> list[dict[str, Any]]:
    with dbos._app_db.engine.connect() as conn:
        rows = conn.execute(
            sa.select(
                ApplicationSchema.transaction_outputs.c.workflow_uuid,
                ApplicationSchema.transaction_outputs.c.function_id,
                ApplicationSchema.transaction_outputs.c.function_name,
                ApplicationSchema.transaction_outputs.c.output,
            ).where(
                ApplicationSchema.transaction_outputs.c.workflow_uuid == workflow_id
            )
        ).mappings()
        return [dict(row) for row in rows]


def wait_success(workflow_id: str, timeout_sec: float = 30.0) -> str:
    deadline = time.time() + timeout_sec
    last = None
    while time.time() < deadline:
        last = DBOS.retrieve_workflow(workflow_id).get_status().status
        if last in ("SUCCESS", "ERROR", "CANCELLED"):
            return last
        time.sleep(0.1)
    return last if last is not None else "UNKNOWN"


# --------------------------------------------------------------------------- #
# Cases.
# --------------------------------------------------------------------------- #
def case_baseline_full_gc_reuse(dbos: Any, plan: CasePlan) -> dict[str, Any]:
    wid_a = plan.workflow_id

    # 1. Run once, wait SUCCESS.
    with SetWorkflowID(wid_a):
        r1 = gc_probe(1)
    status1 = wait_success(wid_a)
    event("baseline_first_run", workflow_id=wid_a, r1=r1, status=status1)
    invariant("b1_first_output", "first_output", r1 == "result-1", r1=r1, status=status1)

    # 2. Full GC (both phases, complete): cutoff in the future so the completed
    #    SUCCESS workflow is eligible.
    cutoff = now_ms() + 3_600_000
    garbage_collect(dbos, cutoff, None)
    event("baseline_full_gc", cutoff=cutoff)

    # 3. Both stores cleared for wid_a.
    sys_after = sys_status_count(dbos, wid_a)
    app_after = len(app_txn_output_rows(dbos, wid_a))
    invariant(
        "b2_full_gc_clears_both",
        "full_gc_clears_both",
        sys_after == 0 and app_after == 0,
        sys_status_rows=sys_after,
        transaction_output_rows=app_after,
    )

    # 4. Reuse the id: with GC complete, it must execute FRESH.
    with SetWorkflowID(wid_a):
        r2 = gc_probe(2)
    wait_success(wid_a)
    eff2 = effects_count(dbos, wid_a, 2)
    invariant(
        "b3_reused_id_executes_fresh",
        "reused_id_executes_fresh",
        r2 == "result-2" and eff2 == 1,
        r2=r2,
        effects_n2=eff2,
    )
    return {
        "scenario": plan.scenario,
        "workflow_id": wid_a,
        "first_output": r1,
        "second_output": r2,
        "effects_n2": eff2,
        "sys_status_rows_after_gc": sys_after,
        "transaction_output_rows_after_gc": app_after,
    }


def case_partial_gc_orphan_reuse(dbos: Any, plan: CasePlan) -> dict[str, Any]:
    wid_b = plan.workflow_id

    # 1. Run once, wait SUCCESS.
    with SetWorkflowID(wid_b):
        r10 = gc_probe(10)
    status1 = wait_success(wid_b)
    event("partial_first_run", workflow_id=wid_b, r10=r10, status=status1)
    invariant("p1_first_output", "first_output", r10 == "result-10", r10=r10, status=status1)

    # 2. PARTIAL GC: crash between phases -- run ONLY the sys-db phase, never the
    #    app-db phase. transaction_outputs for wid_b is left orphaned.
    cutoff = now_ms() + 3_600_000
    res = dbos._sys_db.garbage_collect(cutoff, None, 100)
    event("partial_sysdb_only_gc", cutoff=cutoff, sysdb_gc_result=res)

    # 3. Reachability: the fault window (sys row gone, app orphan present) exists.
    sys_after = sys_status_count(dbos, wid_b)
    app_rows = app_txn_output_rows(dbos, wid_b)
    invariant(
        "p2_orphan_window_reached",
        "orphan_window_reached",
        sys_after == 0 and len(app_rows) == 1,
        sys_status_rows=sys_after,
        transaction_output_rows=len(app_rows),
        orphan_rows=app_rows,
    )

    # 4. Reuse the id -- capture the outcome without aborting the run.
    try:
        with SetWorkflowID(wid_b):
            r20 = gc_probe(20)
        outcome = f"returned:{r20}"
    except Exception as e:
        outcome = f"raised:{type(e).__name__}:{e}"
    effects_n20 = effects_count(dbos, wid_b, 20)
    event("partial_reuse", outcome=outcome, effects_n20=effects_n20)

    # 5. MONEY ORACLE: reusing the id after a partial GC must execute FRESH.
    invariant(
        "p3_reused_id_after_partial_gc_executes_fresh",
        "reused_id_after_partial_gc_executes_fresh",
        outcome == "returned:result-20" and effects_n20 == 1,
        outcome=outcome,
        effects_n20=effects_n20,
        note="RED if stale result-10 replayed / body skipped / conflict raised",
    )
    return {
        "scenario": plan.scenario,
        "workflow_id": wid_b,
        "first_output": r10,
        "sysdb_gc_result": res,
        "sys_status_rows_after_partial_gc": sys_after,
        "orphan_transaction_output_rows": app_rows,
        "reuse_outcome": outcome,
        "effects_n20": effects_n20,
    }


def run_case(plan: CasePlan, artifact_dir: Path) -> int:
    case_artifacts = artifact_dir / plan.case_id
    case_artifacts.mkdir(parents=True, exist_ok=True)
    write_json(case_artifacts / "plan.json", asdict(plan))
    app_url, sys_url, admin_masked = prepare_databases(plan.database_prefix, case_artifacts)
    config = make_config(plan, app_url, sys_url)
    dbos = launch_dbos(config)
    create_effects_table(dbos)
    event(
        "case_start",
        frontier=FRONTIER_ID,
        rung=plan.rung_id,
        admin_url=admin_masked,
        **asdict(plan),
    )
    try:
        if plan.scenario == "baseline-full-gc-reuse":
            result = case_baseline_full_gc_reuse(dbos, plan)
        elif plan.scenario == "partial-gc-orphan-reuse":
            result = case_partial_gc_orphan_reuse(dbos, plan)
        else:
            raise SetupBlock(f"unsupported scenario {plan.scenario}")
        write_json(case_artifacts / "result.json", result)
        event("case_passed", case=plan.case_id, result_summary=result)
        return 0
    finally:
        DBOS.destroy(destroy_registry=False)
        if os.environ.get("WIO_CLEANUP_AFTER", "1") == "1":
            drop_databases(plan.database_prefix)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DBOS GC orphan OAOO workload")
    parser.add_argument("--rung", default=RUNG_ID)
    parser.add_argument("--case", choices=sorted(CASE_MATRIX))
    parser.add_argument("--seed", type=int)
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument(
        "--artifact-dir",
        default="/tmp/wio-artifacts/gc-orphan-oaoo",
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
