#!/usr/bin/env python3
"""WIO workload for DBOS schedule registry concurrency.

Frontier: schedule-registry-concurrency
Rung:
  - rung-001-concurrent-apply-live-update
Protected product promise:
  Public schedule application is atomic, idempotent, and live-update safe under
  concurrent callers and running schedule definition updates.
Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/schedule-registry-concurrency/schedule_registry_concurrency_workload.py \
    --rung rung-001-concurrent-apply-live-update --case case-001 --seed 7410
Seed policy:
  Exact case seeds are 7410, 7411, 7412, and 7413. Each case writes schedule
  snapshots, caller outcomes, and live firing ledger entries under the artifact
  directory.
Invariant oracle:
  Caller outcomes, public/client schedule rows, pause/last-fire state, live
  context version, schedule id replacement, workflow id uniqueness, and cleanup
  quiescence must agree with the modeled schedule state.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

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

    from dbos import DBOS, DBOSClient, DBOSConfig
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "schedule-registry-concurrency"
RUNG_ID = "rung-001-concurrent-apply-live-update"
APP_ID = "wio-sched-registry"
APP_VERSION = "wio-sched-registry-rung-001"
CRON_SLOW = "0 0 * * *"
CRON_FAST = "* * * * * *"

CASE_MATRIX: dict[str, tuple[int, str]] = {
    "case-001": (7410, "public-concurrent-apply"),
    "case-002": (7411, "mixed-public-client-concurrent-apply"),
    "case-003": (7412, "paused-last-fired-reapply-preservation"),
    "case-004": (7413, "live-reapply-stale-thread"),
}

FIRED_LOCK = threading.Lock()
FIRED_LEDGER: list[dict[str, Any]] = []


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
    schedule_name: str
    context_nonce: int
    caller_count: int
    timeout_sec: float
    live_wait_sec: float
    grace_sec: float


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
    if os.environ.get("WIO_SCHEDULE_REGISTRY_KEEP_DATABASES") == "1":
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
        "executor_id": f"wio-sched-{plan.case_id}",
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        "runtimeConfig": {"scheduler_polling_interval_sec": 0.25},
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
        database_prefix=f"wio_sched_{suffix}",
        schedule_name=f"wio-sched-{case_id}-{seed}",
        context_nonce=rng.randint(1000, 9999),
        caller_count=8,
        timeout_sec=20.0,
        live_wait_sec=12.0,
        grace_sec=2.5,
    )


@DBOS.workflow()
def schedule_registry_workflow(scheduled_at: datetime, ctx: Any) -> dict[str, Any]:
    row = {
        "workflow_id": DBOS.workflow_id,
        "scheduled_at": scheduled_at.isoformat(),
        "context": ctx,
        "observed_at_ms": now_ms(),
    }
    with FIRED_LOCK:
        FIRED_LEDGER.append(row)
    return row


def workflow_name() -> str:
    return str(getattr(schedule_registry_workflow, "dbos_function_name"))


def public_definition(plan: CasePlan, *, version: int, cron: str = CRON_SLOW) -> dict[str, Any]:
    return {
        "schedule_name": plan.schedule_name,
        "workflow_fn": schedule_registry_workflow,
        "schedule": cron,
        "context": {
            "case": plan.case_id,
            "seed": plan.seed,
            "version": version,
            "nonce": plan.context_nonce,
        },
        "cron_timezone": None,
        "queue_name": None,
    }


def client_definition(plan: CasePlan, *, version: int, cron: str = CRON_SLOW) -> dict[str, Any]:
    public = public_definition(plan, version=version, cron=cron)
    return {
        "schedule_name": public["schedule_name"],
        "workflow_name": workflow_name(),
        "workflow_class_name": None,
        "schedule": public["schedule"],
        "context": public["context"],
        "cron_timezone": public["cron_timezone"],
        "queue_name": public["queue_name"],
    }


def schedule_rows(name: str, client: DBOSClient | None = None) -> dict[str, Any]:
    public_rows = [
        row
        for row in DBOS.list_schedules(schedule_name_prefix=name)
        if row["schedule_name"] == name
    ]
    public_get = DBOS.get_schedule(name)
    payload: dict[str, Any] = {"public_list": public_rows, "public_get": public_get}
    if client is not None:
        client_rows = [
            row
            for row in client.list_schedules(schedule_name_prefix=name)
            if row["schedule_name"] == name
        ]
        payload["client_list"] = client_rows
        payload["client_get"] = client.get_schedule(name)
    return payload


def assert_single_current_row(
    name: str,
    snapshot: dict[str, Any],
    *,
    expected_context: dict[str, Any],
    expected_cron: str,
    expected_status: str = "ACTIVE",
    require_last_fired: bool | None = None,
) -> dict[str, Any]:
    rows = snapshot["public_list"]
    got = snapshot["public_get"]
    invariant("schedule-public-list-single-row", len(rows) == 1, snapshot=snapshot)
    invariant("schedule-public-get-present", got is not None, snapshot=snapshot)
    row = got if got is not None else rows[0]
    invariant("schedule-name-current", row["schedule_name"] == name, row=row)
    invariant("schedule-context-current", row["context"] == expected_context, row=row)
    invariant("schedule-cron-current", row["schedule"] == expected_cron, row=row)
    invariant("schedule-timezone-current", row["cron_timezone"] is None, row=row)
    invariant("schedule-queue-current", row["queue_name"] is None, row=row)
    invariant("schedule-status-current", row["status"] == expected_status, row=row)
    if require_last_fired is not None:
        ok = row["last_fired_at"] is not None if require_last_fired else row["last_fired_at"] is None
        invariant("schedule-last-fired-model", ok, row=row, require_last_fired=require_last_fired)
    if "client_list" in snapshot:
        client_rows = snapshot["client_list"]
        client_get = snapshot["client_get"]
        invariant("schedule-client-list-single-row", len(client_rows) == 1, snapshot=snapshot)
        invariant("schedule-client-get-present", client_get is not None, snapshot=snapshot)
        invariant("schedule-client-public-agree", client_get == row, public=row, client=client_get)
    return row


def concurrent_apply(
    plan: CasePlan,
    callers: list[tuple[str, Callable[[], None]]],
) -> list[dict[str, Any]]:
    barrier = threading.Barrier(len(callers) + 1)
    outcomes: list[dict[str, Any]] = []
    lock = threading.Lock()

    def worker(index: int, label: str, fn: Callable[[], None]) -> None:
        outcome: dict[str, Any] = {
            "index": index,
            "label": label,
            "ready_at_ms": now_ms(),
        }
        try:
            barrier.wait(timeout=plan.timeout_sec)
            started = time.monotonic()
            outcome["started_at_ms"] = now_ms()
            fn()
            outcome["elapsed_sec"] = time.monotonic() - started
            outcome["ok"] = True
        except BaseException as exc:
            outcome["ok"] = False
            outcome["error_type"] = type(exc).__name__
            outcome["error"] = str(exc)
        finally:
            outcome["finished_at_ms"] = now_ms()
            with lock:
                outcomes.append(outcome)

    threads = [
        threading.Thread(
            target=worker,
            args=(idx, label, fn),
            name=f"wio-sched-apply-{plan.case_id}-{idx}",
            daemon=True,
        )
        for idx, (label, fn) in enumerate(callers)
    ]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=plan.timeout_sec)
    for thread in threads:
        thread.join(timeout=plan.timeout_sec)
    for thread in threads:
        if thread.is_alive():
            outcomes.append(
                {
                    "label": thread.name,
                    "ok": False,
                    "error_type": "ThreadJoinTimeout",
                    "error": f"caller did not finish within {plan.timeout_sec}s",
                }
            )
    outcomes.sort(key=lambda item: item.get("index", 999))
    event("apply_outcomes", case=plan.case_id, outcomes=outcomes)
    return outcomes


def assert_all_callers_ok(outcomes: list[dict[str, Any]]) -> None:
    failures = [
        {
            "index": outcome.get("index"),
            "label": outcome.get("label"),
            "error_type": outcome.get("error_type"),
            "error": outcome.get("error"),
            "elapsed_sec": outcome.get("elapsed_sec"),
            "started_at_ms": outcome.get("started_at_ms"),
            "finished_at_ms": outcome.get("finished_at_ms"),
        }
        for outcome in outcomes
        if not outcome.get("ok")
    ]
    summary = {
        "caller_count": len(outcomes),
        "failure_count": len(failures),
        "ok_labels": [outcome["label"] for outcome in outcomes if outcome.get("ok")],
        "failures": failures,
    }
    invariant("concurrent-apply-callers-ok", not failures, summary=summary)


def wait_until(predicate: Callable[[], bool], timeout_sec: float, interval_sec: float = 0.05) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_sec)
    return predicate()


def fired_snapshot(schedule_name: str) -> list[dict[str, Any]]:
    with FIRED_LOCK:
        return [
            row
            for row in FIRED_LEDGER
            if isinstance(row.get("context"), dict)
            and row["context"].get("schedule_name", schedule_name) == schedule_name
        ]


def clear_fired_ledger() -> None:
    with FIRED_LOCK:
        FIRED_LEDGER.clear()


def run_public_concurrent_apply(plan: CasePlan) -> dict[str, Any]:
    definition = public_definition(plan, version=1)
    callers = [
        (
            f"public-{idx}",
            lambda definition=definition: DBOS.apply_schedules([definition]),
        )
        for idx in range(plan.caller_count)
    ]
    outcomes = concurrent_apply(plan, callers)
    snapshot = schedule_rows(plan.schedule_name)
    expected = dict(definition["context"])
    row = assert_single_current_row(
        plan.schedule_name,
        snapshot,
        expected_context=expected,
        expected_cron=definition["schedule"],
    )
    assert_all_callers_ok(outcomes)
    return {"outcomes": outcomes, "snapshot": snapshot, "row": row}


def run_mixed_public_client_apply(plan: CasePlan, client: DBOSClient) -> dict[str, Any]:
    public = public_definition(plan, version=1)
    client_entry = client_definition(plan, version=1)
    callers: list[tuple[str, Callable[[], None]]] = []
    for idx in range(plan.caller_count):
        if idx % 2 == 0:
            callers.append((f"public-{idx}", lambda public=public: DBOS.apply_schedules([public])))
        else:
            callers.append(
                (f"client-{idx}", lambda client_entry=client_entry: client.apply_schedules([client_entry]))
            )
    outcomes = concurrent_apply(plan, callers)
    snapshot = schedule_rows(plan.schedule_name, client)
    row = assert_single_current_row(
        plan.schedule_name,
        snapshot,
        expected_context=dict(public["context"]),
        expected_cron=public["schedule"],
    )
    assert_all_callers_ok(outcomes)
    return {"outcomes": outcomes, "snapshot": snapshot, "row": row}


def run_paused_last_fired_reapply(plan: CasePlan, client: DBOSClient) -> dict[str, Any]:
    first = public_definition(plan, version=1, cron=CRON_FAST)
    first["context"]["schedule_name"] = plan.schedule_name
    DBOS.apply_schedules([first])
    fired = wait_until(lambda: len(fired_snapshot(plan.schedule_name)) >= 1, plan.live_wait_sec)
    before_pause = schedule_rows(plan.schedule_name, client)
    invariant("live-schedule-fired-before-pause", fired, snapshot=before_pause, fired=fired_snapshot(plan.schedule_name))
    before_row = assert_single_current_row(
        plan.schedule_name,
        before_pause,
        expected_context=dict(first["context"]),
        expected_cron=CRON_FAST,
        require_last_fired=True,
    )
    DBOS.pause_schedule(plan.schedule_name)
    paused = schedule_rows(plan.schedule_name, client)
    paused_row = assert_single_current_row(
        plan.schedule_name,
        paused,
        expected_context=dict(first["context"]),
        expected_cron=CRON_FAST,
        expected_status="PAUSED",
        require_last_fired=True,
    )
    replacement_public = public_definition(plan, version=2, cron=CRON_SLOW)
    replacement_client = client_definition(plan, version=2, cron=CRON_SLOW)
    callers = [
        (
            f"public-reapply-{idx}",
            lambda replacement_public=replacement_public: DBOS.apply_schedules([replacement_public]),
        )
        if idx % 2 == 0
        else (
            f"client-reapply-{idx}",
            lambda replacement_client=replacement_client: client.apply_schedules([replacement_client]),
        )
        for idx in range(plan.caller_count)
    ]
    outcomes = concurrent_apply(plan, callers)
    after = schedule_rows(plan.schedule_name, client)
    after_row = assert_single_current_row(
        plan.schedule_name,
        after,
        expected_context=dict(replacement_public["context"]),
        expected_cron=CRON_SLOW,
        expected_status="PAUSED",
        require_last_fired=True,
    )
    invariant(
        "pause-last-fired-preserved",
        after_row["last_fired_at"] == paused_row["last_fired_at"] == before_row["last_fired_at"],
        before=before_row,
        paused=paused_row,
        after=after_row,
    )
    assert_all_callers_ok(outcomes)
    return {
        "outcomes": outcomes,
        "before_pause": before_pause,
        "paused": paused,
        "after": after,
        "fired": fired_snapshot(plan.schedule_name),
    }


def run_live_reapply_stale_thread(plan: CasePlan, client: DBOSClient) -> dict[str, Any]:
    first = public_definition(plan, version=1, cron=CRON_FAST)
    first["context"]["schedule_name"] = plan.schedule_name
    DBOS.apply_schedules([first])
    fired_v1 = wait_until(
        lambda: any(row["context"].get("version") == 1 for row in fired_snapshot(plan.schedule_name)),
        plan.live_wait_sec,
    )
    before = schedule_rows(plan.schedule_name, client)
    before_row = assert_single_current_row(
        plan.schedule_name,
        before,
        expected_context=dict(first["context"]),
        expected_cron=CRON_FAST,
        require_last_fired=True,
    )
    invariant("live-v1-fired-before-reapply", fired_v1, fired=fired_snapshot(plan.schedule_name), before=before)
    second = public_definition(plan, version=2, cron=CRON_FAST)
    second["context"]["schedule_name"] = plan.schedule_name
    DBOS.apply_schedules([second])
    after = schedule_rows(plan.schedule_name, client)
    after_row = assert_single_current_row(
        plan.schedule_name,
        after,
        expected_context=dict(second["context"]),
        expected_cron=CRON_FAST,
    )
    invariant(
        "live-reapply-fresh-schedule-id",
        after_row["schedule_id"] != before_row["schedule_id"],
        before=before_row,
        after=after_row,
    )
    reapply_ms = now_ms()
    fired_v2 = wait_until(
        lambda: any(
            row["context"].get("version") == 2 and row["observed_at_ms"] >= reapply_ms
            for row in fired_snapshot(plan.schedule_name)
        ),
        plan.live_wait_sec,
    )
    ledger = fired_snapshot(plan.schedule_name)
    stale_cutoff = reapply_ms + int(plan.grace_sec * 1000)
    stale_after_grace = [
        row
        for row in ledger
        if row["observed_at_ms"] >= stale_cutoff and row["context"].get("version") == 1
    ]
    workflow_ids = [row["workflow_id"] for row in ledger]
    invariant("live-v2-fired-after-reapply", fired_v2, fired=ledger, reapply_ms=reapply_ms)
    invariant("live-no-v1-after-grace", not stale_after_grace, stale_after_grace=stale_after_grace, fired=ledger)
    invariant(
        "live-workflow-ids-unique",
        len(workflow_ids) == len(set(workflow_ids)),
        workflow_ids=workflow_ids,
        fired=ledger,
    )
    return {"before": before, "after": after, "fired": ledger, "reapply_ms": reapply_ms}


def run_case(plan: CasePlan, artifact_dir: Path) -> int:
    case_artifacts = artifact_dir / plan.case_id
    case_artifacts.mkdir(parents=True, exist_ok=True)
    write_json(case_artifacts / "plan.json", asdict(plan))
    app_url, sys_url, admin_masked = prepare_databases(plan.database_prefix, case_artifacts)
    config = make_config(plan, app_url, sys_url)
    client: DBOSClient | None = None
    event(
        "case_start",
        frontier=FRONTIER_ID,
        rung=plan.rung_id,
        admin_url=admin_masked,
        **asdict(plan),
    )
    try:
        clear_fired_ledger()
        DBOS.destroy(destroy_registry=False)
        dbos = DBOS(config=config)
        DBOS.launch()
        client = DBOSClient(
            application_database_url=app_url,
            system_database_url=sys_url,
        )
        if plan.scenario == "public-concurrent-apply":
            result = run_public_concurrent_apply(plan)
        elif plan.scenario == "mixed-public-client-concurrent-apply":
            result = run_mixed_public_client_apply(plan, client)
        elif plan.scenario == "paused-last-fired-reapply-preservation":
            result = run_paused_last_fired_reapply(plan, client)
        elif plan.scenario == "live-reapply-stale-thread":
            result = run_live_reapply_stale_thread(plan, client)
        else:
            raise SetupBlock(f"unsupported scenario {plan.scenario}")
        write_json(case_artifacts / "result.json", result)
        event("case_passed", case=plan.case_id, result_summary=result)
        return 0
    finally:
        cleanup_snapshot: dict[str, Any] = {}
        try:
            cleanup_snapshot["before_delete"] = schedule_rows(plan.schedule_name, client)
            DBOS.delete_schedule(plan.schedule_name)
            time.sleep(1.0)
            cleanup_snapshot["after_delete"] = schedule_rows(plan.schedule_name, client)
            cleanup_snapshot["fired_after_delete"] = fired_snapshot(plan.schedule_name)
            write_json(case_artifacts / "cleanup.json", cleanup_snapshot)
        except Exception as exc:
            event(
                "cleanup_best_effort_failed",
                case=plan.case_id,
                error_type=type(exc).__name__,
                error=str(exc),
            )
        if client is not None:
            client.destroy()
        DBOS.destroy(destroy_registry=False)
        if os.environ.get("WIO_CLEANUP_AFTER", "1") == "1":
            drop_databases(plan.database_prefix)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DBOS schedule registry concurrency workload")
    parser.add_argument("--rung", default=RUNG_ID)
    parser.add_argument("--case", choices=sorted(CASE_MATRIX))
    parser.add_argument("--seed", type=int)
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument(
        "--artifact-dir",
        default="/tmp/wio-artifacts/schedule-registry-concurrency",
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
