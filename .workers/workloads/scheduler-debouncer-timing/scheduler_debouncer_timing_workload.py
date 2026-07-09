#!/usr/bin/env python3
"""Fresh WIO workload for DBOS scheduler/debouncer timing.

Frontier: scheduler-debouncer-timing
Rungs:
  - rung-000-timing-smoke
  - rung-001-debouncer-delayed-row
  - rung-002-many-key-worker-pressure
  - rung-003-schedule-overlap-observation
  - rung-004-bounded-seed-sweep
  - rung-005-scheduled-queue-controls-compose
  - rung-006-async-debouncer-worker-starvation
Evidence key:
  evidence-key:events/frontier_designer-20260620T072708734371000Z.prompt.md
Protected product promise:
  Timed and debounced work starts predictably, preserves latest intended input,
  and does not create unbounded worker pressure or stale handles.
Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py \
    --rung rung-001-debouncer-delayed-row --case case-001
Seed policy:
  Exact rung seeds are encoded below; each case writes the derived case JSON and
  operation schedule under the artifact directory.
Invariant oracle:
  Independent debounce model, public DBOS handle results, workflow status rows,
  internal debouncer row observations, and bounded liveness checks.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
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

    from dbos import DBOS, DBOSClient, DBOSConfig, Debouncer, DebouncerClient, SetWorkflowID
    from dbos._core import DEBOUNCER_WORKFLOW_NAME
    from dbos._sys_db import WorkflowStatusString
    from dbos._utils import INTERNAL_QUEUE_NAME
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "scheduler-debouncer-timing"
RUNG_000_ID = "rung-000-timing-smoke"
RUNG_001_ID = "rung-001-debouncer-delayed-row"
RUNG_002_ID = "rung-002-many-key-worker-pressure"
RUNG_003_ID = "rung-003-schedule-overlap-observation"
RUNG_004_ID = "rung-004-bounded-seed-sweep"
RUNG_005_ID = "rung-005-scheduled-queue-controls-compose"
RUNG_006_ID = "rung-006-async-debouncer-worker-starvation"
APP_ID = "wio-scheduler-debouncer-timing"
APP_VERSION = "wio-scheduler-debouncer-rungs-000-006"
PROMPT_PATH = "evidence-key:events/frontier_designer-20260620T072708734371000Z.prompt.md"

TERMINAL_STATUSES = {
    WorkflowStatusString.SUCCESS.value,
    WorkflowStatusString.ERROR.value,
    WorkflowStatusString.CANCELLED.value,
    WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value,
}


class WorkloadFailure(Exception):
    pass


class SetupBlock(Exception):
    pass


@dataclass(frozen=True)
class DebounceSubmission:
    key: str
    value: str
    debounce_period_sec: float
    offset_ms: int


@dataclass(frozen=True)
class CasePlan:
    rung_id: str
    case_id: str
    seed: int
    schedule: str
    focus: str
    database_prefix: str
    debounce_timeout_sec: float | None
    submissions: list[DebounceSubmission] = field(default_factory=list)
    expected_latest: dict[str, str] = field(default_factory=dict)
    expected_handle_groups: dict[str, list[str]] = field(default_factory=dict)
    max_wait_upper_bound_sec: float | None = None
    scheduler_triggers: int = 0
    scheduler_sleep_sec: float = 0.0
    pressure_thread_growth_cap: int = 96
    max_executor_threads: int = 64
    liveness_bound_sec: float = 2.0


_target_ledger: list[dict[str, Any]] = []
_target_ledger_lock = threading.Lock()
_scheduled_ledger: list[dict[str, Any]] = []
_scheduled_ledger_lock = threading.Lock()
_scheduled_queue_ledger: list[dict[str, Any]] = []
_scheduled_queue_ledger_lock = threading.Lock()
_scheduled_queue_release_events: dict[str, threading.Event] = {}


def now_ms() -> int:
    return int(time.time() * 1000)


def event(name: str, **fields: Any) -> None:
    parts = [f"WIO-EVENT {name}"]
    parts.extend(f"{key}={json.dumps(value, sort_keys=True, default=str)}" for key, value in fields.items())
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
                connection.execute(sa.text(f"DROP DATABASE IF EXISTS {quote_ident(database)} WITH (FORCE)"))
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
    if os.environ.get("WIO_SCHEDULER_KEEP_DATABASES") == "1":
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
                    sa.text(f"DROP DATABASE IF EXISTS {quote_ident(prefix + '_' + suffix)} WITH (FORCE)")
                )
    except Exception as exc:
        event("database_cleanup_best_effort_failed", prefix=prefix, error_type=type(exc).__name__, error=str(exc))
    finally:
        engine.dispose()


def make_config(plan: CasePlan, app_url: str, sys_url: str) -> DBOSConfig:
    return {
        "name": APP_ID,
        "application_database_url": app_url,
        "system_database_url": sys_url,
        "application_version": f"{APP_VERSION}-{plan.rung_id}-{plan.case_id}",
        "executor_id": f"wio-scheduler-{plan.case_id}",
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        "run_admin_server": False,
        "runtimeConfig": {"max_executor_threads": plan.max_executor_threads},
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
    if rung_id in {RUNG_001_ID, RUNG_002_ID, RUNG_003_ID}:
        return ["case-001", "case-002", "case-003"]
    if rung_id == RUNG_004_ID:
        return [f"case-{index:03d}" for index in range(1, 25)]
    if rung_id == RUNG_005_ID:
        return ["case-001", "case-002", "case-003"]
    if rung_id == RUNG_006_ID:
        return ["case-001", "case-002", "case-003"]
    raise SetupBlock(f"unsupported rung {rung_id}")


def make_prefix(rung_id: str, case_id: str, seed: int) -> str:
    suffix = uuid.uuid5(uuid.NAMESPACE_DNS, f"{rung_id}:{case_id}:{seed}").hex[:8]
    return f"wio_sched_{seed}_{case_id.replace('-', '_')}_{suffix}"


def make_plan(rung: str, case_id: str) -> CasePlan:
    rung_id = normalize_rung(rung)
    if case_id not in case_ids_for_rung(rung_id):
        raise SetupBlock(f"unknown case {case_id} for {rung_id}")

    def plan(
        seed: int,
        schedule: str,
        focus: str,
        submissions: list[DebounceSubmission],
        expected_latest: dict[str, str],
        *,
        debounce_timeout_sec: float | None = None,
        expected_handle_groups: dict[str, list[str]] | None = None,
        max_wait_upper_bound_sec: float | None = None,
        scheduler_triggers: int = 0,
        scheduler_sleep_sec: float = 0.0,
        max_executor_threads: int = 64,
        liveness_bound_sec: float = 2.0,
    ) -> CasePlan:
        return CasePlan(
            rung_id=rung_id,
            case_id=case_id,
            seed=seed,
            schedule=schedule,
            focus=focus,
            database_prefix=make_prefix(rung_id, case_id, seed),
            debounce_timeout_sec=debounce_timeout_sec,
            submissions=submissions,
            expected_latest=expected_latest,
            expected_handle_groups=expected_handle_groups or {},
            max_wait_upper_bound_sec=max_wait_upper_bound_sec,
            scheduler_triggers=scheduler_triggers,
            scheduler_sleep_sec=scheduler_sleep_sec,
            max_executor_threads=max_executor_threads,
            liveness_bound_sec=liveness_bound_sec,
        )

    if rung_id == RUNG_000_ID:
        seed = 3400
        return plan(
            seed,
            "run-one-sleep-one-scheduled-item-one-debounced-item",
            "setup smoke for durable sleep, trigger_schedule, and a single debounced workflow",
            [DebounceSubmission("smoke-key", f"smoke-{seed}", 0.15, 0)],
            {"smoke-key": f"smoke-{seed}"},
            scheduler_triggers=1,
            scheduler_sleep_sec=0.02,
        )

    if rung_id == RUNG_001_ID:
        if case_id == "case-001":
            seed = 3410
            key = f"latest-{seed}"
            return plan(
                seed,
                "submit-key-a-values-v1-v2-v3-inside-window",
                "latest input wins inside one debounce window",
                [
                    DebounceSubmission(key, "v1", 0.35, 0),
                    DebounceSubmission(key, "v2", 0.35, 80),
                    DebounceSubmission(key, "v3", 0.35, 80),
                ],
                {key: "v3"},
                expected_handle_groups={key: ["v1", "v2", "v3"]},
            )
        if case_id == "case-002":
            seed = 3411
            key = f"maxwait-{seed}"
            return plan(
                seed,
                "submit-repeated-values-until-max-wait-boundary",
                "max-wait fires despite continuing updates",
                [
                    DebounceSubmission(key, "mw-1", 240.0, 0),
                    DebounceSubmission(key, "mw-2", 240.0, 0),
                    DebounceSubmission(key, "mw-3", 240.0, 0),
                    DebounceSubmission(key, "mw-4", 240.0, 0),
                ],
                {key: "mw-4"},
                debounce_timeout_sec=90.0,
                expected_handle_groups={key: ["mw-1", "mw-2", "mw-3", "mw-4"]},
                max_wait_upper_bound_sec=300.0,
            )
        seed = 3412
        key = f"handles-{seed}"
        return plan(
            seed,
            "capture-each-returned-handle-then-await-after-window",
            "all returned handles for superseded calls map consistently to the surviving workflow",
            [
                DebounceSubmission(key, "h1", 0.25, 0),
                DebounceSubmission(key, "h2", 0.25, 40),
                DebounceSubmission(key, "h3", 0.25, 40),
                DebounceSubmission(key, "h4", 0.25, 40),
            ],
            {key: "h4"},
            expected_handle_groups={key: ["h1", "h2", "h3", "h4"]},
        )

    if rung_id == RUNG_002_ID:
        if case_id == "case-001":
            seed = 3420
            submissions = [
                DebounceSubmission(f"key-{index:02d}-{seed}", f"value-{index:02d}", 0.12, 0)
                for index in range(50)
            ]
            return plan(
                seed,
                "submit-bounded-50-key-matrix",
                "many keys finish without unbounded worker/thread pressure",
                submissions,
                {sub.key: sub.value for sub in submissions},
            )
        if case_id == "case-002":
            seed = 3421
            hot = f"hot-{seed}"
            cold_b = f"cold-b-{seed}"
            cold_c = f"cold-c-{seed}"
            submissions = [
                DebounceSubmission(hot, "hot-1", 0.20, 0),
                DebounceSubmission(cold_b, "cold-b", 0.20, 20),
                DebounceSubmission(hot, "hot-2", 0.20, 20),
                DebounceSubmission(cold_c, "cold-c", 0.20, 20),
                DebounceSubmission(hot, "hot-3", 0.20, 20),
                DebounceSubmission(hot, "hot-4", 0.20, 20),
            ]
            return plan(
                seed,
                "spam-key-a-while-keys-b-c-wait",
                "one hot key does not starve cold keys",
                submissions,
                {hot: "hot-4", cold_b: "cold-b", cold_c: "cold-c"},
                expected_handle_groups={hot: ["hot-1", "hot-2", "hot-3", "hot-4"]},
                max_wait_upper_bound_sec=180.0,
            )
        seed = 3422
        key = f"cleanup-{seed}"
        submissions = [
            DebounceSubmission(key, f"storm-{index}", 0.16, 20 if index else 0)
            for index in range(12)
        ]
        return plan(
            seed,
            "submit-replacement-storm-then-idle",
            "superseded delayed/internal rows are cleaned or made terminal after idle",
            submissions,
            {key: "storm-11"},
            expected_handle_groups={key: [f"storm-{index}" for index in range(12)]},
        )

    if rung_id == RUNG_003_ID:
        seed_map = {
            "case-001": (3430, "run-long-schedule-body-with-next-tick-due", 3, 0.30),
            "case-002": (3431, "pause-worker-then-resume-schedule", 2, 0.05),
            "case-003": (3432, "query-rows-during-long-schedule-body", 3, 0.20),
        }
        seed, schedule, triggers, sleep_sec = seed_map[case_id]
        return plan(
            seed,
            schedule,
            "observational scheduler artifact only; overlap/skip policy is not asserted as failure",
            [],
            {},
            scheduler_triggers=triggers,
            scheduler_sleep_sec=sleep_sec,
        )

    if rung_id == RUNG_005_ID:
        seed_map = {
            "case-001": (
                3464,
                "trigger-plus-explicit-backfill",
                "trigger and backfill route through declared queue controls",
            ),
            "case-002": (
                3465,
                "repeated-backfill-idempotency-under-blocked-queue",
                "replayed backfill slots keep deterministic workflow IDs and one terminal effect",
            ),
            "case-003": (
                3466,
                "live-tick-plus-trigger-backlog",
                "live scheduler tick and manual trigger both use the declared queue",
            ),
        }
        seed, schedule, focus = seed_map[case_id]
        return plan(seed, schedule, focus, [], {})

    if rung_id == RUNG_006_ID:
        if case_id == "case-001":
            seed = 3470
            submissions = [
                DebounceSubmission(f"long-{index:02d}-{seed}", f"long-value-{index:02d}", 8.0 + (index % 5), 0)
                for index in range(8)
            ]
            return plan(
                seed,
                "long-debounce-keys-plus-direct-workflows",
                "unrelated direct workflows finish while long internal debouncer rows are active",
                submissions,
                {sub.key: sub.value for sub in submissions},
                max_executor_threads=4,
                liveness_bound_sec=2.0,
            )
        if case_id == "case-002":
            seed = 3471
            hot = f"hot-{seed}"
            cold_a = f"cold-a-{seed}"
            cold_b = f"cold-b-{seed}"
            submissions = [
                DebounceSubmission(hot, "hot-0", 8.0, 0),
                DebounceSubmission(hot, "hot-1", 8.0, 40),
                DebounceSubmission(cold_a, "cold-a", 8.0, 40),
                DebounceSubmission(hot, "hot-2", 8.0, 40),
                DebounceSubmission(cold_b, "cold-b", 8.0, 40),
                DebounceSubmission(hot, "hot-3", 8.0, 40),
            ]
            return plan(
                seed,
                "hot-key-ack-plus-unrelated-queue",
                "hot-key updates and queued unrelated work stay live under scarce executor threads",
                submissions,
                {hot: "hot-3", cold_a: "cold-a", cold_b: "cold-b"},
                expected_handle_groups={hot: ["hot-0", "hot-1", "hot-2", "hot-3"]},
                max_executor_threads=4,
                liveness_bound_sec=2.5,
            )
        seed = 3472
        submissions = [
            DebounceSubmission(f"client-{index:02d}-{seed}", f"client-value-{index:02d}", 8.0 + (index % 3), 0)
            for index in range(6)
        ]
        return plan(
            seed,
            "client-debouncer-pressure",
            "client-created debouncers do not starve runtime workflows and clean up internal rows",
            submissions,
            {sub.key: sub.value for sub in submissions},
            max_executor_threads=4,
            liveness_bound_sec=2.0,
        )

    case_num = int(case_id.rsplit("-", 1)[-1])
    seed = 3439 + case_num
    variant = (case_num - 1) % 6
    if variant == 0:
        key = f"sweep-latest-{seed}"
        values = [f"sv-{seed}-{index}" for index in range(3)]
        submissions = [
            DebounceSubmission(key, values[0], 0.18, 0),
            DebounceSubmission(key, values[1], 0.18, random.Random(seed).randrange(15, 55)),
            DebounceSubmission(key, values[2], 0.18, random.Random(seed + 1).randrange(15, 55)),
        ]
        return plan(seed, "generate-bounded-latest-value-variant-from-seed", "sweep latest-value variant", submissions, {key: values[-1]}, expected_handle_groups={key: values})
    if variant == 1:
        key = f"sweep-maxwait-{seed}"
        values = [f"mw-{seed}-{index}" for index in range(4)]
        submissions = [DebounceSubmission(key, value, 240.0, 0) for value in values]
        return plan(seed, "generate-bounded-max-wait-variant-from-seed", "sweep max-wait variant", submissions, {key: values[-1]}, debounce_timeout_sec=90.0, expected_handle_groups={key: values}, max_wait_upper_bound_sec=300.0)
    if variant == 2:
        count = random.Random(seed).randrange(8, 18)
        submissions = [
            DebounceSubmission(f"sweep-key-{seed}-{index}", f"sweep-value-{index}", 0.10, 0)
            for index in range(count)
        ]
        return plan(seed, "generate-bounded-many-key-pressure-variant-from-seed", "sweep many-key-pressure variant", submissions, {sub.key: sub.value for sub in submissions})
    if variant == 3:
        hot = f"sweep-hot-{seed}"
        cold = f"sweep-cold-{seed}"
        submissions = [
            DebounceSubmission(hot, "hot-a", 0.16, 0),
            DebounceSubmission(cold, "cold", 0.16, 20),
            DebounceSubmission(hot, "hot-b", 0.16, 20),
            DebounceSubmission(hot, "hot-c", 0.16, 20),
        ]
        return plan(seed, "generate-bounded-hot-key-isolation-variant-from-seed", "sweep hot-key isolation variant", submissions, {hot: "hot-c", cold: "cold"}, expected_handle_groups={hot: ["hot-a", "hot-b", "hot-c"]})
    if variant == 4:
        key = f"sweep-cleanup-{seed}"
        count = random.Random(seed).randrange(5, 10)
        submissions = [
            DebounceSubmission(key, f"cleanup-{index}", 0.12, 15 if index else 0)
            for index in range(count)
        ]
        return plan(seed, "generate-bounded-delayed-row-cleanup-variant-from-seed", "sweep delayed-row cleanup variant", submissions, {key: f"cleanup-{count - 1}"}, expected_handle_groups={key: [f"cleanup-{index}" for index in range(count)]})
    return plan(
        seed,
        "generate-bounded-schedule-observation-variant-from-seed",
        "sweep scheduler observation variant",
        [],
        {},
        scheduler_triggers=random.Random(seed).randrange(1, 4),
        scheduler_sleep_sec=random.Random(seed + 17).choice([0.02, 0.08, 0.18]),
    )


@DBOS.workflow()
def debounced_target(key: str, value: str, sequence: int, seed: int) -> dict[str, Any]:
    started = now_ms()
    row = {
        "kind": "debounced_target",
        "key": key,
        "value": value,
        "sequence": sequence,
        "seed": seed,
        "workflow_id": DBOS.workflow_id,
        "started_at_ms": started,
    }
    with _target_ledger_lock:
        _target_ledger.append(row)
    DBOS.set_event(f"effect-{key}", row)
    return row


@DBOS.workflow()
def sleep_smoke_workflow(seconds: float) -> str:
    DBOS.sleep(seconds)
    return f"slept-{seconds}"


@DBOS.workflow()
def scheduled_observation_workflow(scheduled_at: datetime, ctx: Any) -> dict[str, Any]:
    started = now_ms()
    sleep_sec = float(ctx.get("sleep_sec", 0.0)) if isinstance(ctx, dict) else 0.0
    if sleep_sec:
        DBOS.sleep(sleep_sec)
    finished = now_ms()
    row = {
        "kind": "scheduled_observation",
        "workflow_id": DBOS.workflow_id,
        "scheduled_at": scheduled_at.isoformat(),
        "ctx": ctx,
        "started_at_ms": started,
        "finished_at_ms": finished,
        "duration_ms": finished - started,
    }
    with _scheduled_ledger_lock:
        _scheduled_ledger.append(row)
    return row


@DBOS.workflow()
def scheduled_queue_control_workflow(scheduled_at: datetime, ctx: Any) -> dict[str, Any]:
    started_monotonic = time.monotonic()
    started_at_ms = now_ms()
    status = DBOS.get_workflow_status(DBOS.workflow_id)
    gate_key = str(ctx.get("gate_key")) if isinstance(ctx, dict) else ""
    release_event = _scheduled_queue_release_events.get(gate_key)
    row = {
        "kind": "scheduled_queue_control",
        "workflow_id": DBOS.workflow_id,
        "queue_name": status.queue_name if status else None,
        "status_at_start": status.status if status else None,
        "scheduled_at": scheduled_at.isoformat(),
        "slot_id": scheduled_at.isoformat(),
        "ctx": ctx,
        "started_at_ms": started_at_ms,
        "started_monotonic": started_monotonic,
        "finished_at_ms": None,
        "released": None,
    }
    with _scheduled_queue_ledger_lock:
        _scheduled_queue_ledger.append(row)
    if isinstance(ctx, dict) and ctx.get("block_on_gate", False):
        if release_event is None:
            raise RuntimeError(f"missing release gate for {gate_key}")
        row["released"] = release_event.wait(timeout=float(ctx.get("gate_timeout_sec", 12.0)))
        if not row["released"]:
            raise TimeoutError(f"scheduled queue gate {gate_key} was not released")
    row["finished_at_ms"] = now_ms()
    return dict(row)


def workflow_rows(
    dbos: DBOS,
    *,
    workflow_ids: list[str] | None = None,
    names: list[str] | None = None,
    workflow_id_prefix: str | None = None,
    queue_name: str | None = None,
) -> list[dict[str, Any]]:
    table = dbos._sys_db.engine.dialect.identifier_preparer.quote_schema("dbos")
    workflow_status = f"{table}.workflow_status"
    clauses: list[str] = []
    params: dict[str, Any] = {}
    if workflow_ids:
        clauses.append("workflow_uuid = ANY(:workflow_ids)")
        params["workflow_ids"] = workflow_ids
    if names:
        clauses.append("name = ANY(:names)")
        params["names"] = names
    if workflow_id_prefix:
        clauses.append("workflow_uuid LIKE :workflow_id_prefix")
        params["workflow_id_prefix"] = f"{workflow_id_prefix}%"
    if queue_name:
        clauses.append("queue_name = :queue_name")
        params["queue_name"] = queue_name
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = sa.text(
        f"""
        SELECT workflow_uuid, status, name, queue_name, deduplication_id,
               created_at, updated_at, started_at_epoch_ms, delay_until_epoch_ms,
               workflow_timeout_ms, workflow_deadline_epoch_ms, rate_limited
        FROM {workflow_status}
        {where}
        ORDER BY created_at, workflow_uuid
        """
    )
    with dbos._sys_db.engine.begin() as conn:
        rows = conn.execute(query, params).mappings().all()
    return [dict(row) for row in rows]


def active_internal_rows(dbos: DBOS) -> list[dict[str, Any]]:
    rows = workflow_rows(dbos, names=[DEBOUNCER_WORKFLOW_NAME])
    return [
        row
        for row in rows
        if row["status"] not in TERMINAL_STATUSES or row.get("deduplication_id")
    ]


def active_debouncer_wait_rows(dbos: DBOS) -> list[dict[str, Any]]:
    return [
        row
        for row in workflow_rows(dbos, names=[DEBOUNCER_WORKFLOW_NAME])
        if row["status"] not in TERMINAL_STATUSES
    ]


def active_queue_rows(dbos: DBOS, queue_name: str) -> list[dict[str, Any]]:
    return [
        row
        for row in workflow_rows(dbos, queue_name=queue_name)
        if row["status"] not in TERMINAL_STATUSES
    ]


def wait_for_status_rows(dbos: DBOS, names: list[str], min_count: int, timeout_sec: float) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout_sec
    last_rows: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        last_rows = workflow_rows(dbos, names=names)
        if len(last_rows) >= min_count:
            return last_rows
        time.sleep(0.05)
    return last_rows


def wait_for_active_debouncer_rows(dbos: DBOS, min_count: int, timeout_sec: float) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout_sec
    last_rows: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        last_rows = active_debouncer_wait_rows(dbos)
        if len(last_rows) >= min_count:
            return last_rows
        time.sleep(0.05)
    return last_rows


def pressure_min_active_rows(plan: CasePlan) -> int:
    if plan.rung_id == RUNG_006_ID and plan.case_id == "case-002":
        return 1
    return min(len(plan.expected_latest), len(plan.submissions))


@DBOS.workflow()
def unrelated_marker_workflow(label: str, seed: int, sleep_sec: float = 0.0) -> dict[str, Any]:
    started = time.monotonic()
    if sleep_sec:
        DBOS.sleep(sleep_sec)
    return {
        "kind": "unrelated_marker",
        "label": label,
        "seed": seed,
        "workflow_id": DBOS.workflow_id,
        "duration_sec": time.monotonic() - started,
    }


def assert_handle_results_within_bound(
    handles: list[Any],
    *,
    bound_sec: float,
    context: dict[str, Any],
) -> list[dict[str, Any]]:
    started = time.monotonic()
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    for handle in handles:
        remaining = max(0.01, bound_sec - (time.monotonic() - started))
        try:
            results.append(get_handle_result(handle, timeout_seconds=remaining))
        except Exception as exc:
            errors.append(f"{handle.workflow_id}:{type(exc).__name__}:{exc}")
    elapsed = time.monotonic() - started
    invariant(
        "unrelated_workflows_complete_inside_pressure_window",
        not errors and elapsed <= bound_sec,
        elapsed_sec=elapsed,
        bound_sec=bound_sec,
        workflow_ids=[handle.workflow_id for handle in handles],
        results=results,
        errors=errors,
        **context,
    )
    return results


def assert_debounce_results_match_model(
    dbos: DBOS,
    plan: CasePlan,
    handles_by_key: dict[str, list[dict[str, Any]]],
    handle_objects: dict[int, Any],
    *,
    result_timeout_sec: float,
) -> dict[str, Any]:
    results: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for key, expected in plan.expected_latest.items():
        handle_record = handles_by_key[key][-1]
        try:
            result = get_handle_result(handle_objects[handle_record["sequence"]], timeout_seconds=result_timeout_sec)
            results[key] = result
        except Exception as exc:
            errors[key] = f"{type(exc).__name__}: {exc}"
            result = None
        invariant(
            "async_pressure_latest_value_matches_model",
            result is not None and result["value"] == expected,
            key=key,
            expected=expected,
            result=result,
            error=errors.get(key),
        )

    for key, grouped_values in plan.expected_handle_groups.items():
        observed = [item for item in handles_by_key.get(key, []) if item["value"] in grouped_values]
        workflow_ids = {item["workflow_id"] for item in observed}
        invariant(
            "async_pressure_returned_handles_conserve_surviving_workflow",
            len(workflow_ids) == 1,
            key=key,
            grouped_values=grouped_values,
            observed_handles=observed,
            workflow_ids=sorted(workflow_ids),
        )
        for item in observed:
            try:
                result = get_handle_result(handle_objects[item["sequence"]], timeout_seconds=result_timeout_sec)
                error = None
            except Exception as exc:
                result = None
                error = f"{type(exc).__name__}: {exc}"
            invariant(
                "async_pressure_superseded_handle_returns_winning_value",
                result is not None and result["value"] == plan.expected_latest[key],
                key=key,
                submitted_value=item["value"],
                result=result,
                error=error,
                expected=plan.expected_latest[key],
            )

    ledger_by_key: dict[str, list[dict[str, Any]]] = {}
    with _target_ledger_lock:
        for row in _target_ledger:
            ledger_by_key.setdefault(row["key"], []).append(row)
    for key, expected in plan.expected_latest.items():
        observed_values = [row["value"] for row in ledger_by_key.get(key, [])]
        invariant(
            "async_pressure_target_effect_count_and_value_match_model",
            observed_values == [expected],
            key=key,
            observed_values=observed_values,
            expected=[expected],
        )

    remaining_internal = wait_for_active_debouncer_rows(dbos, 1, 0.5)
    invariant(
        "async_pressure_no_active_debouncer_rows_after_idle",
        not remaining_internal,
        remaining_internal_rows=remaining_internal,
    )
    return {
        "results": results,
        "target_ledger": list(_target_ledger),
        "remaining_internal_rows": remaining_internal,
    }


def submit_pressure_debounces(
    plan: CasePlan,
    *,
    use_client: bool,
    app_url: str | None = None,
    sys_url: str | None = None,
    client_queue_name: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]], dict[int, Any], DBOSClient | None]:
    client: DBOSClient | None = None
    if use_client:
        if app_url is None or sys_url is None or client_queue_name is None:
            raise SetupBlock("client debouncer setup missing database URL or queue")
        client = DBOSClient(application_database_url=app_url, system_database_url=sys_url)
        debouncer = DebouncerClient(
            client,
            {"workflow_name": debounced_target.__qualname__, "queue_name": client_queue_name},
        )
    else:
        debouncer = Debouncer.create(debounced_target)

    handles: list[dict[str, Any]] = []
    handles_by_key: dict[str, list[dict[str, Any]]] = {}
    handle_objects: dict[int, Any] = {}
    for sequence, submission in enumerate(plan.submissions):
        if submission.offset_ms:
            time.sleep(submission.offset_ms / 1000)
        handle = debouncer.debounce(
            submission.key,
            submission.debounce_period_sec,
            submission.key,
            submission.value,
            sequence,
            plan.seed,
        )
        record = {
            "sequence": sequence,
            "key": submission.key,
            "value": submission.value,
            "workflow_id": handle.workflow_id,
            "debounce_period_sec": submission.debounce_period_sec,
            "submitted_at_ms": now_ms(),
        }
        handles.append(record)
        handles_by_key.setdefault(submission.key, []).append(record)
        handle_objects[sequence] = handle
        event("async_pressure_debounce_submitted", case_id=plan.case_id, sequence=sequence, key=submission.key, value=submission.value, workflow_id=handle.workflow_id)
    return handles, handles_by_key, handle_objects, client


def run_async_debouncer_pressure(dbos: DBOS, plan: CasePlan, app_url: str, sys_url: str) -> dict[str, Any]:
    use_client = plan.case_id == "case-003"
    client_queue_name = f"wio_client_debounce_target_{plan.seed}" if use_client else None
    if client_queue_name:
        DBOS.register_queue(client_queue_name, worker_concurrency=2, polling_interval_sec=0.05, on_conflict="always_update")

    thread_count_before_pressure = threading.active_count()
    client: DBOSClient | None = None
    try:
        handles, handles_by_key, handle_objects, client = submit_pressure_debounces(
            plan,
            use_client=use_client,
            app_url=app_url,
            sys_url=sys_url,
            client_queue_name=client_queue_name,
        )
        expected_active_rows = pressure_min_active_rows(plan)
        active_rows = wait_for_active_debouncer_rows(dbos, expected_active_rows, 3.0)
        invariant(
            "async_pressure_active_debouncer_rows_observed",
            len(active_rows) >= expected_active_rows,
            active_rows=active_rows,
            expected_min=expected_active_rows,
        )

        if plan.case_id == "case-002":
            queue_name = f"wio_debounce_unrelated_queue_{plan.seed}"
            DBOS.register_queue(queue_name, worker_concurrency=1, polling_interval_sec=0.05, on_conflict="always_update")
            unrelated_handles = [
                DBOS.enqueue_workflow(queue_name, unrelated_marker_workflow, f"queued-{index}", plan.seed)
                for index in range(3)
            ]
            unrelated_kind = "queued"
        else:
            unrelated_handles = [
                DBOS.start_workflow(unrelated_marker_workflow, f"direct-{index}", plan.seed)
                for index in range(4)
            ]
            unrelated_kind = "direct"

        active_rows_during_liveness = active_debouncer_wait_rows(dbos)
        invariant(
            "async_pressure_rows_still_active_before_unrelated_wait",
            bool(active_rows_during_liveness),
            active_rows=active_rows_during_liveness,
            unrelated_kind=unrelated_kind,
        )
        unrelated_results = assert_handle_results_within_bound(
            unrelated_handles,
            bound_sec=plan.liveness_bound_sec,
            context={
                "case_id": plan.case_id,
                "unrelated_kind": unrelated_kind,
                "active_rows_before_wait": active_rows_during_liveness,
            },
        )
        active_rows_after_liveness = active_debouncer_wait_rows(dbos)
        invariant(
            "async_pressure_rows_remain_active_after_unrelated_completion",
            bool(active_rows_after_liveness),
            active_rows=active_rows_after_liveness,
            unrelated_results=unrelated_results,
        )

        thread_count_after_liveness = threading.active_count()
        thread_growth = thread_count_after_liveness - thread_count_before_pressure
        invariant(
            "async_pressure_thread_growth_bounded",
            thread_growth <= plan.max_executor_threads + 12,
            thread_count_before_pressure=thread_count_before_pressure,
            thread_count_after_liveness=thread_count_after_liveness,
            thread_growth=thread_growth,
            max_executor_threads=plan.max_executor_threads,
            cap=plan.max_executor_threads + 12,
        )
        debounce_result = assert_debounce_results_match_model(
            dbos,
            plan,
            handles_by_key,
            handle_objects,
            result_timeout_sec=max(sub.debounce_period_sec for sub in plan.submissions) + 8.0,
        )
        return {
            "submitted_debounces": handles,
            "handles_by_key": handles_by_key,
            "active_rows_initial": active_rows,
            "active_rows_during_liveness": active_rows_during_liveness,
            "active_rows_after_liveness": active_rows_after_liveness,
            "unrelated_kind": unrelated_kind,
            "unrelated_workflow_ids": [handle.workflow_id for handle in unrelated_handles],
            "unrelated_results": unrelated_results,
            "thread_count_before_pressure": thread_count_before_pressure,
            "thread_count_after_liveness": thread_count_after_liveness,
            "thread_growth": thread_growth,
            "client_queue_name": client_queue_name,
            "debounce_result": debounce_result,
        }
    finally:
        if client is not None:
            client.destroy()


def wait_for_scheduler_queue_starts(gate_key: str, min_count: int, timeout_sec: float) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout_sec
    last_rows: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        with _scheduled_queue_ledger_lock:
            last_rows = [dict(row) for row in _scheduled_queue_ledger if row["ctx"].get("gate_key") == gate_key]
        if len(last_rows) >= min_count:
            return last_rows
        time.sleep(0.05)
    return last_rows


def wait_for_no_active_queue_rows(dbos: DBOS, queue_name: str, timeout_sec: float) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout_sec
    last_rows: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        last_rows = active_queue_rows(dbos, queue_name)
        if not last_rows:
            return []
        time.sleep(0.05)
    return last_rows


def submit_debounce_sequence(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    debouncer = Debouncer.create(debounced_target, debounce_timeout_sec=plan.debounce_timeout_sec)
    handles: list[dict[str, Any]] = []
    handle_objects: dict[int, Any] = {}
    snapshots: list[dict[str, Any]] = []
    started_at = time.monotonic()
    for sequence, submission in enumerate(plan.submissions):
        if submission.offset_ms:
            time.sleep(submission.offset_ms / 1000)
        submitted_at_ms = now_ms()
        handle = debouncer.debounce(
            submission.key,
            submission.debounce_period_sec,
            submission.key,
            submission.value,
            sequence,
            plan.seed,
        )
        handle_objects[sequence] = handle
        handles.append(
            {
                "sequence": sequence,
                "key": submission.key,
                "value": submission.value,
                "workflow_id": handle.workflow_id,
                "submitted_at_ms": submitted_at_ms,
                "debounce_period_sec": submission.debounce_period_sec,
            }
        )
        snapshots.append(
            {
                "after_sequence": sequence,
                "active_internal_rows": active_internal_rows(dbos),
                "handle_workflow_id": handle.workflow_id,
            }
        )
        event("debounce_submitted", case_id=plan.case_id, sequence=sequence, key=submission.key, value=submission.value, workflow_id=handle.workflow_id)

    handles_by_key: dict[str, list[dict[str, Any]]] = {}
    for handle in handles:
        handles_by_key.setdefault(handle["key"], []).append(handle)

    results: dict[str, Any] = {}
    result_errors: dict[str, str] = {}
    for key, expected in plan.expected_latest.items():
        handle_record = handles_by_key[key][-1]
        handle = handle_objects[handle_record["sequence"]]
        try:
            results[key] = handle.get_result(timeout_seconds=10)
        except TypeError:
            results[key] = handle.get_result()
        except Exception as exc:
            result_errors[key] = f"{type(exc).__name__}: {exc}"

        invariant(
            "latest_value_matches_model",
            key in results and results[key]["value"] == expected,
            key=key,
            result=results.get(key),
            expected=expected,
            error=result_errors.get(key),
        )

    elapsed_sec = time.monotonic() - started_at
    if plan.max_wait_upper_bound_sec is not None:
        invariant(
            "max_wait_bounded_liveness",
            elapsed_sec <= plan.max_wait_upper_bound_sec,
            elapsed_sec=elapsed_sec,
            upper_bound_sec=plan.max_wait_upper_bound_sec,
        )

    for key, grouped_values in plan.expected_handle_groups.items():
        observed = [item for item in handles_by_key.get(key, []) if item["value"] in grouped_values]
        workflow_ids = {item["workflow_id"] for item in observed}
        invariant(
            "returned_handles_conserve_surviving_workflow",
            len(workflow_ids) == 1,
            key=key,
            grouped_values=grouped_values,
            observed_handles=observed,
            workflow_ids=sorted(workflow_ids),
        )
        for item in observed:
            result = handle_objects[item["sequence"]].get_result()
            invariant(
                "superseded_handle_returns_winning_value",
                result["value"] == plan.expected_latest[key],
                key=key,
                submitted_value=item["value"],
                result=result,
                expected=plan.expected_latest[key],
            )

    target_rows = workflow_rows(dbos, workflow_ids=sorted({item["workflow_id"] for item in handles}))
    terminal_target_ids = {
        row["workflow_uuid"]
        for row in target_rows
        if row["name"] == debounced_target.__qualname__ and row["status"] in TERMINAL_STATUSES
    }
    invariant(
        "one_terminal_target_per_modeled_key",
        len(terminal_target_ids) == len(plan.expected_latest),
        expected_keys=sorted(plan.expected_latest),
        terminal_target_ids=sorted(terminal_target_ids),
        target_rows=target_rows,
    )

    time.sleep(0.15)
    remaining_internal = active_internal_rows(dbos)
    invariant(
        "no_active_debouncer_rows_after_terminal_idle",
        not remaining_internal,
        remaining_internal_rows=remaining_internal,
    )

    ledger_by_key: dict[str, list[dict[str, Any]]] = {}
    with _target_ledger_lock:
        for row in _target_ledger:
            ledger_by_key.setdefault(row["key"], []).append(row)
    for key, expected in plan.expected_latest.items():
        observed_values = [row["value"] for row in ledger_by_key.get(key, [])]
        invariant(
            "target_effect_count_and_value_match_model",
            observed_values == [expected],
            key=key,
            observed_values=observed_values,
            expected=[expected],
        )

    return {
        "handles": handles,
        "handles_by_key": handles_by_key,
        "snapshots": snapshots,
        "results": results,
        "result_errors": result_errors,
        "target_rows": target_rows,
        "target_ledger": list(_target_ledger),
        "remaining_internal_rows": remaining_internal,
        "elapsed_sec": elapsed_sec,
    }


def run_sleep_smoke() -> dict[str, Any]:
    handle = DBOS.start_workflow(sleep_smoke_workflow, 0.03)
    result = handle.get_result()
    invariant("durable_sleep_smoke_completed", result == "slept-0.03", result=result)
    return {"sleep_workflow_id": handle.workflow_id, "result": result}


def run_scheduler_observation(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    if plan.scheduler_triggers <= 0:
        return {"scheduler_triggers": 0, "scheduled_results": []}
    schedule_name = f"wio-sched-{plan.case_id}-{plan.seed}"
    DBOS.create_schedule(
        schedule_name=schedule_name,
        workflow_fn=scheduled_observation_workflow,
        schedule="0 0 1 1 *",
        context={"case_id": plan.case_id, "seed": plan.seed, "sleep_sec": plan.scheduler_sleep_sec},
    )
    schedule_before = DBOS.get_schedule(schedule_name)
    handles = [DBOS.trigger_schedule(schedule_name) for _ in range(plan.scheduler_triggers)]
    rows_after_enqueue = wait_for_status_rows(dbos, [scheduled_observation_workflow.__qualname__], plan.scheduler_triggers, 2.0)
    results = [handle.get_result() for handle in handles]
    rows_after_terminal = workflow_rows(dbos, workflow_ids=[handle.workflow_id for handle in handles])
    DBOS.delete_schedule(schedule_name)
    schedule_after_delete = DBOS.get_schedule(schedule_name)
    invariant(
        "scheduler_triggered_observations_terminal",
        len(results) == plan.scheduler_triggers and all("workflow_id" in result for result in results),
        trigger_count=plan.scheduler_triggers,
        results=results,
    )
    invariant(
        "schedule_delete_removes_public_schedule",
        schedule_after_delete is None,
        schedule_before=schedule_before,
        schedule_after_delete=schedule_after_delete,
    )
    return {
        "schedule_name": schedule_name,
        "schedule_before": schedule_before,
        "trigger_workflow_ids": [handle.workflow_id for handle in handles],
        "rows_after_enqueue": rows_after_enqueue,
        "rows_after_terminal": rows_after_terminal,
        "scheduled_results": results,
        "scheduled_ledger": list(_scheduled_ledger),
    }


def get_handle_result(handle: Any, timeout_seconds: float = 15.0) -> Any:
    try:
        return handle.get_result(timeout_seconds=timeout_seconds)
    except TypeError:
        return handle.get_result()


def scheduler_queue_names(plan: CasePlan) -> tuple[str, str, str]:
    case_fragment = plan.case_id.replace("-", "_")
    queue_name = f"wio_sched_queue_{case_fragment}_{plan.seed}"
    schedule_name = f"wio-sched-queue-{plan.case_id}-{plan.seed}"
    gate_key = f"{plan.rung_id}:{plan.case_id}:{plan.seed}"
    return queue_name, schedule_name, gate_key


def backfill_window_for_case(plan: CasePlan) -> tuple[datetime, datetime] | None:
    if plan.case_id == "case-001":
        return (
            datetime(2025, 1, 1, 0, 30, 0, tzinfo=timezone.utc),
            datetime(2025, 1, 1, 3, 30, 0, tzinfo=timezone.utc),
        )
    if plan.case_id == "case-002":
        return (
            datetime(2025, 2, 1, 0, 30, 0, tzinfo=timezone.utc),
            datetime(2025, 2, 1, 3, 30, 0, tzinfo=timezone.utc),
        )
    return None


def assert_scheduler_queue_rows(
    rows: list[dict[str, Any]],
    *,
    schedule_name: str,
    queue_name: str,
) -> None:
    invariant(
        "scheduled_rows_observed_for_modeled_schedule",
        bool(rows),
        schedule_name=schedule_name,
        rows=rows,
    )
    wrong_queue = [row for row in rows if row["queue_name"] != queue_name]
    invariant(
        "scheduled_rows_use_declared_queue",
        not wrong_queue,
        schedule_name=schedule_name,
        expected_queue=queue_name,
        wrong_queue_rows=wrong_queue,
        rows=rows,
    )
    internal_rows = [row for row in rows if row["queue_name"] == INTERNAL_QUEUE_NAME]
    invariant(
        "scheduled_rows_do_not_use_internal_queue",
        not internal_rows,
        schedule_name=schedule_name,
        internal_queue=INTERNAL_QUEUE_NAME,
        internal_rows=internal_rows,
    )


def assert_scheduler_queue_limiter(start_rows: list[dict[str, Any]]) -> None:
    starts = sorted(float(row["started_monotonic"]) for row in start_rows)
    windows = [
        {"start": start, "count": sum(1 for other in starts if start <= other < start + 1.0)}
        for start in starts
    ]
    invariant(
        "scheduler_queue_limiter_two_per_second",
        all(window["count"] <= 2 for window in windows),
        starts=starts,
        windows=windows,
    )


def run_scheduler_queue_controls(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    queue_name, schedule_name, gate_key = scheduler_queue_names(plan)
    release_event = threading.Event()
    _scheduled_queue_release_events[gate_key] = release_event
    queue_config = {
        "concurrency": 1,
        "worker_concurrency": 1,
        "limiter": {"limit": 2, "period": 1.0},
        "polling_interval_sec": 0.05,
    }
    queue = DBOS.register_queue(queue_name, **queue_config, on_conflict="always_update")
    retrieved_queue = DBOS.retrieve_queue(queue_name)
    invariant(
        "scheduler_queue_registered_with_modeled_controls",
        retrieved_queue is not None
        and retrieved_queue.name == queue_name
        and retrieved_queue.concurrency == queue_config["concurrency"]
        and retrieved_queue.worker_concurrency == queue_config["worker_concurrency"]
        and retrieved_queue.limiter == queue_config["limiter"],
        queue_name=queue_name,
        registered_queue=queue.__dict__,
        retrieved_queue=retrieved_queue.__dict__ if retrieved_queue is not None else None,
        queue_config=queue_config,
    )

    context = {
        "case_id": plan.case_id,
        "seed": plan.seed,
        "gate_key": gate_key,
        "queue_name": queue_name,
        "block_on_gate": True,
        "gate_timeout_sec": 12.0,
    }
    cron = "* * * * * *" if plan.case_id == "case-003" else "0 * * * *"
    DBOS.create_schedule(
        schedule_name=schedule_name,
        workflow_fn=scheduled_queue_control_workflow,
        schedule=cron,
        context=context,
        queue_name=queue_name,
    )
    schedule_before = DBOS.get_schedule(schedule_name)
    invariant(
        "schedule_records_declared_queue",
        schedule_before is not None and schedule_before.get("queue_name") == queue_name,
        schedule=schedule_before,
        queue_name=queue_name,
    )

    handles: dict[str, Any] = {}
    expected_workflow_ids: list[str] = []
    backfill_ids: list[str] = []
    repeated_backfill_ids: list[str] = []
    live_workflow_ids: list[str] = []
    rows_after_enqueue: list[dict[str, Any]] = []

    try:
        if plan.case_id == "case-003":
            prefix = f"sched-{schedule_name}-"
            deadline = time.monotonic() + 7.0
            while time.monotonic() < deadline:
                live_rows = [
                    row
                    for row in workflow_rows(dbos, workflow_id_prefix=prefix)
                    if "-trigger-" not in row["workflow_uuid"]
                ]
                if live_rows:
                    live_workflow_ids = [live_rows[0]["workflow_uuid"]]
                    break
                time.sleep(0.10)
            if not live_workflow_ids:
                raise SetupBlock("live scheduler tick was not observed within bounded window")
            handles[live_workflow_ids[0]] = DBOS.retrieve_workflow(live_workflow_ids[0])
            trigger_handle = DBOS.trigger_schedule(schedule_name)
            DBOS.delete_schedule(schedule_name)
            handles[trigger_handle.workflow_id] = trigger_handle
            expected_workflow_ids = live_workflow_ids + [trigger_handle.workflow_id]
        else:
            if plan.case_id == "case-001":
                trigger_handle = DBOS.trigger_schedule(schedule_name)
                handles[trigger_handle.workflow_id] = trigger_handle
                expected_workflow_ids.append(trigger_handle.workflow_id)
            window = backfill_window_for_case(plan)
            if window is None:
                raise SetupBlock(f"missing backfill window for {plan.case_id}")
            start, end = window
            backfill_handles = DBOS.backfill_schedule(schedule_name, start, end)
            backfill_ids = [handle.workflow_id for handle in backfill_handles]
            for handle in backfill_handles:
                handles[handle.workflow_id] = handle
            expected_workflow_ids.extend(backfill_ids)
            if plan.case_id == "case-002":
                repeated_handles = DBOS.backfill_schedule(schedule_name, start, end)
                repeated_backfill_ids = [handle.workflow_id for handle in repeated_handles]
                for handle in repeated_handles:
                    handles.setdefault(handle.workflow_id, handle)
                invariant(
                    "repeated_backfill_returns_same_workflow_ids",
                    repeated_backfill_ids == backfill_ids,
                    first_backfill_ids=backfill_ids,
                    repeated_backfill_ids=repeated_backfill_ids,
                )

        rows_after_enqueue = workflow_rows(dbos, workflow_ids=expected_workflow_ids)
        assert_scheduler_queue_rows(rows_after_enqueue, schedule_name=schedule_name, queue_name=queue_name)

        starts_before_release = wait_for_scheduler_queue_starts(gate_key, 1, 6.0)
        if not starts_before_release:
            raise SetupBlock("scheduled queue worker did not start the first modeled workflow")
        time.sleep(0.35)
        starts_before_release = wait_for_scheduler_queue_starts(gate_key, 1, 0.1)
        invariant(
            "scheduled_queue_concurrency_blocks_second_start_before_release",
            len(starts_before_release) == 1,
            starts_before_release=starts_before_release,
            queue_config=queue_config,
            rows_after_enqueue=rows_after_enqueue,
        )

        release_event.set()
        modeled_results = {
            workflow_id: get_handle_result(handle, timeout_seconds=20.0)
            for workflow_id, handle in handles.items()
        }
        prefix = f"sched-{schedule_name}-"
        rows_after_modeled_results = workflow_rows(dbos, workflow_id_prefix=prefix)
        additional_workflow_ids = [
            row["workflow_uuid"]
            for row in rows_after_modeled_results
            if row["workflow_uuid"] not in expected_workflow_ids
        ]
        additional_results: dict[str, Any] = {}
        for workflow_id in additional_workflow_ids:
            additional_results[workflow_id] = get_handle_result(
                DBOS.retrieve_workflow(workflow_id),
                timeout_seconds=20.0,
            )
        observed_workflow_ids = expected_workflow_ids + additional_workflow_ids
        terminal_rows = workflow_rows(dbos, workflow_ids=observed_workflow_ids)
        assert_scheduler_queue_rows(terminal_rows, schedule_name=schedule_name, queue_name=queue_name)
        non_success_rows = [row for row in terminal_rows if row["status"] != WorkflowStatusString.SUCCESS.value]
        invariant(
            "all_observed_scheduled_queue_rows_reach_success",
            not non_success_rows and len(terminal_rows) == len(set(observed_workflow_ids)),
            terminal_rows=terminal_rows,
            expected_workflow_ids=expected_workflow_ids,
            additional_workflow_ids=additional_workflow_ids,
            modeled_results=modeled_results,
            additional_results=additional_results,
        )

        with _scheduled_queue_ledger_lock:
            start_rows = [
                dict(row)
                for row in _scheduled_queue_ledger
                if row["ctx"].get("gate_key") == gate_key
            ]
        starts_by_workflow: dict[str, list[dict[str, Any]]] = {}
        for row in start_rows:
            starts_by_workflow.setdefault(row["workflow_id"], []).append(row)
        duplicate_effects = {
            workflow_id: rows
            for workflow_id, rows in starts_by_workflow.items()
            if workflow_id in observed_workflow_ids and len(rows) != 1
        }
        missing_effects = [workflow_id for workflow_id in observed_workflow_ids if workflow_id not in starts_by_workflow]
        invariant(
            "one_terminal_effect_per_observed_slot",
            not duplicate_effects and not missing_effects,
            expected_workflow_ids=expected_workflow_ids,
            additional_workflow_ids=additional_workflow_ids,
            starts_by_workflow=starts_by_workflow,
            duplicate_effects=duplicate_effects,
            missing_effects=missing_effects,
        )
        result_workflow_ids = sorted(result["workflow_id"] for result in modeled_results.values())
        invariant(
            "handle_results_match_modeled_workflow_ids",
            result_workflow_ids == sorted(expected_workflow_ids),
            result_workflow_ids=result_workflow_ids,
            expected_workflow_ids=sorted(expected_workflow_ids),
            results=modeled_results,
        )
        additional_result_workflow_ids = sorted(result["workflow_id"] for result in additional_results.values())
        invariant(
            "additional_live_rows_are_explicitly_accounted",
            additional_result_workflow_ids == sorted(additional_workflow_ids),
            additional_workflow_ids=sorted(additional_workflow_ids),
            additional_result_workflow_ids=additional_result_workflow_ids,
            additional_results=additional_results,
        )
        assert_scheduler_queue_limiter(start_rows)
        remaining_active = wait_for_no_active_queue_rows(dbos, queue_name, 6.0)
        invariant(
            "scheduled_queue_active_rows_cleaned_after_terminal",
            not remaining_active,
            queue_name=queue_name,
            remaining_active_rows=remaining_active,
        )
        return {
            "queue_name": queue_name,
            "schedule_name": schedule_name,
            "queue_config": queue_config,
            "schedule_before": schedule_before,
            "trigger_workflow_ids": [
                workflow_id for workflow_id in expected_workflow_ids if "-trigger-" in workflow_id
            ],
            "backfill_workflow_ids": backfill_ids,
            "repeated_backfill_workflow_ids": repeated_backfill_ids,
            "live_workflow_ids": live_workflow_ids,
            "expected_workflow_ids": expected_workflow_ids,
            "additional_workflow_ids": additional_workflow_ids,
            "observed_workflow_ids": observed_workflow_ids,
            "rows_after_enqueue": rows_after_enqueue,
            "rows_after_modeled_results": rows_after_modeled_results,
            "terminal_rows": terminal_rows,
            "modeled_results": modeled_results,
            "additional_results": additional_results,
            "scheduled_queue_ledger": start_rows,
            "remaining_active_queue_rows": remaining_active,
        }
    finally:
        release_event.set()
        _scheduled_queue_release_events.pop(gate_key, None)
        try:
            DBOS.delete_schedule(schedule_name)
        except Exception as exc:
            event("schedule_cleanup_best_effort_failed", schedule_name=schedule_name, error_type=type(exc).__name__, error=str(exc))


def run_case(plan: CasePlan, artifact_dir: Path) -> dict[str, Any]:
    global _target_ledger, _scheduled_ledger, _scheduled_queue_ledger
    _target_ledger = []
    _scheduled_ledger = []
    _scheduled_queue_ledger = []
    artifact_dir.mkdir(parents=True, exist_ok=True)
    write_json(artifact_dir / "case-plan.json", asdict(plan))
    if plan.rung_id == RUNG_005_ID:
        protected_product_promise = (
            "Scheduled work created with queue_name routes to the declared DB-backed queue "
            "and obeys queue concurrency, limiter, result, idempotency, and cleanup controls."
        )
        invariant_oracle = (
            "declared queue rows, no internal queue fallback, blocked-body concurrency gate, "
            "idempotent backfill workflow IDs, handle results, limiter windows, and active-row cleanup"
        )
    elif plan.rung_id == RUNG_006_ID:
        protected_product_promise = (
            "Active debounce windows preserve trailing-edge latest-input semantics without occupying "
            "scarce executor worker threads or starving unrelated workflows."
        )
        invariant_oracle = (
            "active internal debouncer rows during unrelated direct/queued work, bounded handle result "
            "timing, thread-count delta, returned debounced handles, latest-value ledger, and cleanup rows"
        )
    else:
        protected_product_promise = "Timed and debounced work starts predictably, preserves latest intended input, and does not create unbounded worker pressure or stale handles."
        invariant_oracle = "independent debounce model plus DBOS public handle results and read-only workflow status row observations"
    write_json(
        artifact_dir / "source-contract.json",
        {
            "frontier_id": FRONTIER_ID,
            "rung_id": plan.rung_id,
            "prompt_event_path": PROMPT_PATH,
            "protected_product_promise": protected_product_promise,
            "replay_command": f".workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung {plan.rung_id} --case {plan.case_id}",
            "seed": plan.seed,
            "invariant_oracle": invariant_oracle,
        },
    )
    app_url, sys_url, masked_admin = prepare_databases(plan.database_prefix, artifact_dir)
    thread_count_before = threading.active_count()
    dbos: DBOS | None = None
    try:
        DBOS.destroy(destroy_registry=False)
        dbos = DBOS(config=make_config(plan, app_url, sys_url))
        # Debouncer uses DBOS's internal queue. Force it into the registry before
        # launch so the queue manager starts a worker for it.
        dbos._registry.get_internal_queue()
        DBOS.launch()
        event("case_started", case_id=plan.case_id, rung=plan.rung_id, seed=plan.seed, schedule=plan.schedule)

        debounce_result: dict[str, Any] = {"submissions": 0}
        sleep_result: dict[str, Any] = {}
        if plan.rung_id == RUNG_000_ID:
            sleep_result = run_sleep_smoke()
        if plan.rung_id == RUNG_006_ID:
            debounce_result = run_async_debouncer_pressure(dbos, plan, app_url, sys_url)
        elif plan.submissions:
            debounce_result = submit_debounce_sequence(dbos, plan)
        if plan.rung_id == RUNG_006_ID:
            scheduler_result = {"scheduler_triggers": 0, "scheduled_results": []}
        elif plan.rung_id == RUNG_005_ID:
            scheduler_result = run_scheduler_queue_controls(dbos, plan)
        else:
            scheduler_result = run_scheduler_observation(dbos, plan)

        thread_count_after = threading.active_count()
        if plan.rung_id in {RUNG_002_ID, RUNG_004_ID} and plan.submissions:
            invariant(
                "worker_pressure_thread_growth_bounded",
                thread_count_after - thread_count_before <= plan.pressure_thread_growth_cap,
                thread_count_before=thread_count_before,
                thread_count_after=thread_count_after,
                cap=plan.pressure_thread_growth_cap,
            )

        outcome = {
            "classification": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case_id": plan.case_id,
            "seed": plan.seed,
            "schedule": plan.schedule,
            "focus": plan.focus,
            "redacted_admin_url": masked_admin,
            "thread_count_before": thread_count_before,
            "thread_count_after": thread_count_after,
            "sleep_result": sleep_result,
            "debounce_result": debounce_result,
            "scheduler_result": scheduler_result,
            "all_debouncer_rows": workflow_rows(dbos, names=[DEBOUNCER_WORKFLOW_NAME]),
            "all_target_rows": workflow_rows(dbos, names=[debounced_target.__qualname__]),
            "all_scheduled_queue_rows": workflow_rows(dbos, names=[scheduled_queue_control_workflow.__qualname__]),
            "internal_queue_name": INTERNAL_QUEUE_NAME,
        }
        write_json(artifact_dir / "result.json", outcome)
        event("case_passed", case_id=plan.case_id, rung=plan.rung_id)
        return outcome
    finally:
        try:
            DBOS.destroy(destroy_registry=False, workflow_completion_timeout_sec=2)
        finally:
            drop_databases(plan.database_prefix)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DBOS scheduler/debouncer timing workload")
    parser.add_argument("--rung", default=RUNG_001_ID)
    parser.add_argument("--case")
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument(
        "--artifact-dir",
        default="/tmp/wio-scheduler-debouncer-timing/artifacts",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        rung_id = normalize_rung(args.rung)
        if args.all_cases:
            if not args.sequential:
                raise SetupBlock("--all-cases requires --sequential to keep DBOS global state isolated")
            case_ids = case_ids_for_rung(rung_id)
        elif args.case:
            case_ids = [args.case]
        else:
            raise SetupBlock("--case or --all-cases is required")

        results = []
        for case_id in case_ids:
            plan = make_plan(rung_id, case_id)
            results.append(run_case(plan, Path(args.artifact_dir) / case_id))
        write_json(Path(args.artifact_dir) / "run-result.json", results)
        return 0
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 42
    except WorkloadFailure as exc:
        print(f"FINDING-CANDIDATE {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
