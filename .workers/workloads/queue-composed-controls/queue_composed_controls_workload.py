#!/usr/bin/env python3
"""Fresh WIO workload for DBOS database-backed queue composed controls.

Frontier: queue-composed-controls
Rungs:
  - rung-001-single-queue-ledger-controls
  - rung-002-partition-isolation-matrix
  - rung-003-live-config-rate-limit-matrix
  - rung-004-executor-relaunch-result-durability
  - rung-005-bounded-seed-sweep
  - rung-007-async-partition-worker-concurrency
  - rung-008-rate-limit-partial-index-plan
Evidence keys:
  - evidence-key:events/workload_runner-20260619T220331682226000Z.prompt.md
  - evidence-key:frontiers/queue-composed-controls/rungs/rung-002-partition-isolation-matrix.md
  - evidence-key:frontiers/queue-composed-controls/rungs/rung-003-live-config-rate-limit-matrix.md
  - evidence-key:frontiers/queue-composed-controls/rungs/rung-004-executor-relaunch-result-durability.md
  - evidence-key:frontiers/queue-composed-controls/rungs/rung-005-bounded-seed-sweep.md
  - inline:queue-composed-controls/rungs/rung-007-async-partition-worker-concurrency
Protected product promise: Postgres-backed DBOS queues complete accepted work,
preserve result retrieval, and correctly compose dedupe IDs, delay, priority,
rate limits, live queue configuration, partition keys, terminal statuses,
execution results, and cleanup.
Seed policy: exact rung seeds 2101, 2103, and 2107; each run writes the derived
request plan JSON next to the ledger and result artifact. Rung 002 uses exact
seeds 2111, 2113, 2117, and 2119. Rung 003 uses exact seeds 2123,
2129, 2131, 2137, 2141, and 2143. Rung 004 uses exact seeds 2147,
2153, 2159, 2161, 2167, and 2171. Rung 005 uses exact seeds 2201
through 2357 listed in the bounded sweep rung. Rung 007 uses exact seeds
2431, 2437, and 2441. Rung 008 uses exact seeds 6960, 6961, and 6962.
Invariant oracle: independent request/execution ledger cross-checked against
DBOS handles, terminal workflow status, timing/order predicates, and active
queue cleanup state.
Replay:
  .workers/python-runtime.sh .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-001 --all-cases --sequential
  .workers/python-runtime.sh .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-002 --all-cases --sequential
  .workers/python-runtime.sh .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-003 --case case-001
  .workers/python-runtime.sh .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-004 --case case-001
  .workers/python-runtime.sh .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-005 --all-cases --sequential
  .workers/python-runtime.sh .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-007 --all-cases --sequential
  .workers/python-runtime.sh .workers/workloads/queue-composed-controls/queue_composed_controls_workload.py --rung rung-008 --all-cases --sequential
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
from urllib.parse import quote


REPO_ROOT = Path(__file__).resolve().parents[3]
VENV_SITE_PACKAGES = REPO_ROOT / ".workers" / "vendor" / "dbos-venv" / "lib" / "python3.12" / "site-packages"
if VENV_SITE_PACKAGES.exists():
    sys.path.insert(0, str(VENV_SITE_PACKAGES))

DEFAULT_TARGETS = [
    REPO_ROOT / ".workers" / "vendor" / "dbos-transact-py",
    Path("/Users/viswa/code/workers/dbos-transact-py"),
]
for target in DEFAULT_TARGETS:
    if target.exists():
        sys.path.insert(0, str(target))
        break

try:
    import sqlalchemy as sa
    from sqlalchemy.engine import make_url

    from dbos import DBOS, DBOSClient, DBOSConfig, SetEnqueueOptions, SetWorkflowID
    from dbos._error import DBOSQueueDeduplicatedError
    from dbos._schemas.system_database import SystemSchema
    from dbos._sys_db import WorkflowStatusString
except Exception as exc:  # pragma: no cover - exercised by setup probes.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}")
    raise SystemExit(42)


RUN_ID = "run-local-queue-composed-controls"
FRONTIER_ID = "queue-composed-controls"
SCHEMA_PLACEHOLDER = "__dbos_placeholder_schema__"
RUNG_001_ID = "rung-001-single-queue-ledger-controls"
RUNG_002_ID = "rung-002-partition-isolation-matrix"
RUNG_003_ID = "rung-003-live-config-rate-limit-matrix"
RUNG_004_ID = "rung-004-executor-relaunch-result-durability"
RUNG_005_ID = "rung-005-bounded-seed-sweep"
RUNG_007_ID = "rung-007-async-partition-worker-concurrency"
RUNG_008_ID = "rung-008-rate-limit-partial-index-plan"
RUNG_ALIASES = {
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
    "rung-007": RUNG_007_ID,
    RUNG_007_ID: RUNG_007_ID,
    "rung-008": RUNG_008_ID,
    RUNG_008_ID: RUNG_008_ID,
}
PROMPT_PATHS = {
    RUNG_001_ID: "evidence-key:events/workload_runner-20260619T220331682226000Z.prompt.md",
    RUNG_002_ID: "evidence-key:frontiers/queue-composed-controls/rungs/rung-002-partition-isolation-matrix.md",
    RUNG_003_ID: "evidence-key:frontiers/queue-composed-controls/rungs/rung-003-live-config-rate-limit-matrix.md",
    RUNG_004_ID: "evidence-key:frontiers/queue-composed-controls/rungs/rung-004-executor-relaunch-result-durability.md",
    RUNG_005_ID: "evidence-key:frontiers/queue-composed-controls/rungs/rung-005-bounded-seed-sweep.md",
    RUNG_007_ID: "inline:queue-composed-controls/rungs/rung-007-async-partition-worker-concurrency",
    RUNG_008_ID: "inline:producer-20260624-rate-limit-partial-index-plan",
}
RUNG_005_SEEDS = [
    2201,
    2203,
    2207,
    2213,
    2221,
    2237,
    2239,
    2243,
    2251,
    2267,
    2269,
    2273,
    2281,
    2287,
    2293,
    2297,
    2309,
    2311,
    2333,
    2339,
    2341,
    2347,
    2351,
    2357,
]
ACTIVE_STATUSES = {
    WorkflowStatusString.ENQUEUED.value,
    WorkflowStatusString.DELAYED.value,
    WorkflowStatusString.PENDING.value,
}
TERMINAL_SUCCESS = WorkflowStatusString.SUCCESS.value

_ledger_lock = threading.Lock()
_ledger_rows: list[dict[str, Any]] = []
_active_lock = threading.Lock()
_active_count = 0
_max_active_count = 0
_active_partition_counts: dict[str, int] = {}
_max_active_partition_counts: dict[str, int] = {}
_release_gate = threading.Event()
_async_release_gate: asyncio.Event | None = None
_case_id = ""
_ledger_path: Path | None = None


class WorkloadFailure(Exception):
    pass


class SetupBlock(Exception):
    pass


@dataclass(frozen=True)
class RequestSpec:
    key: str
    workflow_id: str
    expected_result: str
    priority: int | None = None
    deduplication_id: str | None = None
    partition_key: str | None = None
    delay_seconds: float | None = None
    blocks: bool = False


@dataclass(frozen=True)
class CasePlan:
    rung_id: str
    case_id: str
    seed: int
    schedule: str
    fault_model: str
    queue_name: str
    database_prefix: str
    polling_interval_sec: float
    initial_concurrency: int | None
    initial_worker_concurrency: int | None
    partition_queue: bool
    initial_priority_enabled: bool
    initial_limiter: dict[str, float | int] | None
    updated_concurrency: int | None
    updated_worker_concurrency: int | None
    updated_limiter: dict[str, float | int] | None
    updated_priority_enabled: bool | None
    updated_polling_interval_sec: float | None
    release_after_eligible_ms: int
    duplicate_offset_ms: int | None
    requests: list[RequestSpec]
    duplicate_request: RequestSpec | None = None
    update_before_followers: bool = False


@dataclass(frozen=True)
class RateLimitPlan:
    rung_id: str
    case_id: str
    seed: int
    schedule: str
    fault_model: str
    queue_name: str
    database_prefix: str
    non_rate_limited_rows: int
    old_rate_limited_rows: int
    recent_target_rows: int
    recent_other_queue_rows: int
    recent_other_partition_rows: int
    partition_key: str | None
    limiter_limit: int
    limiter_period_sec: float
    functional_sanity: bool = False


def event(name: str, **fields: Any) -> None:
    parts = [f"WIO-EVENT {name}"]
    parts.extend(f"{key}={json.dumps(value, sort_keys=True)}" for key, value in fields.items())
    print(" ".join(parts), flush=True)


def invariant(name: str, ok: bool, **fields: Any) -> None:
    parts = [f"INVARIANT {name}={str(ok).lower()}"]
    parts.extend(f"{key}={json.dumps(value, sort_keys=True)}" for key, value in fields.items())
    print(" ".join(parts), flush=True)
    if not ok:
        raise WorkloadFailure(f"{name} failed: {json.dumps(fields, sort_keys=True)}")


def now_ms() -> int:
    return int(time.time() * 1000)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def append_ledger(row: dict[str, Any]) -> None:
    global _active_count, _max_active_count
    row = dict(row)
    row.setdefault("case_id", _case_id)
    partition_key = row.get("partition_key") or "__queue__"
    with _active_lock:
        _active_count += 1
        _max_active_count = max(_max_active_count, _active_count)
        _active_partition_counts[partition_key] = _active_partition_counts.get(partition_key, 0) + 1
        _max_active_partition_counts[partition_key] = max(
            _max_active_partition_counts.get(partition_key, 0),
            _active_partition_counts[partition_key],
        )
        row["active_count_at_start"] = _active_count
        row["max_active_seen"] = _max_active_count
        row["partition_active_count_at_start"] = _active_partition_counts[partition_key]
        row["max_partition_active_seen"] = _max_active_partition_counts[partition_key]
    with _ledger_lock:
        _ledger_rows.append(row)
        if _ledger_path is not None:
            with _ledger_path.open("a") as f:
                f.write(json.dumps(row, sort_keys=True) + "\n")


def mark_ledger_complete(request_key: str, partition_key: str | None) -> None:
    global _active_count
    partition_bucket = partition_key or "__queue__"
    row = {
        "case_id": _case_id,
        "request_key": request_key,
        "partition_key": partition_key,
        "event": "complete",
        "completed_at_ms": now_ms(),
    }
    with _active_lock:
        _active_count -= 1
        _active_partition_counts[partition_bucket] = _active_partition_counts.get(partition_bucket, 1) - 1
        row["active_count_after_complete"] = _active_count
        row["partition_active_count_after_complete"] = _active_partition_counts[partition_bucket]
    with _ledger_lock:
        _ledger_rows.append(row)
        if _ledger_path is not None:
            with _ledger_path.open("a") as f:
                f.write(json.dumps(row, sort_keys=True) + "\n")


def admin_url() -> sa.URL:
    raw = os.environ.get(
        "DBOS_POSTGRES_ADMIN_URL",
        "postgresql+psycopg://postgres:dbos@localhost:5432/postgres",
    )
    url = make_url(raw)
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+psycopg")
    return url


def db_url(base: sa.URL, database: str, *, driver: str) -> str:
    return str(base.set(drivername=driver, database=database))


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def case_suffix(case_id: str) -> str:
    return case_id.rsplit("-", 1)[-1]


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
        db_url(base, app_db, driver="postgresql"),
        db_url(base, sys_db, driver="postgresql+psycopg"),
        masked,
    )


def drop_databases(prefix: str) -> None:
    if os.environ.get("WIO_QUEUE_KEEP_DATABASES") == "1":
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
    suffix = case_suffix(plan.case_id)
    return {
        "name": f"wio-qcc-{suffix}",
        "application_database_url": app_url,
        "system_database_url": sys_url,
        "application_version": f"wio-{plan.rung_id}-{plan.case_id}",
        "executor_id": f"wio-{plan.case_id}",
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        "run_admin_server": False,
    }


def make_rate_limit_config(plan: RateLimitPlan, app_url: str, sys_url: str) -> DBOSConfig:
    suffix = case_suffix(plan.case_id)
    return {
        "name": f"wio-qcc-{suffix}",
        "application_database_url": app_url,
        "system_database_url": sys_url,
        "application_version": f"wio-{plan.rung_id}-{plan.case_id}",
        "executor_id": f"wio-{plan.case_id}",
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        "run_admin_server": False,
    }


def rate_limit_plan_for(case_id: str) -> RateLimitPlan:
    specs: dict[str, tuple[int, str, str, int, int, int, int, int, str | None, int, bool]] = {
        "case-001": (
            6960,
            "single-queue-rate-limit-plan",
            "50k non-rate-limited rows plus old/recent rate-limited rows for one queue",
            50_000,
            5_000,
            3,
            1_000,
            0,
            None,
            10,
            False,
        ),
        "case-002": (
            6961,
            "multi-queue-selectivity-plan",
            "many unrelated queues plus one hot rate-limited queue",
            50_000,
            5_000,
            4,
            8_000,
            0,
            None,
            10,
            False,
        ),
        "case-003": (
            6962,
            "partitioned-rate-limit-plan-plus-functional-sanity",
            "partitioned queue with hot partition at the limiter and live progress after window advances",
            20_000,
            2_000,
            2,
            2_000,
            3,
            "partition-hot",
            2,
            True,
        ),
    }
    if case_id not in specs:
        raise SetupBlock(f"unsupported case for {RUNG_008_ID}: {case_id}")
    (
        seed,
        schedule,
        fault_model,
        non_rate_limited_rows,
        old_rate_limited_rows,
        recent_target_rows,
        recent_other_queue_rows,
        recent_other_partition_rows,
        partition_key,
        limiter_limit,
        functional_sanity,
    ) = specs[case_id]
    rnd = random.Random(seed)
    return RateLimitPlan(
        rung_id=RUNG_008_ID,
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        fault_model=fault_model,
        queue_name=f"wio_q_rate_{seed}",
        database_prefix=f"wio_queue_rate_{seed}_{rnd.randrange(1000, 9999)}",
        non_rate_limited_rows=non_rate_limited_rows,
        old_rate_limited_rows=old_rate_limited_rows,
        recent_target_rows=recent_target_rows,
        recent_other_queue_rows=recent_other_queue_rows,
        recent_other_partition_rows=recent_other_partition_rows,
        partition_key=partition_key,
        limiter_limit=limiter_limit,
        limiter_period_sec=60.0,
        functional_sanity=functional_sanity,
    )


def plan_for(case_id: str, rung_id: str) -> CasePlan:
    if rung_id == RUNG_007_ID:
        return async_partition_plan_for(case_id)
    if rung_id == RUNG_005_ID:
        return sweep_plan_for(case_id)
    if rung_id == RUNG_004_ID:
        return relaunch_plan_for(case_id)
    if rung_id == RUNG_003_ID:
        return live_config_plan_for(case_id)
    if rung_id == RUNG_002_ID:
        return partition_plan_for(case_id)
    if case_id == "case-001":
        seed = 2101
        rnd = random.Random(seed)
        prefix = f"wio_queue_{seed}_{rnd.randrange(1000, 9999)}"
        dedup = f"dedup-{seed}-{rnd.randrange(10000, 99999)}"
        delayed = RequestSpec(
            key=f"{case_id}-delayed-dedup",
            workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, dedup)}",
            expected_result=f"result-{case_id}-delayed",
            priority=1,
            deduplication_id=dedup,
            delay_seconds=0.5,
        )
        return CasePlan(
            rung_id=rung_id,
            case_id=case_id,
            seed=seed,
            schedule="duplicate-while-delayed",
            fault_model="none; duplicate offset 50ms",
            queue_name=f"wio_q_{seed}",
            database_prefix=prefix,
            polling_interval_sec=0.05,
            initial_concurrency=1,
            initial_worker_concurrency=None,
            partition_queue=False,
            initial_priority_enabled=True,
            initial_limiter={"limit": 10, "period": 1.0},
            updated_concurrency=None,
            updated_worker_concurrency=None,
            updated_limiter=None,
            updated_priority_enabled=None,
            updated_polling_interval_sec=None,
            release_after_eligible_ms=150,
            duplicate_offset_ms=50,
            requests=[
                RequestSpec(
                    key=f"{case_id}-blocker",
                    workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, 'blocker-' + str(seed))}",
                    expected_result=f"result-{case_id}-blocker",
                    priority=1,
                    blocks=True,
                ),
                delayed,
            ],
            duplicate_request=RequestSpec(
                key=f"{case_id}-duplicate-rejected",
                workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, 'duplicate-' + str(seed))}",
                expected_result=f"result-{case_id}-duplicate",
                priority=1,
                deduplication_id=dedup,
                delay_seconds=0.5,
            ),
        )
    if case_id == "case-002":
        seed = 2103
        rnd = random.Random(seed)
        prefix = f"wio_queue_{seed}_{rnd.randrange(1000, 9999)}"
        specs = [
            RequestSpec(
                key=f"{case_id}-blocker",
                workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, 'blocker-' + str(seed))}",
                expected_result=f"result-{case_id}-blocker",
                priority=1,
                blocks=True,
            ),
            RequestSpec(
                key=f"{case_id}-priority-3",
                workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, 'p3-' + str(seed))}",
                expected_result=f"result-{case_id}-p3",
                priority=3,
            ),
            RequestSpec(
                key=f"{case_id}-priority-1-delayed",
                workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, 'p1-' + str(seed))}",
                expected_result=f"result-{case_id}-p1",
                priority=1,
                delay_seconds=0.5,
            ),
            RequestSpec(
                key=f"{case_id}-priority-2",
                workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, 'p2-' + str(seed))}",
                expected_result=f"result-{case_id}-p2",
                priority=2,
            ),
        ]
        return CasePlan(
            rung_id=rung_id,
            case_id=case_id,
            seed=seed,
            schedule="priority-after-release",
            fault_model="none; release after delay plus 150ms",
            queue_name=f"wio_q_{seed}",
            database_prefix=prefix,
            polling_interval_sec=0.05,
            initial_concurrency=1,
            initial_worker_concurrency=None,
            partition_queue=False,
            initial_priority_enabled=True,
            initial_limiter={"limit": 10, "period": 1.0},
            updated_concurrency=None,
            updated_worker_concurrency=None,
            updated_limiter=None,
            updated_priority_enabled=None,
            updated_polling_interval_sec=None,
            release_after_eligible_ms=150,
            duplicate_offset_ms=None,
            requests=specs,
        )
    if case_id == "case-003":
        seed = 2107
        rnd = random.Random(seed)
        prefix = f"wio_queue_{seed}_{rnd.randrange(1000, 9999)}"
        specs = [
            RequestSpec(
                key=f"{case_id}-blocker",
                workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, 'blocker-' + str(seed))}",
                expected_result=f"result-{case_id}-blocker",
                priority=1,
                blocks=True,
            )
        ]
        for idx, priority in enumerate([1, 2, 3, 4], start=1):
            specs.append(
                RequestSpec(
                    key=f"{case_id}-backlog-{idx}",
                    workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, 'backlog-' + str(seed) + '-' + str(idx))}",
                    expected_result=f"result-{case_id}-backlog-{idx}",
                    priority=priority,
                )
            )
        return CasePlan(
            rung_id=rung_id,
            case_id=case_id,
            seed=seed,
            schedule="live-config-backlog",
            fault_model="none; config update before release",
            queue_name=f"wio_q_{seed}",
            database_prefix=prefix,
            polling_interval_sec=0.05,
            initial_concurrency=1,
            initial_worker_concurrency=None,
            partition_queue=False,
            initial_priority_enabled=True,
            initial_limiter={"limit": 2, "period": 0.8},
            updated_concurrency=2,
            updated_worker_concurrency=None,
            updated_limiter={"limit": 2, "period": 0.8},
            updated_priority_enabled=None,
            updated_polling_interval_sec=None,
            release_after_eligible_ms=900,
            duplicate_offset_ms=None,
            requests=specs,
        )
    raise ValueError(f"unknown case: {case_id}")


def async_partition_plan_for(case_id: str) -> CasePlan:
    seeds = {
        "case-001": 2431,
        "case-002": 2437,
        "case-003": 2441,
    }
    schedules = {
        "case-001": "async-two-partition-saturation",
        "case-002": "inherited-partition-child-enqueue",
        "case-003": "live-worker-concurrency-partition-update",
    }
    if case_id not in seeds:
        raise ValueError(f"unknown rung-007 case: {case_id}")
    seed = seeds[case_id]
    rnd = random.Random(seed)
    prefix = f"wio_queue_{seed}_{rnd.randrange(1000, 9999)}"
    partitions = [f"partition-a-{seed}", f"partition-b-{seed}"]

    def req(
        name: str,
        partition: str,
        *,
        blocks: bool = True,
        workflow_id_extra: str = "",
    ) -> RequestSpec:
        identity = str((RUNG_007_ID, case_id, seed, name, partition, workflow_id_extra))
        return RequestSpec(
            key=f"{case_id}-{name}",
            workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, identity)}",
            expected_result=f"result-{case_id}-{name}",
            partition_key=partition,
            blocks=blocks,
        )

    if case_id == "case-002":
        parent_partition = "parent-a"
        requests = [
            req("parent", parent_partition, blocks=False),
            req("child", parent_partition, blocks=False, workflow_id_extra="child"),
        ]
        initial_worker_concurrency = 2
        updated_worker_concurrency = None
        focus = (
            "async dequeued parent enqueues a child on the same partitioned queue "
            "without an inner SetEnqueueOptions queue_partition_key override"
        )
    else:
        requests = [
            req(f"{partition_name}-job-{idx}", partition_name)
            for partition_name in partitions
            for idx in range(1, 5)
        ]
        initial_worker_concurrency = 1 if case_id == "case-003" else 2
        updated_worker_concurrency = 2 if case_id == "case-003" else None
        focus = (
            "async partitioned queue must enforce worker_concurrency independently per partition "
            "and expose live running saturation before gate release"
        )

    return CasePlan(
        rung_id=RUNG_007_ID,
        case_id=case_id,
        seed=seed,
        schedule=schedules[case_id],
        fault_model=focus,
        queue_name=f"wio_qa_{seed}",
        database_prefix=prefix,
        polling_interval_sec=0.05,
        initial_concurrency=None,
        initial_worker_concurrency=initial_worker_concurrency,
        partition_queue=True,
        initial_priority_enabled=False,
        initial_limiter=None,
        updated_concurrency=None,
        updated_worker_concurrency=updated_worker_concurrency,
        updated_limiter=None,
        updated_priority_enabled=None,
        updated_polling_interval_sec=None,
        release_after_eligible_ms=0,
        duplicate_offset_ms=None,
        requests=requests,
    )


def sweep_plan_for(case_id: str) -> CasePlan:
    try:
        case_num = int(case_id.rsplit("-", 1)[-1])
    except ValueError as exc:
        raise ValueError(f"unknown rung-005 case: {case_id}") from exc
    if case_num < 1 or case_num > len(RUNG_005_SEEDS):
        raise ValueError(f"unknown rung-005 case: {case_id}")

    seed = RUNG_005_SEEDS[case_num - 1]
    rnd = random.Random(seed)
    prefix = f"wio_queue_{seed}_{rnd.randrange(1000, 9999)}"

    def req(
        name: str,
        priority: int | None = None,
        *,
        blocks: bool = False,
        delay: float | None = None,
        dedup: str | None = None,
        partition: str | None = None,
    ) -> RequestSpec:
        return RequestSpec(
            key=f"{case_id}-{name}",
            workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, str((seed, name, priority, delay, partition)))}",
            expected_result=f"result-{case_id}-{name}",
            priority=priority,
            deduplication_id=dedup,
            partition_key=partition,
            delay_seconds=delay,
            blocks=blocks,
        )

    release_after_eligible_ms = rnd.randrange(0, 301)
    duplicate_request: RequestSpec | None = None
    update_before_followers = False
    partition_queue = False
    worker_concurrency: int | None = None
    initial_priority_enabled = True
    initial_limiter: dict[str, float | int] | None = {"limit": 10, "period": 1.0}
    updated_concurrency: int | None = None
    updated_limiter: dict[str, float | int] | None = None

    if case_num <= 6:
        delay = round(0.2 + rnd.random() * 0.8, 3)
        dedup = f"sweep-dedup-{seed}-{rnd.randrange(10000, 99999)}"
        requests = [
            req("blocker", 1, blocks=True),
            req("delayed-dedupe", 1, delay=delay, dedup=dedup),
        ]
        for idx in range(rnd.randrange(1, 5)):
            requests.append(req(f"follower-{idx + 1}", rnd.randrange(1, 8)))
        duplicate_request = RequestSpec(
            key=f"{case_id}-duplicate-rejected",
            workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, 'sweep-duplicate-' + str(seed))}",
            expected_result=f"result-{case_id}-duplicate",
            priority=1,
            deduplication_id=dedup,
            delay_seconds=delay,
        )
        schedule = "sweep-dedupe-delay"
        focus = f"dedupe-delay seed sweep; delay={delay}s release_offset={release_after_eligible_ms}ms"
        initial_concurrency = 1
        duplicate_offset_ms = rnd.randrange(0, min(200, int(delay * 1000)))
    elif case_num <= 12:
        backlog_count = rnd.randrange(4, 10)
        requests = [req("blocker", None, blocks=True)]
        priorities = list(range(1, backlog_count + 1))
        rnd.shuffle(priorities)
        requests.extend(req(f"priority-{idx + 1}", priority) for idx, priority in enumerate(priorities))
        schedule = "priority-enable-before-release"
        focus = f"priority/config seed sweep; backlog={backlog_count} priorities={priorities}"
        initial_concurrency = 1
        initial_priority_enabled = False
        updated_concurrency = None
        updated_limiter = {"limit": rnd.choice([2, 3, 4]), "period": round(rnd.choice([0.5, 0.8, 1.0]), 2)}
        initial_limiter = {"limit": 1, "period": 0.8}
        update_before_followers = True
        duplicate_offset_ms = None
    elif case_num <= 18:
        blocked = f"blocked-{seed}-{rnd.randrange(100, 999)}"
        normal_partitions = [f"normal-{idx}-{seed}-{rnd.randrange(100, 999)}" for idx in range(rnd.randrange(1, 4))]
        requests = [req("blocked-root", 1, blocks=True, partition=blocked)]
        for idx in range(rnd.randrange(1, 4)):
            requests.append(req(f"blocked-follower-{idx + 1}", idx + 2, partition=blocked))
        for idx, partition in enumerate(normal_partitions, start=1):
            delay = round(0.2 + rnd.random() * 0.5, 3) if rnd.random() < 0.5 else None
            requests.append(req(f"normal-{idx}", rnd.randrange(1, 5), delay=delay, partition=partition))
        schedule = "sweep-partition-isolation"
        focus = f"partition seed sweep; normal_partitions={len(normal_partitions)} release_offset={release_after_eligible_ms}ms"
        initial_concurrency = 1
        worker_concurrency = 1
        partition_queue = True
        duplicate_offset_ms = None
    elif case_num == 19:
        requests = [req(f"accepted-{idx}", rnd.randrange(1, 8)) for idx in range(1, rnd.randrange(5, 9))]
        schedule = "stop-after-acceptance"
        focus = "relaunch sweep; accepted rows survive stopped-app boundary"
        initial_concurrency = rnd.choice([2, 3])
        duplicate_offset_ms = None
    elif case_num == 20:
        requests = [
            req(f"delayed-{idx}", idx, delay=round(0.2 + rnd.random() * 0.8, 3))
            for idx in range(1, rnd.randrange(4, 7))
        ]
        schedule = "stop-while-delayed"
        focus = "relaunch sweep; delayed rows remain ineligible until modeled time"
        initial_concurrency = 2
        duplicate_offset_ms = None
    elif case_num == 21:
        dedup = f"sweep-relaunch-dedup-{seed}-{rnd.randrange(10000, 99999)}"
        requests = [req("dedupe-accepted", 1, dedup=dedup), req("independent", 2)]
        duplicate_request = RequestSpec(
            key=f"{case_id}-duplicate-rejected",
            workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, 'sweep-relaunch-duplicate-' + str(seed))}",
            expected_result=f"result-{case_id}-duplicate",
            priority=1,
            deduplication_id=dedup,
        )
        schedule = "duplicate-then-stop"
        focus = "relaunch sweep; duplicate rejection remains side-effect-free"
        initial_concurrency = 2
        duplicate_offset_ms = 50
    elif case_num == 22:
        blocked = f"blocked-{seed}-{rnd.randrange(100, 999)}"
        normal = f"normal-{seed}-{rnd.randrange(100, 999)}"
        requests = [
            req("blocked-root", 1, blocks=True, partition=blocked),
            req("blocked-follower", 2, partition=blocked),
            req("normal-a", 1, partition=normal),
            req("normal-b", 2, partition=normal),
        ]
        schedule = "partition-stop-release"
        focus = "relaunch sweep; partition isolation survives relaunch"
        initial_concurrency = 1
        worker_concurrency = 1
        partition_queue = True
        duplicate_offset_ms = None
    elif case_num == 23:
        requests = [req("blocker", 1, blocks=True)] + [req(f"backlog-{idx}", idx) for idx in range(1, 7)]
        schedule = "config-stop-relaunch"
        focus = "relaunch sweep; persisted config survives relaunch"
        initial_concurrency = 1
        initial_limiter = {"limit": 1, "period": 0.8}
        updated_concurrency = 3
        updated_limiter = {"limit": 3, "period": 0.8}
        update_before_followers = True
        duplicate_offset_ms = None
    else:
        requests = [req(f"terminal-{idx}", idx) for idx in range(1, rnd.randrange(5, 9))]
        schedule = "stop-before-cleanup"
        focus = "relaunch sweep; cleanup converges after second app restart"
        initial_concurrency = 2
        duplicate_offset_ms = None

    return CasePlan(
        rung_id=RUNG_005_ID,
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        fault_model=focus,
        queue_name=f"wio_qs_{seed}",
        database_prefix=prefix,
        polling_interval_sec=0.05,
        initial_concurrency=initial_concurrency,
        initial_worker_concurrency=worker_concurrency,
        partition_queue=partition_queue,
        initial_priority_enabled=initial_priority_enabled,
        initial_limiter=initial_limiter,
        updated_concurrency=updated_concurrency,
        updated_worker_concurrency=None,
        updated_limiter=updated_limiter,
        updated_priority_enabled=True if update_before_followers else None,
        updated_polling_interval_sec=None,
        release_after_eligible_ms=release_after_eligible_ms,
        duplicate_offset_ms=duplicate_offset_ms,
        requests=requests,
        duplicate_request=duplicate_request,
        update_before_followers=update_before_followers,
    )


def relaunch_plan_for(case_id: str) -> CasePlan:
    seeds = {
        "case-001": 2147,
        "case-002": 2153,
        "case-003": 2159,
        "case-004": 2161,
        "case-005": 2167,
        "case-006": 2171,
    }
    if case_id not in seeds:
        raise ValueError(f"unknown rung-004 case: {case_id}")
    seed = seeds[case_id]
    rnd = random.Random(seed)
    prefix = f"wio_queue_{seed}_{rnd.randrange(1000, 9999)}"

    def req(
        name: str,
        priority: int | None = None,
        *,
        blocks: bool = False,
        delay: float | None = None,
        dedup: str | None = None,
        partition: str | None = None,
    ) -> RequestSpec:
        return RequestSpec(
            key=f"{case_id}-{name}",
            workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, str((seed, name, priority, delay, partition)))}",
            expected_result=f"result-{case_id}-{name}",
            priority=priority,
            deduplication_id=dedup,
            partition_key=partition,
            delay_seconds=delay,
            blocks=blocks,
        )

    initial_limiter: dict[str, float | int] | None = {"limit": 10, "period": 1.0}
    updated_limiter: dict[str, float | int] | None = None
    updated_concurrency: int | None = None
    partition_queue = False
    initial_worker_concurrency: int | None = None
    initial_priority_enabled = True
    update_before_followers = False

    if case_id == "case-001":
        requests = [req(f"accepted-{idx}", idx) for idx in range(1, 5)]
        schedule = "stop-after-acceptance"
        focus = "app executor stopped after accepted queue rows are durable; relaunch must run all accepted work"
        initial_concurrency = 2
    elif case_id == "case-002":
        requests = [
            req("delayed-a", 1, delay=0.6),
            req("delayed-b", 2, delay=0.7),
            req("delayed-c", 3, delay=0.8),
        ]
        schedule = "stop-while-delayed"
        focus = "app executor stopped while all accepted rows are DELAYED; relaunch before eligibility must not run early"
        initial_concurrency = 2
    elif case_id == "case-003":
        dedup = f"dedup-relaunch-{seed}-{rnd.randrange(10000, 99999)}"
        accepted = req("dedupe-accepted", 1, dedup=dedup)
        requests = [accepted, req("independent", 2)]
        schedule = "duplicate-then-stop"
        focus = "duplicate rejection before relaunch must not become a second execution"
        initial_concurrency = 2
        duplicate_request = RequestSpec(
            key=f"{case_id}-duplicate-rejected",
            workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, 'duplicate-relaunch-' + str(seed))}",
            expected_result=f"result-{case_id}-duplicate",
            priority=1,
            deduplication_id=dedup,
        )
    elif case_id == "case-004":
        blocked = f"blocked-{seed}-{rnd.randrange(100, 999)}"
        normal = f"normal-{seed}-{rnd.randrange(100, 999)}"
        requests = [
            req("blocked-root", 1, blocks=True, partition=blocked),
            req("blocked-follower", 2, partition=blocked),
            req("normal-a", 1, partition=normal),
            req("normal-b", 2, partition=normal),
        ]
        schedule = "partition-stop-release"
        focus = "partitioned queue rows accepted before relaunch must preserve normal-partition progress while blocked waits"
        initial_concurrency = 1
        initial_worker_concurrency = 1
        partition_queue = True
    elif case_id == "case-005":
        requests = [req("blocker", 1, blocks=True)] + [req(f"backlog-{idx}", idx) for idx in range(1, 6)]
        schedule = "config-stop-relaunch"
        focus = "persisted queue config update before stop must be loaded by relaunched executor"
        initial_concurrency = 1
        initial_limiter = {"limit": 1, "period": 0.8}
        updated_concurrency = 3
        updated_limiter = {"limit": 3, "period": 0.8}
        update_before_followers = True
    else:
        requests = [req(f"terminal-{idx}", idx) for idx in range(1, 5)]
        schedule = "stop-before-cleanup"
        focus = "after relaunch completion, a second app restart before cleanup polling must still converge active rows"
        initial_concurrency = 2

    return CasePlan(
        rung_id=RUNG_004_ID,
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        fault_model=focus,
        queue_name=f"wio_qr_{seed}",
        database_prefix=prefix,
        polling_interval_sec=0.05,
        initial_concurrency=initial_concurrency,
        initial_worker_concurrency=initial_worker_concurrency,
        partition_queue=partition_queue,
        initial_priority_enabled=initial_priority_enabled,
        initial_limiter=initial_limiter,
        updated_concurrency=updated_concurrency,
        updated_worker_concurrency=None,
        updated_limiter=updated_limiter,
        updated_priority_enabled=None,
        updated_polling_interval_sec=None,
        release_after_eligible_ms=150,
        duplicate_offset_ms=50,
        requests=requests,
        duplicate_request=duplicate_request if case_id == "case-003" else None,
        update_before_followers=update_before_followers,
    )


def live_config_plan_for(case_id: str) -> CasePlan:
    seeds = {
        "case-001": 2123,
        "case-002": 2129,
        "case-003": 2131,
        "case-004": 2137,
        "case-005": 2141,
        "case-006": 2143,
    }
    if case_id not in seeds:
        raise ValueError(f"unknown rung-003 case: {case_id}")
    seed = seeds[case_id]
    rnd = random.Random(seed)
    prefix = f"wio_queue_{seed}_{rnd.randrange(1000, 9999)}"

    def req(name: str, priority: int | None = None, *, blocks: bool = False, delay: float | None = None) -> RequestSpec:
        return RequestSpec(
            key=f"{case_id}-{name}",
            workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, str((seed, name, priority, delay)))}",
            expected_result=f"result-{case_id}-{name}",
            priority=priority,
            delay_seconds=delay,
            blocks=blocks,
        )

    if case_id == "case-001":
        requests = [req("blocker", 1, blocks=True)] + [req(f"backlog-{idx}", idx) for idx in range(1, 7)]
        schedule = "concurrency-increase"
        focus = "no dependency fault; concurrency 1 -> 3 while backlog is accepted"
        initial_concurrency = 1
        updated_concurrency = 3
        updated_worker_concurrency = None
        initial_limiter = {"limit": 10, "period": 1.0}
        updated_limiter = None
        initial_priority_enabled = True
        updated_priority_enabled = None
        updated_polling_interval_sec = None
        update_before_followers = False
    elif case_id == "case-002":
        requests = [req(f"blocker-{idx}", idx, blocks=True) for idx in range(1, 4)] + [
            req("after-drain-a", 4),
            req("after-drain-b", 5),
        ]
        schedule = "concurrency-decrease"
        focus = "no dependency fault; concurrency 3 -> 1 while active work is present"
        initial_concurrency = 3
        updated_concurrency = 1
        updated_worker_concurrency = None
        initial_limiter = {"limit": 10, "period": 1.0}
        updated_limiter = None
        initial_priority_enabled = True
        updated_priority_enabled = None
        updated_polling_interval_sec = None
        update_before_followers = False
    elif case_id == "case-003":
        requests = [req("blocker", 1, blocks=True)] + [req(f"limited-{idx}", idx) for idx in range(1, 7)]
        schedule = "limiter-increase"
        focus = "no dependency fault; limiter 1/0.8s -> 3/0.8s under backlog"
        initial_concurrency = 3
        updated_concurrency = None
        updated_worker_concurrency = None
        initial_limiter = {"limit": 1, "period": 0.8}
        updated_limiter = {"limit": 3, "period": 0.8}
        initial_priority_enabled = True
        updated_priority_enabled = None
        updated_polling_interval_sec = None
        update_before_followers = False
    elif case_id == "case-004":
        requests = [req("blocker", 1, blocks=True)] + [
            req("delayed-a", 1, delay=0.4),
            req("delayed-b", 2, delay=0.5),
            req("delayed-c", 3, delay=0.6),
            req("delayed-d", 4, delay=0.7),
        ]
        schedule = "polling-plus-delay"
        focus = "no dependency fault; polling interval update must not start delayed rows early"
        initial_concurrency = 2
        updated_concurrency = None
        updated_worker_concurrency = None
        initial_limiter = {"limit": 10, "period": 1.0}
        updated_limiter = None
        initial_priority_enabled = True
        updated_priority_enabled = None
        updated_polling_interval_sec = 0.05
        update_before_followers = False
    elif case_id == "case-005":
        requests = [req("blocker", None, blocks=True)] + [
            req("priority-5", 5),
            req("priority-1", 1),
            req("priority-3", 3),
            req("priority-2", 2),
        ]
        schedule = "priority-enable-before-release"
        focus = "no dependency fault; enable priority before accepting prioritized backlog"
        initial_concurrency = 1
        updated_concurrency = None
        updated_worker_concurrency = None
        initial_limiter = {"limit": 10, "period": 1.0}
        updated_limiter = None
        initial_priority_enabled = False
        updated_priority_enabled = True
        updated_polling_interval_sec = None
        update_before_followers = True
    else:
        requests = [req("blocker", 1, blocks=True)] + [req(f"mixed-{idx}", idx) for idx in range(1, 9)]
        schedule = "concurrency-then-limiter"
        focus = "no dependency fault; concurrency and limiter updates must preserve terminal cleanup"
        initial_concurrency = 1
        updated_concurrency = 2
        updated_worker_concurrency = None
        initial_limiter = {"limit": 1, "period": 0.8}
        updated_limiter = {"limit": 2, "period": 0.8}
        initial_priority_enabled = True
        updated_priority_enabled = None
        updated_polling_interval_sec = None
        update_before_followers = False

    return CasePlan(
        rung_id=RUNG_003_ID,
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        fault_model=focus,
        queue_name=f"wio_ql_{seed}",
        database_prefix=prefix,
        polling_interval_sec=0.1,
        initial_concurrency=initial_concurrency,
        initial_worker_concurrency=None,
        partition_queue=False,
        initial_priority_enabled=initial_priority_enabled,
        initial_limiter=initial_limiter,
        updated_concurrency=updated_concurrency,
        updated_worker_concurrency=updated_worker_concurrency,
        updated_limiter=updated_limiter,
        updated_priority_enabled=updated_priority_enabled,
        updated_polling_interval_sec=updated_polling_interval_sec,
        release_after_eligible_ms=150,
        duplicate_offset_ms=None,
        requests=requests,
        update_before_followers=update_before_followers,
    )


def partition_plan_for(case_id: str) -> CasePlan:
    seeds = {
        "case-001": 2111,
        "case-002": 2113,
        "case-003": 2117,
        "case-004": 2119,
    }
    if case_id not in seeds:
        raise ValueError(f"unknown rung-002 case: {case_id}")
    seed = seeds[case_id]
    rnd = random.Random(seed)
    prefix = f"wio_queue_{seed}_{rnd.randrange(1000, 9999)}"
    blocked = f"blocked-{seed}-{rnd.randrange(100, 999)}"
    normal = f"normal-{seed}-{rnd.randrange(100, 999)}"

    def req(name: str, partition: str, priority: int, *, blocks: bool = False, delay: float | None = None) -> RequestSpec:
        return RequestSpec(
            key=f"{case_id}-{name}",
            workflow_id=f"{case_id}-{uuid.uuid5(uuid.NAMESPACE_DNS, str((seed, name, partition, priority, delay)))}",
            expected_result=f"result-{case_id}-{name}",
            priority=priority,
            partition_key=partition,
            delay_seconds=delay,
            blocks=blocks,
        )

    if case_id == "case-001":
        requests = [
            req("blocked-root", blocked, 1, blocks=True),
            req("blocked-follower", blocked, 2),
            req("normal-immediate", normal, 1),
        ]
        schedule = "blocked-plus-normal"
        focus = "no dependency fault; prove normal partition terminal progress before blocked release"
    elif case_id == "case-002":
        requests = [
            req("blocked-root", blocked, 1, blocks=True),
            req("blocked-follower-a", blocked, 2),
            req("blocked-follower-b", blocked, 3),
            req("normal-a", normal, 1),
            req("normal-b", normal, 2),
        ]
        schedule = "multi-follower"
        focus = "no dependency fault; prove blocked followers wait while normal partition drains"
    elif case_id == "case-003":
        requests = [
            req("blocked-root", blocked, 1, blocks=True),
            req("blocked-p3", blocked, 3),
            req("blocked-p1", blocked, 1),
            req("blocked-p2", blocked, 2),
            req("normal-progress", normal, 1),
        ]
        schedule = "partition-priority"
        focus = "no dependency fault; prove priority order inside blocked partition after release"
    else:
        requests = [
            req("blocked-root", blocked, 1, blocks=True),
            req("blocked-follower", blocked, 2),
            req("normal-delayed", normal, 1, delay=0.5),
        ]
        schedule = "partition-delay"
        focus = "no dependency fault; prove delayed normal work waits for eligibility and still completes before blocked release"

    return CasePlan(
        rung_id=RUNG_002_ID,
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        fault_model=focus,
        queue_name=f"wio_qp_{seed}",
        database_prefix=prefix,
        polling_interval_sec=0.05,
        initial_concurrency=1,
        initial_worker_concurrency=1,
        partition_queue=True,
        initial_priority_enabled=True,
        initial_limiter={"limit": 10, "period": 1.0},
        updated_concurrency=None,
        updated_worker_concurrency=None,
        updated_limiter=None,
        updated_priority_enabled=None,
        updated_polling_interval_sec=None,
        release_after_eligible_ms=150,
        duplicate_offset_ms=None,
        requests=requests,
    )


def workflow_body(
    request_key: str,
    expected_result: str,
    should_block: bool,
    partition_key: str | None = None,
) -> str:
    started = now_ms()
    append_ledger(
        {
            "event": "start",
            "request_key": request_key,
            "partition_key": partition_key,
            "workflow_id": DBOS.workflow_id,
            "started_at_ms": started,
            "thread": threading.current_thread().name,
        }
    )
    if should_block:
        _release_gate.wait(timeout=120)
    time.sleep(0.05)
    mark_ledger_complete(request_key, partition_key)
    return expected_result


async def async_partition_workflow_body(
    request_key: str,
    expected_result: str,
    should_block: bool,
    partition_key: str | None = None,
) -> str:
    append_ledger(
        {
            "event": "start",
            "request_key": request_key,
            "partition_key": partition_key,
            "workflow_id": DBOS.workflow_id,
            "started_at_ms": now_ms(),
            "thread": threading.current_thread().name,
        }
    )
    if should_block:
        if _async_release_gate is None:
            raise WorkloadFailure("async release gate not initialized")
        await asyncio.wait_for(_async_release_gate.wait(), timeout=120)
    await asyncio.sleep(0.05)
    mark_ledger_complete(request_key, partition_key)
    return expected_result


def wait_for_started(request_key: str, timeout_sec: float) -> dict[str, Any]:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        with _ledger_lock:
            for row in _ledger_rows:
                if row.get("event") == "start" and row.get("request_key") == request_key:
                    return row
        time.sleep(0.02)
    raise WorkloadFailure(f"request did not start in time: {request_key}")


def partition_start_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    with _ledger_lock:
        for row in _ledger_rows:
            if row.get("event") != "start":
                continue
            partition_key = row.get("partition_key") or "__queue__"
            counts[partition_key] = counts.get(partition_key, 0) + 1
    return counts


async def wait_for_partition_start_counts(
    expected: dict[str, int],
    *,
    timeout_sec: float,
    cap_per_partition: int,
) -> dict[str, int]:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        counts = partition_start_counts()
        with _active_lock:
            max_seen = dict(_max_active_partition_counts)
        over_cap = {
            partition: value
            for partition, value in max_seen.items()
            if value > cap_per_partition
        }
        invariant(
            "async_partition_running_window_within_modeled_worker_concurrency",
            not over_cap,
            cap_per_partition=cap_per_partition,
            max_active_by_partition=max_seen,
            over_cap=over_cap,
        )
        if all(counts.get(partition, 0) >= needed for partition, needed in expected.items()):
            return counts
        await asyncio.sleep(0.02)
    raise WorkloadFailure(
        "partition saturation window not reached before timeout: "
        + json.dumps(
            {
                "expected": expected,
                "actual": partition_start_counts(),
                "max_active_by_partition": _max_active_partition_counts,
            },
            sort_keys=True,
        )
    )


def status_rows(dbos: Any, queue_name: str) -> list[dict[str, Any]]:
    cols = SystemSchema.workflow_status.c
    with dbos._sys_db.engine.begin() as conn:
        rows = conn.execute(
            sa.select(
                cols.workflow_uuid,
                cols.status,
                cols.queue_name,
                cols.deduplication_id,
                cols.queue_partition_key,
                cols.priority,
                cols.created_at,
                cols.updated_at,
                cols.started_at_epoch_ms,
                cols.delay_until_epoch_ms,
                cols.completed_at,
            )
            .select_from(SystemSchema.workflow_status)
            .where(cols.queue_name == queue_name)
            .order_by(cols.created_at.asc())
        ).mappings()
        return [dict(row) for row in rows]


def active_queue_rows(dbos: Any, queue_name: str) -> list[dict[str, Any]]:
    return [row for row in status_rows(dbos, queue_name) if row["status"] in ACTIVE_STATUSES]


def queue_config_snapshot(dbos: Any, queue_name: str) -> dict[str, Any]:
    queue = DBOS.retrieve_queue(queue_name)
    if queue is None:
        raise WorkloadFailure(f"queue not found: {queue_name}")
    return {
        "name": queue.name,
        "concurrency": queue.concurrency,
        "worker_concurrency": queue.worker_concurrency,
        "limiter": queue.limiter,
        "priority_enabled": queue.priority_enabled,
        "partition_queue": queue.partition_queue,
        "polling_interval_sec": queue.polling_interval_sec,
    }


async def queue_config_snapshot_async(queue: Any) -> dict[str, Any]:
    return {
        "name": queue.name,
        "concurrency": await queue.get_concurrency_async(),
        "worker_concurrency": await queue.get_worker_concurrency_async(),
        "limiter": await queue.get_limiter_async(),
        "priority_enabled": await queue.get_priority_enabled_async(),
        "partition_queue": await queue.get_partition_queue_async(),
        "polling_interval_sec": await queue.get_polling_interval_sec_async(),
    }


def enqueue_request(queue_name: str, request: RequestSpec) -> Any:
    with SetEnqueueOptions(
        deduplication_id=request.deduplication_id,
        priority=request.priority,
        queue_partition_key=request.partition_key,
        delay_seconds=request.delay_seconds,
    ):
        with SetWorkflowID(request.workflow_id):
            return DBOS.enqueue_workflow(
                queue_name,
                workflow_body,
                request.key,
                request.expected_result,
                request.blocks,
                request.partition_key,
            )


async def enqueue_request_async(queue_name: str, request: RequestSpec, workflow_func: Any) -> Any:
    with SetEnqueueOptions(
        deduplication_id=request.deduplication_id,
        priority=request.priority,
        queue_partition_key=request.partition_key,
        delay_seconds=request.delay_seconds,
    ):
        with SetWorkflowID(request.workflow_id):
            return await DBOS.enqueue_workflow_async(
                queue_name,
                workflow_func,
                request.key,
                request.expected_result,
                request.blocks,
                request.partition_key,
            )


def client_enqueue_request(client: DBOSClient, plan: CasePlan, request: RequestSpec, app_version: str) -> Any:
    options: dict[str, Any] = {
        "workflow_name": "workflow_body",
        "queue_name": plan.queue_name,
        "workflow_id": request.workflow_id,
        "app_version": app_version,
    }
    if request.deduplication_id is not None:
        options["deduplication_id"] = request.deduplication_id
    if request.priority is not None:
        options["priority"] = request.priority
    if request.partition_key is not None:
        options["queue_partition_key"] = request.partition_key
    if request.delay_seconds is not None:
        options["delay_seconds"] = request.delay_seconds
    return client.enqueue(
        options, request.key, request.expected_result, request.blocks, request.partition_key
    )


def app_version_for(plan: CasePlan) -> str:
    return f"wio-{plan.rung_id}-{plan.case_id}"


def status_rows_from_engine(engine: Any, queue_name: str) -> list[dict[str, Any]]:
    cols = SystemSchema.workflow_status.c
    with engine.begin() as conn:
        rows = conn.execute(
            sa.select(
                cols.workflow_uuid,
                cols.status,
                cols.queue_name,
                cols.deduplication_id,
                cols.queue_partition_key,
                cols.priority,
                cols.created_at,
                cols.updated_at,
                cols.started_at_epoch_ms,
                cols.delay_until_epoch_ms,
                cols.completed_at,
            )
            .select_from(SystemSchema.workflow_status)
            .where(cols.queue_name == queue_name)
            .order_by(cols.created_at.asc())
        ).mappings()
        return [dict(row) for row in rows]


def workflow_status_row(
    workflow_id: str,
    *,
    queue_name: str,
    created_at: int,
    started_at_epoch_ms: int,
    status: str = WorkflowStatusString.SUCCESS.value,
    rate_limited: bool,
    partition_key: str | None = None,
) -> dict[str, Any]:
    return {
        "workflow_uuid": workflow_id,
        "status": status,
        "name": "rate_limit_plan_probe",
        "authenticated_user": None,
        "assumed_role": None,
        "authenticated_roles": None,
        "output": None,
        "error": None,
        "executor_id": "wio-rate-limit-plan",
        "created_at": created_at,
        "updated_at": created_at,
        "application_version": "wio-rate-limit-plan",
        "application_id": "wio-qcc-rate-limit-plan",
        "class_name": None,
        "config_name": None,
        "recovery_attempts": 1,
        "queue_name": queue_name,
        "workflow_timeout_ms": None,
        "workflow_deadline_epoch_ms": None,
        "started_at_epoch_ms": started_at_epoch_ms,
        "deduplication_id": None,
        "inputs": None,
        "priority": 0,
        "queue_partition_key": partition_key,
        "forked_from": None,
        "was_forked_from": False,
        "owner_xid": None,
        "parent_workflow_id": None,
        "serialization": None,
        "delay_until_epoch_ms": None,
        "rate_limited": rate_limited,
        "completed_at": created_at,
        "attributes": None,
        "schedule_name": None,
    }


def insert_workflow_status_rows(conn: Any, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    for index in range(0, len(rows), 5000):
        conn.execute(sa.insert(SystemSchema.workflow_status), rows[index : index + 5000])


def insert_generated_workflow_status_rows(
    conn: Any,
    *,
    count: int,
    id_prefix: str,
    queue_name: str,
    created_at: int,
    started_expr: str,
    started_at: int,
    rate_limited: bool,
    partition_key: str | None,
    queue_suffix_mod: int | None = None,
) -> None:
    if count <= 0:
        return
    queue_expr = ":queue_name" if queue_suffix_mod is None else "(:queue_name || (gs % :queue_suffix_mod)::text)"
    conn.execute(
        sa.text(
            f"""
            INSERT INTO "dbos"."workflow_status" (
                workflow_uuid,
                status,
                name,
                authenticated_user,
                assumed_role,
                authenticated_roles,
                output,
                error,
                executor_id,
                created_at,
                updated_at,
                application_version,
                application_id,
                class_name,
                config_name,
                recovery_attempts,
                queue_name,
                workflow_timeout_ms,
                workflow_deadline_epoch_ms,
                started_at_epoch_ms,
                deduplication_id,
                inputs,
                priority,
                queue_partition_key,
                forked_from,
                was_forked_from,
                owner_xid,
                parent_workflow_id,
                serialization,
                delay_until_epoch_ms,
                rate_limited,
                completed_at,
                attributes,
                schedule_name
            )
            SELECT
                :id_prefix || gs::text,
                'SUCCESS',
                'rate_limit_plan_probe',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                'wio-rate-limit-plan',
                :created_at - gs,
                :created_at - gs,
                'wio-rate-limit-plan',
                'wio-qcc-rate-limit-plan',
                NULL,
                NULL,
                1,
                {queue_expr},
                NULL,
                NULL,
                {started_expr},
                NULL,
                NULL,
                0,
                CAST(:partition_key AS text),
                NULL,
                false,
                NULL,
                NULL,
                NULL,
                NULL,
                :rate_limited,
                :created_at - gs,
                NULL::jsonb,
                NULL
            FROM generate_series(0, :count - 1) AS gs
            """
        ),
        {
            "count": count,
            "id_prefix": id_prefix,
            "queue_name": queue_name,
            "queue_suffix_mod": queue_suffix_mod,
            "created_at": created_at,
            "started_at": started_at,
            "rate_limited": rate_limited,
            "partition_key": partition_key,
        },
    )


def populate_rate_limit_plan_rows(dbos: Any, plan: RateLimitPlan) -> dict[str, Any]:
    period_ms = int(plan.limiter_period_sec * 1000)
    now = now_ms()
    recent_started = now - max(1000, period_ms // 4)
    old_started = now - period_ms - 10_000
    other_partition = "partition-cold"
    inserted_rows = (
        plan.non_rate_limited_rows
        + plan.old_rate_limited_rows
        + plan.recent_target_rows
        + plan.recent_other_queue_rows
        + plan.recent_other_partition_rows
    )
    event(
        "rate_limit_population_started",
        case_id=plan.case_id,
        total_rows=inserted_rows,
        non_rate_limited_rows=plan.non_rate_limited_rows,
        old_rate_limited_rows=plan.old_rate_limited_rows,
        recent_target_rows=plan.recent_target_rows,
        recent_other_queue_rows=plan.recent_other_queue_rows,
        recent_other_partition_rows=plan.recent_other_partition_rows,
    )
    with dbos._sys_db.engine.begin() as conn:
        insert_generated_workflow_status_rows(
            conn,
            count=plan.non_rate_limited_rows,
            id_prefix=f"{plan.database_prefix}-non-rate-",
            queue_name=f"{plan.queue_name}-noise-",
            queue_suffix_mod=97,
            created_at=now,
            started_expr="(:started_at - (gs % 1000))",
            started_at=recent_started,
            rate_limited=False,
            partition_key=None,
        )
        insert_generated_workflow_status_rows(
            conn,
            count=plan.old_rate_limited_rows,
            id_prefix=f"{plan.database_prefix}-old-rate-",
            queue_name=plan.queue_name,
            created_at=now,
            started_expr="(:started_at - gs)",
            started_at=old_started,
            rate_limited=True,
            partition_key=plan.partition_key,
        )
        insert_generated_workflow_status_rows(
            conn,
            count=plan.recent_target_rows,
            id_prefix=f"{plan.database_prefix}-recent-target-",
            queue_name=plan.queue_name,
            created_at=now,
            started_expr="(:started_at + gs)",
            started_at=recent_started,
            rate_limited=True,
            partition_key=plan.partition_key,
        )
        insert_generated_workflow_status_rows(
            conn,
            count=plan.recent_other_queue_rows,
            id_prefix=f"{plan.database_prefix}-recent-other-queue-",
            queue_name=f"{plan.queue_name}-other-",
            queue_suffix_mod=29,
            created_at=now,
            started_expr="(:started_at + gs)",
            started_at=recent_started,
            rate_limited=True,
            partition_key=plan.partition_key,
        )
        insert_generated_workflow_status_rows(
            conn,
            count=plan.recent_other_partition_rows,
            id_prefix=f"{plan.database_prefix}-recent-other-partition-",
            queue_name=plan.queue_name,
            created_at=now,
            started_expr="(:started_at + gs)",
            started_at=recent_started,
            rate_limited=True,
            partition_key=other_partition,
        )
        conn.execute(sa.text('ANALYZE "dbos"."workflow_status"'))
    event("rate_limit_population_finished", case_id=plan.case_id, total_rows=inserted_rows)
    return {
        "now_ms": now,
        "recent_started_at": recent_started,
        "old_started_at": old_started,
        "period_ms": period_ms,
        "inserted_rows": inserted_rows,
        "expected_recent_count": plan.recent_target_rows,
    }


def rate_limit_count_query(plan: RateLimitPlan, start_time_ms: int) -> Any:
    query = (
        sa.select(sa.func.count())
        .select_from(SystemSchema.workflow_status)
        .where(SystemSchema.workflow_status.c.queue_name == plan.queue_name)
        .where(SystemSchema.workflow_status.c.rate_limited == True)
        .where(
            SystemSchema.workflow_status.c.status.notin_(
                [
                    WorkflowStatusString.ENQUEUED.value,
                    WorkflowStatusString.DELAYED.value,
                ]
            )
        )
        .where(
            SystemSchema.workflow_status.c.started_at_epoch_ms
            > start_time_ms - int(plan.limiter_period_sec * 1000)
        )
    )
    if plan.partition_key is not None:
        query = query.where(SystemSchema.workflow_status.c.queue_partition_key == plan.partition_key)
    return query


def index_definitions(conn: Any) -> list[dict[str, Any]]:
    rows = conn.execute(
        sa.text(
            """
            SELECT schemaname, indexname, indexdef
            FROM pg_indexes
            WHERE tablename = 'workflow_status'
            ORDER BY schemaname, indexname
            """
        )
    ).mappings()
    return [dict(row) for row in rows]


def normalize_explain_json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, str):
        raw = json.loads(raw)
    if isinstance(raw, list):
        return raw[0]
    return raw


def flatten_plan_nodes(node: dict[str, Any]) -> list[dict[str, Any]]:
    nodes = [node]
    for child in node.get("Plans", []) or []:
        nodes.extend(flatten_plan_nodes(child))
    return nodes


def rate_limit_plan_oracle(dbos: Any, plan: RateLimitPlan, population: dict[str, Any]) -> dict[str, Any]:
    query = rate_limit_count_query(plan, population["now_ms"])
    with dbos._sys_db.engine.begin() as conn:
        definitions = index_definitions(conn)
        rate_index_defs = [
            row
            for row in definitions
            if row["indexname"] == "idx_workflow_status_rate_limited"
            and "rate_limited = true" in row["indexdef"].lower()
        ]
        invariant(
            "rate_limit_partial_index_present",
            bool(rate_index_defs),
            matching_indexes=rate_index_defs,
            all_indexes=definitions,
        )
        observed_count = conn.execute(query).scalar_one()
        compiled = query.compile(dialect=conn.dialect, compile_kwargs={"literal_binds": True})
        actual_schema = quote_ident(dbos._sys_db.schema or "dbos")
        query_sql = str(compiled).replace(SCHEMA_PLACEHOLDER, actual_schema)
        explain_raw = conn.execute(sa.text(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query_sql}")).scalar_one()
    explain = normalize_explain_json(explain_raw)
    nodes = flatten_plan_nodes(explain["Plan"])
    index_nodes = [
        {
            "node_type": node.get("Node Type"),
            "index_name": node.get("Index Name"),
            "relation_name": node.get("Relation Name"),
            "actual_rows": node.get("Actual Rows"),
            "shared_hit_blocks": node.get("Shared Hit Blocks"),
            "shared_read_blocks": node.get("Shared Read Blocks"),
        }
        for node in nodes
        if node.get("Index Name")
    ]
    seq_scans = [
        {
            "node_type": node.get("Node Type"),
            "relation_name": node.get("Relation Name"),
            "actual_rows": node.get("Actual Rows"),
            "plan_rows": node.get("Plan Rows"),
        }
        for node in nodes
        if node.get("Node Type") == "Seq Scan" and node.get("Relation Name") == "workflow_status"
    ]
    uses_rate_index = any(node.get("index_name") == "idx_workflow_status_rate_limited" for node in index_nodes)
    invariant(
        "rate_limit_query_count_matches_model",
        observed_count == population["expected_recent_count"],
        observed_count=observed_count,
        expected_count=population["expected_recent_count"],
        query_sql=query_sql,
        partition_key=plan.partition_key,
    )
    invariant(
        "rate_limit_query_uses_partial_index",
        uses_rate_index,
        index_nodes=index_nodes,
        query_sql=query_sql,
    )
    invariant(
        "rate_limit_query_avoids_workflow_status_seq_scan",
        not seq_scans,
        seq_scans=seq_scans,
        index_nodes=index_nodes,
        total_inserted_rows=population["inserted_rows"],
    )
    return {
        "query_sql": query_sql,
        "observed_count": observed_count,
        "expected_count": population["expected_recent_count"],
        "index_definitions": definitions,
        "index_nodes": index_nodes,
        "seq_scans": seq_scans,
        "planning_time_ms": explain.get("Planning Time"),
        "execution_time_ms": explain.get("Execution Time"),
        "top_plan": explain["Plan"],
    }


def advance_recent_target_rows_out_of_limiter_window(dbos: Any, plan: RateLimitPlan, old_started_at: int) -> None:
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.update(SystemSchema.workflow_status)
            .where(SystemSchema.workflow_status.c.workflow_uuid.like(f"{plan.database_prefix}-recent-target-%"))
            .values(started_at_epoch_ms=old_started_at)
        )
        conn.execute(sa.text('ANALYZE "dbos"."workflow_status"'))


def refresh_recent_target_rows_into_limiter_window(dbos: Any, plan: RateLimitPlan) -> None:
    # The population-time started_at ages out of the limiter window while the
    # EXPLAIN/ANALYZE invariants run (minutes on 24k rows); re-touch just
    # before the live hold check so the held/admitted assertion is about the
    # limiter, not about setup latency.
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.update(SystemSchema.workflow_status)
            .where(SystemSchema.workflow_status.c.workflow_uuid.like(f"{plan.database_prefix}-recent-target-%"))
            .values(started_at_epoch_ms=now_ms() - 1000)
        )


def rate_limit_functional_sanity(dbos: Any, plan: RateLimitPlan, population: dict[str, Any]) -> dict[str, Any]:
    if not plan.functional_sanity:
        return {}
    global _case_id, _ledger_path, _active_count, _max_active_count
    _case_id = plan.case_id
    _ledger_rows.clear()
    _release_gate.set()
    _active_count = 0
    _max_active_count = 0
    _active_partition_counts.clear()
    _max_active_partition_counts.clear()

    request = RequestSpec(
        key=f"{plan.case_id}-live-limiter",
        workflow_id=f"{plan.database_prefix}-live-limiter-workflow",
        expected_result=f"rate-limit-live-{plan.seed}",
        partition_key=plan.partition_key,
    )
    refresh_recent_target_rows_into_limiter_window(dbos, plan)
    handle = enqueue_request(plan.queue_name, request)
    time.sleep(1.0)
    held_status = handle.get_status()
    with _ledger_lock:
        held_ledger = list(_ledger_rows)
    invariant(
        "rate_limit_live_holds_extra_start_at_limit",
        held_status.status == WorkflowStatusString.ENQUEUED.value and not held_ledger,
        status=held_status.status,
        ledger_rows=held_ledger,
        limiter_limit=plan.limiter_limit,
        expected_recent_count=population["expected_recent_count"],
    )

    advance_recent_target_rows_out_of_limiter_window(dbos, plan, population["old_started_at"])
    result = handle.get_result()
    final_status = handle.get_status()
    with _ledger_lock:
        final_ledger = list(_ledger_rows)
    invariant(
        "rate_limit_live_progresses_after_window_advances",
        result == request.expected_result and final_status.status == WorkflowStatusString.SUCCESS.value,
        result=result,
        expected_result=request.expected_result,
        final_status=final_status.status,
        ledger_rows=final_ledger,
    )
    return {
        "request": asdict(request),
        "held_status": held_status.status,
        "held_ledger_rows": held_ledger,
        "result": result,
        "final_status": final_status.status,
        "final_ledger_rows": final_ledger,
    }


def run_rate_limit_plan_case(plan: RateLimitPlan, artifact_dir: Path) -> dict[str, Any]:
    global _ledger_path
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _ledger_path = artifact_dir / "rate-limit-functional-ledger.jsonl"
    if _ledger_path.exists():
        _ledger_path.unlink()
    write_json(artifact_dir / "request-plan.json", asdict(plan))
    app_url, sys_url, masked_admin = prepare_databases(plan.database_prefix, artifact_dir)
    dbos: Any | None = None
    try:
        DBOS.destroy(destroy_registry=True)
        dbos = DBOS(config=make_rate_limit_config(plan, app_url, sys_url))
        DBOS.workflow()(workflow_body)
        DBOS.launch()
        event("case_started", case_id=plan.case_id, seed=plan.seed, schedule=plan.schedule)
        population = populate_rate_limit_plan_rows(dbos, plan)
        plan_evidence = rate_limit_plan_oracle(dbos, plan, population)
        if plan.functional_sanity:
            DBOS.register_queue(
                plan.queue_name,
                concurrency=1,
                limiter={"limit": plan.limiter_limit, "period": plan.limiter_period_sec},
                priority_enabled=True,
                partition_queue=plan.partition_key is not None,
                polling_interval_sec=0.05,
                on_conflict="always_update",
            )
        functional_evidence = rate_limit_functional_sanity(dbos, plan, population)
        outcome = {
            "classification": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "prompt_event": PROMPT_PATHS[plan.rung_id],
            "case_id": plan.case_id,
            "seed": plan.seed,
            "schedule": plan.schedule,
            "fault_model": plan.fault_model,
            "queue_name": plan.queue_name,
            "database_prefix": plan.database_prefix,
            "redacted_admin_url": masked_admin,
            "population": population,
            "plan_evidence": plan_evidence,
            "functional_evidence": functional_evidence,
        }
        write_json(artifact_dir / "result.json", outcome)
        event("case_passed", case_id=plan.case_id)
        return outcome
    finally:
        try:
            DBOS.destroy(destroy_registry=True, workflow_completion_timeout_sec=2)
        finally:
            drop_databases(plan.database_prefix)


def launch_dbos_app(plan: CasePlan, app_url: str, sys_url: str, *, queue_conflict: str) -> Any:
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=make_config(plan, app_url, sys_url))
    DBOS.workflow()(workflow_body)
    DBOS.launch()
    DBOS.register_queue(
        plan.queue_name,
        concurrency=plan.initial_concurrency,
        worker_concurrency=plan.initial_worker_concurrency,
        limiter=plan.initial_limiter,
        priority_enabled=plan.initial_priority_enabled,
        partition_queue=plan.partition_queue,
        polling_interval_sec=plan.polling_interval_sec,
        on_conflict=queue_conflict,
    )
    return dbos


def apply_live_updates(queue: Any, plan: CasePlan) -> None:
    updates: dict[str, Any] = {}
    if plan.updated_concurrency is not None:
        queue.set_concurrency(plan.updated_concurrency)
        updates["concurrency"] = plan.updated_concurrency
    if plan.updated_worker_concurrency is not None:
        queue.set_worker_concurrency(plan.updated_worker_concurrency)
        updates["worker_concurrency"] = plan.updated_worker_concurrency
    if plan.updated_limiter is not None:
        queue.set_limiter(plan.updated_limiter)
        updates["limiter"] = plan.updated_limiter
    if plan.updated_priority_enabled is not None:
        queue.set_priority_enabled(plan.updated_priority_enabled)
        updates["priority_enabled"] = plan.updated_priority_enabled
    if plan.updated_polling_interval_sec is not None:
        queue.set_polling_interval_sec(plan.updated_polling_interval_sec)
        updates["polling_interval_sec"] = plan.updated_polling_interval_sec
    if updates:
        event("queue_live_update_applied", case_id=plan.case_id, updates=updates)


def collect_results(handles: dict[str, Any], requests: list[RequestSpec]) -> dict[str, Any]:
    results: dict[str, Any] = {}
    for request in requests:
        handle = handles[request.key]
        results[request.key] = {
            "workflow_id": handle.get_workflow_id(),
            "result": handle.get_result(),
            "status": handle.get_status().status,
        }
    return results


async def collect_results_async(handles: dict[str, Any], requests: list[RequestSpec]) -> dict[str, Any]:
    results: dict[str, Any] = {}
    for request in requests:
        handle = handles[request.key]
        result = await handle.get_result(polling_interval_sec=0.05)
        status = await handle.get_status()
        results[request.key] = {
            "workflow_id": handle.get_workflow_id(),
            "result": result,
            "status": status.status,
        }
    return results


def assert_terminal_conservation(requests: list[RequestSpec], results: dict[str, Any]) -> None:
    expected_keys = {request.key for request in requests}
    terminal_keys = {
        key for key, item in results.items() if item["status"] == TERMINAL_SUCCESS and item["result"] is not None
    }
    invariant(
        "accepted_request_keys_equal_terminal_success_keys",
        expected_keys == terminal_keys,
        expected=sorted(expected_keys),
        actual=sorted(terminal_keys),
    )
    for request in requests:
        invariant(
            f"handle_result_matches_ledger_{request.key}",
            results[request.key]["result"] == request.expected_result,
            expected=request.expected_result,
            actual=results[request.key]["result"],
        )


def assert_delay_windows(plan: CasePlan) -> None:
    starts = {
        row["request_key"]: row["started_at_ms"]
        for row in _ledger_rows
        if row.get("event") == "start"
    }
    for request in plan.requests:
        if request.delay_seconds is None:
            continue
        accepted_at = getattr(request, "_accepted_at_ms", None)
        if accepted_at is None:
            continue
        eligible_at = accepted_at + int(request.delay_seconds * 1000)
        actual = starts.get(request.key)
        invariant(
            f"delayed_request_not_started_before_eligible_{request.key}",
            actual is not None and actual >= eligible_at - 100,
            request_key=request.key,
            eligible_at_ms=eligible_at,
            started_at_ms=actual,
            tolerance_ms=100,
        )


def assert_priority_order(plan: CasePlan) -> None:
    if plan.partition_queue:
        return
    if plan.case_id != "case-002" and plan.schedule != "priority-enable-before-release":
        return
    starts = [
        (
            request.priority,
            request.key,
            next(
                row["started_at_ms"]
                for row in _ledger_rows
                if row.get("event") == "start" and row.get("request_key") == request.key
            ),
        )
        for request in plan.requests
        if not request.blocks
    ]
    actual_keys = [key for _, key, _ in sorted(starts, key=lambda item: item[2])]
    expected_keys = [
        key
        for _, key, _ in sorted(starts, key=lambda item: (item[0] if item[0] is not None else 999, item[2]))
    ]
    invariant(
        "priority_after_release_order_matches_model",
        actual_keys == expected_keys,
        expected=expected_keys,
        actual=actual_keys,
        starts=starts,
    )


def assert_partition_priority_order(plan: CasePlan) -> None:
    if not plan.partition_queue or plan.case_id != "case-003":
        return
    by_key = {
        row["request_key"]: row["started_at_ms"]
        for row in _ledger_rows
        if row.get("event") == "start"
    }
    followers = [
        request
        for request in plan.requests
        if request.partition_key == plan.requests[0].partition_key and not request.blocks
    ]
    actual = [request.key for request in sorted(followers, key=lambda request: by_key[request.key])]
    expected = [request.key for request in sorted(followers, key=lambda request: request.priority or 999)]
    invariant(
        "partition_local_priority_after_release_matches_model",
        actual == expected,
        expected=expected,
        actual=actual,
        starts={request.key: by_key.get(request.key) for request in followers},
    )


def assert_rate_limit(plan: CasePlan) -> None:
    if plan.case_id != "case-003" or plan.initial_limiter is None:
        return
    limiter = plan.updated_limiter if plan.rung_id == RUNG_003_ID and plan.updated_limiter else plan.initial_limiter
    limit = int(limiter["limit"])
    period_ms = int(float(limiter["period"]) * 1000)
    starts = sorted(
        row["started_at_ms"]
        for row in _ledger_rows
        if row.get("event") == "start"
    )
    worst = 0
    for start in starts:
        count = sum(1 for item in starts if start <= item < start + period_ms)
        worst = max(worst, count)
    invariant(
        "rate_limit_start_window_within_model",
        worst <= limit,
        limit=limit,
        period_ms=period_ms,
        worst_observed=worst,
        starts=starts,
    )


def assert_concurrency(plan: CasePlan) -> None:
    cap = plan.initial_concurrency if plan.schedule == "concurrency-decrease" else plan.updated_concurrency or plan.initial_concurrency
    if plan.partition_queue:
        invariant(
            "observed_partition_simultaneous_starts_within_modeled_cap",
            all(value <= cap for value in _max_active_partition_counts.values()),
            max_active_by_partition=_max_active_partition_counts,
            modeled_cap_per_partition=cap,
        )
        return
    invariant(
        "observed_simultaneous_starts_within_modeled_cap",
        _max_active_count <= cap,
        max_active_seen=_max_active_count,
        modeled_cap=cap,
    )


def wait_until_terminal(handle: Any, timeout_sec: float) -> Any:
    deadline = time.time() + timeout_sec
    last_status = None
    while time.time() < deadline:
        status = handle.get_status()
        last_status = status.status
        if status.status not in ACTIVE_STATUSES:
            return status
        time.sleep(0.05)
    raise WorkloadFailure(f"workflow did not reach terminal state before timeout; last_status={last_status}")


def assert_partition_pre_release(plan: CasePlan, handles: dict[str, Any], dbos: Any) -> dict[str, Any]:
    if not plan.partition_queue:
        return {}
    blocker = plan.requests[0]
    blocked_partition = blocker.partition_key
    normal_requests = [
        request
        for request in plan.requests
        if request.partition_key != blocked_partition and not request.blocks
    ]
    blocked_followers = [
        request
        for request in plan.requests
        if request.partition_key == blocked_partition and not request.blocks
    ]
    before_release_results: dict[str, Any] = {}
    for request in normal_requests:
        status = wait_until_terminal(handles[request.key], timeout_sec=8)
        before_release_results[request.key] = {
            "workflow_id": handles[request.key].get_workflow_id(),
            "status": status.status,
            "result": handles[request.key].get_result(),
        }
    rows = status_rows(dbos, plan.queue_name)
    follower_starts = [
        row
        for row in _ledger_rows
        if row.get("event") == "start" and row.get("request_key") in {request.key for request in blocked_followers}
    ]
    blocker_status = handles[blocker.key].get_status().status
    invariant(
        "partition_blocker_pending_before_release",
        blocker_status == WorkflowStatusString.PENDING.value,
        blocker=blocker.key,
        status=blocker_status,
    )
    invariant(
        "normal_partition_terminal_before_blocked_release",
        len(before_release_results) == len(normal_requests)
        and all(item["status"] == TERMINAL_SUCCESS for item in before_release_results.values()),
        normal_results=before_release_results,
        blocked_partition=blocked_partition,
    )
    invariant(
        "blocked_partition_followers_do_not_start_before_release",
        not follower_starts,
        blocked_followers=[request.key for request in blocked_followers],
        follower_starts=follower_starts,
    )
    invariant(
        "partition_keys_recorded_for_all_modeled_requests",
        all(request.partition_key for request in plan.requests)
        and all(row.get("queue_partition_key") for row in rows),
        modeled={request.key: request.partition_key for request in plan.requests},
        status_rows=rows,
    )
    return before_release_results


def assert_no_duplicate_execution(duplicate: RequestSpec | None, accepted: RequestSpec | None) -> None:
    if duplicate is None or accepted is None:
        return
    rows = [row for row in _ledger_rows if row.get("event") == "start"]
    duplicate_rows = [row for row in rows if row["request_key"] == duplicate.key]
    accepted_rows = [row for row in rows if row["request_key"] == accepted.key]
    invariant(
        "rejected_duplicate_has_no_execution_ledger_row",
        len(duplicate_rows) == 0,
        duplicate_key=duplicate.key,
        rows=duplicate_rows,
    )
    invariant(
        "accepted_dedupe_key_executes_exactly_once",
        len(accepted_rows) == 1,
        accepted_key=accepted.key,
        rows=accepted_rows,
    )


def poll_cleanup(dbos: Any, queue_name: str, timeout_sec: float = 10.0) -> list[dict[str, Any]]:
    deadline = time.time() + timeout_sec
    last_rows: list[dict[str, Any]] = []
    while time.time() < deadline:
        last_rows = active_queue_rows(dbos, queue_name)
        if not last_rows:
            return []
        time.sleep(0.5)
    return last_rows


def uses_async_partition_runner(plan: CasePlan) -> bool:
    return plan.rung_id == RUNG_007_ID


async def run_async_partition_case(plan: CasePlan, artifact_dir: Path) -> dict[str, Any]:
    global _case_id, _ledger_path, _active_count, _max_active_count, _async_release_gate
    _case_id = plan.case_id
    _ledger_rows.clear()
    _active_count = 0
    _max_active_count = 0
    _active_partition_counts.clear()
    _max_active_partition_counts.clear()
    _async_release_gate = asyncio.Event()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _ledger_path = artifact_dir / "execution-ledger.jsonl"
    if _ledger_path.exists():
        _ledger_path.unlink()
    write_json(artifact_dir / "request-plan.json", asdict(plan))
    app_url, sys_url, masked_admin = prepare_databases(plan.database_prefix, artifact_dir)
    dbos: Any | None = None
    try:
        DBOS.destroy(destroy_registry=True)
        dbos = DBOS(config=make_config(plan, app_url, sys_url))
        DBOS.workflow()(async_partition_workflow_body)

        if plan.case_id == "case-002":
            child_request = plan.requests[1]

            async def async_partition_parent_workflow(
                request_key: str,
                expected_result: str,
                should_block: bool,
                partition_key: str | None = None,
            ) -> dict[str, Any]:
                append_ledger(
                    {
                        "event": "start",
                        "request_key": request_key,
                        "partition_key": partition_key,
                        "workflow_id": DBOS.workflow_id,
                        "started_at_ms": now_ms(),
                        "thread": threading.current_thread().name,
                    }
                )
                with SetWorkflowID(child_request.workflow_id):
                    child_handle = await DBOS.enqueue_workflow_async(
                        plan.queue_name,
                        async_partition_workflow_body,
                        child_request.key,
                        child_request.expected_result,
                        False,
                        child_request.partition_key,
                    )
                child_result = await child_handle.get_result(polling_interval_sec=0.05)
                child_status = await child_handle.get_status()
                mark_ledger_complete(request_key, partition_key)
                return {
                    "parent_result": expected_result,
                    "child_workflow_id": child_handle.get_workflow_id(),
                    "child_result": child_result,
                    "child_status": child_status.status,
                }

            DBOS.workflow()(async_partition_parent_workflow)
        else:
            async_partition_parent_workflow = None

        DBOS.launch()
        queue = await DBOS.register_queue_async(
            plan.queue_name,
            concurrency=plan.initial_concurrency,
            worker_concurrency=plan.initial_worker_concurrency,
            limiter=plan.initial_limiter,
            priority_enabled=plan.initial_priority_enabled,
            partition_queue=plan.partition_queue,
            polling_interval_sec=plan.polling_interval_sec,
            on_conflict="always_update",
        )
        config_before = await queue_config_snapshot_async(queue)
        event("async_partition_case_started", case_id=plan.case_id, seed=plan.seed, schedule=plan.schedule)

        handles: dict[str, Any] = {}
        status_before_release: list[dict[str, Any]] = []
        config_after_update: dict[str, Any] | None = None
        saturation_counts: dict[str, int] = {}
        child_result: dict[str, Any] | None = None

        if plan.case_id == "case-002":
            parent_request = plan.requests[0]
            child_request = plan.requests[1]
            if async_partition_parent_workflow is None:
                raise WorkloadFailure("parent workflow was not registered")
            handles[parent_request.key] = await enqueue_request_async(
                plan.queue_name, parent_request, async_partition_parent_workflow
            )
            parent_result = await handles[parent_request.key].get_result(polling_interval_sec=0.05)
            parent_status = await handles[parent_request.key].get_status()
            child_handle = await DBOS.retrieve_workflow_async(child_request.workflow_id)
            child_public_result = await child_handle.get_result(polling_interval_sec=0.05)
            child_public_status = await child_handle.get_status()
            child_result = {
                "workflow_id": child_handle.get_workflow_id(),
                "result": child_public_result,
                "status": child_public_status.status,
            }
            results = {
                parent_request.key: {
                    "workflow_id": handles[parent_request.key].get_workflow_id(),
                    "result": parent_result,
                    "status": parent_status.status,
                },
                child_request.key: child_result,
            }
            status_after_terminal = status_rows(dbos, plan.queue_name)
            child_rows = [
                row
                for row in status_after_terminal
                if row["workflow_uuid"] == child_request.workflow_id
            ]
            invariant(
                "async_child_enqueued_from_dequeued_parent_inherits_partition_key",
                len(child_rows) == 1 and child_rows[0]["queue_partition_key"] == parent_request.partition_key,
                parent_partition=parent_request.partition_key,
                child_rows=child_rows,
                parent_result=parent_result,
            )
            invariant(
                "async_parent_child_public_results_match_model",
                parent_status.status == TERMINAL_SUCCESS
                and parent_result["parent_result"] == parent_request.expected_result
                and parent_result["child_result"] == child_request.expected_result
                and child_public_status.status == TERMINAL_SUCCESS
                and child_public_result == child_request.expected_result,
                parent_result=parent_result,
                child_result=child_result,
            )
        else:
            for request in plan.requests:
                handles[request.key] = await enqueue_request_async(
                    plan.queue_name, request, async_partition_workflow_body
                )
            partitions = sorted({request.partition_key for request in plan.requests if request.partition_key})
            if plan.case_id == "case-003":
                initial_expected = {partition: 1 for partition in partitions}
                await wait_for_partition_start_counts(
                    initial_expected,
                    timeout_sec=10,
                    cap_per_partition=1,
                )
                invariant(
                    "async_partitions_reached_initial_worker_concurrency_before_update",
                    all(_max_active_partition_counts.get(partition, 0) == 1 for partition in partitions),
                    expected=initial_expected,
                    max_active_by_partition=dict(_max_active_partition_counts),
                )
                await queue.set_worker_concurrency_async(plan.updated_worker_concurrency)
                config_after_update = await queue_config_snapshot_async(queue)
                event(
                    "async_partition_worker_concurrency_updated",
                    case_id=plan.case_id,
                    worker_concurrency=plan.updated_worker_concurrency,
                )
                modeled_cap = int(plan.updated_worker_concurrency or 0)
            else:
                modeled_cap = int(plan.initial_worker_concurrency or 0)
            expected = {partition: modeled_cap for partition in partitions}
            saturation_counts = await wait_for_partition_start_counts(
                expected,
                timeout_sec=12,
                cap_per_partition=modeled_cap,
            )
            with _active_lock:
                active_at_saturation = dict(_active_partition_counts)
                total_active_at_saturation = _active_count
            invariant(
                "async_all_partitions_reached_modeled_saturation_before_release",
                all(active_at_saturation.get(partition, 0) == modeled_cap for partition in partitions)
                and total_active_at_saturation == modeled_cap * len(partitions),
                expected_active_by_partition=expected,
                active_by_partition=active_at_saturation,
                total_active=total_active_at_saturation,
            )
            status_before_release = status_rows(dbos, plan.queue_name)
            _async_release_gate.set()
            event("async_partition_release_gate_set", case_id=plan.case_id)
            results = await collect_results_async(handles, plan.requests)
            status_after_terminal = status_rows(dbos, plan.queue_name)
            assert_terminal_conservation(plan.requests, results)

        invariant(
            "async_partitioned_queue_rows_have_non_null_partition_keys",
            all(row.get("queue_partition_key") for row in status_after_terminal),
            status_rows=status_after_terminal,
        )
        cleanup_rows = poll_cleanup(dbos, plan.queue_name)
        invariant(
            "async_no_active_queue_rows_after_terminal_cleanup_poll",
            not cleanup_rows,
            active_rows=cleanup_rows,
        )
        invariant(
            "async_partition_running_window_never_exceeded_modeled_cap",
            all(
                value <= int((plan.updated_worker_concurrency if plan.updated_worker_concurrency is not None else plan.initial_worker_concurrency) or 0)
                for value in _max_active_partition_counts.values()
            ),
            max_active_by_partition=dict(_max_active_partition_counts),
            modeled_cap=plan.updated_worker_concurrency if plan.updated_worker_concurrency is not None else plan.initial_worker_concurrency,
        )

        outcome = {
            "classification": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "prompt_event": PROMPT_PATHS[plan.rung_id],
            "case_id": plan.case_id,
            "seed": plan.seed,
            "schedule": plan.schedule,
            "fault_model": plan.fault_model,
            "queue_name": plan.queue_name,
            "database_prefix": plan.database_prefix,
            "redacted_admin_url": masked_admin,
            "config_before": config_before,
            "config_after_update": config_after_update,
            "status_before_release": status_before_release,
            "status_after_terminal": status_after_terminal,
            "results": results,
            "child_result": child_result,
            "saturation_counts": saturation_counts,
            "ledger_rows": list(_ledger_rows),
            "max_active_seen": _max_active_count,
            "max_active_by_partition": dict(_max_active_partition_counts),
            "cleanup_rows": cleanup_rows,
        }
        write_json(artifact_dir / "result.json", outcome)
        event("case_passed", case_id=plan.case_id)
        return outcome
    finally:
        if _async_release_gate is not None:
            _async_release_gate.set()
        try:
            DBOS.destroy(destroy_registry=True, workflow_completion_timeout_sec=2)
        finally:
            _async_release_gate = None
            drop_databases(plan.database_prefix)


def uses_relaunch_runner(plan: CasePlan) -> bool:
    return plan.rung_id == RUNG_004_ID or (
        plan.rung_id == RUNG_005_ID
        and plan.schedule
        in {
            "stop-after-acceptance",
            "stop-while-delayed",
            "duplicate-then-stop",
            "partition-stop-release",
            "config-stop-relaunch",
            "stop-before-cleanup",
        }
    )


def run_relaunch_case(plan: CasePlan, artifact_dir: Path) -> dict[str, Any]:
    global _case_id, _ledger_path, _active_count, _max_active_count
    _case_id = plan.case_id
    _ledger_rows.clear()
    _release_gate.clear()
    _active_count = 0
    _max_active_count = 0
    _active_partition_counts.clear()
    _max_active_partition_counts.clear()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _ledger_path = artifact_dir / "execution-ledger.jsonl"
    if _ledger_path.exists():
        _ledger_path.unlink()
    write_json(artifact_dir / "request-plan.json", asdict(plan))
    app_url, sys_url, masked_admin = prepare_databases(plan.database_prefix, artifact_dir)
    dbos: Any | None = None
    client: DBOSClient | None = None
    app_version = app_version_for(plan)
    accepted_at: dict[str, int] = {}
    duplicate_result: dict[str, Any] | None = None
    status_after_stop_acceptance: list[dict[str, Any]] = []
    status_after_relaunch_start: list[dict[str, Any]] = []
    status_before_cleanup_restart: list[dict[str, Any]] | None = None
    config_after_update: dict[str, Any] | None = None
    try:
        dbos = launch_dbos_app(plan, app_url, sys_url, queue_conflict="always_update")
        queue = DBOS.retrieve_queue(plan.queue_name)
        if queue is None:
            raise WorkloadFailure(f"queue not found after initial launch: {plan.queue_name}")
        config_before = queue_config_snapshot(dbos, plan.queue_name)
        if plan.updated_concurrency is not None or plan.updated_limiter is not None:
            apply_live_updates(queue, plan)
        config_before_stop = queue_config_snapshot(dbos, plan.queue_name)
        event("app_initial_launch_ready", case_id=plan.case_id, seed=plan.seed, schedule=plan.schedule)

        DBOS.destroy(destroy_registry=True, workflow_completion_timeout_sec=0)
        event("app_stopped_before_client_enqueue", case_id=plan.case_id)

        client = DBOSClient(system_database_url=sys_url)
        client_handles: dict[str, Any] = {}
        for request in plan.requests:
            client_handles[request.key] = client_enqueue_request(client, plan, request, app_version)
            accepted_at[request.key] = now_ms()
            object.__setattr__(request, "_accepted_at_ms", accepted_at[request.key])

        if plan.duplicate_request is not None:
            if plan.duplicate_offset_ms:
                time.sleep(plan.duplicate_offset_ms / 1000)
            try:
                client_enqueue_request(client, plan, plan.duplicate_request, app_version)
            except DBOSQueueDeduplicatedError as exc:
                duplicate_result = {
                    "request_key": plan.duplicate_request.key,
                    "workflow_id": plan.duplicate_request.workflow_id,
                    "status": "rejected_duplicate",
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
                event("duplicate_rejected_before_relaunch", case_id=plan.case_id, error_type=type(exc).__name__)
            except Exception as exc:
                if "deduplic" not in str(exc).lower() and "duplicate" not in str(exc).lower():
                    raise
                duplicate_result = {
                    "request_key": plan.duplicate_request.key,
                    "workflow_id": plan.duplicate_request.workflow_id,
                    "status": "rejected_duplicate",
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
                event("duplicate_rejected_before_relaunch", case_id=plan.case_id, error_type=type(exc).__name__)
            else:
                raise WorkloadFailure("duplicate enqueue unexpectedly succeeded before relaunch")

        status_after_stop_acceptance = status_rows_from_engine(client._sys_db.engine, plan.queue_name)
        invariant(
            "accepted_rows_durable_while_app_stopped",
            len(status_after_stop_acceptance) == len(plan.requests),
            expected_request_count=len(plan.requests),
            status_rows=status_after_stop_acceptance,
        )
        invariant(
            "no_request_executed_before_relaunch",
            not _ledger_rows,
            ledger_rows=list(_ledger_rows),
        )

        dbos = launch_dbos_app(plan, app_url, sys_url, queue_conflict="never_update")
        config_after_update = queue_config_snapshot(dbos, plan.queue_name)
        status_after_relaunch_start = status_rows(dbos, plan.queue_name)
        event("app_relaunched", case_id=plan.case_id, status_rows=status_after_relaunch_start)

        handles = {request.key: DBOS.retrieve_workflow(request.workflow_id) for request in plan.requests}
        blocking_requests = [request for request in plan.requests if request.blocks]
        for request in blocking_requests:
            wait_for_started(request.key, timeout_sec=8)

        before_release_results = assert_partition_pre_release(plan, handles, dbos)
        delayed_requests = [request for request in plan.requests if request.delay_seconds is not None]
        if not plan.partition_queue:
            if delayed_requests:
                latest_eligible = max(accepted_at[request.key] + int(request.delay_seconds * 1000) for request in delayed_requests)
                sleep_ms = max(0, latest_eligible + plan.release_after_eligible_ms - now_ms())
                time.sleep(sleep_ms / 1000)
            elif plan.release_after_eligible_ms:
                time.sleep(plan.release_after_eligible_ms / 1000)

        _release_gate.set()
        event("release_gate_set_after_relaunch", case_id=plan.case_id)

        results = collect_results(handles, plan.requests)
        status_after_terminal = status_rows(dbos, plan.queue_name)

        if plan.schedule == "stop-before-cleanup":
            status_before_cleanup_restart = status_after_terminal
            DBOS.destroy(destroy_registry=True, workflow_completion_timeout_sec=0)
            event("app_stopped_before_cleanup_poll", case_id=plan.case_id)
            dbos = launch_dbos_app(plan, app_url, sys_url, queue_conflict="never_update")
            handles = {request.key: DBOS.retrieve_workflow(request.workflow_id) for request in plan.requests}
            results = collect_results(handles, plan.requests)

        cleanup_rows = poll_cleanup(dbos, plan.queue_name)

        assert_terminal_conservation(plan.requests, results)
        accepted_dedupe = plan.requests[0] if plan.duplicate_request is not None else None
        assert_no_duplicate_execution(plan.duplicate_request, accepted_dedupe)
        assert_delay_windows(plan)
        assert_priority_order(plan)
        assert_partition_priority_order(plan)
        assert_concurrency(plan)
        assert_rate_limit(plan)
        invariant(
            "relaunch_preserved_queue_config",
            config_before_stop == config_after_update,
            config_before_stop=config_before_stop,
            config_after_relaunch=config_after_update,
        )
        invariant(
            "no_active_queue_rows_after_terminal_cleanup_poll",
            not cleanup_rows,
            active_rows=cleanup_rows,
        )

        outcome = {
            "classification": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "prompt_event": PROMPT_PATHS[plan.rung_id],
            "case_id": plan.case_id,
            "seed": plan.seed,
            "schedule": plan.schedule,
            "fault_model": plan.fault_model,
            "queue_name": plan.queue_name,
            "database_prefix": plan.database_prefix,
            "redacted_admin_url": masked_admin,
            "app_version": app_version,
            "config_before": config_before,
            "config_before_stop": config_before_stop,
            "config_after_relaunch": config_after_update,
            "status_after_stop_acceptance": status_after_stop_acceptance,
            "status_after_relaunch_start": status_after_relaunch_start,
            "status_after_terminal": status_after_terminal,
            "status_before_cleanup_restart": status_before_cleanup_restart,
            "duplicate_result": duplicate_result,
            "before_release_results": before_release_results,
            "results": results,
            "ledger_rows": list(_ledger_rows),
            "max_active_seen": _max_active_count,
            "max_active_by_partition": dict(_max_active_partition_counts),
            "cleanup_rows": cleanup_rows,
        }
        write_json(artifact_dir / "result.json", outcome)
        event("case_passed", case_id=plan.case_id)
        return outcome
    finally:
        _release_gate.set()
        if client is not None:
            client.destroy()
        try:
            DBOS.destroy(destroy_registry=True, workflow_completion_timeout_sec=2)
        finally:
            drop_databases(plan.database_prefix)


def run_case(plan: CasePlan, artifact_dir: Path) -> dict[str, Any]:
    global _case_id, _ledger_path, _active_count, _max_active_count
    _case_id = plan.case_id
    _ledger_rows.clear()
    _release_gate.clear()
    _active_count = 0
    _max_active_count = 0
    _active_partition_counts.clear()
    _max_active_partition_counts.clear()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _ledger_path = artifact_dir / "execution-ledger.jsonl"
    if _ledger_path.exists():
        _ledger_path.unlink()
    write_json(artifact_dir / "request-plan.json", asdict(plan))
    app_url, sys_url, masked_admin = prepare_databases(plan.database_prefix, artifact_dir)
    dbos: Any | None = None
    try:
        DBOS.destroy(destroy_registry=True)
        dbos = DBOS(config=make_config(plan, app_url, sys_url))

        DBOS.workflow()(workflow_body)
        DBOS.launch()
        queue = DBOS.register_queue(
            plan.queue_name,
            concurrency=plan.initial_concurrency,
            worker_concurrency=plan.initial_worker_concurrency,
            limiter=plan.initial_limiter,
            priority_enabled=plan.initial_priority_enabled,
            partition_queue=plan.partition_queue,
            polling_interval_sec=plan.polling_interval_sec,
            on_conflict="always_update",
        )
        config_before = queue_config_snapshot(dbos, plan.queue_name)
        event("case_started", case_id=plan.case_id, seed=plan.seed, schedule=plan.schedule)

        handles: dict[str, Any] = {}
        accepted_at: dict[str, int] = {}
        blocker = plan.requests[0]
        handles[blocker.key] = enqueue_request(plan.queue_name, blocker)
        accepted_at[blocker.key] = now_ms()
        wait_for_started(blocker.key, timeout_sec=8)
        event("blocker_started", case_id=plan.case_id, request_key=blocker.key)

        if plan.update_before_followers:
            apply_live_updates(queue, plan)

        for request in plan.requests[1:]:
            handles[request.key] = enqueue_request(plan.queue_name, request)
            accepted_at[request.key] = now_ms()
            object.__setattr__(request, "_accepted_at_ms", accepted_at[request.key])

        duplicate_result: dict[str, Any] | None = None
        if plan.duplicate_request is not None:
            if plan.duplicate_offset_ms:
                time.sleep(plan.duplicate_offset_ms / 1000)
            try:
                enqueue_request(plan.queue_name, plan.duplicate_request)
            except DBOSQueueDeduplicatedError as exc:
                duplicate_result = {
                    "request_key": plan.duplicate_request.key,
                    "workflow_id": plan.duplicate_request.workflow_id,
                    "status": "rejected_duplicate",
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
                event("duplicate_rejected", case_id=plan.case_id, error_type=type(exc).__name__)
            else:
                raise WorkloadFailure("duplicate enqueue unexpectedly succeeded")

        config_after_enqueue = queue_config_snapshot(dbos, plan.queue_name)
        status_after_enqueue = status_rows(dbos, plan.queue_name)

        if not plan.update_before_followers:
            apply_live_updates(queue, plan)
        config_after_update = queue_config_snapshot(dbos, plan.queue_name)

        delayed_requests = [request for request in plan.requests if request.delay_seconds is not None]
        before_release_results = assert_partition_pre_release(plan, handles, dbos)
        if not plan.partition_queue:
            if delayed_requests:
                latest_eligible = max(accepted_at[request.key] + int(request.delay_seconds * 1000) for request in delayed_requests)
                sleep_ms = max(0, latest_eligible + plan.release_after_eligible_ms - now_ms())
                time.sleep(sleep_ms / 1000)
            elif plan.release_after_eligible_ms:
                time.sleep(plan.release_after_eligible_ms / 1000)

        _release_gate.set()
        event("release_gate_set", case_id=plan.case_id)

        results = collect_results(handles, plan.requests)
        status_after_terminal = status_rows(dbos, plan.queue_name)
        cleanup_rows = poll_cleanup(dbos, plan.queue_name)

        assert_terminal_conservation(plan.requests, results)
        assert_no_duplicate_execution(plan.duplicate_request, delayed_requests[0] if delayed_requests else None)
        assert_delay_windows(plan)
        assert_priority_order(plan)
        assert_partition_priority_order(plan)
        assert_concurrency(plan)
        assert_rate_limit(plan)
        invariant(
            "no_active_queue_rows_after_terminal_cleanup_poll",
            not cleanup_rows,
            active_rows=cleanup_rows,
        )

        outcome = {
            "classification": "passed",
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "prompt_event": PROMPT_PATHS[plan.rung_id],
            "case_id": plan.case_id,
            "seed": plan.seed,
            "schedule": plan.schedule,
            "fault_model": plan.fault_model,
            "queue_name": plan.queue_name,
            "database_prefix": plan.database_prefix,
            "redacted_admin_url": masked_admin,
            "config_before": config_before,
            "config_after_enqueue": config_after_enqueue,
            "config_after_update": config_after_update,
            "status_after_enqueue": status_after_enqueue,
            "status_after_terminal": status_after_terminal,
            "duplicate_result": duplicate_result,
            "before_release_results": before_release_results,
            "results": results,
            "ledger_rows": list(_ledger_rows),
            "max_active_seen": _max_active_count,
            "max_active_by_partition": dict(_max_active_partition_counts),
            "cleanup_rows": cleanup_rows,
        }
        write_json(artifact_dir / "result.json", outcome)
        event("case_passed", case_id=plan.case_id)
        return outcome
    finally:
        try:
            DBOS.destroy(destroy_registry=True, workflow_completion_timeout_sec=2)
        finally:
            drop_databases(plan.database_prefix)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rung", default="rung-001")
    parser.add_argument("--case")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument(
        "--artifact-dir",
        default="/tmp/wio-artifacts/queue-composed-controls",
    )
    args = parser.parse_args()
    if args.rung not in RUNG_ALIASES:
        raise SystemExit(f"unsupported rung: {args.rung}")
    rung_id = RUNG_ALIASES[args.rung]
    if not args.case and not args.all_cases:
        raise SystemExit("provide --case or --all-cases")
    all_case_ids = {
        RUNG_001_ID: ["case-001", "case-002", "case-003"],
        RUNG_002_ID: ["case-001", "case-002", "case-003", "case-004"],
        RUNG_003_ID: ["case-001", "case-002", "case-003", "case-004", "case-005", "case-006"],
        RUNG_004_ID: ["case-001", "case-002", "case-003", "case-004", "case-005", "case-006"],
        RUNG_005_ID: [f"case-{idx:03d}" for idx in range(1, 25)],
        RUNG_007_ID: ["case-001", "case-002", "case-003"],
        RUNG_008_ID: ["case-001", "case-002", "case-003"],
    }[rung_id]
    case_ids = all_case_ids if args.all_cases else [args.case]
    invalid_cases = [case_id for case_id in case_ids if case_id not in all_case_ids]
    if invalid_cases:
        raise SystemExit(f"unsupported case for {rung_id}: {', '.join(str(item) for item in invalid_cases)}")
    if args.all_cases and not args.sequential:
        raise SystemExit("--all-cases requires --sequential for this workload")

    results = []
    for case_id in case_ids:
        assert case_id is not None
        artifact_dir = Path(args.artifact_dir) / case_id
        try:
            if rung_id == RUNG_008_ID:
                rate_plan = rate_limit_plan_for(case_id)
                if args.seed is not None and args.seed != rate_plan.seed:
                    raise SetupBlock(f"seed {args.seed} does not match {case_id} seed {rate_plan.seed}")
                results.append(run_rate_limit_plan_case(rate_plan, artifact_dir))
                continue
            plan = plan_for(case_id, rung_id)
            if args.seed is not None and args.seed != plan.seed:
                raise SetupBlock(f"seed {args.seed} does not match {case_id} seed {plan.seed}")
            if uses_async_partition_runner(plan):
                results.append(asyncio.run(run_async_partition_case(plan, artifact_dir)))
            elif uses_relaunch_runner(plan):
                results.append(run_relaunch_case(plan, artifact_dir))
            else:
                results.append(run_case(plan, artifact_dir))
        except SetupBlock as exc:
            print(f"SETUP-BLOCK case={case_id} error={exc}", file=sys.stderr)
            return 42
        except WorkloadFailure as exc:
            write_json(artifact_dir / "failure.json", {"case_id": case_id, "error": str(exc)})
            print(f"WORKLOAD-FAIL {exc}", flush=True)
            print(f"FINDING-CANDIDATE case={case_id} error={exc}", file=sys.stderr)
            return 1
    write_json(Path(args.artifact_dir) / "run-result.json", results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
