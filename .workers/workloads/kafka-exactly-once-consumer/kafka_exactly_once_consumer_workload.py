#!/usr/bin/env python3
"""Fresh WIO workload for DBOS Kafka consumer idempotency.

Frontier: kafka-exactly-once-consumer
Rungs:
  - rung-000-kafka-service-smoke
  - rung-001-duplicate-key-idempotency
  - rung-002-rebalance-offset-replay
  - rung-003-broker-restart
  - rung-004-bounded-seed-sweep
  - rung-005-finding-minimization
Evidence key:
  evidence-key:events/frontier_designer-20260620T072714344022000Z.prompt.md
Protected product promise:
  Kafka-triggered DBOS workflows process produced Kafka records durably while
  preserving a modeled one-effect-per-logical-key oracle across duplicate
  external deliveries.
Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py \
    --rung rung-001-duplicate-key-idempotency --case case-001
Seed policy:
  Exact rung seeds are encoded below; every case writes the derived case JSON,
  produced Kafka offsets, expected workflow IDs, observed workflow statuses,
  and ledger rows.
Invariant oracle:
  Independent produced-record model, Kafka delivery metadata, DBOS workflow IDs,
  observed offset ledger rows, and idempotent side-effect ledger rows agree.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

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
if VENV_SITE_PACKAGES.exists() and str(VENV_SITE_PACKAGES) not in sys.path:
    # Keep runtime ABI overrides from .workers/python-runtime.sh ahead of the
    # prepared venv. In cloud, confluent_kafka must resolve from the musl wheel.
    sys.path.append(str(VENV_SITE_PACKAGES))

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

    from confluent_kafka import KafkaException, Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    from dbos import DBOS, DBOSConfig, KafkaMessage
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "kafka-exactly-once-consumer"
RUNG_000_ID = "rung-000-kafka-service-smoke"
RUNG_001_ID = "rung-001-duplicate-key-idempotency"
RUNG_002_ID = "rung-002-rebalance-offset-replay"
RUNG_003_ID = "rung-003-broker-restart"
RUNG_004_ID = "rung-004-bounded-seed-sweep"
RUNG_005_ID = "rung-005-finding-minimization"
APP_ID = "wio-kafka-exactly-once"
APP_VERSION = "wio-kafka-exactly-once-rungs-000-005"
PROMPT_PATH = "evidence-key:events/frontier_designer-20260620T072714344022000Z.prompt.md"

OFFSET_TABLE = "kafka_observed_offsets"
ACCEPTANCE_TABLE = "kafka_acceptance_markers"
EFFECT_TABLE = "kafka_effects"
MOCK_BOOTSTRAP_ENV = "WIO_KAFKA_MOCK_BOOTSTRAP_SERVERS"
STANDALONE_BOOTSTRAP_ENV = "WIO_KAFKA_BROKER_BOOTSTRAP_SERVERS"
_KAFKA_MOCK_HOLDER: Producer | None = None
_KAFKA_BROKER_PROCESS: subprocess.Popen[str] | None = None
_KAFKA_BROKER_BIN: Path | None = None
_KAFKA_BROKER_PORT: int | None = None
_KAFKA_BROKER_DATA_DIR: Path | None = None


class WorkloadFailure(Exception):
    pass


class SetupBlock(Exception):
    pass


@dataclass(frozen=True)
class ProducedRecord:
    logical_key: str
    payload: str
    producer_key: str
    delay_before_seconds: float = 0.0
    workflow_sleep_seconds: float = 0.0


@dataclass(frozen=True)
class CasePlan:
    rung_id: str
    case_id: str
    seed: int
    schedule: str
    focus: str
    topic: str
    group_id: str
    database_prefix: str
    records: tuple[ProducedRecord, ...]
    relaunch_delay_seconds: float = 0.0
    relaunch_after_accept_count: int = 0
    post_terminal_relaunch: bool = False
    stable_after_relaunch_seconds: float = 0.0
    broker_restart_phase: str | None = None
    broker_restart_delay_seconds: float = 0.0
    relaunch_after_broker_restart: bool = False
    late_read_after_idle_seconds: float = 0.0
    delete_topic_after_terminal: bool = False


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


def read_tail(path: Path, max_bytes: int = 4096) -> str:
    try:
        data = path.read_bytes()
    except OSError as exc:
        return f"<unreadable {type(exc).__name__}: {exc}>"
    return data[-max_bytes:].decode("utf-8", errors="replace")


def emit_case_debug_artifacts(artifacts: Path) -> None:
    if not artifacts.exists():
        event("case_debug_artifacts_missing", artifacts=str(artifacts))
        return
    files = sorted(
        str(path.relative_to(artifacts))
        for path in artifacts.rglob("*")
        if path.is_file()
    )
    event("case_debug_artifacts", artifacts=str(artifacts), files=files)
    for name in (
        "kafka-standalone-broker.json",
        "kafka-broker.stdout.log",
        "kafka-broker.stderr.log",
        "pre-relaunch-acceptance-ledger.json",
        "acceptance-ledger.json",
        "offset-ledger.json",
        "workflow-statuses.json",
    ):
        path = artifacts / name
        if path.exists():
            event("case_debug_artifact_tail", path=str(path), tail=read_tail(path))


def stable_suffix(*parts: str, size: int = 12) -> str:
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()
    return digest[:size]


def admin_url() -> sa.URL:
    raw = os.environ.get("DBOS_POSTGRES_ADMIN_URL")
    if raw:
        url = make_url(raw)
    else:
        url = sa.URL.create(
            "postgresql+psycopg",
            username=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", "dbos"),
            host=os.environ.get("PGHOST", "localhost"),
            port=int(os.environ.get("PGPORT", "5432")),
            database=os.environ.get("PGDATABASE", "postgres"),
        )
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
        base.set(drivername="postgresql+psycopg", database=app_db).render_as_string(
            hide_password=False
        ),
        base.set(drivername="postgresql+psycopg", database=sys_db).render_as_string(
            hide_password=False
        ),
        masked,
    )


def drop_databases(prefix: str) -> None:
    if os.environ.get("WIO_KAFKA_KEEP_DATABASES") == "1":
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
        "executor_id": f"wio-kafka-{plan.case_id}",
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
    }
    if rung not in aliases:
        raise SetupBlock(f"unsupported rung {rung}; this workload currently implements rungs 000-005")
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
        return ["case-001", "case-002"]
    raise SetupBlock(f"unsupported rung {rung_id}")


def build_case_plan(rung_id: str, case_id: str) -> CasePlan:
    seeds: dict[tuple[str, str], int] = {
        (RUNG_000_ID, "case-001"): 3700,
        (RUNG_001_ID, "case-001"): 3710,
        (RUNG_001_ID, "case-002"): 3711,
        (RUNG_001_ID, "case-003"): 3712,
        (RUNG_002_ID, "case-001"): 3720,
        (RUNG_002_ID, "case-002"): 3721,
        (RUNG_002_ID, "case-003"): 3722,
        (RUNG_003_ID, "case-001"): 3730,
        (RUNG_003_ID, "case-002"): 3731,
        (RUNG_003_ID, "case-003"): 3732,
        (RUNG_003_ID, "case-004"): 3733,
        (RUNG_003_ID, "case-005"): 3734,
        (RUNG_003_ID, "case-006"): 3735,
        **{
            (RUNG_004_ID, f"case-{index:03d}"): 3739 + index
            for index in range(1, 25)
        },
        (RUNG_005_ID, "case-001"): 3770,
        (RUNG_005_ID, "case-002"): 3771,
    }
    key = (rung_id, case_id)
    if key not in seeds:
        raise SetupBlock(f"unsupported case {rung_id}/{case_id}")
    seed = seeds[key]
    suffix = stable_suffix(rung_id, case_id, str(seed))
    topic = f"wio-dbos-kafka-{seed}-{suffix}"
    group_id = f"wio-dbos-kafka-{seed}-{case_id}-{suffix}"
    database_prefix = f"wio_kafka_{seed}_{case_id.replace('-', '_')}_{suffix}"
    logical_key = f"logical-{seed}-{case_id}"

    if rung_id == RUNG_000_ID:
        return CasePlan(
            rung_id=rung_id,
            case_id=case_id,
            seed=seed,
            schedule="create-topic-produce-one-record-start-consumer",
            focus="Kafka service and DBOS consumer can run in the harness",
            topic=topic,
            group_id=group_id,
            database_prefix=database_prefix,
            records=(
                ProducedRecord(
                    logical_key=logical_key,
                    producer_key=f"kafka-key-{seed}-001",
                    payload=f"payload-{seed}-single",
                ),
            ),
        )

    if rung_id == RUNG_002_ID:
        if case_id == "case-001":
            records = (
                ProducedRecord(
                    logical_key,
                    "restart-before-terminal",
                    f"kafka-key-{seed}-restart",
                    workflow_sleep_seconds=1.5,
                ),
            )
            schedule = "restart-consumer-after-record-accepted-but-before-result"
            focus = "consumer restart near an in-flight workflow preserves the modeled offset"
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule=schedule,
                focus=focus,
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                relaunch_delay_seconds=0.2,
                relaunch_after_accept_count=1,
            )
        if case_id == "case-002":
            records = (
                ProducedRecord(
                    logical_key,
                    "committed-offset-stable",
                    f"kafka-key-{seed}-committed",
                ),
            )
            schedule = "relaunch-after-committed-offset"
            focus = "committed offset is not reprocessed by the same consumer group"
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule=schedule,
                focus=focus,
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                post_terminal_relaunch=True,
                stable_after_relaunch_seconds=1.5,
            )
        if case_id == "case-003":
            records = tuple(
                ProducedRecord(
                    f"{logical_key}-{index}",
                    f"backlog-payload-{index}",
                    f"kafka-key-{seed}-backlog-{index}",
                    workflow_sleep_seconds=0.2 if index == 1 else 0.0,
                )
                for index in range(1, 5)
            )
            schedule = "relaunch-consumer-with-backlog"
            focus = "backlog records all reach terminal workflow state once across relaunch"
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule=schedule,
                focus=focus,
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                relaunch_delay_seconds=0.15,
                relaunch_after_accept_count=1,
            )
        raise SetupBlock(f"unsupported rung-002 case {case_id}")

    if rung_id == RUNG_003_ID:
        if case_id == "case-001":
            records = (
                ProducedRecord(f"{logical_key}-1", "broker-before-poll-1", f"kafka-key-{seed}-pre-1"),
                ProducedRecord(f"{logical_key}-2", "broker-before-poll-2", f"kafka-key-{seed}-pre-2"),
            )
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule="restart-broker-after-produce-before-consume",
                focus="broker restart before poll does not lose produced records",
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                broker_restart_phase="after_produce_before_consumer",
            )
        if case_id == "case-002":
            records = (
                ProducedRecord(
                    logical_key,
                    "broker-during-slow-workflow",
                    f"kafka-key-{seed}-blocked",
                    workflow_sleep_seconds=2.0,
                ),
            )
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule="restart-broker-while-workflow-blocked",
                focus="broker restart after poll before DBOS terminal does not duplicate",
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                broker_restart_phase="after_consumer_launch_delay",
                broker_restart_delay_seconds=0.35,
            )
        if case_id == "case-003":
            records = (
                ProducedRecord(logical_key, "broker-near-offset-commit", f"kafka-key-{seed}-commit", workflow_sleep_seconds=0.25),
            )
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule="restart-during-commit-window",
                focus="broker restart near offset commit remains replay-safe",
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                broker_restart_phase="after_consumer_launch_delay",
                broker_restart_delay_seconds=0.3,
            )
        if case_id == "case-004":
            records = tuple(
                ProducedRecord(f"{logical_key}-{index}", f"broker-relaunch-{index}", f"kafka-key-{seed}-relaunch-{index}")
                for index in range(1, 4)
            )
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule="restart-consumer-after-broker-returns",
                focus="consumer restart after broker recovery preserves terminal artifacts",
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                broker_restart_phase="after_produce_before_consumer",
                relaunch_after_broker_restart=True,
            )
        if case_id == "case-005":
            records = (
                ProducedRecord(logical_key, "late-result-read", f"kafka-key-{seed}-late"),
            )
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule="read-handles-after-consumer-idle",
                focus="late workflow status/result reads do not reconsume Kafka records",
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                late_read_after_idle_seconds=1.5,
            )
        if case_id == "case-006":
            records = (
                ProducedRecord(f"{logical_key}-1", "cleanup-terminal-1", f"kafka-key-{seed}-cleanup-1"),
                ProducedRecord(f"{logical_key}-2", "cleanup-terminal-2", f"kafka-key-{seed}-cleanup-2"),
            )
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule="cleanup-after-modeled-terminal-state-only",
                focus="topic cleanup after terminal state does not hide unfinished work",
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                delete_topic_after_terminal=True,
            )
        raise SetupBlock(f"unsupported rung-003 case {case_id}")

    if rung_id == RUNG_004_ID:
        case_number = int(case_id.removeprefix("case-"))
        variant = (case_number - 1) % 6
        if variant == 0:
            duplicate_shapes = ("same-payload", "slow-first", "changed-payload")
            duplicate_shape = duplicate_shapes[((case_number - 1) // 6) % len(duplicate_shapes)]
            if duplicate_shape == "same-payload":
                records = (
                    ProducedRecord(logical_key, "same-payload", f"kafka-key-{seed}-a"),
                    ProducedRecord(logical_key, "same-payload", f"kafka-key-{seed}-b"),
                )
                schedule = "bounded-duplicate-records-with-same-idempotency-key"
                focus = "duplicate-key sweep preserves one modeled effect"
            elif duplicate_shape == "slow-first":
                records = (
                    ProducedRecord(
                        logical_key,
                        "same-payload-slow-first",
                        f"kafka-key-{seed}-first",
                        workflow_sleep_seconds=1.0,
                    ),
                    ProducedRecord(
                        logical_key,
                        "same-payload-slow-first",
                        f"kafka-key-{seed}-second",
                        delay_before_seconds=0.05,
                    ),
                )
                schedule = "bounded-slow-first-duplicate-record"
                focus = "duplicate-key sweep around slow workflow preserves one effect"
            else:
                records = (
                    ProducedRecord(logical_key, "first-payload-wins", f"kafka-key-{seed}-first"),
                    ProducedRecord(
                        logical_key,
                        "changed-payload-ignored",
                        f"kafka-key-{seed}-changed",
                        delay_before_seconds=0.05,
                    ),
                )
                schedule = "bounded-duplicate-key-with-different-payload"
                focus = "duplicate-key sweep preserves first payload"
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule=schedule,
                focus=focus,
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
            )
        if variant == 1:
            records = (
                ProducedRecord(logical_key, "committed-offset-stable", f"kafka-key-{seed}-committed"),
            )
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule="bounded-relaunch-after-committed-offset",
                focus="offset replay sweep does not reprocess committed records",
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                post_terminal_relaunch=True,
                stable_after_relaunch_seconds=1.5,
            )
        if variant == 2:
            records = tuple(
                ProducedRecord(
                    f"{logical_key}-{index}",
                    f"backlog-payload-{index}",
                    f"kafka-key-{seed}-backlog-{index}",
                    workflow_sleep_seconds=0.2 if index == 1 else 0.0,
                )
                for index in range(1, 5)
            )
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule="bounded-relaunch-consumer-with-backlog",
                focus="rebalance/backlog sweep reaches each terminal workflow once",
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                relaunch_delay_seconds=0.15,
            )
        if variant == 3:
            records = (
                ProducedRecord(f"{logical_key}-1", "broker-before-poll-1", f"kafka-key-{seed}-pre-1"),
                ProducedRecord(f"{logical_key}-2", "broker-before-poll-2", f"kafka-key-{seed}-pre-2"),
            )
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule="bounded-restart-broker-after-produce-before-consume",
                focus="broker-before-poll sweep does not lose produced records",
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                broker_restart_phase="after_produce_before_consumer",
            )
        if variant == 4:
            records = (
                ProducedRecord(
                    logical_key,
                    "broker-during-slow-workflow",
                    f"kafka-key-{seed}-blocked",
                    workflow_sleep_seconds=2.0,
                ),
            )
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule="bounded-restart-broker-while-workflow-blocked",
                focus="broker-during-workflow sweep does not duplicate terminal effects",
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                broker_restart_phase="after_consumer_launch_delay",
                broker_restart_delay_seconds=0.35,
            )

        records = (
            ProducedRecord(logical_key, "late-result-read", f"kafka-key-{seed}-late"),
        )
        return CasePlan(
            rung_id=rung_id,
            case_id=case_id,
            seed=seed,
            schedule="bounded-read-handles-after-consumer-idle",
            focus="late result/status reads do not reconsume Kafka records",
            topic=topic,
            group_id=group_id,
            database_prefix=database_prefix,
            records=records,
            late_read_after_idle_seconds=1.5,
        )

    if rung_id == RUNG_005_ID:
        if case_id == "case-001":
            records = (
                ProducedRecord(
                    logical_key,
                    "minimized-single-inflight",
                    f"kafka-key-{seed}-single",
                    workflow_sleep_seconds=0.2,
                ),
            )
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule="minimized-immediate-relaunch-single-record",
                focus="single consumed Kafka offset is not lost across immediate DBOS relaunch",
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                relaunch_delay_seconds=0.15,
            )
        if case_id == "case-002":
            records = (
                ProducedRecord(
                    f"{logical_key}-1",
                    "minimized-backlog-1",
                    f"kafka-key-{seed}-backlog-1",
                    workflow_sleep_seconds=0.2,
                ),
                ProducedRecord(
                    f"{logical_key}-2",
                    "minimized-backlog-2",
                    f"kafka-key-{seed}-backlog-2",
                ),
            )
            return CasePlan(
                rung_id=rung_id,
                case_id=case_id,
                seed=seed,
                schedule="minimized-immediate-relaunch-two-record-backlog",
                focus="small backlog does not skip the first Kafka offset across immediate DBOS relaunch",
                topic=topic,
                group_id=group_id,
                database_prefix=database_prefix,
                records=records,
                relaunch_delay_seconds=0.15,
            )
        raise SetupBlock(f"unsupported rung-005 case {case_id}")

    if case_id == "case-001":
        records = (
            ProducedRecord(logical_key, "same-payload", f"kafka-key-{seed}-a"),
            ProducedRecord(logical_key, "same-payload", f"kafka-key-{seed}-b"),
        )
        schedule = "produce-duplicate-records-with-same-idempotency-key"
        focus = "same logical key delivered twice creates one modeled effect"
    elif case_id == "case-002":
        records = (
            ProducedRecord(
                logical_key,
                "same-payload-slow-first",
                f"kafka-key-{seed}-first",
                workflow_sleep_seconds=1.0,
            ),
            ProducedRecord(
                logical_key,
                "same-payload-slow-first",
                f"kafka-key-{seed}-second",
                delay_before_seconds=0.05,
            ),
        )
        schedule = "block-first-record-workflow-then-produce-duplicate"
        focus = "out-of-order duplicate around slow workflow does not double effect"
    elif case_id == "case-003":
        records = (
            ProducedRecord(logical_key, "first-payload-wins", f"kafka-key-{seed}-first"),
            ProducedRecord(
                logical_key,
                "changed-payload-ignored",
                f"kafka-key-{seed}-changed",
                delay_before_seconds=0.05,
            ),
        )
        schedule = "produce-duplicate-key-with-different-payload"
        focus = "same key with changed payload keeps the first modeled effect"
    else:
        raise SetupBlock(f"unsupported case {case_id}")

    return CasePlan(
        rung_id=rung_id,
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        focus=focus,
        topic=topic,
        group_id=group_id,
        database_prefix=database_prefix,
        records=records,
    )


def wants_librdkafka_mock() -> bool:
    configured = os.environ.get("WIO_KAFKA_USE_LIBRDKAFKA_MOCK")
    if configured is not None:
        return configured.lower() not in {"0", "false", "no", "off"}
    return False


def wants_standalone_kafka_broker() -> bool:
    configured = os.environ.get("WIO_KAFKA_USE_STANDALONE_BROKER")
    if configured is not None:
        return configured.lower() not in {"0", "false", "no", "off"}
    return not os.environ.get("KAFKA_BOOTSTRAP_SERVERS")


def start_librdkafka_mock(artifacts: Path) -> str:
    global _KAFKA_MOCK_HOLDER
    if _KAFKA_MOCK_HOLDER is None:
        broker_count = int(os.environ.get("WIO_KAFKA_MOCK_BROKERS", "1"))
        _KAFKA_MOCK_HOLDER = Producer(
            {
                "bootstrap.servers": "wio-librdkafka-mock:9092",
                "test.mock.num.brokers": broker_count,
            }
        )
        metadata = _KAFKA_MOCK_HOLDER.list_topics(timeout=5)
        bootstrap_servers = ",".join(
            f"{broker.host}:{broker.port}" for broker in metadata.brokers.values()
        )
        if not bootstrap_servers:
            raise SetupBlock("librdkafka mock did not expose a bootstrap address")
        os.environ[MOCK_BOOTSTRAP_ENV] = bootstrap_servers
        write_json(
            artifacts / "kafka-mock-broker.json",
            {
                "kind": "librdkafka_mock",
                "broker_count": broker_count,
                "bootstrap_servers": bootstrap_servers,
                "brokers": [
                    {"id": broker.id, "host": broker.host, "port": broker.port}
                    for broker in metadata.brokers.values()
                ],
            },
        )
        event(
            "kafka_librdkafka_mock_started",
            bootstrap_servers=bootstrap_servers,
            broker_count=broker_count,
        )
    return os.environ[MOCK_BOOTSTRAP_ENV]


def stop_librdkafka_mock() -> None:
    global _KAFKA_MOCK_HOLDER
    _KAFKA_MOCK_HOLDER = None
    os.environ.pop(MOCK_BOOTSTRAP_ENV, None)


def start_standalone_kafka_broker(artifacts: Path) -> str:
    global _KAFKA_BROKER_BIN, _KAFKA_BROKER_DATA_DIR, _KAFKA_BROKER_PORT, _KAFKA_BROKER_PROCESS
    if _KAFKA_BROKER_PROCESS is None:
        broker_bin = Path(
            os.environ.get(
                "WIO_KAFKA_BROKER_BIN",
                str(REPO_ROOT / ".workers" / "vendor" / "bin" / "wio-kafka-broker-linux-amd64"),
            )
        )
        if not broker_bin.exists():
            raise SetupBlock(f"standalone Kafka broker binary missing: {broker_bin}")
        if not os.access(broker_bin, os.X_OK):
            raise SetupBlock(f"standalone Kafka broker binary is not executable: {broker_bin}")

        port = int(os.environ.get("WIO_KAFKA_BROKER_PORT", "9092"))
        data_dir = Path(
            os.environ.get(
                "WIO_KAFKA_BROKER_DATA_DIR",
                str(artifacts / "kafka-broker-data"),
            )
        )
        sync_writes = os.environ.get("WIO_KAFKA_BROKER_SYNC_WRITES", "1") != "0"
        artifacts.mkdir(parents=True, exist_ok=True)
        data_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = artifacts / "kafka-broker.stdout.log"
        stderr_path = artifacts / "kafka-broker.stderr.log"
        stdout_handle = stdout_path.open("w")
        stderr_handle = stderr_path.open("w")
        command = [
            str(broker_bin),
            "--port",
            str(port),
            "--partitions",
            "1",
            "--data-dir",
            str(data_dir),
        ]
        if sync_writes:
            command.append("--sync")
        try:
            _KAFKA_BROKER_PROCESS = subprocess.Popen(
                command,
                text=True,
                stdout=stdout_handle,
                stderr=stderr_handle,
            )
        finally:
            stdout_handle.close()
            stderr_handle.close()

        bootstrap_servers = f"127.0.0.1:{port}"
        deadline = time.monotonic() + float(os.environ.get("WIO_KAFKA_BROKER_START_TIMEOUT", "15"))
        while time.monotonic() < deadline:
            if _KAFKA_BROKER_PROCESS.poll() is not None:
                stderr = stderr_path.read_text(errors="replace") if stderr_path.exists() else ""
                raise SetupBlock(
                    f"standalone Kafka broker exited early with {_KAFKA_BROKER_PROCESS.returncode}: {stderr}"
                )
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                    break
            except OSError:
                time.sleep(0.25)
        else:
            raise SetupBlock(f"standalone Kafka broker did not listen on {bootstrap_servers}")

        os.environ[STANDALONE_BOOTSTRAP_ENV] = bootstrap_servers
        _KAFKA_BROKER_BIN = broker_bin
        _KAFKA_BROKER_PORT = port
        _KAFKA_BROKER_DATA_DIR = data_dir
        write_json(
            artifacts / "kafka-standalone-broker.json",
            {
                "kind": "kfake_standalone",
                "binary": str(broker_bin),
                "bootstrap_servers": bootstrap_servers,
                "data_dir": str(data_dir),
                "sync_writes": sync_writes,
                "stdout": str(stdout_path),
                "stderr": str(stderr_path),
            },
        )
        event("kafka_standalone_broker_started", bootstrap_servers=bootstrap_servers)
    return os.environ[STANDALONE_BOOTSTRAP_ENV]


def stop_standalone_kafka_broker() -> None:
    global _KAFKA_BROKER_PROCESS
    if _KAFKA_BROKER_PROCESS is not None:
        _KAFKA_BROKER_PROCESS.terminate()
        try:
            _KAFKA_BROKER_PROCESS.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _KAFKA_BROKER_PROCESS.kill()
            _KAFKA_BROKER_PROCESS.wait(timeout=5)
        _KAFKA_BROKER_PROCESS = None
    os.environ.pop(STANDALONE_BOOTSTRAP_ENV, None)


def mock_supports_plan(plan: CasePlan) -> bool:
    return not (
        plan.broker_restart_phase
        or plan.relaunch_delay_seconds
        or plan.post_terminal_relaunch
        or plan.relaunch_after_broker_restart
        or plan.delete_topic_after_terminal
    )


def kafka_bootstrap_servers(artifacts: Path) -> str:
    if wants_librdkafka_mock():
        return start_librdkafka_mock(artifacts)
    if wants_standalone_kafka_broker():
        return start_standalone_kafka_broker(artifacts)
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def kafka_admin(bootstrap_servers: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap_servers})


def preflight_kafka(bootstrap_servers: str, artifacts: Path) -> None:
    if _KAFKA_MOCK_HOLDER is not None:
        metadata = _KAFKA_MOCK_HOLDER.list_topics(timeout=5)
        event(
            "kafka_preflight",
            bootstrap_servers=bootstrap_servers,
            broker_count=len(metadata.brokers or {}),
            topic_count=len(metadata.topics or {}),
            broker_mode="librdkafka_mock",
        )
        return
    try:
        metadata = kafka_admin(bootstrap_servers).list_topics(timeout=5)
    except Exception as exc:
        write_json(
            artifacts / "setup-block.json",
            {
                "kind": "kafka_broker_unreachable",
                "bootstrap_servers": bootstrap_servers,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        )
        raise SetupBlock(
            f"kafka broker unreachable at {bootstrap_servers}: {type(exc).__name__}: {exc}"
        ) from exc
    event(
        "kafka_preflight",
        bootstrap_servers=bootstrap_servers,
        broker_count=len(metadata.brokers or {}),
        topic_count=len(metadata.topics or {}),
    )


def wait_for_kafka(bootstrap_servers: str, artifacts: Path, timeout_seconds: float = 45) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            preflight_kafka(bootstrap_servers, artifacts)
            return
        except SetupBlock as exc:
            last_error = str(exc)
            time.sleep(1)
    raise SetupBlock(f"kafka broker did not become ready after restart: {last_error}")


def restart_kafka_broker(bootstrap_servers: str, artifacts: Path, reason: str) -> None:
    if _KAFKA_MOCK_HOLDER is not None:
        raise SetupBlock("librdkafka mock broker does not support broker restart rungs")
    if _KAFKA_BROKER_PROCESS is not None:
        event(
            "kafka_standalone_broker_restart_start",
            bootstrap_servers=bootstrap_servers,
            data_dir=str(_KAFKA_BROKER_DATA_DIR) if _KAFKA_BROKER_DATA_DIR else None,
            reason=reason,
        )
        started = time.time()
        stop_standalone_kafka_broker()
        restarted_bootstrap_servers = start_standalone_kafka_broker(artifacts)
        record = {
            "kind": "kfake_standalone_restart",
            "bootstrap_servers_before": bootstrap_servers,
            "bootstrap_servers_after": restarted_bootstrap_servers,
            "data_dir": str(_KAFKA_BROKER_DATA_DIR) if _KAFKA_BROKER_DATA_DIR else None,
            "elapsed_seconds": time.time() - started,
            "reason": reason,
        }
        write_json(artifacts / f"kafka-broker-restart-{reason}.json", record)
        if restarted_bootstrap_servers != bootstrap_servers:
            raise SetupBlock(
                "standalone Kafka broker restart changed bootstrap address: "
                f"{bootstrap_servers} -> {restarted_bootstrap_servers}"
            )
        wait_for_kafka(bootstrap_servers, artifacts)
        event(
            "kafka_standalone_broker_restart_complete",
            bootstrap_servers=bootstrap_servers,
            reason=reason,
        )
        return
    container = os.environ.get("WIO_KAFKA_DOCKER_CONTAINER", "wio-kafka-broker")
    event("kafka_broker_restart_start", container=container, reason=reason)
    command = ["docker", "restart", container]
    started = time.time()
    try:
        result = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60,
        )
    except FileNotFoundError as exc:
        raise SetupBlock("docker CLI is required for broker restart rung") from exc
    except subprocess.TimeoutExpired as exc:
        raise SetupBlock(f"docker restart timed out for Kafka container {container}") from exc
    record = {
        "command": command,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "elapsed_seconds": time.time() - started,
        "reason": reason,
    }
    write_json(artifacts / f"kafka-broker-restart-{reason}.json", record)
    if result.returncode != 0:
        raise SetupBlock(
            f"docker restart failed for Kafka container {container}: {result.stderr.strip()}"
        )
    wait_for_kafka(bootstrap_servers, artifacts)
    event("kafka_broker_restart_complete", container=container, reason=reason)


def create_topic(bootstrap_servers: str, topic: str, artifacts: Path) -> None:
    if _KAFKA_MOCK_HOLDER is not None:
        event("kafka_mock_topic_auto_create", topic=topic)
        return
    admin = kafka_admin(bootstrap_servers)
    event("kafka_create_topic", topic=topic)
    try:
        existing = admin.list_topics(topic=topic, timeout=5)
        if topic in (existing.topics or {}):
            event("kafka_topic_delete_before_recreate", topic=topic)
            delete_futures = admin.delete_topics(
                [topic], operation_timeout=10, request_timeout=12
            )
            for delete_topic, future in delete_futures.items():
                future.result(timeout=12)
                event("kafka_topic_deleted", topic=delete_topic)
            delete_deadline = time.monotonic() + 15
            while time.monotonic() < delete_deadline:
                metadata = admin.list_topics(timeout=5)
                if topic not in (metadata.topics or {}):
                    break
                time.sleep(0.25)
        futures = admin.create_topics(
            [NewTopic(topic, num_partitions=1, replication_factor=1)],
            operation_timeout=10,
            request_timeout=12,
        )
        for future_topic, future in futures.items():
            try:
                future.result(timeout=12)
                event("kafka_topic_created", topic=future_topic)
            except KafkaException as exc:
                message = str(exc)
                if "TOPIC_ALREADY_EXISTS" not in message:
                    raise
                event("kafka_topic_already_exists", topic=future_topic)
    except Exception as exc:
        write_json(
            artifacts / "setup-block.json",
            {
                "kind": "kafka_topic_create_failed",
                "topic": topic,
                "bootstrap_servers": bootstrap_servers,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        )
        raise SetupBlock(f"kafka topic create failed: {type(exc).__name__}: {exc}") from exc

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        metadata = admin.list_topics(topic=topic, timeout=5)
        if topic in (metadata.topics or {}):
            return
        time.sleep(0.25)
    raise SetupBlock(f"kafka topic {topic} did not appear in metadata")


def delete_topic(bootstrap_servers: str, topic: str, artifacts: Path) -> None:
    if _KAFKA_MOCK_HOLDER is not None:
        raise SetupBlock("librdkafka mock broker does not support topic deletion rungs")
    admin = kafka_admin(bootstrap_servers)
    event("kafka_delete_topic", topic=topic)
    try:
        futures = admin.delete_topics([topic], operation_timeout=10, request_timeout=12)
        for delete_topic_name, future in futures.items():
            future.result(timeout=12)
            event("kafka_topic_deleted", topic=delete_topic_name)
    except Exception as exc:
        raise SetupBlock(f"kafka topic delete failed for {topic}: {type(exc).__name__}: {exc}") from exc
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        metadata = admin.list_topics(timeout=5)
        if topic not in (metadata.topics or {}):
            write_json(artifacts / "topic-cleanup.json", {"topic": topic, "status": "deleted"})
            return
        time.sleep(0.5)
    raise WorkloadFailure(f"kafka topic {topic} still visible after cleanup")



def init_app_ledger(app_url: str) -> None:
    engine = sa.create_engine(app_url)
    try:
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {ACCEPTANCE_TABLE} (
                        topic TEXT NOT NULL,
                        partition_id INTEGER NOT NULL,
                        offset_id BIGINT NOT NULL,
                        workflow_id TEXT NOT NULL,
                        logical_key TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        producer_key TEXT,
                        accepted_at DOUBLE PRECISION NOT NULL,
                        PRIMARY KEY (topic, partition_id, offset_id)
                    )
                    """
                )
            )
            conn.execute(
                sa.text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {OFFSET_TABLE} (
                        topic TEXT NOT NULL,
                        partition_id INTEGER NOT NULL,
                        offset_id BIGINT NOT NULL,
                        workflow_id TEXT NOT NULL,
                        logical_key TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        producer_key TEXT,
                        observed_at DOUBLE PRECISION NOT NULL,
                        PRIMARY KEY (topic, partition_id, offset_id)
                    )
                    """
                )
            )
            conn.execute(
                sa.text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {EFFECT_TABLE} (
                        logical_key TEXT PRIMARY KEY,
                        first_payload TEXT NOT NULL,
                        first_workflow_id TEXT NOT NULL,
                        first_topic TEXT NOT NULL,
                        first_partition INTEGER NOT NULL,
                        first_offset BIGINT NOT NULL,
                        effect_count INTEGER NOT NULL DEFAULT 1,
                        created_at DOUBLE PRECISION NOT NULL
                    )
                    """
                )
            )
    finally:
        engine.dispose()


def workflow_id_for(plan: CasePlan, partition: int, offset: int) -> str:
    return f"kafka-unique-id-{plan.topic}-{partition}-{plan.group_id}-{offset}"


def record_kafka_effect(app_url: str, msg: KafkaMessage) -> None:
    if msg.value is None:
        raise ValueError("Kafka message had no value")
    payload = json.loads(msg.value.decode("utf-8"))
    sleep_seconds = float(payload.get("workflow_sleep_seconds") or 0.0)

    logical_key = str(payload["logical_key"])
    payload_value = str(payload["payload"])
    producer_key = msg.key.decode("utf-8") if msg.key else None
    workflow_id = workflow_id_for(
        build_case_plan(str(payload["rung_id"]), str(payload["case_id"])),
        int(msg.partition),
        int(msg.offset),
    )
    engine = sa.create_engine(app_url)
    try:
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    f"""
                    INSERT INTO {ACCEPTANCE_TABLE}
                        (topic, partition_id, offset_id, workflow_id, logical_key,
                         payload, producer_key, accepted_at)
                    VALUES
                        (:topic, :partition_id, :offset_id, :workflow_id, :logical_key,
                         :payload, :producer_key, :accepted_at)
                    ON CONFLICT (topic, partition_id, offset_id) DO NOTHING
                    """
                ),
                {
                    "topic": msg.topic,
                    "partition_id": int(msg.partition),
                    "offset_id": int(msg.offset),
                    "workflow_id": workflow_id,
                    "logical_key": logical_key,
                    "payload": payload_value,
                    "producer_key": producer_key,
                    "accepted_at": time.time(),
                },
            )
            event(
                "consumer_accepted_record",
                topic=msg.topic,
                partition=int(msg.partition),
                offset=int(msg.offset),
                workflow_id=workflow_id,
                logical_key=logical_key,
            )
            if sleep_seconds:
                event(
                    "consumer_sleep_before_effect",
                    logical_key=payload["logical_key"],
                    offset=msg.offset,
                    seconds=sleep_seconds,
                )
                time.sleep(sleep_seconds)
            conn.execute(
                sa.text(
                    f"""
                    INSERT INTO {OFFSET_TABLE}
                        (topic, partition_id, offset_id, workflow_id, logical_key,
                         payload, producer_key, observed_at)
                    VALUES
                        (:topic, :partition_id, :offset_id, :workflow_id, :logical_key,
                         :payload, :producer_key, :observed_at)
                    ON CONFLICT (topic, partition_id, offset_id) DO NOTHING
                    """
                ),
                {
                    "topic": msg.topic,
                    "partition_id": int(msg.partition),
                    "offset_id": int(msg.offset),
                    "workflow_id": workflow_id,
                    "logical_key": logical_key,
                    "payload": payload_value,
                    "producer_key": producer_key,
                    "observed_at": time.time(),
                },
            )
            conn.execute(
                sa.text(
                    f"""
                    INSERT INTO {EFFECT_TABLE}
                        (logical_key, first_payload, first_workflow_id, first_topic,
                         first_partition, first_offset, effect_count, created_at)
                    VALUES
                        (:logical_key, :payload, :workflow_id, :topic, :partition_id,
                         :offset_id, 1, :created_at)
                    ON CONFLICT (logical_key) DO NOTHING
                    """
                ),
                {
                    "logical_key": logical_key,
                    "payload": payload_value,
                    "workflow_id": workflow_id,
                    "topic": msg.topic,
                    "partition_id": int(msg.partition),
                    "offset_id": int(msg.offset),
                    "created_at": time.time(),
                },
            )
    finally:
        engine.dispose()


def register_consumer(plan: CasePlan, app_url: str, bootstrap_servers: str) -> None:
    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": plan.group_id,
            "auto.offset.reset": "earliest",
        },
        [plan.topic],
    )
    @DBOS.workflow(name=f"kafka_event_workflow_{plan.seed}_{plan.case_id.replace('-', '_')}")
    def kafka_event_workflow(msg: KafkaMessage) -> None:
        record_kafka_effect(app_url, msg)


def produce_records(plan: CasePlan, bootstrap_servers: str) -> list[dict[str, Any]]:
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    delivered: list[dict[str, Any]] = []
    errors: list[str] = []

    def delivery_report(err: Any, msg: Any) -> None:
        if err is not None:
            errors.append(str(err))
            return
        delivered.append(
            {
                "topic": msg.topic(),
                "partition": int(msg.partition()),
                "offset": int(msg.offset()),
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "value": msg.value().decode("utf-8") if msg.value() else None,
            }
        )

    for index, record in enumerate(plan.records, start=1):
        if record.delay_before_seconds:
            time.sleep(record.delay_before_seconds)
        value = {
            "frontier": FRONTIER_ID,
            "rung_id": plan.rung_id,
            "case_id": plan.case_id,
            "seed": plan.seed,
            "logical_key": record.logical_key,
            "payload": record.payload,
            "record_index": index,
            "workflow_sleep_seconds": record.workflow_sleep_seconds,
        }
        producer.produce(
            plan.topic,
            key=record.producer_key.encode("utf-8"),
            value=json.dumps(value, sort_keys=True).encode("utf-8"),
            partition=0,
            on_delivery=delivery_report,
        )
        producer.poll(0)

    producer.flush(15)
    if errors:
        raise WorkloadFailure(f"kafka delivery errors: {errors}")
    delivered.sort(key=lambda row: (row["partition"], row["offset"]))
    event("kafka_records_produced", delivered=delivered)
    return delivered


def ledger_rows(app_url: str, topic: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    engine = sa.create_engine(app_url)
    try:
        with engine.begin() as conn:
            offsets = [
                dict(row._mapping)
                for row in conn.execute(
                    sa.text(
                        f"""
                        SELECT topic, partition_id, offset_id, workflow_id, logical_key,
                               payload, producer_key
                        FROM {OFFSET_TABLE}
                        WHERE topic = :topic
                        ORDER BY partition_id, offset_id
                        """
                    ),
                    {"topic": topic},
                )
            ]
            effects = [
                dict(row._mapping)
                for row in conn.execute(
                    sa.text(
                        f"""
                        SELECT logical_key, first_payload, first_workflow_id, first_topic,
                               first_partition, first_offset, effect_count
                        FROM {EFFECT_TABLE}
                        ORDER BY logical_key
                        """
                    )
                )
            ]
            return offsets, effects
    finally:
        engine.dispose()


def acceptance_rows(app_url: str, topic: str) -> list[dict[str, Any]]:
    engine = sa.create_engine(app_url)
    try:
        with engine.begin() as conn:
            return [
                dict(row._mapping)
                for row in conn.execute(
                    sa.text(
                        f"""
                        SELECT topic, partition_id, offset_id, workflow_id, logical_key,
                               payload, producer_key
                        FROM {ACCEPTANCE_TABLE}
                        WHERE topic = :topic
                        ORDER BY partition_id, offset_id
                        """
                    ),
                    {"topic": topic},
                )
            ]
    finally:
        engine.dispose()


def wait_for_acceptance(plan: CasePlan, app_url: str, expected_count: int) -> list[dict[str, Any]]:
    deadline = time.monotonic() + float(os.environ.get("WIO_KAFKA_ACCEPT_WAIT_SECONDS", "45"))
    last_rows: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        last_rows = acceptance_rows(app_url, plan.topic)
        if len(last_rows) >= expected_count:
            event(
                "consumer_acceptance_observed",
                topic=plan.topic,
                expected_count=expected_count,
                observed_count=len(last_rows),
                accepted_workflow_ids=sorted(row["workflow_id"] for row in last_rows),
            )
            return last_rows
        time.sleep(0.25)
    event(
        "consumer_acceptance_timeout",
        topic=plan.topic,
        expected_count=expected_count,
        observed_count=len(last_rows),
        accepted_workflow_ids=sorted(row["workflow_id"] for row in last_rows),
    )
    return last_rows


def wait_for_processing(
    plan: CasePlan, app_url: str, expected_offsets: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    deadline = time.monotonic() + float(os.environ.get("WIO_KAFKA_WAIT_SECONDS", "30"))
    last_offsets: list[dict[str, Any]] = []
    last_effects: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        last_offsets, last_effects = ledger_rows(app_url, plan.topic)
        if len(last_offsets) >= expected_offsets:
            return last_offsets, last_effects
        time.sleep(0.25)
    return last_offsets, last_effects


def workflow_statuses(prefix: str) -> list[dict[str, Any]]:
    rows = DBOS.list_workflows(workflow_id_prefix=prefix)
    payload: list[dict[str, Any]] = []
    for row in rows:
        payload.append(
            {
                "workflow_id": row.workflow_id,
                "status": row.status,
                "name": row.name,
                "queue_name": row.queue_name,
            }
        )
    payload.sort(key=lambda item: item["workflow_id"])
    return payload


def relaunch_dbos(
    plan: CasePlan, app_url: str, sys_url: str, bootstrap_servers: str
) -> None:
    event(
        "dbos_kafka_consumer_relaunching",
        topic=plan.topic,
        group_id=plan.group_id,
        rung=plan.rung_id,
        case_id=plan.case_id,
    )
    DBOS.destroy(destroy_registry=True)
    register_consumer(plan, app_url, bootstrap_servers)
    DBOS(config=make_config(plan, app_url, sys_url))
    DBOS.launch()
    event(
        "dbos_kafka_consumer_relaunched",
        topic=plan.topic,
        group_id=plan.group_id,
        rung=plan.rung_id,
        case_id=plan.case_id,
    )


def assert_case_oracle(
    plan: CasePlan,
    delivered: list[dict[str, Any]],
    offsets: list[dict[str, Any]],
    effects: list[dict[str, Any]],
    statuses: list[dict[str, Any]],
) -> None:
    delivered_model = [
        {
            **row,
            "workflow_id": workflow_id_for(plan, int(row["partition"]), int(row["offset"])),
        }
        for row in delivered
    ]
    expected_workflow_ids = sorted(row["workflow_id"] for row in delivered_model)
    observed_workflow_ids = sorted(row["workflow_id"] for row in offsets)
    terminal_workflows = sorted(
        row["workflow_id"] for row in statuses if row.get("status") == "SUCCESS"
    )
    unique_keys = sorted({record.logical_key for record in plan.records})
    first_payload_by_key: dict[str, str] = {}
    for record in plan.records:
        first_payload_by_key.setdefault(record.logical_key, record.payload)
    effect_by_key = {row["logical_key"]: row for row in effects}

    invariant(
        "kafka_delivery_offsets_modeled",
        len(delivered) == len(plan.records)
        and all(row["topic"] == plan.topic for row in delivered)
        and all(row["partition"] == 0 for row in delivered),
        delivered=delivered,
        expected_records=len(plan.records),
    )
    invariant(
        "consumer_observed_every_produced_offset",
        observed_workflow_ids == expected_workflow_ids,
        expected_workflow_ids=expected_workflow_ids,
        observed_workflow_ids=observed_workflow_ids,
        offsets=offsets,
    )
    invariant(
        "terminal_workflows_match_offsets",
        terminal_workflows == expected_workflow_ids,
        expected_workflow_ids=expected_workflow_ids,
        terminal_workflows=terminal_workflows,
        statuses=statuses,
    )
    invariant(
        "one_effect_per_logical_key",
        sorted(effect_by_key) == unique_keys and all(row["effect_count"] == 1 for row in effects),
        expected_keys=unique_keys,
        effects=effects,
    )
    invariant(
        "first_payload_wins_for_duplicate_key",
        all(
            effect_by_key[key]["first_payload"] == first_payload
            for key, first_payload in first_payload_by_key.items()
        ),
        expected=first_payload_by_key,
        effects=effects,
    )


def run_case(plan: CasePlan, artifacts: Path) -> dict[str, Any]:
    bootstrap_servers = kafka_bootstrap_servers(artifacts)
    write_json(artifacts / "case-plan.json", asdict(plan))
    if _KAFKA_MOCK_HOLDER is not None and not mock_supports_plan(plan):
        raise SetupBlock(
            "librdkafka mock broker is only valid for non-relaunch/non-restart/non-delete Kafka rungs"
        )
    preflight_kafka(bootstrap_servers, artifacts)
    app_url, sys_url, masked_admin = prepare_databases(plan.database_prefix, artifacts)
    dbos_started = False
    try:
        init_app_ledger(app_url)
        create_topic(bootstrap_servers, plan.topic, artifacts)
        delivered = produce_records(plan, bootstrap_servers)

        if plan.broker_restart_phase == "after_produce_before_consumer":
            restart_kafka_broker(bootstrap_servers, artifacts, plan.broker_restart_phase)

        DBOS.destroy(destroy_registry=True)
        register_consumer(plan, app_url, bootstrap_servers)
        DBOS(config=make_config(plan, app_url, sys_url))
        DBOS.launch()
        dbos_started = True
        event(
            "dbos_kafka_consumer_launched",
            topic=plan.topic,
            group_id=plan.group_id,
            expected_records=len(delivered),
        )

        if plan.broker_restart_phase == "after_consumer_launch_delay":
            if plan.broker_restart_delay_seconds:
                time.sleep(plan.broker_restart_delay_seconds)
            restart_kafka_broker(bootstrap_servers, artifacts, plan.broker_restart_phase)

        if plan.relaunch_after_broker_restart:
            relaunch_dbos(plan, app_url, sys_url, bootstrap_servers)

        if plan.relaunch_delay_seconds:
            if plan.relaunch_after_accept_count:
                accepted = wait_for_acceptance(
                    plan, app_url, plan.relaunch_after_accept_count
                )
                write_json(artifacts / "pre-relaunch-acceptance-ledger.json", accepted)
                if len(accepted) < plan.relaunch_after_accept_count:
                    raise SetupBlock(
                        "Kafka consumer did not accept enough records before relaunch "
                        f"for {plan.schedule}: expected={plan.relaunch_after_accept_count} "
                        f"observed={len(accepted)}"
                    )
            time.sleep(plan.relaunch_delay_seconds)
            relaunch_dbos(plan, app_url, sys_url, bootstrap_servers)

        if plan.post_terminal_relaunch:
            before_offsets, before_effects = wait_for_processing(
                plan, app_url, len(delivered)
            )
            before_statuses = workflow_statuses(f"kafka-unique-id-{plan.topic}-")
            write_json(artifacts / "pre-relaunch-offset-ledger.json", before_offsets)
            write_json(artifacts / "pre-relaunch-effect-ledger.json", before_effects)
            write_json(artifacts / "pre-relaunch-workflow-statuses.json", before_statuses)
            relaunch_dbos(plan, app_url, sys_url, bootstrap_servers)
            if plan.stable_after_relaunch_seconds:
                time.sleep(plan.stable_after_relaunch_seconds)

        offsets, effects = wait_for_processing(plan, app_url, len(delivered))
        accepted = acceptance_rows(app_url, plan.topic)
        prefix = f"kafka-unique-id-{plan.topic}-"
        statuses = workflow_statuses(prefix)
        write_json(artifacts / "produced-records.json", delivered)
        write_json(artifacts / "acceptance-ledger.json", accepted)
        write_json(artifacts / "offset-ledger.json", offsets)
        write_json(artifacts / "effect-ledger.json", effects)
        write_json(artifacts / "workflow-statuses.json", statuses)
        assert_case_oracle(plan, delivered, offsets, effects, statuses)

        if plan.late_read_after_idle_seconds:
            before_late_offsets = offsets
            before_late_effects = effects
            before_late_statuses = statuses
            time.sleep(plan.late_read_after_idle_seconds)
            late_offsets, late_effects = ledger_rows(app_url, plan.topic)
            late_statuses = workflow_statuses(prefix)
            write_json(artifacts / "late-read-offset-ledger.json", late_offsets)
            write_json(artifacts / "late-read-effect-ledger.json", late_effects)
            write_json(artifacts / "late-read-workflow-statuses.json", late_statuses)
            invariant(
                "late_read_does_not_reconsume_record",
                late_offsets == before_late_offsets
                and late_effects == before_late_effects
                and late_statuses == before_late_statuses,
                before_offsets=before_late_offsets,
                late_offsets=late_offsets,
                before_effects=before_late_effects,
                late_effects=late_effects,
                before_statuses=before_late_statuses,
                late_statuses=late_statuses,
            )

        if plan.delete_topic_after_terminal:
            active_before_cleanup = [
                row for row in statuses if row.get("status") not in {"SUCCESS", "ERROR", "CANCELLED"}
            ]
            invariant(
                "cleanup_only_after_terminal_workflows",
                not active_before_cleanup,
                active_workflows=active_before_cleanup,
                statuses=statuses,
            )
            delete_topic(bootstrap_servers, plan.topic, artifacts)
            post_cleanup_offsets, post_cleanup_effects = ledger_rows(app_url, plan.topic)
            post_cleanup_statuses = workflow_statuses(prefix)
            write_json(artifacts / "post-cleanup-offset-ledger.json", post_cleanup_offsets)
            write_json(artifacts / "post-cleanup-effect-ledger.json", post_cleanup_effects)
            write_json(artifacts / "post-cleanup-workflow-statuses.json", post_cleanup_statuses)
            invariant(
                "cleanup_preserves_terminal_ledger",
                post_cleanup_offsets == offsets
                and post_cleanup_effects == effects
                and post_cleanup_statuses == statuses,
                post_cleanup_offsets=post_cleanup_offsets,
                post_cleanup_effects=post_cleanup_effects,
                post_cleanup_statuses=post_cleanup_statuses,
            )

        result = {
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case": plan.case_id,
            "seed": plan.seed,
            "status": "passed",
            "topic": plan.topic,
            "group_id": plan.group_id,
            "bootstrap_servers": bootstrap_servers,
            "admin_url": masked_admin,
            "delivered": delivered,
            "accepted": accepted,
            "offsets": offsets,
            "effects": effects,
            "workflow_statuses": statuses,
            "broker_restart_phase": plan.broker_restart_phase,
            "late_read_after_idle_seconds": plan.late_read_after_idle_seconds,
            "delete_topic_after_terminal": plan.delete_topic_after_terminal,
        }
        write_json(artifacts / "result.json", result)
        return result
    except (SetupBlock, WorkloadFailure):
        emit_case_debug_artifacts(artifacts)
        raise
    finally:
        if dbos_started:
            DBOS.destroy(destroy_registry=True)
        else:
            DBOS.destroy(destroy_registry=True)
        stop_librdkafka_mock()
        stop_standalone_kafka_broker()
        drop_databases(plan.database_prefix)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WIO DBOS Kafka workload")
    parser.add_argument("--rung", required=True)
    parser.add_argument("--case", dest="case_id")
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument(
        "--artifact-dir",
        default="/tmp/wio-artifacts/kafka-exactly-once-consumer",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        rung_id = normalize_rung(args.rung)
        if args.all_cases:
            case_ids = case_ids_for_rung(rung_id)
        elif args.case_id:
            case_ids = [args.case_id]
        else:
            raise SetupBlock("--case or --all-cases is required")
        if args.all_cases and not args.sequential:
            raise SetupBlock("--all-cases requires --sequential because Kafka topics and DBOS globals are exclusive")

        root_artifacts = Path(args.artifact_dir)
        results = []
        for case_id in case_ids:
            plan = build_case_plan(rung_id, case_id)
            case_artifacts = root_artifacts / plan.rung_id / plan.case_id
            event(
                "case_started",
                frontier=FRONTIER_ID,
                rung=plan.rung_id,
                case_id=plan.case_id,
                seed=plan.seed,
                schedule=plan.schedule,
                focus=plan.focus,
                prompt_path=PROMPT_PATH,
            )
            results.append(run_case(plan, case_artifacts))
            event(
                "case_completed",
                rung=plan.rung_id,
                case_id=plan.case_id,
                seed=plan.seed,
                status="passed",
            )
        write_json(root_artifacts / f"{rung_id}-results.json", results)
        return 0
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 44
    except WorkloadFailure as exc:
        print(f"FINDING-CANDIDATE {exc}", flush=True)
        return 10


if __name__ == "__main__":
    raise SystemExit(main())
