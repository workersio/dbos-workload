#!/usr/bin/env python3
"""WIO workload for DBOS portable input type fidelity.

Frontier: portable-input-type-fidelity
Rung: rung-001-scheduled-datetime-portable-roundtrip
Protected product promise:
  Portable JSON workflow inputs annotated as datetime/date are restored or
  validated consistently across scheduler, queued, class/instance, direct-row,
  invalid-value, and relaunch reads.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[3]
VENV_SITE_PACKAGES = (
    next(
        (REPO_ROOT / ".workers" / "vendor" / "dbos-venv" / "lib").glob(
            "python*/site-packages"
        ),
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
    venv_site = str(VENV_SITE_PACKAGES)
    if venv_site not in sys.path:
        sys.path.append(venv_site)

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
        DBOS,
        DBOSConfiguredInstance,
        DBOSConfig,
        pydantic_args_validator,
    )
    from dbos._serialization import (
        DBOSPortableJSONSerializer,
        WorkflowSerializationFormat,
    )
except Exception as exc:  # pragma: no cover - setup path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "portable-input-type-fidelity"
RUNG_ID = "rung-001-scheduled-datetime-portable-roundtrip"
APP_ID = "wio-portable-input-type"
APP_VERSION = "wio-portable-input-type-rung-001"
RESULT_TIMEOUT_SECONDS = 20.0
TYPE_LEDGER: list[dict[str, Any]] = []
TYPED_INSTANCE: "TypedMethods | None" = None


class WorkloadFailure(Exception):
    pass


class SetupBlock(Exception):
    pass


@dataclass(frozen=True)
class CasePlan:
    case_id: str
    seed: int
    schedule: str
    focus: str
    database_prefix: str
    queue_name: str


def event(name: str, **fields: Any) -> None:
    print(
        " ".join(
            [f"WIO-EVENT {name}"]
            + [
                f"{key}={json.dumps(value, sort_keys=True, default=str)}"
                for key, value in fields.items()
            ]
        ),
        flush=True,
    )


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


def mask_url(raw_url: str) -> str:
    url = make_url(raw_url)
    if url.password is None:
        return raw_url
    return str(url.set(password="***"))


def prepare_databases(prefix: str, artifacts: Path) -> tuple[str, str, str]:
    base = admin_url()
    app_db = f"{prefix}_app"
    sys_db = f"{prefix}_sys"
    admin = str(base.set(database=base.database or "postgres"))
    masked = str(base.set(password="***")) if base.password is not None else str(base)
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
                "kind": "postgres_setup_failed",
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
    if os.environ.get("WIO_PORTABLE_INPUT_KEEP_DATABASES") == "1":
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
        event("database_cleanup_best_effort_failed", error_type=type(exc).__name__, error=str(exc))
    finally:
        engine.dispose()


def make_config(plan: CasePlan, app_url: str, sys_url: str) -> DBOSConfig:
    return {
        "name": APP_ID,
        "application_database_url": app_url,
        "system_database_url": sys_url,
        "application_version": f"{APP_VERSION}-{plan.case_id}",
        "executor_id": f"wio-portable-input-{plan.case_id}",
        "serializer": DBOSPortableJSONSerializer(),
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        "run_admin_server": False,
        "runtimeConfig": {"max_executor_threads": 8},
    }


def make_plan(case_id: str) -> CasePlan:
    specs = {
        "case-001": (7000, "scheduled-trigger-backfill-datetime", "scheduler trigger/backfill datetime coercion"),
        "case-002": (7001, "class-instance-date-param-alignment", "class/instance datetime/date hint alignment"),
        "case-003": (7002, "validation-boundaries-invalid-values", "pydantic and no-validator invalid boundaries"),
    }
    if case_id not in specs:
        raise SetupBlock(f"unknown case {case_id}")
    seed, schedule, focus = specs[case_id]
    return CasePlan(
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        focus=focus,
        database_prefix=f"wio_pin_{seed}_{case_id.replace('-', '_')}",
        queue_name=f"wio_portable_input_q_{seed}",
    )


def case_ids_for_rung(rung: str) -> list[str]:
    if rung != RUNG_ID and rung != "rung-001":
        raise SetupBlock(f"unsupported rung {rung}")
    return ["case-001", "case-002", "case-003"]


def type_ledger(label: str, **values: Any) -> dict[str, Any]:
    row = {"label": label}
    for key, value in values.items():
        row[f"{key}_type"] = type(value).__name__
        row[f"{key}_repr"] = repr(value)
        if isinstance(value, (datetime, date)):
            row[f"{key}_iso"] = value.isoformat()
        else:
            row[key] = value
    TYPE_LEDGER.append(row)
    return row


def error_signature(exc: BaseException | None) -> dict[str, Any]:
    if exc is None:
        return {"type": None, "message": None, "repr": None, "children": []}
    return {
        "type": type(exc).__name__,
        "module": type(exc).__module__,
        "base_types": [cls.__name__ for cls in type(exc).mro()],
        "message": str(exc),
        "repr": repr(exc),
        "portable_name": getattr(exc, "name", None),
        "portable_code": getattr(exc, "code", None),
        "portable_data": getattr(exc, "data", None),
        "cause": error_signature(exc.__cause__) if getattr(exc, "__cause__", None) else None,
        "context": error_signature(exc.__context__) if getattr(exc, "__context__", None) else None,
    }


def signature_text(signature: dict[str, Any]) -> str:
    pieces = []
    for key in ("type", "message", "repr", "portable_name", "portable_code"):
        if signature.get(key) is not None:
            pieces.append(str(signature[key]))
    for key in ("cause", "context"):
        if signature.get(key):
            pieces.append(signature_text(signature[key]))
    return "\n".join(pieces)


def bounded_call(name: str, fn: Callable[[], Any], timeout: float = RESULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    result_queue: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=1)

    def runner() -> None:
        try:
            result_queue.put(("returned", fn()), block=False)
        except BaseException as exc:
            result_queue.put(("exception", exc), block=False)

    started = time.monotonic()
    thread = threading.Thread(target=runner, name=f"wio-{name}", daemon=True)
    thread.start()
    thread.join(timeout)
    elapsed = time.monotonic() - started
    if thread.is_alive():
        return {"name": name, "timed_out": True, "elapsed_seconds": elapsed, "exception": None, "returned": None}
    kind, value = result_queue.get_nowait()
    return {
        "name": name,
        "timed_out": False,
        "elapsed_seconds": elapsed,
        "exception": error_signature(value) if kind == "exception" else None,
        "returned": value if kind == "returned" else None,
    }


def expect_return(name: str, outcome: dict[str, Any]) -> Any:
    invariant(f"{name}_completes", not outcome["timed_out"], outcome=outcome)
    invariant(f"{name}_returns", outcome["exception"] is None, outcome=outcome)
    return outcome["returned"]


def expect_error(name: str, outcome: dict[str, Any], marker: str) -> dict[str, Any]:
    invariant(f"{name}_completes", not outcome["timed_out"], outcome=outcome)
    signature = outcome.get("exception")
    invariant(
        f"{name}_raises_modeled_error",
        signature is not None and marker in signature_text(signature),
        outcome=outcome,
        marker=marker,
    )
    return signature


@DBOS.workflow()
def scheduled_typed_workflow(scheduled_at: datetime, ctx: dict[str, Any]) -> dict[str, Any]:
    return type_ledger("scheduled", scheduled_at=scheduled_at, ctx=ctx)


@DBOS.dbos_class("typedMethods")
class TypedMethods(DBOSConfiguredInstance):
    def __init__(self) -> None:
        super().__init__("typed-instance")

    @classmethod
    @DBOS.workflow(name="classTyped", serialization_type=WorkflowSerializationFormat.PORTABLE)
    def class_typed(cls, label: str, at: datetime, day: date) -> dict[str, Any]:
        return type_ledger(label, at=at, day=day)

    @DBOS.workflow(name="instanceTyped", serialization_type=WorkflowSerializationFormat.PORTABLE)
    def instance_typed(self, label: str, at: datetime, day: date) -> dict[str, Any]:
        return type_ledger(label, at=at, day=day)

    @classmethod
    @DBOS.workflow(
        name="validatedDatetime",
        serialization_type=WorkflowSerializationFormat.PORTABLE,
        validate_args=pydantic_args_validator,
    )
    def validated_datetime(cls, label: str, due: datetime, day: date) -> dict[str, Any]:
        return type_ledger(label, due=due, day=day)


@DBOS.workflow(name="noValidatorDatetime", serialization_type=WorkflowSerializationFormat.PORTABLE)
def no_validator_datetime(label: str, due: datetime) -> dict[str, Any]:
    row = type_ledger(label, due=due)
    if not isinstance(due, datetime):
        raise ValueError(f"WIO_PORTABLE_INVALID_RAW label={label} type={type(due).__name__}")
    return row


def get_typed_instance() -> TypedMethods:
    global TYPED_INSTANCE
    if TYPED_INSTANCE is None:
        TYPED_INSTANCE = TypedMethods()
    return TYPED_INSTANCE


def launch_dbos(config: DBOSConfig, plan: CasePlan) -> None:
    DBOS(config=config)
    get_typed_instance()
    DBOS.launch()
    DBOS.register_queue(plan.queue_name)


def relaunch_dbos(config: DBOSConfig, plan: CasePlan) -> None:
    DBOS.destroy(destroy_registry=False)
    launch_dbos(config, plan)


def insert_portable_row(
    sys_url: str,
    *,
    workflow_id: str,
    name: str,
    queue_name: str,
    args: list[Any],
    class_name: str | None = None,
) -> None:
    engine = sa.create_engine(sys_url, connect_args={"connect_timeout": 5})
    try:
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    """
                    INSERT INTO dbos.workflow_status(
                      workflow_uuid, name, class_name, queue_name, status,
                      inputs, created_at, serialization
                    )
                    VALUES (:workflow_uuid, :name, :class_name, :queue_name, 'ENQUEUED',
                            :inputs, :created_at, 'portable_json')
                    """
                ),
                {
                    "workflow_uuid": workflow_id,
                    "name": name,
                    "class_name": class_name,
                    "queue_name": queue_name,
                    "inputs": json.dumps({"positionalArgs": args, "namedArgs": {}}),
                    "created_at": int(time.time() * 1000),
                },
            )
    finally:
        engine.dispose()


def raw_input_row(sys_url: str, workflow_id: str) -> dict[str, Any]:
    engine = sa.create_engine(sys_url, connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as conn:
            row = conn.execute(
                sa.text(
                    """
                    SELECT workflow_uuid, status, name, class_name, queue_name, inputs, serialization, error
                    FROM dbos.workflow_status
                    WHERE workflow_uuid = :workflow_id
                    """
                ),
                {"workflow_id": workflow_id},
            ).mappings().one()
    finally:
        engine.dispose()
    out = dict(row)
    try:
        out["decoded_inputs"] = json.loads(out["inputs"]) if out.get("inputs") else None
    except Exception as exc:
        out["decoded_inputs"] = {"decode_error": f"{type(exc).__name__}: {exc}"}
    return out


def raw_row_has_portable_shape(row: dict[str, Any]) -> bool:
    decoded = row.get("decoded_inputs") or {}
    args = decoded.get("args")
    return (
        row.get("serialization") in {None, "portable_json"}
        and isinstance(args, list)
        and len(args) >= 1
        and isinstance(args[0], str)
    )


def assert_typed_row(name: str, row: dict[str, Any], datetime_key: str, date_key: str | None = None) -> None:
    invariant(
        f"{name}_datetime_type_restored",
        row.get(f"{datetime_key}_type") == "datetime",
        row=row,
    )
    if date_key:
        invariant(
            f"{name}_date_type_restored",
            row.get(f"{date_key}_type") == "date",
            row=row,
        )


def run_case_001(plan: CasePlan, sys_url: str, config: DBOSConfig) -> dict[str, Any]:
    schedule_name = f"wio-portable-schedule-{plan.seed}"
    DBOS.create_schedule(
        schedule_name=schedule_name,
        workflow_fn=scheduled_typed_workflow,
        schedule="0 * * * *",
        context={"seed": plan.seed, "kind": "trigger-backfill"},
    )
    trigger_handle = DBOS.trigger_schedule(schedule_name)
    trigger_result = expect_return(
        "scheduled_trigger_result",
        bounded_call("scheduled_trigger_result", lambda: trigger_handle.get_result()),
    )
    assert_typed_row("scheduled_trigger", trigger_result, "scheduled_at")

    start = datetime(2025, 6, 1, 0, 30, 0, tzinfo=timezone.utc)
    end = datetime(2025, 6, 1, 2, 30, 0, tzinfo=timezone.utc)
    backfill_handles = DBOS.backfill_schedule(schedule_name, start, end)
    backfill_results = [
        expect_return(
            f"scheduled_backfill_{idx}_result",
            bounded_call(f"scheduled_backfill_{idx}_result", lambda h=handle: h.get_result()),
        )
        for idx, handle in enumerate(backfill_handles)
    ]
    invariant("scheduled_backfill_count", len(backfill_results) == 2, count=len(backfill_results))
    for idx, result in enumerate(backfill_results):
        assert_typed_row(f"scheduled_backfill_{idx}", result, "scheduled_at")

    workflow_ids = [trigger_handle.workflow_id] + [handle.workflow_id for handle in backfill_handles]
    raw_rows_before = [raw_input_row(sys_url, workflow_id) for workflow_id in workflow_ids]
    invariant(
        "scheduled_raw_inputs_are_portable_shape",
        all(raw_row_has_portable_shape(row) for row in raw_rows_before),
        raw_rows=raw_rows_before,
    )

    relaunch_dbos(config, plan)
    relaunch_results = [
        expect_return(
            f"scheduled_relaunch_{idx}_result",
            bounded_call(
                f"scheduled_relaunch_{idx}_result",
                lambda workflow_id=workflow_id: DBOS.retrieve_workflow(workflow_id).get_result(),
            ),
        )
        for idx, workflow_id in enumerate(workflow_ids)
    ]
    invariant(
        "scheduled_relaunch_results_match",
        [row["scheduled_at_iso"] for row in relaunch_results]
        == [row["scheduled_at_iso"] for row in [trigger_result] + backfill_results],
        before=[trigger_result] + backfill_results,
        after=relaunch_results,
    )
    return {
        "workflow_ids": workflow_ids,
        "trigger_result": trigger_result,
        "backfill_results": backfill_results,
        "relaunch_results": relaunch_results,
        "raw_rows": raw_rows_before,
    }


def run_case_002(plan: CasePlan, sys_url: str) -> dict[str, Any]:
    dt = "2025-07-04T12:34:56+00:00"
    day = "2025-07-05"
    inst = get_typed_instance()

    class_handle = DBOS.enqueue_workflow(
        plan.queue_name,
        TypedMethods.class_typed,
        "class-enqueue",
        datetime.fromisoformat(dt),
        date.fromisoformat(day),
    )
    instance_handle = DBOS.enqueue_workflow(
        plan.queue_name,
        inst.instance_typed,
        "instance-enqueue",
        datetime.fromisoformat(dt),
        date.fromisoformat(day),
    )
    direct_id = f"{FRONTIER_ID}-{RUNG_ID}-{plan.case_id}-{plan.seed}-direct-class"
    insert_portable_row(
        sys_url,
        workflow_id=direct_id,
        name="classTyped",
        class_name="typedMethods",
        queue_name=plan.queue_name,
        args=["class-direct", dt, day],
    )
    direct_handle = DBOS.retrieve_workflow(direct_id)

    results = {
        "class_enqueue": expect_return(
            "class_enqueue_result",
            bounded_call("class_enqueue_result", lambda: class_handle.get_result()),
        ),
        "instance_enqueue": expect_return(
            "instance_enqueue_result",
            bounded_call("instance_enqueue_result", lambda: instance_handle.get_result()),
        ),
        "class_direct": expect_return(
            "class_direct_result",
            bounded_call("class_direct_result", lambda: direct_handle.get_result()),
        ),
    }
    for name, row in results.items():
        assert_typed_row(name, row, "at", "day")
    raw_rows = {
        "class_enqueue": raw_input_row(sys_url, class_handle.workflow_id),
        "instance_enqueue": raw_input_row(sys_url, instance_handle.workflow_id),
        "class_direct": raw_input_row(sys_url, direct_id),
    }
    invariant(
        "class_instance_raw_inputs_are_portable_json",
        all(row.get("serialization") == "portable_json" for row in raw_rows.values()),
        raw_rows=raw_rows,
    )
    return {"results": results, "raw_rows": raw_rows}


def run_case_003(plan: CasePlan, sys_url: str) -> dict[str, Any]:
    valid_id = f"{FRONTIER_ID}-{RUNG_ID}-{plan.case_id}-{plan.seed}-valid"
    invalid_string_id = f"{FRONTIER_ID}-{RUNG_ID}-{plan.case_id}-{plan.seed}-invalid-string"
    invalid_bool_id = f"{FRONTIER_ID}-{RUNG_ID}-{plan.case_id}-{plan.seed}-invalid-bool"
    no_validator_id = f"{FRONTIER_ID}-{RUNG_ID}-{plan.case_id}-{plan.seed}-no-validator"

    insert_portable_row(
        sys_url,
        workflow_id=valid_id,
        name="validatedDatetime",
        class_name="typedMethods",
        queue_name=plan.queue_name,
        args=["valid-pydantic", "2025-08-01T09:00:00+00:00", "2025-08-02"],
    )
    insert_portable_row(
        sys_url,
        workflow_id=invalid_string_id,
        name="validatedDatetime",
        class_name="typedMethods",
        queue_name=plan.queue_name,
        args=["invalid-string", "not-a-date-at-all", "2025-08-02"],
    )
    insert_portable_row(
        sys_url,
        workflow_id=invalid_bool_id,
        name="validatedDatetime",
        class_name="typedMethods",
        queue_name=plan.queue_name,
        args=["invalid-bool", True, "2025-08-02"],
    )
    insert_portable_row(
        sys_url,
        workflow_id=no_validator_id,
        name="noValidatorDatetime",
        queue_name=plan.queue_name,
        args=["no-validator-invalid", "not-a-date-at-all"],
    )

    valid_result = expect_return(
        "validated_valid_result",
        bounded_call("validated_valid_result", lambda: DBOS.retrieve_workflow(valid_id).get_result()),
    )
    assert_typed_row("validated_valid", valid_result, "due", "day")

    invalid_string_error = expect_error(
        "validated_invalid_string",
        bounded_call(
            "validated_invalid_string",
            lambda: DBOS.retrieve_workflow(invalid_string_id).get_result(),
        ),
        "ValueError",
    )
    invalid_bool_error = expect_error(
        "validated_invalid_bool",
        bounded_call(
            "validated_invalid_bool",
            lambda: DBOS.retrieve_workflow(invalid_bool_id).get_result(),
        ),
        "ValueError",
    )
    no_validator_error = expect_error(
        "no_validator_invalid_string",
        bounded_call(
            "no_validator_invalid_string",
            lambda: DBOS.retrieve_workflow(no_validator_id).get_result(),
        ),
        "WIO_PORTABLE_INVALID_RAW",
    )
    invariant(
        "invalid_values_not_recorded_as_typed_success",
        not any(row.get("label") in {"invalid-string", "invalid-bool", "no-validator-invalid"} and row.get("due_type") == "datetime" for row in TYPE_LEDGER),
        ledger=TYPE_LEDGER,
    )
    return {
        "valid_result": valid_result,
        "invalid_string_error": invalid_string_error,
        "invalid_bool_error": invalid_bool_error,
        "no_validator_error": no_validator_error,
        "raw_rows": {
            workflow_id: raw_input_row(sys_url, workflow_id)
            for workflow_id in [valid_id, invalid_string_id, invalid_bool_id, no_validator_id]
        },
    }


def run_case(plan: CasePlan, artifacts_root: Path) -> dict[str, Any]:
    TYPE_LEDGER.clear()
    artifacts = artifacts_root / RUNG_ID / plan.case_id
    artifacts.mkdir(parents=True, exist_ok=True)
    app_url, sys_url, admin_masked = prepare_databases(plan.database_prefix, artifacts)
    config = make_config(plan, app_url, sys_url)
    write_json(
        artifacts / "case.json",
        {
            **asdict(plan),
            "frontier": FRONTIER_ID,
            "rung": RUNG_ID,
            "app_url": mask_url(app_url),
            "system_database_url": mask_url(sys_url),
            "admin_url": admin_masked,
        },
    )
    try:
        event("case_start", frontier=FRONTIER_ID, rung=RUNG_ID, case=plan.case_id, seed=plan.seed)
        launch_dbos(config, plan)
        if plan.case_id == "case-001":
            observations = run_case_001(plan, sys_url, config)
        elif plan.case_id == "case-002":
            observations = run_case_002(plan, sys_url)
        elif plan.case_id == "case-003":
            observations = run_case_003(plan, sys_url)
        else:
            raise SetupBlock(f"unknown case {plan.case_id}")
        observations["type_ledger"] = list(TYPE_LEDGER)
        write_json(artifacts / "observations.json", observations)
        event("case_complete", rung=RUNG_ID, case=plan.case_id, status="passed")
        return {"case": asdict(plan), "status": "passed"}
    finally:
        try:
            DBOS.destroy(destroy_registry=False)
        except Exception as exc:
            event("dbos_destroy_best_effort_failed", error_type=type(exc).__name__, error=str(exc))
        drop_databases(plan.database_prefix)


def run_selected(args: argparse.Namespace) -> int:
    case_ids = case_ids_for_rung(args.rung) if args.all_cases else [args.case]
    if not args.all_cases and args.case is None:
        raise SetupBlock("--case is required unless --all-cases is set")
    artifacts_root = Path(args.artifacts_dir)
    results = [run_case(make_plan(case_id), artifacts_root) for case_id in case_ids]
    write_json(artifacts_root / RUNG_ID / "summary.json", results)
    event("rung_complete", rung=RUNG_ID, cases=len(results), status="passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rung", required=True)
    parser.add_argument("--case")
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true", help="accepted for WIO compatibility")
    parser.add_argument(
        "--artifacts-dir",
        default="/tmp/wio-artifacts/portable-input-type-fidelity",
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
