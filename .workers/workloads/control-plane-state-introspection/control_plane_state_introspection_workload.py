#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import sqlalchemy as sa

try:
    from dbos import DBOS, DBOSClient, DBOSConfig
    from dbos._conductor.protocol import ScheduleOutput
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "control-plane-state-introspection"
RUNG_ID = "rung-001-rotten-schedule-context-introspection"
APP_ID = "wio-control-plane-state"
APP_VERSION = "wio-control-plane-rung-001"


class WorkloadFailure(Exception):
    pass


class SetupBlock(Exception):
    pass


class RottenScheduleContext6940:
    def __init__(self, seed: int, region: str) -> None:
        self.seed = seed
        self.region = region


class RottenScheduleContext6941:
    def __init__(self, seed: int, region: str) -> None:
        self.seed = seed
        self.region = region


class RottenScheduleContext6942:
    def __init__(self, seed: int, region: str) -> None:
        self.seed = seed
        self.region = region


ROTTEN_CLASS_BY_SEED = {
    6940: "RottenScheduleContext6940",
    6941: "RottenScheduleContext6941",
    6942: "RottenScheduleContext6942",
}


@dataclass(frozen=True)
class CasePlan:
    rung_id: str
    case_id: str
    seed: int
    schedule: str
    app_db: str
    sys_db: str
    prefix: str
    bad_schedule: str
    good_schedule: str
    other_schedule: str


def event(name: str, **fields: Any) -> None:
    parts = [f"WIO-EVENT {name}"]
    parts.extend(f"{key}={json.dumps(value, sort_keys=True)}" for key, value in fields.items())
    print(" ".join(parts), flush=True)


def invariant(name: str, ok: bool, **fields: Any) -> None:
    status = "PASS" if ok else "FAIL"
    summary = json.dumps(fields, sort_keys=True) if fields else "ok"
    print(f"INVARIANT {name} {name} {status} {summary}", flush=True)
    if not ok:
        raise WorkloadFailure(f"{name} failed: {summary}")


def password() -> str:
    return os.environ.get("PGPASSWORD", "dbos")


def db_url(database: str, *, driver: str = "postgresql+psycopg") -> str:
    return f"{driver}://postgres:{quote(password(), safe='')}@localhost:{os.environ.get('PGPORT', '5432')}/{database}"


def admin_engine() -> sa.Engine:
    return sa.create_engine(db_url("postgres"), connect_args={"connect_timeout": 10})


def cleanup_databases(plan: CasePlan) -> None:
    engine = admin_engine()
    try:
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            for database in (plan.app_db, plan.sys_db):
                conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'))
    finally:
        engine.dispose()


def config(plan: CasePlan) -> DBOSConfig:
    return {
        "name": APP_ID,
        "application_version": APP_VERSION,
        "application_database_url": db_url(plan.app_db, driver="postgresql"),
        "system_database_url": db_url(plan.sys_db),
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.05,
    }


def normalize_rung(rung: str) -> str:
    if rung in {"rung-001", RUNG_ID, "rung-001-rotten-schedule-context-introspection"}:
        return RUNG_ID
    raise SetupBlock(f"unsupported rung {rung}")


def make_plan(case_id: str, seed: int | None = None) -> CasePlan:
    cases = {
        "case-001": (6940, "runtime-client-mixed-rotten-contexts"),
        "case-002": (6941, "filtered-conductor-rotten-contexts"),
        "case-003": (6942, "lifecycle-repair-after-rotten-context"),
    }
    if case_id not in cases:
        raise SetupBlock(f"unknown case {case_id}")
    expected_seed, schedule = cases[case_id]
    if seed is not None and seed != expected_seed:
        raise SetupBlock(f"{case_id} requires seed {expected_seed}, got {seed}")
    suffix = f"001_{case_id.replace('-', '_')}_{expected_seed}_{uuid.uuid5(uuid.NAMESPACE_URL, f'{RUNG_ID}:{case_id}:{expected_seed}').hex[:8]}"
    prefix = f"wio-cp-{case_id}-{expected_seed}"
    return CasePlan(
        rung_id=RUNG_ID,
        case_id=case_id,
        seed=expected_seed,
        schedule=schedule,
        app_db=f"wio_control_app_{suffix}",
        sys_db=f"wio_control_sys_{suffix}",
        prefix=prefix,
        bad_schedule=f"{prefix}-bad",
        good_schedule=f"{prefix}-good",
        other_schedule=f"{prefix}-other",
    )


@DBOS.workflow()
def scheduled_good_workflow(scheduled_at: datetime, ctx: Any) -> dict[str, Any]:
    return {
        "scheduled_at_type": type(scheduled_at).__name__,
        "ctx": ctx,
        "ctx_type": type(ctx).__name__,
    }


@DBOS.workflow()
def scheduled_other_workflow(scheduled_at: datetime, ctx: Any) -> dict[str, Any]:
    return {
        "scheduled_at_type": type(scheduled_at).__name__,
        "ctx": ctx,
        "ctx_type": type(ctx).__name__,
    }


def launch(plan: CasePlan, *, clean: bool = True) -> DBOS:
    if clean:
        cleanup_databases(plan)
    os.environ["DBOS__APPID"] = APP_ID
    os.environ["DBOS__APPVERSION"] = APP_VERSION
    os.environ.pop("DBOS__VMID", None)
    DBOS.destroy(destroy_registry=False)
    dbos = DBOS(config=config(plan))
    DBOS.launch()
    event("case_start", frontier=FRONTIER_ID, **asdict(plan))
    return dbos


def rotten_context(seed: int) -> Any:
    class_name = ROTTEN_CLASS_BY_SEED[seed]
    return getattr(sys.modules[__name__], class_name)(seed, region=f"region-{seed}")


def remove_rotten_class(seed: int) -> str:
    class_name = ROTTEN_CLASS_BY_SEED[seed]
    if hasattr(sys.modules[__name__], class_name):
        delattr(sys.modules[__name__], class_name)
    return class_name


def good_context(plan: CasePlan) -> dict[str, Any]:
    return {"kind": "good", "case": plan.case_id, "seed": plan.seed, "rank": 1}


def other_context(plan: CasePlan) -> dict[str, Any]:
    return {"kind": "other", "case": plan.case_id, "seed": plan.seed, "rank": 2}


def workflow_name(fn: Any) -> str:
    return str(getattr(fn, "dbos_function_name"))


def apply_modeled_schedules(plan: CasePlan, dbos: DBOS) -> str:
    DBOS.apply_schedules(
        [
            {
                "schedule_name": plan.bad_schedule,
                "workflow_fn": scheduled_good_workflow,
                "schedule": "0 0 * * *",
                "context": rotten_context(plan.seed),
            },
            {
                "schedule_name": plan.good_schedule,
                "workflow_fn": scheduled_good_workflow,
                "schedule": "0 0 * * *",
                "context": good_context(plan),
            },
            {
                "schedule_name": plan.other_schedule,
                "workflow_fn": scheduled_other_workflow,
                "schedule": "0 0 * * *",
                "context": other_context(plan),
            },
        ]
    )
    raw_bad = dbos._sys_db.get_schedule(plan.bad_schedule)
    invariant(
        "raw_bad_schedule_context_captured",
        raw_bad is not None and isinstance(raw_bad["context"], str),
        schedule=plan.bad_schedule,
        context_type=type(raw_bad["context"]).__name__ if raw_bad else None,
    )
    removed_class = remove_rotten_class(plan.seed)
    event("rotten_context_class_removed", class_name=removed_class, raw_bad_context=raw_bad["context"])
    return str(raw_bad["context"])


def by_name(schedules: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(schedule["schedule_name"]): schedule for schedule in schedules}


def names(schedules: list[dict[str, Any]]) -> list[str]:
    return sorted(str(schedule["schedule_name"]) for schedule in schedules)


def assert_schedule_set(name: str, schedules: list[dict[str, Any]], expected: set[str]) -> None:
    observed = set(names(schedules))
    invariant(name, observed == expected, observed=sorted(observed), expected=sorted(expected), count=len(schedules))


def assert_context_model(name: str, schedules: list[dict[str, Any]], plan: CasePlan, raw_bad_context: str) -> None:
    schedule_by_name = by_name(schedules)
    expected_names = {plan.bad_schedule, plan.good_schedule, plan.other_schedule}
    ok = (
        set(schedule_by_name) == expected_names
        and schedule_by_name[plan.bad_schedule]["context"] == raw_bad_context
        and isinstance(schedule_by_name[plan.bad_schedule]["context"], str)
        and schedule_by_name[plan.good_schedule]["context"] == good_context(plan)
        and schedule_by_name[plan.other_schedule]["context"] == other_context(plan)
    )
    invariant(
        name,
        ok,
        observed_names=sorted(schedule_by_name),
        expected_names=sorted(expected_names),
        bad_context_type=type(schedule_by_name.get(plan.bad_schedule, {}).get("context")).__name__,
        bad_context_matches=schedule_by_name.get(plan.bad_schedule, {}).get("context") == raw_bad_context,
        good_context=schedule_by_name.get(plan.good_schedule, {}).get("context"),
        other_context=schedule_by_name.get(plan.other_schedule, {}).get("context"),
    )


def assert_bad_get(name: str, schedule: dict[str, Any] | None, plan: CasePlan, raw_bad_context: str) -> None:
    invariant(
        name,
        schedule is not None
        and schedule["schedule_name"] == plan.bad_schedule
        and schedule["context"] == raw_bad_context
        and isinstance(schedule["context"], str),
        schedule=schedule,
        raw_bad_context=raw_bad_context,
    )


def assert_good_get(name: str, schedule: dict[str, Any] | None, plan: CasePlan) -> None:
    invariant(
        name,
        schedule is not None
        and schedule["schedule_name"] == plan.good_schedule
        and schedule["context"] == good_context(plan),
        schedule=schedule,
        expected_context=good_context(plan),
    )


def raw_schedules(dbos: DBOS, plan: CasePlan) -> list[dict[str, Any]]:
    return dbos._sys_db.list_schedules(schedule_name_prefix=plan.prefix)


def raw_schedule_names(dbos: DBOS, plan: CasePlan) -> list[str]:
    return names(raw_schedules(dbos, plan))


async def async_observations(client: DBOSClient, plan: CasePlan) -> dict[str, Any]:
    return {
        "runtime_list": await DBOS.list_schedules_async(schedule_name_prefix=plan.prefix),
        "runtime_bad": await DBOS.get_schedule_async(plan.bad_schedule),
        "client_list": await client.list_schedules_async(schedule_name_prefix=plan.prefix),
        "client_bad": await client.get_schedule_async(plan.bad_schedule),
    }


def runtime_client_mixed_rotten_contexts(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    raw_bad_context = apply_modeled_schedules(plan, dbos)
    client = DBOSClient(system_database_url=db_url(plan.sys_db))
    try:
        runtime_list = DBOS.list_schedules(schedule_name_prefix=plan.prefix)
        runtime_bad = DBOS.get_schedule(plan.bad_schedule)
        runtime_good = DBOS.get_schedule(plan.good_schedule)
        client_list = client.list_schedules(schedule_name_prefix=plan.prefix)
        client_bad = client.get_schedule(plan.bad_schedule)
        client_good = client.get_schedule(plan.good_schedule)
        async_seen = asyncio.run(async_observations(client, plan))
    finally:
        client.destroy()

    expected = {plan.bad_schedule, plan.good_schedule, plan.other_schedule}
    assert_schedule_set("runtime_list_all_modeled_schedules", runtime_list, expected)
    assert_context_model("runtime_list_context_model", runtime_list, plan, raw_bad_context)
    assert_bad_get("runtime_get_bad_raw_context", runtime_bad, plan, raw_bad_context)
    assert_good_get("runtime_get_good_deserialized_context", runtime_good, plan)
    assert_schedule_set("client_list_all_modeled_schedules", client_list, expected)
    assert_context_model("client_list_context_model", client_list, plan, raw_bad_context)
    assert_bad_get("client_get_bad_raw_context", client_bad, plan, raw_bad_context)
    assert_good_get("client_get_good_deserialized_context", client_good, plan)
    assert_context_model("runtime_async_list_context_model", async_seen["runtime_list"], plan, raw_bad_context)
    assert_bad_get("runtime_async_get_bad_raw_context", async_seen["runtime_bad"], plan, raw_bad_context)
    assert_context_model("client_async_list_context_model", async_seen["client_list"], plan, raw_bad_context)
    assert_bad_get("client_async_get_bad_raw_context", async_seen["client_bad"], plan, raw_bad_context)
    invariant("raw_table_schedule_identity_preserved", raw_schedule_names(dbos, plan) == sorted(expected), raw_names=raw_schedule_names(dbos, plan), expected=sorted(expected))
    return {
        "raw_bad_context": raw_bad_context,
        "runtime": runtime_list,
        "client": client_list,
        "async": async_seen,
        "raw_names": raw_schedule_names(dbos, plan),
    }


def assert_filter_result(name: str, observed: list[dict[str, Any]], expected: set[str], plan: CasePlan, raw_bad_context: str) -> None:
    assert_schedule_set(name, observed, expected)
    schedule_by_name = by_name(observed)
    if plan.bad_schedule in expected:
        invariant(
            f"{name}_bad_context_raw",
            schedule_by_name[plan.bad_schedule]["context"] == raw_bad_context,
            context=schedule_by_name[plan.bad_schedule]["context"],
            raw_bad_context=raw_bad_context,
        )
    if plan.good_schedule in expected:
        invariant(
            f"{name}_good_context_decoded",
            schedule_by_name[plan.good_schedule]["context"] == good_context(plan),
            context=schedule_by_name[plan.good_schedule]["context"],
            expected_context=good_context(plan),
        )


def filtered_conductor_rotten_contexts(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    raw_bad_context = apply_modeled_schedules(plan, dbos)
    DBOS.pause_schedule(plan.bad_schedule)
    client = DBOSClient(system_database_url=db_url(plan.sys_db))
    try:
        runtime_filters = {
            "active": DBOS.list_schedules(status="ACTIVE", schedule_name_prefix=plan.prefix),
            "paused": DBOS.list_schedules(status="PAUSED", schedule_name_prefix=plan.prefix),
            "workflow_good": DBOS.list_schedules(workflow_name=workflow_name(scheduled_good_workflow), schedule_name_prefix=plan.prefix),
            "workflow_other": DBOS.list_schedules(workflow_name=workflow_name(scheduled_other_workflow), schedule_name_prefix=plan.prefix),
            "prefix_bad": DBOS.list_schedules(schedule_name_prefix=plan.bad_schedule),
            "combined_bad": DBOS.list_schedules(status="PAUSED", schedule_name_prefix=plan.bad_schedule),
        }
        client_filters = {
            "active": client.list_schedules(status="ACTIVE", schedule_name_prefix=plan.prefix),
            "paused": client.list_schedules(status="PAUSED", schedule_name_prefix=plan.prefix),
            "workflow_good": client.list_schedules(workflow_name=workflow_name(scheduled_good_workflow), schedule_name_prefix=plan.prefix),
            "workflow_other": client.list_schedules(workflow_name=workflow_name(scheduled_other_workflow), schedule_name_prefix=plan.prefix),
            "prefix_bad": client.list_schedules(schedule_name_prefix=plan.bad_schedule),
            "combined_bad": client.list_schedules(status="PAUSED", schedule_name_prefix=plan.bad_schedule),
        }
    finally:
        client.destroy()

    active_expected = {plan.good_schedule, plan.other_schedule}
    paused_expected = {plan.bad_schedule}
    workflow_good_expected = {plan.bad_schedule, plan.good_schedule}
    workflow_other_expected = {plan.other_schedule}
    expectations = {
        "active": active_expected,
        "paused": paused_expected,
        "workflow_good": workflow_good_expected,
        "workflow_other": workflow_other_expected,
        "prefix_bad": paused_expected,
        "combined_bad": paused_expected,
    }
    for key, expected in expectations.items():
        assert_filter_result(f"runtime_filter_{key}_matches_model", runtime_filters[key], expected, plan, raw_bad_context)
        assert_filter_result(f"client_filter_{key}_matches_model", client_filters[key], expected, plan, raw_bad_context)

    raw_by_name = by_name(raw_schedules(dbos, plan))
    conductor_loaded = {
        name: ScheduleOutput.from_schedule(raw_by_name[name], dbos._sys_db.serializer, load_context=True)
        for name in sorted(raw_by_name)
    }
    conductor_unloaded = {
        name: ScheduleOutput.from_schedule(raw_by_name[name], dbos._sys_db.serializer, load_context=False)
        for name in sorted(raw_by_name)
    }
    invariant(
        "conductor_loaded_bad_context_raw",
        conductor_loaded[plan.bad_schedule].context == raw_bad_context
        and conductor_loaded[plan.bad_schedule].schedule_name == plan.bad_schedule
        and conductor_loaded[plan.bad_schedule].status == "PAUSED",
        bad_output=asdict(conductor_loaded[plan.bad_schedule]),
        raw_bad_context=raw_bad_context,
    )
    invariant(
        "conductor_loaded_good_context_present",
        conductor_loaded[plan.good_schedule].context == str(good_context(plan))
        and conductor_loaded[plan.good_schedule].schedule_name == plan.good_schedule,
        good_output=asdict(conductor_loaded[plan.good_schedule]),
        expected_context=str(good_context(plan)),
    )
    invariant(
        "conductor_unloaded_contexts_omitted",
        all(output.context is None for output in conductor_unloaded.values()),
        outputs={name: asdict(output) for name, output in conductor_unloaded.items()},
    )
    return {
        "raw_bad_context": raw_bad_context,
        "runtime_filters": runtime_filters,
        "client_filters": client_filters,
        "conductor_loaded": {name: asdict(output) for name, output in conductor_loaded.items()},
        "conductor_unloaded": {name: asdict(output) for name, output in conductor_unloaded.items()},
    }


def lifecycle_repair_after_rotten_context(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    raw_bad_context = apply_modeled_schedules(plan, dbos)
    client = DBOSClient(system_database_url=db_url(plan.sys_db))
    try:
        client.pause_schedule(plan.bad_schedule)
        paused_bad = client.get_schedule(plan.bad_schedule)
        assert_bad_get("lifecycle_client_get_bad_after_pause", paused_bad, plan, raw_bad_context)
        invariant(
            "lifecycle_bad_schedule_paused",
            paused_bad is not None and paused_bad["status"] == "PAUSED",
            schedule=paused_bad,
        )
        client.resume_schedule(plan.bad_schedule)
        resumed_bad = client.get_schedule(plan.bad_schedule)
        assert_bad_get("lifecycle_client_get_bad_after_resume", resumed_bad, plan, raw_bad_context)
        invariant(
            "lifecycle_bad_schedule_resumed",
            resumed_bad is not None and resumed_bad["status"] == "ACTIVE",
            schedule=resumed_bad,
        )
        client.delete_schedule(plan.bad_schedule)
        after_bad_delete = client.list_schedules(schedule_name_prefix=plan.prefix)
        client_good_after_delete = client.get_schedule(plan.good_schedule)
    finally:
        client.destroy()

    remaining_expected = {plan.good_schedule, plan.other_schedule}
    assert_filter_result("lifecycle_bad_delete_preserves_good_schedules", after_bad_delete, remaining_expected, plan, raw_bad_context)
    assert_good_get("lifecycle_client_good_context_after_bad_delete", client_good_after_delete, plan)

    trigger_handle = DBOS.trigger_schedule(plan.good_schedule)
    trigger_result = trigger_handle.get_result()
    invariant(
        "lifecycle_unrelated_good_schedule_triggers",
        trigger_result["ctx"] == good_context(plan)
        and trigger_result["ctx_type"] == "dict"
        and trigger_result["scheduled_at_type"] == "datetime",
        workflow_id=trigger_handle.workflow_id,
        trigger_result=trigger_result,
        expected_context=good_context(plan),
    )

    DBOS.delete_schedule(plan.good_schedule)
    DBOS.delete_schedule(plan.other_schedule)
    final_public = DBOS.list_schedules(schedule_name_prefix=plan.prefix)
    final_raw = raw_schedules(dbos, plan)
    invariant(
        "lifecycle_final_cleanup_empty",
        final_public == [] and final_raw == [],
        public=final_public,
        raw=final_raw,
    )
    return {
        "raw_bad_context": raw_bad_context,
        "after_bad_delete": after_bad_delete,
        "trigger_workflow_id": trigger_handle.workflow_id,
        "trigger_result": trigger_result,
        "final_public": final_public,
        "final_raw": final_raw,
    }


def write_artifact(artifact_dir: Path, plan: CasePlan, result: dict[str, Any]) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    data = {"plan": asdict(plan), "result": result}
    (artifact_dir / f"{plan.case_id}.json").write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_plan_artifact(artifact_dir: Path, plan: CasePlan) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    data = {
        "frontier_id": FRONTIER_ID,
        "rung_id": plan.rung_id,
        "case_id": plan.case_id,
        "seed": plan.seed,
        "schedule": plan.schedule,
        "derived_plan": asdict(plan),
    }
    (artifact_dir / f"{plan.case_id}-plan.json").write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def run_case(plan: CasePlan, artifact_dir: Path) -> int:
    write_plan_artifact(artifact_dir, plan)
    dbos = launch(plan)
    try:
        if plan.schedule == "runtime-client-mixed-rotten-contexts":
            result = runtime_client_mixed_rotten_contexts(dbos, plan)
        elif plan.schedule == "filtered-conductor-rotten-contexts":
            result = filtered_conductor_rotten_contexts(dbos, plan)
        elif plan.schedule == "lifecycle-repair-after-rotten-context":
            result = lifecycle_repair_after_rotten_context(dbos, plan)
        else:
            raise SetupBlock(f"unsupported schedule {plan.schedule}")
        write_artifact(artifact_dir, plan, result)
        event("case_passed", case=plan.case_id, result=result)
        return 0
    finally:
        DBOS.destroy(destroy_registry=False)
        if os.environ.get("WIO_CLEANUP_AFTER") == "1":
            cleanup_databases(plan)


def case_ids_for_rung(rung: str) -> list[str]:
    if rung == RUNG_ID:
        return ["case-001", "case-002", "case-003"]
    raise SetupBlock(f"unsupported rung {rung}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DBOS control-plane state introspection workload")
    parser.add_argument("--rung", default=RUNG_ID)
    parser.add_argument("--case", choices=["case-001", "case-002", "case-003"])
    parser.add_argument("--seed", type=int)
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument("--artifact-dir", default="/tmp/wio-artifacts/control-plane-state-introspection")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rung = normalize_rung(args.rung)
    if args.all_cases:
        cases = case_ids_for_rung(rung)
    elif args.case:
        cases = [args.case]
    else:
        raise SetupBlock("--case or --all-cases is required")
    if args.all_cases and not args.sequential:
        raise SetupBlock("--all-cases currently requires --sequential to keep DBOS global state isolated")
    try:
        for case_id in cases:
            seed = args.seed if not args.all_cases else None
            run_case(make_plan(case_id, seed), Path(args.artifact_dir))
        return 0
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 42
    except WorkloadFailure as exc:
        print(f"WORKLOAD-FAIL {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
