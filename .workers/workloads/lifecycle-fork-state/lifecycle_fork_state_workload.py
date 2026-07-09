#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

import sqlalchemy as sa

try:
    from dbos import (
        DBOS,
        DBOSClient,
        DBOSConfig,
        SetEnqueueOptions,
        SetWorkflowAttributes,
        SetWorkflowID,
        WorkflowStatus,
        WorkflowStatusString,
        error as dbos_error,
    )
    from dbos._schemas.application_database import ApplicationSchema
    from dbos._workflow_commands import garbage_collect, global_timeout
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "lifecycle-fork-state"
RUNG_001_ID = "rung-001-state-machine-core"
RUNG_002_ID = "rung-002-child-fork-event-attributes"
RUNG_003_ID = "rung-003-recovery-during-management"
RUNG_004_ID = "rung-004-bounded-seed-sweep"
RUNG_005_ID = "rung-005-cancel-children-terminal-immutability"
APP_ID = "wio-lifecycle-fork-state"
APP_VERSION = "wio-lifecycle-rungs-001-005"
QUEUE_NAME = "lifecycle_r001_queue"
IDLE_QUEUE_NAME = "lifecycle_r003_idle_queue"


class WorkloadFailure(Exception):
    pass


class SetupBlock(Exception):
    pass


@dataclass(frozen=True)
class CasePlan:
    rung_id: str
    case_id: str
    seed: int
    schedule: str
    workflow_prefix: str
    app_db: str
    sys_db: str
    artifact_name: str


_COUNTERS_LOCK = threading.Lock()
_COUNTERS: dict[str, dict[str, int]] = {}
_GATES: dict[str, dict[str, threading.Event]] = {}
_MULTIPLIERS: dict[str, int] = {}
_CHILD_IDS: dict[str, list[str]] = {}


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


def config(plan: CasePlan, *, executor_id: str | None = None) -> DBOSConfig:
    return {
        "name": APP_ID,
        "application_version": APP_VERSION,
        "executor_id": executor_id,
        "application_database_url": db_url(plan.app_db, driver="postgresql"),
        "system_database_url": db_url(plan.sys_db),
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
    }


def normalize_rung(rung: str) -> str:
    if rung in {"rung-001", RUNG_001_ID, "rung-001-state-machine-core"}:
        return RUNG_001_ID
    if rung in {"rung-002", RUNG_002_ID, "rung-002-child-fork-event-attributes"}:
        return RUNG_002_ID
    if rung in {"rung-003", RUNG_003_ID, "rung-003-recovery-during-management"}:
        return RUNG_003_ID
    if rung in {"rung-004", RUNG_004_ID, "rung-004-bounded-seed-sweep"}:
        return RUNG_004_ID
    if rung in {"rung-005", RUNG_005_ID, "rung-005-cancel-children-terminal-immutability"}:
        return RUNG_005_ID
    raise SetupBlock(f"unsupported rung {rung}")


def make_plan(rung: str, case_id: str, seed_override: int | None = None) -> CasePlan:
    normalized_rung = normalize_rung(rung)
    cases_by_rung: dict[str, dict[str, tuple[int, str]]] = {
        RUNG_001_ID: {
            "case-001": (3210, "cancel-after-final-step-before-return"),
            "case-002": (3211, "delayed-cancel-before-poller-then-resume"),
            "case-003": (3212, "stale-commands-after-success-and-error"),
        },
        RUNG_002_ID: {
            "case-001": (3220, "fork-step-prefixes-1-2-4"),
            "case-002": (3221, "fork-event-prefix-before-after-update"),
            "case-003": (3222, "fork-stream-interleaved-prefix"),
            "case-004": (3223, "fork-attribute-update-clear-filter"),
            "case-005": (3224, "replacement-children-parent-refork"),
            "case-006": (3225, "delete-parent-with-and-without-children"),
            "case-007": (3226, "replacement-children-delete-minimization"),
        },
        RUNG_003_ID: {
            "case-001": (3230, "cancel-before-recovery-claim"),
            "case-002": (3231, "resume-after-recovery-handle-before-release"),
            "case-003": (3232, "fork-during-recovered-execution"),
            "case-004": (3233, "dlq-resume-recover"),
            "case-005": (3234, "timeout-sweep-near-lifecycle-states"),
            "case-006": (3235, "garbage-collect-active-forked-queued"),
            "case-007": (3236, "global-timeout-delayed-minimization"),
        },
        RUNG_004_ID: {
            "case-001": (3240, "cancel-after-final-step-before-return"),
            "case-002": (3241, "delayed-cancel-before-poller-then-resume"),
            "case-003": (3242, "stale-commands-after-success-and-error"),
            "case-004": (3243, "cancel-after-final-step-before-return"),
            "case-005": (3244, "delayed-cancel-before-poller-then-resume"),
            "case-006": (3245, "stale-commands-after-success-and-error"),
            "case-007": (3246, "fork-step-prefixes-1-2-4"),
            "case-008": (3247, "fork-event-prefix-before-after-update"),
            "case-009": (3248, "fork-stream-interleaved-prefix"),
            "case-010": (3249, "fork-attribute-update-clear-filter"),
            "case-011": (3250, "replacement-children-parent-refork"),
            "case-012": (3251, "delete-parent-with-and-without-children"),
            "case-013": (3252, "cancel-before-recovery-claim"),
            "case-014": (3253, "resume-after-recovery-handle-before-release"),
            "case-015": (3254, "fork-during-recovered-execution"),
            "case-016": (3255, "dlq-resume-recover"),
            "case-017": (3256, "timeout-sweep-near-lifecycle-states"),
            "case-018": (3257, "garbage-collect-active-forked-queued"),
            "case-019": (3258, "timeout-sweep-near-lifecycle-states"),
            "case-020": (3259, "global-timeout-delayed-minimization"),
            "case-021": (3260, "garbage-collect-active-forked-queued"),
            "case-022": (3261, "delete-parent-with-and-without-children"),
            "case-023": (3262, "replacement-children-parent-refork"),
            "case-024": (3263, "fork-step-prefixes-1-2-4"),
        },
        RUNG_005_ID: {
            "case-001": (7010, "parent-child-grandchild-cancel-mode-toggle"),
            "case-002": (7011, "recursive-cancel-with-queued-descendant"),
            "case-003": (7030, "cancel-after-final-step-before-return"),
            "case-004": (7031, "client-recursive-cancel-result-parity"),
        },
    }
    cases = cases_by_rung[normalized_rung]
    if case_id not in cases:
        raise SetupBlock(f"unknown case {case_id} for rung {normalized_rung}")
    seed, schedule = cases[case_id]
    if seed_override is not None:
        seed = seed_override
    token = uuid.uuid5(uuid.NAMESPACE_URL, f"{normalized_rung}:{case_id}:{seed}").hex[:8]
    rung_tokens = {RUNG_001_ID: "r001", RUNG_002_ID: "r002", RUNG_003_ID: "r003", RUNG_004_ID: "r004", RUNG_005_ID: "r005"}
    rung_token = rung_tokens[normalized_rung]
    suffix = f"{rung_token}_{case_id.replace('-', '_')}_{seed}_{token}"
    return CasePlan(
        rung_id=normalized_rung,
        case_id=case_id,
        seed=seed,
        schedule=schedule,
        workflow_prefix=f"lfs-{rung_token}-{case_id}-{seed}-{token}",
        app_db=f"wio_lfs_app_{suffix}",
        sys_db=f"wio_lfs_sys_{suffix}",
        artifact_name=f"{case_id}.json",
    )


def reset_case_state(plan: CasePlan) -> None:
    with _COUNTERS_LOCK:
        for key in list(_COUNTERS):
            if key.startswith(plan.workflow_prefix):
                del _COUNTERS[key]
    for key in list(_GATES):
        if key.startswith(plan.workflow_prefix):
            del _GATES[key]
    for key in list(_MULTIPLIERS):
        if key.startswith(plan.workflow_prefix):
            del _MULTIPLIERS[key]
    for key in list(_CHILD_IDS):
        if key.startswith(plan.workflow_prefix):
            del _CHILD_IDS[key]


def gates_for(workflow_id: str) -> dict[str, threading.Event]:
    gates = {
        "after_step_one": threading.Event(),
        "after_final_step_before_return": threading.Event(),
        "release_workflow": threading.Event(),
    }
    _GATES[workflow_id] = gates
    return gates


def bump(workflow_id: str, name: str) -> int:
    with _COUNTERS_LOCK:
        workflow_counts = _COUNTERS.setdefault(workflow_id, {})
        workflow_counts[name] = workflow_counts.get(name, 0) + 1
        return workflow_counts[name]


def counters(workflow_id: str) -> dict[str, int]:
    with _COUNTERS_LOCK:
        return dict(_COUNTERS.get(workflow_id, {}))


@DBOS.step()
def counted_step(workflow_id: str, step_name: str) -> str:
    count = bump(workflow_id, step_name)
    event("step_executed", workflow_id=workflow_id, step_name=step_name, count=count)
    return f"{step_name}:{count}"


@DBOS.workflow()
def blocking_two_step_workflow(workflow_id: str, payload: str) -> str:
    counted_step(workflow_id, "step_one")
    _GATES[workflow_id]["after_step_one"].set()
    counted_step(workflow_id, "step_two")
    _GATES[workflow_id]["after_final_step_before_return"].set()
    if not _GATES[workflow_id]["release_workflow"].wait(timeout=20):
        raise TimeoutError("release_workflow gate did not open")
    return payload


@DBOS.workflow()
def recovery_two_step_workflow(workflow_id: str, payload: str) -> str:
    counted_step(workflow_id, "step_one")
    _GATES[workflow_id]["after_step_one"].set()
    if not _GATES[workflow_id]["release_workflow"].wait(timeout=20):
        raise TimeoutError("recovery release_workflow gate did not open")
    counted_step(workflow_id, "step_two")
    return payload


@DBOS.workflow()
def active_two_step_workflow(workflow_id: str, payload: str) -> str:
    counted_step(workflow_id, "step_one")
    _GATES[workflow_id]["after_step_one"].set()
    if not _GATES[workflow_id]["release_workflow"].wait(timeout=180):
        raise TimeoutError("active release_workflow gate did not open")
    counted_step(workflow_id, "step_two")
    return payload


@DBOS.workflow()
def instant_success_workflow(workflow_id: str, payload: str) -> str:
    counted_step(workflow_id, "success_step")
    return payload


@DBOS.workflow()
def deterministic_error_workflow(workflow_id: str) -> str:
    counted_step(workflow_id, "error_step")
    raise RuntimeError(f"modeled failure for {workflow_id}")


@DBOS.workflow()
def queue_success_workflow(workflow_id: str, payload: str) -> str:
    counted_step(workflow_id, "queue_step")
    return payload


@DBOS.workflow(max_recovery_attempts=1)
def dlq_once_workflow(workflow_id: str) -> str:
    counted_step(workflow_id, "dlq_attempt")
    return workflow_id


@DBOS.step()
def multiplier_step(workflow_id: str, step_name: str, value: int) -> int:
    count = bump(workflow_id, step_name)
    multiplier = _MULTIPLIERS.get(workflow_id, 1)
    result = value * multiplier
    event(
        "multiplier_step",
        workflow_id=workflow_id,
        step_name=step_name,
        value=value,
        multiplier=multiplier,
        result=result,
        count=count,
    )
    return result


@DBOS.workflow()
def four_step_multiplier_workflow(workflow_id: str) -> int:
    return (
        multiplier_step(workflow_id, "step_one", 1)
        + multiplier_step(workflow_id, "step_two", 2)
        + multiplier_step(workflow_id, "step_three", 3)
        + multiplier_step(workflow_id, "step_four", 4)
    )


@DBOS.step()
def set_event_step(key: str, value: str) -> str:
    DBOS.set_event(key, value)
    return value


@DBOS.workflow()
def fork_event_prefix_workflow(workflow_id: str, key: str) -> str:
    if not _GATES[workflow_id]["release_workflow"].wait(timeout=20):
        raise TimeoutError("event fork release_workflow gate did not open")
    DBOS.set_event(key, "v1")
    set_event_step(key, "step-v")
    DBOS.set_event(key, "v2")
    return DBOS.workflow_id or workflow_id


@DBOS.step()
def write_stream_step(key: str, value: str) -> str:
    DBOS.write_stream(key, value)
    return value


@DBOS.workflow()
def fork_stream_prefix_workflow(workflow_id: str, stream_a: str, stream_b: str) -> str:
    if not _GATES[workflow_id]["release_workflow"].wait(timeout=20):
        raise TimeoutError("stream fork release_workflow gate did not open")
    DBOS.write_stream(stream_a, "a0")
    DBOS.write_stream(stream_b, "b0")
    write_stream_step(stream_a, "a1")
    DBOS.close_stream(stream_b)
    DBOS.write_stream(stream_a, "a2")
    DBOS.close_stream(stream_a)
    return DBOS.workflow_id or workflow_id


@DBOS.workflow()
def attribute_noop_workflow() -> str:
    return DBOS.workflow_id or "missing-workflow-id"


@DBOS.step()
def child_multiplier_step(parent_key: str, x: int) -> int:
    multiplier = _MULTIPLIERS.get(parent_key, 2)
    return x * multiplier


@DBOS.workflow()
def replacement_child_workflow(parent_key: str, x: int) -> int:
    return child_multiplier_step(parent_key, x)


@DBOS.step()
def combine_child_results(results: list[int]) -> int:
    return sum(results)


@DBOS.workflow()
def replacement_parent_workflow(parent_key: str) -> int:
    values = [10, 20, 30, 40, 50]
    handles = [DBOS.start_workflow(replacement_child_workflow, parent_key, value) for value in values]
    _CHILD_IDS[parent_key] = [handle.workflow_id for handle in handles]
    return combine_child_results([handle.get_result() for handle in handles])


@DBOS.transaction()
def delete_case_transaction(x: int) -> int:
    DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
    return x


@DBOS.workflow()
def delete_case_child_workflow(x: int) -> int:
    delete_case_transaction(x)
    return x * 2


@DBOS.workflow()
def delete_case_parent_workflow(x: int) -> int:
    child_handle = DBOS.start_workflow(delete_case_child_workflow, x)
    return child_handle.get_result()


@DBOS.workflow()
def cancel_tree_leaf_workflow(workflow_id: str) -> str:
    counted_step(workflow_id, "entered")
    _GATES[workflow_id]["after_step_one"].set()
    if not _GATES[workflow_id]["release_workflow"].wait(timeout=180):
        raise TimeoutError("cancel tree leaf release_workflow gate did not open")
    counted_step(workflow_id, "released")
    return workflow_id


@DBOS.workflow()
def cancel_tree_child_workflow(workflow_id: str, grandchild_id: str) -> str:
    with SetWorkflowID(grandchild_id):
        grandchild_handle = DBOS.start_workflow(cancel_tree_leaf_workflow, grandchild_id)
    _CHILD_IDS[workflow_id] = [grandchild_handle.workflow_id]
    counted_step(workflow_id, "entered")
    _GATES[workflow_id]["after_step_one"].set()
    if not _GATES[workflow_id]["release_workflow"].wait(timeout=180):
        raise TimeoutError("cancel tree child release_workflow gate did not open")
    counted_step(workflow_id, "released")
    return workflow_id


@DBOS.workflow()
def cancel_tree_parent_workflow(workflow_id: str, child_id: str, grandchild_id: str) -> str:
    with SetWorkflowID(child_id):
        child_handle = DBOS.start_workflow(cancel_tree_child_workflow, child_id, grandchild_id)
    _CHILD_IDS[workflow_id] = [child_handle.workflow_id]
    counted_step(workflow_id, "entered")
    _GATES[workflow_id]["after_step_one"].set()
    if not _GATES[workflow_id]["release_workflow"].wait(timeout=180):
        raise TimeoutError("cancel tree parent release_workflow gate did not open")
    counted_step(workflow_id, "released")
    return workflow_id


@DBOS.workflow()
def queue_blocker_workflow(workflow_id: str) -> str:
    counted_step(workflow_id, "entered")
    _GATES[workflow_id]["after_step_one"].set()
    if not _GATES[workflow_id]["release_workflow"].wait(timeout=180):
        raise TimeoutError("queue blocker release_workflow gate did not open")
    counted_step(workflow_id, "released")
    return workflow_id


@DBOS.workflow()
def queued_descendant_workflow(workflow_id: str) -> str:
    counted_step(workflow_id, "queued_body")
    return workflow_id


@DBOS.workflow()
def parent_with_queued_descendant_workflow(workflow_id: str, descendant_id: str, queue_name: str) -> str:
    with SetWorkflowID(descendant_id):
        descendant_handle = DBOS.enqueue_workflow(queue_name, queued_descendant_workflow, descendant_id)
    _CHILD_IDS[workflow_id] = [descendant_handle.workflow_id]
    counted_step(workflow_id, "entered")
    _GATES[workflow_id]["after_step_one"].set()
    if not _GATES[workflow_id]["release_workflow"].wait(timeout=180):
        raise TimeoutError("queued-descendant parent release_workflow gate did not open")
    counted_step(workflow_id, "released")
    return workflow_id


def executor_a(plan: CasePlan) -> str:
    return f"{plan.workflow_prefix}-executor-a"


def executor_b(plan: CasePlan) -> str:
    return f"{plan.workflow_prefix}-executor-b"


def launch(plan: CasePlan, *, executor_id: str | None = None, clean: bool = True) -> DBOS:
    if clean:
        cleanup_databases(plan)
        reset_case_state(plan)
    os.environ["DBOS__APPID"] = APP_ID
    os.environ["DBOS__APPVERSION"] = APP_VERSION
    if executor_id is not None:
        os.environ["DBOS__VMID"] = executor_id
    else:
        os.environ.pop("DBOS__VMID", None)
    DBOS.destroy(destroy_registry=False)
    dbos = DBOS(config=config(plan, executor_id=executor_id))
    DBOS.launch()
    DBOS.register_queue(QUEUE_NAME, polling_interval_sec=0.1)
    event(
        "case_start" if clean else "case_relaunch",
        frontier=FRONTIER_ID,
        rung=plan.rung_id,
        executor_id=executor_id,
        **asdict(plan),
    )
    return dbos


def relaunch_same_databases(plan: CasePlan, *, executor_id: str) -> DBOS:
    return launch(plan, executor_id=executor_id, clean=False)


def status_of(workflow_id: str) -> WorkflowStatus | None:
    return DBOS.get_workflow_status(workflow_id)


def status_value(workflow_id: str) -> str | None:
    status = status_of(workflow_id)
    return None if status is None else status.status


def status_snapshot(workflow_id: str) -> dict[str, Any] | None:
    status = status_of(workflow_id)
    if status is None:
        return None
    return {
        "workflow_id": status.workflow_id,
        "status": status.status,
        "queue_name": status.queue_name,
        "created_at": status.created_at,
        "updated_at": status.updated_at,
        "completed_at": getattr(status, "completed_at", None),
        "delay_until_epoch_ms": status.delay_until_epoch_ms,
        "deduplication_id": status.deduplication_id,
        "executor_id": getattr(status, "executor_id", None),
        "recovery_attempts": getattr(status, "recovery_attempts", None),
        "forked_from": getattr(status, "forked_from", None),
    }


def list_ids(ids: list[str]) -> list[str]:
    return sorted(workflow.workflow_id for workflow in DBOS.list_workflows(workflow_ids=ids))


def queued_ids(ids: list[str]) -> list[str]:
    return sorted(workflow.workflow_id for workflow in DBOS.list_queued_workflows(workflow_ids=ids))


def workflow_attrs(workflow_id: str) -> dict[str, Any] | None:
    rows = DBOS.list_workflows(workflow_ids=[workflow_id])
    return None if not rows else rows[0].attributes


def attribute_filter_ids(attributes: dict[str, Any]) -> list[str]:
    return sorted(workflow.workflow_id for workflow in DBOS.list_workflows(attributes=attributes))


def all_events(workflow_id: str) -> dict[str, Any]:
    return DBOS.get_all_events(workflow_id)


def read_stream_count(workflow_id: str, key: str, count: int) -> list[Any]:
    values = []
    gen = DBOS.read_stream(workflow_id, key)
    for _ in range(count):
        values.append(next(gen))
    return values


def read_stream_all(workflow_id: str, key: str) -> list[Any]:
    return list(DBOS.read_stream(workflow_id, key))


def child_ids(dbos: DBOS, workflow_id: str) -> list[str]:
    return sorted(dbos._sys_db.get_workflow_children(workflow_id))


def transaction_output_count(dbos: DBOS, workflow_id: str) -> int:
    if not dbos._app_db:
        raise SetupBlock("missing DBOS application database")
    with dbos._app_db.engine.begin() as conn:
        rows = conn.execute(
            sa.select(ApplicationSchema.transaction_outputs.c.workflow_uuid).where(
                ApplicationSchema.transaction_outputs.c.workflow_uuid == workflow_id
            )
        ).all()
    return len(rows)


def wait_for_status(workflow_id: str, expected: str, *, timeout_seconds: float = 10) -> dict[str, Any] | None:
    deadline = time.monotonic() + timeout_seconds
    last = None
    while time.monotonic() < deadline:
        last = status_snapshot(workflow_id)
        if last and last["status"] == expected:
            return last
        time.sleep(0.05)
    return last


def assert_status(workflow_id: str, expected: str, name: str) -> dict[str, Any] | None:
    observed = status_snapshot(workflow_id)
    invariant(name, observed is not None and observed["status"] == expected, workflow_id=workflow_id, expected=expected, observed=observed)
    return observed


def assert_list_exact(ids: list[str], expected: list[str], name: str) -> None:
    observed = list_ids(ids)
    invariant(name, observed == sorted(expected), expected=sorted(expected), observed=observed)


def assert_queued_absent(workflow_id: str, name: str) -> None:
    observed = queued_ids([workflow_id])
    invariant(name, workflow_id not in observed, workflow_id=workflow_id, queued=observed)


def start_recovery_blocked_workflow(plan: CasePlan, suffix: str, payload: str) -> tuple[str, Any]:
    workflow_id = f"{plan.workflow_prefix}-{suffix}"
    gates = gates_for(workflow_id)
    with SetWorkflowID(workflow_id):
        handle = DBOS.start_workflow(recovery_two_step_workflow, workflow_id, payload)
    reached = gates["after_step_one"].wait(timeout=10)
    invariant(
        f"{suffix}_target_window_reached",
        reached,
        workflow_id=workflow_id,
        counters=counters(workflow_id),
        status=status_snapshot(workflow_id),
    )
    invariant(
        f"{suffix}_step_one_checkpointed_once",
        counters(workflow_id) == {"step_one": 1},
        workflow_id=workflow_id,
        counters=counters(workflow_id),
    )
    return workflow_id, handle


def start_active_blocked_workflow(plan: CasePlan, suffix: str, payload: str) -> tuple[str, Any]:
    workflow_id = f"{plan.workflow_prefix}-{suffix}"
    gates = gates_for(workflow_id)
    with SetWorkflowID(workflow_id):
        handle = DBOS.start_workflow(active_two_step_workflow, workflow_id, payload)
    reached = gates["after_step_one"].wait(timeout=10)
    invariant(
        f"{suffix}_target_window_reached",
        reached,
        workflow_id=workflow_id,
        counters=counters(workflow_id),
        status=status_snapshot(workflow_id),
    )
    invariant(
        f"{suffix}_step_one_checkpointed_once",
        counters(workflow_id) == {"step_one": 1},
        workflow_id=workflow_id,
        counters=counters(workflow_id),
    )
    return workflow_id, handle


def release_recovery_workflow(workflow_id: str) -> None:
    _GATES[workflow_id]["release_workflow"].set()


def recover_for(plan: CasePlan, dead_executor_id: str) -> list[Any]:
    handles = DBOS._recover_pending_workflows([dead_executor_id])
    event(
        "recover_pending_workflows",
        dead_executor_id=dead_executor_id,
        active_executor_id=executor_b(plan),
        handles=[handle.workflow_id for handle in handles],
    )
    return handles


def handle_result(handle: Any) -> dict[str, Any]:
    try:
        return {"ok": True, "value": handle.get_result(polling_interval_sec=0.05)}
    except BaseException as exc:
        return {"ok": False, "error_type": type(exc).__name__, "error": str(exc)}


def snapshot_from_status(status: WorkflowStatus | None) -> dict[str, Any] | None:
    if status is None:
        return None
    return {
        "workflow_id": status.workflow_id,
        "status": status.status,
        "queue_name": status.queue_name,
        "created_at": status.created_at,
        "updated_at": status.updated_at,
        "completed_at": getattr(status, "completed_at", None),
        "delay_until_epoch_ms": status.delay_until_epoch_ms,
        "deduplication_id": status.deduplication_id,
        "executor_id": getattr(status, "executor_id", None),
        "recovery_attempts": getattr(status, "recovery_attempts", None),
        "forked_from": getattr(status, "forked_from", None),
    }


def client_status_snapshot(client: DBOSClient, workflow_id: str) -> dict[str, Any] | None:
    rows = client.list_workflows(workflow_ids=[workflow_id])
    if not rows:
        return None
    return snapshot_from_status(rows[0])


def client_queued_ids(client: DBOSClient, ids: list[str]) -> list[str]:
    return sorted(workflow.workflow_id for workflow in client.list_queued_workflows(workflow_ids=ids))


def cancel_tree_ids(plan: CasePlan, suffix: str) -> dict[str, str]:
    return {
        "parent": f"{plan.workflow_prefix}-{suffix}-parent",
        "child": f"{plan.workflow_prefix}-{suffix}-child",
        "grandchild": f"{plan.workflow_prefix}-{suffix}-grandchild",
    }


def prepare_cancel_tree(dbos: DBOS, plan: CasePlan, suffix: str) -> tuple[dict[str, str], Any, dict[str, Any]]:
    ids = cancel_tree_ids(plan, suffix)
    for workflow_id in ids.values():
        gates_for(workflow_id)
    with SetWorkflowID(ids["parent"]):
        parent_handle = DBOS.start_workflow(cancel_tree_parent_workflow, ids["parent"], ids["child"], ids["grandchild"])
    reached = {name: _GATES[workflow_id]["after_step_one"].wait(timeout=10) for name, workflow_id in ids.items()}
    statuses = {name: status_snapshot(workflow_id) for name, workflow_id in ids.items()}
    graph = child_ids(dbos, ids["parent"])
    expected_graph = sorted([ids["child"], ids["grandchild"]])
    invariant(f"{suffix}_tree_all_nodes_running", all(reached.values()), reached=reached, statuses=statuses, ids=ids)
    invariant(f"{suffix}_tree_graph_matches_model", graph == expected_graph, graph=graph, expected=expected_graph, ids=ids)
    invariant(
        f"{suffix}_tree_initial_statuses_pending",
        all(snapshot is not None and snapshot["status"] == WorkflowStatusString.PENDING.value for snapshot in statuses.values()),
        statuses=statuses,
    )
    return ids, parent_handle, {"reached": reached, "statuses": statuses, "graph": graph}


def release_cancel_tree(ids: dict[str, str]) -> None:
    for workflow_id in ids.values():
        _GATES[workflow_id]["release_workflow"].set()


def assert_tree_statuses(ids: dict[str, str], expected: dict[str, str], name: str) -> dict[str, Any]:
    observed = {node: status_snapshot(workflow_id) for node, workflow_id in ids.items()}
    observed_values = {node: None if snapshot is None else snapshot["status"] for node, snapshot in observed.items()}
    invariant(name, observed_values == expected, observed=observed_values, expected=expected, snapshots=observed)
    return observed


def assert_cancelled_result(workflow_id: str, handle: Any | None, name: str) -> dict[str, Any]:
    result = handle_result(handle if handle is not None else DBOS.retrieve_workflow(workflow_id))
    invariant(
        name,
        result.get("ok") is False and result.get("error_type") == "DBOSAwaitedWorkflowCancelledError",
        workflow_id=workflow_id,
        result=result,
    )
    return result


def case_cancel_tree_mode_toggle(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    isolate_ids, isolate_handle, isolate_setup = prepare_cancel_tree(dbos, plan, "mode-isolate")
    unrelated_id, unrelated_handle = start_active_blocked_workflow(plan, "mode-unrelated", f"unrelated-{plan.seed}")

    before_isolate_cancel = {name: status_snapshot(workflow_id) for name, workflow_id in isolate_ids.items()}
    DBOS.cancel_workflow(isolate_ids["parent"], cancel_children=False)
    after_isolate_cancel = assert_tree_statuses(
        isolate_ids,
        {
            "parent": WorkflowStatusString.CANCELLED.value,
            "child": WorkflowStatusString.PENDING.value,
            "grandchild": WorkflowStatusString.PENDING.value,
        },
        "cancel_tree_false_isolates_parent",
    )
    invariant(
        "cancel_tree_false_unrelated_still_pending",
        status_value(unrelated_id) == WorkflowStatusString.PENDING.value,
        status=status_snapshot(unrelated_id),
    )
    release_cancel_tree(isolate_ids)
    release_recovery_workflow(unrelated_id)
    isolate_parent_result = assert_cancelled_result(isolate_ids["parent"], isolate_handle, "cancel_tree_false_parent_result_cancelled")
    child_result = DBOS.retrieve_workflow(isolate_ids["child"]).get_result(polling_interval_sec=0.05)
    grandchild_result = DBOS.retrieve_workflow(isolate_ids["grandchild"]).get_result(polling_interval_sec=0.05)
    unrelated_result = unrelated_handle.get_result(polling_interval_sec=0.05)
    isolate_final = assert_tree_statuses(
        isolate_ids,
        {
            "parent": WorkflowStatusString.CANCELLED.value,
            "child": WorkflowStatusString.SUCCESS.value,
            "grandchild": WorkflowStatusString.SUCCESS.value,
        },
        "cancel_tree_false_descendants_survive_after_release",
    )
    invariant(
        "cancel_tree_false_descendant_results_match_model",
        child_result == isolate_ids["child"] and grandchild_result == isolate_ids["grandchild"] and unrelated_result == f"unrelated-{plan.seed}",
        child_result=child_result,
        grandchild_result=grandchild_result,
        unrelated_result=unrelated_result,
    )

    cascade_ids, cascade_handle, cascade_setup = prepare_cancel_tree(dbos, plan, "mode-cascade")
    cascade_unrelated_id, cascade_unrelated_handle = start_active_blocked_workflow(plan, "mode-cascade-unrelated", f"cascade-unrelated-{plan.seed}")
    DBOS.cancel_workflow(cascade_ids["parent"], cancel_children=True)
    after_cascade_cancel = assert_tree_statuses(
        cascade_ids,
        {
            "parent": WorkflowStatusString.CANCELLED.value,
            "child": WorkflowStatusString.CANCELLED.value,
            "grandchild": WorkflowStatusString.CANCELLED.value,
        },
        "cancel_tree_true_cascades_to_modeled_descendants",
    )
    invariant(
        "cancel_tree_true_unrelated_not_cancelled",
        status_value(cascade_unrelated_id) == WorkflowStatusString.PENDING.value,
        status=status_snapshot(cascade_unrelated_id),
    )
    release_cancel_tree(cascade_ids)
    release_recovery_workflow(cascade_unrelated_id)
    cascade_results = {
        node: assert_cancelled_result(workflow_id, cascade_handle if node == "parent" else None, f"cancel_tree_true_{node}_result_cancelled")
        for node, workflow_id in cascade_ids.items()
    }
    cascade_unrelated_result = cascade_unrelated_handle.get_result(polling_interval_sec=0.05)
    after_cascade_release = assert_tree_statuses(
        cascade_ids,
        {
            "parent": WorkflowStatusString.CANCELLED.value,
            "child": WorkflowStatusString.CANCELLED.value,
            "grandchild": WorkflowStatusString.CANCELLED.value,
        },
        "cancel_tree_true_statuses_still_cancelled_after_release",
    )
    invariant(
        "cancel_tree_true_unrelated_completes",
        cascade_unrelated_result == f"cascade-unrelated-{plan.seed}" and status_value(cascade_unrelated_id) == WorkflowStatusString.SUCCESS.value,
        result=cascade_unrelated_result,
        status=status_snapshot(cascade_unrelated_id),
    )
    return {
        "isolate": {
            "ids": isolate_ids,
            "setup": isolate_setup,
            "before_cancel": before_isolate_cancel,
            "after_cancel": after_isolate_cancel,
            "final": isolate_final,
            "parent_result": isolate_parent_result,
        },
        "cascade": {
            "ids": cascade_ids,
            "setup": cascade_setup,
            "after_cancel": after_cascade_cancel,
            "after_release": after_cascade_release,
            "results": cascade_results,
        },
    }


def case_recursive_cancel_queued_descendant(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    DBOS.register_queue(QUEUE_NAME, concurrency=1, polling_interval_sec=0.05, on_conflict="always_update")
    blocker_id = f"{plan.workflow_prefix}-queue-blocker"
    parent_id = f"{plan.workflow_prefix}-queue-parent"
    descendant_id = f"{plan.workflow_prefix}-queue-descendant"
    for workflow_id in (blocker_id, parent_id):
        gates_for(workflow_id)

    with SetWorkflowID(blocker_id):
        blocker_handle = DBOS.enqueue_workflow(QUEUE_NAME, queue_blocker_workflow, blocker_id)
    invariant(
        "queued_descendant_blocker_started",
        _GATES[blocker_id]["after_step_one"].wait(timeout=10)
        and status_value(blocker_id) == WorkflowStatusString.PENDING.value,
        status=status_snapshot(blocker_id),
    )

    with SetWorkflowID(parent_id):
        parent_handle = DBOS.start_workflow(parent_with_queued_descendant_workflow, parent_id, descendant_id, QUEUE_NAME)
    invariant(
        "queued_descendant_parent_started",
        _GATES[parent_id]["after_step_one"].wait(timeout=10)
        and status_value(parent_id) == WorkflowStatusString.PENDING.value,
        parent=status_snapshot(parent_id),
        descendant=status_snapshot(descendant_id),
    )
    graph = child_ids(dbos, parent_id)
    before_cancel = {
        "parent": status_snapshot(parent_id),
        "descendant": status_snapshot(descendant_id),
        "blocker": status_snapshot(blocker_id),
        "queued": queued_ids([descendant_id]),
        "graph": graph,
    }
    invariant("queued_descendant_graph_matches_model", graph == [descendant_id], graph=graph, expected=[descendant_id])
    invariant(
        "queued_descendant_waits_behind_blocker",
        before_cancel["descendant"] is not None
        and before_cancel["descendant"]["status"] == WorkflowStatusString.ENQUEUED.value
        and before_cancel["queued"] == [descendant_id],
        before=before_cancel,
    )

    DBOS.cancel_workflow(parent_id, cancel_children=True)
    after_cancel = {
        "parent": status_snapshot(parent_id),
        "descendant": status_snapshot(descendant_id),
        "blocker": status_snapshot(blocker_id),
        "queued": queued_ids([descendant_id]),
    }
    invariant(
        "queued_descendant_cancelled_and_removed_from_queue",
        after_cancel["parent"] is not None
        and after_cancel["parent"]["status"] == WorkflowStatusString.CANCELLED.value
        and after_cancel["descendant"] is not None
        and after_cancel["descendant"]["status"] == WorkflowStatusString.CANCELLED.value
        and after_cancel["queued"] == [],
        after_cancel=after_cancel,
    )
    _GATES[parent_id]["release_workflow"].set()
    _GATES[blocker_id]["release_workflow"].set()
    parent_result = assert_cancelled_result(parent_id, parent_handle, "queued_descendant_parent_result_cancelled")
    blocker_result = blocker_handle.get_result(polling_interval_sec=0.05)
    descendant_result = assert_cancelled_result(descendant_id, None, "queued_descendant_result_cancelled")
    final = {
        "parent": status_snapshot(parent_id),
        "descendant": status_snapshot(descendant_id),
        "blocker": status_snapshot(blocker_id),
        "queued": queued_ids([descendant_id]),
        "counters": {
            parent_id: counters(parent_id),
            descendant_id: counters(descendant_id),
            blocker_id: counters(blocker_id),
        },
    }
    invariant(
        "queued_descendant_never_runs_after_queue_release",
        final["descendant"] is not None
        and final["descendant"]["status"] == WorkflowStatusString.CANCELLED.value
        and final["queued"] == []
        and counters(descendant_id) == {},
        final=final,
        descendant_result=descendant_result,
    )
    invariant(
        "queued_descendant_blocker_completes_control",
        blocker_result == blocker_id and status_value(blocker_id) == WorkflowStatusString.SUCCESS.value,
        result=blocker_result,
        status=status_snapshot(blocker_id),
    )
    return {
        "workflow_ids": {"parent": parent_id, "descendant": descendant_id, "blocker": blocker_id},
        "before_cancel": before_cancel,
        "after_cancel": after_cancel,
        "final": final,
        "results": {"parent": parent_result, "descendant": descendant_result, "blocker": blocker_result},
    }


def case_client_recursive_cancel_result_parity(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    ids, parent_handle, setup = prepare_cancel_tree(dbos, plan, "client-cascade")
    client = DBOSClient(system_database_url=db_url(plan.sys_db), application_database_url=db_url(plan.app_db, driver="postgresql"))
    before = {
        node: {"runtime": status_snapshot(workflow_id), "client": client_status_snapshot(client, workflow_id)}
        for node, workflow_id in ids.items()
    }
    invariant(
        "client_cancel_before_statuses_match_runtime",
        all(pair["runtime"] is not None and pair["client"] is not None and pair["runtime"]["status"] == pair["client"]["status"] for pair in before.values()),
        before=before,
    )
    client.cancel_workflow(ids["parent"], cancel_children=True)
    after_cancel = {
        node: {"runtime": status_snapshot(workflow_id), "client": client_status_snapshot(client, workflow_id)}
        for node, workflow_id in ids.items()
    }
    invariant(
        "client_cancel_recursive_statuses_match_runtime",
        all(
            pair["runtime"] is not None
            and pair["client"] is not None
            and pair["runtime"]["status"] == pair["client"]["status"] == WorkflowStatusString.CANCELLED.value
            for pair in after_cancel.values()
        ),
        after_cancel=after_cancel,
    )
    invariant(
        "client_cancel_queued_listing_empty_for_tree",
        client_queued_ids(client, list(ids.values())) == [] and queued_ids(list(ids.values())) == [],
        client_queued=client_queued_ids(client, list(ids.values())),
        runtime_queued=queued_ids(list(ids.values())),
    )
    release_cancel_tree(ids)
    runtime_results = {
        node: assert_cancelled_result(workflow_id, parent_handle if node == "parent" else None, f"client_cancel_runtime_{node}_result_cancelled")
        for node, workflow_id in ids.items()
    }
    client_results = {}
    for node, workflow_id in ids.items():
        try:
            client.retrieve_workflow(workflow_id).get_result(polling_interval_sec=0.05)
            client_results[node] = {"ok": True}
        except BaseException as exc:
            client_results[node] = {"ok": False, "error_type": type(exc).__name__, "error": str(exc)}
    invariant(
        "client_cancel_result_errors_match_runtime",
        all(result.get("error_type") == "DBOSAwaitedWorkflowCancelledError" for result in client_results.values()),
        client_results=client_results,
        runtime_results=runtime_results,
    )
    final = {
        node: {"runtime": status_snapshot(workflow_id), "client": client_status_snapshot(client, workflow_id)}
        for node, workflow_id in ids.items()
    }
    invariant(
        "client_cancel_final_statuses_still_cancelled",
        all(
            pair["runtime"] is not None
            and pair["client"] is not None
            and pair["runtime"]["status"] == pair["client"]["status"] == WorkflowStatusString.CANCELLED.value
            for pair in final.values()
        ),
        final=final,
    )
    return {
        "ids": ids,
        "setup": setup,
        "before": before,
        "after_cancel": after_cancel,
        "runtime_results": runtime_results,
        "client_results": client_results,
        "final": final,
        "graph": child_ids(dbos, ids["parent"]),
    }


def case_recovery_cancel_before_claim(plan: CasePlan) -> dict[str, Any]:
    workflow_id, original_handle = start_recovery_blocked_workflow(plan, "recover-cancel", f"cancel-{plan.seed}")
    before_relaunch = assert_status(workflow_id, WorkflowStatusString.PENDING.value, "recovery_cancel_pending_under_executor_a")
    relaunch_same_databases(plan, executor_id=executor_b(plan))

    DBOS.cancel_workflow(workflow_id)
    after_cancel = assert_status(workflow_id, WorkflowStatusString.CANCELLED.value, "recovery_cancel_status_cancelled_before_recovery")
    recovery_handles = recover_for(plan, executor_a(plan))
    invariant(
        "recovery_cancel_no_handle_for_cancelled_workflow",
        workflow_id not in [handle.workflow_id for handle in recovery_handles],
        workflow_id=workflow_id,
        handles=[handle.workflow_id for handle in recovery_handles],
    )
    release_recovery_workflow(workflow_id)
    original_result = handle_result(original_handle)
    invariant(
        "recovery_cancel_body_not_resurrected",
        counters(workflow_id) == {"step_one": 1} and status_value(workflow_id) == WorkflowStatusString.CANCELLED.value,
        workflow_id=workflow_id,
        counters=counters(workflow_id),
        status=status_snapshot(workflow_id),
        original_result=original_result,
    )
    return {
        "workflow_id": workflow_id,
        "executors": {"dead": executor_a(plan), "recovering": executor_b(plan)},
        "statuses": {"before_relaunch": before_relaunch, "after_cancel": after_cancel, "final": status_snapshot(workflow_id)},
        "recovery_handles": [handle.workflow_id for handle in recovery_handles],
        "original_result": original_result,
        "counters": counters(workflow_id),
    }


def case_recovery_resume_after_handle(plan: CasePlan) -> dict[str, Any]:
    workflow_id, _original_handle = start_recovery_blocked_workflow(plan, "recover-resume", f"resume-{plan.seed}")
    relaunch_same_databases(plan, executor_id=executor_b(plan))
    recovery_handles = recover_for(plan, executor_a(plan))
    invariant("recovery_resume_handle_created", [h.workflow_id for h in recovery_handles] == [workflow_id], handles=[h.workflow_id for h in recovery_handles], expected=[workflow_id])

    resumed_handle = DBOS.resume_workflow(workflow_id)
    after_resume = status_snapshot(workflow_id)
    release_recovery_workflow(workflow_id)
    recovery_results = [handle_result(handle) for handle in recovery_handles]
    resumed_result = handle_result(resumed_handle)
    final = wait_for_status(workflow_id, WorkflowStatusString.SUCCESS.value, timeout_seconds=10)
    invariant("recovery_resume_final_success", final is not None and final["status"] == WorkflowStatusString.SUCCESS.value, final=final)
    invariant("recovery_resume_no_duplicate_steps", counters(workflow_id) == {"step_one": 1, "step_two": 1}, counters=counters(workflow_id), recovery_results=recovery_results, resumed_result=resumed_result)
    invariant("recovery_resume_result_observed", any(result.get("ok") and result.get("value") == f"resume-{plan.seed}" for result in [*recovery_results, resumed_result]), recovery_results=recovery_results, resumed_result=resumed_result)
    return {
        "workflow_id": workflow_id,
        "executors": {"dead": executor_a(plan), "recovering": executor_b(plan)},
        "after_resume": after_resume,
        "recovery_results": recovery_results,
        "resumed_result": resumed_result,
        "final": final,
        "counters": counters(workflow_id),
    }


def case_recovery_fork_during_execution(plan: CasePlan) -> dict[str, Any]:
    workflow_id, _original_handle = start_recovery_blocked_workflow(plan, "recover-fork", f"fork-{plan.seed}")
    relaunch_same_databases(plan, executor_id=executor_b(plan))
    recovery_handles = recover_for(plan, executor_a(plan))
    invariant("recovery_fork_handle_created", [h.workflow_id for h in recovery_handles] == [workflow_id], handles=[h.workflow_id for h in recovery_handles])

    fork_id = f"{workflow_id}-fork-step-2"
    with SetWorkflowID(fork_id):
        fork_handle = DBOS.fork_workflow(workflow_id, 2)
    fork_pre_release = status_snapshot(fork_id)
    release_recovery_workflow(workflow_id)
    recovery_results = [handle_result(handle) for handle in recovery_handles]
    fork_result = handle_result(fork_handle)
    original_final = wait_for_status(workflow_id, WorkflowStatusString.SUCCESS.value, timeout_seconds=10)
    fork_final = wait_for_status(fork_id, WorkflowStatusString.SUCCESS.value, timeout_seconds=10)
    invariant("recovery_fork_original_and_fork_success", original_final is not None and fork_final is not None and original_final["status"] == fork_final["status"] == WorkflowStatusString.SUCCESS.value, original=original_final, fork=fork_final)
    invariant("recovery_fork_prefix_not_duplicated_suffix_runs_twice", counters(workflow_id) == {"step_one": 1, "step_two": 2}, counters=counters(workflow_id), recovery_results=recovery_results, fork_result=fork_result)
    invariant("recovery_fork_status_links_to_original", fork_final is not None and fork_final["forked_from"] == workflow_id, fork_final=fork_final, original_id=workflow_id)
    return {
        "workflow_id": workflow_id,
        "fork_id": fork_id,
        "fork_pre_release": fork_pre_release,
        "recovery_results": recovery_results,
        "fork_result": fork_result,
        "statuses": {"original": original_final, "fork": fork_final},
        "counters": counters(workflow_id),
    }


def case_dlq_resume_recover(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    workflow_id = f"{plan.workflow_prefix}-dlq"
    with SetWorkflowID(workflow_id):
        handle = DBOS.start_workflow(dlq_once_workflow, workflow_id)
    first_result = handle.get_result(polling_interval_sec=0.05)
    invariant("dlq_initial_success", first_result == workflow_id and counters(workflow_id) == {"dlq_attempt": 1}, result=first_result, counters=counters(workflow_id))

    recovery_observations = []
    for attempt in (1, 2, 3):
        dbos._sys_db.update_workflow_outcome(workflow_id, WorkflowStatusString.PENDING.value)
        before = status_snapshot(workflow_id)
        handles = DBOS._recover_pending_workflows([executor_a(plan)])
        results = [handle_result(recovery_handle) for recovery_handle in handles]
        after = status_snapshot(workflow_id)
        recovery_observations.append({"attempt": attempt, "before": before, "handles": [h.workflow_id for h in handles], "results": results, "after": after})

    dlq_status = status_snapshot(workflow_id)
    invariant(
        "dlq_status_after_max_attempts",
        dlq_status is not None and dlq_status["status"] == WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value,
        status=dlq_status,
        observations=recovery_observations,
    )
    resumed_handle = DBOS.resume_workflow(workflow_id)
    resumed_result = handle_result(resumed_handle)
    final = wait_for_status(workflow_id, WorkflowStatusString.SUCCESS.value, timeout_seconds=10)
    invariant("dlq_resume_reaches_success", final is not None and final["status"] == WorkflowStatusString.SUCCESS.value and resumed_result.get("ok"), final=final, resumed_result=resumed_result)
    invariant("dlq_resume_preserves_checkpointed_side_effect_once", counters(workflow_id) == {"dlq_attempt": 1}, counters=counters(workflow_id), observations=recovery_observations)
    return {
        "workflow_id": workflow_id,
        "first_result": first_result,
        "recovery_observations": recovery_observations,
        "dlq_status": dlq_status,
        "resumed_result": resumed_result,
        "final": final,
        "counters": counters(workflow_id),
    }


def case_global_timeout_sweep(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    pending_id, _pending_handle = start_active_blocked_workflow(plan, "timeout-pending-old", f"timeout-{plan.seed}")

    delayed_id = f"{plan.workflow_prefix}-timeout-delayed-old"
    with SetWorkflowID(delayed_id):
        with SetEnqueueOptions(delay_seconds=60.0):
            DBOS.enqueue_workflow(QUEUE_NAME, queue_success_workflow, delayed_id, "delayed")

    cancelled_id = f"{plan.workflow_prefix}-timeout-cancelled-old"
    with SetWorkflowID(cancelled_id):
        with SetEnqueueOptions(delay_seconds=60.0):
            DBOS.enqueue_workflow(QUEUE_NAME, queue_success_workflow, cancelled_id, "cancelled")
    DBOS.cancel_workflow(cancelled_id)

    success_id = f"{plan.workflow_prefix}-timeout-success-old"
    with SetWorkflowID(success_id):
        success_result = instant_success_workflow(success_id, "success")
    invariant("timeout_success_setup", success_result == "success" and status_value(success_id) == WorkflowStatusString.SUCCESS.value, status=status_snapshot(success_id))

    time.sleep(1.1)
    cutoff_epoch_ms = int(time.time() * 1000)
    fresh_pending_id, _fresh_handle = start_active_blocked_workflow(plan, "timeout-pending-fresh", f"fresh-{plan.seed}")
    before = {wid: status_snapshot(wid) for wid in [pending_id, delayed_id, cancelled_id, success_id, fresh_pending_id]}
    global_timeout(dbos, cutoff_epoch_ms)
    after = {wid: status_snapshot(wid) for wid in [pending_id, delayed_id, cancelled_id, success_id, fresh_pending_id]}

    invariant("timeout_old_pending_cancelled", after[pending_id] is not None and after[pending_id]["status"] == WorkflowStatusString.CANCELLED.value, before=before[pending_id], after=after[pending_id])
    invariant("timeout_old_delayed_preserved", after[delayed_id] is not None and after[delayed_id]["status"] == WorkflowStatusString.DELAYED.value, before=before[delayed_id], after=after[delayed_id])
    invariant("timeout_terminal_rows_preserved", after[cancelled_id] is not None and after[cancelled_id]["status"] == WorkflowStatusString.CANCELLED.value and after[success_id] is not None and after[success_id]["status"] == WorkflowStatusString.SUCCESS.value, before=before, after=after)
    invariant("timeout_fresh_pending_preserved", after[fresh_pending_id] is not None and after[fresh_pending_id]["status"] == WorkflowStatusString.PENDING.value, before=before[fresh_pending_id], after=after[fresh_pending_id])
    release_recovery_workflow(pending_id)
    release_recovery_workflow(fresh_pending_id)
    return {"cutoff_epoch_ms": cutoff_epoch_ms, "before": before, "after": after, "counters": {pending_id: counters(pending_id), fresh_pending_id: counters(fresh_pending_id)}}


def case_global_timeout_delayed_minimization(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    delayed_id = f"{plan.workflow_prefix}-timeout-delayed-only"
    with SetWorkflowID(delayed_id):
        with SetEnqueueOptions(delay_seconds=60.0):
            DBOS.enqueue_workflow(QUEUE_NAME, queue_success_workflow, delayed_id, "delayed-only")

    before = status_snapshot(delayed_id)
    invariant(
        "timeout_min_delayed_setup",
        before is not None and before["status"] == WorkflowStatusString.DELAYED.value and before["delay_until_epoch_ms"] is not None,
        before=before,
    )
    time.sleep(1.1)
    cutoff_epoch_ms = int(time.time() * 1000)
    global_timeout(dbos, cutoff_epoch_ms)
    after = status_snapshot(delayed_id)
    invariant(
        "timeout_min_delayed_preserved",
        after is not None and after["status"] == WorkflowStatusString.DELAYED.value,
        cutoff_epoch_ms=cutoff_epoch_ms,
        before=before,
        after=after,
    )
    return {"workflow_id": delayed_id, "cutoff_epoch_ms": cutoff_epoch_ms, "before": before, "after": after}


def case_garbage_collect_active_work(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    completed_id = f"{plan.workflow_prefix}-gc-completed-old"
    with SetWorkflowID(completed_id):
        completed_result = instant_success_workflow(completed_id, "gc-completed")
    invariant("gc_completed_setup", completed_result == "gc-completed", status=status_snapshot(completed_id))

    pending_id, _pending_handle = start_active_blocked_workflow(plan, "gc-pending-old", f"gc-pending-{plan.seed}")

    delayed_id = f"{plan.workflow_prefix}-gc-delayed-old"
    with SetWorkflowID(delayed_id):
        with SetEnqueueOptions(delay_seconds=60.0):
            DBOS.enqueue_workflow(QUEUE_NAME, queue_success_workflow, delayed_id, "gc-delayed")

    DBOS.register_queue(IDLE_QUEUE_NAME, worker_concurrency=0, concurrency=0, polling_interval_sec=0.1, on_conflict="always_update")
    enqueued_id = f"{plan.workflow_prefix}-gc-enqueued-old"
    with SetWorkflowID(enqueued_id):
        DBOS.enqueue_workflow(IDLE_QUEUE_NAME, queue_success_workflow, enqueued_id, "gc-enqueued")

    fork_source_id, _source_handle = start_active_blocked_workflow(plan, "gc-fork-source-old", f"gc-fork-{plan.seed}")
    fork_id = f"{fork_source_id}-fork-step-2"
    with SetWorkflowID(fork_id):
        DBOS.fork_workflow(fork_source_id, 2)

    time.sleep(0.3)
    ids = [completed_id, pending_id, delayed_id, enqueued_id, fork_source_id, fork_id]
    before = {wid: status_snapshot(wid) for wid in ids}
    cutoff_epoch_ms = int(time.time() * 1000)
    garbage_collect(dbos, cutoff_epoch_timestamp_ms=cutoff_epoch_ms, rows_threshold=None)
    after = {wid: status_snapshot(wid) for wid in ids}

    invariant("gc_completed_deleted", after[completed_id] is None, before=before[completed_id], after=after[completed_id])
    invariant("gc_active_pending_preserved", after[pending_id] is not None and after[pending_id]["status"] == WorkflowStatusString.PENDING.value, before=before[pending_id], after=after[pending_id])
    invariant("gc_delayed_preserved", after[delayed_id] is not None and after[delayed_id]["status"] == WorkflowStatusString.DELAYED.value, before=before[delayed_id], after=after[delayed_id])
    invariant("gc_enqueued_preserved", after[enqueued_id] is not None and after[enqueued_id]["status"] == WorkflowStatusString.ENQUEUED.value, before=before[enqueued_id], after=after[enqueued_id])
    invariant("gc_fork_graph_active_rows_preserved", after[fork_source_id] is not None and after[fork_id] is not None, before={fork_source_id: before[fork_source_id], fork_id: before[fork_id]}, after={fork_source_id: after[fork_source_id], fork_id: after[fork_id]})
    release_recovery_workflow(pending_id)
    release_recovery_workflow(fork_source_id)
    return {"cutoff_epoch_ms": cutoff_epoch_ms, "before": before, "after": after}


def case_cancel_after_final_step(plan: CasePlan) -> dict[str, Any]:
    workflow_id = f"{plan.workflow_prefix}-cancel-final"
    payload = f"payload-{plan.seed}"
    gates = gates_for(workflow_id)
    with SetWorkflowID(workflow_id):
        handle = DBOS.start_workflow(blocking_two_step_workflow, workflow_id, payload)

    reached = gates["after_final_step_before_return"].wait(timeout=10)
    invariant("cancel_final_target_window_reached", reached, workflow_id=workflow_id, counters=counters(workflow_id))
    before_cancel = assert_status(workflow_id, WorkflowStatusString.PENDING.value, "cancel_final_status_pending_before_cancel")
    invariant("cancel_final_step_counters_before_cancel", counters(workflow_id) == {"step_one": 1, "step_two": 1}, counters=counters(workflow_id))

    DBOS.cancel_workflow(workflow_id)
    after_cancel = assert_status(workflow_id, WorkflowStatusString.CANCELLED.value, "cancel_final_status_cancelled_after_cancel")
    assert_queued_absent(workflow_id, "cancel_final_not_queued_after_cancel")
    gates["release_workflow"].set()

    cancelled_error = None
    try:
        handle.get_result(polling_interval_sec=0.05)
    except BaseException as exc:
        cancelled_error = type(exc).__name__
        invariant(
            "cancel_final_handle_raises_cancelled",
            isinstance(exc, dbos_error.DBOSAwaitedWorkflowCancelledError),
            workflow_id=workflow_id,
            error=cancelled_error,
        )
    invariant("cancel_final_handle_observed_cancelled", cancelled_error is not None, workflow_id=workflow_id)
    after_original_release = assert_status(workflow_id, WorkflowStatusString.CANCELLED.value, "cancel_final_status_still_cancelled_after_late_return")

    first_resume = DBOS.resume_workflow(workflow_id)
    resume_result = first_resume.get_result(polling_interval_sec=0.05)
    after_resume = assert_status(workflow_id, WorkflowStatusString.SUCCESS.value, "cancel_final_status_success_after_resume")
    invariant("cancel_final_resume_result_matches_payload", resume_result == payload, result=resume_result, expected=payload)
    invariant("cancel_final_resume_did_not_duplicate_checkpointed_steps", counters(workflow_id) == {"step_one": 1, "step_two": 1}, counters=counters(workflow_id))

    second_resume = DBOS.resume_workflow(workflow_id)
    second_result = second_resume.get_result(polling_interval_sec=0.05)
    after_second_resume = assert_status(workflow_id, WorkflowStatusString.SUCCESS.value, "cancel_final_status_success_after_second_resume")
    invariant("cancel_final_second_resume_idempotent_result", second_result == payload, result=second_result, expected=payload)
    invariant("cancel_final_second_resume_no_duplicate_steps", counters(workflow_id) == {"step_one": 1, "step_two": 1}, counters=counters(workflow_id))
    assert_list_exact([workflow_id], [workflow_id], "cancel_final_live_row_present_after_success")

    return {
        "workflow_id": workflow_id,
        "statuses": {
            "before_cancel": before_cancel,
            "after_cancel": after_cancel,
            "after_original_release": after_original_release,
            "after_resume": after_resume,
            "after_second_resume": after_second_resume,
        },
        "counters": counters(workflow_id),
        "cancelled_error": cancelled_error,
        "resume_result": resume_result,
        "second_result": second_result,
    }


def case_delayed_cancel_resume(plan: CasePlan) -> dict[str, Any]:
    workflow_id = f"{plan.workflow_prefix}-delayed"
    payload = f"delayed-{plan.seed}"
    with SetWorkflowID(workflow_id):
        with SetEnqueueOptions(delay_seconds=60.0):
            handle = DBOS.enqueue_workflow(QUEUE_NAME, queue_success_workflow, workflow_id, payload)

    delayed = assert_status(workflow_id, WorkflowStatusString.DELAYED.value, "delayed_status_initially_delayed")
    invariant("delayed_workflow_has_delay_metadata", delayed is not None and delayed["delay_until_epoch_ms"] is not None, snapshot=delayed)
    assert_list_exact([workflow_id], [workflow_id], "delayed_live_row_present_before_cancel")

    DBOS.cancel_workflow(workflow_id)
    cancelled = assert_status(workflow_id, WorkflowStatusString.CANCELLED.value, "delayed_status_cancelled")
    assert_queued_absent(workflow_id, "delayed_cancelled_absent_from_queued_listing")
    invariant("delayed_cancelled_did_not_execute_workflow", counters(workflow_id) == {}, counters=counters(workflow_id))

    resumed_handle = DBOS.resume_workflow(workflow_id)
    resumed_result = resumed_handle.get_result(polling_interval_sec=0.05)
    success = assert_status(workflow_id, WorkflowStatusString.SUCCESS.value, "delayed_status_success_after_resume")
    invariant("delayed_resume_result_matches_payload", resumed_result == payload, result=resumed_result, expected=payload)
    invariant("delayed_resume_runs_once", counters(workflow_id) == {"queue_step": 1}, counters=counters(workflow_id))
    assert_queued_absent(workflow_id, "delayed_success_absent_from_queued_listing")
    invariant("delayed_original_handle_observes_success", handle.get_status().status == WorkflowStatusString.SUCCESS.value, status=handle.get_status().status)

    return {
        "workflow_id": workflow_id,
        "statuses": {
            "delayed": delayed,
            "cancelled": cancelled,
            "success": success,
        },
        "counters": counters(workflow_id),
        "resume_result": resumed_result,
    }


def call_stale_commands(workflow_id: str) -> list[dict[str, Any]]:
    observations = []
    for operation, fn in [
        ("cancel_workflow", lambda: DBOS.cancel_workflow(workflow_id)),
        ("resume_workflow", lambda: DBOS.resume_workflow(workflow_id)),
        ("set_workflow_delay", lambda: DBOS.set_workflow_delay(workflow_id, delay_seconds=1.0)),
    ]:
        before = status_snapshot(workflow_id)
        result_type = None
        error_type = None
        try:
            result = fn()
            result_type = type(result).__name__
        except BaseException as exc:
            error_type = type(exc).__name__
        after = status_snapshot(workflow_id)
        observations.append(
            {
                "operation": operation,
                "before": before,
                "after": after,
                "result_type": result_type,
                "error_type": error_type,
            }
        )
    return observations


def case_terminal_immutability(plan: CasePlan) -> dict[str, Any]:
    success_id = f"{plan.workflow_prefix}-success"
    error_id = f"{plan.workflow_prefix}-error"
    success_payload = f"success-{plan.seed}"

    with SetWorkflowID(success_id):
        success_result = instant_success_workflow(success_id, success_payload)
    invariant("terminal_success_initial_result_matches", success_result == success_payload, result=success_result, expected=success_payload)
    success_before = assert_status(success_id, WorkflowStatusString.SUCCESS.value, "terminal_success_initial_status")
    invariant("terminal_success_counter_once", counters(success_id) == {"success_step": 1}, counters=counters(success_id))

    with SetWorkflowID(error_id):
        error_handle = DBOS.start_workflow(deterministic_error_workflow, error_id)
    error_name = None
    try:
        error_handle.get_result(polling_interval_sec=0.05)
    except BaseException as exc:
        error_name = type(exc).__name__
    invariant("terminal_error_initial_result_raises", error_name == "RuntimeError", error=error_name)
    error_before = assert_status(error_id, WorkflowStatusString.ERROR.value, "terminal_error_initial_status")
    invariant("terminal_error_counter_once", counters(error_id) == {"error_step": 1}, counters=counters(error_id))

    observations = {
        success_id: call_stale_commands(success_id),
        error_id: call_stale_commands(error_id),
    }
    success_after_commands = assert_status(success_id, WorkflowStatusString.SUCCESS.value, "terminal_success_status_immutable_after_stale_commands")
    error_after_commands = assert_status(error_id, WorkflowStatusString.ERROR.value, "terminal_error_status_immutable_after_stale_commands")
    invariant("terminal_success_steps_not_duplicated", counters(success_id) == {"success_step": 1}, counters=counters(success_id))
    invariant("terminal_error_steps_not_duplicated", counters(error_id) == {"error_step": 1}, counters=counters(error_id))
    assert_list_exact([success_id, error_id], [success_id, error_id], "terminal_rows_present_before_delete")

    DBOS.delete_workflow(success_id, delete_children=False)
    DBOS.delete_workflow(error_id, delete_children=False)
    success_deleted = status_snapshot(success_id)
    error_deleted = status_snapshot(error_id)
    invariant("terminal_success_deleted_not_queryable", success_deleted is None, snapshot=success_deleted)
    invariant("terminal_error_deleted_not_queryable", error_deleted is None, snapshot=error_deleted)
    assert_list_exact([success_id, error_id], [], "terminal_deleted_rows_absent_from_list")

    return {
        "workflow_ids": {"success": success_id, "error": error_id},
        "statuses": {
            "success_before": success_before,
            "error_before": error_before,
            "success_after_commands": success_after_commands,
            "error_after_commands": error_after_commands,
            "success_deleted": success_deleted,
            "error_deleted": error_deleted,
        },
        "observations": observations,
        "counters": {success_id: counters(success_id), error_id: counters(error_id)},
    }


def case_fork_step_prefixes(plan: CasePlan) -> dict[str, Any]:
    original_id = f"{plan.workflow_prefix}-steps-original"
    _MULTIPLIERS[original_id] = 1
    with SetWorkflowID(original_id):
        original_result = four_step_multiplier_workflow(original_id)
    invariant("fork_steps_original_result", original_result == 10, result=original_result, expected=10)
    invariant(
        "fork_steps_original_counters_once",
        counters(original_id) == {"step_one": 1, "step_two": 1, "step_three": 1, "step_four": 1},
        counters=counters(original_id),
    )

    _MULTIPLIERS[original_id] = 10
    expected_results = {1: 100, 2: 91, 4: 46}
    expected_counter_deltas = {
        1: {"step_one": 1, "step_two": 1, "step_three": 1, "step_four": 1},
        2: {"step_two": 1, "step_three": 1, "step_four": 1},
        4: {"step_four": 1},
    }
    fork_results: dict[str, Any] = {}
    for start_step in (1, 2, 4):
        before = counters(original_id)
        fork_id = f"{plan.workflow_prefix}-steps-fork-{start_step}"
        with SetWorkflowID(fork_id):
            fork_handle = DBOS.fork_workflow(original_id, start_step)
        fork_status = fork_handle.get_status()
        invariant("fork_steps_status_forked_from", fork_status.forked_from == original_id, fork_id=fork_id, status=status_snapshot(fork_id))
        result = fork_handle.get_result(polling_interval_sec=0.05)
        after = counters(original_id)
        delta = {key: after.get(key, 0) - before.get(key, 0) for key in sorted(after)}
        delta = {key: value for key, value in delta.items() if value}
        invariant(f"fork_steps_result_start_{start_step}", result == expected_results[start_step], result=result, expected=expected_results[start_step])
        invariant(f"fork_steps_suffix_counters_start_{start_step}", delta == expected_counter_deltas[start_step], delta=delta, expected=expected_counter_deltas[start_step])
        fork_results[str(start_step)] = {"workflow_id": fork_id, "result": result, "delta": delta, "status": status_snapshot(fork_id)}

    original_status = status_of(original_id)
    invariant("fork_steps_original_marked_was_forked_from", original_status is not None and original_status.was_forked_from is True, status=status_snapshot(original_id))
    fork_ids = sorted(data["workflow_id"] for data in fork_results.values())
    listed_forks = sorted(workflow.workflow_id for workflow in DBOS.list_workflows(forked_from=original_id))
    invariant("fork_steps_list_forked_from_matches_model", listed_forks == fork_ids, listed=listed_forks, expected=fork_ids)
    invariant("fork_steps_original_result_not_mutated", status_value(original_id) == WorkflowStatusString.SUCCESS.value, status=status_snapshot(original_id))
    return {"original_id": original_id, "forks": fork_results, "counters": counters(original_id)}


def case_fork_event_prefix(plan: CasePlan) -> dict[str, Any]:
    original_id = f"{plan.workflow_prefix}-events-original"
    key = f"event-key-{plan.seed}"
    gates = gates_for(original_id)
    gates["release_workflow"].set()
    with SetWorkflowID(original_id):
        original_handle = DBOS.start_workflow(fork_event_prefix_workflow, original_id, key)
    invariant("fork_events_original_completed", original_handle.get_result(polling_interval_sec=0.05) == original_id, workflow_id=original_id)
    invariant("fork_events_original_final_event", DBOS.get_event(original_id, key) == "v2", events=all_events(original_id))

    gates["release_workflow"].clear()
    expected_prefixes = {1: {}, 2: {key: "v1"}, 3: {key: "step-v"}, 4: {key: "v2"}}
    fork_data: dict[str, Any] = {}
    for start_step, expected_events in expected_prefixes.items():
        fork_id = f"{plan.workflow_prefix}-events-fork-{start_step}"
        with SetWorkflowID(fork_id):
            handle = DBOS.fork_workflow(original_id, start_step)
        observed_events = all_events(fork_id)
        observed_value = DBOS.get_event(fork_id, key, timeout_seconds=0.0)
        expected_value = expected_events.get(key)
        invariant(f"fork_events_prefix_start_{start_step}", observed_events == expected_events and observed_value == expected_value, observed_events=observed_events, observed_value=observed_value, expected_events=expected_events, expected_value=expected_value)
        fork_data[str(start_step)] = {"workflow_id": fork_id, "pre_events": observed_events, "pre_value": observed_value}

    gates["release_workflow"].set()
    for start_step, data in fork_data.items():
        result = DBOS.retrieve_workflow(data["workflow_id"]).get_result(polling_interval_sec=0.05)
        final_events = all_events(data["workflow_id"])
        invariant(f"fork_events_final_converges_start_{start_step}", result == data["workflow_id"] and final_events == {key: "v2"}, result=result, final_events=final_events)
        data["final_events"] = final_events
    return {"original_id": original_id, "key": key, "forks": fork_data, "original_events": all_events(original_id)}


def case_fork_stream_prefix(plan: CasePlan) -> dict[str, Any]:
    original_id = f"{plan.workflow_prefix}-streams-original"
    stream_a = f"stream-a-{plan.seed}"
    stream_b = f"stream-b-{plan.seed}"
    gates = gates_for(original_id)
    gates["release_workflow"].set()
    with SetWorkflowID(original_id):
        original_handle = DBOS.start_workflow(fork_stream_prefix_workflow, original_id, stream_a, stream_b)
    invariant("fork_streams_original_completed", original_handle.get_result(polling_interval_sec=0.05) == original_id, workflow_id=original_id)
    invariant("fork_streams_original_a_values", read_stream_all(original_id, stream_a) == ["a0", "a1", "a2"], values=read_stream_all(original_id, stream_a))
    invariant("fork_streams_original_b_values", read_stream_all(original_id, stream_b) == ["b0"], values=read_stream_all(original_id, stream_b))

    gates["release_workflow"].clear()
    expected_counts = {
        1: {stream_a: [], stream_b: []},
        2: {stream_a: ["a0"], stream_b: []},
        3: {stream_a: ["a0"], stream_b: ["b0"]},
        4: {stream_a: ["a0", "a1"], stream_b: ["b0"]},
        5: {stream_a: ["a0", "a1"], stream_b: ["b0"]},
    }
    fork_data: dict[str, Any] = {}
    for start_step, expected in expected_counts.items():
        fork_id = f"{plan.workflow_prefix}-streams-fork-{start_step}"
        with SetWorkflowID(fork_id):
            handle = DBOS.fork_workflow(original_id, start_step)
        observed = {
            stream_a: read_stream_count(fork_id, stream_a, len(expected[stream_a])),
            stream_b: read_stream_count(fork_id, stream_b, len(expected[stream_b])),
        }
        invariant(f"fork_streams_prefix_start_{start_step}", observed == expected, observed=observed, expected=expected)
        fork_data[str(start_step)] = {"workflow_id": fork_id, "pre_streams": observed}

    gates["release_workflow"].set()
    for start_step, data in fork_data.items():
        result = DBOS.retrieve_workflow(data["workflow_id"]).get_result(polling_interval_sec=0.05)
        final_streams = {stream_a: read_stream_all(data["workflow_id"], stream_a), stream_b: read_stream_all(data["workflow_id"], stream_b)}
        invariant(f"fork_streams_final_converges_start_{start_step}", result == data["workflow_id"] and final_streams == {stream_a: ["a0", "a1", "a2"], stream_b: ["b0"]}, result=result, final_streams=final_streams)
        data["final_streams"] = final_streams
    return {"original_id": original_id, "streams": [stream_a, stream_b], "forks": fork_data}


def case_attribute_update_clear_filter(plan: CasePlan) -> dict[str, Any]:
    original_id = f"{plan.workflow_prefix}-attrs-original"
    initial = {"customer": "acme", "tier": 1, "active": True}
    updated = {"customer": "acme", "tier": 2, "active": True}
    fork_id = f"{plan.workflow_prefix}-attrs-fork"
    with SetWorkflowAttributes(initial):
        with SetWorkflowID(original_id):
            attribute_noop_workflow()
    invariant("fork_attrs_initial_recorded", workflow_attrs(original_id) == initial, attrs=workflow_attrs(original_id), expected=initial)
    DBOS.update_workflow_attributes(original_id, updated)
    invariant("fork_attrs_update_replaces_whole_dict", workflow_attrs(original_id) == updated, attrs=workflow_attrs(original_id), expected=updated)
    with SetWorkflowID(fork_id):
        fork_handle = DBOS.fork_workflow(original_id, 1)
    invariant("fork_attrs_fork_completed", fork_handle.get_result(polling_interval_sec=0.05) == fork_id, result=fork_handle.get_result(polling_interval_sec=0.05), fork_id=fork_id)
    invariant("fork_attrs_fork_inherits_current_attrs", workflow_attrs(fork_id) == updated, attrs=workflow_attrs(fork_id), expected=updated)
    invariant("fork_attrs_filter_before_clear", attribute_filter_ids({"customer": "acme", "tier": 2}) == sorted([original_id, fork_id]), observed=attribute_filter_ids({"customer": "acme", "tier": 2}), expected=sorted([original_id, fork_id]))
    DBOS.update_workflow_attributes(original_id, None)
    invariant("fork_attrs_original_clear", workflow_attrs(original_id) is None, attrs=workflow_attrs(original_id))
    invariant("fork_attrs_fork_survives_original_clear", workflow_attrs(fork_id) == updated, attrs=workflow_attrs(fork_id), expected=updated)
    invariant("fork_attrs_filter_after_clear", attribute_filter_ids({"customer": "acme", "tier": 2}) == [fork_id], observed=attribute_filter_ids({"customer": "acme", "tier": 2}), expected=[fork_id])
    return {"original_id": original_id, "fork_id": fork_id, "initial": initial, "updated": updated}


def case_replacement_children(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    parent_key = f"{plan.workflow_prefix}-replacement"
    parent_id = f"{parent_key}-parent"
    _MULTIPLIERS[parent_key] = 2
    with SetWorkflowID(parent_id):
        parent_handle = DBOS.start_workflow(replacement_parent_workflow, parent_key)
    original_result = parent_handle.get_result(polling_interval_sec=0.05)
    original_child_ids = list(_CHILD_IDS[parent_key])
    invariant("replacement_parent_original_result", original_result == 300, result=original_result, expected=300, children=original_child_ids)
    invariant("replacement_parent_original_children_count", len(original_child_ids) == 5, children=original_child_ids)

    _MULTIPLIERS[parent_key] = 10
    replacement_map: dict[str, str] = {}
    replacement_results: dict[str, int] = {}
    for index in (0, 2, 4):
        child_id = original_child_ids[index]
        fork_id = f"{child_id}-replacement"
        with SetWorkflowID(fork_id):
            child_fork = DBOS.fork_workflow(child_id, 1)
        replacement_map[child_id] = child_fork.workflow_id
        replacement_results[child_id] = child_fork.get_result(polling_interval_sec=0.05)
    invariant("replacement_children_results_match_multiplier", replacement_results == {original_child_ids[0]: 100, original_child_ids[2]: 300, original_child_ids[4]: 500}, observed=replacement_results)

    forked_parent_id = f"{parent_key}-parent-fork"
    with SetWorkflowID(forked_parent_id):
        forked_parent = DBOS.fork_workflow(parent_id, 6, replacement_children=replacement_map)
    forked_result = forked_parent.get_result(polling_interval_sec=0.05)
    invariant("replacement_parent_result_uses_replacements", forked_result == 1020, result=forked_result, expected=1020, replacement_map=replacement_map)
    invariant("replacement_original_parent_unchanged", DBOS.retrieve_workflow(parent_id).get_result(polling_interval_sec=0.05) == 300, parent_id=parent_id)
    invariant("replacement_original_children_still_queryable", list_ids(original_child_ids) == sorted(original_child_ids), observed=list_ids(original_child_ids), expected=sorted(original_child_ids))
    fork_children = child_ids(dbos, forked_parent_id)
    invariant("replacement_fork_parent_children_match_replacements", set(replacement_map.values()) <= set(fork_children), fork_children=fork_children, replacement_children=sorted(replacement_map.values()))
    return {"parent_id": parent_id, "original_children": original_child_ids, "replacement_map": replacement_map, "forked_parent_id": forked_parent_id, "fork_children": fork_children}


def replacement_child_setup(plan: CasePlan) -> dict[str, Any]:
    parent_key = f"{plan.workflow_prefix}-replacement-min"
    parent_id = f"{parent_key}-parent"
    _MULTIPLIERS[parent_key] = 2
    with SetWorkflowID(parent_id):
        parent_handle = DBOS.start_workflow(replacement_parent_workflow, parent_key)
    original_result = parent_handle.get_result(polling_interval_sec=0.05)
    original_child_ids = list(_CHILD_IDS[parent_key])
    invariant("replacement_min_original_result", original_result == 300 and len(original_child_ids) == 5, result=original_result, children=original_child_ids)
    _MULTIPLIERS[parent_key] = 10
    replacement_map: dict[str, str] = {}
    for index in (0, 2, 4):
        child_id = original_child_ids[index]
        fork_id = f"{child_id}-replacement"
        with SetWorkflowID(fork_id):
            child_fork = DBOS.fork_workflow(child_id, 1)
        replacement_map[child_id] = child_fork.workflow_id
        child_fork.get_result(polling_interval_sec=0.05)
    forked_parent_id = f"{parent_key}-parent-fork"
    with SetWorkflowID(forked_parent_id):
        forked_parent = DBOS.fork_workflow(parent_id, 6, replacement_children=replacement_map)
    forked_result = forked_parent.get_result(polling_interval_sec=0.05)
    invariant("replacement_min_forked_parent_uses_replacements", forked_result == 1020, result=forked_result, replacement_map=replacement_map)
    return {
        "parent_id": parent_id,
        "original_children": original_child_ids,
        "replacement_map": replacement_map,
        "replacement_children": sorted(replacement_map.values()),
        "forked_parent_id": forked_parent_id,
    }


def case_replacement_delete_minimization(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    setup = replacement_child_setup(plan)
    forked_parent_id = setup["forked_parent_id"]
    replacement_children = setup["replacement_children"]
    before_children = child_ids(dbos, forked_parent_id)
    event("replacement_min_before_delete", forked_parent_id=forked_parent_id, graph_children=before_children, replacement_children=replacement_children)
    DBOS.delete_workflow(forked_parent_id, delete_children=True)
    forked_parent_status = status_snapshot(forked_parent_id)
    replacement_statuses = {child_id: status_snapshot(child_id) for child_id in replacement_children}
    invariant("replacement_min_forked_parent_deleted", forked_parent_status is None, status=forked_parent_status)
    invariant("replacement_min_delete_children_removes_replacements", all(status is None for status in replacement_statuses.values()), replacement_statuses=replacement_statuses, before_graph_children=before_children)
    return {**setup, "before_graph_children": before_children, "replacement_statuses_after_delete": replacement_statuses}


def case_delete_children_cleanup(dbos: DBOS, plan: CasePlan) -> dict[str, Any]:
    parent_keep_id = f"{plan.workflow_prefix}-delete-keep-parent"
    with SetWorkflowID(parent_keep_id):
        keep_result = delete_case_parent_workflow(5)
    keep_children = child_ids(dbos, parent_keep_id)
    invariant("delete_keep_parent_result", keep_result == 10 and len(keep_children) == 1, result=keep_result, children=keep_children)
    keep_child = keep_children[0]
    invariant("delete_keep_child_transaction_output_exists", transaction_output_count(dbos, keep_child) == 1, count=transaction_output_count(dbos, keep_child))
    DBOS.delete_workflow(parent_keep_id, delete_children=False)
    invariant("delete_keep_parent_removed", status_snapshot(parent_keep_id) is None, status=status_snapshot(parent_keep_id))
    invariant("delete_keep_child_survives", status_snapshot(keep_child) is not None, status=status_snapshot(keep_child))
    invariant("delete_keep_child_transaction_output_survives", transaction_output_count(dbos, keep_child) == 1, count=transaction_output_count(dbos, keep_child))

    parent_delete_id = f"{plan.workflow_prefix}-delete-cascade-parent"
    with SetWorkflowID(parent_delete_id):
        cascade_result = delete_case_parent_workflow(7)
    cascade_children = child_ids(dbos, parent_delete_id)
    invariant("delete_cascade_parent_result", cascade_result == 14 and len(cascade_children) == 1, result=cascade_result, children=cascade_children)
    cascade_child = cascade_children[0]
    invariant("delete_cascade_child_transaction_output_exists", transaction_output_count(dbos, cascade_child) == 1, count=transaction_output_count(dbos, cascade_child))
    DBOS.delete_workflow(parent_delete_id, delete_children=True)
    invariant("delete_cascade_parent_removed", status_snapshot(parent_delete_id) is None, status=status_snapshot(parent_delete_id))
    invariant("delete_cascade_child_removed", status_snapshot(cascade_child) is None, status=status_snapshot(cascade_child))
    invariant("delete_cascade_child_transaction_output_removed", transaction_output_count(dbos, cascade_child) == 0, count=transaction_output_count(dbos, cascade_child))
    return {
        "keep": {"parent": parent_keep_id, "child": keep_child},
        "cascade": {"parent": parent_delete_id, "child": cascade_child},
    }


def write_artifact(artifact_dir: Path, plan: CasePlan, result: dict[str, Any]) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    promise = (
        "cancel, resume, delete, delay, success, and error lifecycle commands obey durable DBOS workflow-state legality"
        if plan.rung_id == RUNG_001_ID
        else (
            "forked and child workflow state carries exactly the modeled steps, events, streams, attributes, and delete semantics"
            if plan.rung_id == RUNG_002_ID
            else (
                "lifecycle commands remain legal and durable when recovery or executor interruption races with management actions"
                if plan.rung_id == RUNG_003_ID
                else (
                    "the lifecycle state-machine, fork graph, recovery, timeout, and cleanup invariants survive a bounded cross-product sweep"
                    if plan.rung_id == RUNG_004_ID
                    else "recursive cancellation through DBOS and DBOSClient preserves modeled child ownership, queue cleanup, result errors, and terminal CANCELLED immutability"
                )
            )
        )
    )
    oracle = (
        "independent lifecycle state model checked after each public lifecycle command using status/list/queued APIs and step counters"
        if plan.rung_id == RUNG_001_ID
        else (
            "independent fork graph model checked against forked_from, was_forked_from, child links, events, stream prefixes, attribute filters, and transaction-output rows"
            if plan.rung_id == RUNG_002_ID
            else (
                "recovery-window model checked against executor IDs, recovery handles, workflow statuses, step side-effect counters, DLQ status, timeout, and cleanup preservation"
                if plan.rung_id == RUNG_003_ID
                else (
                    "bounded sweep reuses the independent lifecycle, fork graph, recovery, timeout, and cleanup models without weakening invariants"
                    if plan.rung_id == RUNG_004_ID
                    else "independent tree and queue model checked against child graph traversal, status/list/queued APIs, DBOSClient parity, result exceptions, and step counters after cancellation and release"
                )
            )
        )
    )
    data = {
        "frontier_id": FRONTIER_ID,
        "rung_id": plan.rung_id,
        "prompt_event_path": "evidence-key:events/frontier_designer-20260620T072707741733000Z.prompt.md",
        "protected_product_promise": promise,
        "replay_command": f"python .workers/workloads/lifecycle-fork-state/lifecycle_fork_state_workload.py --rung {plan.rung_id} --case {plan.case_id} --artifact-dir <artifact-dir>",
        "seed_policy": "fixed seed per rung front matter; seed derives workflow IDs and database names only",
        "invariant_oracle": oracle,
        "plan": asdict(plan),
        "result": result,
    }
    (artifact_dir / plan.artifact_name).write_text(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n")


def run_case(plan: CasePlan, artifact_dir: Path) -> int:
    dbos = launch(plan, executor_id=executor_a(plan) if plan.rung_id in {RUNG_003_ID, RUNG_004_ID} else None)
    try:
        runners: dict[str, Callable[[DBOS, CasePlan], dict[str, Any]]] = {
            "cancel-after-final-step-before-return": lambda _dbos, p: case_cancel_after_final_step(p),
            "delayed-cancel-before-poller-then-resume": lambda _dbos, p: case_delayed_cancel_resume(p),
            "stale-commands-after-success-and-error": lambda _dbos, p: case_terminal_immutability(p),
            "fork-step-prefixes-1-2-4": lambda _dbos, p: case_fork_step_prefixes(p),
            "fork-event-prefix-before-after-update": lambda _dbos, p: case_fork_event_prefix(p),
            "fork-stream-interleaved-prefix": lambda _dbos, p: case_fork_stream_prefix(p),
            "fork-attribute-update-clear-filter": lambda _dbos, p: case_attribute_update_clear_filter(p),
            "replacement-children-parent-refork": case_replacement_children,
            "delete-parent-with-and-without-children": case_delete_children_cleanup,
            "replacement-children-delete-minimization": case_replacement_delete_minimization,
            "cancel-before-recovery-claim": lambda _dbos, p: case_recovery_cancel_before_claim(p),
            "resume-after-recovery-handle-before-release": lambda _dbos, p: case_recovery_resume_after_handle(p),
            "fork-during-recovered-execution": lambda _dbos, p: case_recovery_fork_during_execution(p),
            "dlq-resume-recover": case_dlq_resume_recover,
            "timeout-sweep-near-lifecycle-states": case_global_timeout_sweep,
            "garbage-collect-active-forked-queued": case_garbage_collect_active_work,
            "global-timeout-delayed-minimization": case_global_timeout_delayed_minimization,
            "parent-child-grandchild-cancel-mode-toggle": case_cancel_tree_mode_toggle,
            "recursive-cancel-with-queued-descendant": case_recursive_cancel_queued_descendant,
            "client-recursive-cancel-result-parity": case_client_recursive_cancel_result_parity,
        }
        if plan.schedule not in runners:
            raise SetupBlock(f"unsupported schedule {plan.schedule}")
        result = runners[plan.schedule](dbos, plan)
        write_artifact(artifact_dir, plan, result)
        event("case_passed", case=plan.case_id, result=result)
        return 0
    finally:
        DBOS.destroy(destroy_registry=False)
        cleanup_databases(plan)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DBOS lifecycle fork state workload")
    parser.add_argument("--rung", default=RUNG_001_ID)
    parser.add_argument("--case", choices=[f"case-{i:03d}" for i in range(1, 25)])
    parser.add_argument("--seed", type=int)
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument("--artifact-dir", default="/tmp/wio-artifacts/lifecycle-fork-state")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rung = normalize_rung(args.rung)
    if args.all_cases:
        if rung == RUNG_001_ID:
            cases = ["case-001", "case-002", "case-003"]
        elif rung == RUNG_004_ID:
            cases = [f"case-{i:03d}" for i in range(1, 25)]
        elif rung == RUNG_005_ID:
            cases = ["case-001", "case-002", "case-003", "case-004"]
        else:
            cases = ["case-001", "case-002", "case-003", "case-004", "case-005", "case-006"]
    elif args.case:
        cases = [args.case]
    else:
        raise SetupBlock("--case or --all-cases is required")
    if args.all_cases and not args.sequential:
        raise SetupBlock("--all-cases currently requires --sequential to keep DBOS global state isolated")
    try:
        for case_id in cases:
            run_case(make_plan(rung, case_id, args.seed if len(cases) == 1 else None), Path(args.artifact_dir))
        return 0
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 42
    except WorkloadFailure as exc:
        print(f"WORKLOAD-FAIL {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
