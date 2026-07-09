#!/usr/bin/env python3
"""
P7: Postgres restart during recovery — the system database crashes while a
worker is recovering another worker's workflows.

# SURFACE: Sys DB reconnect + recovery scanner + queue manager under DB outage
# MODELS:  worker-a crashes mid-jobs; worker-b recovers; postmaster is SIGKILLed
#          and restarted while recovery/drain is in flight
# ORACLE:  F1/F2 outage proven; D1 exactly-once SQL, D2 no stuck queue rows,
#          D3 all workflows SUCCESS (transient outage must not durably fail),
#          D4 bounded recovery attempts, D5 recoverer liveness (no retry storm)
# ISSUES:  #679 class (transient PG error -> durable failure), db_retry unbounded
#          loop, notification listener reconnect, half-state in step recording
# VARIANCE: WORKLOAD_SEED -> job count, crash step, kill delay, outage duration
"""

from __future__ import annotations

import argparse
import faulthandler
import json
import os
import signal
import subprocess
import sys
import threading
import time

from dbos_workload_common import (
    APP_DB,
    RUN_DIR,
    SYS_DB,
    dbos_config,
    invariant,
    kill_postgres_hard,
    postgres_ready,
    progress,
    psql,
    restart_postgres,
    subphase_env,
    workload_main,
    workload_seed_raw,
)

QUEUE_NAME = "formal_pg_restart"
WORKER_A = "worker-a"
WORKER_B = "worker-b"
NO_DEQUEUE = "__formal_no_dequeue__"

META_PATH = RUN_DIR / "meta.json"
JOBS_PATH = RUN_DIR / "jobs.json"
GO_PATH = RUN_DIR / "recover.go"
RECOVER_STARTED_PATH = RUN_DIR / "recover.started"

RECOVERER_STARTUP_TIMEOUT_SEC = int(os.environ.get("RECOVERER_STARTUP_TIMEOUT_SEC", "1200"))
RECOVERER_PRODUCT_TIMEOUT_SEC = int(os.environ.get("RECOVERER_PRODUCT_TIMEOUT_SEC", "420"))
WORKFLOW_RESULT_TIMEOUT_SEC = int(os.environ.get("WORKFLOW_RESULT_TIMEOUT_SEC", "240"))
# Inner deadlines (recover call + shared result wait) must classify the failure
# before the parent's RECOVERER_PRODUCT_TIMEOUT_SEC kill fires.
RECOVER_CALL_TIMEOUT_SEC = int(os.environ.get("RECOVER_CALL_TIMEOUT_SEC", "150"))

QUEUE_OPTS = {
    "concurrency": 4,
    "worker_concurrency": 4,
    "polling_interval_sec": 0.02,
}

faulthandler.enable()
try:
    faulthandler.register(signal.SIGUSR1, all_threads=True)
except (AttributeError, RuntimeError, ValueError):
    pass


def init_app_schema() -> None:
    psql(
        """
        CREATE TABLE IF NOT EXISTS processed_jobs(
          job_id TEXT PRIMARY KEY,
          executor TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS step_runs(
          id SERIAL PRIMARY KEY,
          job_id TEXT NOT NULL,
          step_name TEXT NOT NULL,
          executor TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS job_state(
          job_id TEXT PRIMARY KEY,
          state TEXT NOT NULL CHECK(state IN ('inflight', 'done'))
        );
        TRUNCATE step_runs, job_state, processed_jobs RESTART IDENTITY;
        """,
        database=APP_DB,
    )


def build_plan(root_seed: int) -> dict[str, object]:
    job_count = 8 + (root_seed % 5)
    crash_at_step = 1 if (root_seed % 2) == 0 else 2
    kill_delay_ms = (root_seed >> 3) % 1600
    outage_sec = 1 + ((root_seed >> 11) % 4)
    return {
        "jobs": [
            {"job_id": f"pgr_job_{i:03d}", "crash_at_step": crash_at_step}
            for i in range(job_count)
        ],
        "job_count": job_count,
        "crash_at_step": crash_at_step,
        "kill_delay_ms": kill_delay_ms,
        "outage_sec": outage_sec,
    }


def register_queue(dbos) -> None:
    dbos.register_queue(
        QUEUE_NAME,
        concurrency=QUEUE_OPTS["concurrency"],
        worker_concurrency=QUEUE_OPTS["worker_concurrency"],
        polling_interval_sec=QUEUE_OPTS["polling_interval_sec"],
    )


def run_phase_enqueue(jobs: list[dict[str, object]]) -> list[str]:
    from dbos import DBOS

    DBOS.destroy(destroy_registry=True)
    DBOS(config=dbos_config())
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_queue_wf import JobWF

    workflow_ids: list[str] = []
    for job in jobs:
        handle = DBOS.enqueue_workflow(
            QUEUE_NAME, JobWF.process_job, str(job["job_id"]), int(job["crash_at_step"])
        )
        workflow_ids.append(handle.workflow_id)
        progress("enqueued", f"job_id={job['job_id']} workflow_id={handle.workflow_id}")

    DBOS.destroy(destroy_registry=True)
    return workflow_ids


def run_phase_worker_a() -> None:
    from dbos import DBOS

    DBOS.destroy(destroy_registry=True)
    DBOS(config=dbos_config())
    DBOS.listen_queues([QUEUE_NAME])
    os.environ["DBOS_CRASH_NOW"] = "1"
    DBOS.launch()
    register_queue(DBOS)

    from dbos_queue_wf import JobWF  # noqa: F401

    if not threading.Event().wait(timeout=180):
        os._exit(1)


def wait_all_terminal_outage_tolerant(dbos, workflow_ids: list[str]) -> dict[str, str]:
    """Poll all workflows under one shared deadline, riding through DB connection
    errors during the outage. A shared deadline keeps the worst case bounded by
    WORKFLOW_RESULT_TIMEOUT_SEC total, so the parent's product timeout cannot
    fire before this loop classifies the failure."""
    from dbos._sys_db import WorkflowStatusString

    terminal = {
        WorkflowStatusString.SUCCESS.value,
        WorkflowStatusString.ERROR.value,
        WorkflowStatusString.CANCELLED.value,
        WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value,
    }
    deadline = time.monotonic() + WORKFLOW_RESULT_TIMEOUT_SEC
    statuses: dict[str, str] = dict.fromkeys(workflow_ids, "UNKNOWN")
    remaining = set(workflow_ids)
    while remaining and time.monotonic() < deadline:
        for workflow_id in sorted(remaining):
            try:
                status = dbos.retrieve_workflow(workflow_id).get_status().status
                statuses[workflow_id] = status
                if status in terminal:
                    progress("workflow_terminal", f"workflow_id={workflow_id} status={status}")
                    remaining.discard(workflow_id)
            except Exception as e:
                progress("status_poll_error", f"workflow_id={workflow_id} err={str(e)[:200]}")
        if remaining:
            time.sleep(0.25)
    for workflow_id in remaining:
        statuses[workflow_id] = f"TIMEOUT(last={statuses[workflow_id]})"
    return statuses


def run_phase_recoverer(workflow_ids: list[str]) -> None:
    from dbos import DBOS

    executor = os.environ.get("DBOS__VMID", "unknown")
    progress("recoverer_init_start", f"executor={executor}")
    DBOS.destroy(destroy_registry=True)
    DBOS(config=dbos_config())
    DBOS.listen_queues([QUEUE_NAME])
    DBOS.launch()
    progress("recoverer_launched", f"executor={executor}")
    register_queue(DBOS)

    from dbos_queue_wf import JobWF  # noqa: F401

    (RUN_DIR / f"{executor}.ready").write_text("ready")
    progress("recoverer_ready", f"executor={executor}")
    deadline = time.monotonic() + RECOVERER_STARTUP_TIMEOUT_SEC
    while not GO_PATH.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    if not GO_PATH.exists():
        invariant("R0b", "recoverer_barrier_released", False, f"executor={executor}")

    RECOVER_STARTED_PATH.write_text("started")
    progress("recover_worker_a", f"executor={executor}")
    recover_deadline = time.monotonic() + RECOVER_CALL_TIMEOUT_SEC
    last_error = ""
    while True:
        try:
            DBOS._recover_pending_workflows([WORKER_A])
            break
        except Exception as e:
            # The DB outage may land inside the recovery call itself; the product
            # promise is that recovery eventually completes once the DB is back.
            last_error = str(e)[:300]
            progress("recover_call_error", last_error)
            if time.monotonic() > recover_deadline:
                print(
                    f"recovery call still failing after {RECOVER_CALL_TIMEOUT_SEC}s: {last_error}",
                    file=sys.stderr,
                )
                sys.exit(4)
            time.sleep(0.5)
    progress("recover_worker_a_done", f"executor={executor}")

    statuses = wait_all_terminal_outage_tolerant(DBOS, workflow_ids)
    failures = [(wfid, status) for wfid, status in statuses.items() if status != "SUCCESS"]

    if failures:
        # Dump before destroy so the stuck workflow threads are still parked
        # wherever they are stuck — this is the root-cause evidence.
        dump_stuck_state(executor, [wfid for wfid, _ in failures])
    DBOS.destroy(destroy_registry=True)
    if failures:
        print(f"recoverer saw non-success terminals: {failures}", file=sys.stderr)
        sys.exit(3)


def dump_stuck_state(executor: str, stuck_ids: list[str]) -> None:
    print(f"=== STUCK_STATE_DUMP executor={executor} stuck={len(stuck_ids)} ===", flush=True)
    ids = ",".join(f"'{wfid}'" for wfid in stuck_ids)
    try:
        rows = psql(
            "SELECT workflow_uuid, status, executor_id, recovery_attempts, queue_name, "
            "application_version, started_at_epoch_ms, updated_at "
            f"FROM dbos.workflow_status WHERE workflow_uuid IN ({ids});",
            database=SYS_DB,
        )
        print(f"--- workflow_status rows:\n{rows}", flush=True)
        ops = psql(
            "SELECT workflow_uuid, function_id, function_name, "
            "(output IS NOT NULL) AS has_output, (error IS NOT NULL) AS has_error, "
            "started_at_epoch_ms, completed_at_epoch_ms "
            f"FROM dbos.operation_outputs WHERE workflow_uuid IN ({ids}) "
            "ORDER BY workflow_uuid, function_id;",
            database=SYS_DB,
        )
        print(f"--- operation_outputs rows:\n{ops}", flush=True)
    except Exception as e:
        print(f"--- sysdb dump failed: {e}", flush=True)
    print("--- all thread stacks:", flush=True)
    sys.stdout.flush()
    faulthandler.dump_traceback(all_threads=True, file=sys.stdout)
    sys.stdout.flush()
    print("=== STUCK_STATE_DUMP_END ===", flush=True)


def sql_scalar(sql: str, database: str = APP_DB) -> str:
    return psql(sql, database=database).splitlines()[-1].strip()


def assert_final_invariants(workflow_ids: list[str], job_count: int) -> None:
    from dbos import DBOS
    from dbos._sys_db import WorkflowStatusString

    DBOS.destroy(destroy_registry=True)
    DBOS(config=dbos_config())
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()
    register_queue(DBOS)

    done_jobs = int(sql_scalar("SELECT COUNT(*) FROM processed_jobs;"))
    inflight = int(sql_scalar("SELECT COUNT(*) FROM job_state WHERE state = 'inflight';"))
    max_begin = int(
        sql_scalar(
            "SELECT COALESCE(MAX(c), 0) FROM ("
            "  SELECT COUNT(*) AS c FROM step_runs WHERE step_name = 'begin' GROUP BY job_id"
            ") s;"
        )
    )
    max_complete = int(
        sql_scalar(
            "SELECT COALESCE(MAX(c), 0) FROM ("
            "  SELECT COUNT(*) AS c FROM step_runs WHERE step_name = 'complete' GROUP BY job_id"
            ") s;"
        )
    )
    invariant(
        "D1",
        "jobs_exactly_once",
        done_jobs == job_count and inflight == 0 and max_begin <= 1 and max_complete <= 1,
        f"processed_jobs={done_jobs} expected={job_count} inflight={inflight} "
        f"max_begin={max_begin} max_complete={max_complete}",
    )

    pending = DBOS.list_workflows(status=WorkflowStatusString.PENDING.value)
    enqueued = DBOS.list_workflows(status=WorkflowStatusString.ENQUEUED.value)
    invariant(
        "D2",
        "no_stuck_queue_rows",
        len(pending) == 0 and len(enqueued) == 0,
        f"pending={len(pending)} enqueued={len(enqueued)}",
    )

    non_success = []
    for workflow_id in workflow_ids:
        status = DBOS.retrieve_workflow(workflow_id).get_status().status
        if status != WorkflowStatusString.SUCCESS.value:
            non_success.append((workflow_id, status))
    invariant(
        "D3",
        "transient_outage_not_durable_failure",
        not non_success,
        f"non_success={non_success}" if non_success else f"all {job_count} SUCCESS",
    )

    max_attempts = int(
        sql_scalar(
            "SELECT COALESCE(MAX(recovery_attempts), 0) FROM dbos.workflow_status;",
            database=SYS_DB,
        )
    )
    invariant(
        "D4",
        "recovery_attempts_bounded",
        max_attempts <= 3,
        f"max_recovery_attempts={max_attempts} bound=3",
    )

    DBOS.destroy(destroy_registry=True)


def _subphase_env(env: dict[str, str]) -> dict[str, str]:
    return subphase_env(__file__, env)


def _run_subphase(phase: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    progress(f"subphase_{phase}_start")
    proc = subprocess.Popen(
        [sys.executable, __file__, "--phase", phase],
        env=_subphase_env(env),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    captured: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        captured.append(line)
        print(line, end="", flush=True)
    rc = proc.wait()
    progress(f"subphase_{phase}_done", f"rc={rc}")
    return subprocess.CompletedProcess(
        args=[__file__, "--phase", phase], returncode=rc, stdout="".join(captured), stderr=""
    )


def scenario_pg_restart(root_seed: int) -> None:
    plan = build_plan(root_seed)
    job_count = int(plan["job_count"])
    JOBS_PATH.write_text(json.dumps(plan["jobs"]))
    META_PATH.write_text(json.dumps({"workflow_ids": []}))

    progress("schema_init")
    init_app_schema()
    progress(
        "plan",
        f"jobs={job_count} crash_at_step={plan['crash_at_step']} "
        f"kill_delay_ms={plan['kill_delay_ms']} outage_sec={plan['outage_sec']}",
    )

    base_env = {"WORKLOAD_SEED": workload_seed_raw()}
    enqueue = _run_subphase("enqueue", base_env)
    invariant("G1", "enqueue_phase_ready", enqueue.returncode == 0, f"rc={enqueue.returncode}")

    workflow_ids = json.loads(META_PATH.read_text()).get("workflow_ids") or []
    invariant(
        "G2",
        "enqueue_workflows_recorded",
        len(workflow_ids) == job_count,
        f"workflow_ids={len(workflow_ids)} expected={job_count}",
    )

    worker_a = _run_subphase("worker_a", {**base_env, "DBOS__VMID": WORKER_A})
    invariant(
        "G3",
        "crash_phase_reached",
        worker_a.returncode == 99,
        f"expected_rc=99 actual_rc={worker_a.returncode} tail={(worker_a.stdout or '')[-1200:]}",
    )

    for path in (RUN_DIR / f"{WORKER_B}.ready", GO_PATH, RECOVER_STARTED_PATH):
        path.unlink(missing_ok=True)

    progress("subphase_recoverer_start", WORKER_B)
    captured: list[str] = []
    recoverer = subprocess.Popen(
        [sys.executable, __file__, "--phase", "recoverer"],
        env=_subphase_env({**base_env, "DBOS__VMID": WORKER_B}),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def drain() -> None:
        assert recoverer.stdout is not None
        for line in recoverer.stdout:
            captured.append(line)
            print(f"[{WORKER_B}] {line}", end="", flush=True)

    drain_thread = threading.Thread(target=drain)
    drain_thread.start()

    ready_deadline = time.monotonic() + RECOVERER_STARTUP_TIMEOUT_SEC
    while time.monotonic() < ready_deadline and not (RUN_DIR / f"{WORKER_B}.ready").exists():
        if recoverer.poll() is not None:
            break
        time.sleep(0.1)
    invariant(
        "G4",
        "recoverer_participant_ready",
        (RUN_DIR / f"{WORKER_B}.ready").exists(),
        f"rc={recoverer.poll()} tail={''.join(captured)[-800:]}",
    )

    GO_PATH.write_text("go")
    started_deadline = time.monotonic() + 60
    while time.monotonic() < started_deadline and not RECOVER_STARTED_PATH.exists():
        time.sleep(0.02)
    invariant(
        "F0",
        "recovery_window_entered",
        RECOVER_STARTED_PATH.exists(),
        "recoverer wrote recover.started",
    )

    time.sleep(int(plan["kill_delay_ms"]) / 1000.0)
    progress("fault_kill_postgres", f"delay_ms={plan['kill_delay_ms']}")
    killed_pid = kill_postgres_hard()
    invariant(
        "F1",
        "postgres_outage_applied",
        not postgres_ready(),
        f"postmaster_pid={killed_pid} SIGKILLed; pg_isready=down",
    )

    time.sleep(int(plan["outage_sec"]))
    progress("fault_restart_postgres", f"outage_sec={plan['outage_sec']}")
    restart_postgres()
    invariant("F2", "postgres_restarted", postgres_ready(), "pg_isready=up after WAL recovery")

    try:
        rc = recoverer.wait(timeout=RECOVERER_PRODUCT_TIMEOUT_SEC)
    except subprocess.TimeoutExpired:
        progress("recoverer_product_timeout", "dumping_stack")
        try:
            recoverer.send_signal(signal.SIGUSR1)
            time.sleep(1)
        except ProcessLookupError:
            pass
        recoverer.kill()
        rc = recoverer.wait()
    drain_thread.join(timeout=5)
    progress("subphase_recoverer_done", f"rc={rc}")

    if rc != 0:
        try:
            assert_final_invariants(workflow_ids, job_count)
        except Exception as invariant_error:
            progress("post_failure_invariant_error", str(invariant_error)[:500])
        invariant(
            "D5",
            "recoverer_survives_outage",
            False,
            f"rc={rc} tail={''.join(captured)[-1200:]}",
        )

    assert_final_invariants(workflow_ids, job_count)
    progress(
        "scenario_done",
        f"jobs={job_count} kill_delay_ms={plan['kill_delay_ms']} outage_sec={plan['outage_sec']}",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["enqueue", "worker_a", "recoverer"], default="")
    args = parser.parse_args()

    if args.phase == "enqueue":
        if not META_PATH.exists() or not JOBS_PATH.exists():
            return 2
        jobs = json.loads(JOBS_PATH.read_text())
        progress("dbos_enqueue_launch")
        workflow_ids = run_phase_enqueue(jobs)
        meta = json.loads(META_PATH.read_text())
        meta["workflow_ids"] = workflow_ids
        META_PATH.write_text(json.dumps(meta))
        return 0

    if args.phase == "worker_a":
        progress("dbos_worker_a_launch")
        run_phase_worker_a()
        return 0

    if args.phase == "recoverer":
        if not META_PATH.exists():
            return 2
        workflow_ids = json.loads(META_PATH.read_text()).get("workflow_ids") or []
        progress("dbos_recoverer_launch", os.environ.get("DBOS__VMID", "unknown"))
        run_phase_recoverer(workflow_ids)
        return 0

    return workload_main("dbos_pg_restart_recovery", scenario_pg_restart)


if __name__ == "__main__":
    raise SystemExit(main())
