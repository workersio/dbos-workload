#!/usr/bin/env python3
"""
P5da: aggressive dual recovery race — more jobs, higher concurrency, parallel recoverer start.

# SURFACE: Recovery ownership + queue handoff
# MODELS:  worker-a crashes; worker-b and worker-c start together and race recovery
# ORACLE:  all workflows success, no stuck queue rows, no duplicate begin/complete effects
# ISSUES:  recovery claim races, duplicate replay, stuck pending recovery rows
# VARIANCE: job count (12–20), crash window from WORKLOAD_SEED / WENV_SEED
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
from dataclasses import dataclass
from pathlib import Path

from dbos_workload_common import (
    APP_DB,
    RUN_DIR,
    dbos_config,
    invariant,
    progress,
    psql,
    seed_int,
    subphase_env,
    workload_main,
    workload_seed_raw,
)

QUEUE_NAME = "formal_recovery_race_aggr"
WORKER_A = "worker-a"
WORKER_B = "worker-b"
WORKER_C = "worker-c"
NO_DEQUEUE = "__formal_no_dequeue__"

META_PATH = RUN_DIR / "meta.json"
JOBS_PATH = RUN_DIR / "jobs.json"
GO_PATH = RUN_DIR / "recover.go"

RECOVERER_STARTUP_TIMEOUT_SEC = int(os.environ.get("RECOVERER_STARTUP_TIMEOUT_SEC", "1200"))
RECOVERER_PRODUCT_TIMEOUT_SEC = int(os.environ.get("RECOVERER_PRODUCT_TIMEOUT_SEC", "420"))
WORKFLOW_RESULT_TIMEOUT_SEC = int(os.environ.get("WORKFLOW_RESULT_TIMEOUT_SEC", "240"))

QUEUE_OPTS = {
    "concurrency": 6,
    "worker_concurrency": 6,
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
        """,
        database=APP_DB,
    )


def reset_app_tables() -> None:
    psql(
        "TRUNCATE step_runs, job_state, processed_jobs RESTART IDENTITY;",
        database=APP_DB,
    )


def build_jobs(root_seed: int) -> tuple[list[dict[str, object]], int, str]:
    job_count = 12 + (root_seed % 9)
    crash_at_step = 1 if (root_seed % 2) == 0 else 2
    crash_mode = "mid_job" if crash_at_step == 1 else "after_complete"
    jobs = [
        {"job_id": f"race_job_{i:03d}", "crash_at_step": crash_at_step}
        for i in range(job_count)
    ]
    return jobs, job_count, crash_mode


def ready_path(worker: str) -> Path:
    return RUN_DIR / f"{worker}.ready"


def register_queue(dbos) -> None:
    dbos.register_queue(
        QUEUE_NAME,
        concurrency=QUEUE_OPTS["concurrency"],
        worker_concurrency=QUEUE_OPTS["worker_concurrency"],
        polling_interval_sec=QUEUE_OPTS["polling_interval_sec"],
    )


@dataclass
class RecovererRun:
    results: dict[str, subprocess.CompletedProcess[str]]
    ready: set[str]
    barrier_released: bool


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
        job_id = str(job["job_id"])
        crash_at = int(job["crash_at_step"])
        handle = DBOS.enqueue_workflow(QUEUE_NAME, JobWF.process_job, job_id, crash_at)
        workflow_ids.append(handle.workflow_id)
        progress("enqueued", f"job_id={job_id} workflow_id={handle.workflow_id}")

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


def wait_for_workflow_result(dbos, workflow_id: str, executor: str):
    from dbos._sys_db import WorkflowStatusString

    handle = dbos.retrieve_workflow(workflow_id)
    terminal = {
        WorkflowStatusString.SUCCESS.value,
        WorkflowStatusString.ERROR.value,
        WorkflowStatusString.CANCELLED.value,
        WorkflowStatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value,
    }
    deadline = time.monotonic() + WORKFLOW_RESULT_TIMEOUT_SEC
    last_status = "UNKNOWN"
    while time.monotonic() < deadline:
        status = handle.get_status().status
        last_status = status
        if status == WorkflowStatusString.SUCCESS.value:
            return handle.get_result()
        if status in terminal:
            raise RuntimeError(
                f"workflow {workflow_id} terminal status={status} recoverer={executor}"
            )
        time.sleep(0.25)

    invariant(
        "R0",
        "workflow_result_deadline",
        False,
        f"workflow_id={workflow_id} last_status={last_status} recoverer={executor}",
    )


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
    progress("recoverer_queue_registered", f"executor={executor}")

    from dbos_queue_wf import JobWF  # noqa: F401

    ready_path(executor).write_text("ready")
    progress("recoverer_ready", f"executor={executor}")
    deadline = time.monotonic() + RECOVERER_STARTUP_TIMEOUT_SEC
    while not GO_PATH.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    if not GO_PATH.exists():
        invariant(
            "R0b",
            "recoverer_barrier_released",
            False,
            f"executor={executor} waited_for={GO_PATH}",
        )

    progress("recover_worker_a", f"executor={executor}")
    DBOS._recover_pending_workflows([WORKER_A])
    progress("recover_worker_a_done", f"executor={executor}")

    for workflow_id in workflow_ids:
        wait_for_workflow_result(DBOS, workflow_id, executor)
        progress("recoverer_workflow_success", f"executor={executor} workflow_id={workflow_id}")

    DBOS.destroy(destroy_registry=True)


def sql_scalar(sql: str) -> str:
    return psql(sql, database=APP_DB).splitlines()[-1].strip()


def assert_sql_invariants(job_count: int) -> None:
    done_jobs = int(sql_scalar("SELECT COUNT(*) FROM processed_jobs;"))
    inflight = int(sql_scalar("SELECT COUNT(*) FROM job_state WHERE state = 'inflight';"))
    max_begin = int(
        sql_scalar(
            "SELECT COALESCE(MAX(c), 0) FROM ("
            "  SELECT COUNT(*) AS c FROM step_runs WHERE step_name = 'begin' "
            "  GROUP BY job_id"
            ") s;"
        )
    )
    max_complete = int(
        sql_scalar(
            "SELECT COALESCE(MAX(c), 0) FROM ("
            "  SELECT COUNT(*) AS c FROM step_runs WHERE step_name = 'complete' "
            "  GROUP BY job_id"
            ") s;"
        )
    )
    total_steps = int(sql_scalar("SELECT COUNT(*) FROM step_runs;"))

    invariant(
        "R1",
        "jobs_all_completed",
        done_jobs == job_count and inflight == 0,
        f"processed_jobs={done_jobs} expected={job_count} inflight={inflight}",
    )
    invariant(
        "R2",
        "no_duplicate_begin",
        max_begin <= 1,
        f"max_begin_attempts_per_job={max_begin}",
    )
    invariant(
        "R3",
        "no_duplicate_complete",
        max_complete <= 1,
        f"max_complete_attempts_per_job={max_complete}",
    )
    invariant(
        "R4",
        "step_rows_bounded",
        total_steps <= job_count * 2,
        f"total_step_rows={total_steps} expected_max={job_count * 2}",
    )


def assert_queue_invariants(workflow_ids: list[str], job_count: int) -> None:
    from dbos import DBOS
    from dbos._sys_db import WorkflowStatusString

    DBOS.destroy(destroy_registry=True)
    DBOS(config=dbos_config())
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()
    register_queue(DBOS)

    pending = DBOS.list_workflows(
        status=WorkflowStatusString.PENDING.value,
        queue_name=QUEUE_NAME,
    )
    enqueued = DBOS.list_workflows(
        status=WorkflowStatusString.ENQUEUED.value,
        queue_name=QUEUE_NAME,
    )
    invariant(
        "R5",
        "no_stuck_queue_rows",
        len(pending) == 0 and len(enqueued) == 0,
        f"pending={len(pending)} enqueued={len(enqueued)}",
    )

    for workflow_id in workflow_ids:
        status = DBOS.retrieve_workflow(workflow_id).get_status().status
        invariant(
            "R6",
            "workflow_terminal_success",
            status == WorkflowStatusString.SUCCESS.value,
            f"workflow_id={workflow_id} status={status}",
        )

    DBOS.destroy(destroy_registry=True)
    assert_sql_invariants(job_count)


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
        args=[__file__, "--phase", phase],
        returncode=rc,
        stdout="".join(captured),
        stderr="",
    )


def _run_recoverers(base_env: dict[str, str]) -> RecovererRun:
    procs: dict[str, subprocess.Popen[str]] = {}
    captured: dict[str, list[str]] = {WORKER_B: [], WORKER_C: []}
    ready_workers: set[str] = set()
    barrier_released = False
    for path in (ready_path(WORKER_B), ready_path(WORKER_C), GO_PATH):
        path.unlink(missing_ok=True)

    threads: list[threading.Thread] = []

    def start_recoverer(worker: str) -> None:
        progress("subphase_recoverer_start", worker)
        procs[worker] = subprocess.Popen(
            [sys.executable, __file__, "--phase", "recoverer"],
            env=_subphase_env({**base_env, "DBOS__VMID": worker}),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        thread = threading.Thread(target=drain, args=(worker,))
        thread.start()
        threads.append(thread)

    def drain(worker: str) -> None:
        proc = procs[worker]
        assert proc.stdout is not None
        for line in proc.stdout:
            captured[worker].append(line)
            print(f"[{worker}] {line}", end="", flush=True)

    def wait_ready(worker: str, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if ready_path(worker).exists():
                progress("recoverer_ready_seen", worker)
                return True
            proc = procs.get(worker)
            if proc is not None and proc.poll() is not None:
                progress("recoverer_exited_before_ready", f"{worker}:rc={proc.returncode}")
                return False
            time.sleep(0.1)
        progress("recoverer_ready_timeout", f"{worker} dumping_stack")
        proc = procs.get(worker)
        if proc is not None:
            try:
                proc.send_signal(signal.SIGUSR1)
            except ProcessLookupError:
                pass
        return False

    # Start both recoverers together; release GO when both are ready (parallel startup).
    for worker in (WORKER_B, WORKER_C):
        start_recoverer(worker)

    ready_workers: set[str] = set()
    deadline = time.monotonic() + RECOVERER_STARTUP_TIMEOUT_SEC
    while time.monotonic() < deadline:
        ready_workers = {
            worker for worker in (WORKER_B, WORKER_C) if ready_path(worker).exists()
        }
        if ready_workers == {WORKER_B, WORKER_C}:
            break
        for worker in (WORKER_B, WORKER_C):
            proc = procs.get(worker)
            if proc is not None and proc.poll() is not None:
                progress("recoverer_exited_before_ready", f"{worker}:rc={proc.returncode}")
                break
        else:
            time.sleep(0.1)
            continue
        break

    if ready_workers == {WORKER_B, WORKER_C}:
        progress("recoverer_barrier_release")
        GO_PATH.write_text("go")
        barrier_released = True
    else:
        progress(
            "recoverer_startup_gate_failed",
            f"ready={sorted(ready_workers)} expected={[WORKER_B, WORKER_C]}",
        )
        for proc in procs.values():
            if proc.poll() is None:
                try:
                    proc.send_signal(signal.SIGUSR1)
                except ProcessLookupError:
                    pass
                time.sleep(1)
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
        time.sleep(1)

    results: dict[str, subprocess.CompletedProcess[str]] = {}
    timed_out: set[str] = set()
    for worker, proc in procs.items():
        try:
            rc = proc.wait(timeout=RECOVERER_PRODUCT_TIMEOUT_SEC)
        except subprocess.TimeoutExpired:
            timed_out.add(worker)
            progress("subphase_recoverer_timeout", f"{worker} dumping_stack")
            try:
                proc.send_signal(signal.SIGUSR1)
                time.sleep(1)
            except ProcessLookupError:
                pass
            proc.kill()
            rc = proc.wait()
        progress("subphase_recoverer_done", f"{worker} rc={rc}")

    for thread in threads:
        thread.join(timeout=5)

    for worker, proc in procs.items():
        assert proc.returncode is not None
        results[worker] = subprocess.CompletedProcess(
            args=[__file__, "--phase", "recoverer"],
            returncode=proc.returncode,
            stdout="".join(captured[worker]),
            stderr="timeout" if worker in timed_out else "",
        )
    return RecovererRun(results=results, ready=ready_workers, barrier_released=barrier_released)


def scenario_dual_recovery(root_seed: int) -> None:
    jobs, job_count, crash_mode = build_jobs(root_seed)
    JOBS_PATH.write_text(json.dumps(jobs))
    META_PATH.write_text(
        json.dumps(
            {
                "job_count": job_count,
                "crash_mode": crash_mode,
                "workflow_ids": [],
            }
        )
    )

    progress("schema_init")
    init_app_schema()
    reset_app_tables()
    progress(
        "gate_timeouts",
        f"recoverer_startup={RECOVERER_STARTUP_TIMEOUT_SEC}s "
        f"recoverer_product={RECOVERER_PRODUCT_TIMEOUT_SEC}s "
        f"workflow_result={WORKFLOW_RESULT_TIMEOUT_SEC}s",
    )

    base_env = {"WORKLOAD_SEED": workload_seed_raw()}
    enqueue = _run_subphase("enqueue", base_env)
    invariant(
        "G1",
        "enqueue_phase_ready",
        enqueue.returncode == 0,
        f"rc={enqueue.returncode}",
    )

    meta = json.loads(META_PATH.read_text())
    workflow_ids = meta.get("workflow_ids") or []
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

    recoverer_run = _run_recoverers(base_env)
    recoverers = recoverer_run.results
    invariant(
        "G4",
        "recoverer_participants_ready",
        recoverer_run.ready == {WORKER_B, WORKER_C} and recoverer_run.barrier_released,
        f"ready={sorted(recoverer_run.ready)} barrier_released={recoverer_run.barrier_released}",
    )
    failures = {
        worker: result
        for worker, result in recoverers.items()
        if result.returncode != 0
    }
    if failures:
        try:
            assert_queue_invariants(workflow_ids, job_count)
        except Exception as invariant_error:
            progress("post_failure_invariant_error", str(invariant_error)[:500])
        detail = {
            worker: (result.stdout or "")[-1200:]
            for worker, result in failures.items()
        }
        invariant(
            "G5",
            "recoverer_processes_clean",
            False,
            f"recoverer_failures={detail}",
        )

    assert_queue_invariants(workflow_ids, job_count)
    progress(
        "scenario_done",
        f"jobs={job_count} crash_mode={crash_mode} recoverers={WORKER_B},{WORKER_C}",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--phase",
        choices=["enqueue", "worker_a", "recoverer"],
        default="",
    )
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
        meta = json.loads(META_PATH.read_text())
        workflow_ids = meta.get("workflow_ids") or []
        progress("dbos_recoverer_launch", os.environ.get("DBOS__VMID", "unknown"))
        run_phase_recoverer(workflow_ids)
        return 0

    return workload_main("dbos_dual_recovery_race_aggressive", scenario_dual_recovery)


if __name__ == "__main__":
    raise SystemExit(main())
