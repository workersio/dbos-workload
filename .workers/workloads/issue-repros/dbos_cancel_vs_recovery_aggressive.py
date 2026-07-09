#!/usr/bin/env python3
"""
P8a: aggressive cancellation vs recovery — cancel nearly all jobs before/during recovery.

# SURFACE: Cancel semantics x recovery ownership x workflow state machine
# MODELS:  worker-a crashes mid-jobs; worker-b recovers while admin cancels all-but-one jobs
# ORACLE:  D1 cancel ack coherent, D2 cancelled stays cancelled (no SUCCESS
#          overwrite, no resurrect to PENDING/ENQUEUED), D3 bounded post-cancel
#          side effects, D4 untargeted jobs complete exactly once, D5 no stuck
#          rows, D6 bounded recovery attempts
# ISSUES:  update_workflow_outcome writes SUCCESS/ERROR with no status guard
#          (_sys_db.py); recovery executes CANCELLED rows (should_execute stays
#          True for recovery requests); cancel checked only at step boundaries
# VARIANCE: WORKLOAD_SEED -> job count, crash step, cancel nearly-all target set,
#          negative cancel timing offset (cancel before recover)
"""

from __future__ import annotations

import argparse
import faulthandler
import json
import os
import random
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
    progress,
    psql,
    subphase_env,
    workload_main,
    workload_seed_raw,
)

QUEUE_NAME = "formal_cancel_recovery_aggr"
WORKER_A = "worker-a"
WORKER_B = "worker-b"
CANCELLER = "canceller"
NO_DEQUEUE = "__formal_no_dequeue__"

META_PATH = RUN_DIR / "meta.json"
JOBS_PATH = RUN_DIR / "jobs.json"
GO_RECOVER_PATH = RUN_DIR / "recover.go"
GO_CANCEL_PATH = RUN_DIR / "cancel.go"
ACKS_PATH = RUN_DIR / "cancel_acks.json"
RECOVER_STARTED_PATH = RUN_DIR / "recover.started"

RECOVERER_STARTUP_TIMEOUT_SEC = int(os.environ.get("RECOVERER_STARTUP_TIMEOUT_SEC", "1200"))
RECOVERER_PRODUCT_TIMEOUT_SEC = int(os.environ.get("RECOVERER_PRODUCT_TIMEOUT_SEC", "420"))
WORKFLOW_RESULT_TIMEOUT_SEC = int(os.environ.get("WORKFLOW_RESULT_TIMEOUT_SEC", "240"))

# Offset of GO_CANCEL relative to GO_RECOVER. Negative = cancel released first.
CANCEL_OFFSET_CHOICES_MS = [-200, -100, -50, 0, 50]

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

TERMINAL_OK = {"SUCCESS", "ERROR", "CANCELLED", "MAX_RECOVERY_ATTEMPTS_EXCEEDED"}


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
    rng = random.Random(root_seed)
    job_count = 10 + (root_seed % 6)
    crash_at_step = 1 if (root_seed % 2) == 0 else 2
    target_count = max(1, job_count - 1)
    target_indexes = sorted(rng.sample(range(job_count), target_count))
    cancel_offset_ms = CANCEL_OFFSET_CHOICES_MS[(root_seed >> 5) % len(CANCEL_OFFSET_CHOICES_MS)]
    return {
        "jobs": [
            {"job_id": f"cvr_job_{i:03d}", "crash_at_step": crash_at_step}
            for i in range(job_count)
        ],
        "job_count": job_count,
        "crash_at_step": crash_at_step,
        "target_indexes": target_indexes,
        "cancel_offset_ms": cancel_offset_ms,
    }


def register_queue(dbos) -> None:
    dbos.register_queue(
        QUEUE_NAME,
        concurrency=QUEUE_OPTS["concurrency"],
        worker_concurrency=QUEUE_OPTS["worker_concurrency"],
        polling_interval_sec=QUEUE_OPTS["polling_interval_sec"],
    )


def job_effect_snapshot(job_id: str) -> dict[str, int]:
    steps = int(
        psql(
            f"SELECT COUNT(*) FROM step_runs WHERE job_id = '{job_id}';", database=APP_DB
        )
    )
    processed = int(
        psql(
            f"SELECT COUNT(*) FROM processed_jobs WHERE job_id = '{job_id}';",
            database=APP_DB,
        )
    )
    return {"step_rows": steps, "processed": processed}


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
    while not GO_RECOVER_PATH.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    if not GO_RECOVER_PATH.exists():
        invariant("R0b", "recoverer_barrier_released", False, f"executor={executor}")

    RECOVER_STARTED_PATH.write_text("started")
    progress("recover_worker_a", f"executor={executor}")
    DBOS._recover_pending_workflows([WORKER_A])
    progress("recover_worker_a_done", f"executor={executor}")

    # Cancelled workflows are legitimate terminals here; the parent judges which
    # terminal each workflow was allowed to reach. The recoverer only proves
    # quiescence: every workflow reaches some terminal state.
    not_terminal: list[tuple[str, str]] = []
    deadline = time.monotonic() + WORKFLOW_RESULT_TIMEOUT_SEC
    remaining = dict.fromkeys(workflow_ids, "UNKNOWN")
    while remaining and time.monotonic() < deadline:
        for workflow_id in list(remaining):
            status = DBOS.retrieve_workflow(workflow_id).get_status().status
            remaining[workflow_id] = status
            if status in TERMINAL_OK:
                progress("workflow_terminal", f"workflow_id={workflow_id} status={status}")
                del remaining[workflow_id]
        if remaining:
            time.sleep(0.25)
    not_terminal = list(remaining.items())

    DBOS.destroy(destroy_registry=True)
    if not_terminal:
        print(f"workflows never reached terminal state: {not_terminal}", file=sys.stderr)
        sys.exit(3)


def run_phase_cancel(workflow_ids: list[str], jobs: list[dict[str, object]], target_indexes: list[int]) -> None:
    from dbos import DBOS

    progress("canceller_init_start")
    DBOS.destroy(destroy_registry=True)
    DBOS(config=dbos_config())
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()
    register_queue(DBOS)

    from dbos_queue_wf import JobWF  # noqa: F401

    (RUN_DIR / f"{CANCELLER}.ready").write_text("ready")
    progress("canceller_ready")
    deadline = time.monotonic() + RECOVERER_STARTUP_TIMEOUT_SEC
    while not GO_CANCEL_PATH.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    if not GO_CANCEL_PATH.exists():
        invariant("R0c", "canceller_barrier_released", False, "cancel.go never appeared")

    acks: dict[str, dict[str, object]] = {}
    for index in target_indexes:
        workflow_id = workflow_ids[index]
        job_id = str(jobs[index]["job_id"])
        DBOS.cancel_workflow(workflow_id)
        ack_status = DBOS.retrieve_workflow(workflow_id).get_status().status
        snapshot = job_effect_snapshot(job_id)
        acks[workflow_id] = {"job_id": job_id, "ack_status": ack_status, **snapshot}
        progress(
            "cancel_acked",
            f"workflow_id={workflow_id} job_id={job_id} ack_status={ack_status} "
            f"step_rows={snapshot['step_rows']} processed={snapshot['processed']}",
        )

    ACKS_PATH.write_text(json.dumps(acks))
    DBOS.destroy(destroy_registry=True)


def sql_scalar(sql: str, database: str = APP_DB) -> str:
    return psql(sql, database=database).splitlines()[-1].strip()


def assert_final_invariants(
    workflow_ids: list[str],
    jobs: list[dict[str, object]],
    target_indexes: list[int],
) -> None:
    from dbos import DBOS
    from dbos._sys_db import WorkflowStatusString

    acks = json.loads(ACKS_PATH.read_text())
    target_ids = {workflow_ids[i] for i in target_indexes}

    DBOS.destroy(destroy_registry=True)
    DBOS(config=dbos_config())
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()
    register_queue(DBOS)

    final_status = {
        workflow_id: DBOS.retrieve_workflow(workflow_id).get_status().status
        for workflow_id in workflow_ids
    }

    bad_acks = {
        wfid: ack["ack_status"]
        for wfid, ack in acks.items()
        if ack["ack_status"] not in ("CANCELLED", "SUCCESS")
    }
    invariant(
        "D1",
        "cancel_ack_coherent",
        not bad_acks,
        f"acks_not_in_cancelled_or_success={bad_acks}" if bad_acks else f"acks={len(acks)}",
    )

    flipped = {
        wfid: final_status[wfid]
        for wfid, ack in acks.items()
        if ack["ack_status"] == "CANCELLED" and final_status[wfid] != "CANCELLED"
    }
    invariant(
        "D2",
        "cancelled_stays_cancelled",
        not flipped,
        f"acked_cancelled_but_final={flipped}" if flipped else "no post-cancel status flips",
    )

    # Cancellation preempts at the next step boundary, so a single in-flight
    # step may still commit after the ack; more growth means resurrected work.
    overgrown: dict[str, str] = {}
    for wfid, ack in acks.items():
        if ack["ack_status"] != "CANCELLED":
            continue
        now = job_effect_snapshot(str(ack["job_id"]))
        if now["step_rows"] > int(ack["step_rows"]) + 1:
            overgrown[wfid] = (
                f"job_id={ack['job_id']} step_rows {ack['step_rows']} -> {now['step_rows']}"
            )
    invariant(
        "D3",
        "post_cancel_side_effects_bounded",
        not overgrown,
        f"resurrected_work={overgrown}" if overgrown else "step growth <= 1 per cancelled job",
    )

    untargeted_bad = {
        wfid: status
        for wfid, status in final_status.items()
        if wfid not in target_ids and status != "SUCCESS"
    }
    untargeted_jobs = [
        str(jobs[i]["job_id"]) for i in range(len(jobs)) if i not in set(target_indexes)
    ]
    missing_jobs = [
        job_id
        for job_id in untargeted_jobs
        if int(sql_scalar(f"SELECT COUNT(*) FROM processed_jobs WHERE job_id = '{job_id}';")) != 1
    ]
    invariant(
        "D4",
        "untargeted_jobs_complete",
        not untargeted_bad and not missing_jobs,
        f"non_success={untargeted_bad} missing_processed={missing_jobs}",
    )

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
        "D5",
        "steps_exactly_once",
        max_begin <= 1 and max_complete <= 1,
        f"max_begin={max_begin} max_complete={max_complete}",
    )

    pending = DBOS.list_workflows(status=WorkflowStatusString.PENDING.value)
    enqueued = DBOS.list_workflows(status=WorkflowStatusString.ENQUEUED.value)
    invariant(
        "D6",
        "no_stuck_queue_rows",
        len(pending) == 0 and len(enqueued) == 0,
        f"pending={len(pending)} enqueued={len(enqueued)}",
    )

    max_attempts = int(
        sql_scalar(
            "SELECT COALESCE(MAX(recovery_attempts), 0) FROM dbos.workflow_status;",
            database=SYS_DB,
        )
    )
    invariant(
        "D7",
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


def _start_participant(phase: str, label: str, env: dict[str, str]) -> tuple[subprocess.Popen[str], list[str], threading.Thread]:
    progress(f"subphase_{label}_start")
    proc = subprocess.Popen(
        [sys.executable, __file__, "--phase", phase],
        env=_subphase_env(env),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    captured: list[str] = []

    def drain() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            captured.append(line)
            print(f"[{label}] {line}", end="", flush=True)

    thread = threading.Thread(target=drain)
    thread.start()
    return proc, captured, thread


def _wait_ready(label: str, proc: subprocess.Popen[str]) -> bool:
    deadline = time.monotonic() + RECOVERER_STARTUP_TIMEOUT_SEC
    ready = RUN_DIR / f"{label}.ready"
    while time.monotonic() < deadline:
        if ready.exists():
            progress("participant_ready_seen", label)
            return True
        if proc.poll() is not None:
            progress("participant_exited_before_ready", f"{label}:rc={proc.returncode}")
            return False
        time.sleep(0.1)
    progress("participant_ready_timeout", f"{label} dumping_stack")
    try:
        proc.send_signal(signal.SIGUSR1)
    except ProcessLookupError:
        pass
    return False


def scenario_cancel_vs_recovery(root_seed: int) -> None:
    plan = build_plan(root_seed)
    job_count = int(plan["job_count"])
    jobs = list(plan["jobs"])
    target_indexes = list(plan["target_indexes"])
    cancel_offset_ms = int(plan["cancel_offset_ms"])
    JOBS_PATH.write_text(json.dumps(jobs))
    META_PATH.write_text(json.dumps({"workflow_ids": [], "target_indexes": target_indexes}))

    progress("schema_init")
    init_app_schema()
    progress(
        "plan",
        f"jobs={job_count} crash_at_step={plan['crash_at_step']} "
        f"targets={target_indexes} cancel_offset_ms={cancel_offset_ms}",
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

    for path in (
        RUN_DIR / f"{WORKER_B}.ready",
        RUN_DIR / f"{CANCELLER}.ready",
        GO_RECOVER_PATH,
        GO_CANCEL_PATH,
        ACKS_PATH,
        RECOVER_STARTED_PATH,
    ):
        path.unlink(missing_ok=True)

    # Same sequential-start pattern as P5: imports contend in the guest, so each
    # participant reaches ready before the next starts; only the barriers race.
    recoverer, rec_captured, rec_thread = _start_participant(
        "recoverer", WORKER_B, {**base_env, "DBOS__VMID": WORKER_B}
    )
    canceller = None
    can_captured: list[str] = []
    can_thread = None
    recoverer_ready = _wait_ready(WORKER_B, recoverer)
    canceller_ready = False
    if recoverer_ready:
        canceller, can_captured, can_thread = _start_participant(
            "cancel", CANCELLER, {**base_env, "DBOS__VMID": CANCELLER}
        )
        canceller_ready = _wait_ready(CANCELLER, canceller)
    if not (recoverer_ready and canceller_ready):
        for proc in [p for p in (recoverer, canceller) if p is not None]:
            if proc.poll() is None:
                try:
                    proc.send_signal(signal.SIGUSR1)
                    time.sleep(1)
                except ProcessLookupError:
                    pass
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
    invariant(
        "G4",
        "participants_ready",
        recoverer_ready and canceller_ready,
        f"recoverer_ready={recoverer_ready} canceller_ready={canceller_ready}",
    )
    assert canceller is not None and can_thread is not None

    if cancel_offset_ms < 0:
        GO_CANCEL_PATH.write_text("go")
        time.sleep(-cancel_offset_ms / 1000.0)
        GO_RECOVER_PATH.write_text("go")
    else:
        GO_RECOVER_PATH.write_text("go")
        if cancel_offset_ms:
            time.sleep(cancel_offset_ms / 1000.0)
        GO_CANCEL_PATH.write_text("go")
    progress("fault_barriers_released", f"cancel_offset_ms={cancel_offset_ms}")

    # One shared product-timeout window bounds both participants, so the outer
    # VM timeout cannot fire before the slower participant is classified.
    wait_deadline = time.monotonic() + RECOVERER_PRODUCT_TIMEOUT_SEC
    results: dict[str, int] = {}
    for label, proc, thread in ((CANCELLER, canceller, can_thread), (WORKER_B, recoverer, rec_thread)):
        try:
            rc = proc.wait(timeout=max(1.0, wait_deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            progress("participant_product_timeout", f"{label} dumping_stack")
            try:
                proc.send_signal(signal.SIGUSR1)
                time.sleep(1)
            except ProcessLookupError:
                pass
            proc.kill()
            rc = proc.wait()
        thread.join(timeout=5)
        results[label] = rc
        progress(f"subphase_{label}_done", f"rc={rc}")

    invariant(
        "G5",
        "canceller_process_clean",
        results[CANCELLER] == 0 and ACKS_PATH.exists(),
        f"rc={results[CANCELLER]} acks_written={ACKS_PATH.exists()} "
        f"tail={''.join(can_captured)[-800:] if results[CANCELLER] != 0 else ''}",
    )
    acks = json.loads(ACKS_PATH.read_text())
    invariant(
        "F1",
        "cancel_window_proven",
        len(acks) == len(target_indexes),
        f"acks={len(acks)} expected={len(target_indexes)}",
    )

    if results[WORKER_B] != 0:
        try:
            assert_final_invariants(workflow_ids, jobs, target_indexes)
        except Exception as invariant_error:
            progress("post_failure_invariant_error", str(invariant_error)[:500])
        invariant(
            "G6",
            "recoverer_process_clean",
            False,
            f"rc={results[WORKER_B]} tail={''.join(rec_captured)[-1200:]}",
        )

    assert_final_invariants(workflow_ids, jobs, target_indexes)
    progress(
        "scenario_done",
        f"jobs={job_count} targets={len(target_indexes)} cancel_offset_ms={cancel_offset_ms}",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--phase", choices=["enqueue", "worker_a", "recoverer", "cancel"], default=""
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
        workflow_ids = json.loads(META_PATH.read_text()).get("workflow_ids") or []
        progress("dbos_recoverer_launch", os.environ.get("DBOS__VMID", "unknown"))
        run_phase_recoverer(workflow_ids)
        return 0

    if args.phase == "cancel":
        if not META_PATH.exists() or not JOBS_PATH.exists():
            return 2
        meta = json.loads(META_PATH.read_text())
        workflow_ids = meta.get("workflow_ids") or []
        target_indexes = meta.get("target_indexes") or []
        jobs = json.loads(JOBS_PATH.read_text())
        progress("dbos_canceller_launch")
        run_phase_cancel(workflow_ids, jobs, target_indexes)
        return 0

    return workload_main("dbos_cancel_vs_recovery_aggressive", scenario_cancel_vs_recovery)


if __name__ == "__main__":
    raise SystemExit(main())
