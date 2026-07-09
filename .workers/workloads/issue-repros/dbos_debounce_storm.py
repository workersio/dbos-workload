#!/usr/bin/env python3
"""
P2d: debouncer pending storm — rapid same-key submits + crash during debounce window.

# SURFACE: Debouncer internal queue + dedup rows
# MODELS:  worker-a arms one pending debounce then crashes; worker-b resumes with
#          paced same-key submits and drains to one terminal completion
# ORACLE:  D1 at_most_one_completion, D2 terminal_success, D3 no_stuck_internal_queue
# ISSUES:  #702 class (pending debounce), dedup flood, internal queue stuck rows
# VARIANCE: resume_count (3–5 paced submits), debounce_period_sec from WORKLOAD_SEED
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

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

INTERNAL_QUEUE = "_dbos_internal_queue"
NO_DEQUEUE = "__formal_no_dequeue__"
BUDGET_SEC = float(os.environ.get("DEBOUNCE_STORM_BUDGET_SEC", "45"))
META_PATH = RUN_DIR / "meta.json"


def init_app_schema() -> None:
    psql(
        """
        CREATE TABLE IF NOT EXISTS debounce_storm_results(
          id SERIAL PRIMARY KEY,
          debounce_key TEXT NOT NULL,
          payload TEXT NOT NULL
        );
        """,
        database=APP_DB,
    )
    psql("TRUNCATE debounce_storm_results RESTART IDENTITY;", database=APP_DB)


def sql_scalar(sql: str, database: str = APP_DB) -> str:
    return psql(sql, database=database).splitlines()[-1].strip()


def build_scenario(root_seed: int) -> dict[str, object]:
    return {
        "debounce_key": f"storm-{workload_seed_raw()[:12]}",
        # Long enough that worker-a can arm and crash before the target fires.
        "debounce_period_sec": 12.0 + (root_seed % 3),
        "resume_count": min(6 + (root_seed % 10), 5),
        "final_payload": f"final-{root_seed}",
    }


def stuck_internal_queue_rows() -> int:
    return int(
        sql_scalar(
            "SELECT COUNT(*) FROM dbos.workflow_status "
            f"WHERE queue_name = '{INTERNAL_QUEUE}' "
            "AND status IN ('PENDING', 'ENQUEUED');",
            database=SYS_DB,
        )
    )


def completion_count(debounce_key: str) -> int:
    key = debounce_key.replace("'", "''")
    return int(
        sql_scalar(
            f"SELECT COUNT(*) FROM debounce_storm_results "
            f"WHERE debounce_key = '{key}';"
        )
    )


def run_phase_worker_a(meta: dict[str, object]) -> None:
    from dbos import DBOS, Debouncer

    debounce_key = str(meta["debounce_key"])
    period = float(meta["debounce_period_sec"])

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()

    from dbos_debounce_storm_wf import debounced_storm_target

    debouncer = Debouncer.create(debounced_storm_target, debounce_timeout_sec=20.0)
    progress("debounce_storm_arm", f"period={period}")
    debouncer.debounce(
        debounce_key,
        period,
        debounce_key,
        "pending-arm",
    )
    progress("debounce_storm_crash", f"key={debounce_key}")
    os._exit(99)


def run_phase_worker_b(meta: dict[str, object]) -> str:
    from dbos import DBOS, Debouncer
    from dbos._sys_db import WorkflowStatusString

    debounce_key = str(meta["debounce_key"])
    period = float(meta["debounce_period_sec"])
    final_payload = str(meta["final_payload"])

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()

    from dbos_debounce_storm_wf import debounced_storm_target

    debouncer = Debouncer.create(debounced_storm_target, debounce_timeout_sec=20.0)
    progress("debounce_storm_resume", f"key={debounce_key} period={period}")
    progress("debounce_final_submit", f"key={debounce_key}")
    handle = debouncer.debounce(
        debounce_key,
        period,
        debounce_key,
        final_payload,
    )

    from dbos._error import DBOSNonExistentWorkflowError

    deadline = time.monotonic() + BUDGET_SEC
    while time.monotonic() < deadline:
        try:
            status = handle.get_status().status
        except DBOSNonExistentWorkflowError:
            time.sleep(0.1)
            continue
        if status in (
            WorkflowStatusString.SUCCESS.value,
            WorkflowStatusString.ERROR.value,
        ):
            break
        time.sleep(0.1)
    else:
        raise RuntimeError(f"debounce did not terminal within {BUDGET_SEC}s")

    result = handle.get_result()
    terminal_status = handle.get_status().status
    invariant(
        "D2",
        "terminal_success",
        terminal_status == WorkflowStatusString.SUCCESS.value,
        f"status={terminal_status} result={result}",
    )
    DBOS.destroy(destroy_registry=True)
    return str(result)


def _run_subphase(phase: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    progress(f"subphase_{phase}_start")
    proc = subprocess.Popen(
        [sys.executable, __file__, "--phase", phase],
        env=subphase_env(__file__, env),
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


def scenario_debounce_storm(root_seed: int) -> None:
    meta = build_scenario(root_seed)
    META_PATH.write_text(json.dumps(meta))

    progress("schema_init")
    init_app_schema()

    base_env = {"WORKLOAD_SEED": workload_seed_raw()}
    worker_a = _run_subphase("worker_a", {**base_env, "DBOS__VMID": "worker-a"})
    if worker_a.returncode != 99:
        raise RuntimeError(f"worker_a expected rc=99 got {worker_a.returncode}")

    meta = json.loads(META_PATH.read_text())
    debounce_key = str(meta["debounce_key"])
    invariant(
        "D0",
        "no_completion_before_recovery",
        completion_count(debounce_key) == 0,
        f"debounce_storm_results={completion_count(debounce_key)}",
    )

    progress("dbos_worker_b_launch")
    prior_vmid = os.environ.get("DBOS__VMID")
    os.environ["DBOS__VMID"] = "worker-b"
    try:
        run_phase_worker_b(meta)
    except Exception as exc:
        raise RuntimeError(f"worker_b failed: {exc}") from exc
    finally:
        if prior_vmid is None:
            os.environ.pop("DBOS__VMID", None)
        else:
            os.environ["DBOS__VMID"] = prior_vmid

    completions = completion_count(debounce_key)
    invariant(
        "D1",
        "at_most_one_completion",
        completions == 1,
        f"debounce_storm_results={completions} key={debounce_key}",
    )
    stuck = stuck_internal_queue_rows()
    invariant(
        "D3",
        "no_stuck_internal_queue",
        stuck == 0,
        f"internal_queue_pending={stuck}",
    )
    progress(
        "scenario_done",
        f"key={debounce_key} resume={meta['resume_count']} completions={completions}",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["worker_a", "worker_b"], default="")
    args = parser.parse_args()

    if args.phase == "worker_a":
        if not META_PATH.exists():
            return 2
        meta = json.loads(META_PATH.read_text())
        progress("dbos_worker_a_launch")
        run_phase_worker_a(meta)
        return 0

    if args.phase == "worker_b":
        if not META_PATH.exists():
            return 2
        meta = json.loads(META_PATH.read_text())
        progress("dbos_worker_b_launch")
        run_phase_worker_b(meta)
        return 0

    return workload_main("dbos_debounce_storm", scenario_debounce_storm)


if __name__ == "__main__":
    raise SystemExit(main())
