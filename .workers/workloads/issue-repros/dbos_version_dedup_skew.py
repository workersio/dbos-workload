#!/usr/bin/env python3
"""
P3: debouncer version / dedup skew (#702).

# SURFACE: debounce_async + application_version redeploy
# MODELS:  debounce key, destroy/relaunch with new DBOS__APPVERSION, debounce again
# ORACLE:  debounce_completes_within_budget; target_workflow_success
# ISSUES:  #702
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from pathlib import Path

from dbos_workload_common import (
    APP_DB,
    invariant,
    progress,
    psql,
    workload_main,
    workload_seed_raw,
)

RUN_DIR = Path(os.environ.get("DBOS_WORKLOAD_RUN_DIR", os.environ.get("TMPDIR", "/tmp"))) / (
    "dbos-workload-dbos_version_dedup_skew"
)
META_PATH = RUN_DIR / "meta.json"
BUDGET_SEC = float(os.environ.get("DEBOUNCE_SKEW_BUDGET_SEC", "25"))
SUBPHASE_TIMEOUT_SEC = int(os.environ.get("DEBOUNCE_SKEW_SUBPHASE_TIMEOUT_SEC", str(int(BUDGET_SEC + 30))))


def init_schema() -> None:
    psql(
        """
        CREATE TABLE IF NOT EXISTS debounce_completions(
          id SERIAL PRIMARY KEY,
          debounce_key TEXT NOT NULL,
          payload TEXT NOT NULL,
          deploy_version TEXT NOT NULL
        );
        """,
        database=APP_DB,
    )
    psql("TRUNCATE debounce_completions RESTART IDENTITY;", database=APP_DB)


def sql_scalar(sql: str, database: str = APP_DB) -> str:
    return psql(sql, database=database).splitlines()[-1].strip()


def debounce_with_timeout(debouncer, debounce_key: str, period_sec: float, *args) -> tuple[object | None, float]:
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(debouncer.debounce, debounce_key, period_sec, *args)
        try:
            handle = future.result(timeout=BUDGET_SEC)
            return handle, time.monotonic() - started
        except FuturesTimeout:
            progress("debounce_call_timeout", f"budget_sec={BUDGET_SEC}")
            return None, time.monotonic() - started


def run_deploy(deploy_version: str, phase: str, debounce_key: str, payload: str) -> float:
    from dbos import DBOS, Debouncer
    from dbos_workload_common import dbos_config

    os.environ["DBOS__APPVERSION"] = deploy_version
    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    if phase == "deploy_a":
        DBOS.listen_queues(["__formal_no_dequeue__"])
    DBOS.launch()

    from dbos_version_dedup_wf import debounced_target

    debouncer = Debouncer.create(debounced_target, debounce_timeout_sec=8.0)
    period_sec = 3.0 if phase == "deploy_a" else 0.2
    handle, debounce_elapsed = debounce_with_timeout(
        debouncer,
        debounce_key,
        period_sec,
        debounce_key,
        payload,
    )

    if phase == "deploy_a":
        time.sleep(0.1)
        DBOS.destroy(destroy_registry=True)
        progress(
            phase,
            f"deploy={deploy_version} debounce_elapsed_sec={debounce_elapsed:.2f}",
        )
        return debounce_elapsed

    if handle is None:
        progress(phase, f"deploy={deploy_version} debounce_spin_detected")
        return BUDGET_SEC + 1.0

    deadline = time.monotonic() + BUDGET_SEC
    while time.monotonic() < deadline:
        status = handle.get_status().status
        if status in ("SUCCESS", "ERROR"):
            break
        time.sleep(0.1)
    else:
        progress("deploy_b_wait_timeout", f"budget_sec={BUDGET_SEC}")
    try:
        handle.get_result()
    except Exception as exc:
        progress("deploy_b_result_error", str(exc)[:300])

    DBOS.destroy(destroy_registry=True)
    progress(
        phase,
        f"deploy={deploy_version} debounce_elapsed_sec={debounce_elapsed:.2f}",
    )
    return debounce_elapsed


def _deploy_worker(phase: str, deploy_version: str, payload: str) -> None:
    workloads_dir = Path(__file__).resolve().parent
    vendor_py = workloads_dir.parents[2] / ".workers" / "vendor" / "py"
    os.environ["DBOS_WORKLOAD_RUN_DIR"] = str(RUN_DIR.parent)
    os.environ["PYTHONPATH"] = os.pathsep.join([str(workloads_dir), str(vendor_py)])

    meta = json.loads(META_PATH.read_text())
    debounce_key = meta["debounce_key"]
    elapsed = run_deploy(deploy_version, phase, debounce_key, payload)
    if phase == "deploy_b":
        meta["debounce_elapsed_b"] = elapsed
        META_PATH.write_text(json.dumps(meta))


def _run_deploy_process(phase: str, deploy_version: str, payload: str) -> float:
    progress(f"subphase_{phase}_start")
    proc = mp.Process(
        target=_deploy_worker,
        args=(phase, deploy_version, payload),
        daemon=True,
    )
    proc.start()
    proc.join(timeout=SUBPHASE_TIMEOUT_SEC)
    if proc.is_alive():
        proc.terminate()
        proc.join(timeout=5)
        progress(f"subphase_{phase}_timeout", f"timeout_sec={SUBPHASE_TIMEOUT_SEC}")
        return BUDGET_SEC + 1.0 if phase == "deploy_b" else 0.0
    progress(f"subphase_{phase}_done", f"exitcode={proc.exitcode}")
    if phase == "deploy_b" and META_PATH.exists():
        meta = json.loads(META_PATH.read_text())
        return float(meta.get("debounce_elapsed_b") or 0.0)
    return 0.0


def scenario_version_dedup_skew(root_seed: int) -> None:
    debounce_key = f"skew-{workload_seed_raw()[:12]}"
    payload_v1 = f"v1-{root_seed}"
    payload_v2 = f"v2-{root_seed}"

    META_PATH.write_text(
        json.dumps(
            {
                "debounce_key": debounce_key,
                "payload_v1": payload_v1,
                "payload_v2": payload_v2,
                "debounce_elapsed_b": 0.0,
            }
        )
    )

    init_schema()
    _run_deploy_process("deploy_a", "deploy-a-v1", payload_v1)
    debounce_elapsed_b = _run_deploy_process("deploy_b", "deploy-b-v2", payload_v2)

    completions = int(sql_scalar("SELECT COUNT(*) FROM debounce_completions;"))
    invariant(
        "P3",
        "debounce_completes_within_budget",
        debounce_elapsed_b <= BUDGET_SEC,
        f"debounce_return_sec={debounce_elapsed_b:.2f} budget={BUDGET_SEC}",
    )
    invariant(
        "P3b",
        "target_workflow_success",
        completions >= 1,
        f"debounce_completions={completions}",
    )
    progress("scenario_done", f"key={debounce_key} completions={completions}")


def main() -> int:
    return workload_main("dbos_version_dedup_skew", scenario_version_dedup_skew)


if __name__ == "__main__":
    raise SystemExit(main())
