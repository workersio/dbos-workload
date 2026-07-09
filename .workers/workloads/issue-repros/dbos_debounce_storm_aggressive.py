#!/usr/bin/env python3
"""
P2da: aggressive debouncer pending storm — shake out duplicate-completion races.

# SURFACE: Debouncer internal queue + dedup rows under flood + crash + recovery gap
# MODELS:  worker-a rapid same-key storm then crash; worker-b subprocess recovery
#          with concurrent debounce submits and a final drain debounce
# ORACLE:  D0 no_completion_before_recovery, D1 at_most_one_completion,
#          D1b final_payload_only_completion, D2 terminal_success,
#          D3 no_stuck_internal_queue
# ISSUES:  #702 class (pending debounce), dedup flood, ack-race duplicate debouncers
# VARIANCE: storm_count (10–20), debounce_period_sec (6–8), concurrent submits on worker-b
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
BUDGET_SEC = float(os.environ.get("DEBOUNCE_STORM_AGGR_BUDGET_SEC", "60"))
META_PATH = RUN_DIR / "meta.json"


def init_app_schema() -> None:
    psql(
        """
        CREATE TABLE IF NOT EXISTS debounce_storm_results(
          id SERIAL PRIMARY KEY,
          debounce_key TEXT NOT NULL,
          payload TEXT NOT NULL,
          completed_at_epoch_sec DOUBLE PRECISION,
          source_workflow_id TEXT
        );
        """,
        database=APP_DB,
    )
    psql(
        "ALTER TABLE debounce_storm_results "
        "ADD COLUMN IF NOT EXISTS completed_at_epoch_sec DOUBLE PRECISION;",
        database=APP_DB,
    )
    psql(
        "ALTER TABLE debounce_storm_results "
        "ADD COLUMN IF NOT EXISTS source_workflow_id TEXT;",
        database=APP_DB,
    )
    psql("TRUNCATE debounce_storm_results RESTART IDENTITY;", database=APP_DB)


def sql_scalar(sql: str, database: str = APP_DB) -> str:
    return psql(sql, database=database).splitlines()[-1].strip()


def build_scenario(root_seed: int) -> dict[str, object]:
    storm_count = int(os.environ.get("DEBOUNCE_STORM_AGGR_COUNT", 10 + (root_seed % 11)))
    period = float(
        os.environ.get("DEBOUNCE_STORM_AGGR_PERIOD_SEC", 6.0 + (root_seed % 3))
    )
    return {
        "debounce_key": f"aggr-{workload_seed_raw()[:12]}",
        "storm_count": storm_count,
        "debounce_period_sec": period,
        "concurrent_submitters": 3 + (root_seed % 3),
        "final_payload": f"final-{root_seed}",
        "post_storm_crash_delay_sec": float(
            os.environ.get("DEBOUNCE_STORM_AGGR_POST_STORM_CRASH_DELAY_SEC", "0")
        ),
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


def completion_payloads(debounce_key: str) -> list[str]:
    key = debounce_key.replace("'", "''")
    rows = psql(
        f"SELECT payload FROM debounce_storm_results "
        f"WHERE debounce_key = '{key}' ORDER BY id;",
        database=APP_DB,
    ).splitlines()
    return [line.strip() for line in rows if line.strip()]


def completion_rows(debounce_key: str) -> list[tuple[str, float, str]]:
    key = debounce_key.replace("'", "''")
    rows = psql(
        "SELECT payload, COALESCE(completed_at_epoch_sec, 0), COALESCE(source_workflow_id, '') "
        "FROM debounce_storm_results "
        f"WHERE debounce_key = '{key}' ORDER BY id;",
        database=APP_DB,
    ).splitlines()
    parsed: list[tuple[str, float, str]] = []
    for row in rows:
        if not row.strip():
            continue
        payload, completed_at, source_workflow_id = row.split("|", 2)
        parsed.append((payload, float(completed_at), source_workflow_id))
    return parsed


def _storm_debouncer(
    debouncer,
    debounce_key: str,
    period: float,
    payload: str,
    label: str,
) -> None:
    debouncer.debounce(debounce_key, period, debounce_key, payload)
    progress("debounce_submit", f"{label}")


def run_phase_worker_a(meta: dict[str, object]) -> None:
    from dbos import DBOS, Debouncer

    debounce_key = str(meta["debounce_key"])
    storm_count = int(meta["storm_count"])
    period = float(meta["debounce_period_sec"])

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()

    from dbos_debounce_storm_wf import debounced_storm_target

    debouncer = Debouncer.create(debounced_storm_target, debounce_timeout_sec=15.0)
    progress("debounce_aggr_arm", f"count={storm_count} period={period}")
    first_submit_at: float | None = None
    last_submit_at: float | None = None
    for index in range(storm_count):
        if first_submit_at is None:
            first_submit_at = time.time()
        _storm_debouncer(
            debouncer,
            debounce_key,
            period,
            f"a-payload-{index}",
            f"worker_a index={index}",
        )
        last_submit_at = time.time()

    meta["worker_a_first_submit_at_epoch_sec"] = first_submit_at or time.time()
    meta["worker_a_last_submit_at_epoch_sec"] = last_submit_at or time.time()
    meta["worker_a_due_at_epoch_sec"] = min(
        float(meta["worker_a_last_submit_at_epoch_sec"]) + period,
        float(meta["worker_a_first_submit_at_epoch_sec"]) + 15.0,
    )
    META_PATH.write_text(json.dumps(meta))

    post_storm_crash_delay_sec = float(meta.get("post_storm_crash_delay_sec") or 0)
    if post_storm_crash_delay_sec > 0:
        progress("debounce_aggr_pre_crash_delay", f"sec={post_storm_crash_delay_sec}")
        time.sleep(post_storm_crash_delay_sec)

    meta["worker_a_crash_at_epoch_sec"] = time.time()
    META_PATH.write_text(json.dumps(meta))
    progress("debounce_aggr_crash", f"key={debounce_key}")
    os._exit(99)


def run_phase_worker_b(meta: dict[str, object]) -> str:
    from dbos import DBOS, Debouncer
    from dbos._error import DBOSNonExistentWorkflowError
    from dbos._sys_db import WorkflowStatusString

    debounce_key = str(meta["debounce_key"])
    storm_count = int(meta["storm_count"])
    period = float(meta["debounce_period_sec"])
    concurrent_submitters = int(meta["concurrent_submitters"])
    final_payload = str(meta["final_payload"])

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()

    from dbos_debounce_storm_wf import debounced_storm_target

    progress("recover_worker_a")
    DBOS._recover_pending_workflows(["worker-a"])

    debouncer = Debouncer.create(debounced_storm_target, debounce_timeout_sec=15.0)
    progress(
        "debounce_aggr_resume",
        f"count={storm_count} period={period} concurrent={concurrent_submitters}",
    )

    meta["worker_b_submit_start_at_epoch_sec"] = time.time()
    with ThreadPoolExecutor(max_workers=concurrent_submitters) as pool:
        futures = [
            pool.submit(
                _storm_debouncer,
                debouncer,
                debounce_key,
                period,
                f"b-payload-{index}",
                f"worker_b index={index}",
            )
            for index in range(storm_count)
        ]
        for future in as_completed(futures):
            future.result()
    meta["worker_b_resubmit_done_at_epoch_sec"] = time.time()

    meta["worker_b_final_submit_start_at_epoch_sec"] = time.time()
    progress("debounce_aggr_final_submit", f"key={debounce_key}")
    handle = debouncer.debounce(
        debounce_key,
        period,
        debounce_key,
        final_payload,
    )
    meta["worker_b_final_submit_done_at_epoch_sec"] = time.time()
    META_PATH.write_text(json.dumps(meta))

    deadline = time.monotonic() + BUDGET_SEC
    while time.monotonic() < deadline:
        try:
            status = handle.get_status().status
        except DBOSNonExistentWorkflowError:
            time.sleep(0.05)
            continue
        if status in (
            WorkflowStatusString.SUCCESS.value,
            WorkflowStatusString.ERROR.value,
        ):
            break
        time.sleep(0.05)
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


def scenario_debounce_storm_aggressive(root_seed: int) -> None:
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
    before_rows = completion_rows(debounce_key)
    before_payloads = [payload for payload, _, _ in before_rows]
    before_recovery = len(before_rows)
    due_at = float(meta.get("worker_a_due_at_epoch_sec") or 0)
    premature_rows = [
        (payload, completed_at)
        for payload, completed_at, _ in before_rows
        if due_at == 0 or completed_at < due_at
    ]
    progress(
        "pre_recovery_payload_audit",
        f"count={before_recovery} due_at={due_at:.6f} rows={before_rows}",
    )
    invariant(
        "D0",
        "no_premature_completion",
        len(premature_rows) == 0,
        f"premature={premature_rows} due_at={due_at:.6f} "
        f"all_pre_recovery={before_rows}",
    )
    invariant(
        "D0b",
        "at_most_one_pre_recovery_completion",
        before_recovery <= 1,
        f"debounce_storm_results={before_recovery} payloads={before_payloads}",
    )
    expected_a_payload = f"a-payload-{int(meta['storm_count']) - 1}"
    invariant(
        "D0c",
        "legal_pre_recovery_payload",
        before_recovery == 0 or before_payloads == [expected_a_payload],
        f"payloads={before_payloads} expected_if_completed={[expected_a_payload]}",
    )

    worker_b = _run_subphase("worker_b", {**base_env, "DBOS__VMID": "worker-b"})
    if worker_b.returncode != 0:
        raise RuntimeError(f"worker_b failed rc={worker_b.returncode}")
    meta = json.loads(META_PATH.read_text())
    period = float(meta["debounce_period_sec"])

    payloads = completion_payloads(debounce_key)
    final_rows = completion_rows(debounce_key)
    completions = len(payloads)
    expected_payloads = before_payloads + [str(meta["final_payload"])]
    worker_b_span = float(meta["worker_b_final_submit_start_at_epoch_sec"]) - float(
        meta["worker_b_submit_start_at_epoch_sec"]
    )
    strict_single_window = worker_b_span < period
    progress(
        "payload_audit",
        f"count={completions} rows={final_rows} "
        f"worker_b_span={worker_b_span:.6f} period={period:.6f} "
        f"strict_single_window={strict_single_window}",
    )
    invariant(
        "D1",
        "one_completion_per_debounce_window",
        (not strict_single_window) or payloads == expected_payloads,
        f"payloads={payloads} expected={expected_payloads} key={debounce_key}",
    )
    invariant(
        "D1b",
        "final_payload_completed_after_recovery",
        bool(payloads) and payloads[-1] == str(meta["final_payload"]),
        f"payloads={payloads} expected_final={meta['final_payload']}",
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
        f"key={debounce_key} storm={meta['storm_count']} completions={completions}",
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

    return workload_main(
        "dbos_debounce_storm_aggressive",
        scenario_debounce_storm_aggressive,
    )


if __name__ == "__main__":
    raise SystemExit(main())
