#!/usr/bin/env python3
"""
P3ext: external side-effect idempotency — crash after API call, recover.

# SURFACE: Step retries, external side effects vs durable step completion
# MODELS:  workflow calls mock external API (records attempt), crashes before
#          step returns; recover and complete
# ORACLE:  X1 processed_exactly_once, X2 attempts_bounded, X3 terminal_success,
#          X4 recovery_bounded
# ISSUES:  duplicate external calls beyond retry envelope
# VARIANCE: op_id from WORKLOAD_SEED
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from dbos_workload_common import (
    APP_DB,
    RUN_DIR,
    dbos_config,
    invariant,
    progress,
    psql,
    workload_main,
    workload_seed_raw,
)

MAX_RECOVERY_ATTEMPTS = 8
MAX_EXTERNAL_ATTEMPTS = 4

META_PATH = RUN_DIR / "meta.json"
WORKFLOW_ID = os.environ.get("DBOS_WORKFLOW_ID", "")


def init_app_schema() -> None:
    psql(
        """
        CREATE TABLE IF NOT EXISTS external_attempts(
          id SERIAL PRIMARY KEY,
          op_id TEXT NOT NULL,
          run_id TEXT NOT NULL,
          executor TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS processed_external(
          op_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL
        );
        """,
        database=APP_DB,
    )


def reset_app_tables() -> None:
    psql(
        "TRUNCATE external_attempts, processed_external RESTART IDENTITY;",
        database=APP_DB,
    )


def build_scenario(root_seed: int) -> dict[str, object]:
    run_id = f"external-{workload_seed_raw()[:16]}"
    op_id = f"op_{root_seed % 10000:04d}_{workload_seed_raw()[:8]}"
    return {
        "run_id": run_id,
        "op_id": op_id,
        "workflow_id": f"ext-{workload_seed_raw()[:16]}",
    }


def sql_scalar(sql: str) -> str:
    return psql(sql, database=APP_DB).splitlines()[-1].strip()


def run_phase_crash(run_id: str, op_id: str, workflow_id: str) -> None:
    from dbos import DBOS, SetWorkflowID

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.launch()

    from dbos_external_idempotency_wf import ExternalWF

    os.environ["DBOS_CRASH_NOW"] = "1"
    with SetWorkflowID(workflow_id):
        ExternalWF.idempotency_workflow(run_id, op_id)

    DBOS.destroy(destroy_registry=True)


def run_phase_recover(workflow_id: str) -> None:
    from dbos import DBOS

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.launch()

    from dbos_external_idempotency_wf import ExternalWF  # noqa: F401

    DBOS._recover_pending_workflows()

    handle = DBOS.retrieve_workflow(workflow_id)
    result = handle.get_result()
    status = handle.get_status()
    progress("recover_result", f"result={result} status={status.status}")

    invariant(
        "X3",
        "workflow_terminal_success",
        status.status == "SUCCESS",
        f"status={status.status} error={(status.error or '')[:200]}",
    )
    invariant(
        "X4",
        "recovery_attempts_bounded",
        (status.recovery_attempts or 0) <= MAX_RECOVERY_ATTEMPTS,
        f"recovery_attempts={status.recovery_attempts}",
    )

    DBOS.destroy(destroy_registry=True)


def assert_external_oracles(run_id: str, op_id: str) -> None:
    rid = run_id.replace("'", "''")
    oid = op_id.replace("'", "''")

    attempts = int(
        sql_scalar(
            f"SELECT COUNT(*) FROM external_attempts "
            f"WHERE run_id = '{rid}' AND op_id = '{oid}';"
        )
    )
    processed = int(
        sql_scalar(
            f"SELECT COUNT(*) FROM processed_external "
            f"WHERE run_id = '{rid}' AND op_id = '{oid}';"
        )
    )

    invariant(
        "X1",
        "processed_exactly_once",
        processed == 1,
        f"processed={processed} op_id={op_id}",
    )
    invariant(
        "X2",
        "external_attempts_bounded",
        1 <= attempts <= MAX_EXTERNAL_ATTEMPTS,
        f"attempts={attempts} max={MAX_EXTERNAL_ATTEMPTS} op_id={op_id}",
    )


def _run_subphase(phase: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        **env,
        "DBOS_WORKLOAD_RUN_DIR": str(RUN_DIR.parent),
        "PYTHONPATH": os.pathsep.join(
            [
                str(Path(__file__).resolve().parent),
                str(Path(__file__).resolve().parents[2] / ".workers" / "vendor" / "py"),
            ]
        ),
    }
    progress(f"subphase_{phase}_start")
    proc = subprocess.Popen(
        [sys.executable, __file__, "--phase", phase],
        env=env,
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


def scenario_external_idempotency(root_seed: int) -> None:
    meta = build_scenario(root_seed)
    META_PATH.write_text(json.dumps(meta))

    progress("schema_init")
    init_app_schema()
    reset_app_tables()

    run_id = str(meta["run_id"])
    op_id = str(meta["op_id"])
    workflow_id = str(meta["workflow_id"])

    base_env = {
        "WORKLOAD_SEED": workload_seed_raw(),
        "DBOS_WORKFLOW_ID": workflow_id,
    }

    crash = _run_subphase("crash", base_env)
    if crash.returncode != 99:
        raise RuntimeError(f"crash phase expected rc=99 got {crash.returncode}")

    recover = _run_subphase("recover", base_env)
    if recover.returncode != 0:
        raise RuntimeError(f"recover phase failed rc={recover.returncode}")

    assert_external_oracles(run_id, op_id)
    progress("scenario_done", f"run_id={run_id} op_id={op_id}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["crash", "recover"], default="")
    args = parser.parse_args()

    if args.phase == "crash":
        if not META_PATH.exists():
            return 2
        meta = json.loads(META_PATH.read_text())
        progress("dbos_crash_launch")
        run_phase_crash(
            str(meta["run_id"]),
            str(meta["op_id"]),
            str(meta["workflow_id"]),
        )
        return 99

    if args.phase == "recover":
        if not META_PATH.exists():
            return 2
        meta = json.loads(META_PATH.read_text())
        progress("dbos_recover_launch")
        run_phase_recover(str(meta["workflow_id"]))
        return 0

    return workload_main("dbos_external_idempotency", scenario_external_idempotency)


if __name__ == "__main__":
    raise SystemExit(main())
