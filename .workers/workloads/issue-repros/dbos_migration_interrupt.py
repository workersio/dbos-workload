#!/usr/bin/env python3
"""
P8m: migration under interruption — kill DBOS during launch/migrations, then recover.

# SURFACE: DBOS system migrations + launch
# MODELS:  subprocess DBOS.launch() interrupted 1–3 times, then clean launch + workflow
# ORACLE:  M1 migration version at head, M2 required tables exist, M3 smoke workflow SUCCESS
# ISSUES:  migration idempotency, partial schema, startup recovery
# VARIANCE: interrupt_count (1–3) from WORKLOAD_SEED
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

NO_DEQUEUE = "__formal_no_dequeue__"
META_PATH = RUN_DIR / "meta.json"
LAUNCH_READY = RUN_DIR / "launch.ready"


def init_app_schema() -> None:
    psql(
        """
        CREATE TABLE IF NOT EXISTS migration_smoke(
          run_id TEXT PRIMARY KEY,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
        database=APP_DB,
    )
    psql("TRUNCATE migration_smoke;", database=APP_DB)


def sql_scalar(sql: str, database: str = SYS_DB) -> str:
    return psql(sql, database=database).splitlines()[-1].strip()


def expected_migration_version() -> int:
    from dbos._migration import get_dbos_migrations

    return len(get_dbos_migrations("dbos", True))


def migration_version() -> int:
    return int(sql_scalar('SELECT version FROM dbos.dbos_migrations;'))


def required_tables_present() -> bool:
    required = (
        "workflow_status",
        "operation_outputs",
        "streams",
        "dbos_migrations",
    )
    rows = sql_scalar(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = 'dbos' AND table_name IN ("
        + ",".join(f"'{name}'" for name in required)
        + ");"
    )
    return rows == str(len(required))


def run_phase_launch(block_after: bool) -> int:
    from dbos import DBOS

    if LAUNCH_READY.exists():
        LAUNCH_READY.unlink()
    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([NO_DEQUEUE])
    progress("launch_begin")
    DBOS.launch()
    progress("launch_done")
    LAUNCH_READY.write_text("ready")
    if block_after:
        time.sleep(2)
    DBOS.destroy(destroy_registry=True)
    return 0


def run_phase_smoke(run_id: str) -> int:
    from dbos import DBOS
    from dbos._sys_db import WorkflowStatusString

    from dbos_migration_interrupt_wf import smoke_after_migration

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.listen_queues([NO_DEQUEUE])
    DBOS.launch()

    handle = DBOS.start_workflow(smoke_after_migration, run_id)
    result = handle.get_result()
    status = handle.get_status().status
    invariant(
        "M3",
        "smoke_workflow_success",
        status == WorkflowStatusString.SUCCESS.value and result == run_id,
        f"workflow_id={handle.workflow_id} status={status} result={result}",
    )
    DBOS.destroy(destroy_registry=True)
    return 0


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


def interrupt_launch(block_after: bool, env: dict[str, str]) -> int:
    proc = subprocess.Popen(
        [sys.executable, __file__, "--phase", "launch"],
        env=subphase_env(__file__, {**env, "LAUNCH_BLOCK": "1" if block_after else "0"}),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    assert proc.stdout is not None
    killed = False
    for line in proc.stdout:
        print(line, end="", flush=True)
        if not killed and "PROGRESS launch_begin" in line:
            progress("launch_interrupt_kill")
            proc.kill()
            killed = True
    return proc.wait()


def scenario_migration_interrupt(root_seed: int) -> None:
    interrupt_count = 1 + (root_seed % 3)
    run_id = f"migrate-{workload_seed_raw()[:12]}"
    META_PATH.write_text(
        json.dumps({"interrupt_count": interrupt_count, "run_id": run_id})
    )

    progress("schema_init")
    init_app_schema()

    base_env = {"WORKLOAD_SEED": workload_seed_raw()}
    for index in range(interrupt_count):
        progress("interrupt_round", f"{index + 1}/{interrupt_count}")
        rc = interrupt_launch(block_after=False, env=base_env)
        progress("interrupt_round_done", f"round={index + 1} rc={rc}")

    progress("clean_launch_start")
    launch = _run_subphase("launch", base_env)
    if launch.returncode != 0:
        raise RuntimeError(f"clean launch failed rc={launch.returncode}")
    progress("clean_launch_done")

    expected = expected_migration_version()
    actual = migration_version()
    invariant(
        "M1",
        "migration_version_at_head",
        actual == expected,
        f"version={actual} expected={expected}",
    )
    invariant(
        "M2",
        "required_tables_exist",
        required_tables_present(),
        "dbos.workflow_status, operation_outputs, streams, dbos_migrations",
    )

    smoke = _run_subphase("smoke", {**base_env, "SMOKE_RUN_ID": run_id})
    if smoke.returncode != 0:
        raise RuntimeError(f"smoke phase failed rc={smoke.returncode}")
    rows = int(sql_scalar("SELECT COUNT(*) FROM migration_smoke;", database=APP_DB))
    invariant(
        "M3b",
        "smoke_record_once",
        rows == 1,
        f"migration_smoke={rows}",
    )
    progress("scenario_done", f"interrupts={interrupt_count} version={actual}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["launch", "smoke"], default="")
    args = parser.parse_args()

    if args.phase == "launch":
        block_after = os.environ.get("LAUNCH_BLOCK", "0") == "1"
        return run_phase_launch(block_after=block_after)

    if args.phase == "smoke":
        run_id = os.environ.get("SMOKE_RUN_ID", "")
        if not run_id:
            return 2
        progress("dbos_smoke_launch")
        return run_phase_smoke(run_id)

    return workload_main("dbos_migration_interrupt", scenario_migration_interrupt)


if __name__ == "__main__":
    raise SystemExit(main())
