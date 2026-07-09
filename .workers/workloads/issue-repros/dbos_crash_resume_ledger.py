#!/usr/bin/env python3
"""
Crash mid-workflow, relaunch, recover — with ledger conservation invariants.

Mirrors dbos-transact-py tests/queuedworkflow.py but adds independent SQL oracles
for Formal / Workers IO exploration.

Stdout includes machine-readable `INVARIANT ` lines (via dbos_workload_common.invariant).
"""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
from pathlib import Path

from dbos_workload_common import (
    APP_DB,
    RUN_DIR,
    InvariantFailure,
    dbos_config,
    invariant,
    progress,
    psql,
    reset_run_dir,
    seed_int,
    start_postgres,
    stop_postgres,
    vendor_ready,
    workload_main,
    workload_seed_raw,
)

# ---------------------------------------------------------------------------
# Phase runner (subprocess target)
# ---------------------------------------------------------------------------

WORKFLOW_ID = os.environ.get("DBOS_WORKFLOW_ID", "")
OPS_PATH = RUN_DIR / "ops.json"
META_PATH = RUN_DIR / "meta.json"


def init_app_schema() -> None:
    psql(
        """
        CREATE TABLE IF NOT EXISTS account(
          id INTEGER PRIMARY KEY,
          balance INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ledger(
          entry_id INTEGER PRIMARY KEY,
          op_id TEXT NOT NULL,
          account_id INTEGER NOT NULL,
          delta INTEGER NOT NULL,
          UNIQUE(op_id, account_id)
        );
        CREATE TABLE IF NOT EXISTS applied_ops(
          op_id TEXT PRIMARY KEY,
          state TEXT NOT NULL CHECK(state IN ('inflight', 'done'))
        );
        """,
        database=APP_DB,
    )


def seed_accounts(account_count: int, initial_balance: int) -> None:
    values = ", ".join(f"({i}, {initial_balance})" for i in range(1, account_count + 1))
    psql(
        f"""
        TRUNCATE applied_ops, ledger, account RESTART IDENTITY;
        INSERT INTO account(id, balance) VALUES {values};
        """,
        database=APP_DB,
    )


def build_ops(root_seed: int, count: int, account_count: int) -> list[dict[str, int | str]]:
    rng = random.Random(root_seed ^ 0xDB05)
    ops: list[dict[str, int | str]] = []
    for step in range(count):
        src = 1 + rng.randrange(account_count)
        dst = 1 + rng.randrange(account_count - 1)
        if dst >= src:
            dst += 1
        amount = 1 + rng.randrange(25)
        ops.append(
            {
                "op_id": f"op_{step:03d}",
                "src": src,
                "dst": dst,
                "amount": amount,
            }
        )
    return ops


def run_phase_crash(ops: list[dict], crash_after: int) -> None:
    from dbos import DBOS, SetWorkflowID

    config = dbos_config()

    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.launch()

    # Import after DBOS init; destroy_registry clears decorator registrations.
    from dbos_ledger_wf import LedgerWF

    os.environ["DBOS_CRASH_NOW"] = "1"
    with SetWorkflowID(WORKFLOW_ID):
        LedgerWF.ledger_workflow(json.dumps(ops), crash_after)

    DBOS.destroy(destroy_registry=True)


def run_phase_recover(expected_ops: int) -> None:
    from dbos import DBOS

    config = dbos_config()

    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.launch()

    from dbos_ledger_wf import LedgerWF  # noqa: F401 — registers workflows

    DBOS._recover_pending_workflows()

    handle = DBOS.retrieve_workflow(WORKFLOW_ID)
    result = handle.get_result()
    status = handle.get_status().status

    invariant(
        "I3",
        "workflow_terminal_success",
        status == "SUCCESS",
        f"workflow_id={WORKFLOW_ID} status={status} result={result}",
    )
    invariant(
        "I3b",
        "workflow_result_count",
        result == expected_ops,
        f"expected={expected_ops} actual={result}",
    )

    DBOS.destroy(destroy_registry=True)


def sql_scalar(sql: str) -> str:
    return psql(sql, database=APP_DB).splitlines()[-1].strip()


def assert_ledger_invariants(account_count: int, initial_balance: int, expected_ops: int) -> None:
    initial_total = account_count * initial_balance
    balance_sum = sql_scalar("SELECT COALESCE(SUM(balance), 0) FROM account;")
    ledger_rows = sql_scalar("SELECT COUNT(*) FROM ledger;")
    done_ops = sql_scalar("SELECT COUNT(*) FROM applied_ops WHERE state = 'done';")
    inflight_ops = sql_scalar("SELECT COUNT(*) FROM applied_ops WHERE state = 'inflight';")
    ledger_net = sql_scalar("SELECT COALESCE(SUM(delta), 0) FROM ledger;")

    invariant(
        "I1",
        "ledger_conservation",
        balance_sum == str(initial_total),
        f"balance_sum={balance_sum} expected={initial_total}",
    )
    invariant(
        "I2",
        "step_exactly_once",
        done_ops == str(expected_ops) and inflight_ops == "0",
        f"done_ops={done_ops} inflight_ops={inflight_ops} expected_done={expected_ops}",
    )
    invariant(
        "I2b",
        "ledger_row_count",
        ledger_rows == str(expected_ops * 2),
        f"ledger_rows={ledger_rows} expected={expected_ops * 2}",
    )
    invariant(
        "I2c",
        "ledger_net_zero",
        ledger_net == "0",
        f"ledger_net={ledger_net}",
    )


def _run_subphase(phase: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        **env,
        # DBOS_WORKLOAD_RUN_DIR is the base dir; common appends dbos-workload-<stem>.
        "DBOS_WORKLOAD_RUN_DIR": str(RUN_DIR.parent),
        "PYTHONPATH": os.pathsep.join(
            [str(Path(__file__).resolve().parent), str(Path(__file__).resolve().parents[2] / ".workers" / "vendor" / "py")]
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
    stdout = "".join(captured)
    return subprocess.CompletedProcess(
        args=[__file__, "--phase", phase],
        returncode=rc,
        stdout=stdout,
        stderr="",
    )


def scenario_crash_resume(root_seed: int) -> None:
    global WORKFLOW_ID
    account_count = 8
    transfer_count = 6
    initial_balance = 1000
    crash_after = 1 + (root_seed % transfer_count)

    WORKFLOW_ID = f"crash-ledger-{workload_seed_raw()[:16]}"
    ops = build_ops(root_seed, transfer_count, account_count)

    META_PATH.write_text(
        json.dumps(
            {
                "workflow_id": WORKFLOW_ID,
                "crash_after": crash_after,
                "transfer_count": transfer_count,
                "account_count": account_count,
                "initial_balance": initial_balance,
            }
        )
    )
    OPS_PATH.write_text(json.dumps(ops))

    progress("schema_init")
    init_app_schema()
    progress("accounts_seed")
    seed_accounts(account_count, initial_balance)

    base_env = {
        "DBOS_WORKFLOW_ID": WORKFLOW_ID,
        "WORKLOAD_SEED": workload_seed_raw(),
    }

    crash = _run_subphase("crash", base_env)
    if crash.returncode == 0:
        invariant(
            "I0",
            "crash_phase_exited_nonzero",
            False,
            f"expected crash exit != 0 got rc={crash.returncode} stderr={crash.stderr[-500:]}",
        )
    if crash.returncode != 99:
        err = (crash.stderr or crash.stdout or "").strip()
        if "Traceback" in err:
            err = err[err.index("Traceback") :]
        detail = err[:4000] if len(err) > 4000 else err
        if crash.stderr:
            print(crash.stderr, file=sys.stderr, end="" if crash.stderr.endswith("\n") else "\n")
        raise RuntimeError(
            f"crash phase failed rc={crash.returncode} (expected 99); detail={detail}"
        )

    recover = _run_subphase("recover", base_env)
    if recover.stdout:
        print(recover.stdout, end="" if recover.stdout.endswith("\n") else "\n")
    if recover.returncode != 0:
        if recover.stderr:
            print(recover.stderr, file=sys.stderr, end="" if recover.stderr.endswith("\n") else "\n")
        raise RuntimeError(f"recover phase failed rc={recover.returncode}")


def main() -> int:
    global WORKFLOW_ID

    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["crash", "recover"], default="")
    args = parser.parse_args()

    if args.phase == "crash":
        if not OPS_PATH.exists() or not META_PATH.exists():
            print("missing ops/meta; run full workload first", file=sys.stderr)
            return 2
        meta = json.loads(META_PATH.read_text())
        ops = json.loads(OPS_PATH.read_text())
        WORKFLOW_ID = meta["workflow_id"]
        progress("dbos_crash_launch")
        run_phase_crash(ops, int(meta["crash_after"]))
        return 99

    if args.phase == "recover":
        if not META_PATH.exists():
            print("missing meta", file=sys.stderr)
            return 2
        meta = json.loads(META_PATH.read_text())
        WORKFLOW_ID = meta["workflow_id"]
        progress("dbos_recover_launch")
        run_phase_recover(int(meta["transfer_count"]))
        assert_ledger_invariants(
            int(meta["account_count"]),
            int(meta["initial_balance"]),
            int(meta["transfer_count"]),
        )
        return 0

    return workload_main("dbos_crash_resume_ledger", scenario_crash_resume)


if __name__ == "__main__":
    raise SystemExit(main())
