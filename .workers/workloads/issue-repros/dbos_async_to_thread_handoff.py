#!/usr/bin/env python3
"""
P4t: asyncio.to_thread + @DBOS.transaction (#664).

# SURFACE: async workflow + concurrent to_thread(transaction)
# MODELS:  gather of three to_thread transfer transactions
# ORACLE:  all tx_audit rows under correct workflow_id; no context-loss errors
# ISSUES:  #664
"""

from __future__ import annotations

import asyncio

from dbos_workload_common import (
    APP_DB,
    invariant,
    progress,
    psql,
    workload_main,
    workload_seed_raw,
)


def init_schema() -> None:
    psql(
        """
        CREATE TABLE IF NOT EXISTS tx_audit(
          id SERIAL PRIMARY KEY,
          run_id TEXT NOT NULL,
          tag TEXT NOT NULL,
          workflow_id TEXT NOT NULL,
          executor TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS hot_account(
          id INTEGER PRIMARY KEY,
          balance INTEGER NOT NULL
        );
        """,
        database=APP_DB,
    )
    psql(
        "TRUNCATE tx_audit RESTART IDENTITY; "
        "INSERT INTO hot_account(id, balance) VALUES (1, 1000) "
        "ON CONFLICT (id) DO UPDATE SET balance = 1000;",
        database=APP_DB,
    )


def sql_scalar(sql: str) -> str:
    return psql(sql, database=APP_DB).splitlines()[-1].strip()


async def run_scenario_async(root_seed: int) -> None:
    from dbos import DBOS
    from dbos_workload_common import dbos_config

    run_id = f"to-thread-{workload_seed_raw()[:16]}"
    amount = 1 + (root_seed % 5)

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    from dbos_async_to_thread_wf import gather_to_thread_wf

    DBOS.launch()

    progress("gather_to_thread_start", f"run_id={run_id} amount={amount}")
    handle = await DBOS.start_workflow_async(gather_to_thread_wf, run_id, amount)
    try:
        result = await handle.get_result()
    except Exception as exc:
        invariant(
            "P4t",
            "no_context_loss_error",
            "Transactions must be called from within workflows" not in str(exc),
            str(exc)[:400],
        )
        raise

    status = handle.get_status()
    invariant(
        "P4t",
        "workflow_terminal_success",
        status.status == "SUCCESS",
        f"status={status.status} result={result} error={(status.error or '')[:200]}",
    )

    expected_wf = handle.workflow_id
    rows = int(sql_scalar(f"SELECT COUNT(*) FROM tx_audit WHERE run_id = '{run_id}';"))
    mismatch = int(
        sql_scalar(
            f"SELECT COUNT(*) FROM tx_audit WHERE run_id = '{run_id}' "
            f"AND workflow_id <> '{expected_wf}';"
        )
    )
    invariant(
        "P4t",
        "all_tx_rows_correct_workflow_id",
        rows == 3 and mismatch == 0,
        f"rows={rows} workflow_id={expected_wf} mismatches={mismatch}",
    )
    invariant(
        "P4t",
        "no_context_loss_error",
        True,
        "no Transactions must be called from within workflows",
    )

    DBOS.destroy(destroy_registry=True)
    progress("scenario_done", f"run_id={run_id} rows={rows}")


def scenario_to_thread(root_seed: int) -> None:
    init_schema()
    asyncio.run(run_scenario_async(root_seed))


def main() -> int:
    return workload_main("dbos_async_to_thread_handoff", scenario_to_thread)


if __name__ == "__main__":
    raise SystemExit(main())
