#!/usr/bin/env python3
"""
P5f: PG serialization conflict retry (#679).

# SURFACE: concurrent queue workers on same ledger op
# MODELS:  two dequeued workflows sharing one op_id
# ORACLE:  exactly_once_op; ledger_conservation
# ISSUES:  #679
"""

from __future__ import annotations

import json

from dbos_workload_common import (
    APP_DB,
    invariant,
    progress,
    psql,
    workload_main,
    workload_seed_raw,
)

QUEUE_NAME = "formal_pg_conflict"


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


def sql_scalar(sql: str) -> str:
    return psql(sql, database=APP_DB).splitlines()[-1].strip()


def scenario_pg_conflict(root_seed: int) -> None:
    from dbos import DBOS
    from dbos_workload_common import dbos_config

    init_app_schema()
    seed_accounts(4, 1000)

    account_count = 4
    initial_balance = 1000
    amount = 10 + (root_seed % 20)
    op_id = f"op_{workload_seed_raw()[:8]}"
    op = {"op_id": op_id, "src": 1, "dst": 2, "amount": amount}
    serialized = json.dumps(op)

    config = dbos_config()
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)
    DBOS.launch()
    DBOS.register_queue(
        QUEUE_NAME,
        concurrency=2,
        worker_concurrency=2,
        polling_interval_sec=0.05,
    )
    from dbos_pg_conflict_wf import same_op_wf

    progress("parallel_same_op_start", f"op_id={op_id} amount={amount}")
    handles = [
        DBOS.enqueue_workflow(QUEUE_NAME, same_op_wf, serialized),
        DBOS.enqueue_workflow(QUEUE_NAME, same_op_wf, serialized),
    ]
    try:
        for handle in handles:
            handle.get_result()
    except Exception as exc:
        progress("workflow_error", str(exc)[:400])
        raise

    statuses = [handle.get_status().status for handle in handles]
    invariant(
        "P5f",
        "workflow_terminal_success",
        all(status == "SUCCESS" for status in statuses),
        f"statuses={statuses}",
    )

    done_ops = int(
        sql_scalar(
            f"SELECT COUNT(*) FROM applied_ops WHERE op_id = '{op_id}' AND state = 'done';"
        )
    )
    inflight_ops = int(
        sql_scalar(
            f"SELECT COUNT(*) FROM applied_ops WHERE op_id = '{op_id}' AND state = 'inflight';"
        )
    )
    ledger_rows = int(sql_scalar(f"SELECT COUNT(*) FROM ledger WHERE op_id = '{op_id}';"))
    total_balance = int(sql_scalar("SELECT COALESCE(SUM(balance), 0) FROM account;"))
    expected_total = account_count * initial_balance

    invariant(
        "P5f",
        "exactly_once_op",
        done_ops == 1 and inflight_ops == 0 and ledger_rows == 2,
        f"done={done_ops} inflight={inflight_ops} ledger_rows={ledger_rows}",
    )
    invariant(
        "P5f",
        "ledger_conservation",
        total_balance == expected_total,
        f"total_balance={total_balance} expected={expected_total}",
    )

    DBOS.destroy(destroy_registry=True)
    progress("scenario_done", f"op_id={op_id}")


def main() -> int:
    return workload_main("dbos_pg_conflict_retry", scenario_pg_conflict)


if __name__ == "__main__":
    raise SystemExit(main())
