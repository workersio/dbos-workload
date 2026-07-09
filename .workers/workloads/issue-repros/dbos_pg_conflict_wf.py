"""Workflow definitions for PG serialization conflict retry (P5f)."""

from __future__ import annotations

import json

import sqlalchemy as sa
from dbos import DBOS


@DBOS.transaction()
def contested_transfer(op_id: str, src: int, dst: int, amount: int) -> None:
    session = DBOS.sql_session
    params = {
        "op_id": op_id,
        "src": src,
        "dst": dst,
        "amount": amount,
        "neg_amount": -amount,
        "entry_src": int(op_id.split("_")[1]) * 2 + 1,
        "entry_dst": int(op_id.split("_")[1]) * 2 + 2,
    }
    inflight = "SELECT 1 FROM applied_ops WHERE op_id = :op_id AND state = 'inflight'"
    session.execute(
        sa.text(
            "INSERT INTO applied_ops(op_id, state) VALUES (:op_id, 'inflight') "
            "ON CONFLICT (op_id) DO NOTHING"
        ),
        params,
    )
    session.execute(
        sa.text(
            f"UPDATE account SET balance = balance - :amount "
            f"WHERE id = :src AND EXISTS ({inflight})"
        ),
        params,
    )
    session.execute(
        sa.text(
            f"UPDATE account SET balance = balance + :amount "
            f"WHERE id = :dst AND EXISTS ({inflight})"
        ),
        params,
    )
    session.execute(
        sa.text(
            f"INSERT INTO ledger(entry_id, op_id, account_id, delta) "
            f"SELECT :entry_src, :op_id, :src, :neg_amount "
            f"WHERE EXISTS ({inflight})"
        ),
        params,
    )
    session.execute(
        sa.text(
            f"INSERT INTO ledger(entry_id, op_id, account_id, delta) "
            f"SELECT :entry_dst, :op_id, :dst, :amount "
            f"WHERE EXISTS ({inflight})"
        ),
        params,
    )
    session.execute(
        sa.text("UPDATE applied_ops SET state = 'done' WHERE op_id = :op_id"),
        params,
    )


@DBOS.workflow()
def same_op_wf(serialized_op: str) -> int:
    op = json.loads(serialized_op)
    contested_transfer(
        op["op_id"],
        int(op["src"]),
        int(op["dst"]),
        int(op["amount"]),
    )
    return 1
