"""Workflow definitions for asyncio.to_thread transaction handoff (P4t)."""

from __future__ import annotations

import asyncio
import os

from dbos import DBOS
from dbos._context import get_local_dbos_context


@DBOS.transaction()
def audited_bump(tag: str, run_id: str, amount: int) -> None:
    import sqlalchemy as sa

    ctx = get_local_dbos_context()
    if ctx is None or not ctx.is_workflow():
        raise RuntimeError("Transactions must be called from within workflows")
    workflow_id = ctx.workflow_id
    executor = os.environ.get("DBOS__VMID", "local")
    session = DBOS.sql_session
    session.execute(
        sa.text("UPDATE hot_account SET balance = balance - :amount WHERE id = 1"),
        {"amount": amount},
    )
    session.execute(
        sa.text(
            "INSERT INTO tx_audit(run_id, tag, workflow_id, executor) "
            "VALUES (:run_id, :tag, :workflow_id, :executor)"
        ),
        {
            "run_id": run_id,
            "tag": tag,
            "workflow_id": workflow_id,
            "executor": executor,
        },
    )


@DBOS.workflow()
async def gather_to_thread_wf(run_id: str, amount: int) -> int:
    await asyncio.gather(
        asyncio.to_thread(audited_bump, "a", run_id, amount),
        asyncio.to_thread(audited_bump, "b", run_id, amount),
        asyncio.to_thread(audited_bump, "c", run_id, amount),
    )
    return 3
