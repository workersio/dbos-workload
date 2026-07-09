# SURFACE: Stream / fork (P7f)
# MODELS:  write_stream from workflow + step; fork_workflow from recovered parent
# ORACLE:  stream_results table + read_stream checks in parent harness
# ISSUES:  #577, #421
# VARIANCE: stream_key, crash_after_writes, fork_step from parent scenario

from __future__ import annotations

import json
import os

import sqlalchemy as sa
from dbos import DBOS


@DBOS.dbos_class()
class StreamForkWF:
    @staticmethod
    @DBOS.transaction()
    def record_terminal(
        run_id: str, workflow_id: str, stream_key: str, values: list[int]
    ) -> None:
        session = DBOS.sql_session
        session.execute(
            sa.text(
                "INSERT INTO stream_results(run_id, workflow_id, stream_key, values_json, executor) "
                "VALUES (:run_id, :workflow_id, :stream_key, :values_json, :executor)"
            ),
            {
                "run_id": run_id,
                "workflow_id": workflow_id,
                "stream_key": stream_key,
                "values_json": json.dumps(values),
                "executor": os.environ.get("DBOS__VMID", "local"),
            },
        )

    @staticmethod
    @DBOS.step()
    def step_write(stream_key: str, value: int) -> int:
        DBOS.write_stream(stream_key, value)
        return value

    @staticmethod
    @DBOS.workflow()
    def stream_parent(run_id: str, stream_key: str, crash_after_writes: int) -> str:
        DBOS.write_stream(stream_key, 0)
        DBOS.write_stream(stream_key, 1)
        if crash_after_writes == 2 and os.environ.get("DBOS_CRASH_NOW") == "1":
            print(
                f"PROGRESS stream_crash point=after_wf_writes key={stream_key}",
                flush=True,
            )
            os._exit(99)
        StreamForkWF.step_write(stream_key, 2)
        if crash_after_writes == 3 and os.environ.get("DBOS_CRASH_NOW") == "1":
            print(
                f"PROGRESS stream_crash point=after_step_write key={stream_key}",
                flush=True,
            )
            os._exit(99)
        DBOS.close_stream(stream_key)
        workflow_id = DBOS.workflow_id
        if workflow_id is None:
            raise RuntimeError("missing workflow_id after close_stream")
        StreamForkWF.record_terminal(run_id, workflow_id, stream_key, [0, 1, 2])
        print(f"PROGRESS stream_parent_done workflow_id={workflow_id}", flush=True)
        return workflow_id
