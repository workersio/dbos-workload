# SURFACE: Debouncer pending storm (P2d)
# MODELS:  debounced target workflow recorded in app DB
# ORACLE:  completion markers for storm probes
# ISSUES:  debouncer internal queue, dedup rows, crash during pending window

from __future__ import annotations

import time

import sqlalchemy as sa
from dbos import DBOS

from dbos_workload_common import APP_DB, dbos_config  # noqa: F401

_APP_ENGINE = None


def _app_engine():
    global _APP_ENGINE
    if _APP_ENGINE is None:
        url = str(dbos_config()["application_database_url"]).replace(
            "postgresql://", "postgresql+psycopg://", 1
        )
        from sqlalchemy.pool import NullPool

        _APP_ENGINE = sa.create_engine(
            url,
            connect_args={"application_name": "dbos_workload_oracle"},
            poolclass=NullPool,
        )
    return _APP_ENGINE


@DBOS.workflow()
def debounced_storm_target(debounce_key: str, payload: str) -> str:
    with _app_engine().begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO debounce_storm_results("
                "debounce_key, payload, completed_at_epoch_sec, source_workflow_id"
                ") VALUES ("
                ":debounce_key, :payload, :completed_at_epoch_sec, :source_workflow_id"
                ")"
            ),
            {
                "debounce_key": debounce_key,
                "payload": payload,
                "completed_at_epoch_sec": time.time(),
                "source_workflow_id": DBOS.workflow_id,
            },
        )
    return payload
