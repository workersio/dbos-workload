# SURFACE: Debouncer version skew (P3)
# MODELS:  debounced target workflow recorded in app DB
# ORACLE:  completion markers for debounce skew probes
# ISSUES:  #702

from __future__ import annotations

import os

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


def _record_completion(debounce_key: str, payload: str) -> None:
    deploy = os.environ.get("DBOS__APPVERSION", "unknown")
    with _app_engine().begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO debounce_completions(debounce_key, payload, deploy_version) "
                "VALUES (:debounce_key, :payload, :deploy_version)"
            ),
            {
                "debounce_key": debounce_key,
                "payload": payload,
                "deploy_version": deploy,
            },
        )


@DBOS.workflow()
def debounced_target(debounce_key: str, payload: str) -> str:
    _record_completion(debounce_key, payload)
    return payload
