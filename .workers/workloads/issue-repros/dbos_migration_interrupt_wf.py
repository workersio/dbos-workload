# SURFACE: Migration interrupt (P8m)
# MODELS:  smoke workflow after interrupted DBOS.launch() cycles
# ORACLE:  completion marker in app DB
# ISSUES:  migration idempotency, startup recovery

from __future__ import annotations

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
def smoke_after_migration(run_id: str) -> str:
    with _app_engine().begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO migration_smoke(run_id) VALUES (:run_id) "
                "ON CONFLICT (run_id) DO NOTHING"
            ),
            {"run_id": run_id},
        )
    return run_id
