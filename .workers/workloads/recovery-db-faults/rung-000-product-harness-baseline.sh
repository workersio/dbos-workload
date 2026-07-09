#!/bin/sh
set -eu

# Frontier: recovery-db-faults
# Rung: rung-000-product-harness-baseline
# Product promise: Postgres-checkpointed DBOS workflows resume after failure.
# Oracle: the selected product recovery pytest reaches Postgres mode and exits zero.

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
TARGET_SRC="${DBOS_TARGET_SRC:-${ROOT}/.workers/vendor/dbos-transact-py}"
VENV="${DBOS_TARGET_VENV:-${ROOT}/.workers/vendor/dbos-venv}"
PYTHON="${DBOS_RUNTIME_PYTHON:-${ROOT}/.workers/python-runtime.sh}"
PYTEST_TARGET="${DBOS_BASELINE_PYTEST_TARGET:-tests/test_failures.py::test_recovery_during_retries}"

echo "WIO-EVENT frontier=recovery-db-faults rung=rung-000-product-harness-baseline phase=preflight"

if [ ! -x "${PYTHON}" ] || [ ! -d "${VENV}/lib/python3.12/site-packages" ]; then
  echo "INVARIANT dbos_baseline_prepared_python=false reason=missing-runtime"
  echo "setup-block: missing prepared Python runtime at ${PYTHON} or ${VENV}/lib/python3.12/site-packages" >&2
  echo "next: run WIO project prepare so .workers/build.sh creates the venv" >&2
  exit 42
fi

if [ ! -f "${TARGET_SRC}/tests/test_failures.py" ]; then
  echo "INVARIANT dbos_baseline_target_checkout=false reason=missing-target-src"
  echo "setup-block: missing vendored dbos-transact-py checkout at ${TARGET_SRC}" >&2
  echo "next: run WIO project prepare so .workers/build.sh fetches DBOS_TARGET_REF" >&2
  exit 43
fi

echo "INVARIANT dbos_baseline_prepared_python=true"
echo "INVARIANT dbos_baseline_target_checkout=true"

export PGPASSWORD="${PGPASSWORD:-dbos}"

if ! "${PYTHON}" - <<'PY'
import os
import psycopg

password = os.environ.get("PGPASSWORD", "dbos")
try:
    with psycopg.connect(
        f"postgresql://postgres:{password}@localhost:5432/postgres",
        connect_timeout=5,
    ):
        pass
except Exception as exc:
    print(f"setup-block: postgres localhost:5432 is not ready: {exc}")
    raise SystemExit(44)
PY
then
  if [ "${WIO_DBOS_START_POSTGRES:-0}" = "1" ] && command -v docker >/dev/null 2>&1; then
    "${PYTHON}" "${TARGET_SRC}/dbos/_templates/dbos-db-starter/start_postgres_docker.py"
  else
    echo "INVARIANT dbos_baseline_postgres_ready=false reason=postgres-unavailable"
    echo "next: provide an owned cloud Postgres service or rerun with WIO_DBOS_START_POSTGRES=1 where docker is available" >&2
    exit 44
  fi
fi

echo "INVARIANT dbos_baseline_postgres_ready=true"
echo "WIO-EVENT frontier=recovery-db-faults rung=rung-000-product-harness-baseline phase=pytest target=${PYTEST_TARGET}"
cd "${TARGET_SRC}"
"${PYTHON}" -m pytest "${PYTEST_TARGET}" -q
echo "INVARIANT dbos_baseline_product_recovery_pytest=true"
