#!/bin/sh
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV="${DBOS_TARGET_VENV:-${ROOT}/.workers/vendor/dbos-venv}"
PYTHON="${DBOS_RUNTIME_PYTHON:-${VENV}/bin/python}"
SITE_PACKAGES="${DBOS_TARGET_SITE_PACKAGES:-}"
MUSL_SITE_PACKAGES="${ROOT}/.workers/vendor/musl-site-packages"
MUSL_LIBS="${ROOT}/.workers/vendor/musl-libs"

if [ ! -x "${PYTHON}" ]; then
  PYTHON="$(command -v python3 || true)"
fi

if [ -z "${PYTHON}" ] || [ ! -x "${PYTHON}" ]; then
  echo "setup-block: no runtime python3 executable found" >&2
  exit 42
fi

if [ -z "${SITE_PACKAGES}" ]; then
  SITE_PACKAGES="$(find "${VENV}/lib" -maxdepth 2 -type d -name site-packages 2>/dev/null | head -n 1 || true)"
fi

if [ -z "${SITE_PACKAGES}" ] || [ ! -d "${SITE_PACKAGES}" ]; then
  echo "setup-block: missing prepared DBOS site-packages under ${VENV}/lib" >&2
  echo "next: run WIO project prepare so .workers/build.sh installs dependencies" >&2
  exit 43
fi

export PYTHONPATH="${SITE_PACKAGES}${PYTHONPATH:+:${PYTHONPATH}}"
PY_EXT_SUFFIX="$("${PYTHON}" - <<'PY' 2>/dev/null || true
import sysconfig
print(sysconfig.get_config_var("EXT_SUFFIX") or "")
PY
)"
if printf '%s' "${PY_EXT_SUFFIX}" | grep -q 'linux-musl'; then
  if [ -d "${MUSL_SITE_PACKAGES}" ]; then
    export PYTHONPATH="${MUSL_SITE_PACKAGES}:${PYTHONPATH}"
  fi
  if [ -d "${MUSL_LIBS}" ]; then
    export LD_LIBRARY_PATH="${MUSL_LIBS}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
  fi
fi
exec "${PYTHON}" "$@"
