#!/bin/sh
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET_REPO_URL="${DBOS_TARGET_REPO_URL:-}"
TARGET_REF="${DBOS_TARGET_REF:-9922c1d0639a9899760e7232e7fe47ded44eea83}"
TARGET_VERSION="${DBOS_TARGET_VERSION:-0.0.0+$(printf '%s' "${TARGET_REF}" | cut -c1-12)}"
TARGET_SRC="${ROOT}/.workers/vendor/dbos-transact-py"
VENV="${ROOT}/.workers/vendor/dbos-venv"
TMP="${ROOT}/.workers/tmp"
MUSL_WHEEL_DIR="${ROOT}/.workers/vendor/musl-wheels"
MUSL_SITE="${ROOT}/.workers/vendor/musl-site-packages"
KAFKA_BROKER_SRC="${ROOT}/.workers/kafka-broker"
KAFKA_BROKER_BIN="${ROOT}/.workers/vendor/bin/wio-kafka-broker-linux-amd64"

mkdir -p "${ROOT}/.workers/vendor" "${ROOT}/.workers/vendor/bin" "${TMP}"

if [ "${WIO_BUILD_LOG_CAPTURED:-0}" != "1" ]; then
  BUILD_STDOUT="${TMP}/build.stdout.log"
  BUILD_STDERR="${TMP}/build.stderr.log"
  rm -f "${BUILD_STDOUT}" "${BUILD_STDERR}"
  if WIO_BUILD_LOG_CAPTURED=1 sh "$0" "$@" >"${BUILD_STDOUT}" 2>"${BUILD_STDERR}"; then
    cat "${BUILD_STDOUT}"
    cat "${BUILD_STDERR}" >&2
    exit 0
  else
    status=$?
    cat "${BUILD_STDOUT}" >&2
    cat "${BUILD_STDERR}" >&2
    exit "${status}"
  fi
fi

rm -rf "${TARGET_SRC}"

UV_BIN=""
ensure_python() {
  if command -v python3 >/dev/null 2>&1 && python3 - <<'PY' >/dev/null 2>&1; then
import ensurepip
import venv
PY
    PYTHON_BOOTSTRAP="python3"
    return
  fi

  UV_VERSION="${UV_VERSION:-0.7.13}"
  case "$(uname -m)" in
    x86_64 | amd64) UV_ARCH="x86_64-unknown-linux-gnu" ;;
    aarch64 | arm64) UV_ARCH="aarch64-unknown-linux-gnu" ;;
    *)
      echo "unsupported architecture for Python bootstrap: $(uname -m)" >&2
      exit 1
      ;;
  esac

  UV_URL="https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-${UV_ARCH}.tar.gz"
  curl -fsSL --retry 3 -o "${TMP}/uv.tar.gz" "${UV_URL}"
  tar -C "${TMP}" -xzf "${TMP}/uv.tar.gz"
  UV_BIN="${TMP}/uv-${UV_ARCH}/uv"
  if [ ! -x "${UV_BIN}" ]; then
    echo "uv binary missing after extract" >&2
    find "${TMP}" -maxdepth 2 -type f >&2
    exit 1
  fi

  export UV_CACHE_DIR="${TMP}/uv-cache"
  export UV_PYTHON_INSTALL_DIR="${TMP}/python"
  export UV_PYTHON_DOWNLOADS=true
  "${UV_BIN}" python install 3.12
  PYTHON_BOOTSTRAP="$("${UV_BIN}" python find 3.12)"
}

prepare_kafka_broker() {
  if command -v go >/dev/null 2>&1; then
    (
      cd "${KAFKA_BROKER_SRC}"
      GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build \
        -trimpath \
        -ldflags="-s -w" \
        -o "${KAFKA_BROKER_BIN}" \
        .
    )
  fi

  if [ ! -x "${KAFKA_BROKER_BIN}" ]; then
    echo "missing executable Kafka broker artifact: ${KAFKA_BROKER_BIN}" >&2
    exit 1
  fi
  "${VENV}/bin/python" - <<PY
from pathlib import Path
path = Path("${KAFKA_BROKER_BIN}")
print("prepared kafka broker:", path, path.stat().st_size)
PY
}

# Vendor the target source. This repo IS a fork of dbos-transact-py (main =
# upstream ${TARGET_REF} + .workers/ on top), so default to copying the repo
# root locally — the build host's anonymous git fetches to GitHub get
# throttled (authenticated repo clone works; unauthenticated vendor fetch
# fast-fails). Set DBOS_TARGET_REPO_URL to force a network fetch of a
# different repo/ref instead.
if [ -n "${TARGET_REPO_URL}" ]; then
  git init "${TARGET_SRC}"
  git -C "${TARGET_SRC}" remote add origin "${TARGET_REPO_URL}"
  git -C "${TARGET_SRC}" fetch --depth 1 origin "${TARGET_REF}"
  git -C "${TARGET_SRC}" checkout --detach FETCH_HEAD
else
  mkdir -p "${TARGET_SRC}"
  (cd "${ROOT}" && tar --exclude ./.workers --exclude ./.git -cf - .) \
    | tar -C "${TARGET_SRC}" -xf -
fi

ensure_python
rm -rf "${VENV}"
if [ -n "${UV_BIN}" ]; then
  "${UV_BIN}" venv --seed --python "${PYTHON_BOOTSTRAP}" "${VENV}"
else
  "${PYTHON_BOOTSTRAP}" -m venv "${VENV}"
fi

"${VENV}/bin/python" -m pip install --upgrade pip
export PDM_BUILD_SCM_VERSION="${TARGET_VERSION}"
"${VENV}/bin/python" -m pip install \
  "${TARGET_SRC}[validation]" \
  "pytest>=8.3.3" \
  "pytest-asyncio>=0.25.0" \
  "pytest-mock>=3.14.0" \
  "pytest-order>=1.3.0" \
  "pytest-timeout>=2.3.1" \
  "fastapi>=0.121.0" \
  "uvicorn>=0.35.0" \
  "pydantic>=2.0" \
  "flask>=3.0.3" \
  "opentelemetry-sdk>=1.37.0" \
  "requests>=2.32.3" \
  "pytz>=2024.2" \
  "greenlet>=3.2.4"

if "${VENV}/bin/python" -m pip install "confluent-kafka==2.6.1"; then
  PREPARED_CONFLUENT_KAFKA=1
else
  PREPARED_CONFLUENT_KAFKA=0
  if [ "$(uname -s)" = "Linux" ]; then
    echo "failed to install confluent-kafka==2.6.1 on Linux preparation host" >&2
    exit 1
  fi
  echo "warning: skipped host confluent-kafka install on non-Linux preparation host" >&2
fi

rm -rf "${MUSL_SITE}"
mkdir -p "${MUSL_SITE}"
"${VENV}/bin/python" - <<PY
from pathlib import Path
import zipfile

wheel_dir = Path("${MUSL_WHEEL_DIR}")
target = Path("${MUSL_SITE}")
wheels = sorted(wheel_dir.glob("confluent_kafka-*.whl"))
if not wheels:
    raise SystemExit(f"missing musl confluent-kafka wheel in {wheel_dir}")
with zipfile.ZipFile(wheels[0]) as zf:
    zf.extractall(target)
print("prepared musl confluent_kafka override:", wheels[0])
PY

PYDANTIC_CORE_VERSION="$("${VENV}/bin/python" - <<'PY'
import pydantic_core

print(pydantic_core.__version__)
PY
)"
PYTHON_TAG="$("${VENV}/bin/python" - <<'PY'
import sys

print(f"cp{sys.version_info.major}{sys.version_info.minor}")
PY
)"
rm -rf "${TMP}/pydantic-core-musl"
mkdir -p "${TMP}/pydantic-core-musl"
"${VENV}/bin/python" -m pip download \
  --only-binary=:all: \
  --platform musllinux_1_1_x86_64 \
  --python-version "${PYTHON_TAG#cp}" \
  --implementation cp \
  --abi "${PYTHON_TAG}" \
  --dest "${TMP}/pydantic-core-musl" \
  "pydantic-core==${PYDANTIC_CORE_VERSION}"
"${VENV}/bin/python" - <<PY
from pathlib import Path
import zipfile

wheel_dir = Path("${TMP}/pydantic-core-musl")
target = Path("${MUSL_SITE}")
wheels = sorted(wheel_dir.glob("pydantic_core-*.whl"))
if not wheels:
    raise SystemExit(f"missing musl pydantic-core wheel in {wheel_dir}")
with zipfile.ZipFile(wheels[0]) as zf:
    zf.extractall(target)
print("prepared musl pydantic_core override:", wheels[0])
PY

GREENLET_VERSION="$("${VENV}/bin/python" - <<'PY'
import greenlet

print(greenlet.__version__)
PY
)"
rm -rf "${TMP}/greenlet-musl"
mkdir -p "${TMP}/greenlet-musl"
"${VENV}/bin/python" -m pip download \
  --only-binary=:all: \
  --platform musllinux_1_2_x86_64 \
  --python-version "${PYTHON_TAG#cp}" \
  --implementation cp \
  --abi "${PYTHON_TAG}" \
  --dest "${TMP}/greenlet-musl" \
  "greenlet==${GREENLET_VERSION}"
"${VENV}/bin/python" - <<PY
from pathlib import Path
import zipfile

wheel_dir = Path("${TMP}/greenlet-musl")
target = Path("${MUSL_SITE}")
wheels = sorted(wheel_dir.glob("greenlet-*.whl"))
if not wheels:
    raise SystemExit(f"missing musl greenlet wheel in {wheel_dir}")
with zipfile.ZipFile(wheels[0]) as zf:
    zf.extractall(target)
print("prepared musl greenlet override:", wheels[0])
PY

"${VENV}/bin/python" - <<'PY'
import dbos

print("prepared dbos package from target commit:", dbos.__file__)
PY

if [ "${PREPARED_CONFLUENT_KAFKA}" = "1" ]; then
  "${VENV}/bin/python" - <<'PY'
import confluent_kafka
from confluent_kafka import Consumer, Producer

print("prepared confluent_kafka package:", confluent_kafka.__file__)
print("prepared confluent_kafka producer/consumer:", Producer, Consumer)
PY
fi

"${VENV}/bin/python" - <<PY
from pathlib import Path
import sysconfig
import zipfile

wheel_dir = Path("${TMP}/greenlet-musl")
target = Path(sysconfig.get_paths()["purelib"])
wheels = sorted(wheel_dir.glob("greenlet-*.whl"))
if not wheels:
    raise SystemExit(f"missing musl greenlet wheel in {wheel_dir}")
with zipfile.ZipFile(wheels[0]) as zf:
    zf.extractall(target)
print("prepared runtime greenlet override:", wheels[0], "->", target)
PY

prepare_kafka_broker
