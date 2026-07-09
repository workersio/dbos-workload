#!/usr/bin/env python3
"""Shared helpers for DBOS Workers IO workloads."""

from __future__ import annotations

import hashlib
import os
import secrets
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import quote


ROOT = Path(__file__).resolve().parents[2]
VENDOR_PY = ROOT / ".workers" / "vendor" / "py"
if VENDOR_PY.is_dir():
    sys.path.insert(0, str(VENDOR_PY))

RUN_DIR = Path(os.environ.get("DBOS_WORKLOAD_RUN_DIR", os.environ.get("TMPDIR", "/tmp"))) / (
    f"dbos-workload-{Path(sys.argv[0]).stem}"
)

WORKLOAD_SEED_RAW: str | None = None
PGPASSWORD = os.environ.get("PGPASSWORD", "dbos")
APP_DB = os.environ.get("DBOS_APP_DB", "dbostestpy")
SYS_DB = os.environ.get("DBOS_SYS_DB", f"{APP_DB}_dbos_sys")


class InvariantFailure(Exception):
    pass


def workload_seed_raw() -> str:
    global WORKLOAD_SEED_RAW
    if WORKLOAD_SEED_RAW is None:
        WORKLOAD_SEED_RAW = (
            os.environ.get("DBOS_WORKLOAD_SEED")
            or os.environ.get("WORKLOAD_SEED")
            or os.environ.get("WENV_SEED")
            or os.environ.get("FORMAL_SEED")
            or secrets.token_hex(16)
        )
    return WORKLOAD_SEED_RAW


def seed_int() -> int:
    digest = hashlib.sha256(f"dbos-workload:{workload_seed_raw()}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def progress(stage: str, detail: str = "") -> None:
    """Emit a human-visible line in wio workloads logs (guest stdout is streamed)."""
    suffix = f" {detail}" if detail else ""
    print(f"PROGRESS {stage}{suffix}", flush=True)


def invariant(id_: str, name: str, ok: bool, summary: str) -> None:
    status = "PASS" if ok else "FAIL"
    print(f"INVARIANT {id_} {name} {status} {summary}", flush=True)
    if not ok:
        raise InvariantFailure(f"{id_} {name}: {summary}")


def reset_run_dir() -> None:
    if RUN_DIR.exists():
        shutil.rmtree(RUN_DIR)
    RUN_DIR.mkdir(parents=True)


def pg_bindir() -> Path:
    for candidate in (
        Path("/usr/libexec/postgresql16"),
        Path("/usr/bin"),
    ):
        if (candidate / "initdb").exists() and (candidate / "postgres").exists():
            return candidate
    raise RuntimeError("postgres initdb/postgres not found; guest needs postgresql16 (see README guestProfile)")


def pg_root() -> Path:
    return Path("/tmp") / RUN_DIR.name


def start_postgres() -> None:
    bindir = pg_bindir()
    pg_base = pg_root()
    pgdata = pg_base / "pgdata"
    log_dir = RUN_DIR / "pglogs"
    pid_file = RUN_DIR / "postgres.pid"
    log_dir.mkdir(parents=True, exist_ok=True)

    if pg_base.exists():
        shutil.rmtree(pg_base)
    pg_base.mkdir(parents=True)

    stdout_log = log_dir / "postgres.stdout"
    stderr_log = log_dir / "postgres.stderr"
    initdb_log = log_dir / "initdb.log"
    socket_host = str(pg_base)

    start_script = f"""
set -eu
mkdir -p {shlex.quote(str(RUN_DIR))} {shlex.quote(str(log_dir))} {shlex.quote(str(pgdata))}
chown -R postgres:postgres {shlex.quote(str(pg_base))}
su postgres -c {shlex.quote(f"{bindir}/initdb -D {pgdata} -A trust --no-locale")} >{shlex.quote(str(initdb_log))} 2>&1
su postgres -c {shlex.quote(f"{bindir}/postgres -D {pgdata} -k {pg_base} -h 127.0.0.1 -p 5432")} \
  >>{shlex.quote(str(stdout_log))} 2>>{shlex.quote(str(stderr_log))} &
echo $! > {shlex.quote(str(pid_file))}
for i in $(seq 1 600); do
  if {shlex.quote(str(bindir / "pg_isready"))} -h {shlex.quote(socket_host)} -U postgres >/dev/null 2>&1; then
    exit 0
  fi
  sleep 0.1
done
exit 1
"""
    result = subprocess.run(["sh", "-c", start_script], text=True, capture_output=True)
    if result.returncode != 0:
        details = []
        for path in (initdb_log, stderr_log, stdout_log):
            if path.exists():
                details.append(f"{path.name}: {path.read_text()[-800:]}")
        raise RuntimeError(
            "postgres did not become ready; "
            + (" | ".join(details) if details else result.stderr.strip() or "no logs")
        )


def require_uuid_ossp() -> None:
    """DBOS system migrations need uuid-ossp from postgresql16-contrib in the guest image."""
    try:
        psql('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
    except RuntimeError as e:
        raise RuntimeError(
            "postgresql16-contrib is missing from the guest image (uuid-ossp unavailable). "
            "Do not use runtime apk add in deterministic VMs (no outbound network). "
            "Bake postgresql16-contrib with `wenv images build alpine` on your worker, "
            "then set the project guestProfile or copy artifacts into alpine-node. "
            "See dbos-workload README § Guest image."
        ) from e


def pgdata_dir() -> Path:
    return pg_root() / "pgdata"


def postgres_ready() -> bool:
    bindir = pg_bindir()
    proc = subprocess.run(
        [str(bindir / "pg_isready"), "-h", str(pg_root()), "-U", "postgres"],
        capture_output=True,
    )
    return proc.returncode == 0


def kill_postgres_hard(wait_sec: float = 30.0) -> int:
    """SIGKILL the postmaster to model a database crash. Returns the killed pid.

    Backends detect postmaster death and exit on their own; we only wait until
    the server stops answering pg_isready so the fault window is proven.
    """
    pid = int((pgdata_dir() / "postmaster.pid").read_text().splitlines()[0].strip())
    os.kill(pid, 9)
    deadline = time.monotonic() + wait_sec
    while time.monotonic() < deadline:
        if not postgres_ready():
            return pid
        time.sleep(0.1)
    raise RuntimeError(f"postgres still ready {wait_sec}s after SIGKILL of postmaster pid={pid}")


def restart_postgres(timeout_sec: float = 180.0) -> None:
    """Relaunch postgres on the existing pgdata after kill_postgres_hard.

    Postgres removes the stale postmaster.pid itself once the recorded pid is
    dead, but orphaned backends can briefly hold the old shared memory segment,
    so launching is retried until the deadline.
    """
    bindir = pg_bindir()
    pgdata = pgdata_dir()
    pg_base = pg_root()
    log_dir = RUN_DIR / "pglogs"
    pid_file = RUN_DIR / "postgres.pid"
    stdout_log = log_dir / "postgres.stdout"
    stderr_log = log_dir / "postgres.stderr"

    launch = f"""
set -eu
su postgres -c {shlex.quote(f"{bindir}/postgres -D {pgdata} -k {pg_base} -h 127.0.0.1 -p 5432")} \
  >>{shlex.quote(str(stdout_log))} 2>>{shlex.quote(str(stderr_log))} &
"""
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        subprocess.run(["sh", "-c", launch], text=True, capture_output=True)
        attempt_deadline = min(deadline, time.monotonic() + 15)
        while time.monotonic() < attempt_deadline:
            if postgres_ready():
                # Record the live postmaster pid (not the launch wrapper's $!):
                # a retried launch may lose the race to an earlier slow attempt,
                # and stop_postgres must kill whichever postmaster actually won.
                pid_file.write_text(
                    (pgdata / "postmaster.pid").read_text().splitlines()[0].strip()
                )
                return
            time.sleep(0.2)
    tail = stderr_log.read_text()[-800:] if stderr_log.exists() else "no stderr log"
    raise RuntimeError(f"postgres did not come back within {timeout_sec}s after restart; {tail}")


def stop_postgres() -> None:
    pid_file = RUN_DIR / "postgres.pid"
    if pid_file.exists():
        pid = pid_file.read_text().strip()
        subprocess.run(["sh", "-c", f"kill {shlex.quote(pid)} 2>/dev/null || true"], check=False)
    pg_base = pg_root()
    if pg_base.exists():
        shutil.rmtree(pg_base, ignore_errors=True)


def psql(sql: str, database: str = "postgres") -> str:
    bindir = pg_bindir()
    proc = subprocess.run(
        [
            str(bindir / "psql"),
            "-h",
            str(pg_root()),
            "-U",
            "postgres",
            "-d",
            database,
            "-v",
            "ON_ERROR_STOP=1",
            "-At",
        ],
        input=sql,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"psql failed rc={proc.returncode}\n{proc.stderr}\nSQL:\n{sql}")
    return proc.stdout.strip()


def ensure_databases() -> None:
    for db in (APP_DB, SYS_DB):
        exists = psql(
            "SELECT 1 FROM pg_database WHERE datname = "
            f"'{db.replace(chr(39), chr(39)+chr(39))}';"
        )
        if not exists:
            psql(f'CREATE DATABASE "{db}";')


def dbos_config() -> dict[str, object]:
    password = quote(PGPASSWORD, safe="")
    # initdb --no-locale on Alpine defaults to SQL_ASCII; psycopg3 then returns bytes
    # and SQLAlchemy's version probe fails unless client encoding is UTF-8.
    enc = "client_encoding=utf8"
    return {
        "name": "dbos-workload",
        "application_database_url": f"postgresql://postgres:{password}@127.0.0.1:5432/{APP_DB}?{enc}",
        "system_database_url": f"postgresql+psycopg://postgres:{password}@127.0.0.1:5432/{SYS_DB}?{enc}",
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        "scheduler_polling_interval_sec": 1,
    }


def subphase_env(workload_file: str, env: dict[str, str]) -> dict[str, str]:
    """Environment for a workload subphase subprocess (phase-gated orchestration)."""
    workloads_dir = Path(workload_file).resolve().parent
    return {
        **os.environ,
        **env,
        "DBOS_WORKLOAD_RUN_DIR": str(RUN_DIR.parent),
        "PYTHONPATH": os.pathsep.join([str(workloads_dir), str(VENDOR_PY)]),
    }


def vendor_ready() -> bool:
    if not VENDOR_PY.is_dir():
        return False
    try:
        import dbos  # noqa: F401
    except Exception:
        return False
    return True


def workload_main(name: str, scenario) -> int:
    reset_run_dir()
    if not vendor_ready():
        print(
            f"missing DBOS deps in {VENDOR_PY}; run .workers/build.sh during project prepare",
            file=sys.stderr,
        )
        return 2

    root_seed = seed_int()
    try:
        progress("postgres_start")
        start_postgres()
        progress("postgres_ready")
        require_uuid_ossp()
        progress("uuid_ossp_ok")
        ensure_databases()
        progress("scenario_start", name)
        scenario(root_seed)
        print(
            f"dbos workload complete workload_seed={workload_seed_raw()} "
            f"root_seed={root_seed} workload_name={name} dir={RUN_DIR}"
        )
        return 0
    except InvariantFailure as e:
        print(
            f"workload failed workload_seed={workload_seed_raw()} root_seed={root_seed} "
            f"workload_name={name}: {e}",
            file=sys.stderr,
        )
        return 1
    except Exception as e:
        print(
            f"workload error workload_seed={workload_seed_raw()} root_seed={root_seed} "
            f"workload_name={name}: {e}",
            file=sys.stderr,
        )
        return 1
    finally:
        stop_postgres()
