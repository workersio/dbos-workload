#!/usr/bin/env python3
"""
P6: startup storm — many DBOS workers initialize concurrently.

# SURFACE: dependency import, DBOS import/startup, migration, queue listener initialization
# MODELS:  N subprocess workers importing a target or reaching DBOS.launch together
# ORACLE:  every worker reaches ready, exits cleanly, and startup latency stays bounded
# ISSUES:  concurrent import/pycache stalls, heavy eager imports, startup liveness
# VARIANCE: worker count and startup mode from WORKLOAD_SEED / STARTUP_MODE
"""

from __future__ import annotations

import argparse
import faulthandler
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from dbos_workload_common import (
    RUN_DIR,
    dbos_config,
    invariant,
    progress,
    seed_int,
    workload_main,
)

NO_DEQUEUE = "__formal_no_dequeue__"
READY_DIR = RUN_DIR / "ready"
META_PATH = RUN_DIR / "startup-meta.json"

faulthandler.enable()
try:
    faulthandler.register(signal.SIGUSR1, all_threads=True)
except (AttributeError, RuntimeError, ValueError):
    pass


def startup_mode(root_seed: int) -> str:
    override = os.environ.get("STARTUP_MODE")
    if override:
        return override
    return ("normal", "no_pyc", "prewarm_import")[root_seed % 3]


def worker_count(root_seed: int) -> int:
    override = os.environ.get("STARTUP_WORKERS")
    if override:
        return max(1, int(override))
    return 2 + (root_seed % 4)


def startup_target() -> str:
    return os.environ.get("STARTUP_TARGET", "dbos_launch")


def ready_path(worker_id: str) -> Path:
    return READY_DIR / f"{worker_id}.ready"


def worker_env(base_env: dict[str, str], mode: str) -> dict[str, str]:
    env = {
        **os.environ,
        **base_env,
        "PYTHONUNBUFFERED": "1",
        "DBOS_WORKLOAD_RUN_DIR": str(RUN_DIR.parent),
        "PYTHONPATH": os.pathsep.join(
            [
                str(Path(__file__).resolve().parent),
                str(Path(__file__).resolve().parents[2] / ".workers" / "vendor" / "py"),
            ]
        ),
    }
    if mode == "no_pyc":
        env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def run_worker_phase(worker_id: str) -> int:
    target = startup_target()
    progress("startup_worker_target_start", f"{worker_id} target={target}")

    if target == "psycopg":
        progress("startup_worker_import_start", f"{worker_id} module=psycopg")
        import psycopg  # noqa: F401

        progress("startup_worker_import_done", f"{worker_id} module=psycopg")
    elif target == "sqlalchemy":
        progress("startup_worker_import_start", f"{worker_id} module=sqlalchemy")
        import sqlalchemy  # noqa: F401
        import sqlalchemy.ext.asyncio  # noqa: F401

        progress("startup_worker_import_done", f"{worker_id} module=sqlalchemy")
    elif target == "dbos_import":
        progress("startup_worker_import_start", f"{worker_id} module=dbos")
        import dbos  # noqa: F401

        progress("startup_worker_import_done", f"{worker_id} module=dbos")
    elif target == "dbos_launch":
        progress("startup_worker_import_start", f"{worker_id} module=dbos")
        from dbos import DBOS

        progress("startup_worker_import_done", f"{worker_id} module=dbos")
        DBOS.destroy(destroy_registry=True)
        DBOS(config=dbos_config())
        DBOS.listen_queues([NO_DEQUEUE])
        progress("startup_worker_launch_start", worker_id)
        DBOS.launch()
    else:
        raise RuntimeError(f"unknown STARTUP_TARGET={target!r}")

    progress("startup_worker_ready", worker_id)
    ready_path(worker_id).write_text("ready")
    time.sleep(1)
    if target == "dbos_launch":
        DBOS.destroy(destroy_registry=True)
    progress("startup_worker_done", worker_id)
    return 0


def spawn_worker(
    worker_id: str, mode: str, base_env: dict[str, str]
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, __file__, "--worker-id", worker_id],
        env=worker_env(base_env, mode),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def dump_and_kill(worker_id: str, proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    progress("startup_worker_timeout", f"{worker_id} dumping_stack")
    try:
        proc.send_signal(signal.SIGUSR1)
        time.sleep(1)
    except ProcessLookupError:
        pass
    proc.kill()
    proc.wait()


def run_prewarm(mode: str, base_env: dict[str, str]) -> None:
    if mode != "prewarm_import":
        return
    progress("startup_prewarm_start")
    proc = spawn_worker("prewarm", "normal", base_env)
    assert proc.stdout is not None
    for line in proc.stdout:
        print(f"[prewarm] {line}", end="", flush=True)
    rc = proc.wait(timeout=240)
    progress("startup_prewarm_done", f"rc={rc}")
    if rc != 0:
        raise RuntimeError(f"prewarm failed rc={rc}")


def scenario_startup_storm(root_seed: int) -> None:
    mode = startup_mode(root_seed)
    target = startup_target()
    count = worker_count(root_seed)
    READY_DIR.mkdir(parents=True, exist_ok=True)
    for path in READY_DIR.glob("*.ready"):
        path.unlink()
    META_PATH.write_text(json.dumps({"mode": mode, "target": target, "workers": count}))

    base_env = {"STARTUP_MODE": mode, "STARTUP_TARGET": target}
    progress("startup_storm_config", f"mode={mode} target={target} workers={count}")
    run_prewarm(mode, base_env)
    for path in READY_DIR.glob("*.ready"):
        path.unlink()

    procs: dict[str, subprocess.Popen[str]] = {}
    captured: dict[str, list[str]] = {}

    def drain(worker_id: str) -> None:
        proc = procs[worker_id]
        assert proc.stdout is not None
        for line in proc.stdout:
            captured[worker_id].append(line)
            print(f"[{worker_id}] {line}", end="", flush=True)

    start = time.monotonic()
    threads: list[threading.Thread] = []
    for index in range(count):
        worker_id = f"startup-{index:02d}"
        captured[worker_id] = []
        progress("startup_worker_spawn", worker_id)
        procs[worker_id] = spawn_worker(worker_id, mode, base_env)
        thread = threading.Thread(target=drain, args=(worker_id,))
        thread.start()
        threads.append(thread)

    ready_deadline = time.monotonic() + 300
    ready_elapsed = 300.0
    while time.monotonic() < ready_deadline:
        ready = sorted(path.stem for path in READY_DIR.glob("*.ready"))
        if len(ready) >= count:
            ready_elapsed = time.monotonic() - start
            break
        if all(proc.poll() is not None for proc in procs.values()):
            ready_elapsed = time.monotonic() - start
            break
        time.sleep(0.25)

    ready = sorted(path.stem for path in READY_DIR.glob("*.ready"))
    for worker_id, proc in procs.items():
        if worker_id not in ready:
            dump_and_kill(worker_id, proc)

    results: dict[str, int] = {}
    for worker_id, proc in procs.items():
        try:
            rc = proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            dump_and_kill(worker_id, proc)
            rc = proc.returncode or -9
        results[worker_id] = rc

    for thread in threads:
        thread.join(timeout=5)

    elapsed = time.monotonic() - start
    ready_count = len([worker_id for worker_id in procs if ready_path(worker_id).exists()])
    failures = {worker_id: rc for worker_id, rc in results.items() if rc != 0}

    invariant(
        "S1",
        "all_workers_ready",
        ready_count == count,
        f"ready={ready_count} expected={count} mode={mode} target={target} results={results}",
    )
    invariant(
        "S2",
        "workers_exit_cleanly",
        not failures,
        f"failures={failures} mode={mode} target={target}",
    )
    invariant(
        "S3",
        "startup_latency_bounded",
        ready_elapsed <= 90,
        f"ready_elapsed_sec={ready_elapsed:.1f} total_elapsed_sec={elapsed:.1f} workers={count} mode={mode} target={target}",
    )
    progress(
        "startup_storm_done",
        f"workers={count} mode={mode} target={target} elapsed_sec={elapsed:.1f}",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", default="")
    args = parser.parse_args()

    if args.worker_id:
        return run_worker_phase(args.worker_id)

    return workload_main("dbos_startup_storm", scenario_startup_storm)


if __name__ == "__main__":
    raise SystemExit(main())
