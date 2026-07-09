#!/usr/bin/env python3
"""DBOS CLI starter onboarding workload.

This workload exercises the generated starter as a developer/operator session:
initialize from the package-local starter template, run DBOS migrations against
real Postgres, start the generated app through `dbos start`, and verify app-level
HTTP behavior plus durable database state.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
import sqlalchemy as sa
import yaml
from sqlalchemy import make_url, text


FRONTIER_ID = "cli-starter-onboarding"
RUNG_000_ID = "rung-000-starter-init-migrate-start"
RUNG_001_ID = "rung-001-config-env-secrets"
RUNG_002_ID = "rung-002-postgres-cloud-config"
RUNG_003_ID = "rung-003-template-matrix"
PROMPT_PATH = "evidence-key:events/frontier_designer-20260620T072715949416000Z.prompt.md"
DEFAULT_TEMPLATE = "dbos-db-starter"
RUNG_003_VARIANTS = [
    "template-python-fastapi",
    "template-flask",
    "env-secret-file",
    "postgres-url-local",
    "cloud-config-override",
    "rerun-migrate-start",
]
DEFAULT_COMMAND_TIMEOUT_SECONDS = 300
INIT_COMMAND_TIMEOUT_SECONDS = 720
MIGRATE_COMMAND_TIMEOUT_SECONDS = 1200
START_SMOKE_TIMEOUT_SECONDS = 1800


class SetupBlock(Exception):
    pass


class Finding(Exception):
    pass


@dataclass(frozen=True)
class CasePlan:
    rung_id: str
    case_id: str
    seed: int
    project_name: str
    package_name: str
    template: str
    schedule: str
    greeting_name: str
    config_database_url_token: str = "${DBOS_DATABASE_URL}"
    migrate_from_config: bool = False
    expected_migrate_success: bool = True
    expect_start: bool = True
    docker_secret_name: str | None = None
    missing_secret_name: str | None = None
    config_system_database_url_token: str | None = None
    migrate_with_cli_urls: bool = False
    rerun_start_count: int = 1
    variant: str = "baseline"
    setup_block_reason: str | None = None


def emit(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, sort_keys=True), flush=True)


def invariant(name: str, ok: bool, summary: str, **details: Any) -> None:
    status = "PASS" if ok else "FAIL"
    payload = {"summary": summary, **details}
    print(f"INVARIANT {name} {name} {status} {json.dumps(payload, sort_keys=True)}", flush=True)
    if not ok:
        raise Finding(f"{name}: {summary}")


def redact_url(url: str) -> str:
    try:
        parsed = make_url(url)
        return parsed.render_as_string(hide_password=True)
    except Exception:
        return "<unparseable-url>"


def redact_text(value: str, secrets: list[str]) -> str:
    redacted = value
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "<redacted>")
            redacted = redacted.replace(redact_url(secret), redact_url(secret))
    return redacted


def redact_argv(argv: list[str], secrets: list[str]) -> list[str]:
    return [redact_text(arg, secrets) for arg in argv]


def timeout_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def file_tail(path: Path, limit: int = 12_000) -> str:
    if not path.exists():
        return ""
    content = path.read_text(encoding="utf-8", errors="replace")
    if len(content) <= limit:
        return content
    return content[-limit:]


def redact_file(path: Path, secrets: list[str]) -> None:
    if not path.exists():
        return
    content = path.read_text(encoding="utf-8", errors="replace")
    path.write_text(redact_text(content, secrets), encoding="utf-8")


def artifact_tree_contains_secret(root: Path, secrets: list[str]) -> bool:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        content = path.read_text(encoding="utf-8", errors="ignore")
        if any(secret and secret in content for secret in secrets):
            return True
    return False


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def normalize_rung(rung: str) -> str:
    aliases = {
        "rung-000": RUNG_000_ID,
        RUNG_000_ID: RUNG_000_ID,
        "rung-001": RUNG_001_ID,
        RUNG_001_ID: RUNG_001_ID,
        "rung-002": RUNG_002_ID,
        RUNG_002_ID: RUNG_002_ID,
        "rung-003": RUNG_003_ID,
        RUNG_003_ID: RUNG_003_ID,
    }
    if rung not in aliases:
        raise SetupBlock(f"unsupported rung {rung}; this workload implements rungs 000-003")
    return aliases[rung]


def case_ids_for_rung(rung_id: str) -> list[str]:
    if rung_id == RUNG_000_ID:
        return ["case-001"]
    if rung_id == RUNG_001_ID:
        return ["case-001", "case-002", "case-003"]
    if rung_id == RUNG_002_ID:
        return ["case-001", "case-002", "case-003"]
    if rung_id == RUNG_003_ID:
        return [f"case-{idx:03d}" for idx in range(1, 25)]
    raise SetupBlock(f"unsupported rung {rung_id}")


def build_case_plan(rung_id: str, case_id: str) -> CasePlan:
    seeds = {
        (RUNG_000_ID, "case-001"): 3600,
        (RUNG_001_ID, "case-001"): 3610,
        (RUNG_001_ID, "case-002"): 3611,
        (RUNG_001_ID, "case-003"): 3612,
        (RUNG_002_ID, "case-001"): 3620,
        (RUNG_002_ID, "case-002"): 3621,
        (RUNG_002_ID, "case-003"): 3622,
    }
    if rung_id == RUNG_003_ID:
        valid_case_ids = case_ids_for_rung(rung_id)
        if case_id not in valid_case_ids:
            raise SetupBlock(f"unknown case {case_id} for {rung_id}")
        case_index = valid_case_ids.index(case_id)
        seed = 3630 + case_index
        variant = RUNG_003_VARIANTS[case_index % len(RUNG_003_VARIANTS)]
        project_name = f"wio-cli-{seed}"
        base = CasePlan(
            rung_id=rung_id,
            case_id=case_id,
            seed=seed,
            project_name=project_name,
            package_name=project_name.replace("-", "_"),
            template=DEFAULT_TEMPLATE,
            schedule=f"bounded-{variant}-variant",
            greeting_name=f"{variant.replace('-', '_')}-{seed}",
            variant=variant,
        )
        if variant == "template-python-fastapi":
            return base
        if variant == "template-flask":
            return CasePlan(
                **{**asdict(base), "setup_block_reason": "dbos-transact-py exposes no local Flask starter template; local package templates only include dbos-db-starter"}
            )
        if variant == "env-secret-file":
            secret_name = f"wio_dbos_database_url_{seed}"
            return CasePlan(
                **{
                    **asdict(base),
                    "config_database_url_token": f"${{DOCKER_SECRET:{secret_name}}}",
                    "migrate_from_config": True,
                    "docker_secret_name": secret_name,
                }
            )
        if variant == "postgres-url-local":
            return CasePlan(**{**asdict(base), "migrate_with_cli_urls": True})
        if variant == "cloud-config-override":
            return CasePlan(
                **{
                    **asdict(base),
                    "config_database_url_token": "postgresql+psycopg://postgres@127.0.0.1:1/wio_cli_bogus_app",
                    "config_system_database_url_token": "postgresql+psycopg://postgres@127.0.0.1:1/wio_cli_bogus_sys",
                    "migrate_with_cli_urls": True,
                }
            )
        if variant == "rerun-migrate-start":
            return CasePlan(**{**asdict(base), "rerun_start_count": 2})
        raise SetupBlock(f"unhandled rung 003 variant {variant}")

    key = (rung_id, case_id)
    if key not in seeds:
        raise SetupBlock(f"unknown case {case_id} for {rung_id}")
    seed = seeds[key]
    project_name = f"wio-cli-{seed}"
    if rung_id == RUNG_001_ID and case_id == "case-001":
        return CasePlan(
            rung_id=rung_id,
            case_id=case_id,
            seed=seed,
            project_name=project_name,
            package_name=project_name.replace("-", "_"),
            template=DEFAULT_TEMPLATE,
            schedule="set-dbos-database-url-and-app-env-before-migrate-start",
            greeting_name=f"env-{seed}",
            migrate_from_config=True,
        )
    if rung_id == RUNG_001_ID and case_id == "case-002":
        secret_name = f"wio_dbos_database_url_{seed}"
        return CasePlan(
            rung_id=rung_id,
            case_id=case_id,
            seed=seed,
            project_name=project_name,
            package_name=project_name.replace("-", "_"),
            template=DEFAULT_TEMPLATE,
            schedule="provide-url-through-docker-secret-file",
            greeting_name=f"secret-{seed}",
            config_database_url_token=f"${{DOCKER_SECRET:{secret_name}}}",
            migrate_from_config=True,
            docker_secret_name=secret_name,
        )
    if rung_id == RUNG_001_ID and case_id == "case-003":
        secret_name = f"wio_missing_database_url_{seed}"
        return CasePlan(
            rung_id=rung_id,
            case_id=case_id,
            seed=seed,
            project_name=project_name,
            package_name=project_name.replace("-", "_"),
            template=DEFAULT_TEMPLATE,
            schedule="point-config-at-missing-secret-file",
            greeting_name=f"missing-{seed}",
            config_database_url_token=f"${{DOCKER_SECRET:{secret_name}}}",
            migrate_from_config=True,
            expected_migrate_success=False,
            expect_start=False,
            missing_secret_name=secret_name,
        )
    if rung_id == RUNG_002_ID and case_id == "case-001":
        return CasePlan(
            rung_id=rung_id,
            case_id=case_id,
            seed=seed,
            project_name=project_name,
            package_name=project_name.replace("-", "_"),
            template=DEFAULT_TEMPLATE,
            schedule="run-local-postgres-url-plus-cloud-style-override-fields",
            greeting_name=f"pg-cloud-{seed}",
            migrate_from_config=True,
            config_system_database_url_token="${DBOS_SYSTEM_DATABASE_URL}",
            variant="postgres-cloud-config",
        )
    if rung_id == RUNG_002_ID and case_id == "case-002":
        return CasePlan(
            rung_id=rung_id,
            case_id=case_id,
            seed=seed,
            project_name=project_name,
            package_name=project_name.replace("-", "_"),
            template=DEFAULT_TEMPLATE,
            schedule="apply-cloud-config-values-then-local-env-overrides",
            greeting_name=f"override-{seed}",
            config_database_url_token="postgresql+psycopg://postgres@127.0.0.1:1/wio_cli_bogus_app",
            config_system_database_url_token="postgresql+psycopg://postgres@127.0.0.1:1/wio_cli_bogus_sys",
            migrate_with_cli_urls=True,
            variant="cloud-config-override",
        )
    if rung_id == RUNG_002_ID and case_id == "case-003":
        return CasePlan(
            rung_id=rung_id,
            case_id=case_id,
            seed=seed,
            project_name=project_name,
            package_name=project_name.replace("-", "_"),
            template=DEFAULT_TEMPLATE,
            schedule="run-migrate-start-twice-on-same-generated-app",
            greeting_name=f"rerun-{seed}",
            rerun_start_count=2,
            variant="rerun-migrate-start",
        )
    return CasePlan(
        rung_id=rung_id,
        case_id=case_id,
        seed=seed,
        project_name=project_name,
        package_name=project_name.replace("-", "_"),
        template=DEFAULT_TEMPLATE,
        schedule="run-init-dependency-install-migrate-and-start-in-temp-project",
        greeting_name=f"seed-{seed}",
    )


def admin_url_from_env() -> str:
    explicit = os.environ.get("DBOS_POSTGRES_ADMIN_URL")
    if explicit:
        return explicit
    user = os.environ.get("PGUSER", "postgres")
    password = os.environ.get("PGPASSWORD", "dbos")
    host = os.environ.get("PGHOST", "127.0.0.1")
    port = os.environ.get("PGPORT", "5432")
    database = os.environ.get("PGDATABASE", "postgres")
    return f"postgresql+psycopg://{quote(user)}:{quote(password)}@{host}:{port}/{database}"


def db_url(admin_url: str, database: str) -> str:
    return make_url(admin_url).set(database=database).render_as_string(hide_password=False)


def ensure_postgres(admin_url: str) -> sa.Engine:
    try:
        engine = sa.create_engine(admin_url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as exc:
        raise SetupBlock(f"postgres setup failed: {type(exc).__name__}: {exc}") from exc


def recreate_database(engine: sa.Engine, database: str) -> None:
    safe_name = '"' + database.replace('"', '""') + '"'
    with engine.connect() as conn:
        conn.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = :database AND pid <> pg_backend_pid()"
            ),
            {"database": database},
        )
        conn.execute(text(f"DROP DATABASE IF EXISTS {safe_name} WITH (FORCE)"))
        conn.execute(text(f"CREATE DATABASE {safe_name}"))


def command_env(app_url: str, sys_url: str, project_dir: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["DBOS_DATABASE_URL"] = app_url
    env["DBOS_SYSTEM_DATABASE_URL"] = sys_url
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONPATH"] = os.pathsep.join(
        [str(project_dir), *(p for p in [env.get("PYTHONPATH", "")] if p)]
    )
    venv_bin = Path(sys.executable).parent
    env["PATH"] = str(venv_bin) + os.pathsep + env.get("PATH", "")
    return env


def run_command(
    name: str,
    argv: list[str],
    cwd: Path,
    env: dict[str, str],
    artifacts: Path,
    secrets: list[str],
    timeout: float = DEFAULT_COMMAND_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    display_argv = redact_argv(argv, secrets)
    emit("command_start", name=name, argv=display_argv, cwd=str(cwd))
    started = time.time()
    try:
        result = subprocess.run(
            argv,
            cwd=cwd,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        elapsed = time.time() - started
        record = {
            "name": name,
            "argv": display_argv,
            "cwd": str(cwd),
            "returncode": result.returncode,
            "elapsed_seconds": elapsed,
            "stdout": redact_text(result.stdout, secrets),
            "stderr": redact_text(result.stderr, secrets),
        }
    except subprocess.TimeoutExpired as exc:
        elapsed = time.time() - started
        record = {
            "name": name,
            "argv": display_argv,
            "cwd": str(cwd),
            "returncode": 124,
            "elapsed_seconds": elapsed,
            "timeout_seconds": timeout,
            "stdout": redact_text(timeout_output(exc.stdout), secrets),
            "stderr": redact_text(timeout_output(exc.stderr), secrets),
        }
        write_json(artifacts / "commands" / f"{name}.json", record)
        emit("command_timeout", name=name, timeout_seconds=timeout, elapsed_seconds=elapsed)
        raise Finding(f"{name} timed out after {timeout} seconds")
    write_json(artifacts / "commands" / f"{name}.json", record)
    emit("command_done", name=name, returncode=result.returncode, elapsed_seconds=elapsed)
    if result.returncode != 0:
        raise Finding(f"{name} exited {result.returncode}")
    return record


def run_maybe_failing_command(
    name: str,
    argv: list[str],
    cwd: Path,
    env: dict[str, str],
    artifacts: Path,
    secrets: list[str],
    timeout: float = DEFAULT_COMMAND_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    display_argv = redact_argv(argv, secrets)
    emit("command_start", name=name, argv=display_argv, cwd=str(cwd))
    started = time.time()
    try:
        result = subprocess.run(
            argv,
            cwd=cwd,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        elapsed = time.time() - started
        record = {
            "name": name,
            "argv": display_argv,
            "cwd": str(cwd),
            "returncode": result.returncode,
            "elapsed_seconds": elapsed,
            "stdout": redact_text(result.stdout, secrets),
            "stderr": redact_text(result.stderr, secrets),
        }
    except subprocess.TimeoutExpired as exc:
        elapsed = time.time() - started
        record = {
            "name": name,
            "argv": display_argv,
            "cwd": str(cwd),
            "returncode": 124,
            "elapsed_seconds": elapsed,
            "timeout_seconds": timeout,
            "stdout": redact_text(timeout_output(exc.stdout), secrets),
            "stderr": redact_text(timeout_output(exc.stderr), secrets),
        }
        write_json(artifacts / "commands" / f"{name}.json", record)
        emit("command_timeout", name=name, timeout_seconds=timeout, elapsed_seconds=elapsed)
        return record
    write_json(artifacts / "commands" / f"{name}.json", record)
    emit("command_done", name=name, returncode=result.returncode, elapsed_seconds=elapsed)
    return record


def patch_config_database_urls(project_dir: Path, database_url_token: str, system_database_url_token: str | None) -> None:
    config_path = project_dir / "dbos-config.yaml"
    raw_config = config_path.read_text(encoding="utf-8")
    if "database_url: ${DBOS_DATABASE_URL}" not in raw_config:
        raise Finding("generated config no longer has expected database_url token")
    raw_config = raw_config.replace("database_url: ${DBOS_DATABASE_URL}", f"database_url: {database_url_token}")
    if system_database_url_token:
        raw_config = raw_config.replace(
            f"database_url: {database_url_token}",
            f"database_url: {database_url_token}\nsystem_database_url: {system_database_url_token}",
        )
    config_path.write_text(raw_config, encoding="utf-8")


def write_docker_secret(secret_name: str, value: str) -> Path:
    secrets_dir = Path("/run/secrets")
    try:
        secrets_dir.mkdir(parents=True, exist_ok=True)
        secret_path = secrets_dir / secret_name
        secret_path.write_text(value, encoding="utf-8")
        return secret_path
    except OSError as exc:
        raise SetupBlock(
            f"cannot create Docker secret file /run/secrets/{secret_name}: {type(exc).__name__}: {exc}; run in a container/WIO guest with writable /run/secrets"
        ) from exc


def remove_docker_secret(secret_name: str | None) -> None:
    if not secret_name:
        return
    try:
        (Path("/run/secrets") / secret_name).unlink(missing_ok=True)
    except Exception:
        pass


def inspect_generated_project(project_dir: Path, plan: CasePlan) -> dict[str, Any]:
    expected = {
        "config": project_dir / "dbos-config.yaml",
        "main": project_dir / plan.package_name / "main.py",
        "schema": project_dir / plan.package_name / "schema.py",
        "migration": project_dir / "migrations" / "create_table.py",
    }
    missing = [name for name, path in expected.items() if not path.exists()]
    invariant("starter_generated_expected_files", not missing, "starter generated expected files", missing=missing)

    raw_config = expected["config"].read_text(encoding="utf-8")
    parsed_config = yaml.safe_load(raw_config)
    observed = {
        "files": {name: str(path.relative_to(project_dir)) for name, path in expected.items()},
        "raw_config": raw_config,
        "parsed_config": parsed_config,
        "main_contains_project_name": plan.project_name in expected["main"].read_text(encoding="utf-8"),
    }
    invariant(
        "starter_config_matches_template_model",
        parsed_config["name"] == plan.project_name
        and parsed_config["runtimeConfig"]["start"] == [f"python3 -m {plan.package_name}.main"]
        and parsed_config["database"]["migrate"] == ["python3 migrations/create_table.py"]
        and plan.config_database_url_token in raw_config,
        "generated dbos-config.yaml matches selected template and env substitution model",
        config_name=parsed_config.get("name"),
        start=parsed_config.get("runtimeConfig", {}).get("start"),
        database_url_token=plan.config_database_url_token,
    )
    invariant(
        "starter_main_embeds_modeled_app",
        observed["main_contains_project_name"],
        "generated package main embeds the modeled DBOS app name",
        package=plan.package_name,
    )
    return observed


def inspect_database(app_url: str, sys_url: str) -> dict[str, Any]:
    app_engine = sa.create_engine(app_url)
    sys_engine = sa.create_engine(sys_url)
    try:
        with sys_engine.connect() as conn:
            migration_version = conn.execute(text("SELECT version FROM dbos.dbos_migrations")).scalar_one()
            workflow_count = conn.execute(text("SELECT count(*) FROM dbos.workflow_status")).scalar_one()
        with app_engine.connect() as conn:
            table_count = conn.execute(
                text(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = 'dbos_hello'"
                )
            ).scalar_one()
            rows = conn.execute(
                text("SELECT name, greet_count FROM dbos_hello ORDER BY greet_count")
            ).fetchall()
        return {
            "migration_version": int(migration_version),
            "workflow_count": int(workflow_count),
            "dbos_hello_table_count": int(table_count),
            "dbos_hello_rows": [{"name": row[0], "greet_count": row[1]} for row in rows],
        }
    finally:
        app_engine.dispose()
        sys_engine.dispose()


def inspect_database_absence(app_url: str, sys_url: str) -> dict[str, Any]:
    app_engine = sa.create_engine(app_url)
    sys_engine = sa.create_engine(sys_url)
    try:
        with sys_engine.connect() as conn:
            system_schema_count = conn.execute(
                text("SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'dbos'")
            ).scalar_one()
            migration_table_count = conn.execute(
                text(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema = 'dbos' AND table_name = 'dbos_migrations'"
                )
            ).scalar_one()
        with app_engine.connect() as conn:
            app_table_count = conn.execute(
                text(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = 'dbos_hello'"
                )
            ).scalar_one()
        return {
            "system_schema_count": int(system_schema_count),
            "migration_table_count": int(migration_table_count),
            "dbos_hello_table_count": int(app_table_count),
        }
    finally:
        app_engine.dispose()
        sys_engine.dispose()


def start_and_smoke(
    project_dir: Path,
    env: dict[str, str],
    artifacts: Path,
    plan: CasePlan,
    secrets: list[str],
    smoke_run: int = 1,
    expected_first_greet_count: int = 1,
) -> dict[str, Any]:
    command_name = "start" if smoke_run == 1 else f"start-{smoke_run}"
    stdout_path = artifacts / "commands" / f"{command_name}.stdout.log"
    stderr_path = artifacts / "commands" / f"{command_name}.stderr.log"
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.time()
    with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open("w", encoding="utf-8") as stderr:
        process = subprocess.Popen(
            [sys.executable, "-m", "dbos.cli.cli", "start"],
            cwd=project_dir,
            env=env,
            stdout=stdout,
            stderr=stderr,
            text=True,
            preexec_fn=os.setsid if hasattr(os, "setsid") else None,
        )
        try:
            greeting_name = plan.greeting_name if smoke_run == 1 else f"{plan.greeting_name}-{smoke_run}"
            url = f"http://127.0.0.1:8000/greeting/{quote(greeting_name)}"
            readiness_url = "http://127.0.0.1:8000/"
            responses: list[dict[str, Any]] = []
            deadline = time.monotonic() + START_SMOKE_TIMEOUT_SECONDS
            last_error = ""
            server_ready = False
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    break
                try:
                    response = requests.get(readiness_url, timeout=10)
                    if response.status_code == 200:
                        server_ready = True
                        break
                    else:
                        last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                except Exception as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                time.sleep(0.5)

            if server_ready:
                for _ in range(2):
                    try:
                        response = requests.get(url, timeout=60)
                        responses.append(
                            {
                                "status_code": response.status_code,
                                "body": response.text,
                                "elapsed_seconds": time.time() - started,
                            }
                        )
                        if response.status_code != 200:
                            last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                            break
                    except Exception as exc:
                        last_error = f"{type(exc).__name__}: {exc}"
                        break

            stdout.flush()
            stderr.flush()
            stdout_tail = redact_text(file_tail(stdout_path), secrets)
            stderr_tail = redact_text(file_tail(stderr_path), secrets)
            record = {
                "name": "start",
                "argv": [sys.executable, "-m", "dbos.cli.cli", "start"],
                "smoke_run": smoke_run,
                "returncode_during_smoke": process.poll(),
                "responses": responses,
                "last_error": last_error,
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
                "stdout_tail": stdout_tail,
                "stderr_tail": stderr_tail,
            }
            write_json(artifacts / "commands" / f"{command_name}.json", record)
            invariant(
                "starter_http_smoke_returns_modeled_greetings",
                len(responses) == 2
                and f"Greetings, {greeting_name}! You have been greeted {expected_first_greet_count} times." in responses[0]["body"]
                and f"Greetings, {greeting_name}! You have been greeted {expected_first_greet_count + 1} times." in responses[1]["body"],
                "generated app served the modeled global greeting counter twice",
                smoke_run=smoke_run,
                expected_first_greet_count=expected_first_greet_count,
                responses=responses,
                last_error=last_error,
                server_ready=server_ready,
                process_returncode=process.poll(),
                stdout_tail=stdout_tail,
                stderr_tail=stderr_tail,
            )
            return record
        finally:
            if process.poll() is None:
                if hasattr(os, "killpg"):
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                else:
                    process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    if hasattr(os, "killpg"):
                        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                    else:
                        process.kill()
                    process.wait(timeout=5)
            redact_file(stdout_path, secrets)
            redact_file(stderr_path, secrets)


def run_case(plan: CasePlan, artifacts_root: Path) -> dict[str, Any]:
    case_artifacts = artifacts_root / plan.rung_id / plan.case_id
    case_artifacts.mkdir(parents=True, exist_ok=True)
    write_json(case_artifacts / "case-plan.json", asdict(plan))

    if plan.setup_block_reason:
        observed = {
            "frontier": FRONTIER_ID,
            "rung": plan.rung_id,
            "case": plan.case_id,
            "seed": plan.seed,
            "template": plan.template,
            "variant": plan.variant,
            "status": "setup_block",
            "reason": plan.setup_block_reason,
        }
        write_json(case_artifacts / "setup-block.json", observed)
        write_json(case_artifacts / "result.json", observed)
        emit("case_setup_block", frontier=FRONTIER_ID, rung=plan.rung_id, case=plan.case_id, reason=plan.setup_block_reason)
        return observed

    admin_url = admin_url_from_env()
    admin_engine = ensure_postgres(admin_url)
    app_database = f"wio_cli_app_{plan.seed}"
    sys_database = f"{app_database}_dbos_sys"
    app_url = db_url(admin_url, app_database)
    sys_url = db_url(admin_url, sys_database)
    observed: dict[str, Any] = {
        "frontier": FRONTIER_ID,
        "rung": plan.rung_id,
        "case": plan.case_id,
        "seed": plan.seed,
        "template": plan.template,
        "variant": plan.variant,
        "app_url": redact_url(app_url),
        "system_url": redact_url(sys_url),
    }

    try:
        recreate_database(admin_engine, app_database)
        recreate_database(admin_engine, sys_database)
        workspace = Path(tempfile.mkdtemp(prefix=f"wio-cli-{plan.seed}-", dir=str(case_artifacts)))
        project_dir = workspace / "project"
        project_dir.mkdir()
        env = command_env(app_url, sys_url, project_dir)
        secret_path: Path | None = None
        secrets = [app_url, sys_url]
        if plan.docker_secret_name:
            try:
                secret_path = write_docker_secret(plan.docker_secret_name, app_url)
            except SetupBlock as exc:
                observed["status"] = "setup_block"
                observed["reason"] = str(exc)
                write_json(case_artifacts / "setup-block.json", observed)
                write_json(case_artifacts / "result.json", observed)
                emit("case_setup_block", frontier=FRONTIER_ID, rung=plan.rung_id, case=plan.case_id, reason=str(exc))
                return observed
            secrets.append(app_url)

        run_command(
            "init",
            [sys.executable, "-m", "dbos.cli.cli", "init", plan.project_name, "--template", plan.template],
            project_dir,
            env,
            case_artifacts,
            secrets,
            timeout=INIT_COMMAND_TIMEOUT_SECONDS,
        )
        if plan.config_database_url_token != "${DBOS_DATABASE_URL}" or plan.config_system_database_url_token:
            patch_config_database_urls(project_dir, plan.config_database_url_token, plan.config_system_database_url_token)
        observed["generated"] = inspect_generated_project(project_dir, plan)
        migrate_argv = [sys.executable, "-m", "dbos.cli.cli", "migrate"]
        if plan.migrate_with_cli_urls:
            migrate_argv.extend(["--db-url", app_url, "--sys-db-url", sys_url])
        elif not plan.migrate_from_config:
            migrate_argv.extend(["--db-url", app_url])
        migrate_record = run_maybe_failing_command(
            "migrate",
            migrate_argv,
            project_dir,
            env,
            case_artifacts,
            secrets,
            timeout=MIGRATE_COMMAND_TIMEOUT_SECONDS,
        )
        if not plan.expected_migrate_success:
            observed["negative_migrate"] = migrate_record
            observed["database_after_negative_migrate"] = inspect_database_absence(app_url, sys_url)
            actionable_error = (
                migrate_record["returncode"] != 0
                and (plan.missing_secret_name or "") in (migrate_record["stderr"] + migrate_record["stdout"])
            )
            observed["status"] = "finding_candidate" if not actionable_error else "passed"
            write_json(case_artifacts / "result.json", observed)
            invariant(
                "missing_secret_fails_before_partial_migrate",
                actionable_error
                and observed["database_after_negative_migrate"]["migration_table_count"] == 0
                and observed["database_after_negative_migrate"]["dbos_hello_table_count"] == 0,
                "missing Docker secret must fail with actionable error before schema/app writes",
                returncode=migrate_record["returncode"],
                stdout=migrate_record["stdout"],
                stderr=migrate_record["stderr"],
                database_after_negative_migrate=observed["database_after_negative_migrate"],
            )
            observed["status"] = "passed"
            write_json(case_artifacts / "result.json", observed)
            return observed
        invariant(
            "config_migrate_completed_with_modeled_source",
            migrate_record["returncode"] == 0,
            "dbos migrate accepted the modeled config/env source",
            migrate_from_config=plan.migrate_from_config,
            config_database_url_token=plan.config_database_url_token,
        )
        observed["database_after_migrate"] = inspect_database(app_url, sys_url)
        invariant(
            "starter_migration_created_modeled_schema_once",
            observed["database_after_migrate"]["migration_version"] >= 1
            and observed["database_after_migrate"]["dbos_hello_table_count"] == 1
            and observed["database_after_migrate"]["dbos_hello_rows"] == [],
            "dbos migrate created DBOS system schema and starter app table before startup",
            database_after_migrate=observed["database_after_migrate"],
        )
        observed["start"] = start_and_smoke(project_dir, env, case_artifacts, plan, secrets)
        observed["database_after_start"] = inspect_database(app_url, sys_url)
        invariant(
            "starter_database_state_matches_http_smoke",
            observed["database_after_start"]["dbos_hello_rows"]
            == [
                {"name": plan.greeting_name, "greet_count": 1},
                {"name": plan.greeting_name, "greet_count": 2},
            ]
            and observed["database_after_start"]["workflow_count"] >= 2,
            "starter app and DBOS system rows match the two HTTP greeting calls",
            database_after_start=observed["database_after_start"],
        )
        if plan.rerun_start_count > 1:
            migrate_again = run_maybe_failing_command(
                "migrate-rerun",
                migrate_argv,
                project_dir,
                env,
                case_artifacts,
                secrets,
                timeout=MIGRATE_COMMAND_TIMEOUT_SECONDS,
            )
            invariant(
                "starter_migrate_rerun_is_idempotent",
                migrate_again["returncode"] == 0,
                "rerunning dbos migrate on the generated starter remains successful",
                returncode=migrate_again["returncode"],
                stdout=migrate_again["stdout"],
                stderr=migrate_again["stderr"],
            )
            observed["database_after_migrate_rerun"] = inspect_database(app_url, sys_url)
            invariant(
                "starter_migration_version_stable_after_rerun",
                observed["database_after_migrate_rerun"]["migration_version"]
                == observed["database_after_migrate"]["migration_version"]
                and observed["database_after_migrate_rerun"]["dbos_hello_table_count"] == 1,
                "rerunning dbos migrate does not duplicate or lose the starter schema",
                database_after_migrate=observed["database_after_migrate"],
                database_after_migrate_rerun=observed["database_after_migrate_rerun"],
            )
            observed["start_rerun"] = start_and_smoke(
                project_dir,
                env,
                case_artifacts,
                plan,
                secrets,
                smoke_run=2,
                expected_first_greet_count=3,
            )
            observed["database_after_start_rerun"] = inspect_database(app_url, sys_url)
            expected_rows = [
                {"name": plan.greeting_name, "greet_count": 1},
                {"name": plan.greeting_name, "greet_count": 2},
                {"name": f"{plan.greeting_name}-2", "greet_count": 3},
                {"name": f"{plan.greeting_name}-2", "greet_count": 4},
            ]
            invariant(
                "starter_start_rerun_preserves_existing_state_and_progresses",
                observed["database_after_start_rerun"]["dbos_hello_rows"] == expected_rows
                and observed["database_after_start_rerun"]["workflow_count"]
                >= observed["database_after_start"]["workflow_count"] + 2,
                "second dbos start smoke preserves prior rows and records new workflow progress",
                expected_rows=expected_rows,
                database_after_start_rerun=observed["database_after_start_rerun"],
            )
        invariant(
            "starter_artifacts_redact_database_credentials",
            "dbos@" not in json.dumps(observed)
            and not artifact_tree_contains_secret(case_artifacts, secrets),
            "structured results and artifact files redact database credentials",
        )
        observed["status"] = "passed"
        write_json(case_artifacts / "result.json", observed)
        return observed
    finally:
        remove_docker_secret(plan.docker_secret_name)
        admin_engine.dispose()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rung", default=RUNG_000_ID)
    parser.add_argument("--case")
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument("--artifact-dir", default="/tmp/wio-artifacts/cli-starter-onboarding")
    return parser.parse_args()


def run_selected(args: argparse.Namespace) -> int:
    rung_id = normalize_rung(args.rung)
    if args.all_cases:
        if not args.sequential:
            raise SetupBlock("--all-cases requires --sequential because the generated app uses port 8000")
        case_ids = case_ids_for_rung(rung_id)
    else:
        if not args.case:
            raise SetupBlock("--case is required unless --all-cases is set")
        case_ids = [args.case]

    artifacts = Path(args.artifact_dir)
    artifacts.mkdir(parents=True, exist_ok=True)
    results = [run_case(build_case_plan(rung_id, case_id), artifacts) for case_id in case_ids]
    write_json(artifacts / f"{rung_id}-results.json", results)
    statuses = [result.get("status") for result in results]
    emit("workload_complete", frontier=FRONTIER_ID, rung=rung_id, cases=case_ids, statuses=statuses)
    if statuses and all(status == "setup_block" for status in statuses):
        return 42
    return 0


def main() -> int:
    try:
        return run_selected(parse_args())
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 42
    except Finding as exc:
        print(f"FINDING-CANDIDATE {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
