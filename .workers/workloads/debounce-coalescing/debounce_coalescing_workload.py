#!/usr/bin/env python3
"""WIO workload: concurrent debounce coalescing on the new DELAYED protocol (#752).

Frontier: scheduler-debouncer-timing
Rung:
  - rung-001-concurrent-bounce-coalescing
Protected product promise:
  A burst of debounce calls on one key coalesces to exactly-once executions
  carrying the latest committed input; concurrent bouncers racing the
  DELAYED->ENQUEUED flip must not double-execute a window or silently drop a
  bounce that returned a handle.

Mechanism (verified in dbos/_debouncer.py + dbos/_sys_db.py):
  - Debouncer.create(target).debounce(key, period_sec, *args) atomically extends
    an existing debounced DELAYED workflow (same deduplication_id) or enqueues a
    fresh one, returning a handle.
  - transition_delayed_workflows() flips DELAYED->ENQUEUED (~1.0s queue tick) and
    clears the dedup key in the same transaction, so a later same-key bounce
    starts a fresh window instead of extending the now-committed one.
  - debounce()'s retry loop self-documents a race: a same-name debounced holder
    that flips out of DELAYED mid-bounce (retry), plus a loser that catches
    DBOSQueueDeduplicatedError at enqueue and re-bounces.

Oracle (independent app-db execution ledger vs public handles + status rows):
  - exactly-once per settled window: no workflow_id body runs twice;
  - executions <= total bounces;
  - latest-input-wins / no-silent-drop: the global-last committed seq (a lone
    pin bounce issued after the concurrent burst, hence the max seq) is carried
    by some execution -- its handle resolves to a terminal execution;
  - reachability: >=2 threads issued bounces before the first execution flipped.

Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/debounce-coalescing/debounce_coalescing_workload.py \
    --rung rung-001-concurrent-bounce-coalescing --case case-001
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
VENV_ROOT = REPO_ROOT / ".workers" / "vendor" / "dbos-venv"
VENDOR_ROOT = REPO_ROOT / ".workers" / "vendor" / "dbos-transact-py"
LOCAL_TARGET = REPO_ROOT / "target"

site_packages = sorted(VENV_ROOT.glob("lib/python*/site-packages"))
if site_packages:
    sys.path.insert(0, str(site_packages[-1]))

for _target in [REPO_ROOT, VENDOR_ROOT, LOCAL_TARGET]:
    if (_target / "dbos" / "__init__.py").exists():
        sys.path.insert(0, str(_target))
        break

try:
    import sqlalchemy as sa
    from sqlalchemy.engine import make_url

    from dbos import DBOS, DBOSConfig, Debouncer, SetWorkflowID  # noqa: F401
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK dbos_imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "scheduler-debouncer-timing"
RUNG_ID = "rung-001-concurrent-bounce-coalescing"
APP_ID = "wio-debounce-coalescing"
APP_VERSION = "wio-debounce-coalescing"

TERMINAL_STATUSES = {"SUCCESS", "ERROR", "CANCELLED", "MAX_RECOVERY_ATTEMPTS_EXCEEDED"}
LEDGER_TABLE = "wio_debounce_execs"

# The debounced target writes its side effect through this engine, deliberately
# independent of DBOS bookkeeping so executions are counted from the app db only.
_APP_ENGINE: Any = None
_BODY_SLEEP_SEC = 0.3


class WorkloadFailure(Exception):
    pass


class SetupBlock(Exception):
    pass


@dataclass(frozen=True)
class CasePlan:
    rung_id: str
    case_id: str
    seed: int
    scenario: str
    database_prefix: str
    debounce_key: str
    num_threads: int
    rounds: int
    period_sec: float
    round_gap_sec: float
    round_gap_jitter_sec: float
    bounce_jitter_sec: float
    debounce_timeout_sec: float | None
    max_executor_threads: int
    resolve_deadline_sec: float
    expect_single_window: bool


CASE_MATRIX: dict[str, CasePlan] = {
    # Single-window: a tight concurrent burst (all bounces inside one period P),
    # then a lone pin bounce carrying the max seq -> exactly ONE execution.
    "case-001": CasePlan(
        rung_id=RUNG_ID, case_id="case-001", seed=9201,
        scenario="single-window-burst",
        database_prefix="wio_debounce_9201_case_001",
        debounce_key="k-9201", num_threads=4, rounds=2,
        period_sec=2.0, round_gap_sec=0.03, round_gap_jitter_sec=0.02,
        bounce_jitter_sec=0.02, debounce_timeout_sec=None,
        max_executor_threads=8, resolve_deadline_sec=45.0,
        expect_single_window=True,
    ),
    # Straddle-the-flip: small P with round gaps > P + the ~1.0s transition tick,
    # so windows flip between rounds. Bounded oracle: 1..bounces executions, none
    # double, global-max seq carried by some execution.
    "case-002": CasePlan(
        rung_id=RUNG_ID, case_id="case-002", seed=9202,
        scenario="straddle-the-flip",
        database_prefix="wio_debounce_9202_case_002",
        debounce_key="k-9202", num_threads=2, rounds=4,
        period_sec=0.5, round_gap_sec=1.6, round_gap_jitter_sec=0.3,
        bounce_jitter_sec=0.03, debounce_timeout_sec=8.0,
        max_executor_threads=8, resolve_deadline_sec=60.0,
        expect_single_window=False,
    ),
    # Hot-key contention: M=4 threads hammer one key with interleaved seqs inside
    # one window, then pin -> exactly one execution carrying the max seq.
    "case-003": CasePlan(
        rung_id=RUNG_ID, case_id="case-003", seed=9203,
        scenario="hot-key-contention",
        database_prefix="wio_debounce_9203_case_003",
        debounce_key="k-9203", num_threads=4, rounds=3,
        period_sec=2.5, round_gap_sec=0.03, round_gap_jitter_sec=0.02,
        bounce_jitter_sec=0.02, debounce_timeout_sec=None,
        max_executor_threads=8, resolve_deadline_sec=45.0,
        expect_single_window=True,
    ),
}


# --------------------------------------------------------------------------- #
# Emitters (WIO contract)                                                     #
# --------------------------------------------------------------------------- #
def now_ms() -> int:
    return int(time.time() * 1000)


def event(name: str, **fields: Any) -> None:
    parts = [f"WIO-EVENT {name}"]
    parts.extend(
        f"{key}={json.dumps(value, sort_keys=True, default=str)}"
        for key, value in fields.items()
    )
    print(" ".join(parts), flush=True)


def invariant(id_: str, name: str, ok: bool, **fields: Any) -> None:
    status = "PASS" if ok else "FAIL"
    summary = json.dumps(fields, sort_keys=True, default=str) if fields else "ok"
    print(f"INVARIANT {id_} {name} {status} {summary}", flush=True)
    if not ok:
        raise WorkloadFailure(f"{id_} {name} failed: {summary}")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


# --------------------------------------------------------------------------- #
# Postgres lifecycle (per-case app + sys databases)                           #
# --------------------------------------------------------------------------- #
def admin_url() -> sa.URL:
    raw = os.environ.get(
        "DBOS_POSTGRES_ADMIN_URL",
        "postgresql+psycopg://postgres:dbos@localhost:5432/postgres",
    )
    url = make_url(raw)
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+psycopg")
    return url


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def prepare_databases(prefix: str, artifacts: Path) -> tuple[str, str, str]:
    base = admin_url()
    app_db = f"{prefix}_app"
    sys_db = f"{prefix}_sys"
    admin = base.set(database=base.database or "postgres").render_as_string(
        hide_password=False
    )
    masked = base.set(password="***" if base.password else None).render_as_string(
        hide_password=False
    )
    event("postgres_preflight", admin_url=masked, app_db=app_db, sys_db=sys_db)
    try:
        engine = sa.create_engine(admin, connect_args={"connect_timeout": 5})
        with engine.connect() as raw_connection:
            connection = raw_connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(sa.text("SET statement_timeout = '8000ms'"))
            for database in (app_db, sys_db):
                connection.execute(
                    sa.text(f"DROP DATABASE IF EXISTS {quote_ident(database)} WITH (FORCE)")
                )
                connection.execute(sa.text(f"CREATE DATABASE {quote_ident(database)}"))
        engine.dispose()
    except Exception as exc:
        write_json(
            artifacts / "setup-block.json",
            {
                "kind": "postgres_unavailable_or_database_create_failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "admin_url": masked,
            },
        )
        raise SetupBlock(f"postgres setup failed: {type(exc).__name__}: {exc}") from exc
    return (
        base.set(drivername="postgresql", database=app_db).render_as_string(
            hide_password=False
        ),
        base.set(drivername="postgresql+psycopg", database=sys_db).render_as_string(
            hide_password=False
        ),
        masked,
    )


def drop_databases(prefix: str) -> None:
    if os.environ.get("WIO_DEBOUNCE_KEEP_DATABASES") == "1":
        return
    base = admin_url()
    admin = base.set(database=base.database or "postgres").render_as_string(
        hide_password=False
    )
    engine = sa.create_engine(admin, connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as raw_connection:
            connection = raw_connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(sa.text("SET statement_timeout = '5000ms'"))
            connection.execute(sa.text("SET lock_timeout = '3000ms'"))
            for suffix in ("app", "sys"):
                connection.execute(
                    sa.text(
                        f"DROP DATABASE IF EXISTS {quote_ident(prefix + '_' + suffix)} WITH (FORCE)"
                    )
                )
    except Exception as exc:
        event(
            "database_cleanup_best_effort_failed",
            prefix=prefix, error_type=type(exc).__name__, error=str(exc),
        )
    finally:
        engine.dispose()


def make_config(plan: CasePlan, app_url: str, sys_url: str) -> DBOSConfig:
    return {
        "name": APP_ID,
        "application_database_url": app_url,
        "system_database_url": sys_url,
        "application_version": f"{APP_VERSION}-{plan.case_id}",
        "executor_id": f"wio-debounce-{plan.case_id}",
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
        "run_admin_server": False,
        "runtimeConfig": {"max_executor_threads": plan.max_executor_threads},
    }


def launch_dbos(config: DBOSConfig) -> Any:
    DBOS.destroy(destroy_registry=False)
    dbos = DBOS(config=config)
    # The debouncer runs on DBOS's internal queue. Force it into the registry
    # before launch so the queue manager starts its worker (which also runs
    # transition_delayed_workflows, the DELAYED->ENQUEUED flip).
    dbos._registry.get_internal_queue()
    DBOS.launch()
    return dbos


# --------------------------------------------------------------------------- #
# Independent app-db execution ledger                                         #
# --------------------------------------------------------------------------- #
def create_ledger(engine: Any) -> None:
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                f"CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} ("
                "exec_uuid TEXT PRIMARY KEY, "
                "workflow_id TEXT, "
                "observed_seq INT, "
                "ts DOUBLE PRECISION)"
            )
        )


def read_ledger(engine: Any) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                f"SELECT exec_uuid, workflow_id, observed_seq, ts "
                f"FROM {LEDGER_TABLE} ORDER BY ts"
            )
        ).mappings()
        return [dict(row) for row in rows]


@DBOS.workflow()
def debounced_target(seq: int) -> int:
    """One row per real body execution, then a short blocking sleep so
    overlapping executions are observable. Returns the input seq it carried."""
    engine = _APP_ENGINE
    exec_uuid = str(uuid.uuid4())
    workflow_id = DBOS.workflow_id
    ts = time.time()
    if engine is not None:
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    f"INSERT INTO {LEDGER_TABLE} "
                    "(exec_uuid, workflow_id, observed_seq, ts) "
                    "VALUES (:u, :w, :s, :t)"
                ),
                {"u": exec_uuid, "w": workflow_id, "s": int(seq), "t": ts},
            )
    time.sleep(_BODY_SLEEP_SEC)
    return int(seq)


# --------------------------------------------------------------------------- #
# Concurrency driver                                                          #
# --------------------------------------------------------------------------- #
@dataclass
class BounceRecord:
    seq: int
    thread_id: int
    workflow_id: str
    completed_at: float
    kind: str  # "burst" or "pin"


class SeqGen:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._n = 0

    def next(self) -> int:
        with self._lock:
            self._n += 1
            return self._n


def run_burst(plan: CasePlan, debouncer: Debouncer, seqgen: SeqGen, rng_seed: int) -> list[BounceRecord]:
    barrier = threading.Barrier(plan.num_threads)
    records: list[BounceRecord] = []
    records_lock = threading.Lock()

    def worker(thread_index: int) -> None:
        rng = random.Random(rng_seed + thread_index * 1009)
        tid = threading.get_ident()
        for round_index in range(plan.rounds):
            # Synchronize the start of every round so threads genuinely contend
            # on the key (and, across rounds with a gap, straddle the flip).
            try:
                barrier.wait(timeout=30)
            except threading.BrokenBarrierError:
                return
            if plan.bounce_jitter_sec:
                time.sleep(rng.uniform(0.0, plan.bounce_jitter_sec))
            seq = seqgen.next()
            handle = debouncer.debounce(plan.debounce_key, plan.period_sec, seq)
            with records_lock:
                records.append(
                    BounceRecord(
                        seq=seq, thread_id=tid,
                        workflow_id=handle.workflow_id,
                        completed_at=time.time(), kind="burst",
                    )
                )
            event(
                "bounce_issued", case=plan.case_id, thread=thread_index,
                round=round_index, seq=seq, workflow_id=handle.workflow_id,
            )
            if round_index < plan.rounds - 1 and plan.round_gap_sec:
                gap = plan.round_gap_sec + rng.uniform(0.0, plan.round_gap_jitter_sec)
                time.sleep(gap)

    threads = [
        threading.Thread(target=worker, args=(i,), name=f"bouncer-{i}")
        for i in range(plan.num_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=90)
    return records


# --------------------------------------------------------------------------- #
# Handle resolution / quiescence                                              #
# --------------------------------------------------------------------------- #
def wait_for_terminal(dbos: Any, workflow_ids: list[str], deadline_sec: float) -> dict[str, str | None]:
    deadline = time.monotonic() + deadline_sec
    statuses: dict[str, str | None] = {wid: None for wid in workflow_ids}
    while time.monotonic() < deadline:
        pending = [wid for wid, st in statuses.items() if st not in TERMINAL_STATUSES]
        if not pending:
            return statuses
        for wid in pending:
            st = dbos.get_workflow_status(wid)
            statuses[wid] = st.status if st is not None else None
        if all(statuses[wid] in TERMINAL_STATUSES for wid in workflow_ids):
            return statuses
        time.sleep(0.1)
    return statuses


# --------------------------------------------------------------------------- #
# Case runner + oracle                                                        #
# --------------------------------------------------------------------------- #
def run_case(plan: CasePlan, artifact_dir: Path) -> int:
    global _APP_ENGINE
    case_artifacts = artifact_dir / plan.case_id
    case_artifacts.mkdir(parents=True, exist_ok=True)
    write_json(case_artifacts / "plan.json", asdict(plan))

    app_url, sys_url, admin_masked = prepare_databases(plan.database_prefix, case_artifacts)
    config = make_config(plan, app_url, sys_url)
    dbos = launch_dbos(config)
    _APP_ENGINE = dbos._app_db.engine
    create_ledger(_APP_ENGINE)

    event(
        "case_start", frontier=FRONTIER_ID, rung=plan.rung_id,
        case=plan.case_id, seed=plan.seed, scenario=plan.scenario,
        admin_url=admin_masked, period_sec=plan.period_sec,
        num_threads=plan.num_threads, rounds=plan.rounds,
    )

    try:
        debouncer = Debouncer.create(
            debounced_target, debounce_timeout_sec=plan.debounce_timeout_sec
        )
        seqgen = SeqGen()

        # (1) Concurrent burst: M threads contend on one key.
        burst_records = run_burst(plan, debouncer, seqgen, plan.seed)

        # (2) Global-last committed input: a lone pin bounce issued AFTER the
        # burst settled, so its seq is the max and it is the last committer.
        # Latest-input-wins => this seq must be carried by some execution.
        pin_seq = seqgen.next()
        pin_handle = debouncer.debounce(plan.debounce_key, plan.period_sec, pin_seq)
        pin_record = BounceRecord(
            seq=pin_seq, thread_id=threading.get_ident(),
            workflow_id=pin_handle.workflow_id, completed_at=time.time(), kind="pin",
        )
        event("pin_bounce_issued", case=plan.case_id, seq=pin_seq, workflow_id=pin_handle.workflow_id)

        all_records = burst_records + [pin_record]
        all_seqs = [r.seq for r in all_records]
        max_seq = max(all_seqs)
        total_bounces = len(all_records)
        distinct_threads = {r.thread_id for r in burst_records}

        # (3) Wait for every returned handle's workflow to reach a terminal state.
        returned_ids = sorted({r.workflow_id for r in all_records})
        statuses = wait_for_terminal(dbos, returned_ids, plan.resolve_deadline_sec)

        # (4) Also resolve each returned workflow id explicitly (bounded: all
        # already terminal, so get_result returns immediately).
        handle_results: dict[str, Any] = {}
        handle_errors: dict[str, str] = {}
        for wid in returned_ids:
            try:
                res = dbos.retrieve_workflow(wid).get_result(polling_interval_sec=0.1)
                handle_results[wid] = res
            except Exception as exc:  # pragma: no cover - failure evidence path
                handle_errors[wid] = f"{type(exc).__name__}: {exc}"

        # (5) Let any straggler flip settle, then read the independent ledger.
        time.sleep(max(1.5, plan.period_sec + 1.2))
        ledger = read_ledger(_APP_ENGINE)

        exec_count = len(ledger)
        observed_seqs = sorted(row["observed_seq"] for row in ledger)
        by_workflow: dict[str, int] = {}
        for row in ledger:
            by_workflow[row["workflow_id"]] = by_workflow.get(row["workflow_id"], 0) + 1
        double_run_ids = {wid: n for wid, n in by_workflow.items() if n > 1}
        first_exec_ts = min((row["ts"] for row in ledger), default=None)
        threads_before_first = (
            {r.thread_id for r in burst_records if first_exec_ts is not None and r.completed_at < first_exec_ts}
            if first_exec_ts is not None else set()
        )
        nonterminal = {wid: st for wid, st in statuses.items() if st not in TERMINAL_STATUSES}

        raw = {
            "case": plan.case_id,
            "scenario": plan.scenario,
            "seed": plan.seed,
            "total_bounces": total_bounces,
            "max_seq": max_seq,
            "all_seqs": all_seqs,
            "distinct_burst_threads": len(distinct_threads),
            "exec_count": exec_count,
            "observed_seqs": observed_seqs,
            "executions_per_workflow_id": by_workflow,
            "double_run_ids": double_run_ids,
            "distinct_returned_workflow_ids": len(returned_ids),
            "returned_workflow_ids": returned_ids,
            "handle_statuses": statuses,
            "nonterminal_handles": nonterminal,
            "handle_errors": handle_errors,
            "first_exec_ts": first_exec_ts,
            "threads_bounced_before_first_exec": len(threads_before_first),
            "ledger": ledger,
        }
        write_json(case_artifacts / "result.json", raw)
        event("case_raw_counts", **{k: raw[k] for k in (
            "exec_count", "observed_seqs", "max_seq", "total_bounces",
            "double_run_ids", "nonterminal_handles", "distinct_returned_workflow_ids",
        )})

        # ---------------- Oracle (do NOT weaken) ---------------- #

        # Reachability: prove concurrent bounces genuinely contended before the
        # first window flipped and executed.
        invariant(
            f"{_cid(plan)}_contention_reached", "contention_reached",
            len(threads_before_first) >= 2,
            threads_before_first=len(threads_before_first),
            first_exec_ts=first_exec_ts, distinct_burst_threads=len(distinct_threads),
        )

        # No-double-exec: no workflow_id body ran twice (fresh-uuid ledger rows).
        invariant(
            f"{_cid(plan)}_no_double_exec", "no_double_exec",
            len(double_run_ids) == 0,
            double_run_ids=double_run_ids, executions_per_workflow_id=by_workflow,
        )

        # Executions never exceed bounces.
        invariant(
            f"{_cid(plan)}_exec_le_bounces", "exec_le_bounces",
            1 <= exec_count <= total_bounces,
            exec_count=exec_count, total_bounces=total_bounces,
        )

        # No-silent-drop: every returned handle resolved to a terminal execution.
        invariant(
            f"{_cid(plan)}_all_handles_resolved", "all_handles_resolved",
            len(nonterminal) == 0 and not handle_errors,
            nonterminal_handles=nonterminal, handle_errors=handle_errors,
        )

        # Latest-input-wins: the global-last committed seq (the pin, == max seq)
        # is carried by some execution -- not silently dropped or superseded.
        invariant(
            f"{_cid(plan)}_latest_input_wins", "latest_input_wins",
            max_seq in observed_seqs,
            max_seq=max_seq, observed_seqs=observed_seqs,
        )

        # Every distinct successful returned workflow appears once in the ledger:
        # terminal SUCCESS <=> exactly one body execution (ties to no-double-exec).
        success_ids = sorted(wid for wid, st in statuses.items() if st == "SUCCESS")
        ledger_ids = sorted(by_workflow)
        invariant(
            f"{_cid(plan)}_success_matches_ledger", "success_matches_ledger",
            success_ids == ledger_ids,
            success_workflow_ids=success_ids, ledger_workflow_ids=ledger_ids,
        )

        if plan.expect_single_window:
            # A tight burst inside one period must coalesce to exactly ONE
            # execution carrying the max seq.
            invariant(
                f"{_cid(plan)}_exactly_one_execution", "exactly_one_execution",
                exec_count == 1,
                exec_count=exec_count, observed_seqs=observed_seqs,
            )
            invariant(
                f"{_cid(plan)}_single_window_max_seq", "single_window_max_seq",
                observed_seqs == [max_seq],
                observed_seqs=observed_seqs, max_seq=max_seq,
            )

        event("case_passed", case=plan.case_id, exec_count=exec_count, observed_seqs=observed_seqs)
        return 0
    finally:
        try:
            DBOS.destroy(destroy_registry=False, workflow_completion_timeout_sec=3)
        finally:
            _APP_ENGINE = None
            if os.environ.get("WIO_CLEANUP_AFTER", "1") == "1":
                drop_databases(plan.database_prefix)


def _cid(plan: CasePlan) -> str:
    return plan.case_id.replace("-", "_")


# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #
def normalize_rung(rung: str) -> str:
    if rung in {RUNG_ID, "rung-001"}:
        return RUNG_ID
    raise SetupBlock(f"unsupported rung: {rung}")


def make_plan(rung: str, case_id: str, seed_override: int | None) -> CasePlan:
    normalize_rung(rung)
    if case_id not in CASE_MATRIX:
        raise SetupBlock(f"unsupported case: {case_id}")
    plan = CASE_MATRIX[case_id]
    if seed_override is not None and seed_override != plan.seed:
        raise SetupBlock(f"{case_id} requires seed {plan.seed}, got {seed_override}")
    return plan


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DBOS concurrent debounce coalescing workload")
    parser.add_argument("--rung", default=RUNG_ID)
    parser.add_argument("--case", choices=sorted(CASE_MATRIX))
    parser.add_argument("--seed", type=int)
    parser.add_argument("--all-cases", action="store_true")
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument(
        "--artifact-dir",
        default="/tmp/wio-artifacts/debounce-coalescing",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.all_cases:
        cases = sorted(CASE_MATRIX)
    elif args.case:
        cases = [args.case]
    else:
        raise SystemExit("SETUP-BLOCK --case or --all-cases is required")
    if args.all_cases and not args.sequential:
        raise SystemExit("SETUP-BLOCK --all-cases requires --sequential to keep DBOS global state isolated")
    try:
        for case_id in cases:
            seed = args.seed if len(cases) == 1 else None
            run_case(make_plan(args.rung, case_id, seed), Path(args.artifact_dir))
        return 0
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 42
    except WorkloadFailure as exc:
        print(f"WORKLOAD-FAIL {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
