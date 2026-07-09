#!/usr/bin/env python3
"""WIO workload — write_stream from a STEP context is not exactly-once.

Frontier: stream-step-oaoo
Rung: rung-001-stream-step-oaoo

Protected product promise:
  DBOS.write_stream is a durable, exactly-once stream primitive. The same
  logical write, re-executed by the framework (step retry or crash replay),
  must appear in the stream exactly once — regardless of whether it was issued
  from a workflow or a step context. The public API (DBOS.write_stream /
  write_stream_async) draws no distinction.

Mechanism under test (source-grounded):
  write_stream() routes by caller context (dbos/_core.py:2187):
    * workflow context -> _sys_db.write_stream_from_workflow  — records an
      operation_output and guards re-execution with
      _check_operation_execution_txn ("DBOS.writeStream"); exactly-once.
    * step context     -> _sys_db.write_stream_from_step      — NO recorded
      operation, NO guard; it just inserts at offset max(offset)+1.
  The `streams` PK is (workflow_uuid, key, offset) and EXCLUDES function_id
  (dbos/_schemas/system_database.py). A @DBOS.step(max_attempts>1) re-invokes
  its body under the SAME ctx.function_id on every retry (dbos/_core.py retry
  loop), so a step that writes a stream value then fails re-inserts a DUPLICATE
  value at a new offset on each attempt. A consumer of DBOS.read_stream() then
  sees the value K times.

Differential (this is why the duplicate is a bug, not "steps are at-least-once"):
  The framework DOES make write_stream exactly-once — on the workflow path. The
  step path simply omits the same recorded-operation guard. Control case proves
  the workflow path yields exactly one copy; the step cases show K copies for
  the identical single logical write through the identical public API.

Cases:
  case-001 control-workflow-context (seed 9201) — workflow-context write, GREEN.
  case-002 step-retry-sync         (seed 9202) — DBOS.write_stream in a retrying
                                                  @DBOS.step; RED (K copies).
  case-003 step-retry-async        (seed 9203) — DBOS.write_stream_async in a
                                                  retrying async @DBOS.step
                                                  (async parity); RED.

v0.6.0 oracle plane:
  * primary oracle   — stream_exactly_once_<case>: observed stream copies of the
    single acked value must == 1.
  * durawatch        — the acked stream content is re-observed across a delay
    ladder; a duplicate is a mutation of the acked observable (persistent -> FAIL).
  * crashclock       — retry multiplicity K is a DECLARED op_index space
    (retry_multiplicity in [2,4]), seed-derived; a CLOCK line records the point.
    Wider K = wider duplication. VOID if the retry never armed (K<2).
  * liveness         — a watchdog thread fails the run if a workflow hangs.
  * terminal sweep   — every workflow driven must reach a terminal SUCCESS state.
  * selftest         — ORACLE_SELFTEST plants a duplicate into the CONTROL, so
    the exactly-once oracle MUST go RED; proof a green run was not vacuous.

Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/stream-step-oaoo/stream_step_oaoo_workload.py \
    --rung rung-001-stream-step-oaoo --all-cases --sequential
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[3]
VENDOR_ROOT = REPO_ROOT / ".workers" / "vendor" / "dbos-transact-py"
LOCAL_TARGET = REPO_ROOT / "target"
VENV_ROOT = REPO_ROOT / ".workers" / "vendor" / "dbos-venv"
LIB_ROOT = REPO_ROOT / ".workers" / "lib"

site_packages = sorted(VENV_ROOT.glob("lib/python*/site-packages"))
if site_packages:
    sys.path.insert(0, str(site_packages[-1]))
for _t in [VENDOR_ROOT, LOCAL_TARGET, Path("/Users/viswa/code/workers/dbos-transact-py")]:
    if _t.exists():
        sys.path.insert(0, str(_t))
        break
sys.path.insert(0, str(LIB_ROOT))

try:
    import crashclock
    import durawatch
    import sqlalchemy as sa
    from sqlalchemy.engine import make_url

    from dbos import DBOS, DBOSConfig, SetWorkflowID
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "stream-step-oaoo"
RUNG_ID = "rung-001-stream-step-oaoo"
APP_ID = "wio-stream-step-oaoo"

# case_id -> (seed, scenario, context)
CASE_MATRIX: dict[str, tuple[int, str, str]] = {
    "case-001": (9201, "control-workflow-context", "workflow"),
    "case-002": (9202, "step-retry-sync", "step-sync"),
    "case-003": (9203, "step-retry-async", "step-async"),
}

# Declared fault-timing space: how many times the retrying step's body runs
# before it succeeds (K-1 injected failures). Seed-derived, swept by depth.
RETRY_SPACE = crashclock.op_index("retry_multiplicity", lo=2, hi=4)

STREAM_KEY = "wio-stream"
VALUE = "payload-v1"
LADDER = (0.0, 2.0, 5.0)  # duplication is instantaneous; ladder confirms it persists
LIVENESS_BUDGET_S = 120.0


class SetupBlock(Exception):
    pass


# --------------------------------------------------------------------------- #
# Emission (4-field INVARIANT lines the WIO runtime parses) + verdict aggregate
# --------------------------------------------------------------------------- #
_INV: list[tuple[str, str, bool]] = []
_VOID_REASON: Optional[str] = None


def event(name: str, **fields: Any) -> None:
    parts = [f"WIO-EVENT {name}"]
    parts += [f"{k}={json.dumps(v, sort_keys=True, default=str)}" for k, v in fields.items()]
    print(" ".join(parts), flush=True)


def invariant(id_: str, name: str, ok: bool, **fields: Any) -> None:
    summary = json.dumps(fields, sort_keys=True, default=str) if fields else "ok"
    print(f"INVARIANT {id_} {name} {'PASS' if ok else 'FAIL'} {summary}", flush=True)
    _INV.append((id_, name, ok))


def mark_void(reason: str) -> None:
    global _VOID_REASON
    _VOID_REASON = reason


def final_verdict() -> int:
    if any(not ok for _, _, ok in _INV):
        fails = [i for i, _, ok in _INV if not ok]
        print(f"VERDICT: RED — {len(fails)} invariant(s) failed: {','.join(fails)}", flush=True)
        return 1
    if _VOID_REASON is not None:
        print(f"VERDICT: VOID — {_VOID_REASON}", flush=True)
        return 3
    print("VERDICT: GREEN — exactly-once held on every observed path", flush=True)
    return 0


# --------------------------------------------------------------------------- #
# Liveness watchdog
# --------------------------------------------------------------------------- #
class Liveness:
    def __init__(self, budget_s: float, label: str):
        self.budget_s = budget_s
        self.label = label
        self._done = threading.Event()
        self._fired = threading.Event()

    def _watch(self) -> None:
        if not self._done.wait(self.budget_s):
            self._fired.set()
            # Emit directly — the main thread is presumed stuck.
            print(
                f"INVARIANT liveness_{self.label} workflow_makes_progress FAIL "
                + json.dumps({"budget_s": self.budget_s, "note": "watchdog fired; run hung"}),
                flush=True,
            )
            print("VERDICT: RED — liveness watchdog fired", flush=True)
            os._exit(1)

    def __enter__(self) -> "Liveness":
        self._t = threading.Thread(target=self._watch, daemon=True)
        self._t.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._done.set()


# --------------------------------------------------------------------------- #
# Postgres / DBOS setup
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


def quote_ident(v: str) -> str:
    return '"' + v.replace('"', '""') + '"'


def prepare_databases(prefix: str) -> tuple[str, str]:
    base = admin_url()
    app_db, sys_db = f"{prefix}_app", f"{prefix}_sys"
    admin = base.set(database=base.database or "postgres").render_as_string(hide_password=False)
    try:
        engine = sa.create_engine(admin, connect_args={"connect_timeout": 5})
        with engine.connect() as raw:
            c = raw.execution_options(isolation_level="AUTOCOMMIT")
            c.execute(sa.text("SET statement_timeout = '8000ms'"))
            for db in (app_db, sys_db):
                c.execute(sa.text(f"DROP DATABASE IF EXISTS {quote_ident(db)} WITH (FORCE)"))
                c.execute(sa.text(f"CREATE DATABASE {quote_ident(db)}"))
        engine.dispose()
    except Exception as exc:
        raise SetupBlock(f"postgres setup failed: {type(exc).__name__}: {exc}") from exc
    return (
        base.set(drivername="postgresql", database=app_db).render_as_string(hide_password=False),
        base.set(drivername="postgresql+psycopg", database=sys_db).render_as_string(hide_password=False),
    )


def drop_databases(prefix: str) -> None:
    if os.environ.get("WIO_STREAM_KEEP_DATABASES") == "1":
        return
    base = admin_url()
    admin = base.set(database=base.database or "postgres").render_as_string(hide_password=False)
    engine = sa.create_engine(admin, connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as raw:
            c = raw.execution_options(isolation_level="AUTOCOMMIT")
            c.execute(sa.text("SET statement_timeout = '5000ms'"))
            for suffix in ("app", "sys"):
                c.execute(sa.text(
                    f"DROP DATABASE IF EXISTS {quote_ident(prefix + '_' + suffix)} WITH (FORCE)"))
    except Exception:
        pass
    finally:
        engine.dispose()


def make_config(prefix: str, case_id: str) -> DBOSConfig:
    app_url, sys_url = prepare_databases(prefix)
    return {
        "name": APP_ID,
        "application_database_url": app_url,
        "system_database_url": sys_url,
        "application_version": f"{APP_ID}-{case_id}",
        "executor_id": f"wio-stream-{case_id}",
        "enable_otlp": False,
    }


# --------------------------------------------------------------------------- #
# The probe workflows. Registered once at import; behavior driven by globals.
# --------------------------------------------------------------------------- #
_ATTEMPTS: dict[str, int] = {}   # workflow_id -> body invocations seen
_K = {"value": 2}                # failures-before-success target (K attempts total)


@DBOS.step(retries_allowed=True, max_attempts=8, interval_seconds=0.0)
def flaky_stream_step(value: str, wid: str) -> str:
    _ATTEMPTS[wid] = _ATTEMPTS.get(wid, 0) + 1
    DBOS.write_stream(STREAM_KEY, value)          # STEP-context write (no guard)
    if _ATTEMPTS[wid] < _K["value"]:
        raise RuntimeError(f"injected transient failure attempt {_ATTEMPTS[wid]}")
    return value


@DBOS.workflow()
def wf_step_sync(value: str, wid: str) -> str:
    r = flaky_stream_step(value, wid)
    DBOS.close_stream(STREAM_KEY)
    return r


@DBOS.step(retries_allowed=True, max_attempts=8, interval_seconds=0.0)
async def flaky_stream_step_async(value: str, wid: str) -> str:
    _ATTEMPTS[wid] = _ATTEMPTS.get(wid, 0) + 1
    await DBOS.write_stream_async(STREAM_KEY, value)   # async STEP-context write
    if _ATTEMPTS[wid] < _K["value"]:
        raise RuntimeError(f"injected transient failure attempt {_ATTEMPTS[wid]}")
    return value


@DBOS.workflow()
async def wf_step_async(value: str, wid: str) -> str:
    r = await flaky_stream_step_async(value, wid)
    await DBOS.close_stream_async(STREAM_KEY)
    return r


@DBOS.workflow()
def wf_workflow_ctx(value: str, wid: str, plant_dup: bool) -> str:
    _ATTEMPTS[wid] = _ATTEMPTS.get(wid, 0) + 1
    DBOS.write_stream(STREAM_KEY, value)          # WORKFLOW-context write (guarded)
    if plant_dup:
        # SELFTEST ONLY: force a second physical copy so the exactly-once oracle
        # must catch it — proof the control's green is not vacuous.
        DBOS.write_stream(STREAM_KEY, value)
    DBOS.close_stream(STREAM_KEY)
    return value


# --------------------------------------------------------------------------- #
# Case runner
# --------------------------------------------------------------------------- #
@dataclass
class CasePlan:
    case_id: str
    seed: int
    scenario: str
    context: str
    k: int
    prefix: str


def make_plan(case_id: str, seed_override: Optional[int]) -> CasePlan:
    if case_id not in CASE_MATRIX:
        raise SetupBlock(f"unsupported case {case_id}")
    seed, scenario, context = CASE_MATRIX[case_id]
    if seed_override is not None:
        if seed_override != seed:
            raise SetupBlock(f"{case_id} requires seed {seed}, got {seed_override}")
    # K comes from the declared crashclock op_index space, seeded.
    k = int(RETRY_SPACE.point(seed)["K"])
    return CasePlan(
        case_id=case_id, seed=seed, scenario=scenario, context=context, k=k,
        prefix=f"wio_stream_{seed}_{case_id.replace('-', '_')}",
    )


def read_stream_values(wid: str) -> list[Any]:
    return list(DBOS.read_stream(wid, STREAM_KEY))


def terminal_sweep(case_id: str, wids: list[str]) -> None:
    for wid in wids:
        try:
            status = DBOS.retrieve_workflow(wid).get_status().status
        except Exception as exc:
            invariant(f"terminal_state_{case_id}", "workflow_reaches_terminal", False,
                      wid=wid, error=f"{type(exc).__name__}: {exc}")
            continue
        ok = status == "SUCCESS"
        invariant(f"terminal_state_{case_id}", "workflow_reaches_terminal", ok,
                  wid=wid, status=status)


def run_durawatch(case_id: str, wid: str, tmpdir: str) -> None:
    """Re-observe the acked stream content across the delay ladder.

    The acked observable is the exactly-once view: a single copy of VALUE. A
    duplicate makes the observed content differ from the acked hash — durawatch
    reports it as a persistent mutation (FAIL), and also guards against loss.
    """
    m = durawatch.Manifest.start(
        case=case_id, path=os.path.join(tmpdir, f"dw_{case_id}.json"),
        ladder=LADDER, void_floor=1,
    )
    # Acked expectation: the stream holds exactly one copy of VALUE.
    m.record(eid=f"stream:{wid}", query={"wid": wid, "key": STREAM_KEY}, payload=[VALUE])

    def observe(eff: durawatch.Effect) -> Optional[Any]:
        vals = read_stream_values(wid)
        return vals if vals else None

    try:
        m.run_ladder(observe)
        invariant(f"durability_watch_{case_id}", "acked_stream_content_stable", True)
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else 1
        if code == 3:
            mark_void(f"durawatch void for {case_id}")
            invariant(f"durability_watch_{case_id}", "acked_stream_content_stable", True,
                      note="void")
        else:
            invariant(f"durability_watch_{case_id}", "acked_stream_content_stable", False,
                      note="durawatch flagged persistent mutation/loss")


def run_case(plan: CasePlan) -> None:
    selftest = crashclock.selftest_active()
    event("case_begin", case=plan.case_id, scenario=plan.scenario, context=plan.context,
          seed=plan.seed, k=plan.k, selftest=selftest)
    crashclock.clock_armed(plan.case_id, RETRY_SPACE.point(plan.seed))

    # Anti-vacuity: the step cases must actually arm a retry (K>=2).
    if plan.context in ("step-sync", "step-async") and plan.k < 2:
        mark_void(f"{plan.case_id}: retry multiplicity K={plan.k} < 2, no retry armed")

    _K["value"] = plan.k
    config = make_config(plan.prefix, plan.case_id)
    tmpdir = os.environ.get("WIO_STREAM_TMP", "/tmp")

    DBOS.destroy(destroy_registry=False)
    DBOS(config=config)
    DBOS.launch()
    wids: list[str] = []
    try:
        with Liveness(LIVENESS_BUDGET_S, plan.case_id):
            wid = f"{FRONTIER_ID}-{plan.case_id}-{plan.seed}-{uuid.uuid4().hex[:8]}"
            wids.append(wid)
            if plan.context == "workflow":
                plant = selftest  # selftest plants a duplicate into the control
                with SetWorkflowID(wid):
                    wf_workflow_ctx(VALUE, wid, plant)
            elif plan.context == "step-sync":
                with SetWorkflowID(wid):
                    wf_step_sync(VALUE, wid)
            elif plan.context == "step-async":
                async def _run() -> None:
                    with SetWorkflowID(wid):
                        await wf_step_async(VALUE, wid)
                asyncio.run(_run())
            else:  # pragma: no cover
                raise SetupBlock(f"unknown context {plan.context}")

            values = read_stream_values(wid)
            copies = sum(1 for v in values if v == VALUE)
            attempts = _ATTEMPTS.get(wid, 0)
            event("stream_observed", case=plan.case_id, wid=wid, values=values,
                  copies=copies, attempts=attempts)

            # Anti-vacuity: the write must have happened at all.
            if copies == 0:
                mark_void(f"{plan.case_id}: no stream value observed, oracle vacuous")

            # Primary oracle: exactly-once — the single logical write appears once.
            ok = copies == 1
            invariant(f"stream_exactly_once_{plan.case_id}", "single_write_appears_once", ok,
                      context=plan.context, copies=copies, expected=1, attempts=attempts,
                      k=plan.k, values=values)

            terminal_sweep(plan.case_id, wids)
            run_durawatch(plan.case_id, wid, tmpdir)
    finally:
        DBOS.destroy(destroy_registry=False)
        drop_databases(plan.prefix)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--rung", default=RUNG_ID)
    p.add_argument("--case", default=None)
    p.add_argument("--all-cases", action="store_true")
    p.add_argument("--sequential", action="store_true")
    p.add_argument("--seed", type=int, default=None)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.rung not in (RUNG_ID, "rung-001"):
        print(f"SETUP-BLOCK unsupported rung {args.rung}", flush=True)
        return 42

    if args.all_cases:
        cases = list(CASE_MATRIX)
    elif args.case:
        cases = [args.case]
    else:
        cases = ["case-001"]

    try:
        for cid in cases:
            plan = make_plan(cid, args.seed if len(cases) == 1 else None)
            run_case(plan)
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 44

    return final_verdict()


if __name__ == "__main__":
    raise SystemExit(main())
