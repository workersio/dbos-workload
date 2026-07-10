#!/usr/bin/env python3
"""WIO workload — recv delivery latency when a notification signal is missed.

Frontier: notify-reconnect-latency  (backlog W2-3 / parked S5, availability)
Rung: rung-001-missed-notify-fallback-latency

Protected product promise (`notifications-deliver-exactly-once`, availability facet):
  A recipient blocked in DBOS.recv is delivered a durably-committed message
  promptly. DBOS drives recv via a LISTEN/NOTIFY listener with a 60s DB-poll
  fallback "safety net against dropped notifications" (dbos/_sys_db.py:3071).

Mechanism (source-grounded):
  `send`/`send_bulk` commits a `dbos.notifications` row; an AFTER-INSERT trigger
  `dbos_notifications_trigger` fires `pg_notify('dbos_notifications_channel', …)`
  (dbos/_migration.py:314). The listener thread (LISTEN on that channel,
  _sys_db_postgres.py:169) sets the recv waiter's in-memory event. If that async
  notification is lost — e.g. the listener's connection is resetting across a DB
  reconnect — the event is never set, so recv falls back to a DB re-check whose
  interval, WITH a listener running, is `_notification_fallback_polling_interval`
  = 60s (`_event_recheck_interval`, :3082). recv's wait loop
  (`event.wait(timeout=min(remaining, recheck_interval))`, :3112) therefore does
  not re-poll the DB until 60s — so a message durably available at t≈0 is not
  delivered until ~min(recv_timeout, 60s). No loss (recv_consume's final poll
  delivers), but up to a 60s availability stall.

Deterministic model of the missed notification (no netem timing luck):
  DISABLE `dbos_notifications_trigger`, then `DBOS.send` (row commits, NO
  pg_notify emitted → listener never signaled — identical to a notification lost
  in transit), then re-enable. This reproduces the exact recv-side state a
  dropped NOTIFY produces, deterministically.

Cases:
  case-001 control-notify-delivered (seed 5501) — normal send (trigger enabled):
    listener signals the waiter; recv delivers in ≪1s. GREEN.
  case-002 missed-notify-stall     (seed 5502) — send with the trigger disabled:
    the waiter is never signaled; recv delivers only via the 60s fallback poll.
    The DIFFERENTIAL (identical durable message, delivered ~60s later purely
    because the async signal was missed) is the availability characterization.

Oracles (v0.6.0 plane):
  * delivered (hard, both cases): recv returns the message — no loss. Proves the
    fallback works and the run is non-vacuous (a message WAS sent + is durable).
  * timely_vs_control (characterization/graded): a durably-committed message must
    be delivered without a listener-fallback-sized stall. Graded as a DIFFERENTIAL
    against the control: case-002 latency must be < STALL_FLOOR (well below the 60s
    fallback, well above the control's sub-second path). A ~60s delivery FAILS —
    flagging the stall. Framed as availability (weight 2), NOT an exactly-once
    violation: the message is delivered, just late.
  * liveness watchdog; terminal-state sweep; crashclock declares the recv-timeout
    space; ORACLE_SELFTEST disables the trigger in the CONTROL too, so its
    timely_vs_control oracle must go RED (proof the oracle is live).

Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/notify-reconnect-latency/notify_reconnect_latency_workload.py \
    --rung rung-001-missed-notify-fallback-latency --all-cases --sequential
"""
from __future__ import annotations

import argparse
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
for _t in [VENDOR_ROOT, LOCAL_TARGET]:
    if _t.exists():
        sys.path.insert(0, str(_t))
        break
sys.path.insert(0, str(LIB_ROOT))

try:
    import crashclock
    import sqlalchemy as sa
    from sqlalchemy.engine import make_url

    from dbos import DBOS, DBOSConfig, SetWorkflowID
except Exception as exc:  # pragma: no cover
    print(f"SETUP-BLOCK imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "notify-reconnect-latency"
RUNG_ID = "rung-001-missed-notify-fallback-latency"
APP_ID = "wio-notify-reconnect-latency"

CASE_MATRIX: dict[str, tuple[int, str]] = {
    "case-001": (5501, "control-notify-delivered"),
    "case-002": (5502, "missed-notify-stall"),
}

# Declared swept space: the recv timeout (seconds). Wide enough that the 60s
# fallback poll fires and delivers even in the missed-notify case (so we observe
# the stall latency, not just a timeout→None).
RECV_TIMEOUT_SPACE = crashclock.op_index("recv_timeout_s", lo=90, hi=90)
TOPIC = "wio-topic"
VALUE = "notify-payload-v1"
# A durably-committed message delivered this many seconds late (or more) is a
# listener-fallback-sized stall. Sits far below the 60s fallback and far above
# the sub-second signalled path — so it cleanly separates the two.
STALL_FLOOR_S = 8.0
LIVENESS_BUDGET_S = 150.0


class SetupBlock(Exception):
    pass


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
    print("VERDICT: GREEN — notifications delivered without a fallback-sized stall", flush=True)
    return 0


class Liveness:
    def __init__(self, budget_s: float, label: str):
        self.budget_s, self.label = budget_s, label
        self._done = threading.Event()

    def _watch(self) -> None:
        if not self._done.wait(self.budget_s):
            print(f"INVARIANT liveness_{self.label} makes_progress FAIL "
                  + json.dumps({"budget_s": self.budget_s, "note": "watchdog fired"}), flush=True)
            print("VERDICT: RED — liveness watchdog fired", flush=True)
            os._exit(1)

    def __enter__(self) -> "Liveness":
        self._t = threading.Thread(target=self._watch, daemon=True)
        self._t.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._done.set()


# --------------------------------------------------------------------------- #
# Postgres / DBOS setup (single shared DB per case; listener runs in-process)
# --------------------------------------------------------------------------- #
_SYS_URL = {"value": ""}


def admin_url() -> sa.URL:
    raw = os.environ.get("DBOS_POSTGRES_ADMIN_URL",
                         "postgresql+psycopg://postgres:dbos@localhost:5432/postgres")
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
    _SYS_URL["value"] = sys_url
    return {
        "name": APP_ID,
        "application_database_url": app_url,
        "system_database_url": sys_url,
        "application_version": f"{APP_ID}-{case_id}",
        "executor_id": f"wio-notify-{case_id}",
        "enable_otlp": False,
    }


def set_notify_trigger(enabled: bool) -> None:
    """Enable/disable the AFTER-INSERT pg_notify trigger on dbos.notifications."""
    verb = "ENABLE" if enabled else "DISABLE"
    engine = sa.create_engine(_SYS_URL["value"], connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as raw:
            c = raw.execution_options(isolation_level="AUTOCOMMIT")
            c.execute(sa.text(
                f"ALTER TABLE dbos.notifications {verb} TRIGGER dbos_notifications_trigger"))
    finally:
        engine.dispose()


def unconsumed_rows(dest_id: str) -> int:
    engine = sa.create_engine(_SYS_URL["value"], connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as c:
            return c.execute(sa.text(
                "SELECT count(*) FROM dbos.notifications "
                "WHERE destination_uuid = :d AND topic = :t AND consumed = false"
            ), {"d": dest_id, "t": TOPIC}).scalar() or 0
    finally:
        engine.dispose()


# --------------------------------------------------------------------------- #
# Workflows: a recipient that blocks in recv, and a sender.
# --------------------------------------------------------------------------- #
_RECV_TIMEOUT = {"value": 90}


@DBOS.workflow()
def recipient() -> dict[str, Any]:
    t0 = time.time()
    got = DBOS.recv(TOPIC, timeout_seconds=float(_RECV_TIMEOUT["value"]))
    return {"got": got, "latency_s": round(time.time() - t0, 3)}


@DBOS.workflow()
def sender(dest_id: str) -> None:
    DBOS.send(dest_id, VALUE, TOPIC)


@dataclass
class CasePlan:
    case_id: str
    seed: int
    scenario: str
    prefix: str
    recv_timeout: int


def make_plan(case_id: str, seed_override: Optional[int]) -> CasePlan:
    if case_id not in CASE_MATRIX:
        raise SetupBlock(f"unsupported case {case_id}")
    seed, scenario = CASE_MATRIX[case_id]
    if seed_override is not None and seed_override != seed:
        raise SetupBlock(f"{case_id} requires seed {seed}, got {seed_override}")
    rt = int(RECV_TIMEOUT_SPACE.point(seed)["K"])
    return CasePlan(case_id, seed, scenario, f"wio_notify_{seed}_{case_id.replace('-', '_')}", rt)


def run_case(plan: CasePlan) -> None:
    selftest = crashclock.selftest_active()
    crashclock.clock_armed(plan.case_id, RECV_TIMEOUT_SPACE.point(plan.seed))
    # miss the notify in the stall case; selftest ALSO misses it in the control.
    miss_notify = (plan.scenario == "missed-notify-stall") or selftest
    _RECV_TIMEOUT["value"] = plan.recv_timeout
    event("case_begin", case=plan.case_id, scenario=plan.scenario, seed=plan.seed,
          recv_timeout=plan.recv_timeout, miss_notify=miss_notify, selftest=selftest)

    config = make_config(plan.prefix, plan.case_id)
    DBOS.destroy(destroy_registry=False)
    DBOS(config=config)
    DBOS.launch()
    wids: list[str] = []
    try:
        with Liveness(LIVENESS_BUDGET_S, plan.case_id):
            uniq = uuid.uuid4().hex[:8]
            dest_id = f"{FRONTIER_ID}-dest-{plan.case_id}-{uniq}"

            with SetWorkflowID(dest_id):
                recip = DBOS.start_workflow(recipient)   # begins blocking in recv
            wids.append(dest_id)
            time.sleep(2.0)  # let the recipient register its waiter + listener settle

            if miss_notify:
                set_notify_trigger(False)                # model the dropped NOTIFY
            sender_id = f"{FRONTIER_ID}-sender-{plan.case_id}-{uniq}"
            with SetWorkflowID(sender_id):
                sender(dest_id)                          # row commits (durable)
            wids.append(sender_id)
            if miss_notify:
                set_notify_trigger(True)

            result = recip.get_result()
            got, latency = result["got"], result["latency_s"]
            event("delivery_observed", case=plan.case_id, delivered=(got == VALUE),
                  latency_s=latency, miss_notify=miss_notify)

            # Anti-vacuity: a message must actually have been sent.
            if got is None and unconsumed_rows(dest_id) == 0:
                mark_void(f"{plan.case_id}: no message durably present, oracle vacuous")

            # Hard oracle: delivered (no loss). The fallback must eventually work.
            invariant(f"delivered_{plan.case_id}", "message_delivered", got == VALUE,
                      got=got, latency_s=latency)

            # Characterization oracle: a durably-committed message must not incur a
            # listener-fallback-sized stall (availability). Differential: the
            # control path delivers ≪1s; a missed-notify delivery ~60s FAILS.
            timely = latency < STALL_FLOOR_S
            invariant(f"timely_vs_control_{plan.case_id}", "delivered_without_fallback_stall",
                      timely, latency_s=latency, stall_floor_s=STALL_FLOOR_S,
                      miss_notify=miss_notify)

            for wid in wids:
                try:
                    st = DBOS.retrieve_workflow(wid).get_status().status
                    invariant(f"terminal_state_{plan.case_id}", "workflow_reaches_terminal",
                              st == "SUCCESS", wid=wid, status=st)
                except Exception as exc:
                    invariant(f"terminal_state_{plan.case_id}", "workflow_reaches_terminal",
                              False, wid=wid, error=f"{type(exc).__name__}: {exc}")
    finally:
        try:
            set_notify_trigger(True)
        except Exception:
            pass
        DBOS.destroy(destroy_registry=False)
        drop_databases(plan.prefix)


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
            run_case(make_plan(cid, args.seed if len(cases) == 1 else None))
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 44
    return final_verdict()


if __name__ == "__main__":
    raise SystemExit(main())
