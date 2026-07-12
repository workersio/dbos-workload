#!/usr/bin/env python3
"""Flow drivers for the DBOS Transact (Python) usage model.

Executor-owned. One driver class per model flow (check.py G2 bijection).

ARCHITECTURE (one boot per run). A live DBOS instance cannot boot in the
run_scenario spine process (its background daemon threads outrun the sandbox's
virtual-time liveness watchdog), and booting a fresh subprocess per actor-flow
invocation is far too slow — DBOS boot costs ~555s of *virtual* time, and an
actor may run a flow several times. So `make_sut` starts ONE persistent
subprocess (the same venv python) that boots DBOS exactly once, then serves
commands over a line protocol: the driver writes one JSON request per line to
its stdin and a reader thread matches `WIORESP <json>` replies by id. Requests
run in their own server-side thread, so concurrent actors exercise real
concurrent DBOS workflow calls in one process — the faithful model, and fast.

The exactly-once oracle rides process-global side-effect counters in the server
(STEP_RUNS/TASK_RUNS), keyed by the per-invocation-unique ids the driver sends.
The crash-restart event is injected the vendor's way inside a request (force the
request's SUCCESS rows to PENDING + `_recover_pending_workflows()`), so the
counters survive crash-and-recover and a re-run step shows up as count > 1.
"""
from __future__ import annotations

import itertools
import json
import os
import subprocess
import sys
import threading
from urllib.parse import quote


def _pg_pw() -> str:
    return quote(os.environ.get("PGPASSWORD", "dbos"), safe="")


# --------------------------------------------------------------------------- #
# The persistent server: boots DBOS once, serves JSON commands on stdin.
#   request  {id, cmd: "warmup"|"durable"|"enqueue", ...}
#   reply    WIORESP {id, facts}
# --------------------------------------------------------------------------- #
# CAP_N: the concurrency cap for the two-executor cap-under-recovery scenario.
# Fixed here AND in EnqueueTaskFlow (they must agree — the queue is registered at
# boot with this concurrency and the flow declares the same N to the oracle).
CAP_N = 4
CAP_GATE = 4242  # the Postgres advisory-lock id used as the cross-process barrier

# Shared cap machinery: registered identically in BOTH executor A (SRV_SRC) and
# executor B (EXEC_B_SRC) so recovery in B can look up and re-run cap_wf. The
# gauge/barrier live in the APP database (a plain SQLAlchemy engine, NOT the DBOS
# system db), so both OS processes share one cluster-wide concurrency gauge and
# one advisory-lock gate. The gate is a Postgres-side block (immune to the
# sandbox's virtual-time shim) — A holds it EXCLUSIVE; each cap step waits on a
# SHARED lock, so all bodies release together the instant A opens the gate.
CAP_DEFS = r'''
import sqlalchemy as _sa
# Use psycopg3 (installed) for the gauge engine; the bare app_url would default
# to psycopg2, which is not in the venv.
_appurl = CFG["app_url"].replace("postgresql://", "postgresql+psycopg://", 1)
_geng = _sa.create_engine(_appurl, pool_size=CAP_N * 4 + 8, max_overflow=16)

def _gauge_setup():
    with _geng.begin() as c:
        c.exec_driver_sql("CREATE TABLE IF NOT EXISTS wio_cap_gauge (id int primary key, cur int, mx int)")

def _gauge_reset():
    with _geng.begin() as c:
        c.exec_driver_sql("INSERT INTO wio_cap_gauge(id,cur,mx) VALUES(1,0,0) "
                          "ON CONFLICT(id) DO UPDATE SET cur=0, mx=0")

def _gauge_read():
    with _geng.begin() as c:
        row = c.exec_driver_sql("SELECT cur,mx FROM wio_cap_gauge WHERE id=1").fetchone()
    return (row[0], row[1]) if row else (0, 0)

# Coordination between A and B is DB-STATE-driven, never time-driven: both
# processes run under the sandbox's virtual clock (sleeps fast-forward), so A
# cannot "wait N seconds" for B's real ~20s boot. Instead A blocks on the flags B
# sets, and B — whose own re-dispatched bodies block at the same gate — measures
# the gauge peak itself and reports it.
def _coord_setup():
    with _geng.begin() as c:
        c.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS wio_cap_coord "
            "(id int primary key, b_ready bool, b_done bool, b_recovered int, b_saw_max int)")

def _coord_reset():
    with _geng.begin() as c:
        c.exec_driver_sql(
            "INSERT INTO wio_cap_coord(id,b_ready,b_done,b_recovered,b_saw_max) "
            "VALUES(1,false,false,0,0) ON CONFLICT(id) DO UPDATE SET "
            "b_ready=false,b_done=false,b_recovered=0,b_saw_max=0")

def _coord_read():
    with _geng.begin() as c:
        row = c.exec_driver_sql(
            "SELECT b_ready,b_done,b_recovered,b_saw_max FROM wio_cap_coord WHERE id=1").fetchone()
    return row if row else (False, False, 0, 0)

@DBOS.step()
def cap_block(token):
    # Enter: bump the cluster gauge (row lock serializes concurrent bumps).
    with _geng.begin() as c:
        c.exec_driver_sql("UPDATE wio_cap_gauge SET cur=cur+1, mx=GREATEST(mx,cur+1) WHERE id=1")
    # Wait at the gate: block on the SHARED advisory lock until A drops EXCLUSIVE.
    # This is a real Postgres wait, so the virtual-time shim cannot fast-forward
    # past it — the body genuinely stays live (holding a cap slot) meanwhile.
    with _geng.connect() as c:
        c.exec_driver_sql("SELECT pg_advisory_lock_shared(%s)" % CAP_GATE)
        c.exec_driver_sql("SELECT pg_advisory_unlock_shared(%s)" % CAP_GATE)
    # Exit: drop the gauge.
    with _geng.begin() as c:
        c.exec_driver_sql("UPDATE wio_cap_gauge SET cur=cur-1 WHERE id=1")
    return "ok"

@DBOS.workflow()
def cap_wf(token):
    return cap_block(token)
'''
# NB: cap_queue (Queue("wio_cap_q", ...)) is created AFTER DBOS(config=...) is
# instantiated in each process (like the main queue) — a Queue built before the
# DBOS instance exists hangs boot. See the launch blocks in SRV_SRC/EXEC_B_SRC.

SRV_SRC = ("""
import json, os, sys, subprocess, threading, time

CFG = json.loads(os.environ["WIO_CFG"])
CAP_N = %d
CAP_GATE = %d
STEP_RUNS = {}
TASK_RUNS = {}

from dbos import DBOS, Queue, SetWorkflowID, SetEnqueueOptions

DBOS.destroy(destroy_registry=True)

@DBOS.step()
def wio_step(tag, i):
    k = tag + ":" + str(i)
    STEP_RUNS[k] = STEP_RUNS.get(k, 0) + 1
    return i

@DBOS.workflow()
def wio_durable_workflow(tag, n):
    for i in range(n):
        wio_step(tag, i)
    return tag + ":done:" + str(n)

@DBOS.step()
def wio_task_step(label):
    TASK_RUNS[label] = TASK_RUNS.get(label, 0) + 1
    return label + ":ok"

@DBOS.workflow()
def wio_task(label):
    return wio_task_step(label)
""" % (CAP_N, CAP_GATE)) + CAP_DEFS + r'''
config = {
    "name": "wioapp",
    "application_database_url": CFG["app_url"],
    "system_database_url": CFG["sys_url"],
    "enable_otlp": False,
    "notification_listener_polling_interval_sec": 0.02,
}
inst = DBOS(config=config)
# Fast queue polling: under the deterministic sandbox each poll sleep fast-forwards
# virtual time, so the default 1.0s interval makes a multi-actor enqueue request
# consume tens of virtual seconds and trip the interleave scheduler's step timeout.
queue = Queue("wio_queue", concurrency=4, polling_interval_sec=0.05)
cap_queue = Queue("wio_cap_q", concurrency=CAP_N, worker_concurrency=CAP_N,
                  polling_interval_sec=0.05)
DBOS.launch()

_out = threading.Lock()
def emit(obj):
    with _out:
        sys.stdout.write("WIORESP " + json.dumps(obj) + "\n")
        sys.stdout.flush()

def crash_and_recover(wfids):
    import sqlalchemy as sa
    from dbos._schemas.system_database import SystemSchema
    T = SystemSchema.workflow_status
    with inst._sys_db.engine.begin() as c:
        c.execute(sa.update(T).values({"status": "PENDING"})
                  .where(T.c.status == "SUCCESS").where(T.c.workflow_uuid.in_(wfids)))
    DBOS._recover_pending_workflows()

def wait_terminal(wfids, deadline_s=25.0):
    end = time.monotonic() + deadline_s
    term = ("SUCCESS", "ERROR", "CANCELLED", "MAX_RECOVERY_ATTEMPTS_EXCEEDED")
    while time.monotonic() < end:
        sts = [DBOS.get_workflow_status(w) for w in wfids]
        if all(s and s.status in term for s in sts):
            return
        time.sleep(0.1)

def do_durable(req):
    items = req["items"]
    wfids = [it["wfid"] for it in items]
    for it in items:
        with SetWorkflowID(it["wfid"]):
            wio_durable_workflow(it["wfid"], it["n"])
    if req.get("crash"):
        crash_and_recover(wfids)
        wait_terminal(wfids)
    wf = {}
    for it in items:
        wfid = it["wfid"]
        st = DBOS.get_workflow_status(wfid)
        status = st.status if st else None
        try:
            res = DBOS.retrieve_workflow(wfid).get_result() if status == "SUCCESS" else None
        except Exception:
            res = None
        steps = {str(i): STEP_RUNS.get(wfid + ":" + str(i), 0) for i in range(it["n"])}
        wf[wfid] = {"status": status, "result": res, "steps": steps}
    return {"workflows": wf}

def do_enqueue(req):
    base, k = req["base"], req["k"]
    labels = [base + ":" + str(j) for j in range(k)]
    # Set the workflow id per enqueue so it equals the label: get_workflow_status
    # and the crash reset can address the task, and the handle result is captured.
    handles = []
    for lb in labels:
        with SetWorkflowID(lb):
            handles.append((lb, queue.enqueue(wio_task, lb)))
    dd_id = base + ":dd"
    dd_label = base + ":dedup"
    # A deduplication id is set via SetEnqueueOptions (a context manager), not an
    # enqueue kwarg. The refused duplicate is enqueued while the first is still
    # in flight (before any get_result), so the live dedup id must reject it.
    with SetWorkflowID(dd_label):
        with SetEnqueueOptions(deduplication_id=dd_id):
            h_first = queue.enqueue(wio_task, dd_label)
    refused_label = base + ":dedup-dup"
    refused = False
    refused_err = None
    try:
        with SetWorkflowID(refused_label):
            with SetEnqueueOptions(deduplication_id=dd_id):
                queue.enqueue(wio_task, refused_label)
    except Exception as e:
        refused = True
        refused_err = repr(e)
    allids = labels + [dd_label]
    results = {}
    for lb, h in handles:
        try:
            results[lb] = h.get_result(polling_interval_sec=0.05)
        except Exception as e:
            results[lb] = {"_err": repr(e)}
    try:
        first_res = h_first.get_result(polling_interval_sec=0.05)
    except Exception as e:
        first_res = {"_err": repr(e)}
    if req.get("crash"):
        crash_and_recover(allids)
        wait_terminal(allids, deadline_s=60.0)
        for lb, h in handles:
            try: results[lb] = DBOS.retrieve_workflow(lb).get_result(polling_interval_sec=0.05)
            except Exception: pass
    tasks = {}
    for lb in labels:
        st = DBOS.get_workflow_status(lb)
        tasks[lb] = {"result": results.get(lb), "runs": TASK_RUNS.get(lb, 0),
                     "status": (st.status if st else None)}
    return {
        "tasks": tasks,
        "dedup": {
            "first": {"result": first_res, "runs": TASK_RUNS.get(dd_label, 0)},
            "refused": refused,
            "refused_err": refused_err,
            "refused_runs": TASK_RUNS.get(refused_label, 0),
            "refused_label": refused_label,
            "dd_label": dd_label,
        },
    }

def do_caprace(req):
    # Two-executor concurrency-cap-under-recovery probe. THIS process is executor
    # A (DBOS__VMID=wioA). Fill the cap with n blocked cap_wf, then spawn executor
    # B (DBOS__VMID=wioB) which recovers A's still-running queued rows; measure the
    # cluster-wide gauge peak. A cap of n but a peak > n is the violation.
    n = req["n"]
    nonce = req.get("nonce", "x")
    _gauge_setup(); _gauge_reset()
    _coord_setup(); _coord_reset()

    # Hold the gate EXCLUSIVE on a dedicated connection kept open for the whole
    # race, so every cap_block (on A and on B) blocks at the shared lock.
    gate_conn = _geng.connect()
    gate_conn.exec_driver_sql("SELECT pg_advisory_lock(%s)" % CAP_GATE)
    try:
        wfids = []
        for j in range(n):
            wid = "cap-%s-%d" % (nonce, j)
            with SetWorkflowID(wid):
                cap_queue.enqueue(cap_wf, wid)
            wfids.append(wid)

        # Wait until A has dequeued and entered all n (cap full) — gauge.cur == n.
        # A's own dispatch is prompt, so a bounded spin is fine here.
        cur_full = 0
        for _ in range(200000):
            cur, mx = _gauge_read()
            if cur >= n:
                cur_full = cur
                break
            time.sleep(0.02)

        # Spawn executor B: a second live DBOS on the SAME databases, VMID=wioB.
        bproc = subprocess.Popen(
            [sys.executable, "-c", os.environ["WIO_EXEC_B"]],
            env={**os.environ, "DBOS__VMID": "wioB"},
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
        )
        # Block on B's DB flags (NOT a virtual timer): B boots (~20s real), recovers
        # A's live rows, its poller re-dispatches them (they block at THIS gate,
        # bumping the gauge), B measures the peak and sets b_done. The spin is real-
        # time-bounded only as a crash backstop (~a few hundred k DB reads).
        b_ready = b_done = False
        b_rec = b_saw = 0
        for _ in range(2_000_000):
            b_ready, b_done, b_rec, b_saw = _coord_read()
            if b_done:
                break
            if bproc.poll() is not None:
                break  # B died
            time.sleep(0.01)
        _, gauge_max = _gauge_read()
    finally:
        # Open the gate: all blocked bodies (A's and B's) proceed and finish.
        gate_conn.exec_driver_sql("SELECT pg_advisory_unlock(%s)" % CAP_GATE)
        gate_conn.close()

    wait_terminal(wfids, deadline_s=120.0)
    try:
        bproc.stdin.write("quit\n"); bproc.stdin.flush()
        bproc.wait(timeout=15)
    except Exception:
        try:
            bproc.kill()
        except Exception:
            pass
    states = {}
    for w in wfids:
        st = DBOS.get_workflow_status(w)
        states[w] = st.status if st else None
    b_err = ""
    try:
        if bproc.poll() is not None and bproc.poll() != 0:
            b_err = (bproc.stderr.read() or "")[-600:]
    except Exception:
        pass
    return {"cap": n, "gauge_max": gauge_max, "cur_full": cur_full,
            "b_ready": bool(b_ready), "b_done": bool(b_done),
            "b_recovered": b_rec, "b_saw_max": b_saw,
            "states": states, "b_exit": bproc.poll(), "b_err": b_err}


def handle(req):
    rid = req.get("id")
    try:
        cmd = req["cmd"]
        if cmd == "warmup":
            with SetWorkflowID("warmup-canary-" + str(rid)):
                r = wio_durable_workflow("warmup-canary-" + str(rid), 1)
            facts = {"warmup": r}
        elif cmd == "durable":
            facts = do_durable(req)
        elif cmd == "enqueue":
            facts = do_enqueue(req)
        elif cmd == "caprace":
            facts = do_caprace(req)
        else:
            facts = {"error": "unknown cmd " + str(cmd)}
    except Exception as e:
        facts = {"error": repr(e)}
    emit({"id": rid, "facts": facts})

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
    except Exception:
        continue
    if req.get("cmd") == "quit":
        break
    threading.Thread(target=handle, args=(req,), daemon=True).start()

try:
    DBOS.destroy(destroy_registry=True)
except Exception:
    pass
'''


# --------------------------------------------------------------------------- #
# Executor B: a SECOND live DBOS on the SAME databases (DBOS__VMID=wioB). It
# registers the SAME cap_wf so recovery can re-run it, boots, recovers executor
# A's still-running queued rows, then idles (its queue poller re-dispatches the
# recovered rows) until told to quit. Passed to A via WIO_EXEC_B.
# --------------------------------------------------------------------------- #
EXEC_B_SRC = ("""
import json, os, sys, time

CFG = json.loads(os.environ["WIO_CFG"])
CAP_N = %d
CAP_GATE = %d

from dbos import DBOS, Queue, SetWorkflowID

DBOS.destroy(destroy_registry=True)
""" % (CAP_N, CAP_GATE)) + CAP_DEFS + r'''
config = {
    "name": "wioapp",
    "application_database_url": CFG["app_url"],
    "system_database_url": CFG["sys_url"],
    "enable_otlp": False,
    "notification_listener_polling_interval_sec": 0.02,
}
inst = DBOS(config=config)
cap_queue = Queue("wio_cap_q", concurrency=CAP_N, worker_concurrency=CAP_N,
                  polling_interval_sec=0.05)
DBOS.launch()
_nrec = -1
try:
    _rec = DBOS._recover_pending_workflows(["wioA"])
    _nrec = len(_rec)
    sys.stderr.write("B recovered %d\n" % (_nrec,))
    sys.stderr.flush()
except Exception as e:
    sys.stderr.write("B recover error: %r\n" % (e,))
    sys.stderr.flush()
with _geng.begin() as _c:
    _c.exec_driver_sql("UPDATE wio_cap_coord SET b_ready=true, b_recovered=%d WHERE id=1" % _nrec)
# B's poller re-dispatches the recovered rows; they run cap_block on B and block
# at the shared gate (held by A), bumping the cluster gauge. Watch the peak until
# a breach (mx > CAP_N) or a bounded backstop, then report it to A via b_done.
_saw = 0
for _i in range(2_000_000):
    _cur, _mx = _gauge_read()
    if _mx > _saw:
        _saw = _mx
    if _mx > CAP_N:
        break
    time.sleep(0.01)
with _geng.begin() as _c:
    _c.exec_driver_sql("UPDATE wio_cap_coord SET b_done=true, b_saw_max=%d WHERE id=1" % _saw)
sys.stderr.write("B done, saw_max=%d\n" % (_saw,))
sys.stderr.flush()
for _line in sys.stdin:
    if _line.strip() == "quit":
        break
try:
    DBOS.destroy(destroy_registry=True)
except Exception:
    pass
'''


# --------------------------------------------------------------------------- #
# The SUT: config holder + persistent server client. No in-process DBOS.
# --------------------------------------------------------------------------- #
class DbosSUT:
    def __init__(self, meta, seed: int):
        self.seed = seed
        self.db = f"wio_{seed}"
        pw = _pg_pw()
        self.app_url = f"postgresql://postgres:{pw}@localhost:5432/{self.db}"
        self.sys_url = f"postgresql+psycopg://postgres:{pw}@localhost:5432/{self.db}_dbos_sys"
        self.maint_url = f"postgresql://postgres:{pw}@localhost:5432/postgres"
        self.crash_armed = False
        self.faildeath_armed = False

        self._resp: dict = {}
        self._events: dict = {}
        self._lock = threading.Lock()
        self._wlock = threading.Lock()
        self._ids = itertools.count()

        self._ensure_databases()
        cfg = {"app_url": self.app_url, "sys_url": self.sys_url}
        self.proc = subprocess.Popen(
            [sys.executable, "-c", SRV_SRC],
            env={**os.environ, "WIO_CFG": json.dumps(cfg),
                 "DBOS__VMID": "wioA", "WIO_EXEC_B": EXEC_B_SRC},
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
        )
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()
        # Warmup: the first request pays the one-time DBOS boot; a large timeout
        # covers the ~555s-virtual boot. Failure here is a clean setup-block.
        facts = self.request({"cmd": "warmup"}, timeout=1500)
        if "warmup" not in facts:
            raise RuntimeError(f"DBOS server failed to boot: {facts!r} :: {self._stderr_tail()}")

    def _ensure_databases(self):
        import psycopg

        for db in (self.db, f"{self.db}_dbos_sys"):
            with psycopg.connect(self.maint_url, autocommit=True) as conn:
                row = conn.execute(
                    "SELECT 1 FROM pg_database WHERE datname=%s", (db,)
                ).fetchone()
                if not row:
                    conn.execute(f'CREATE DATABASE "{db}"')

    def _read_loop(self):
        for line in self.proc.stdout:
            if not line.startswith("WIORESP "):
                continue
            try:
                obj = json.loads(line[len("WIORESP "):])
            except Exception:
                continue
            rid = obj.get("id")
            with self._lock:
                self._resp[rid] = obj.get("facts") or {}
                ev = self._events.get(rid)
            if ev is not None:
                ev.set()

    def _stderr_tail(self) -> str:
        try:
            if self.proc.poll() is not None:
                return (self.proc.stderr.read() or "")[-2500:]
        except Exception:
            pass
        return "(server still running)"

    def request(self, cmd: dict, timeout: float = 1500) -> dict:
        rid = next(self._ids)
        ev = threading.Event()
        with self._lock:
            self._events[rid] = ev
        payload = json.dumps({**cmd, "id": rid})
        with self._wlock:
            self.proc.stdin.write(payload + "\n")
            self.proc.stdin.flush()
        if not ev.wait(timeout):
            raise RuntimeError(f"server request {rid} timed out :: {self._stderr_tail()}")
        with self._lock:
            self._events.pop(rid, None)
            return self._resp.pop(rid, {})

    def stop(self):
        try:
            with self._wlock:
                self.proc.stdin.write('{"cmd":"quit"}\n')
                self.proc.stdin.flush()
            self.proc.wait(timeout=30)
        except Exception:
            try:
                self.proc.kill()
            except Exception:
                pass


def make_sut(meta, seed):
    return DbosSUT(meta, seed)


# --------------------------------------------------------------------------- #
# Flow drivers
# --------------------------------------------------------------------------- #
class DurableWorkflowFlow:
    key = "durable-workflow"
    invariants = ("step-exactly-once", "resumes-after-crash", "workflow-terminal")
    documented: dict = {}
    bounds: dict = {}

    N = 3

    def run(self, ctx):
        sut = ctx.sut
        # Unique per invocation (an actor may run this flow several times; a reused
        # wfid hits DBOS idempotency and a fresh oracle would see 0 step-runs).
        tag = f"wf-{ctx.actor_id}-{sut.seed}-{ctx.rng.randrange(1_000_000_000)}"
        expected = f"{tag}:done:{self.N}"

        ctx.step("submit")  # a crash-restart event, if armed, flips sut.crash_armed here
        facts = sut.request(
            {"cmd": "durable", "items": [{"wfid": tag, "n": self.N}], "crash": sut.crash_armed}
        )
        wf = (facts.get("workflows") or {}).get(tag, {})

        ctx.ledger.acked("workflow-result", tag, expected)
        for i in range(self.N):
            ctx.ledger.acked("step-runs", f"{tag}:{i}", 1)

        ctx.step("crashpoint")

        status = wf.get("status")
        ctx.ledger.observe(
            "workflow-result", tag, value=wf.get("result"), present=(status == "SUCCESS")
        )
        steps = wf.get("steps") or {}
        for i in range(self.N):
            runs = steps.get(str(i), 0)
            ctx.ledger.observe("step-runs", f"{tag}:{i}", value=runs, present=(runs >= 1))
        ctx.step("done")


class EnqueueTaskFlow:
    key = "enqueue-task"
    invariants = ("task-completes-once", "dedup-id-enforced", "queue-concurrency-capped")
    documented: dict = {}
    bounds: dict = {}

    K = 3
    N = 4  # cap for the false-death-recovery race; MUST equal SRV_SRC CAP_N

    def run(self, ctx):
        sut = ctx.sut
        if sut.faildeath_armed:
            return self._run_cap_race(ctx)
        base = f"task-{ctx.actor_id}-{sut.seed}-{ctx.rng.randrange(1_000_000_000)}"

        ctx.step("enqueue")
        facts = sut.request({"cmd": "enqueue", "base": base, "k": self.K, "crash": sut.crash_armed})
        tasks = facts.get("tasks") or {}
        dedup = facts.get("dedup") or {}

        for j in range(self.K):
            label = f"{base}:{j}"
            ctx.ledger.acked("task-result", label, f"{label}:ok")
        ctx.step("collect")
        for j in range(self.K):
            label = f"{base}:{j}"
            t = tasks.get(label, {})
            present = t.get("runs") == 1 and t.get("result") == f"{label}:ok"
            ctx.ledger.observe("task-result", label, value=t.get("result"), present=present)

        dd_label = dedup.get("dd_label", f"{base}:dedup")
        first = dedup.get("first", {})
        ctx.ledger.acked("task-result", dd_label, f"{dd_label}:ok")
        ctx.ledger.observe(
            "task-result", dd_label, value=first.get("result"),
            present=(first.get("runs") == 1 and first.get("result") == f"{dd_label}:ok"),
        )

        refused_label = dedup.get("refused_label", f"{base}:dedup-dup")
        if dedup.get("refused"):
            ctx.ledger.denied("task-result", refused_label, "deduplicated")
        else:
            ctx.ledger.acked("task-result", refused_label, f"{refused_label}:ok")
        ran = dedup.get("refused_runs", 0)
        ctx.ledger.observe("task-result", refused_label, value=None, present=(ran > 0))
        ctx.step("done")

    def _run_cap_race(self, ctx):
        # false-death-recovery: a queue with declared concurrency N must never run
        # more than N bodies at once across the cluster, even while a second
        # executor recovers this one's in-flight queued rows. The oracle is a
        # cluster-wide gauge peak, expressed as a DENY (the cap forbids > N).
        sut = ctx.sut
        n = self.N
        nonce = f"{ctx.actor_id}-{sut.seed}-{ctx.rng.randrange(1_000_000_000)}"
        qkey = "wio_cap_q"

        ctx.step("fill-cap")
        facts = sut.request({"cmd": "caprace", "n": n, "nonce": nonce}, timeout=1500)
        gauge_max = facts.get("gauge_max")
        try:
            import json as _json
            print("WIODIAG caprace " + _json.dumps({
                "cap": n, "gauge_max": gauge_max, "cur_full": facts.get("cur_full"),
                "b_ready": facts.get("b_ready"), "b_done": facts.get("b_done"),
                "b_recovered": facts.get("b_recovered"), "b_saw_max": facts.get("b_saw_max"),
                "b_exit": facts.get("b_exit"),
                "b_err": (facts.get("b_err") or "")[-300:],
            }), flush=True)
        except Exception:
            pass

        # The cap forbids more than n concurrent bodies. If the peak exceeded n,
        # the denied thing happened -> RED.
        ctx.ledger.denied("cap-breach", qkey, f"concurrency={n}")
        ctx.step("observe")
        breached = isinstance(gauge_max, int) and gauge_max > n
        ctx.ledger.observe("cap-breach", qkey, value=gauge_max, present=breached)
        ctx.step("done")


FLOWS = {
    "durable-workflow": DurableWorkflowFlow,
    "enqueue-task": EnqueueTaskFlow,
}


def fire_crash_restart(sut):
    sut.crash_armed = True


def fire_false_death(sut):
    sut.faildeath_armed = True


EVENTS = {"crash-restart": fire_crash_restart, "false-death-recovery": fire_false_death}
