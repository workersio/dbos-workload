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
SRV_SRC = r'''
import json, os, sys, threading, time

CFG = json.loads(os.environ["WIO_CFG"])
STEP_RUNS = {}
TASK_RUNS = {}

from dbos import DBOS, Queue, SetWorkflowID

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

config = {
    "name": "wioapp",
    "application_database_url": CFG["app_url"],
    "system_database_url": CFG["sys_url"],
    "enable_otlp": False,
    "notification_listener_polling_interval_sec": 0.02,
}
inst = DBOS(config=config)
queue = Queue("wio_queue", concurrency=4)
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
    with SetWorkflowID(dd_label):
        h_first = queue.enqueue(wio_task, dd_label, deduplication_id=dd_id)
    refused_label = base + ":dedup-dup"
    refused = False
    refused_err = None
    try:
        with SetWorkflowID(refused_label):
            queue.enqueue(wio_task, refused_label, deduplication_id=dd_id)
    except Exception as e:
        refused = True
        refused_err = repr(e)
    allids = labels + [dd_label]
    results = {}
    for lb, h in handles:
        try:
            results[lb] = h.get_result()
        except Exception as e:
            results[lb] = {"_err": repr(e)}
    try:
        first_res = h_first.get_result()
    except Exception as e:
        first_res = {"_err": repr(e)}
    if req.get("crash"):
        crash_and_recover(allids)
        wait_terminal(allids, deadline_s=60.0)
        for lb, h in handles:
            try: results[lb] = DBOS.retrieve_workflow(lb).get_result()
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

        self._resp: dict = {}
        self._events: dict = {}
        self._lock = threading.Lock()
        self._wlock = threading.Lock()
        self._ids = itertools.count()

        self._ensure_databases()
        cfg = {"app_url": self.app_url, "sys_url": self.sys_url}
        self.proc = subprocess.Popen(
            [sys.executable, "-c", SRV_SRC],
            env={**os.environ, "WIO_CFG": json.dumps(cfg)},
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
                return (self.proc.stderr.read() or "")[-600:]
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
    invariants = ("task-completes-once", "dedup-id-enforced")
    documented: dict = {}
    bounds: dict = {}

    K = 3

    def run(self, ctx):
        sut = ctx.sut
        base = f"task-{ctx.actor_id}-{sut.seed}-{ctx.rng.randrange(1_000_000_000)}"

        ctx.step("enqueue")
        facts = sut.request({"cmd": "enqueue", "base": base, "k": self.K, "crash": sut.crash_armed})
        tasks = facts.get("tasks") or {}
        dedup = facts.get("dedup") or {}
        print("WIODIAG enqueue " + json.dumps(facts)[:900], flush=True)  # TEMP diagnostic

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


FLOWS = {
    "durable-workflow": DurableWorkflowFlow,
    "enqueue-task": EnqueueTaskFlow,
}


def fire_crash_restart(sut):
    sut.crash_armed = True


EVENTS = {"crash-restart": fire_crash_restart}
