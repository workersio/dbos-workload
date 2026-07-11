#!/usr/bin/env python3
"""Flow drivers for the DBOS Transact (Python) usage model.

Executor-owned. One driver class per model flow (check.py G2 bijection).

ARCHITECTURE (why a subprocess): the run_scenario spine runs flow drivers
in-process, but a live DBOS instance cannot boot in the spine's process under the
deterministic virtual-time sandbox — DBOS's background daemon threads (queue,
recovery, notification pollers) advance virtual time past the liveness watchdog
and the run hangs. So each unit of DBOS work runs in a short-lived SUBPROCESS
(the same venv python, `sys.executable`) that boots DBOS, does the work, reports
machine-readable facts on stdout (`WIOFACT <json>`), and exits — the v1 corpus's
proven pattern (git fa50292 workloads run DBOS via an embedded APP module). The
driver parses those facts and feeds the persona-ledger oracle in-process.

The exactly-once oracle rides process-global side-effect counters inside the app
subprocess (STEP_RUNS/TASK_RUNS). Because the crash is simulated *within* that
same subprocess (force SUCCESS rows to PENDING + `_recover_pending_workflows()`,
the vendor's own injection at tests/test_dbos.py:425-433), the counters survive
the crash-and-recover and a re-run step shows up as count > 1 — an `acked_mutated`
violation the ledger reports as RED.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from urllib.parse import quote


def _pg_pw() -> str:
    return quote(os.environ.get("PGPASSWORD", "dbos"), safe="")


# --------------------------------------------------------------------------- #
# The embedded DBOS app. Runs one "spec" of work and prints one WIOFACT line.
# spec (via WIO_SPEC env, JSON):
#   {kind: "warmup"|"durable"|"enqueue", app_url, sys_url, crash: bool,
#    items: [...]}   # durable: [{wfid, n}]   enqueue: {base, k}
# --------------------------------------------------------------------------- #
APP_SRC = textwrap.dedent(
    '''
    import json, os, sys, time

    SPEC = json.loads(os.environ["WIO_SPEC"])
    KIND = SPEC["kind"]

    STEP_RUNS = {}
    TASK_RUNS = {}

    from dbos import DBOS, Queue, SetWorkflowID

    DBOS.destroy(destroy_registry=True)

    @DBOS.step()
    def wio_step(tag, i):
        STEP_RUNS[tag + ":" + str(i)] = STEP_RUNS.get(tag + ":" + str(i), 0) + 1
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
        "application_database_url": SPEC["app_url"],
        "system_database_url": SPEC["sys_url"],
        "enable_otlp": False,
        "notification_listener_polling_interval_sec": 0.01,
    }
    inst = DBOS(config=config)
    queue = Queue("wio_queue", concurrency=4)
    DBOS.launch()

    def crash_and_recover():
        import sqlalchemy as sa
        from dbos._schemas.system_database import SystemSchema
        with inst._sys_db.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .values({"status": "PENDING"})
                .where(SystemSchema.workflow_status.c.status == "SUCCESS")
            )
        DBOS._recover_pending_workflows()

    def wait_terminal(wfids, deadline_s=20.0):
        end = time.monotonic() + deadline_s
        while time.monotonic() < end:
            sts = [DBOS.get_workflow_status(w) for w in wfids]
            if all(s and s.status in ("SUCCESS", "ERROR", "CANCELLED",
                                      "MAX_RECOVERY_ATTEMPTS_EXCEEDED") for s in sts):
                return
            time.sleep(0.1)

    facts = {}
    try:
        if KIND == "warmup":
            # boot canary: launch + a trivial workflow proves the whole path.
            with SetWorkflowID("warmup-canary"):
                r = wio_durable_workflow("warmup-canary", 1)
            facts = {"warmup": r}

        elif KIND == "durable":
            wfids = []
            for it in SPEC["items"]:
                wfid, n = it["wfid"], it["n"]
                wfids.append(wfid)
                with SetWorkflowID(wfid):
                    wio_durable_workflow(wfid, n)
            if SPEC.get("crash"):
                crash_and_recover()
                wait_terminal(wfids)
            wf = {}
            for it in SPEC["items"]:
                wfid = it["wfid"]
                st = DBOS.get_workflow_status(wfid)
                status = st.status if st else None
                try:
                    res = DBOS.retrieve_workflow(wfid).get_result() if status == "SUCCESS" else None
                except Exception:
                    res = None
                steps = {str(i): STEP_RUNS.get(wfid + ":" + str(i), 0) for i in range(it["n"])}
                wf[wfid] = {"status": status, "result": res, "steps": steps}
            facts = {"workflows": wf}

        elif KIND == "enqueue":
            base, k = SPEC["base"], SPEC["k"]
            labels = [base + ":" + str(j) for j in range(k)]
            handles = [(lb, queue.enqueue(wio_task, lb)) for lb in labels]

            dd_label = base + ":dedup"
            h_first = queue.enqueue(wio_task, dd_label, deduplication_id="dd")
            refused_label = base + ":dedup-dup"
            refused = False
            try:
                queue.enqueue(wio_task, refused_label, deduplication_id="dd")
            except Exception:
                refused = True

            allids = labels + [dd_label]
            for lb, h in handles:
                try:
                    h.get_result()
                except Exception:
                    pass
            try:
                h_first.get_result()
            except Exception:
                pass
            if SPEC.get("crash"):
                crash_and_recover()
            wait_terminal(allids)

            tasks = {}
            for lb in labels:
                st = DBOS.get_workflow_status(lb)
                try:
                    res = DBOS.retrieve_workflow(lb).get_result() if st and st.status == "SUCCESS" else None
                except Exception:
                    res = None
                tasks[lb] = {"result": res, "runs": TASK_RUNS.get(lb, 0)}
            st = DBOS.get_workflow_status(dd_label)
            try:
                first_res = DBOS.retrieve_workflow(dd_label).get_result() if st and st.status == "SUCCESS" else None
            except Exception:
                first_res = None
            facts = {
                "tasks": tasks,
                "dedup": {
                    "first": {"result": first_res, "runs": TASK_RUNS.get(dd_label, 0)},
                    "refused": refused,
                    "refused_runs": TASK_RUNS.get(refused_label, 0),
                    "refused_label": refused_label,
                    "dd_label": dd_label,
                },
            }
        print("WIOFACT " + json.dumps(facts), flush=True)
    finally:
        try:
            DBOS.destroy(destroy_registry=True)
        except Exception:
            pass
    '''
)


# --------------------------------------------------------------------------- #
# The SUT: config holder + subprocess launcher. No in-process DBOS.
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
        self._ensure_databases()
        # Warmup canary: validates the whole boot path once (in a subprocess) and
        # creates the DBOS schema so concurrent actor subprocesses skip migration.
        facts = self.run_app({"kind": "warmup"})
        if "warmup" not in facts:
            raise RuntimeError(f"warmup canary produced no fact: {facts!r}")

    def _ensure_databases(self):
        import psycopg

        for db in (self.db, f"{self.db}_dbos_sys"):
            with psycopg.connect(self.maint_url, autocommit=True) as conn:
                row = conn.execute(
                    "SELECT 1 FROM pg_database WHERE datname=%s", (db,)
                ).fetchone()
                if not row:
                    conn.execute(f'CREATE DATABASE "{db}"')

    def run_app(self, spec: dict) -> dict:
        spec = {**spec, "app_url": self.app_url, "sys_url": self.sys_url}
        env = {**os.environ, "WIO_SPEC": json.dumps(spec)}
        proc = subprocess.run(
            [sys.executable, "-c", APP_SRC],
            env=env,
            capture_output=True,
            text=True,
            # NOTE: this timeout is measured in the sandbox's VIRTUAL time, and a
            # DBOS boot (migration + startup recovery + launch) costs ~555s virtual
            # even though it is ~20s real — so this must sit well above one boot.
            timeout=1200,
        )
        facts = {}
        for line in proc.stdout.splitlines():
            if line.startswith("WIOFACT "):
                facts = json.loads(line[len("WIOFACT "):])
        if not facts and proc.returncode != 0:
            tail = (proc.stderr or "")[-800:]
            raise RuntimeError(
                f"DBOS app failed (rc={proc.returncode}) spec={spec.get('kind')}: {tail}"
            )
        return facts

    def stop(self):
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
        tag = f"wf-{ctx.actor_id}-{sut.seed}"
        expected = f"{tag}:done:{self.N}"

        ctx.step("submit")  # a crash-restart event, if armed, flips sut.crash_armed here
        facts = sut.run_app(
            {"kind": "durable", "items": [{"wfid": tag, "n": self.N}], "crash": sut.crash_armed}
        )
        wf = (facts.get("workflows") or {}).get(tag, {})

        # The product promised: this workflow completes with `expected` and each
        # step runs exactly once, durably (survives the crash).
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
        base = f"task-{ctx.actor_id}-{sut.seed}"

        ctx.step("enqueue")
        facts = sut.run_app(
            {"kind": "enqueue", "base": base, "k": self.K, "crash": sut.crash_armed}
        )
        tasks = facts.get("tasks") or {}
        dedup = facts.get("dedup") or {}

        # Each enqueued task: promised to complete once with its result.
        for j in range(self.K):
            label = f"{base}:{j}"
            ctx.ledger.acked("task-result", label, f"{label}:ok")
        ctx.step("collect")
        for j in range(self.K):
            label = f"{base}:{j}"
            t = tasks.get(label, {})
            present = t.get("runs") == 1 and t.get("result") == f"{label}:ok"
            ctx.ledger.observe("task-result", label, value=t.get("result"), present=present)

        # dedup first task must complete once.
        dd_label = dedup.get("dd_label", f"{base}:dedup")
        first = dedup.get("first", {})
        ctx.ledger.acked("task-result", dd_label, f"{dd_label}:ok")
        ctx.ledger.observe(
            "task-result", dd_label, value=first.get("result"),
            present=(first.get("runs") == 1 and first.get("result") == f"{dd_label}:ok"),
        )

        # The duplicate enqueue must be refused AND never run.
        refused_label = dedup.get("refused_label", f"{base}:dedup-dup")
        if dedup.get("refused"):
            ctx.ledger.denied("task-result", refused_label, "deduplicated")
        else:
            # not refused -> the product silently accepted a duplicate; if it ran,
            # that surfaces as a ledger violation below.
            ctx.ledger.acked("task-result", refused_label, f"{refused_label}:ok")
        ran = dedup.get("refused_runs", 0)
        ctx.ledger.observe("task-result", refused_label, value=None, present=(ran > 0))
        ctx.step("done")


FLOWS = {
    "durable-workflow": DurableWorkflowFlow,
    "enqueue-task": EnqueueTaskFlow,
}


def fire_crash_restart(sut):
    # The event arms the crash; the actual reset+recover happens inside the DBOS
    # app subprocess (where DBOS lives). This keeps the crash faithful while the
    # spine's in-process event timing still decides whether it lands.
    sut.crash_armed = True


EVENTS = {"crash-restart": fire_crash_restart}
