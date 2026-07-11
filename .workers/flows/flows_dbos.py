#!/usr/bin/env python3
"""Flow drivers for the DBOS Transact (Python) usage model.

Executor-owned. One driver class per model flow (check.py G2 bijection). The
Python spine (lib/run_scenario.py) owns seeds, clocks, the persona ledger, and
the INVARIANT lines; these drivers own only *what a real user does* against a
live DBOS instance and *what the product told them* (acks/denials/observations).

Boot mirrors the vendor's own test fixture (tests/conftest.py:default_config):
a real DBOS on the Postgres the runner started (run-with-postgres.sh), app DB +
`postgresql+psycopg` system DB, otlp off. The crash-restart event is injected the
vendor's way (tests/test_dbos.py:425-433): force in-flight rows to PENDING then
`_recover_pending_workflows()`.

The exactly-once oracle rides *process-global* side-effect counters that DBOS
does not checkpoint (STEP_RUNS / TASK_RUNS). A completed step's body must never
run twice, even across recovery, so a re-run is a counter > 1 — directly visible
to the persona ledger as an `acked_mutated` violation.
"""
from __future__ import annotations

import os
from urllib.parse import quote

# --------------------------------------------------------------------------- #
# Process-global side-effect counters (NOT checkpointed by DBOS).
# Keyed so every actor/seed is isolated within one run.
# --------------------------------------------------------------------------- #
STEP_RUNS: dict[str, int] = {}
TASK_RUNS: dict[str, int] = {}


def _pg_pw() -> str:
    return quote(os.environ.get("PGPASSWORD", "dbos"), safe="")


# --------------------------------------------------------------------------- #
# The SUT: one live DBOS instance shared by all actor threads in a run.
# --------------------------------------------------------------------------- #
class DbosSUT:
    """Owns the DBOS lifecycle. Registers the workflows/queue both flows need,
    then launches. `.stop()` destroys it; `.crash_restart()` injects a crash."""

    def __init__(self, meta, seed: int):
        self.seed = seed
        self.db = f"wio_{seed}"
        pw = _pg_pw()
        self.app_url = f"postgresql://postgres:{pw}@localhost:5432/{self.db}"
        self.sys_url = f"postgresql+psycopg://postgres:{pw}@localhost:5432/{self.db}_dbos_sys"
        self._boot()

    def _config(self):
        return {
            "name": "wioapp",
            "application_database_url": self.app_url,
            "system_database_url": self.sys_url,
            "enable_otlp": False,
            "notification_listener_polling_interval_sec": 0.01,
        }

    def _boot(self):
        # Imports here so an import failure surfaces as a setup-block, never a verdict.
        from dbos import DBOS, Queue, SetWorkflowID  # noqa: F401

        DBOS.destroy(destroy_registry=True)
        self.DBOS = DBOS
        self.SetWorkflowID = SetWorkflowID

        # -- durable-workflow: a workflow of N steps, each step a non-checkpointed
        #    side effect (increment a process-global counter keyed by wf id + step).
        @DBOS.step()
        def wio_step(tag: str, i: int) -> int:
            STEP_RUNS[f"{tag}:{i}"] = STEP_RUNS.get(f"{tag}:{i}", 0) + 1
            return i

        @DBOS.workflow()
        def wio_durable_workflow(tag: str, n: int) -> str:
            for i in range(n):
                wio_step(tag, i)
            return f"{tag}:done:{n}"

        # -- enqueue-task: one task = one workflow that bumps a process-global
        #    counter and returns its label.
        @DBOS.workflow()
        def wio_task(label: str) -> str:
            TASK_RUNS[label] = TASK_RUNS.get(label, 0) + 1
            return f"{label}:ok"

        self.wio_durable_workflow = wio_durable_workflow
        self.wio_task = wio_task
        self.instance = self.DBOS(config=self._config())
        self.queue = Queue("wio_queue", concurrency=4)

        DBOS.launch()

    # -- event injection ---------------------------------------------------- #
    def crash_restart(self):
        """Force every terminal-SUCCESS workflow row back to PENDING and recover
        it — the vendor's own crash simulation (tests/test_dbos.py:425-433).
        Recovery must re-run the workflow body while skipping completed steps."""
        import sqlalchemy as sa
        from dbos._schemas.system_database import SystemSchema

        DBOS = self.DBOS
        with self.instance._sys_db.engine.begin() as c:
            c.execute(
                sa.update(SystemSchema.workflow_status)
                .values({"status": "PENDING"})
                .where(SystemSchema.workflow_status.c.status == "SUCCESS")
            )
        DBOS._recover_pending_workflows()

    def workflow_status(self, wfid: str):
        """(status, result) re-read durably from the system DB, or (None, None)."""
        try:
            h = self.DBOS.retrieve_workflow(wfid)
            st = h.get_status()
            return (st.status if st else None), h.get_result()
        except Exception:
            return None, None

    def stop(self):
        try:
            self.DBOS.destroy(destroy_registry=True)
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

    N = 3  # steps per workflow

    def run(self, ctx):
        sut = ctx.sut
        tag = f"wf-{ctx.actor_id}-{sut.seed}"
        expected = f"{tag}:done:{self.N}"

        ctx.step("submit")
        # The user submits a durable workflow under an idempotency key and is
        # promised it completes exactly-once and survives a crash.
        with sut.SetWorkflowID(tag):
            result = sut.wio_durable_workflow(tag, self.N)  # noqa: F841
        ctx.ledger.acked("workflow-result", tag, expected)
        for i in range(self.N):
            ctx.ledger.acked("step-runs", f"{tag}:{i}", 1)

        # A crash-restart event, if armed for this scenario, lands here.
        ctx.step("crashpoint")

        # Re-observe the world after any crash/recovery: the durable result must
        # still be there and each step must have run exactly once.
        status, obs_result = sut.workflow_status(tag)
        ctx.ledger.observe(
            "workflow-result", tag, value=obs_result, present=(status == "SUCCESS")
        )
        for i in range(self.N):
            runs = STEP_RUNS.get(f"{tag}:{i}", 0)
            ctx.ledger.observe("step-runs", f"{tag}:{i}", value=runs, present=(runs >= 1))
        ctx.step("done")


class EnqueueTaskFlow:
    key = "enqueue-task"
    invariants = ("task-completes-once", "dedup-id-enforced")
    documented: dict = {}
    bounds: dict = {}

    K = 3  # tasks enqueued

    def run(self, ctx):
        sut = ctx.sut
        base = f"task-{ctx.actor_id}-{sut.seed}"

        ctx.step("enqueue")
        handles = []
        for j in range(self.K):
            label = f"{base}:{j}"
            h = sut.queue.enqueue(sut.wio_task, label)
            handles.append((label, h))
            ctx.ledger.acked("task-result", label, f"{label}:ok")

        # dedup-id-enforced: a second enqueue with a live deduplication_id must be
        # refused (DBOSQueueDeduplicatedError) and must NOT run.
        dd_label = f"{base}:dedup"
        h_first = sut.queue.enqueue(sut.wio_task, dd_label, deduplication_id="dd")
        ctx.ledger.acked("task-result", dd_label, f"{dd_label}:ok")
        refused_label = f"{base}:dedup-dup"
        try:
            sut.queue.enqueue(sut.wio_task, refused_label, deduplication_id="dd")
            # No refusal: record the ack so a silent duplicate surfaces as a run.
            ctx.ledger.acked("task-result", refused_label, f"{refused_label}:ok")
        except Exception:
            ctx.ledger.denied("task-result", refused_label, "deduplicated")

        ctx.step("collect")
        for label, h in handles:
            try:
                res = h.get_result()
            except Exception:
                res = None
            present = TASK_RUNS.get(label, 0) == 1 and res == f"{label}:ok"
            ctx.ledger.observe("task-result", label, value=res, present=present)

        # dedup first task must complete once.
        try:
            first_res = h_first.get_result()
        except Exception:
            first_res = None
        ctx.ledger.observe(
            "task-result", dd_label, value=first_res,
            present=(TASK_RUNS.get(dd_label, 0) == 1 and first_res == f"{dd_label}:ok"),
        )
        # The refused duplicate must never have run.
        ran = TASK_RUNS.get(refused_label, 0)
        ctx.ledger.observe("task-result", refused_label, value=None, present=(ran > 0))
        ctx.step("done")


FLOWS = {
    "durable-workflow": DurableWorkflowFlow,
    "enqueue-task": EnqueueTaskFlow,
}


def fire_crash_restart(sut):
    sut.crash_restart()


EVENTS = {"crash-restart": fire_crash_restart}
