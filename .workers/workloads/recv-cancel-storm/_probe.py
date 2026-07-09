#!/usr/bin/env python3
"""White-box probe for S4: does a DOUBLE-cancelled recv setup leave a stale
notifications_map entry that a later recv on the same (workflow, topic) trips?

_run_event_setup_async(self, event_map, setup_fn, *args) uses NO self state — only
event_map (a ThreadSafeEventDict) and setup_fn (recv_setup, which registers an
entry via event_map.set and returns (False, event, timeout, payload, start_time)).
So we drive the exact cancellation-cleanup logic with a stub self + a minimal
setup_fn that mimics recv_setup's registration, and force the double-cancel path:

  1st cancel -> except CancelledError: await setup_task inline, then unregister.
  2nd cancel while that await is in flight -> inner except: if setup_task not done,
       add_done_callback(unregister) + raise  (the DEFERRED cleanup path).

Oracle: after everything settles, event_map must be EMPTY (the abandoned recv's
entry is gone). A leftover entry is the bug — the next recv on the same key would
see set()->success=False and raise DBOSWorkflowConflictIDError (docstring: "parks
the caller in await_workflow_result forever").
"""
from __future__ import annotations
import asyncio
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[3]
VENV = REPO_ROOT / ".workers" / "vendor" / "dbos-venv"
VENDOR = REPO_ROOT / ".workers" / "vendor" / "dbos-transact-py"
sp = sorted(VENV.glob("lib/python*/site-packages"))
if sp:
    sys.path.insert(0, str(sp[-1]))
for t in [VENDOR, REPO_ROOT / "target"]:
    if t.exists():
        sys.path.insert(0, str(t))
        break

from dbos._sys_db import SystemDatabase, ThreadSafeEventDict

# The unbound method — no self state used inside.
run_setup = SystemDatabase._run_event_setup_async
stub = SimpleNamespace()

PAYLOAD = "wf-1::topic"


def make_setup_fn(event_map, register_delay_s):
    """Mimics recv_setup's registration: sleep (models the to_thread work of
    check_operation_execution/recv_check/record_sleep), then register in the map."""
    def setup_fn():
        time.sleep(register_delay_s)                 # thread cannot be cancelled
        ev = threading.Event()
        success, ev2 = event_map.set(PAYLOAD, ev, ("wf-1", "topic"))
        # recv_setup returns (False, event, actual_timeout, payload, start_time)
        return (False, ev2, 60.0, PAYLOAD, int(time.time() * 1000))
    return setup_fn


async def drive_double_cancel(event_map, register_delay_s, second_cancel_after_s):
    # A concurrent racer models a second recv on the same (workflow, topic):
    # it busy-checks whether the abandoned entry is EVER visible (the window in
    # which a real recv would set()->success=False and raise ConflictID / park).
    window_seen = {"hit": False}
    stop = {"v": False}

    async def racer():
        while not stop["v"]:
            if event_map.get(PAYLOAD) is not None:
                window_seen["hit"] = True
            await asyncio.sleep(0)   # yield without releasing wall-clock

    race_task = asyncio.ensure_future(racer())

    task = asyncio.ensure_future(
        run_setup(stub, event_map, make_setup_fn(event_map, register_delay_s))
    )
    await asyncio.sleep(0.01)         # let the coroutine reach the awaited shield
    task.cancel()                     # 1st cancel -> inline-wait cleanup branch
    await asyncio.sleep(second_cancel_after_s)  # land inside the inline await...
    task.cancel()                     # 2nd cancel -> DEFERRED (done-callback) path
    try:
        await task
    except asyncio.CancelledError:
        pass
    # Let the registering thread finish + the done-callback (if any) run.
    for _ in range(200):
        await asyncio.sleep(0.01)
        if event_map.get(PAYLOAD) is None:
            break
    stop["v"] = True
    await race_task
    # Return (final leftover, whether the window was ever observable to a racer)
    return event_map.get(PAYLOAD), window_seen["hit"]


async def main():
    # Sweep the 2nd-cancel timing so we cover: before the thread registers, during,
    # and after — one of these lands in the deferred window.
    results = []
    for i, (rd, sc) in enumerate([
        (0.15, 0.02),   # 2nd cancel well before setup thread registers
        (0.15, 0.08),   # 2nd cancel mid-registration
        (0.05, 0.06),   # 2nd cancel just after registration
        (0.10, 0.10),   # 2nd cancel right at registration boundary
    ]):
        em = ThreadSafeEventDict()
        leftover, window_hit = await drive_double_cancel(em, rd, sc)
        snap = em.snapshot()
        status = "LEFTOVER" if leftover is not None else "clean"
        results.append((i, rd, sc, status, window_hit, snap))
        print(f"trial {i}: register_delay={rd} second_cancel_after={sc} -> {status} "
              f"(map size={len(snap)}), racer-observed-window={window_hit}")

    any_leftover = any(r[3] == "LEFTOVER" for r in results)
    any_window = any(r[4] for r in results)
    print()
    if any_leftover:
        print("PERSISTENT LEFTOVER: a double-cancelled recv setup left a stale "
              "notifications_map entry that never drained — a later recv on the "
              "same key would raise DBOSWorkflowConflictIDError.")
        sys.exit(1)
    elif any_window:
        print("TRANSIENT WINDOW: the entry drained, but a concurrent racer "
              "observed it mid-window — a recv racing that instant would trip "
              "ConflictID. Reachability depends on two recvs on the same "
              "(workflow, topic), which requires concurrent same-workflow execution.")
        sys.exit(2)
    else:
        print("CLEAN: deferred cleanup drained the map and no racer ever observed "
              "the abandoned entry.")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
