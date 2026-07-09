"""Standalone repro: invoking a completed async workflow re-executes its body.

Claim under test: invoking an ALREADY-COMPLETED async `@DBOS.workflow` with the
same workflow id re-executes the workflow *function body* (and therefore any
ordinary application decorator wrapping it) before returning the durably
recorded result. The equivalent SYNC workflow does NOT re-execute its body --
it returns the recorded result directly. Steps stay protected in both cases.

Root cause (target ref 3df88c4):
  - sync  : dbos/_core.py:661  -> _get_wf_invoke_func(...)(thunk)
            `persist` short-circuits a completed workflow WITHOUT calling thunk.
  - async : dbos/_core.py:704  -> Pending(thunk).then(persist)
            dbos/_outcome.py:198 Pending._wrap awaits `func()` (the body)
            BEFORE `persist` can short-circuit on the completed status.

No external services required: uses a SQLite system database.

Requirements:
    pip install dbos

Run:
    python e024_decorator_replay_hook.py

Exit 0 => async did NOT re-run its body (does not reproduce).
Exit 1 => async re-ran its body while sync did not (reproduces).
"""

import asyncio
import functools
import os
import tempfile

from dbos import DBOS, DBOSConfig, SetWorkflowID

# Plain application state, entirely outside DBOS durable step/output protection.
HOOK_CALLS: list[str] = []      # appended by the wrapping application decorator
BODY_RUNS: list[str] = []       # appended inside the workflow body, around the step
STEP_RUNS: list[str] = []       # appended inside the step body (should stay protected)


def app_hook(label: str):
    """An ordinary application decorator (logging / metrics / auth shape)."""

    def decorator(func):
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                HOOK_CALLS.append(f"{label}:before")
                result = await func(*args, **kwargs)
                HOOK_CALLS.append(f"{label}:after")
                return result

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            HOOK_CALLS.append(f"{label}:before")
            result = func(*args, **kwargs)
            HOOK_CALLS.append(f"{label}:after")
            return result

        return sync_wrapper

    return decorator


@DBOS.step(name="e024_async_step")
async def e024_async_step(payload: str) -> str:
    STEP_RUNS.append("async")
    return payload + ":step"


@DBOS.step(name="e024_sync_step")
def e024_sync_step(payload: str) -> str:
    STEP_RUNS.append("sync")
    return payload + ":step"


@DBOS.workflow(name="e024_async_workflow")
@app_hook("async")
async def e024_async_workflow(payload: str) -> str:
    BODY_RUNS.append("async")
    return await e024_async_step(payload)


@DBOS.workflow(name="e024_sync_workflow")
@app_hook("sync")
def e024_sync_workflow(payload: str) -> str:
    BODY_RUNS.append("sync")
    return e024_sync_step(payload)


def invocations(events: list[str], key: str) -> int:
    """Count workflow invocations: one ``<key>:before`` (or one ``<key>``) each."""
    before = f"{key}:before"
    return sum(1 for e in events if e == before or e == key)


async def main() -> int:
    tmpdir = tempfile.mkdtemp(prefix="e024-")
    sysdb = os.path.join(tmpdir, "e024_sys.sqlite")
    config: DBOSConfig = {
        "name": "e024repro",
        "system_database_url": f"sqlite:///{sysdb}",
    }
    DBOS(config=config)
    DBOS.launch()
    try:
        # ---- async workflow: run once, then invoke same id again (replay) ----
        with SetWorkflowID("e024-async"):
            a_first = await e024_async_workflow("direct")
        with SetWorkflowID("e024-async"):
            a_replay = await e024_async_workflow("mutated")

        # ---- sync workflow: same pattern, as the control ----
        with SetWorkflowID("e024-sync"):
            s_first = e024_sync_workflow("direct")
        with SetWorkflowID("e024-sync"):
            s_replay = e024_sync_workflow("mutated")

        async_hook = invocations(HOOK_CALLS, "async")
        sync_hook = invocations(HOOK_CALLS, "sync")
        async_body = invocations(BODY_RUNS, "async")
        sync_body = invocations(BODY_RUNS, "sync")
        async_step = invocations(STEP_RUNS, "async")
        sync_step = invocations(STEP_RUNS, "sync")

        print(f"async: first={a_first!r} replay={a_replay!r}")
        print(f"sync : first={s_first!r} replay={s_replay!r}")
        print()
        print(f"{'':14}{'hook runs':>11}{'body runs':>11}{'step runs':>11}")
        print(f"{'async workflow':14}{async_hook:>11}{async_body:>11}{async_step:>11}")
        print(f"{'sync workflow':14}{sync_hook:>11}{sync_body:>11}{sync_step:>11}")
        print()
        print("Expected for a completed-workflow replay: 1 hook, 1 body, 1 step "
              "(stored result returned, nothing re-executed).")
        print()

        # Durable result must be preserved across replay in both cases.
        durable_ok = a_replay == "direct:step" and s_replay == "direct:step"
        # Steps must stay protected (no duplicate execution) in both cases.
        steps_protected = async_step == 1 and sync_step == 1
        # The signature: async body/hook re-run, sync body/hook do not.
        async_reran = async_hook == 2 and async_body == 2
        sync_did_not = sync_hook == 1 and sync_body == 1

        print(f"durable result preserved on replay (both)   : {durable_ok}")
        print(f"step bodies stayed protected (both)         : {steps_protected}")
        print(f"async body/hook re-ran on replay            : {async_reran}")
        print(f"sync  body/hook did NOT re-run (control)    : {sync_did_not}")
        print()

        if durable_ok and steps_protected and async_reran and sync_did_not:
            print("REPRODUCES: async completed-replay re-executed the "
                  "workflow body while the sync control did not.")
            return 1
        print("Did NOT reproduce with the expected sync/async asymmetry.")
        return 0
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
