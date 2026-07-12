---
key: fork-nonexistent-raw-exception
scenario: management-illegal-transitions
title: "DBOS.fork_workflow on a nonexistent id raises a bare Exception, not DBOSNonExistentWorkflowError"
class: wrong-error
weight: 1
status: held
discovered: 2026-07-12
replay: {run: nd7cr1kbwf5c2y3a8y8vmfcxyd8adpsb, seed: all}
redproof: {run: 01KXAWNRK248Y3J8V0C2BMS39Y, seed: 557653039}
upstream: null
---

# fork_workflow on a nonexistent id leaks a raw `Exception`

## What breaks

The synchronous `DBOS.fork_workflow(workflow_id, start_step)` raises a bare
`Exception("Workflow <id> not found")` when `workflow_id` names a workflow that
does not exist, instead of the typed `DBOSNonExistentWorkflowError` that the rest
of DBOS raises for an unknown workflow id. A caller cannot
`except DBOSNonExistentWorkflowError` around a fork — they are forced to catch a
bare `Exception` (and string-match the message) to distinguish "no such workflow"
from any other failure.

## Why it is a bug and not intended (the asymmetry)

DBOS raises `DBOSNonExistentWorkflowError` (`dbos/_error.py:116`) for an unknown
id **everywhere else**, and the sync fork path is the lone outlier:

- `retrieve_workflow` on a missing id raises it — `dbos/_client.py:523`,
  `dbos/_dbos.py:1418`; the vendor's own test asserts it:
  `tests/test_client.py:664-666` (`pytest.raises(DBOSNonExistentWorkflowError)`).
- A workflow handle's `get_result` raises it — `dbos/_core.py:156,193`.
- **`DBOS.fork_workflow_async` — the async twin of the buggy verb — raises the
  typed error** for a nonexistent target: `dbos/_dbos.py:2304`. The export/client
  fork path does too: `dbos/_sys_db.py:4708`.

Only the **synchronous** `fork_workflow` sys_db path is missing the guard: it
raises `raise Exception(f"Workflow {original_workflow_id} not found")` at
`dbos/_sys_db.py:1180-1181` (reached via `DBOS.fork_workflow` `_dbos.py:2229` →
`_workflow_commands.fork_workflow` `_workflow_commands.py:30-56` →
`sys_db.fork_workflow` `_sys_db.py:1136`). No vendor test forks a nonexistent id
(every fork test forks an existing workflow), so nothing blesses the raw
`Exception`.

## Severity

Weight 1 (wrong-error). The existence check fires inside the fork transaction
(`_sys_db.py:1157` `with engine.begin()`) **before** any insert (`:1184`), so the
transaction rolls back cleanly — no partial fork row, no silent success, no state
corruption. It is purely the exception **type**.

## Minimal reproduction (maintainer-ready, no framework vocabulary)

```python
from dbos import DBOS, DBOSConfig
# ... configure DBOS against a Postgres system db, DBOS.launch() ...

try:
    DBOS.fork_workflow("id-that-was-never-created", 1)
except Exception as e:
    print(type(e).__name__)   # -> "Exception"   (expected: DBOSNonExistentWorkflowError)
```

Compare with the read verb, which does it right:

```python
from dbos._error import DBOSNonExistentWorkflowError
try:
    DBOS.retrieve_workflow("id-that-was-never-created").get_result()
except DBOSNonExistentWorkflowError:
    pass   # typed, catchable
```

Suggested fix: add the same pre-existence check the async path uses to the sync
`fork_workflow` sys_db path, raising `DBOSNonExistentWorkflowError` at
`_sys_db.py:1180` instead of the bare `Exception`.

## How this was caught (internal)

Scenario `management-illegal-transitions` (L0, one ops-operator). The
error-contract oracle ran three wrong-state verbs; `fork-missing` reported
`undocumented_error` (raw `Exception` where a typed error is contracted) on all
3 seeds (VERDICT RED), while the two control verbs — resume a completed workflow,
cancel an already-cancelled one (both guarded no-ops, `_sys_db.py:963-970` /
`:917-936`) — stayed green, so the oracle discriminates rather than always-reds.
The `--redproof` run planted an undocumented outcome into a green control and the
oracle caught it (`ORACLE_SELFTEST PASS`). test-reviewer gate: KEEP.

## Reporting guardrail

Scope the "raised everywhere else" claim to the **read/retrieve verbs and
`fork_workflow_async`** (which have the typed raise). Do NOT claim
`cancel_workflow`/`resume_workflow` raise the typed error on a *missing* id — they
have no existence check and silently no-op (`_sys_db.py:907-936, 951-981`), a
separate (weaker) behavior. Replay: `wio workloads rerun 01KXAWJHBQV072BH4XTFHRXERP`.
