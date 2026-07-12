---
key: gc-deletes-referenced-child-strands-parent
scenario: workflow-graph-retention-gc
title: "garbage_collect deletes a child workflow a still-pending parent references, stranding the parent forever on recovery"
class: availability
weight: 2
status: held
discovered: 2026-07-12
replay: {run: nd7ar33zr6vktnp5f262v1bqc98ad1b6, seed: all}
redproof: {run: 01KXB0153P12GBK3A5YXKTB24Z, seed: 1}
upstream: null
---

# `garbage_collect` strands a pending parent by deleting its completed child

## What breaks

`garbage_collect(cutoff_epoch_timestamp_ms=…)` deletes a **completed child
workflow** even when a still-`PENDING` parent workflow references that child's id
in its `operation_outputs`. When the parent is later recovered (the normal
crash-recovery path), it re-awaits the child's result — but the child's row is
gone, so the await polls forever and the parent is stranded `PENDING`
indefinitely. A routine retention sweep silently wedges live work.

## Why it happens

- **gc guards only a row's own status, not inbound references.** The gc filter
  (`dbos/_sys_db.py:4415-4424`) deletes every workflow with `created_at < cutoff`
  whose status is not `PENDING`/`ENQUEUED`/`DELAYED`. A `SUCCESS` child aged past
  the cutoff qualifies; nothing checks whether another workflow still points at
  it. `operation_outputs.child_workflow_id` is a plain column with **no foreign
  key** (`dbos/_schemas/system_database.py:171`), so the delete is neither
  blocked nor cascaded.
- **A recovered parent re-awaits the child from scratch.** On recovery the parent
  replays its body; the child call returns a `WorkflowHandlePolling` for the
  recorded child id (`dbos/_core.py:1002-1003`) and `get_result` calls
  `await_workflow_result` **unconditionally** — it never short-circuits on the
  result the parent already recorded on its first run (`dbos/_core.py:169-188`;
  `record_get_result` notes "there's no corresponding check",
  `dbos/_sys_db.py:2519`).
- **A missing child row reads as "not done", not as an error.**
  `check_workflow_result` returns `NoResult()` when the row is absent
  (`dbos/_sys_db.py:1583,1602`, docstring "…/not found"), and
  `await_workflow_result` is `while True: … time.sleep(polling_interval)` on
  `NoResult` (`dbos/_sys_db.py:1604-1609`). So the parent polls a row that will
  never reappear. It does **not** raise `DBOSNonExistentWorkflowError`; it hangs.

## Why it is a bug and not intended

The retention job and the recovery promise are both first-class DBOS features, and
here they silently conflict. No vendor test covers a parent/child graph under gc:
`tests/test_workflow_management.py:1017-1078` sweeps only **independent** siblings
(`workflow(i)` called at top level) plus one independent `blocked_workflow` that
calls a `@transaction`, never a workflow that called a **child workflow**. Nothing
documents or blesses "gc may delete a workflow another pending workflow depends
on."

## Severity

Weight 2 (availability). The parent never reaches a terminal state and never
raises — it is wedged `PENDING` forever, holding whatever it was driving. It is
not data-loss (the child's *effects* already committed on its first run) and not
wrong-error (nothing is raised); it is a durability/availability hole: recovery,
the feature that is supposed to always finish the workflow, cannot.

## Minimal reproduction (maintainer-ready, no framework vocabulary)

```python
import time
from dbos import DBOS, DBOSConfig, SetWorkflowID
from dbos._workflow_commands import garbage_collect

# ... configure DBOS against a Postgres system db, DBOS.launch() ...

@DBOS.step()
def child_step(cid): return cid + ":ok"

@DBOS.workflow()
def child(cid): return child_step(cid)

@DBOS.workflow()
def parent(pid, cid):
    with SetWorkflowID(cid):
        h = DBOS.start_workflow(child, cid)   # records child id in parent's outputs
    return "parent:" + str(h.get_result())    # re-awaited verbatim on recovery

PID, CID = "parent-1", "child-1"
with SetWorkflowID(PID):
    parent(PID, CID)                          # both reach SUCCESS

# A crash leaves the parent PENDING mid-flight (child already SUCCESS). Simulate
# by flipping only the parent's status back to PENDING:
import sqlalchemy as sa
from dbos._schemas.system_database import SystemSchema as S
with DBOS._get_dbos_instance()._sys_db.engine.begin() as c:
    c.execute(sa.update(S.workflow_status).values(status="PENDING")
              .where(S.workflow_status.c.workflow_uuid == PID))

# A routine retention sweep deletes all old finished workflows — including the
# child, which the still-PENDING parent depends on:
garbage_collect(DBOS._get_dbos_instance(),
                cutoff_epoch_timestamp_ms=int(time.time() * 1000) + 1000,
                rows_threshold=None)

# Recover the parent. It re-awaits the (now-deleted) child and hangs forever:
DBOS._recover_pending_workflows([<this-process-executor-id>])
time.sleep(30)
print(DBOS.get_workflow_status(PID).status)   # -> "PENDING"  (never SUCCESS)
```

Control: the identical crash + `_recover_pending_workflows` **without** the
`garbage_collect` call drives the parent to `SUCCESS` within a second — so the
strand is caused specifically by gc deleting the referenced child.

Suggested fix: exclude workflows still referenced by a non-terminal parent's
`operation_outputs.child_workflow_id` from the gc delete set (or make the parent's
recovery treat a missing child as a hard error rather than an unbounded poll).

## How this was caught (internal)

Scenario `workflow-graph-retention-gc` (one ops-operator). The persona-ledger ran
two arms that differ only in the gc call: a **control** graph (crash + recover, no
gc) reaches `SUCCESS` (green), a **strand** graph (crash + gc + recover) stays
`PENDING` (`acked_lost`, VERDICT RED). Depth-3, all 3 seeds RED; each strand arm:
`{n_rec:1, after:PENDING, child_after:null, child_gone:true}` — recovery ran
(`n_rec:1`) yet the parent is wedged because the child was deleted. The `--redproof`
run planted an `acked_lost` into the green control channel and the oracle caught it
(`ORACLE_SELFTEST PASS`). test-reviewer gate: KEEP. Replay:
`wio workloads rerun 01KXB04TJK6Q2GKEA6ZC1A89F6`.

## Reporting guardrail

State the strand precisely: the parent **hangs `PENDING`** (unbounded poll), it
does **not** raise. Do not claim data loss — the child's committed effects survive;
the loss is the parent's ability to ever complete. Scope the "no vendor test"
claim to the gc suite (`tests/test_workflow_management.py`), which sweeps only
independent workflows. The crash is modelled by flipping the parent row to
`PENDING` (the vendor's own recovery-test injection style); a real mid-flight crash
reaches the same state.
