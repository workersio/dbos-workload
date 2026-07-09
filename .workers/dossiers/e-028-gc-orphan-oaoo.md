# DRAFT dossier — e-028 (GC orphan → stale step replay). NOT FILED. Hold for Viswa.

Target: `dbos-inc/dbos-transact-py`. Account to file from: `viswa-abe`.
Framing rules (per prior filings #767/#768): plain user report, ZERO product
vocabulary (no wio/workers/harness/invariant/exploration/oracle), repro inside a
collapsed `<details>`. Below is the proposed issue body verbatim.

---

**Title:** Garbage collection can leave orphaned `transaction_outputs`, causing a
reused workflow ID to replay a stale step result

**Body:**

Hi — I hit a durability issue with `garbage_collect` (the new incremental GC in
#751) and wanted to flag it.

`garbage_collect` deletes a workflow's rows from the **system database**
(`workflow_status`) and its step outputs from the **application database**
(`transaction_outputs`) in two separate phases, and there's no transaction
spanning the two databases. If the process is interrupted between the phases —
or the application-database delete loop fails partway (its batches each commit
in their own transaction) — the `transaction_outputs` rows are left behind after
the corresponding `workflow_status` row is already gone.

That orphaned row is keyed by `(workflow_uuid, function_id)`, and
`check_transaction_execution` returns any matching row and skips the step body.
So if an application later starts a **new** workflow that reuses the same
workflow ID (a normal idempotency-key pattern — e.g. an order id), and its first
`@DBOS.transaction` step lands on the same `function_id`, the new workflow
returns the *old, collected* workflow's step output and never runs its own body.
The new work is silently skipped and stale data is returned.

Expected: reusing a collected workflow ID runs the new workflow fresh.
Actual: it replays the previous workflow's recorded step output.

<details>
<summary>Reproduction</summary>

```python
# postgres running locally; DBOS system + app DBs configured as usual.
import uuid
import sqlalchemy as sa
from dbos import DBOS, SetWorkflowID
from dbos._workflow_commands import garbage_collect

DBOS(config=...)          # your standard system+application DB config
DBOS.launch()
inst = DBOS._instance      # the DBOS instance (for the two GC phases below)

# app-side table so we can see whether the body actually ran
with inst._app_db.engine.begin() as c:
    c.execute(sa.text("CREATE TABLE IF NOT EXISTS effects(id text primary key, n int)"))

@DBOS.transaction()
def step(n: int) -> str:
    DBOS.sql_session.execute(
        sa.text("INSERT INTO effects(id, n) VALUES (:i, :n)"),
        {"i": str(uuid.uuid4()), "n": n},
    )
    return f"result-{n}"

@DBOS.workflow()
def wf(n: int) -> str:
    return step(n)

wid = "order-123"
with SetWorkflowID(wid):
    print(wf(10))          # -> "result-10", one effects row

# Garbage-collect this (completed) workflow. The two phases are not atomic.
# Model an interruption after the system-DB phase commits but before the
# application-DB phase runs (a crash, a lost DB connection, or a mid-batch
# failure in the app-DB delete loop): run only the system-DB phase.
cutoff = int(__import__("time").time() * 1000) + 3_600_000
inst._sys_db.garbage_collect(cutoff, None, 100)   # deletes workflow_status; app-DB phase never runs

# workflow_status for `order-123` is gone; its transaction_outputs row survives (orphan).

with SetWorkflowID(wid):
    print(wf(20))          # EXPECTED "result-20" + a new effects row
                           # ACTUAL   "result-10" and NO new effects row (body skipped)
```

Observed: the second call prints `result-10` and inserts no new `effects` row —
the orphaned `transaction_outputs` row from the first workflow was replayed.

The same orphan (and same result) also occurs entirely through the public
`garbage_collect(...)` when the **application-database** batched delete loop
fails partway — e.g. a transient connection error after the first batch commits.
The resumable-GC handling added in #751 covers a failure on the system-database
side but not the application-database side.

</details>

A fix might delete `transaction_outputs` before (or in the same logical unit as)
`workflow_status`, or have step lookup ignore outputs whose workflow row no
longer exists. Happy to help test. Thanks!

---

## Notes for Viswa (not part of the issue)

- Evidence: `runs/E-028.md`. Cloud-confirmed both ways — crash-between-phases
  (`01KX460BYM2JHVTJKT2XBQE4WN`) and public-API app-db-batch partial failure
  (`01KX4BZJHVB2V4MKPA9FDY08JE`).
- The repro above uses `inst._sys_db.garbage_collect` for a short, deterministic
  script; if you prefer a fully-public-API repro for the issue, the app-db-batch
  variant (case-003 in the workload) triggers it through `garbage_collect(...)`
  with a transient app-db fault — I can expand that into the issue body instead.
- `DBOS._instance` / `_get_dbos_instance()` accessor name should be verified
  against the released version before posting.
