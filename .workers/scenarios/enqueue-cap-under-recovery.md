---
key: enqueue-cap-under-recovery
rung: L3
cast: {task-producer: 1}
flows: [enqueue-task]
event: {key: false-death-recovery, at: crashclock}
depth: 5
status: blocked
result: blocked
replay: null
redproof: null
invariants: [queue-concurrency-capped]
story: >-
  You set a queue to run at most N jobs at once so it never overloads a shared
  resource. A second worker wrongly thinks the first died and recovers its
  running jobs — and for a moment twice as many jobs run at once as you allowed.
---
L3 world-event probe for the queue's concurrency cap under `false-death-recovery`.
The cap `queue-concurrency-capped` is the vendor's OWN invariant
(tests/test_queue.py:1289-1353 recovers a LIVE executor and asserts the running
count does not grow past the cap, :1350) — but the vendor test is single-process,
where the in-memory `ActiveWorkflowById` guard (_core.py:619-639) hides a
cross-process gap. This scenario runs TWO live executors on one system database
so the gap is exercised.

## Mechanism under test

- Global concurrency is counted `count(status==PENDING)` then dispatch-the-remainder
  (_sys_db.py:3877-3899); worker concurrency is counted from a per-process in-memory
  set. Neither is cluster-atomic.
- Recovery re-enqueues a live executor's in-flight queued rows via
  `clear_queue_assignment` (_recovery.py:16-22, _sys_db.py:4007-4021) with NO
  liveness/CAS check and WITHOUT changing `executor_id`.
- So a second executor B that recovers executor A's still-running queued rows
  makes them ENQUEUED again; B's poller (its own worker slots free, and A's
  in-memory guard invisible to it) dequeues and runs them a SECOND time while
  they are still running on A → up to 2N bodies for a `concurrency=N` queue.

## Oracle (cluster-wide live gauge — NOT a row count)

The DB row count settles back to N after the barrier releases, so a row-count
oracle is green-trivial. The witness is a **shared Postgres gauge** both
processes write: a row `wio_cap_gauge(cur,max)` incremented on cap-step ENTRY and
decremented on EXIT, with `max=GREATEST(max, cur)`. The cap invariant is expressed
on the persona ledger as a DENY: `denied("cap-breach", queue, "concurrency=N")`
then `observe(present = gauge.max > N)`. `gauge.max > N` ⇒ `denied_happened` RED;
`gauge.max <= N` ⇒ PASS (a real witness that the cap held, not VOID). Redproof
plants a `denied_happened` in the observation channel only.

## Driver build-spec (executor owns this — flows/flows_dbos.py)

1. Set executor A's id: launch the persistent server with `DBOS__VMID=wioA`.
2. SRV_SRC (A) additions:
   - A dedicated SQLAlchemy engine to `app_url` for the gauge+barrier tables
     (both processes share it): `wio_cap_gauge(id int pk, cur int, max int)`,
     `wio_cap_barrier(id int pk, released bool)`.
   - `@DBOS.workflow cap_wf` → one `@DBOS.step cap_block` that: bumps the gauge
     (`INSERT .. ON CONFLICT DO UPDATE cur=cur+1, max=GREATEST(max,cur+1)`), then
     spins until `wio_cap_barrier.released`, then `cur=cur-1`, returns "ok".
   - `cmd == "caprace"` handler with param `n`: reset gauge(cur=0,max=0)+barrier
     (released=false); `Queue("wio_cap_q", concurrency=n, worker_concurrency=n,
     polling_interval_sec=0.05)`; enqueue n `cap_wf`; wait until `gauge.cur == n`
     (cap full) with a deadline; spawn executor B (EXEC_B_SRC) as a subprocess
     with `DBOS__VMID=wioB` + `WIO_CFG`; wait until `gauge.max` stops rising (B's
     re-dispatch settled) or a deadline; read `gauge.max`; set barrier.released;
     wait all n wfids terminal; join B; return `{cap:n, gauge_max:max, states:{...}}`.
3. EXEC_B_SRC: a second server that boots DBOS with the SAME config +
   `DBOS__VMID=wioB`, registers the SAME `cap_wf`/`cap_block`, launches (its queue
   poller live), calls `DBOS._recover_pending_workflows(["wioA"])`, then stays
   alive ~a fixed budget so its poller dequeues+runs the re-enqueued rows (each
   blocks on the barrier, bumping the gauge), then exits on a sentinel/quit.
4. `EVENTS["false-death-recovery"] = fire_false_death` sets `sut.faildeath_armed`.
5. `EnqueueTaskFlow.run`: if `sut.faildeath_armed`, take the caprace path —
   `facts = sut.request({"cmd":"caprace","n":N})`; then
   `ctx.ledger.denied("cap-breach", "wio_cap_q", f"concurrency={N}")` and
   `ctx.ledger.observe("cap-breach", "wio_cap_q", present=(facts["gauge_max"] > N))`.
   Keep the normal enqueue path otherwise. N = 4.

## Red-proof and rungs

First run is `--redproof` (plants the `denied_happened`, must PASS). No-event
siblings are enqueue-solo/contention/crash-recovery (all green). If this reds, it
crystallizes as an availability finding (weight 2): the queue concurrency cap the
user set to protect a scarce resource is silently exceeded during recovery.

## BLOCKED (e11, harness limitation — NOT a DBOS fact)

The full two-executor driver is built and correct (executor A fills the cap,
holds a Postgres advisory-lock gate, spawns executor B; A/B coordinate purely via
DB state so the differing clocks don't matter). But **executor B — a second full
Python/DBOS process spawned by the flow driver — cannot boot under the
deterministic-time sandbox.** A `faulthandler` stack dump shows B hanging inside
plain stdlib import (`import dbos` → `dis` → executing `opcode.py`, frozen in
`importlib._bootstrap`); B prints `B0/B1/B2` then never returns from the import.
It is not env-based (stripping `LD_PRELOAD`/`FAKETIME*` changed nothing —
`stripped: []`), so the sandbox traps/stalls a driver-spawned grandchild process
at import. Single-process reproduction is impossible by construction: the vendor's
own `test_queue_concurrency_under_recovery` shows the in-memory `ActiveWorkflowById`
guard blocks re-dispatch within ONE process — the breach *requires* a second live
executor without that guard.

STATUS OF THE FINDING: **source-confirmed, execution-blocked.** The invariant is
the vendor's own (`tests/test_queue.py:1350` asserts the running count stays at the
cap after recovering a LIVE executor); the cross-process gap is real in the source
(cap = count-PENDING-then-dispatch `_sys_db.py:3877-3899`; worker count is a
per-process in-memory set; `clear_queue_assignment` re-enqueues live rows with no
liveness/CAS check). It is NOT crystallized (no reproduced red → integrity intact).
Unblock path: a harness change letting a flow driver run a second live executor
process under the sandbox (mirrors how the interleave step-timeout lib fix 8952058
unblocked the enqueue rungs). See ../../friction.md.
