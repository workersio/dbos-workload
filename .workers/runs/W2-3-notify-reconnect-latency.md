# Run W2-3 — notify-reconnect-latency (Wave 2, axis 2: UNPARK S5)

Corridor: `notify-loss-db-reconnect` (parked S5, score 5; UNPARKED for Wave 2,
threshold waived). Work-item: `e-034`. Availability facet of
`notifications-deliver-exactly-once`.

New workload `.workers/workloads/notify-reconnect-latency/notify_reconnect_latency_workload.py`.
Deterministic model of a NOTIFY lost across a DB reconnect: DISABLE the
`dbos_notifications_trigger`, `DBOS.send` (row commits, no `pg_notify` emitted →
the listener is never signaled — identical recv-side state to a dropped NOTIFY),
re-enable. No netem timing luck required.

## Local result (pg :5459) — RED (deterministic characterization)

| case | scenario | recv latency | oracle |
|---|---|---|---|
| case-001 | control (trigger enabled → NOTIFY signals waiter) | **2.0s** | delivered ✓, timely ✓ GREEN |
| case-002 | missed-notify (trigger disabled during send) | **60.025s** | delivered ✓ (no loss), timely **FAIL** (≥8s stall floor) |

`60.025s` == `_notification_fallback_polling_interval` (60s) exactly — the waiter
was never signalled, so recv delivered only via the fallback DB poll. Both cases
delivered the message (no loss): this is an **availability stall, weight 2**, not
an exactly-once/data-loss defect. The differential (identical durable message, 2s
vs 60s, purely from a missed async signal) is the S5 characterization.

Oracle plane: liveness watchdog; terminal-state sweep (all workflows SUCCESS);
crashclock-declared recv-timeout space; `ORACLE_SELFTEST` disables the trigger in
the CONTROL too so its timely oracle must go RED (oracle is live).

## Disposition

Classified **characterization**, not finding_candidate: the 60s fallback is a
documented, deliberate safety net; the stall is its cost, not a broken invariant.
Deterministic + reproducible offline; a cloud artifact adds little (no fault
dependency — the trigger-disable is in-workload). Axis-2 (unpark S5) satisfied
with a concrete, grounded executor result. Filing (if any) stays with Viswa.

## Cloud confirm — RED (reproduced in guest)

Exploration `nd7464zdrvzsk2q97sg61ergg18a8jgw` (run `01KX62N5AM…`, image `fa50292`,
`--depth 1`): case-001 control delivered in **6.345s** (PASS), case-002
missed-notify in **63.575s** (FAIL) — both delivered (no loss). The ~63.6s ≈ the
60s fallback interval. Deterministic finding reproduces in cloud.

Note: the cloud control latency (~6.3s) runs closer to the 8s stall floor than
local (~2s) — the unambiguous signal is the ~60s stall (== fallback interval), a
10× differential; the floor has thin margin against control-side cloud jitter, so
read the finding off the ~60s absolute, not the 8s threshold.
