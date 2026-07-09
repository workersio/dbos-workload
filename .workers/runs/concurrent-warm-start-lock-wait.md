# Run evidence — concurrent-warm-start-lock-wait

Exploration: `concurrent-warm-start-lock-wait`
Promise: `startup-migrations-stay-bounded` (area: platform / migration-startup-liveness)
Rung: `rung-001-migration-early-exit-advisory-lock` case-004 seed 6773

## Verdict: FINDING (RED confirmed) — E-022

Invariant `up_to_date_migrations_return_before_lock_release` **FAIL**. Six
worker processes start concurrently against a schema already at the latest
version (41). One holder takes the migration advisory lock and holds it ~9s
(released at +9086ms). All six up-to-date warm starts should short-circuit and
return *before* the lock is released, but each blocked on the lock and returned
only after ~15–16s (elapsed 15.3–16.9s per worker) — availability regression:
an already-migrated fleet cannot warm-start while any one member holds the
migration lock.

## Runs

| purpose | batch | run id | image | state | invariants |
|---|---|---|---|---|---|
| draft (pre workload-fix, red invisible) | nd74vcqv7dys1nygaw2th4m0998a6ddk | 01KX3YEY3380T3AZN607NP65P2 | d255d25 | failed | 0 parsed (3-field emit) |
| re-confirm (post workload-fix) | nd719e9f2ca1m6m70zmb2a5g5h8a6vna | 01KX3YSJ3T4MH1RJX5NZTF840J | acaa92e | failed | hasInvariantViolation=True; 3 PASS + FAIL up_to_date_migrations_return_before_lock_release |

Command: `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/migration-startup-liveness/migration_startup_liveness_workload.py --rung rung-001-migration-early-exit-advisory-lock --case case-004 --seed 6773` (depth 1, no faults).

## Harness fix applied this episode

The migration workload emitted `INVARIANT <name> <status> <summary>` (3 fields)
and signalled failure with the legacy `FINDING-CANDIDATE` marker, so the runtime
parsed 0 invariants and the red was invisible. Corrected to
`INVARIANT <id> <name> PASS|FAIL <summary>` + `WORKLOAD-FAIL` (commit acaa92e);
oracle unchanged.

## Interpretation

Real availability finding on the pinned target. Not yet filed upstream — filing
is a human decision (`reported: null`). Published to the internal status page as
a red.
