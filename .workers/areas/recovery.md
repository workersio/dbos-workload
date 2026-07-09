---
key: recovery
title: Recovery
description: Interrupted workflows resume from their last completed step exactly once — across database faults, executor crashes, concurrent recoverers, migrations, and shutdown.
order: 50
---

# Recovery

What this area covers: DBOS's promise that durability is not just storage but
liveness — a workflow interrupted by a database fault, an executor crash, or a
process restart resumes from its last completed step without duplicate side
effects, and the machinery around it (system-database retries, startup
migrations, shutdown) never wedges the runtime.

Boundaries:
- In scope: recovery after DB/executor interruption, system-DB retry
  idempotence, migration/startup liveness under shared Postgres, shutdown and
  event-loop liveness.
- Out of scope until a promise names them: multi-region failover, backup or
  restore tooling.

Evidence lineage: legacy hunt corpora in `areas/recovery-db-faults.md`
(rungs 000–006; promoted finding dbos-inc/dbos-transact-py#742, fix PR #744
open), `areas/system-db-retry-idempotence.md` (rung 001),
`areas/migration-startup-liveness.md` (rung 001; open candidate on concurrent
warm-start lock waits), and `areas/runtime-shutdown-event-loop-liveness.md`
(rung 001).
