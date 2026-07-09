---
key: startup-migrations-stay-bounded
area: recovery
title: Startup migrations stay bounded
claim: >-
  DBOS startup and system-database migrations remain bounded and safe when
  many processes share one Postgres — an up-to-date schema starts quickly
  and never corrupts, even while another session holds the migration lock.
status: active
provenance: https://docs.dbos.dev/python/reference/configuration (system database migration on launch; advisory-lock serialization across processes)
explorations:
  - key: migration-early-exit-baseline
    title: Up-to-date schemas start clean
    description: >-
      A process starting against an up-to-date system schema must exit the
      migration path early and cleanly; stale and partial schemas must be
      repaired safely.
    status: done
    result: null
    reason: null
    workload: workloads/migration-startup-liveness/migration_startup_liveness_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/migration-startup-liveness/migration_startup_liveness_workload.py --rung rung-001-migration-early-exit-advisory-lock --case case-001
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd7cyk2wjfnwdx7wwn8qm8gs1n8a73xm
  - key: concurrent-warm-start-lock-wait
    title: Warm starts should not wait on the lock
    description: >-
      Six up-to-date workers starting concurrently should return before the
      migration advisory lock is released. RED confirmed at target 9922c1d
      (E-022): all six warm-start workers waited out the lock holder
      (~15–16s against a ~9s hold) even though schema was already at latest
      (version 41) — invariant up_to_date_migrations_return_before_lock_release
      FAIL. Not yet filed upstream; filing pending human decision.
    status: done
    result: finding
    reason: null
    workload: workloads/migration-startup-liveness/migration_startup_liveness_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/migration-startup-liveness/migration_startup_liveness_workload.py --rung rung-001-migration-early-exit-advisory-lock --case case-004 --seed 6773
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: "run 01KX3YSJ3T4MH1RJX5NZTF840J case-004 seed 6773 — INVARIANT up_to_date_migrations_return_before_lock_release FAIL (6 workers blocked ~15-16s)"
    freshness: new-current
    reported: null
    published: nd7fsts55w4yvef67z23q36qjx8a7x73
---

# Startup migrations stay bounded

Evidence lineage: `areas/migration-startup-liveness.md` rung 001. Cases
001–003 green on the pinned target 3df88c4b; case-004 (concurrent
up-to-date warm starts waiting on the advisory lock) is an open finding
candidate, reproduced in matrix and focused runs, not yet filed upstream.
