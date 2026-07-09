---
key: starter-onboarding-works
area: platform
title: Starter onboarding works
claim: >-
  The CLI starter path — init, migrate, start, config, secrets — gives a new
  user a correctly configured DBOS app with no hidden database or config
  drift between what migration wrote and what the app runs against.
status: active
provenance: https://docs.dbos.dev/quickstart (starter init/migrate/start); secret fallback reported as dbos-inc/dbos-transact-py#734, closed by maintainer as intended (docs recommend CLI flags)
explorations:
  - key: starter-init-migrate-start
    title: Init, migrate, start round-trips
    description: >-
      Baseline onboarding: a starter app initialized, migrated, and started
      must come up against the database its migration wrote, across the
      supported template matrix.
    status: done
    result: null
    reason: null
    workload: workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-000-starter-init-migrate-start --all-cases --sequential
    faults: []
    depth: 1
    timeout: 900
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd78d3adtf2h98680r3y9gxeeh8a6dg3
  - key: config-secret-no-silent-fallback
    title: Missing secrets must not split the database
    description: >-
      A missing Docker secret for database_url makes the runtime fall back
      to SQLite while migration wrote Postgres — app and migration silently
      diverge. Reported upstream as #734; closed by the maintainer as
      intended behavior (fix deemed breaking; docs now recommend CLI
      flags). Behaviour still reproduces at target 9922c1d (invariant
      missing_secret_fails_before_partial_migrate FAIL, run
      01KX3YF0K06ZTQZYCBTCQ5J3JG) but is parked, not published as a
      scorecard row, per the maintainer's closed-as-intended disposition.
    status: parked
    result: finding
    reason: "closed-as-intended upstream #734; documented reproduction, deliberately excluded from the scorecard (publish.py skips non-done)"
    workload: workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-001-config-env-secrets --case case-003
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: "run 01KX3YF0K06ZTQZYCBTCQ5J3JG case-003 — INVARIANT missing_secret_fails_before_partial_migrate FAIL"
    freshness: new-current
    reported: "dbos-inc/dbos-transact-py#734"
    published: null
---

# Starter onboarding works

Evidence lineage: `areas/cli-starter-onboarding.md` rungs 000–003. Rung-002
green in cloud, rung-003 template matrix green locally. The promoted
finding (rung-001 case-003, silent SQLite fallback on missing secret) was
filed as #734 and closed by the maintainer as intended behavior on
2026-06-22; the behavior is unchanged on the pinned target.
