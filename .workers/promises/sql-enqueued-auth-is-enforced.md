---
key: sql-enqueued-auth-is-enforced
area: platform
title: SQL-enqueued auth is enforced
claim: >-
  Auth metadata attached by dbos.enqueue_workflow(...) in SQL reaches
  required-role enforcement and terminal workflow state — allowed roles
  execute with their context intact, denied roles fail terminally with
  DBOSNotAuthorizedError instead of hanging.
status: active
provenance: https://docs.dbos.dev/python/tutorials/authentication-authorization (required-role enforcement); denied-role hang reported as dbos-inc/dbos-transact-py#743, fix PR #744 open
explorations:
  - key: sql-auth-context-roundtrip
    title: Allowed roles carry their context through
    description: >-
      A workflow enqueued from SQL with auth metadata holding the required
      role must dequeue, execute with authenticated user and roles intact,
      and record them in terminal state.
    status: done
    result: null
    reason: null
    workload: workloads/auth-context-sql-enqueue/auth_context_sql_enqueue_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/auth-context-sql-enqueue/auth_context_sql_enqueue_workload.py --rung rung-001-sql-auth-context-recovery-query --case case-001
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd7cjh10kq0jk3av41pf7pzdzn8a7k6r
  - key: denied-role-reaches-terminal-error
    title: Denied roles must fail terminally
    description: >-
      A SQL-enqueued workflow missing the required role must finalize as a
      terminal DBOSNotAuthorizedError. On the pinned target it stays
      PENDING forever with null output — reported upstream as
      dbos-inc/dbos-transact-py#743, fix PR #744 open.
    status: done
    result: null
    reason: null
    workload: workloads/auth-context-sql-enqueue/auth_context_sql_enqueue_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/auth-context-sql-enqueue/auth_context_sql_enqueue_workload.py --rung rung-001-sql-auth-context-recovery-query --case case-002
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: "dbos-inc/dbos-transact-py#743"
    published: nd7660g6xwd723qm4wwyxk1f6n8a6m91
---

# SQL-enqueued auth is enforced

Evidence lineage: `areas/auth-context-sql-enqueue.md` rung 001 on the
pinned target 3df88c4b: case-001 (allowed role) green; case-002 (denied
role) red — the workflow hangs PENDING instead of failing terminally,
filed as #743 with fix PR #744 proposing terminal ERROR across queue,
recovery, and async paths.
