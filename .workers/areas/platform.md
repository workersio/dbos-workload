---
key: platform
title: Platform
description: The surfaces around the runtime hold up — starter onboarding works out of the box, introspection survives bad data, auth metadata is enforced, and schemas stay isolated.
order: 90
---

# Platform

What this area covers: the operational envelope of a DBOS deployment — the
CLI starter path a new user follows, the control-plane introspection APIs an
operator relies on, auth context carried by SQL-enqueued workflows, and
schema isolation when multiple clients or runtimes share one Postgres.

Boundaries:
- In scope: starter init/migrate/start and config/secret handling,
  introspection over partially undeserializable durable records, SQL-origin
  auth metadata and required-role enforcement, multi-schema client isolation.
- Out of scope until a promise names them: DBOS Cloud/Conductor hosted
  behavior, template variants beyond the supported matrix.

Evidence lineage: legacy hunt corpora in `areas/cli-starter-onboarding.md`
(rungs 000–003; finding #734 closed by maintainer as intended behavior),
`areas/control-plane-state-introspection.md` (rung 001, PR #694 verified),
`areas/auth-context-sql-enqueue.md` (rung 001; finding #743, fix PR #744
open), and `areas/schema-isolation-multi-client.md` (rung 001; fix proposed
in PR #728).
