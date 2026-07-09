# [closed] Missing Docker secret falls back to SQLite while app migration writes Postgres

Status: `closed`

Disposition: filed upstream as `dbos-inc/dbos-transact-py#734` and closed by
maintainer decision on 2026-06-22. Keep as local history; do not file a
duplicate.

## Summary

The historical DBOS workload finding showed that a missing Docker secret for
`database_url` could let DBOS CLI commands fall back to SQLite for the system
database while app migration wrote to Postgres. Local and cloud evidence
existed, and positive env/secret-file controls passed.

## Upstream Status

- Issue: https://github.com/dbos-inc/dbos-transact-py/issues/734
- State checked on 2026-06-25: `CLOSED`
- Maintainer response summary: changing CLI default behavior was considered a
  breaking change; CLI commands log which database they connect to; docs
  recommend CLI flags for parameterized database URLs.

## Local Evidence

- Historical finding report:
  `/Users/viswa/code/workers/dbos-workload-findings-report-20260621.md`
- Prioritization note:
  `/Users/viswa/code/workers/dbos-local-bug-prioritization-20260622.md`
- Map promoted finding:
  `.workers/map.md`
- Area: `.workers/areas/cli-starter-onboarding.md`

## Local Disposition

Closed as filed/declined. This remains useful as onboarding/config history but
should not be rediscovered or filed again unless new evidence changes the
contract or user impact.
