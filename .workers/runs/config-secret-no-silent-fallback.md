# Run evidence — config-secret-no-silent-fallback

Exploration: `config-secret-no-silent-fallback`
Promise: `starter-onboarding-works` (area: platform / cli-starter-onboarding)
Rung: `rung-001-config-env-secrets` case-003

## Verdict: FINDING reproduced — PARKED (not a scorecard row)

Invariant `missing_secret_fails_before_partial_migrate` **FAIL**. A missing
Docker secret for `database_url` makes the runtime fall back to SQLite while the
migration wrote Postgres — app and migration silently diverge. Still reproduces
at target 9922c1d. hasInvariantViolation=True, 4 invariants (3 PASS + 1 FAIL).

## Run

| batch | run id | image | state | invariants |
|---|---|---|---|---|
| nd7dxaxzwbk1zwcs4dpqrfd44h8a7djr | 01KX3YF0K06ZTQZYCBTCQ5J3JG | d255d25 | failed | hasInvariantViolation=True; FAIL missing_secret_fails_before_partial_migrate |

Command: `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-001-config-env-secrets --case case-003` (depth 1, no faults).

## Disposition: PARKED (deliberately excluded from the scorecard)

Filed upstream as `dbos-inc/dbos-transact-py#734` and **closed by the maintainer
as intended behaviour** (a hard-fail was deemed breaking; docs now recommend CLI
flags). The reproduction is preserved here as documented evidence, but the
exploration is `status: parked` so `publish.py` (which publishes only
`status: done`) never lands it as a scorecard red. Not treated as producible
work for the stop condition — it is a terminal, closed-as-intended disposition.
See `issues/HIST-cli-missing-docker-secret-closed-734.md`.
