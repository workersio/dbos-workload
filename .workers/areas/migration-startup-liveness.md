# Area: migration-startup-liveness

## Current State

Current status: cloud finding candidate from PR `#677` (`Migration Early
Exit`). `E-022` reproduced a concurrent warm-start liveness failure while the
system schema was already current and an external session held the DBOS
migration advisory lock.

Evidence:

- PR `#677` added `should_migrate(...)` and changed Postgres
  `run_migrations()` to skip advisory-lock acquisition when the DBOS schema is
  already at the latest migration version.
- `E-022` cloud full-matrix run `01KVYBF56Z8YX1V3MPC8Y06SCP` and focused
  replay `01KVYBK5ABV603KE9EMGPDBM25` both failed
  `up_to_date_migrations_return_before_lock_release` for `case-004`.
- `target/dbos/_migration.py`: `should_migrate`, `ensure_dbos_schema`, and
  `run_dbos_migrations`.
- `target/dbos/_sys_db_postgres.py`: Postgres `run_migrations()` advisory-lock
  path and early-exit check.
- `target/tests/test_schema_migration.py::test_should_migrate` covers the
  narrow predicate, and `test_concurrent_migrations` covers concurrent initial
  migration, but neither holds the advisory lock while exercising up-to-date
  startup.

## Product Promise

DBOS startup and explicit system database migrations remain bounded and safe
when many processes share one Postgres system database. Up-to-date schemas must
skip advisory-lock acquisition and return quickly even if another session holds
the DBOS migration lock. Schemas that are missing, partially initialized, or
behind the latest migration version must not be falsely skipped; after the lock
holder releases, migrations must complete exactly once and leave a coherent
`dbos_migrations` version row.

## Why This Matters

Rolling deploys, app restarts, and serverless/container warmups can create many
DBOS processes that all call `run_migrations()` against the same system
database. If an already-current schema waits on a stale or long-running advisory
lock, healthy deployments can stall for the lock timeout. If the early-exit
predicate is too broad, startup can skip required migrations and let runtime
code operate on a stale schema.

## What Not To Repeat

- Do not repeat `test_should_migrate` as a direct predicate test; the workload
  must prove startup behavior while the advisory lock is externally held.
- Do not repeat `test_concurrent_migrations`; initial concurrent creation is
  already product-covered. This frontier targets up-to-date vs stale schema
  lock behavior.
- Do not reuse cli-starter migration smoke as evidence for advisory-lock
  liveness; it proves starter bootstrap, not lock acquisition avoidance.

## Search Directions

| Direction | Why It Is Distinct |
|---|---|
| Up-to-date startup under held lock | Validates PR `#677`'s core product promise: no advisory-lock wait when no migration work is pending. |
| Stale schema under held lock | Guards the safety side: pending migrations must not be skipped just to avoid a lock. |
| Partial schema state | Missing `dbos_migrations` table or schema must enter the migration path and repair state. |
| Many concurrent warm starts | Models rolling deploy pressure and catches accidental serial startup waits. |

## Rung Design Requirements

New rungs must record the schema state before and after migration, whether the
advisory lock was intentionally held, elapsed bounded runtime, observed
`dbos_migrations` version, and any runtime smoke proving the migrated schema is
usable.

## Rung Index

```yaml
schema_version: 1
columns: [id, path, status, order, level, workload_file, matrix, summary]
rungs:
  - [
      "rung-001-migration-early-exit-advisory-lock",
      "inline:rung-001-migration-early-exit-advisory-lock",
      "finding_candidate",
      "1",
      "startup-liveness",
      ".workers/workloads/migration-startup-liveness/migration_startup_liveness_workload.py",
      "4 cases",
      "up-to-date schemas skip migration advisory-lock waits while stale or partial schemas still migrate safely after lock release",
    ]
```

## Rung Details

### Rung: rung-001-migration-early-exit-advisory-lock

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-migration-early-exit-advisory-lock
frontier: migration-startup-liveness
status: finding_candidate
order: 1
level: startup-liveness
workload_file: .workers/workloads/migration-startup-liveness/migration_startup_liveness_workload.py
seeds: [6770, 6771, 6772, 6773]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/pull/677
  - target/dbos/_migration.py
  - target/dbos/_sys_db_postgres.py
  - target/tests/test_schema_migration.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/risk-based-testing/overview.md
gate_results:
  surface_evidence: ready_from_pr_677_target_should_migrate_and_postgres_run_migrations_code
  duplicate_check: distinct_from_schema_migration_predicate_tests_and_cli_starter_smoke_because_it_models_advisory_lock_liveness_under_real_startup_pressure
  oracle_critic: ready_with_elapsed_bounds_schema_version_state_lock_release_and_runtime_smoke_invariants
  executor_feasibility: default_profile; postgres_required; no_optional_services; exclusive_temp_database_per_seed
  cloud_result: finding_candidate_case_004_concurrent_up_to_date_warm_starts_waited_for_lock_release
```

#### Source Contract

- Frontier ID: `migration-startup-liveness`.
- Rung ID: `rung-001-migration-early-exit-advisory-lock`.
- Protected product promise: up-to-date Postgres DBOS schemas return from
  `run_migrations()` without waiting on the migration advisory lock, while
  stale or partial schemas do not falsely skip required migration work.
- Replay command:
  `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/migration-startup-liveness/migration_startup_liveness_workload.py --rung rung-001-migration-early-exit-advisory-lock --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `6770`, `6771`, `6772`, `6773`; every run must
  persist database/schema names, lock-holder timing, migration version before
  and after, elapsed migration call durations, worker count, and runtime smoke
  result.
- Invariant oracle: independent schema-state model, advisory-lock holder
  timeline, elapsed duration bounds, `dbos_migrations` version row, and DBOS
  runtime smoke must agree.

#### Workload Shape

- Type: startup/operator liveness workload.
- Build profile: `default`.
- Setup: real Postgres through `.workers/run-with-postgres.sh`; use a unique
  temporary system database or schema per seed and drop it during cleanup.
- Entry points:
  - `SystemDatabase.create(...).run_migrations()`
  - read-only SQL against `dbos.dbos_migrations` or a seed-derived schema
  - a direct Postgres connection that holds advisory lock `1234567890`
  - minimal DBOS runtime smoke after migrated state is expected usable
- Fault model: hold the DBOS migration advisory lock externally, then run
  migration/startup calls against up-to-date, stale, missing-table, and
  concurrent warm-start schemas.

#### Parameter Matrix

| Case | Seed | Scenario | Fault model | Primary oracle |
|---|---:|---|---|---|
| `case-001` | 6770 | up-to-date schema while advisory lock is held | early-exit regression would wait for lock timeout despite no pending migrations | `run_migrations()` returns below the short bound, version unchanged at latest, lock holder remains active |
| `case-002` | 6771 | stale version while advisory lock is held then released | unsafe early exit would skip pending migrations; unsafe lock handling would race migration | call remains pending until release, then version reaches latest exactly once and runtime smoke works |
| `case-003` | 6772 | missing `dbos_migrations` table while advisory lock is held then released | partial schema could be mistaken for up-to-date or repaired incorrectly | migration path waits for release, recreates table, records latest version, and leaves required tables queryable |
| `case-004` | 6773 | many concurrent up-to-date warm starts under held lock | rolling deploy could serialize or timeout on a lock no process needs | all workers return below bound, version stays latest, no worker reports lock-timeout warning or schema mutation |

#### Invariants

- Must hold: every case records the latest migration count from
  `get_dbos_migrations(schema, use_listen_notify=True, is_cockroach=False)` or
  an equivalent target helper before mutating the schema.
- Must hold for up-to-date cases: the external advisory lock is still held when
  each migration call returns, proving the code did not need to acquire it.
- Must hold for stale or partial cases: migration calls do not complete before
  the modeled lock release, then finish within the healthy bound after release.
- Must hold: final `dbos_migrations.version` equals the latest modeled version
  for every repaired schema and remains unchanged for up-to-date schemas.
- Must hold: no duplicate migration-version rows, missing migration table,
  invalid index residue, or runtime smoke failure remains after terminal
  observation.
- Must never happen: the workload classifies a slow run as a product finding
  without proving the lock was held/released as modeled and the schema state was
  correct before injection.

#### Expected Signatures

- Success: all cases meet elapsed bounds, schema-version invariants, and runtime
  smoke.
- Finding: up-to-date startup waits on the advisory lock, stale or missing-table
  state returns before lock release without migrating, final version is wrong,
  duplicate migration rows appear, or runtime smoke fails after claimed success.
- Setup block: Postgres cannot create isolated databases/schemas, advisory lock
  control is unavailable, or the workload cannot safely clean up seed-specific
  resources.
- Low signal: direct `should_migrate` assertions without a held advisory lock,
  CLI smoke without schema-state inspection, or a timeout-only failure without
  lock/version evidence.

#### Observed Cloud Finding Candidate

2026-06-25 cloud evidence:

- Full matrix batch/run:
  `nd79qb4tv8qetasxb7nyydp07n89b0k9` /
  `01KVYBF56Z8YX1V3MPC8Y06SCP`, exit `10`.
- Focused replay batch/run:
  `nd7as6ahf6ya2b6m49spf51cg189b1jd` /
  `01KVYBK5ABV603KE9EMGPDBM25`, exit `10`.
- Harness/image commit:
  `798d77d3397d2f93fb3a47b96dd4cc9d177ef6d2`.
- Prepared image SHA:
  `6c41f12282306ae1fcb7559a528a5ee0e0b71efbe4d34052632af0ab9783f87f`.

Both runs failed `case-004` with
`up_to_date_migrations_return_before_lock_release`: six warm-start workers
against an already-current schema returned after the modeled advisory-lock
release instead of returning while the lock was still held. The focused replay
observed worker elapsed times from approximately `14.34s` to `16.28s`; the
external advisory lock was released after roughly `8.89s`.

Cases for stale and partial schema safety passed before the failing full-matrix
case, so this evidence currently narrows to the concurrent up-to-date
warm-start liveness path.

## Oracle Contract

The oracle is a migration-state and liveness ledger keyed by database/schema,
case, seed, lock-holder interval, worker ID, and migration version. It checks
both liveness and safety: healthy up-to-date startup must not block on an
irrelevant advisory lock, but required migrations must not be skipped.

## Stale Conditions

Mark stale if DBOS changes the migration advisory lock ID, the Postgres
migration runner, `dbos_migrations` version semantics, online migration cleanup,
system database startup path, or if the target adds a product test that already
models advisory-lock-held early-exit behavior with the same oracle.
