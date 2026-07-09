# Area: cli-starter-onboarding

## Current State

Current status: one cloud-confirmed config finding, one cloud-passed config
frontier, and one template matrix closure.

Promoted finding: missing Docker secret for `database_url` falls back to SQLite
system DB while app migration writes Postgres.

Evidence:

- `evidence-key:findings/cli-starter-onboarding-missing-docker-secret-falls-back-sqlite.md`
- `evidence-key:findings/cli-starter-onboarding-missing-docker-secret-issue-draft.md`
- `evidence-key:frontiers/cli-starter-onboarding/frontier.md`

## Product Promise

Starter init, migrate, start, config, secret substitution, and cloud override
paths give new users a correct DBOS app without hidden database/config drift.

## What Not To Repeat

- Do not rediscover missing Docker secret fallback to SQLite.
- Do not treat unavailable package-local Flask templates as DBOS failures.
- Do not accept successful command exit as an oracle without checking DBOS
  system DB and app DB state.

## Loop-1 Search Directions

| Direction | Why It Is Deeper |
|---|---|
| Other required config substitutions | Missing env vars, malformed URLs, and cloud override precedence can fail differently from Docker secrets. |
| Migrate/start idempotence under partial failure | Onboarding commands can mutate app/system DBs before failing. |
| Cloud/local config drift | DBOS Cloud config overrides may diverge from local config expectations. |
| Template dependency matrix | Only package-supported templates should be tested; dependency/version drift can still break onboarding. |

## Rung Design Requirements

Every rung must observe generated files, command exit, stdout/stderr, DBOS system
DB state, app DB state, and whether any partial mutation happened before a
configuration failure.

## Stale Conditions

Mark stale if starter templates, config substitution, DBOS Cloud config override
semantics, or migration command behavior changes.

## Rung Index

Evidence source: `evidence-key:frontiers/cli-starter-onboarding/rungs.index.yaml`

```yaml
schema_version: 1
columns: [id, path, status, order, level, workload_file, matrix, summary]
rungs:
  - [
      "rung-000-starter-init-migrate-start",
      "rungs/rung-000-starter-init-migrate-start.md",
      "passed_local",
      "0",
      "baseline",
      ".workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py",
      "1 case",
      "local package-template init, migrate, start, HTTP smoke, and DB state oracle passed",
    ]
  - [
      "rung-001-config-env-secrets",
      "rungs/rung-001-config-env-secrets.md",
      "finding_confirmed_cloud",
      "1",
      "contract",
      ".workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py",
      "3 cases",
      "cloud run 01KVKD7KK5GYRHQ4X95GX51JJ4: env and positive Docker-secret cases passed; missing Docker secret returned 0, fell back to SQLite system DB, and partially migrated app table",
    ]
  - [
      "rung-002-postgres-cloud-config",
      "rungs/rung-002-postgres-cloud-config.md",
      "passed_cloud",
      "2",
      "adversarial",
      ".workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py",
      "3 cases",
      "cloud run 01KVKE6XYHCVT6DMVMY8MXMGHQ passed all 3 cases for config URLs, CLI override precedence, migrate/start rerun idempotence, and credential redaction",
    ]
  - [
      "rung-003-template-matrix",
      "rungs/rung-003-template-matrix.md",
      "closed_supported_variants_passed_flask_template_unavailable",
      "3",
      "sweep",
      ".workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py",
      "24 cases",
      "rung closed for current target package: 16 runnable local variants passed; cloud probe 01KVKF3Q4GP2401118SK3YW1RK passed env-secret-file; Flask-template cases are unsupported because dbos-transact-py has no package-local Flask starter template",
    ]
```

## Rung Details

These are the known rung contracts and outcomes for this frontier. Treat completed, finding, and closed rungs as current reality and design constraints, not as active executor queue rows.

### Rung: rung-000-starter-init-migrate-start

Evidence source: `evidence-key:frontiers/cli-starter-onboarding/rungs/rung-000-starter-init-migrate-start.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-starter-init-migrate-start
frontier: cli-starter-onboarding
status: selected
order: 0
level: baseline
workload_file: .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py
seeds:
  - 3600
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 000 Starter Init Migrate Start

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072715949416000Z.prompt.md`.
- Frontier ID: `cli-starter-onboarding`.
- Rung ID: `rung-000-starter-init-migrate-start`.
- Protected product promise: preserve the concrete `cli-starter-onboarding` promise from `frontier.md` and `strategy/candidates/cli-starter-onboarding.md`.
- Replay command: `python .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-000-starter-init-migrate-start --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree.

##### Goal

- Build and run: starter init, dependency setup, migrate, and start smoke.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `cli-starter-onboarding` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: product-native setup proof.
- Entry points: `dbos init`, generated project files, `dbos migrate`, `dbos start`, env/config values, Docker secret-style paths, and Postgres URLs.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | setup | starter path can bootstrap without hidden manual steps | run init, dependency install, migrate, and start in a temp project | app starts and smoke workflow returns modeled output | file/config/migration/startup oracle |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3600 | run-init-dependency-install-migrate-and-start-in | none unless case says setup block | starter path can bootstrap without hidden manual steps | file/config/migration/startup oracle |


##### Invariants

- Must hold: generated project files match the selected template and option model before commands are run.
- Must hold: env/config overrides are visible in the generated app exactly as modeled, with no fallback to stale defaults.
- Must hold: migrations run once against the intended Postgres URL and leave a replayable schema/version artifact.
- Must hold: startup success is proven by an app-level workflow/HTTP smoke, not only by process exit.
- Must never happen: credentials, secret file contents, or cloud tokens are printed in artifacts.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/cli-starter-onboarding.md`
  - `evidence-key:frontiers/cli-starter-onboarding/frontier.md`
- Suggested command family:
  - `python .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-000-starter-init-migrate-start --case case-001`
  - `python .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-000-starter-init-migrate-start --all-cases --sequential`
- Setup assumptions:
  - Use the harness repository for generated workload code.
  - Do not modify `/Users/viswa/code/workers/dbos-transact-py`.
  - Use real Postgres when the area promise depends on durable DBOS state.
  - Record exact dependency/setup blockers instead of weakening the oracle.
- Per-case evidence to record:
  - seed, derived case JSON, DBOS workflow IDs, public API calls, expected model state, observed results, terminal rows/statuses, product commit, and redacted DB connection details.
- Replay notes:
  - Seed alone is not enough when timing/order is calibrated; persist the derived operation schedule.

##### Expected Signatures

- Success: every matrix case reaches its target window and all invariants pass with replay artifacts.
- Finding: any model/result disagreement, duplicate side effect, illegal state transition, stale durable state, lost event/result, unexpected terminal state, or bounded liveness failure.
- Setup block: dependency bootstrap, DB isolation, optional service provisioning, or target-window calibration prevents execution under the allowed scope.
- Low signal: the runner only wraps existing tests, checks command completion, or never reaches the mapped failure mechanism.
- Goal drift: the runner changes area, product promise, oracle, or broadens into a general DBOS suite.

##### Stop Conditions

- Stop when: all matrix cases pass with artifacts, one strong finding is captured, or a setup/window blocker is documented.
- Escalate when: reaching the target requires product source edits, existing workload code, unbounded compute, or a different oracle.

### Rung: rung-001-config-env-secrets

Evidence source: `evidence-key:frontiers/cli-starter-onboarding/rungs/rung-001-config-env-secrets.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-config-env-secrets
frontier: cli-starter-onboarding
status: finding_confirmed_cloud
order: 1
level: contract
workload_file: .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py
seeds:
  - 3610
  - 3611
  - 3612
updated_at: 2026-06-20T20:58:21Z
```

#### Rung 001 Config Env Secrets

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072715949416000Z.prompt.md`.
- Frontier ID: `cli-starter-onboarding`.
- Rung ID: `rung-001-config-env-secrets`.
- Protected product promise: preserve the concrete `cli-starter-onboarding` promise from `frontier.md` and `strategy/candidates/cli-starter-onboarding.md`.
- Replay command: `python .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-001-config-env-secrets --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree.

##### Goal

- Build and run: env substitution, Docker-secret style config, and startup override behavior.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `cli-starter-onboarding` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: `dbos init`, generated project files, `dbos migrate`, `dbos start`, env/config values, Docker secret-style paths, and Postgres URLs.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | config boundary | env vars override template defaults | set DBOS_DATABASE_URL and app env before migrate/start | generated config and runtime env reflect overrides | config model and runtime smoke agree |
| case-002 | secret path | Docker-secret style file values override plain env when configured | provide password/URL through temp secret file and redacted env | app connects without logging secret value | redacted config and successful migration agree |
| case-003 | error handling | bad secret path fails before partial migrate/start | point config at missing secret file | command fails with actionable config error and no app DB writes | no schema/version row created |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3610 | set-dbos-database-url-and-app-env-before-migrate | none unless case says setup block | env vars override template defaults | config model and runtime smoke agree |
| case-002 | 3611 | provide-password-url-through-temp-secret-file-an | none unless case says setup block | Docker-secret style file values override plain env when configured | redacted config and successful migration agree |
| case-003 | 3612 | point-config-at-missing-secret-file | none unless case says setup block | bad secret path fails before partial migrate/start | no schema/version row created |


##### Invariants

- Must hold: generated project files match the selected template and option model before commands are run.
- Must hold: env/config overrides are visible in the generated app exactly as modeled, with no fallback to stale defaults.
- Must hold: migrations run once against the intended Postgres URL and leave a replayable schema/version artifact.
- Must hold: startup success is proven by an app-level workflow/HTTP smoke, not only by process exit.
- Must never happen: credentials, secret file contents, or cloud tokens are printed in artifacts.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/cli-starter-onboarding.md`
  - `evidence-key:frontiers/cli-starter-onboarding/frontier.md`
- Suggested command family:
  - `python .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-001-config-env-secrets --case case-001`
  - `python .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-001-config-env-secrets --all-cases --sequential`
- Setup assumptions:
  - Use the harness repository for generated workload code.
  - Do not modify `/Users/viswa/code/workers/dbos-transact-py`.
  - Use real Postgres when the area promise depends on durable DBOS state.
  - Record exact dependency/setup blockers instead of weakening the oracle.
- Per-case evidence to record:
  - seed, derived case JSON, DBOS workflow IDs, public API calls, expected model state, observed results, terminal rows/statuses, product commit, and redacted DB connection details.
- Replay notes:
  - Seed alone is not enough when timing/order is calibrated; persist the derived operation schedule.

##### Expected Signatures

- Success: every matrix case reaches its target window and all invariants pass with replay artifacts.
- Finding: any model/result disagreement, duplicate side effect, illegal state transition, stale durable state, lost event/result, unexpected terminal state, or bounded liveness failure.
- Setup block: dependency bootstrap, DB isolation, optional service provisioning, or target-window calibration prevents execution under the allowed scope.
- Low signal: the runner only wraps existing tests, checks command completion, or never reaches the mapped failure mechanism.
- Goal drift: the runner changes area, product promise, oracle, or broadens into a general DBOS suite.

##### Stop Conditions

- Stop when: all matrix cases pass with artifacts, one strong finding is captured, or a setup/window blocker is documented.
- Escalate when: reaching the target requires product source edits, existing workload code, unbounded compute, or a different oracle.

##### Execution Result

Rung 001 is cloud-confirmed as a product finding.

- Cloud run summary:
  `evidence-key:runs/run-20260620T205821Z-cli-starter-onboarding-rung-001-config-env-secrets-cloud/summary.md`
- Finding record:
  `evidence-key:findings/cli-starter-onboarding-missing-docker-secret-falls-back-sqlite.md`
- Workload: `01KVKD7KK5GYRHQ4X95GX51JJ4`
- Exploration: `nd7ahg3sbv31ne6vnvjz8qhtp5890prd`

`case-001` and `case-002` passed the positive env/secret-file paths. `case-003`
failed invariant `missing_secret_fails_before_partial_migrate`: the missing
Docker-secret config returned `0`, migrated DBOS system tables into
`sqlite:///wio_cli_3612.sqlite`, and still created the starter app table in
Postgres.

### Rung: rung-002-postgres-cloud-config

Evidence source: `evidence-key:frontiers/cli-starter-onboarding/rungs/rung-002-postgres-cloud-config.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-002-postgres-cloud-config
frontier: cli-starter-onboarding
status: passed_local
order: 2
level: adversarial
workload_file: .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py
seeds:
  - 3620
  - 3621
  - 3622
updated_at: 2026-06-20T19:10:00Z
```

#### Rung 002 Postgres Cloud Config

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072715949416000Z.prompt.md`.
- Frontier ID: `cli-starter-onboarding`.
- Rung ID: `rung-002-postgres-cloud-config`.
- Protected product promise: preserve the concrete `cli-starter-onboarding` promise from `frontier.md` and `strategy/candidates/cli-starter-onboarding.md`.
- Replay command: `python .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-002-postgres-cloud-config --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree.

##### Goal

- Build and run: Postgres config, DBOS Cloud override shape, and generated app workflow smoke.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `cli-starter-onboarding` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: `dbos init`, generated project files, `dbos migrate`, `dbos start`, env/config values, Docker secret-style paths, and Postgres URLs.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | dependency boundary | Postgres URL selection is unambiguous | run local Postgres URL plus cloud-style override fields | migration targets modeled database only | schema exists only in expected database |
| case-002 | cloud override | cloud config shape does not mask local app settings | apply cloud config values then local env overrides | runtime reports final modeled values | config precedence oracle agrees |
| case-003 | startup replay | rerunning migrate/start is idempotent | run migrate/start twice on same generated app | second run does not duplicate migrations or corrupt state | migration/version and smoke output stable |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3620 | run-local-postgres-url-plus-cloud-style-override | modeled dependency/process fault | Postgres URL selection is unambiguous | schema exists only in expected database |
| case-002 | 3621 | apply-cloud-config-values-then-local-env-overrid | none unless case says setup block | cloud config shape does not mask local app settings | config precedence oracle agrees |
| case-003 | 3622 | run-migrate-start-twice-on-same-generated-app | none unless case says setup block | rerunning migrate/start is idempotent | migration/version and smoke output stable |


##### Invariants

- Must hold: generated project files match the selected template and option model before commands are run.
- Must hold: env/config overrides are visible in the generated app exactly as modeled, with no fallback to stale defaults.
- Must hold: migrations run once against the intended Postgres URL and leave a replayable schema/version artifact.
- Must hold: startup success is proven by an app-level workflow/HTTP smoke, not only by process exit.
- Must never happen: credentials, secret file contents, or cloud tokens are printed in artifacts.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/cli-starter-onboarding.md`
  - `evidence-key:frontiers/cli-starter-onboarding/frontier.md`
- Suggested command family:
  - `python .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-002-postgres-cloud-config --case case-001`
  - `python .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-002-postgres-cloud-config --all-cases --sequential`
- Setup assumptions:
  - Use the harness repository for generated workload code.
  - Do not modify `/Users/viswa/code/workers/dbos-transact-py`.
  - Use real Postgres when the area promise depends on durable DBOS state.
  - Record exact dependency/setup blockers instead of weakening the oracle.
- Per-case evidence to record:
  - seed, derived case JSON, DBOS workflow IDs, public API calls, expected model state, observed results, terminal rows/statuses, product commit, and redacted DB connection details.
- Replay notes:
  - Seed alone is not enough when timing/order is calibrated; persist the derived operation schedule.

##### Expected Signatures

- Success: every matrix case reaches its target window and all invariants pass with replay artifacts.
- Finding: any model/result disagreement, duplicate side effect, illegal state transition, stale durable state, lost event/result, unexpected terminal state, or bounded liveness failure.
- Setup block: dependency bootstrap, DB isolation, optional service provisioning, or target-window calibration prevents execution under the allowed scope.
- Low signal: the runner only wraps existing tests, checks command completion, or never reaches the mapped failure mechanism.
- Goal drift: the runner changes area, product promise, oracle, or broadens into a general DBOS suite.

##### Stop Conditions

- Stop when: all matrix cases pass with artifacts, one strong finding is captured, or a setup/window blocker is documented.
- Escalate when: reaching the target requires product source edits, existing workload code, unbounded compute, or a different oracle.

### Rung: rung-003-template-matrix

Evidence source: `evidence-key:frontiers/cli-starter-onboarding/rungs/rung-003-template-matrix.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-003-template-matrix
frontier: cli-starter-onboarding
status: closed_supported_variants_passed_flask_template_unavailable
order: 3
level: sweep
workload_file: .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py
seeds:
  - 3630
  - 3631
  - 3632
  - 3633
  - 3634
  - 3635
  - 3636
  - 3637
  - 3638
  - 3639
  - 3640
  - 3641
  - 3642
  - 3643
  - 3644
  - 3645
  - 3646
  - 3647
  - 3648
  - 3649
  - 3650
  - 3651
  - 3652
  - 3653
updated_at: 2026-06-21T00:19:07Z
```

#### Rung 003 Template Matrix

##### Run Status

- Status: closed for the current target package surface.
- Local evidence: `evidence-key:runs/run-20260620T191900Z-cli-starter-onboarding-rung-003-template-matrix-local/summary.md`.
- Cloud evidence: `evidence-key:runs/run-20260620T214000Z-cli-starter-onboarding-rung-003-env-secret-cloud-probe/summary.md`.
- Closure note: `evidence-key:runs/run-20260621T001907Z-cli-starter-onboarding-rung-003-closure/summary.md`.
- Result: 16 runnable DB starter variants passed locally; the env-secret branch passed in cloud; Flask-template variants are closed as unsupported because `dbos-transact-py` ships Flask runtime integration but no package-local Flask starter template under `dbos/_templates`.
- Product finding: none from this rung.

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072715949416000Z.prompt.md`.
- Frontier ID: `cli-starter-onboarding`.
- Rung ID: `rung-003-template-matrix`.
- Protected product promise: preserve the concrete `cli-starter-onboarding` promise from `frontier.md` and `strategy/candidates/cli-starter-onboarding.md`.
- Replay command: `python .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-003-template-matrix --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree.

##### Goal

- Build and run: bounded template/options matrix for onboarding regression coverage.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `cli-starter-onboarding` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: `dbos init`, generated project files, `dbos migrate`, `dbos start`, env/config values, Docker secret-style paths, and Postgres URLs.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | bounded sweep | template-python-fastapi preserves the frontier oracle | generate bounded template-python-fastapi variant from seed | case reaches template-python-fastapi evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-002 | bounded sweep | template-flask preserves the frontier oracle | generate bounded template-flask variant from seed | case reaches template-flask evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-003 | bounded sweep | env-secret-file preserves the frontier oracle | generate bounded env-secret-file variant from seed | case reaches env-secret-file evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-004 | bounded sweep | postgres-url-local preserves the frontier oracle | generate bounded postgres-url-local variant from seed | case reaches postgres-url-local evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-005 | bounded sweep | cloud-config-override preserves the frontier oracle | generate bounded cloud-config-override variant from seed | case reaches cloud-config-override evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-006 | bounded sweep | rerun-migrate-start preserves the frontier oracle | generate bounded rerun-migrate-start variant from seed | case reaches rerun-migrate-start evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-007 | bounded sweep | template-python-fastapi preserves the frontier oracle | generate bounded template-python-fastapi variant from seed | case reaches template-python-fastapi evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-008 | bounded sweep | template-flask preserves the frontier oracle | generate bounded template-flask variant from seed | case reaches template-flask evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-009 | bounded sweep | env-secret-file preserves the frontier oracle | generate bounded env-secret-file variant from seed | case reaches env-secret-file evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-010 | bounded sweep | postgres-url-local preserves the frontier oracle | generate bounded postgres-url-local variant from seed | case reaches postgres-url-local evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-011 | bounded sweep | cloud-config-override preserves the frontier oracle | generate bounded cloud-config-override variant from seed | case reaches cloud-config-override evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-012 | bounded sweep | rerun-migrate-start preserves the frontier oracle | generate bounded rerun-migrate-start variant from seed | case reaches rerun-migrate-start evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-013 | bounded sweep | template-python-fastapi preserves the frontier oracle | generate bounded template-python-fastapi variant from seed | case reaches template-python-fastapi evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-014 | bounded sweep | template-flask preserves the frontier oracle | generate bounded template-flask variant from seed | case reaches template-flask evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-015 | bounded sweep | env-secret-file preserves the frontier oracle | generate bounded env-secret-file variant from seed | case reaches env-secret-file evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-016 | bounded sweep | postgres-url-local preserves the frontier oracle | generate bounded postgres-url-local variant from seed | case reaches postgres-url-local evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-017 | bounded sweep | cloud-config-override preserves the frontier oracle | generate bounded cloud-config-override variant from seed | case reaches cloud-config-override evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-018 | bounded sweep | rerun-migrate-start preserves the frontier oracle | generate bounded rerun-migrate-start variant from seed | case reaches rerun-migrate-start evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-019 | bounded sweep | template-python-fastapi preserves the frontier oracle | generate bounded template-python-fastapi variant from seed | case reaches template-python-fastapi evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-020 | bounded sweep | template-flask preserves the frontier oracle | generate bounded template-flask variant from seed | case reaches template-flask evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-021 | bounded sweep | env-secret-file preserves the frontier oracle | generate bounded env-secret-file variant from seed | case reaches env-secret-file evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-022 | bounded sweep | postgres-url-local preserves the frontier oracle | generate bounded postgres-url-local variant from seed | case reaches postgres-url-local evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-023 | bounded sweep | cloud-config-override preserves the frontier oracle | generate bounded cloud-config-override variant from seed | case reaches cloud-config-override evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |
| case-024 | bounded sweep | rerun-migrate-start preserves the frontier oracle | generate bounded rerun-migrate-start variant from seed | case reaches rerun-migrate-start evidence point | generated files/config, migration state, startup logs, HTTP/workflow smoke, and redacted environment model agree |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3630 | generate-bounded-template-python-fastapi-variant | none unless case says setup block | template-python-fastapi preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-002 | 3631 | generate-bounded-template-flask-variant-from-see | none unless case says setup block | template-flask preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-003 | 3632 | generate-bounded-env-secret-file-variant-from-se | none unless case says setup block | env-secret-file preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-004 | 3633 | generate-bounded-postgres-url-local-variant-from | none unless case says setup block | postgres-url-local preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-005 | 3634 | generate-bounded-cloud-config-override-variant-f | none unless case says setup block | cloud-config-override preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-006 | 3635 | generate-bounded-rerun-migrate-start-variant-fro | none unless case says setup block | rerun-migrate-start preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-007 | 3636 | generate-bounded-template-python-fastapi-variant | none unless case says setup block | template-python-fastapi preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-008 | 3637 | generate-bounded-template-flask-variant-from-see | none unless case says setup block | template-flask preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-009 | 3638 | generate-bounded-env-secret-file-variant-from-se | none unless case says setup block | env-secret-file preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-010 | 3639 | generate-bounded-postgres-url-local-variant-from | none unless case says setup block | postgres-url-local preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-011 | 3640 | generate-bounded-cloud-config-override-variant-f | none unless case says setup block | cloud-config-override preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-012 | 3641 | generate-bounded-rerun-migrate-start-variant-fro | none unless case says setup block | rerun-migrate-start preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-013 | 3642 | generate-bounded-template-python-fastapi-variant | none unless case says setup block | template-python-fastapi preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-014 | 3643 | generate-bounded-template-flask-variant-from-see | none unless case says setup block | template-flask preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-015 | 3644 | generate-bounded-env-secret-file-variant-from-se | none unless case says setup block | env-secret-file preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-016 | 3645 | generate-bounded-postgres-url-local-variant-from | none unless case says setup block | postgres-url-local preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-017 | 3646 | generate-bounded-cloud-config-override-variant-f | none unless case says setup block | cloud-config-override preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-018 | 3647 | generate-bounded-rerun-migrate-start-variant-fro | none unless case says setup block | rerun-migrate-start preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-019 | 3648 | generate-bounded-template-python-fastapi-variant | none unless case says setup block | template-python-fastapi preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-020 | 3649 | generate-bounded-template-flask-variant-from-see | none unless case says setup block | template-flask preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-021 | 3650 | generate-bounded-env-secret-file-variant-from-se | none unless case says setup block | env-secret-file preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-022 | 3651 | generate-bounded-postgres-url-local-variant-from | none unless case says setup block | postgres-url-local preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-023 | 3652 | generate-bounded-cloud-config-override-variant-f | none unless case says setup block | cloud-config-override preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |
| case-024 | 3653 | generate-bounded-rerun-migrate-start-variant-fro | none unless case says setup block | rerun-migrate-start preserves the frontier oracle | generated files/config, migration state, startup logs, HTTP/workflow smoke, and |


##### Invariants

- Must hold: generated project files match the selected template and option model before commands are run.
- Must hold: env/config overrides are visible in the generated app exactly as modeled, with no fallback to stale defaults.
- Must hold: migrations run once against the intended Postgres URL and leave a replayable schema/version artifact.
- Must hold: startup success is proven by an app-level workflow/HTTP smoke, not only by process exit.
- Must never happen: credentials, secret file contents, or cloud tokens are printed in artifacts.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/cli-starter-onboarding.md`
  - `evidence-key:frontiers/cli-starter-onboarding/frontier.md`
- Suggested command family:
  - `python .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-003-template-matrix --case case-001`
  - `python .workers/workloads/cli-starter-onboarding/cli_starter_onboarding_workload.py --rung rung-003-template-matrix --all-cases --sequential`
- Setup assumptions:
  - Use the harness repository for generated workload code.
  - Do not modify `/Users/viswa/code/workers/dbos-transact-py`.
  - Use real Postgres when the area promise depends on durable DBOS state.
  - Record exact dependency/setup blockers instead of weakening the oracle.
- Per-case evidence to record:
  - seed, derived case JSON, DBOS workflow IDs, public API calls, expected model state, observed results, terminal rows/statuses, product commit, and redacted DB connection details.
- Replay notes:
  - Seed alone is not enough when timing/order is calibrated; persist the derived operation schedule.

##### Expected Signatures

- Success: every matrix case reaches its target window and all invariants pass with replay artifacts.
- Finding: any model/result disagreement, duplicate side effect, illegal state transition, stale durable state, lost event/result, unexpected terminal state, or bounded liveness failure.
- Setup block: dependency bootstrap, DB isolation, optional service provisioning, or target-window calibration prevents execution under the allowed scope.
- Low signal: the runner only wraps existing tests, checks command completion, or never reaches the mapped failure mechanism.
- Goal drift: the runner changes area, product promise, oracle, or broadens into a general DBOS suite.

##### Stop Conditions

- Stop when: all matrix cases pass with artifacts, one strong finding is captured, or a setup/window blocker is documented.
- Escalate when: reaching the target requires product source edits, existing workload code, unbounded compute, or a different oracle.
