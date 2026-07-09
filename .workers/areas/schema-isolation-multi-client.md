# Area: schema-isolation-multi-client

## Current State

Current status: new area opened from recent target churn; one
executor-ready rung queued.

Recent issue/PR scan used for this frontier:

- PR `#728`: "Isolate System Database Schema" fixed mutable global schema
  metadata by replacing class-level table schema mutation with per-engine
  `schema_translate_map`.
- PR `#728` added product tests for two system schemas in one process and for
  caller-owned SQLAlchemy sessions whose connections are procured before
  `DBOSClient.enqueue_in_transaction` or `DBOSClient.send_in_transaction`.
- Current target ref `0c41e6df...` predates PR `#728`.
- Existing closed issues/PRs around CLI secrets, Kafka offset loss, portable
  exception serialization, lifecycle replacement children, global timeout,
  async checkpointing, and debouncer timing were checked first and are already
  represented by existing frontiers or queued rows.

## Product Promise

Multiple DBOS clients, runtime instances, application databases, and datasource
objects using different Postgres schemas in one Python process remain isolated:
each write, query, enqueue, transaction, message, and operation-output read
targets the schema declared for that specific object, not whichever schema was
initialized most recently.

## Why This Matters

DBOS exposes custom system schema configuration, DBOSClient access, caller-owned
transaction helpers, and SQLAlchemy datasource objects. Tests, operators, and
multi-app processes may create more than one client or datasource in one
process. If DBOS table metadata is mutable global state, a later client can make
an earlier client read or write the wrong schema, causing cross-app data leaks,
missing workflow state, failed enqueue/send paths, or corrupt exactly-once
operation records.

## Evidence

- Code:
  - `target/dbos/_schemas/system_database.py`: `SystemSchema.set_schema(...)`
    mutates class-level SQLAlchemy `Table.schema` fields.
  - `target/dbos/_sys_db.py`: `SystemDatabase.__init__` calls
    `SystemSchema.set_schema(self.schema)` for every system database/client.
  - `target/dbos/_app_db.py`: `ApplicationDatabase.__init__` mutates
    `ApplicationSchema.transaction_outputs.schema`.
  - `target/dbos/_datasource.py`: sync and async datasource constructors mutate
    `DatasourceSchema.datasource_outputs.schema`.
  - `target/dbos/_client.py`: `DBOSClient` creates a `SystemDatabase` without
    migrations but exposes list, enqueue, send, and caller-owned transaction
    helpers through that schema-bound object.
- Tests:
  - `target/tests/test_dbos.py::test_custom_schema` covers one custom schema
    plus a client after DBOS teardown, but not two live schema-bound objects.
  - `target/tests/test_datasource.py` covers datasource custom schema fixtures
    one at a time, not two datasources with different schemas coexisting.
  - PR `#728` added `test_two_schemas_isolated_in_one_process` and
    caller-owned session-after-statement tests, showing the missing coverage
    shape in the pinned target.
- Existing workloads/frontiers:
  - `datasource-transaction-oaoo` checks app/system agreement for one schema,
    not cross-schema isolation.
  - `cli-starter-onboarding` checks generated app config and secret drift, not
    two schema-bound DBOS objects in one process.
  - `workflow-attributes-query` and queue workloads create clients but do not
    assert multi-schema isolation.

## What Not To Repeat

- Do not repeat single custom-schema smoke tests.
- Do not treat SQLite as meaningful for this frontier; SQLite has no Postgres
  schema isolation.
- Do not rely solely on `SystemSchema` / `ApplicationSchema` /
  `DatasourceSchema` Core queries for the oracle, because those are exactly the
  mutable objects under test. Use physical SQL against quoted schema-qualified
  tables as the independent check.

## Adversarial Model

The frontier attacks process-global mutable schema metadata. It constructs two
or more DBOS-facing objects with distinct schemas, performs operations through
an earlier object after a later object has been initialized, and checks that
public results plus physical schema-qualified rows agree with the per-object
model.

The adversarial schedule intentionally alternates object construction and use:
initialize schema A, initialize schema B, then use A again; procure caller-owned
SQLAlchemy connections before DBOS helper calls; create datasource A and
datasource B, then run operations through both and inspect physical rows.

## Rung Index

```yaml
schema_version: 1
columns: [id, path, status, order, level, workload_file, matrix, summary]
rungs:
  - [
      "rung-000-single-schema-baseline",
      "inline:rung-000-single-schema-baseline",
      "not_run_optional",
      "0",
      "baseline",
      "read-only:target/tests/test_dbos.py,target/tests/test_datasource.py",
      "2 existing product test families",
      "read-only evidence for single custom system schema and datasource schema support",
    ]
  - [
      "rung-001-two-schema-client-datasource-isolation",
      "inline:rung-001-two-schema-client-datasource-isolation",
      "queued",
      "1",
      "adversarial",
      ".workers/workloads/schema-isolation-multi-client/schema_isolation_multi_client_workload.py",
      "3 cases",
      "two live schema-bound clients/datasources must not read or write through the last initialized schema",
    ]
```

## Rung Details

### Rung: rung-000-single-schema-baseline

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-single-schema-baseline
frontier: schema-isolation-multi-client
status: not_run_optional
order: 0
level: baseline
workload_file: read-only:target/tests/test_dbos.py,target/tests/test_datasource.py
seeds: [0]
updated_at: 2026-06-24T00:00:00Z
```

#### Source Contract

- Frontier ID: `schema-isolation-multi-client`.
- Rung ID: `rung-000-single-schema-baseline`.
- Protected product promise: DBOS custom schema support works for a single
  active schema-bound object.
- Replay command: optional read-only product pytest selection; no generated
  workload code is needed for this baseline.
- Seed policy: fixed seed `0`.
- Invariant oracle: existing product tests for one custom schema and one
  datasource schema pass under Postgres.

### Rung: rung-001-two-schema-client-datasource-isolation

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-two-schema-client-datasource-isolation
frontier: schema-isolation-multi-client
status: queued
order: 1
level: adversarial
workload_file: .workers/workloads/schema-isolation-multi-client/schema_isolation_multi_client_workload.py
seeds: [7420, 7421, 7422]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/pull/728
  - target/dbos/_schemas/system_database.py
  - target/dbos/_sys_db.py
  - target/dbos/_app_db.py
  - target/dbos/_datasource.py
  - target/dbos/_client.py
  - target/tests/test_dbos.py
  - target/tests/test_datasource.py
  - target/tests/test_client.py
  - target/tests/test_schema_migration.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-data-and-fixtures/overview.md
gate_results:
  surface_evidence: ready_from_pr_728_and_target_mutable_schema_code
  recent_issue_pr_flake_check: pr_728_used_recent_ci_run_28050935566_checked_but_not_behavioral
  oracle_critic: ready_with_physical_schema_qualified_sql_and_public_api_pairing
  executor_feasibility: default_real_postgres_profile_required_sqlite_not_meaningful
```

#### Source Contract

- Frontier ID: `schema-isolation-multi-client`.
- Rung ID: `rung-001-two-schema-client-datasource-isolation`.
- Protected product promise: each schema-bound DBOS object targets its own
  configured Postgres schema even after another object has initialized a
  different schema in the same Python process.
- Replay command:
  `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/schema-isolation-multi-client/schema_isolation_multi_client_workload.py --rung rung-001-two-schema-client-datasource-isolation --case <case-id>`.
- Seed policy: exact seeds `7420`, `7421`, `7422`; every run must persist
  generated schema names, workflow IDs, queue names, topics, datasource names,
  and physical SQL observations.
- Invariant oracle: public DBOS/DBOSClient/datasource results and independent
  physical schema-qualified SQL must agree with the per-schema model.

#### Goal

Build a Postgres-only workload that proves schema isolation across two
schema-bound objects in one process. The workload must expose the failure mode
where object B mutates global schema metadata and object A subsequently reads
or writes B's schema.

#### Workload File

- Expected path:
  `.workers/workloads/schema-isolation-multi-client/schema_isolation_multi_client_workload.py`.
- Create or reuse: create this file for this frontier; later rungs can reuse it
  while the oracle remains multi-schema isolation.
- Why one file is enough: the setup, cleanup, physical SQL helpers, and
  schema-model ledger are shared by system DB, client transaction, and
  datasource cases.

#### Workload Shape

- Type: Postgres stateful API/client workload.
- Build profile: `default` with real Postgres through
  `.workers/run-with-postgres.sh`; SQLite is `blocked_setup` for this frontier.
- Isolation:
  - Generate unique schemas `wio_schema_a_<seed>` and `wio_schema_b_<seed>`.
  - Drop both schemas in `finally` cleanup with explicit quoted identifiers.
  - Use unique workflow IDs, queue names, topics, and datasource row IDs per
    case.
- Observation points:
  - Public DBOS and DBOSClient list/status/result APIs.
  - Public caller-owned transaction helpers.
  - Public SQLAlchemyDatasource / AsyncSQLAlchemyDatasource operation results.
  - Independent `SELECT ... FROM "<schema>".<table>` physical SQL for
    `workflow_status`, `application_versions`, `operation_outputs`,
    `notifications`, `transaction_outputs`, and `datasource_outputs` where
    relevant.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 7420 | dbos-client-a-after-client-b | create schema A runtime and workflow, create schema B runtime and workflow, then instantiate clients A and B and alternate `list_workflows` / `list_workflow_steps` after both clients exist | client A sees only A workflow rows, client B sees only B workflow rows, and physical `workflow_status`/`operation_outputs` rows live only in their modeled schemas |
| case-002 | 7421 | caller-owned-transaction-after-schema-switch | create client A, create client B, then use client A `enqueue_in_transaction` and `send_in_transaction` through a caller-owned SQLAlchemy `Session` that has already executed `SELECT 1` | transaction helpers write/enqueue/send in schema A, not schema B or an unqualified/default schema; handle result and physical notification/workflow rows agree |
| case-003 | 7422 | datasource-a-after-datasource-b | create sync or async datasource A with schema A, datasource B with schema B, then run a DBOS workflow that calls datasource A after B has initialized and another that calls datasource B after A | datasource operation outputs and app transaction outputs are present exactly once in the modeled physical schema with no cross-schema leakage |

#### Invariants

- Must hold:
  - No public API call through schema-bound object A returns rows created only
    for schema B, and vice versa.
  - Physical schema-qualified SQL shows each modeled workflow, operation output,
    notification/event, transaction output, and datasource output in exactly one
    expected schema.
  - Alternating client/datasource use after both objects exist does not change
    previous observations.
  - Caller-owned transaction helpers commit only after caller commit and leave
    no visible workflow/send effect after rollback.
- Must never happen:
  - Object A writes to the schema initialized by object B merely because B was
    constructed later.
  - The workload uses unqualified `SystemSchema`/`ApplicationSchema`/
    `DatasourceSchema` queries as the sole oracle.
  - SQLite pass is treated as frontier success.

#### Expected Signatures

- Success: all three cases satisfy public API and physical SQL isolation
  invariants under real Postgres.
- Finding: cross-schema read/write leak, wrong-schema row placement, missing
  row after public success, duplicate row across schemas, caller-owned
  transaction effect before commit, or rollback effect that remains visible.
- Setup block: Postgres schema creation/drop, migrations, DBOS launch/destroy,
  or datasource dependency setup prevents the cases from reaching the modeled
  two-schema window.
- Low signal: workload only runs one custom schema at a time or only checks
  command completion.

## Oracle Contract

The oracle is a per-schema ledger plus physical schema-qualified SQL. Public
DBOS results must match the ledger, and independent physical SQL must confirm
row placement. The workload must not compute expected placement from the same
mutable SQLAlchemy table objects whose schema isolation is under test.

## Stale Conditions

Mark stale if DBOS adopts per-engine schema translation, removes custom
Postgres schema support, changes DBOSClient caller-owned transaction semantics,
or target ref advances past PR `#728` and the rung needs to be reframed from
bug-hunt to regression-proof.
