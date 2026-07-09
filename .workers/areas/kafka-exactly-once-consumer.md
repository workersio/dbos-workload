# Area: kafka-exactly-once-consumer

## Current State

Current status: one historical cloud-minimized finding plus broker-restart cloud
confirmation. The current target ref includes upstream PR `#738` / issue `#733`,
which changed DBOS Kafka offset storage so commits cannot outrun durable
workflow enqueue; treat the old offset-loss finding as fixed-upstream regression
context unless a fresh rerun proves otherwise.

Promoted finding: produced Kafka offset can disappear after immediate DBOS
relaunch; persisted broker restart confirmed a related missing offset `0`.

Evidence:

- `evidence-key:findings/kafka-exactly-once-consumer-lost-offset-after-relaunch.md`
- `evidence-key:frontiers/kafka-exactly-once-consumer/frontier.md`
- `evidence-key:runs/run-20260621T001316Z-kafka-exactly-once-consumer-rung-005-cloud-minimized/summary.md`
- `evidence-key:runs/run-20260621T003742Z-kafka-exactly-once-consumer-rung-003-cloud-confirmed/summary.md`
- Upstream issue `#733`: `[kafka] Consumer can commit a polled offset before durable workflow creation`.
- Upstream PR `#738`: `Fix Kafka Offset Issue`, merged as `9ed4feca5cd4b99f4e04848fcf696c10819031e0` and present in target ref `3df88c4bcc3a2b73d91b765b459f4c7beb6a690c`.

## Product Promise

Kafka-triggered workflows process external events durably and idempotently
across duplicate messages, offset/rebalance behavior, broker interruption, and
consumer relaunch.

## What Not To Repeat

- Do not rediscover offset `0` loss after immediate DBOS relaunch; in the
  current target this is covered by upstream issue `#733` / PR `#738`.
- Do not use mock broker evidence for relaunch/restart semantics.
- Do not promote Kafka product findings until broker persistence, native musl
  packaging, and artifact paths are setup-proven.

## Loop-1 Search Directions

| Direction | Why It Is Deeper |
|---|---|
| Kafka plus transactional side effects | Offset commit and DBOS durable state may disagree around app DB commits. |
| Consumer group rebalance beyond one record | Multi-partition/multi-consumer behavior can expose different ordering/commit bugs. |
| Retry/DLQ integration | Failing workflows and Kafka offset management may lose or duplicate poison records. |
| Manual commit semantics | If DBOS changes auto-commit behavior, design rungs around commit-after-durable-success. |

## Rung Design Requirements

Setup proof is mandatory before product claims. Record broker mode, persistence,
produced offsets, consumer group, workflow ids, DBOS durable rows, and app
effects.

## Stale Conditions

Mark stale if DBOS Kafka adapter changes offset commit policy, consumer config
defaults, or supported broker setup. For the current target, rerun and minimize
before treating the historical offset-loss evidence as an active product
finding, because PR `#738` disables automatic offset storage and stores offsets
only after durable enqueue.

## Rung Index

Evidence source: `evidence-key:frontiers/kafka-exactly-once-consumer/rungs.index.yaml`

```yaml
schema_version: 1
columns: [id, path, status, order, level, workload_file, matrix, summary]
rungs:
  - [
      "rung-000-kafka-service-smoke",
      "rungs/rung-000-kafka-service-smoke.md",
      "passed_cloud_mock",
      "0",
      "baseline",
      ".workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py",
      "1 case",
      "local real Kafka/Postgres smoke passed; WIO cloud mock-broker smoke 01KVKM059HSXYMWBQ4J9S45JR3 passed with five parsed PASS invariants",
    ]
  - [
      "rung-001-duplicate-key-idempotency",
      "rungs/rung-001-duplicate-key-idempotency.md",
      "passed_cloud_mock",
      "1",
      "adversarial",
      ".workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py",
      "3 cases",
      "three local real Kafka/Postgres duplicate-key cases passed; WIO cloud mock-broker run 01KVKM60G1HTPGNFYXWJG34QR8 passed all three cases",
    ]
  - [
      "rung-002-rebalance-offset-replay",
      "rungs/rung-002-rebalance-offset-replay.md",
      "passed_cloud_standalone",
      "2",
      "adversarial",
      ".workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py",
      "3 cases",
      "three local real Kafka/Postgres restart/rebalance cases passed; corrected WIO cloud standalone-broker run 01KVKP7B0JGVNW14FZYBZN4PC9 passed all three cases after relaunch was gated on durable consumer acceptance",
    ]
  - [
      "rung-003-broker-restart",
      "rungs/rung-003-broker-restart.md",
      "finding_confirmed_cloud",
      "3",
      "failure",
      ".workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py",
      "6 cases",
      "persisted standalone broker restart removed the setup gap; cases 001-003 passed locally, and case 004 cloud workload 01KVKSNAGTVQPN7ESF5SEQNQBH lost produced offset 0 after broker restart plus DBOS relaunch",
    ]
  - [
      "rung-004-bounded-seed-sweep",
      "rungs/rung-004-bounded-seed-sweep.md",
      "finding_candidate_cloud",
      "4",
      "sweep",
      ".workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py",
      "24 cases",
      "24/24 local real Kafka/Postgres cases passed; cloud representative sweep passed cases 001,002,006 and found case 003 missing offset 0 after immediate DBOS Kafka relaunch",
    ]
  - [
      "rung-005-finding-minimization",
      "rungs/rung-005-finding-minimization.md",
      "finding_minimized_cloud",
      "5",
      "minimization",
      ".workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py",
      "2 cases",
      "case 001 minimized the rung 004 cloud finding to one produced Kafka offset lost after immediate DBOS relaunch; cloud workload 01KVKR8552CZXRG3EYQTC5ZVMH reproduced it",
    ]
```

## Rung Details

These are the known rung contracts and outcomes for this frontier. Treat completed, finding, and closed rungs as current reality and design constraints, not as active executor queue rows.

### Rung: rung-000-kafka-service-smoke

Evidence source: `evidence-key:frontiers/kafka-exactly-once-consumer/rungs/rung-000-kafka-service-smoke.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-kafka-service-smoke
frontier: kafka-exactly-once-consumer
status: selected
order: 0
level: baseline
workload_file: .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py
seeds:
  - 3700
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 000 Kafka Service Smoke

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072714344022000Z.prompt.md`.
- Frontier ID: `kafka-exactly-once-consumer`.
- Rung ID: `rung-000-kafka-service-smoke`.
- Protected product promise: preserve the concrete `kafka-exactly-once-consumer` promise from `frontier.md` and `strategy/candidates/kafka-exactly-once-consumer.md`.
- Replay command: `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-000-kafka-service-smoke --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key.

##### Goal

- Build and run: prove Kafka broker provisioning, topic creation, producer, and DBOS consumer startup.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `kafka-exactly-once-consumer` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: product-native setup proof.
- Entry points: Kafka broker/topic/group generation, produced records, keys, offsets, DBOS workflow IDs, idempotency keys, side-effect ledger rows, and offset commits.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | setup | Kafka service and DBOS consumer can run in the harness | create topic, produce one record, start consumer workflow | one terminal workflow and one side-effect row | broker/topic/consumer smoke oracle |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3700 | create-topic-produce-one-record-start-consumer-w | none unless case says setup block | Kafka service and DBOS consumer can run in the harness | broker/topic/consumer smoke oracle |


##### Invariants

- Must hold: each produced record is modeled by topic, partition, offset, key, value, and expected workflow id before consumption.
- Must hold: duplicate keys or replayed offsets produce at most one modeled side effect.
- Must hold: consumer restart/rebalance never loses a committed record or creates an extra terminal workflow.
- Must hold: broker interruption artifacts include offset/consumer generation evidence, not only command exit.
- Must never happen: a run passes without proving Kafka broker/topic/group setup and modeled offset windows.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/kafka-exactly-once-consumer.md`
  - `evidence-key:frontiers/kafka-exactly-once-consumer/frontier.md`
- Suggested command family:
  - `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-000-kafka-service-smoke --case case-001`
  - `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-000-kafka-service-smoke --all-cases --sequential`
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

### Rung: rung-001-duplicate-key-idempotency

Evidence source: `evidence-key:frontiers/kafka-exactly-once-consumer/rungs/rung-001-duplicate-key-idempotency.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-duplicate-key-idempotency
frontier: kafka-exactly-once-consumer
status: ready
order: 1
level: adversarial
workload_file: .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py
seeds:
  - 3710
  - 3711
  - 3712
updated_at: 2026-06-20T07:42:15Z
```

#### Rung 001 Duplicate Key Idempotency

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072714344022000Z.prompt.md`.
- Frontier ID: `kafka-exactly-once-consumer`.
- Rung ID: `rung-001-duplicate-key-idempotency`.
- Protected product promise: preserve the concrete `kafka-exactly-once-consumer` promise from `frontier.md` and `strategy/candidates/kafka-exactly-once-consumer.md`.
- Replay command: `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-001-duplicate-key-idempotency --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key.

##### Goal

- Build and run: duplicate records and idempotency keys produce one modeled side effect.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `kafka-exactly-once-consumer` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: Kafka broker/topic/group generation, produced records, keys, offsets, DBOS workflow IDs, idempotency keys, side-effect ledger rows, and offset commits.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | duplicate/replay | same key delivered twice creates one modeled effect | produce duplicate records with same idempotency key | consumer observes both offsets but one effect | side-effect ledger count one |
| case-002 | ordering | out-of-order duplicate around slow workflow does not double execute | block first record workflow then produce duplicate | second record does not create second terminal effect | workflow id/effect ledger unique |
| case-003 | boundary data | same key with changed payload is rejected or ignored consistently | produce duplicate key with different payload | modeled first payload wins or explicit error recorded | effect payload matches model |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3710 | produce-duplicate-records-with-same-idempotency- | none unless case says setup block | same key delivered twice creates one modeled effect | side-effect ledger count one |
| case-002 | 3711 | block-first-record-workflow-then-produce-duplica | none unless case says setup block | out-of-order duplicate around slow workflow does not double execute | workflow id/effect ledger unique |
| case-003 | 3712 | produce-duplicate-key-with-different-payload | none unless case says setup block | same key with changed payload is rejected or ignored consistently | effect payload matches model |


##### Invariants

- Must hold: each produced record is modeled by topic, partition, offset, key, value, and expected workflow id before consumption.
- Must hold: duplicate keys or replayed offsets produce at most one modeled side effect.
- Must hold: consumer restart/rebalance never loses a committed record or creates an extra terminal workflow.
- Must hold: broker interruption artifacts include offset/consumer generation evidence, not only command exit.
- Must never happen: a run passes without proving Kafka broker/topic/group setup and modeled offset windows.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/kafka-exactly-once-consumer.md`
  - `evidence-key:frontiers/kafka-exactly-once-consumer/frontier.md`
- Suggested command family:
  - `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-001-duplicate-key-idempotency --case case-001`
  - `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-001-duplicate-key-idempotency --all-cases --sequential`
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

### Rung: rung-002-rebalance-offset-replay

Evidence source: `evidence-key:frontiers/kafka-exactly-once-consumer/rungs/rung-002-rebalance-offset-replay.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-002-rebalance-offset-replay
frontier: kafka-exactly-once-consumer
status: passed_cloud_standalone
order: 2
level: adversarial
workload_file: .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py
seeds:
  - 3720
  - 3721
  - 3722
updated_at: 2026-06-20T23:41:42Z
```

#### Rung 002 Rebalance Offset Replay

##### Run Status

- Status: passed in WIO cloud with the workload-owned standalone Kafka broker.
- Evidence: `evidence-key:runs/run-20260620T234142Z-kafka-exactly-once-consumer-rung-002-cloud-standalone/summary.md`.
- Workload: `01KVKP7B0JGVNW14FZYBZN4PC9`.
- Cases: 3/3 passed sequentially.

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072714344022000Z.prompt.md`.
- Frontier ID: `kafka-exactly-once-consumer`.
- Rung ID: `rung-002-rebalance-offset-replay`.
- Protected product promise: preserve the concrete `kafka-exactly-once-consumer` promise from `frontier.md` and `strategy/candidates/kafka-exactly-once-consumer.md`.
- Replay command: `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-002-rebalance-offset-replay --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key.

##### Goal

- Build and run: consumer restart/rebalance and offset replay without missing or duplicate terminal workflows.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `kafka-exactly-once-consumer` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: Kafka broker/topic/group generation, produced records, keys, offsets, DBOS workflow IDs, idempotency keys, side-effect ledger rows, and offset commits.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | rebalance | consumer restart after poll before workflow terminal keeps exactly-once effect | restart consumer after record accepted but before result | one terminal workflow after restart | offset/workflow ledger agrees |
| case-002 | offset replay | committed offset replay does not rerun side effect | force replay of already-processed offset | result/effect count unchanged | offset replay oracle |
| case-003 | generation change | group generation change preserves pending records | start second consumer and rebalance with backlog | all modeled records terminal once | no lost or duplicate effects |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3720 | restart-consumer-after-record-accepted-but-befor | none unless case says setup block | consumer restart after poll before workflow terminal keeps exactly-onc | offset/workflow ledger agrees |
| case-002 | 3721 | force-replay-of-already-processed-offset | none unless case says setup block | committed offset replay does not rerun side effect | offset replay oracle |
| case-003 | 3722 | start-second-consumer-and-rebalance-with-backlog | none unless case says setup block | group generation change preserves pending records | no lost or duplicate effects |


##### Invariants

- Must hold: each produced record is modeled by topic, partition, offset, key, value, and expected workflow id before consumption.
- Must hold: duplicate keys or replayed offsets produce at most one modeled side effect.
- Must hold: consumer restart/rebalance never loses a committed record or creates an extra terminal workflow.
- Must hold: broker interruption artifacts include offset/consumer generation evidence, not only command exit.
- Must never happen: a run passes without proving Kafka broker/topic/group setup and modeled offset windows.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/kafka-exactly-once-consumer.md`
  - `evidence-key:frontiers/kafka-exactly-once-consumer/frontier.md`
- Suggested command family:
  - `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-002-rebalance-offset-replay --case case-001`
  - `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-002-rebalance-offset-replay --all-cases --sequential`
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

### Rung: rung-003-broker-restart

Evidence source: `evidence-key:frontiers/kafka-exactly-once-consumer/rungs/rung-003-broker-restart.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-003-broker-restart
frontier: kafka-exactly-once-consumer
status: finding_confirmed_cloud
order: 3
level: failure
workload_file: .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py
seeds:
  - 3730
  - 3731
  - 3732
  - 3733
  - 3734
  - 3735
updated_at: 2026-06-21T00:37:42Z
```

#### Rung 003 Broker Restart

##### Run Status

- Status: cloud-confirmed finding in case 004.
- Evidence: `evidence-key:runs/run-20260620T181500Z-kafka-exactly-once-consumer-rung-003-broker-restart-local/summary.md`.
- Cases: 6/6 passed against local Docker Kafka and Postgres.
- Cloud evidence: `evidence-key:runs/run-20260621T001000Z-kafka-exactly-once-consumer-rung-003-partial-cloud/summary.md`.
- Cloud result: cases 005 and 006 passed; representative restart case 001 setup-blocked because the current standalone broker cannot provide durable restart lifecycle.
- Persisted standalone restart evidence: `evidence-key:runs/run-20260621T002514Z-kafka-exactly-once-consumer-rung-003-persisted-restart-local/summary.md`.
- Persisted standalone result: cases 001, 002, and 003 passed locally; case 004 failed `consumer_observed_every_produced_offset` after broker restart plus DBOS relaunch because produced offset 0 disappeared from DBOS acceptance, offset, and workflow-status ledgers.
- Cloud confirmation: `evidence-key:runs/run-20260621T003742Z-kafka-exactly-once-consumer-rung-003-cloud-confirmed/summary.md`.
- Cloud workload `01KVKSNAGTVQPN7ESF5SEQNQBH` reproduced the same missing offset `0` on commit `6a51b7dfe06eb25c04e0c509e75a192301a0ae24`.

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072714344022000Z.prompt.md`.
- Frontier ID: `kafka-exactly-once-consumer`.
- Rung ID: `rung-003-broker-restart`.
- Protected product promise: preserve the concrete `kafka-exactly-once-consumer` promise from `frontier.md` and `strategy/candidates/kafka-exactly-once-consumer.md`.
- Replay command: `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-003-broker-restart --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key.

##### Goal

- Build and run: broker interruption while workflow processing and offset commits are in flight.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `kafka-exactly-once-consumer` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: Kafka broker/topic/group generation, produced records, keys, offsets, DBOS workflow IDs, idempotency keys, side-effect ledger rows, and offset commits.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | dependency fault | broker restart before poll does not lose produced records | restart broker after produce before consume | consumer later processes records once | produced/consumed ledger equal |
| case-002 | dependency fault | broker restart after poll before DBOS terminal does not duplicate | restart broker while workflow blocked | one terminal effect after recovery | side-effect count one |
| case-003 | dependency fault | broker restart during offset commit is replay-safe | restart during commit window | offset either commits once or record replays safely | offset/effect model agrees |
| case-004 | recovery | consumer restart after broker recovery preserves group generation evidence | restart consumer after broker returns | all records terminal once | generation and workflow artifacts recorded |
| case-005 | late read | late workflow result read does not reconsume record | read handles after consumer idle | results stable | effect count unchanged |
| case-006 | cleanup | topic/group cleanup does not hide unfinished work | cleanup after modeled terminal state only | no active modeled workflows remain | terminal ledger complete |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3730 | restart-broker-after-produce-before-consume | modeled dependency/process fault | broker restart before poll does not lose produced records | produced/consumed ledger equal |
| case-002 | 3731 | restart-broker-while-workflow-blocked | modeled dependency/process fault | broker restart after poll before DBOS terminal does not duplicate | side-effect count one |
| case-003 | 3732 | restart-during-commit-window | modeled dependency/process fault | broker restart during offset commit is replay-safe | offset/effect model agrees |
| case-004 | 3733 | restart-consumer-after-broker-returns | modeled dependency/process fault | consumer restart after broker recovery preserves group generation evid | generation and workflow artifacts recorded |
| case-005 | 3734 | read-handles-after-consumer-idle | none unless case says setup block | late workflow result read does not reconsume record | effect count unchanged |
| case-006 | 3735 | cleanup-after-modeled-terminal-state-only | none unless case says setup block | topic/group cleanup does not hide unfinished work | terminal ledger complete |


##### Invariants

- Must hold: each produced record is modeled by topic, partition, offset, key, value, and expected workflow id before consumption.
- Must hold: duplicate keys or replayed offsets produce at most one modeled side effect.
- Must hold: consumer restart/rebalance never loses a committed record or creates an extra terminal workflow.
- Must hold: broker interruption artifacts include offset/consumer generation evidence, not only command exit.
- Must never happen: a run passes without proving Kafka broker/topic/group setup and modeled offset windows.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/kafka-exactly-once-consumer.md`
  - `evidence-key:frontiers/kafka-exactly-once-consumer/frontier.md`
- Suggested command family:
  - `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-003-broker-restart --case case-001`
  - `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-003-broker-restart --all-cases --sequential`
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

### Rung: rung-004-bounded-seed-sweep

Evidence source: `evidence-key:frontiers/kafka-exactly-once-consumer/rungs/rung-004-bounded-seed-sweep.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-004-bounded-seed-sweep
frontier: kafka-exactly-once-consumer
status: passed_local
order: 4
level: sweep
workload_file: .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py
seeds:
  - 3740
  - 3741
  - 3742
  - 3743
  - 3744
  - 3745
  - 3746
  - 3747
  - 3748
  - 3749
  - 3750
  - 3751
  - 3752
  - 3753
  - 3754
  - 3755
  - 3756
  - 3757
  - 3758
  - 3759
  - 3760
  - 3761
  - 3762
  - 3763
updated_at: 2026-06-20T18:38:38Z
```

#### Rung 004 Bounded Seed Sweep

##### Run Status

- Status: passed locally.
- Evidence: `evidence-key:runs/run-20260620T183000Z-kafka-exactly-once-consumer-rung-004-bounded-seed-sweep-local-fixed/summary.md`.
- Cases: 24/24 passed against local Docker Kafka and Docker Postgres.
- Environment: `PGPORT=55432`, `PGPASSWORD=dbos`, Kafka on `localhost:9092`.
- Cloud execution: pending.

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072714344022000Z.prompt.md`.
- Frontier ID: `kafka-exactly-once-consumer`.
- Rung ID: `rung-004-bounded-seed-sweep`.
- Protected product promise: preserve the concrete `kafka-exactly-once-consumer` promise from `frontier.md` and `strategy/candidates/kafka-exactly-once-consumer.md`.
- Replay command: `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-004-bounded-seed-sweep --case <case-id>`.
- Seed policy: exact seeds listed in front matter; every run must persist seed plus derived case JSON.
- Invariant oracle: produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key.

##### Goal

- Build and run: rare-bug search across duplicate timing, restart windows, and consumer generation changes.
- Preserve: the area promise, failure mechanism, and oracle already mapped for `kafka-exactly-once-consumer` without broadening into unrelated DBOS surfaces.

##### Workload File

- Expected path: `.workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py`.
- Create or reuse: create this file when Workload Runner first reaches this frontier; reuse it for later rungs when the actor, product goal, and oracle remain the same.
- Why one file is enough for this rung: the matrix varies seed, schedule, and case shape while preserving one frontier and one oracle family.
- When to create a new file instead: only when setup, actor, or oracle shape would become ambiguous in one parameterized harness.

##### Workload Shape

- Type: Python module/integration stateful workload.
- Entry points: Kafka broker/topic/group generation, produced records, keys, offsets, DBOS workflow IDs, idempotency keys, side-effect ledger rows, and offset commits.
- Sequence:
  - Launch the workload against an isolated DBOS/Postgres environment when durability matters.
  - Build an independent case model before calling DBOS APIs.
  - Execute the matrix case sequentially and record every generated operation.
  - Compare public results, terminal state, and any read-only durable observations against the model.
- Variance: seed controls identifiers, operation order, timing offsets, and data shape within this rung's bounded matrix.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | bounded sweep | duplicate-key preserves the frontier oracle | generate bounded duplicate-key variant from seed | case reaches duplicate-key evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-002 | bounded sweep | offset-replay preserves the frontier oracle | generate bounded offset-replay variant from seed | case reaches offset-replay evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-003 | bounded sweep | rebalance-backlog preserves the frontier oracle | generate bounded rebalance-backlog variant from seed | case reaches rebalance-backlog evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-004 | bounded sweep | broker-before-poll preserves the frontier oracle | generate bounded broker-before-poll variant from seed | case reaches broker-before-poll evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-005 | bounded sweep | broker-during-workflow preserves the frontier oracle | generate bounded broker-during-workflow variant from seed | case reaches broker-during-workflow evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-006 | bounded sweep | late-result-read preserves the frontier oracle | generate bounded late-result-read variant from seed | case reaches late-result-read evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-007 | bounded sweep | duplicate-key preserves the frontier oracle | generate bounded duplicate-key variant from seed | case reaches duplicate-key evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-008 | bounded sweep | offset-replay preserves the frontier oracle | generate bounded offset-replay variant from seed | case reaches offset-replay evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-009 | bounded sweep | rebalance-backlog preserves the frontier oracle | generate bounded rebalance-backlog variant from seed | case reaches rebalance-backlog evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-010 | bounded sweep | broker-before-poll preserves the frontier oracle | generate bounded broker-before-poll variant from seed | case reaches broker-before-poll evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-011 | bounded sweep | broker-during-workflow preserves the frontier oracle | generate bounded broker-during-workflow variant from seed | case reaches broker-during-workflow evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-012 | bounded sweep | late-result-read preserves the frontier oracle | generate bounded late-result-read variant from seed | case reaches late-result-read evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-013 | bounded sweep | duplicate-key preserves the frontier oracle | generate bounded duplicate-key variant from seed | case reaches duplicate-key evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-014 | bounded sweep | offset-replay preserves the frontier oracle | generate bounded offset-replay variant from seed | case reaches offset-replay evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-015 | bounded sweep | rebalance-backlog preserves the frontier oracle | generate bounded rebalance-backlog variant from seed | case reaches rebalance-backlog evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-016 | bounded sweep | broker-before-poll preserves the frontier oracle | generate bounded broker-before-poll variant from seed | case reaches broker-before-poll evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-017 | bounded sweep | broker-during-workflow preserves the frontier oracle | generate bounded broker-during-workflow variant from seed | case reaches broker-during-workflow evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-018 | bounded sweep | late-result-read preserves the frontier oracle | generate bounded late-result-read variant from seed | case reaches late-result-read evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-019 | bounded sweep | duplicate-key preserves the frontier oracle | generate bounded duplicate-key variant from seed | case reaches duplicate-key evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-020 | bounded sweep | offset-replay preserves the frontier oracle | generate bounded offset-replay variant from seed | case reaches offset-replay evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-021 | bounded sweep | rebalance-backlog preserves the frontier oracle | generate bounded rebalance-backlog variant from seed | case reaches rebalance-backlog evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-022 | bounded sweep | broker-before-poll preserves the frontier oracle | generate bounded broker-before-poll variant from seed | case reaches broker-before-poll evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-023 | bounded sweep | broker-during-workflow preserves the frontier oracle | generate bounded broker-during-workflow variant from seed | case reaches broker-during-workflow evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |
| case-024 | bounded sweep | late-result-read preserves the frontier oracle | generate bounded late-result-read variant from seed | case reaches late-result-read evidence point | produced record model, consumed offsets, workflow IDs, and side-effect rows agree with one terminal effect per modeled key |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3740 | generate-bounded-duplicate-key-variant-from-seed | none unless case says setup block | duplicate-key preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-002 | 3741 | generate-bounded-offset-replay-variant-from-seed | none unless case says setup block | offset-replay preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-003 | 3742 | generate-bounded-rebalance-backlog-variant-from- | none unless case says setup block | rebalance-backlog preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-004 | 3743 | generate-bounded-broker-before-poll-variant-from | none unless case says setup block | broker-before-poll preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-005 | 3744 | generate-bounded-broker-during-workflow-variant- | none unless case says setup block | broker-during-workflow preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-006 | 3745 | generate-bounded-late-result-read-variant-from-s | none unless case says setup block | late-result-read preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-007 | 3746 | generate-bounded-duplicate-key-variant-from-seed | none unless case says setup block | duplicate-key preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-008 | 3747 | generate-bounded-offset-replay-variant-from-seed | none unless case says setup block | offset-replay preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-009 | 3748 | generate-bounded-rebalance-backlog-variant-from- | none unless case says setup block | rebalance-backlog preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-010 | 3749 | generate-bounded-broker-before-poll-variant-from | none unless case says setup block | broker-before-poll preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-011 | 3750 | generate-bounded-broker-during-workflow-variant- | none unless case says setup block | broker-during-workflow preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-012 | 3751 | generate-bounded-late-result-read-variant-from-s | none unless case says setup block | late-result-read preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-013 | 3752 | generate-bounded-duplicate-key-variant-from-seed | none unless case says setup block | duplicate-key preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-014 | 3753 | generate-bounded-offset-replay-variant-from-seed | none unless case says setup block | offset-replay preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-015 | 3754 | generate-bounded-rebalance-backlog-variant-from- | none unless case says setup block | rebalance-backlog preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-016 | 3755 | generate-bounded-broker-before-poll-variant-from | none unless case says setup block | broker-before-poll preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-017 | 3756 | generate-bounded-broker-during-workflow-variant- | none unless case says setup block | broker-during-workflow preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-018 | 3757 | generate-bounded-late-result-read-variant-from-s | none unless case says setup block | late-result-read preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-019 | 3758 | generate-bounded-duplicate-key-variant-from-seed | none unless case says setup block | duplicate-key preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-020 | 3759 | generate-bounded-offset-replay-variant-from-seed | none unless case says setup block | offset-replay preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-021 | 3760 | generate-bounded-rebalance-backlog-variant-from- | none unless case says setup block | rebalance-backlog preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-022 | 3761 | generate-bounded-broker-before-poll-variant-from | none unless case says setup block | broker-before-poll preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-023 | 3762 | generate-bounded-broker-during-workflow-variant- | none unless case says setup block | broker-during-workflow preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |
| case-024 | 3763 | generate-bounded-late-result-read-variant-from-s | none unless case says setup block | late-result-read preserves the frontier oracle | produced record model, consumed offsets, workflow IDs, and side-effect rows agre |


##### Invariants

- Must hold: each produced record is modeled by topic, partition, offset, key, value, and expected workflow id before consumption.
- Must hold: duplicate keys or replayed offsets produce at most one modeled side effect.
- Must hold: consumer restart/rebalance never loses a committed record or creates an extra terminal workflow.
- Must hold: broker interruption artifacts include offset/consumer generation evidence, not only command exit.
- Must never happen: a run passes without proving Kafka broker/topic/group setup and modeled offset windows.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/`
  - `evidence-key:strategy/candidates/kafka-exactly-once-consumer.md`
  - `evidence-key:frontiers/kafka-exactly-once-consumer/frontier.md`
- Suggested command family:
  - `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-004-bounded-seed-sweep --case case-001`
  - `python .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-004-bounded-seed-sweep --all-cases --sequential`
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

### Rung: rung-005-finding-minimization

Evidence source: `evidence-key:frontiers/kafka-exactly-once-consumer/rungs/rung-005-finding-minimization.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-005-finding-minimization
frontier: kafka-exactly-once-consumer
status: finding_minimized_cloud
order: 5
level: minimization
workload_file: .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py
seeds:
  - 3770
  - 3771
updated_at: 2026-06-21T00:13:16Z
```

#### Rung 005 Finding Minimization

##### Run Status

- Status: finding minimized in cloud.
- Trigger: cloud finding candidate from rung 004 case `case-003`.
- Source evidence: `evidence-key:runs/run-20260621T000200Z-kafka-exactly-once-consumer-rung-004-cloud-diagnosis/summary.md`.
- Local minimization evidence: `evidence-key:runs/run-20260621T000739Z-kafka-exactly-once-consumer-rung-005-local-minimized/summary.md`.
- Cloud minimization evidence: `evidence-key:runs/run-20260621T001316Z-kafka-exactly-once-consumer-rung-005-cloud-minimized/summary.md`.

##### Source Contract

- Frontier ID: `kafka-exactly-once-consumer`.
- Rung ID: `rung-005-finding-minimization`.
- Protected product promise: every Kafka record accepted by the consumer group must either create a durable DBOS workflow/effect or remain available for re-consumption after DBOS relaunch.
- Replay command: `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/kafka-exactly-once-consumer/kafka_exactly_once_consumer_workload.py --rung rung-005 --case <case-id>`.
- Invariant oracle: produced record model, consumed offsets, workflow IDs, terminal workflow rows, and side-effect rows agree.

##### Goal

Shrink the rung 004 cloud failure from a four-record bounded sweep into the smallest deterministic reproducer that still preserves the same failure mechanism: immediate DBOS relaunch after Kafka consumer start can skip a produced offset.

##### Attack Plan

| Case | Seed | Axis | Perturbation | Oracle |
| --- | --- | --- | --- | --- |
| case-001 | 3770 | single in-flight offset | produce one slow workflow record, launch consumer, relaunch after 150 ms without acceptance gate | the single produced offset is observed and reaches terminal workflow/effect state |
| case-002 | 3771 | two-record backlog | produce two records, make the first workflow slow, launch consumer, relaunch after 150 ms without acceptance gate | both produced offsets are observed and reach terminal workflow/effect state |

##### Expected Signatures

- Finding preserved: a produced offset is missing from `acceptance-ledger.json`, `offset-ledger.json`, and `workflow-statuses.json` after relaunch.
- Finding shrunk: `case-001` fails with one record. If `case-001` passes and `case-002` fails, the minimal known reproducer is a two-record backlog.
- Not a finding: both cases pass under the same cloud worker/image class, which means the rung 004 failure requires the wider four-record timing shape.

##### Current Evidence

Local and cloud `case-001` failed with one produced offset and empty acceptance, offset, effect, and workflow-status ledgers after immediate DBOS relaunch. The invariant `consumer_observed_every_produced_offset` failed, preserving the original rung 004 cloud failure mechanism in a smaller case.

Cloud workload: `01KVKR8552CZXRG3EYQTC5ZVMH`.

##### Stop Conditions

- Stop after one cloud case preserves the failure with smaller record count.
- Stop as not reproduced only after both minimized cases pass in cloud on a prepared image that includes this rung.
