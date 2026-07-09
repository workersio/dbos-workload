---
id: dbos.overview
area: repo-overview
promise: DBOS workload evidence remains discoverable without turning map.md into a live queue or strategy document
migration_status: enriched
classification: context
workloads: []
evidence:
  - ../map.md
faults: []
---

# DBOS Workload Overview

## Purpose

This overview gives producer agents higher-order search context connected from
the map. The map can remain the broad starting index; this file helps decide
which surfaces are already harvested and where deeper or broader search is
worthwhile.

## Carried-Forward Reality Notes

| Reality | Consequence |
|---|---|
| Five candidate product findings survived minimization or cloud confirmation. | Treat their areas as already harvested unless designing a strictly deeper adjacent bug surface. |
| One recovery DB candidate was closed as a workload-model artifact. | Recovery rungs must not assume `recover_pending_workflows()` is a barrier before recovered workflow bodies execute. |
| Several setup/platform issues were real but not DBOS bugs. | Separate setup blockers from product findings before adding work-item rungs. |
| Green bounded sweeps existed for many areas. | Green means modeled cases passed, not that the product surface is exhausted. |
| Kafka only became meaningful after broker persistence, musl packaging, and cloud path issues were fixed. | Optional-service areas need setup proof before product oracles. |
| Cloud `/workspace` is read-only. | New workloads must write artifacts under `/tmp/...`, not under `/workspace`. |
| Live Formal once mislabeled baseline invariant failures as fault-model failures. | Classify with run command and invariant evidence, not labels alone. |

## Promoted Historical Findings

| Finding | Area | Evidence key |
|---|---|---|
| Kafka produced offset disappears after immediate DBOS relaunch; fixed upstream by issue `#733` / PR `#738` in current target | kafka-exactly-once-consumer | `evidence-key:findings/kafka-exactly-once-consumer-lost-offset-after-relaunch.md` |
| Missing Docker secret falls back to SQLite while app migration writes Postgres | cli-starter-onboarding | `evidence-key:findings/cli-starter-onboarding-missing-docker-secret-falls-back-sqlite.md` |
| Native workflow under portable JSON config masks app exception | serialization-error-fidelity | `evidence-key:findings/serialization-error-fidelity-native-workflow-portable-config-masks-error.md` |
| Fork with `replacement_children` uses replacement outputs but child graph/delete does not own them | lifecycle-fork-state | `evidence-key:findings/lifecycle-fork-state-replacement-children-missing-child-links.md` |
| `global_timeout` cancels future delayed workflow | lifecycle-fork-state | `evidence-key:findings/lifecycle-fork-state-global-timeout-cancels-delayed.md` |

## Closed Historical Candidate

| Candidate | Final status | Why it matters |
|---|---|---|
| Recovery DB restart after first recovered workflow caused gate timeout | `closed_workload_model_artifact` | Future recovery workloads must not treat recovery handle collection as a barrier. |

## Area Context Index

| Area | Current reality | Area file |
|---|---|---|
| recovery-db-faults | Completed; one candidate closed as workload-model artifact; stale queued recovery finding filed as `#742` with PR `#744` open | ../areas/recovery-db-faults.md |
| queue-composed-controls | Completed green through bounded sweep; async partition worker-concurrency finding represented; rate-limit partial-index plan rung ready | ../areas/queue-composed-controls.md |
| message-event-cancellation | Completed green through bounded sweep; live stream resume/listener-offset rung represented; client get_event prompt polling rung ready | ../areas/message-event-cancellation.md |
| lifecycle-fork-state | Two minimized findings; cancel-children terminal immutability rung ready | ../areas/lifecycle-fork-state.md |
| scheduler-debouncer-timing | Completed green; scheduled-queue controls and async debouncer worker-starvation rungs represented; overlap policy remains open in issue `#718` | ../areas/scheduler-debouncer-timing.md |
| datasource-transaction-oaoo | Cloud finding candidate from PR `#680`: SQLite locked datasource retry recorded terminal error before any retry attempt; Postgres serialization/deadlock and non-retryable controls passed | ../areas/datasource-transaction-oaoo.md |
| schema-isolation-multi-client | New from PR `#728`; multi-schema client/datasource isolation rung represented | ../areas/schema-isolation-multi-client.md |
| workflow-attributes-query | Completed green; scheduled workflow identity, legacy scheduler app-version, and temporal introspection rungs represented | ../areas/workflow-attributes-query.md |
| async-checkpoint-determinism | Composed async checkpoint green; queued async cancellation task-release finding represented; preemptible-step cancellation rung represented | ../areas/async-checkpoint-determinism.md |
| serialization-error-fidelity | One minimized cloud finding; structured portable metadata and retry-class stored-error liveness rungs represented | ../areas/serialization-error-fidelity.md |
| portable-input-type-fidelity | New from issue `#697` and PR `#700`; portable JSON datetime/date input type rung represented | ../areas/portable-input-type-fidelity.md |
| control-plane-state-introspection | New from PR `#694`; rotten schedule-context introspection rung represented | ../areas/control-plane-state-introspection.md |
| auth-context-sql-enqueue | SQL-origin auth context finding filed as `#743` with PR `#744` open | ../areas/auth-context-sql-enqueue.md |
| system-db-retry-idempotence | New from PR `#740`; committed sys-db retry re-entry rung represented | ../areas/system-db-retry-idempotence.md |
| runtime-shutdown-event-loop-liveness | New from PR `#722`; adopted-loop timeout destroy liveness rung represented | ../areas/runtime-shutdown-event-loop-liveness.md |
| schedule-registry-concurrency | Concurrent public apply caller conflict finding represented | ../areas/schedule-registry-concurrency.md |
| kafka-exactly-once-consumer | Historical minimized/confirmed cloud finding; fixed upstream by PR `#738` in current target | ../areas/kafka-exactly-once-consumer.md |
| cli-starter-onboarding | One cloud-confirmed config finding; template matrix partly closed | ../areas/cli-starter-onboarding.md |
| migration-startup-liveness | Cloud finding candidate from PR `#677`; concurrent up-to-date warm starts waited behind held migration advisory lock | ../areas/migration-startup-liveness.md |
| decorator-composition-fidelity | Cloud finding candidate from PR `#706`: completed replay of DBOS-outer wrapped async workflow reruns inner app hook | ../areas/decorator-composition-fidelity.md |

## Producer Search Summary

| Area | Harvest status | Deeper search signal | Broader search signal |
|---|---|---|---|
| recovery-db-faults | Existing recovery evidence plus `E-002` stale queued recovery finding filed as `#742`; PR `#744` is open. One old candidate was closed as workload-model artifact. | Queue/recovery ownership can still be deepened only with a distinct no-barrier oracle; do not rediscover stale queued recovery from `E-002`. | Recovery plus lifecycle/result-retrieval remains possible if product promise is distinct from old gate-timeout artifact and `#742`. |
| queue-composed-controls | Functional composed-control rungs are green; `E-006` preserves async partition overstart evidence, and `E-027` is a ready Postgres rate-limit partial-index plan guard from issue `#696` / PR `#698`. | Deeper search should add a distinct queue correctness or scalability oracle, not another rate-limit behavior seed or partition overstart replay. | Broader queue corridors can include multi-queue fairness, recovery/lifecycle joins, or other query-plan guards if they prove a production-scale access path. |
| message-event-cancellation | Completed green through cancellation cleanup, fallback, fork/stream, recovery replay, bounded sweep, and `E-012` live stream offset/listener paths. PR `#721` cancelled waiter cleanup is already harvested here; `E-025` represents the client `get_event` prompt polling gap from PR `#713`. | Deeper search should target a new communication-state join, such as cleanup after delete/fork lifecycle or client-facing waiters beyond the represented `get_event` and stream paths. | Broader search can include cross-workflow fanout or framework/client API waiters if the oracle is distinct from stale waiter cleanup, client event polling, and stream offset replay. |
| scheduler-debouncer-timing | Scheduled queue controls are green; `E-008` is environment-sensitive finding evidence. Schedule overlap remains policy-only while issue `#718` is open. | Need standalone or matched-environment minimization before treating debouncer starvation as settled; do not turn overlap observations into failing invariants until DBOS defines the policy. | Other scheduler/debouncer contracts are valid only if they avoid repeating `E-001`/`E-008`; scheduled overlap can become a workload only after a concrete product contract exists. |
| serialization-error-fidelity | Historical native portable-config masking finding plus `E-003`; retry-class liveness green in `E-010`. | Different error paths can be searched if they preserve actionable error metadata. | Serialization surfaces outside durable error retrieval may justify new work items. |
| system-db-retry-idempotence | New ready corridor from PR `#740`; no run evidence yet. | Deeper search should add a different sys-db retry surface or a stronger deterministic fault injector, not just more seeds. | Other resilience corridors may come from dependency failures outside system DB retry loops. |
| datasource-transaction-oaoo | Transaction, cleanup, bounded sweep, and transactional send rungs are green; `E-023` preserves a SQLite locked datasource retry liveness finding candidate from issue `#679` / PR `#680`, while the Postgres serialization/deadlock and non-retryable controls passed. | Deeper search should target a distinct datasource retry/session freshness surface, not another SQLite locked retry replay. | Broader datasource corridors can include cross-schema retry, external dependency outage, or transaction helper composition if they keep durable app/output/replay agreement. |
| runtime-shutdown-event-loop-liveness | New ready corridor from PR `#722`; no run evidence yet. | Deeper search should add a distinct shutdown lifecycle fault, such as admin server/listener/queue-worker teardown under active work. | Broader runtime lifecycle corridors can include process startup/shutdown, atexit, and framework lifespan integration if they have bounded oracles. |
| schedule-registry-concurrency | `E-018` preserves concurrent public apply caller conflict evidence from the delete-then-create schedule registry path. | Deeper search should add a distinct registry mutation surface, such as create/delete/pause races, mixed client/public repair validation, live reapply after the caller race is fixed, or multi-process schedulers, not only more concurrent apply seeds. | Broader scheduler control-plane corridors can include conductor-driven schedule mutation or distributed scheduler coordination if they have durable row and live-execution oracles. |
| auth-context-sql-enqueue | `E-015` denied SQL-origin required-role finding filed as `#743`; PR `#744` is open. | Deeper search should target a different SQL-origin auth surface, such as assumed-role semantics or auth metadata through export/import after repair, not the denied-role pending row. | Broader auth corridors can include non-SQL producers only if they preserve an authorization/ownership oracle distinct from workflow attributes. |
| async-checkpoint-determinism | `E-004` is green; `E-019` preserves a queued async cancellation task-release finding from the #710/#711 task-pinning surface; `E-021` preemptible-step cancellation is green from issue `#660` / PR `#671`. | Deeper search should preserve a distinct async liveness/preemption oracle such as task reachability, operation-output isolation, error-branch cleanup, or event-loop ownership, not repeat narrow gather/patch/product GC/cancellation-leak/preemptible-step tests. | Broader async corridors can include framework lifespan, many-client async APIs, nested preemption with child workflows, or recovery joins if they have terminal-state and task-lifecycle observations. |
| workflow-attributes-query | Attribute query, scheduled identity, legacy app-version, and `E-020` temporal introspection/aggregate rungs are green. | Deeper search should target a different introspection contract, such as conductor pagination under high cardinality or migration of historical rows, not just more timestamp or aggregate seeds. | Broader query/introspection corridors can include operator-facing pagination, conductor protocol parity outside aggregates, or cross-version export/import if they keep a durable status/query oracle. |
| decorator-composition-fidelity | `E-024` preserves a completed replay hook-duplication finding candidate from PR `#706`; the remaining entrypoint matrix cases did not run because `case-001` failed first. | Deeper search should target different metadata-preserving wrappers, framework decorators, auth decorators, or invalid missing-`wraps` rejection controls without repeating the completed-replay hook duplication signature. | Broader API-composition corridors can include decorators combined with validation, roles, or framework adapters if they keep durable DBOS metadata and replay oracles. |
| lifecycle-fork-state | Replacement-child graph and global-timeout findings already harvested; `E-026` is a ready cancellation cascade and terminal `CANCELLED` immutability corridor from PRs `#701` / `#703`. | Adjacent lifecycle APIs must avoid those exact minimized signatures and avoid repeating `E-026`'s recursive cancel, queued descendant, late completion, and client parity oracle. | Query/filter, deletion, events/streams, recovery joins, or cancellation composed with other DBOS subsystems can become broader corridors when they add a distinct state model. |
| kafka-exactly-once-consumer | Offset-loss after relaunch already harvested and fixed upstream by issue `#733` / PR `#738` in the current target. | Continue only with setup proof and a different Kafka guarantee, or rerun the historical repro strictly as a regression guard. | Multi-topic, transaction coupling, retry/DLQ, rebalance semantics, or behavior around user-supplied commit/storage config are broader directions. |
| migration-startup-liveness | `E-022` is a cloud finding candidate: concurrent up-to-date warm starts waited behind a held migration advisory lock even though stale/partial schema safety cases passed. | Deeper search should target a distinct startup/migration fault such as invalid online-index cleanup after crash or CockroachDB-specific semantics, not just more warm-start workers under the same lock model. | Broader startup corridors can include app-database migration commands, generated-app bootstrap, or cloud config only if they keep a schema-state and startup-liveness oracle distinct from cli-starter smoke. |

## Claimable Work Convention

Producer marks a rung `ready` only when fault details, freshness, oracle,
workload plan, and replay command are concrete enough for executor. Executor may
write a mechanical blocked/done status and run link back to the selected work
item, but producer/triage owns curated interpretation.

Current claimable rungs:

- None. Current work-item front matter has no `classification: ready` entries.

## Producer Guidance

Use this context when deciding whether new work is genuinely adjacent or merely
rediscovering an old finding. Do not copy these tables back into `map.md`.
