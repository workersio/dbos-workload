---
target: dbos
runner: .workers/run-with-postgres.sh .workers/python-runtime.sh
actor-model: process-parallel
personas:
  workflow-runner:
    weight: 0.55
    flows: [durable-workflow]
    citation: "README.md:41-42 'DBOS workflows make your program durable by checkpointing its state in Postgres. If your program ever fails, when it restarts all your workflows will automatically resume from the last completed step.' — the headline quickstart flow every user writes first."
  task-producer:
    weight: 0.40
    flows: [enqueue-task]
    citation: "README.md:78-79 'you can enqueue a task ... one of your processes will pick it up ... it guarantees that tasks complete, and that their callers get their results without needing to resubmit them, even if your application is interrupted.'"
  api-explorer:
    weight: 0.05
    flows: []
    citation: builtin
flows:
  durable-workflow:
    invariants: [step-exactly-once, resumes-after-crash, workflow-terminal]
    citation: "README.md:41-42 (durability + resume-from-last-step); enforced by the vendor at tests/test_dbos.py:432-438 (recovery re-runs the workflow body but a completed step's counter stays at 1) and _core.py:1816-1847 (recorded step output replayed, not re-run)."
  enqueue-task:
    invariants: [task-completes-once, dedup-id-enforced]
    citation: "README.md:78-79 (tasks complete exactly-once, results collected without resubmit); dedup at tests/test_queue.py:1863-1898 and _sys_db.py:783-791 (unique (queue_name, deduplication_id) -> DBOSQueueDeduplicatedError)."
events:
  crash-restart:
    amplification: 25
    citation: "README.md:42 'if your program ever fails, when it restarts all your workflows will automatically resume' — the product's core promise. Simulated with the vendor's own injection: force in-flight rows to status PENDING then DBOS._recover_pending_workflows() (tests/test_dbos.py:425-433)."
modules:
  - {name: _core.py, covered-by: [durable-workflow, enqueue-task]}
  - {name: _dbos.py, covered-by: [durable-workflow, enqueue-task]}
  - {name: _sys_db.py, covered-by: [durable-workflow, enqueue-task]}
  - {name: _sys_db_postgres.py, covered-by: [durable-workflow, enqueue-task]}
  - {name: _app_db.py, covered-by: [durable-workflow]}
  - {name: _queue.py, covered-by: [enqueue-task]}
  - {name: _recovery.py, covered-by: [durable-workflow]}
  - {name: _registrations.py, covered-by: [durable-workflow, enqueue-task]}
  - {name: _context.py, covered-by: [durable-workflow, enqueue-task]}
  - {name: _serialization.py, covered-by: [durable-workflow, enqueue-task]}
  - {name: _migration.py, covered-by: [durable-workflow]}
  - {name: _dbos_config.py, covered-by: [durable-workflow]}
  - {name: _outcome.py, covered-by: [durable-workflow]}
  - {name: _error.py, covered-by: [enqueue-task, durable-workflow]}
  - {name: _schemas, covered-by: [durable-workflow, enqueue-task]}
  - {name: __init__.py, covered-by: [durable-workflow]}
  - {name: _client.py, parked: "DBOSClient management flows (list/cancel/resume/fork) modeled in the next producer refresh (row 4)"}
  - {name: _workflow_commands.py, parked: "cancel/resume/fork/gc/global-timeout flows — next refresh"}
  - {name: _admin_server.py, parked: "admin HTTP surface — next refresh"}
  - {name: _scheduler.py, parked: "cron/schedule flow — next refresh"}
  - {name: _scheduler_decorator.py, parked: "cron/schedule flow — next refresh"}
  - {name: _croniter.py, parked: "cron parsing — next refresh with the schedule flow"}
  - {name: _kafka.py, parked: "kafka consumer flow — next refresh; needs the kafka-broker recipe"}
  - {name: _kafka_message.py, parked: "kafka consumer flow — next refresh"}
  - {name: _debouncer.py, parked: "debounce flow — next refresh"}
  - {name: _datasource.py, parked: "external datasource decorator — next refresh"}
  - {name: _datasource_postgres.py, parked: "external datasource decorator — next refresh"}
  - {name: _datasource_sqlite.py, parked: "sqlite datasource; postgres runner only"}
  - {name: _sys_db_sqlite.py, parked: "sqlite system-db backend; postgres runner only"}
  - {name: _roles.py, parked: "RBAC/authz flow — next refresh"}
  - {name: _fastapi.py, parked: "FastAPI integration; no durability contract exercised in sim"}
  - {name: _flask.py, parked: "Flask integration; no durability contract exercised in sim"}
  - {name: _event_loop.py, parked: "async event loop; async flow forms deferred to a later refresh"}
  - {name: _validation.py, parked: "input validation; not yet under test"}
  - {name: _tracer.py, parked: "OTLP tracing; disabled via enable_otlp=False"}
  - {name: _logger.py, parked: "logging; carries no durability contract"}
  - {name: _classproperty.py, parked: "DBOS.x classproperty plumbing; no runtime contract"}
  - {name: _utils.py, parked: "internal helpers; no user contract"}
  - {name: _debug_trigger.py, parked: "debug hook; not a user surface"}
  - {name: _docker_pg_helper.py, parked: "docker postgres bootstrap; the runner provides postgres"}
  - {name: _conductor, parked: "conductor cloud connection — next refresh"}
  - {name: cli, parked: "CLI entrypoint; no runtime surface reachable in sim"}
  - {name: _templates, parked: "project scaffolding templates; not runtime"}
---

# Usage model — DBOS Transact (Python)

DBOS is a library that adds **durable workflows** to an ordinary Python program
backed by Postgres. The one promise under everything (README.md:41-42): *if the
process dies, on restart every workflow resumes from its last completed step and
each step runs exactly once.* This model starts from the two hottest ways users
lean on that promise and amplifies the one event that tests it — a crash.

## Personas

- **workflow-runner** (0.55) — the application developer's process running
  `@DBOS.workflow()`/`@DBOS.step()` code. Every README example and the starter
  template is this actor. Drives `durable-workflow`.
- **task-producer** (0.40) — a process that enqueues background tasks on a DBOS
  `Queue` and collects their results (README.md:73-105). Drives `enqueue-task`.
- **api-explorer** (0.05) — the built-in rarity sampler over the cold verb
  inventory (management/notifications/scheduler surfaces parked below); feeds the
  api-floor candidate source until those flows are modeled.

Deliberately left out of the first model (added by later row-4 refreshes, in
priority order): the **ops-operator** persona (DBOSClient cancel/resume/fork —
_client.py, _workflow_commands.py), durable **notifications** (send/recv,
set/get_event — _core.py), the **scheduler** (cron), **kafka**, **streaming**,
and **debounce** flows. The strongest untested interactions the scouts flagged
live there (queue × cancel × restart; concurrent multi-executor recovery) — this
model floors the two core flows first so those interaction reds are attributable.

REFUTED (e7): the scout-flagged `write_stream_from_step` "OAOO gap" is NOT a bug —
DBOS steps are at-least-once for their body side effects, and the vendor's own
`tests/test_streaming.py:604-659` (`test_stream_write_from_step`) asserts one
stream value PER ATTEMPT ("each failure should still write to the stream"). A
`stream-write-once` invariant contradicts documented, tested behavior. Lesson: a
scout's "suspected gap" must be checked against the vendor's own tests before it
becomes a flow invariant — the strategy-critic gate should run on every refresh.

## Flows and what they promise

- **durable-workflow** — run a multi-step workflow to a terminal state.
  Invariants: `step-exactly-once` (a completed step never re-runs, even after a
  crash-restart), `resumes-after-crash` (a workflow interrupted mid-flight is
  driven to SUCCESS by recovery and returns its correct result), and
  `workflow-terminal` (the workflow reaches a terminal status). The oracle rides
  process-global side-effect counters that DBOS does *not* checkpoint, so a
  re-run step is directly visible.
- **enqueue-task** — enqueue tasks on a `Queue` and collect results.
  Invariants: `task-completes-once` (each enqueued task runs to completion once
  and its result is collectable without resubmit) and `dedup-id-enforced` (a
  second enqueue with a live `deduplication_id` is refused, and the refused task
  never runs).

## The event

- **crash-restart** (amplification 25) — a realistic process crash mid-workflow.
  Real programs crash rarely; we land it far more often than life to probe the
  recovery promise. Injected exactly as the vendor's own tests do: force the
  in-flight workflow rows to `PENDING` and call `_recover_pending_workflows()`,
  which re-runs workflow bodies while skipping already-completed steps.

## Amplification / what the weights mean

Weights are docs-emphasis, not traffic telemetry (no telemetry exists — G6).
`durable-workflow` outranks `enqueue-task` because it is the quickstart's first
lesson; the crash-restart amplification is high on purpose — the whole product
exists for that moment, so uniform sampling would almost never test it.
