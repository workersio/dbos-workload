# Area: message-event-cancellation

## Current State

Current status: completed green through bounded sweep and the loop-1 live
stream resume/listener-offset rung. No product finding; a new client
`get_event` prompt polling rung from PR `#713` is executor-ready.

Evidence:

- `evidence-key:frontiers/message-event-cancellation/frontier.md`
- `evidence-key:runs/run-20260620T124800Z-message-event-cancellation-rung-004-bounded-seed-sweep/summary.md`
- `.workers/workloads/message-event-cancellation/message_event_cancellation_workload.py`
- Issue `#686` and PR `#691`: stream reads now use LISTEN/NOTIFY for blocked
  readers, with polling fallback and stream listener cleanup.
- Issue `#692` and PR `#693`: public runtime and client stream readers accept
  an explicit offset so reconnect-style consumers can resume from a last-seen
  event instead of replaying the whole stream.
- PRs `#691` and `#693` passed integration checks across Postgres and SQLite
  for Python 3.10 through 3.14.
- WIO cloud run `01KVVR752868AG2HW0ZWJ5MKTA` passed 48/48 live stream
  resume/listener-offset invariants for runtime sync/async, client sync/async,
  listener-disabled fallback, unclosed stream termination, listener cleanup,
  and post-relaunch offset resume. No product finding.
- PR `#713`: "Fix Timing Issues" fixed DBOSClient `get_event` polling so a
  client without an in-process notification listener re-checks the database
  promptly instead of waiting for the full timeout.

## Product Promise

Messages, events, streams, waits, timeouts, cancellation, fork delivery, and
recovery replay preserve exactly the expected observable notifications and do
not leak stale waiters or duplicate deliveries.

## What Not To Repeat

- Do not repeat duplicate-send, timeout, fallback, fork stream, or basic replay
  matrices without a new state model.
- Do not assert scheduler/overlap policy from this frontier unless the contract
  is sourced from DBOS docs/code/tests.

## Loop-1 Search Directions

| Direction | Why It Is Deeper |
|---|---|
| Events plus lifecycle cleanup | Delete/cancel/fork can leave stale event waiters or stream rows after workflow cleanup. |
| Events plus recovery ownership | Recovered workflows may replay event waits or stream writes differently from normal execution. |
| Client/API waiters | Existing workload is runtime-heavy; client-facing wait/query paths may expose different stale state. |
| Cross-workflow fanout | Many waiters and forked listeners can test durable notification conservation beyond single workflow cases. |
| Live stream resume/readers | Recent stream LISTEN/NOTIFY and offset APIs add blocked-reader, reconnect, fallback, and listener-cleanup state not covered by completed-stream offset reads. |

## Rung Design Requirements

Each rung must name the notification/state ledger, expected delivery count,
timeout behavior, and cleanup expectation.

## Stale Conditions

Mark stale if DBOS changes event, notification, stream, or cancellation APIs.

## Rung Index

Evidence source: `evidence-key:frontiers/message-event-cancellation/rungs.index.yaml`

```yaml
schema_version: 1
columns: [id, path, status, order, level, workload_file, matrix, summary]
rungs:
  - [
      "rung-000-message-event-smoke",
      "rungs/rung-000-message-event-smoke.md",
      "not_run_optional",
      "0",
      "baseline",
      "read-only:/Users/viswa/code/workers/dbos-transact-py/tests/test_dbos.py",
      "1 case",
      "optional read-only smoke superseded by completed cloud workload rungs 001-004",
    ]
  - [
      "rung-001-duplicate-timeout-cancel",
      "rungs/rung-001-duplicate-timeout-cancel.md",
      "passed",
      "1",
      "adversarial",
      ".workers/workloads/message-event-cancellation/message_event_cancellation_workload.py",
      "3 cases",
      "all three WIO cloud cases passed cancellation, duplicate-send, durable-timeout, and stale-waiter invariants",
    ]
  - [
      "rung-002-listener-fallback-fork-stream",
      "rungs/rung-002-listener-fallback-fork-stream.md",
      "passed",
      "2",
      "adversarial",
      ".workers/workloads/message-event-cancellation/message_event_cancellation_workload.py",
      "5 cases",
      "all five WIO cloud cases passed listener fallback, fork fanout/event reads, and stream offset conservation",
    ]
  - [
      "rung-003-recovery-replay-cancellation",
      "rungs/rung-003-recovery-replay-cancellation.md",
      "passed",
      "3",
      "failure",
      ".workers/workloads/message-event-cancellation/message_event_cancellation_workload.py",
      "8 cases",
      "all eight WIO cloud cases passed executor relaunch and repeated cancellation/fallback invariants",
    ]
  - [
      "rung-004-bounded-seed-sweep",
      "rungs/rung-004-bounded-seed-sweep.md",
      "passed",
      "4",
      "sweep",
      ".workers/workloads/message-event-cancellation/message_event_cancellation_workload.py",
      "24 cases",
      "24-case WIO cloud bounded sweep passed across cancellation offsets, fallback polling, forks, streams, and replay interleavings",
    ]
  - [
      "rung-005-live-stream-resume-listener-offsets",
      "inline:loop-1-added-rung-rung-005-live-stream-resume-listener-offsets",
      "passed",
      "5",
      "cross-frontier",
      ".workers/workloads/message-event-cancellation/message_event_cancellation_workload.py",
      "3 cases",
      "WIO cloud run 01KVVR752868AG2HW0ZWJ5MKTA passed live stream explicit-offset resume, LISTEN/NOTIFY or fallback wakeup, listener cleanup, and unclosed termination invariants",
    ]
  - [
      "rung-006-client-get-event-prompt-polling",
      "inline:rung-006-client-get-event-prompt-polling",
      "ready",
      "6",
      "client-api-liveness",
      ".workers/workloads/message-event-cancellation/message_event_cancellation_workload.py",
      "4 cases",
      "DBOSClient sync/async get_event must poll promptly without an in-process listener, return terminal misses, clean waiter state, and preserve event values",
    ]
```

## Rung Details

These are the known rung contracts and outcomes for this frontier. Treat completed, finding, and closed rungs as current reality and design constraints, not as active executor queue rows.

### Loop-1 Added Rung: rung-005-live-stream-resume-listener-offsets

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-005-live-stream-resume-listener-offsets
frontier: message-event-cancellation
status: passed
order: 5
level: cross-frontier
workload_file: .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py
seeds: [3181, 3182, 3183]
updated_at: 2026-06-24T00:00:00Z
run_evidence:
  - ../runs/E-012.md
  - WIO cloud run 01KVVR752868AG2HW0ZWJ5MKTA
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/issues/686
  - https://github.com/dbos-inc/dbos-transact-py/issues/692
  - https://github.com/dbos-inc/dbos-transact-py/pull/691
  - https://github.com/dbos-inc/dbos-transact-py/pull/693
  - target/dbos/_dbos.py
  - target/dbos/_client.py
  - target/dbos/_sys_db.py
  - target/dbos/_sys_db_postgres.py
  - target/tests/test_streaming.py
  - .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-data-and-fixtures/overview.md
gate_results:
  surface_evidence: ready_from_issues_686_692_and_prs_691_693
  duplicate_check: existing_stream_cases_read_completed_streams_but_do_not_model_live_reconnect_reader_state
  oracle_critic: ready_with_resume_ledger_latency_bounds_listener_cleanup_and_terminal_parity
  executor_feasibility: default_profile_realistic_postgres_primary_sqlite_fallback_meaningful
```

#### Source Contract

- Frontier ID: `message-event-cancellation`.
- Rung ID: `rung-005-live-stream-resume-listener-offsets`.
- Protected product promise: DBOS stream readers deliver a gapless suffix from
  the requested offset while the writer is live, wake promptly through
  LISTEN/NOTIFY or documented polling fallback, clean their registered listener
  state, and terminate when the workflow ends even if the stream is never
  explicitly closed.
- Replay command:
  `python .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-005-live-stream-resume-listener-offsets --case <case-id>`.
- Seed policy: exact seeds `3181`, `3182`, `3183`; every run must persist
  workflow IDs, stream keys, initial offsets, reconnect offsets, write times,
  reader start/stop times, listener payload keys, fallback intervals, observed
  suffixes, and terminal workflow status.
- Invariant oracle: modeled stream sequence, public runtime/client reader
  observations, latency bounds, `streams_map` cleanup, workflow terminal state,
  and relaunch/reconnect reads must agree within bounded time.

#### Execution Evidence

- Run: `01KVVR752868AG2HW0ZWJ5MKTA`.
- Harness commit: `ddf016afbc9c9f1eaadd4591b149755d11250b55`.
- Target package: `dbos==0.0.0+3df88c4bcc3a`.
- Command:
  `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-005-live-stream-resume-listener-offsets --all-cases --sequential`.
- Result: exit code `0`; 48/48 invariants passed.
- Classification: green evidence, no product finding.

Covered cases:

- `case-001`, seed `3181`: runtime sync and async readers resumed from offset
  `2`, observed suffix `[2, 3, 4, 5]`, met live wakeup bounds, and left no
  stream listener entries.
- `case-002`, seed `3182`: DBOSClient sync/async readers and listener-disabled
  fallback resumed from offset `2`, delivered the modeled suffix, and cleaned
  listener state.
- `case-003`, seed `3183`: blocked unclosed reader exited after terminal
  workflow success, and post-relaunch offset read returned only the modeled
  suffix.

#### Goal

Extend the existing message/event workload with live stream reader cases that
exercise the state added by PRs `#691` and `#693`. The workload must not just
read offsets from already-closed streams; it should model reconnecting readers
that start at nonzero offsets while writes are still arriving, readers blocked
waiting for future values, and workflows that terminate without a close marker.

#### Workload File

- Expected path:
  `.workers/workloads/message-event-cancellation/message_event_cancellation_workload.py`.
- Create or reuse: reuse the existing message/event workload file; add a
  separate rung dispatch for these stream-reader cases.
- Why one file is enough: the existing workload already owns DBOS launch,
  Postgres setup, stream sequence ledgers, fallback controls, relaunch helpers,
  and artifact formatting for this frontier.

#### Workload Shape

- Type: product-runtime adversarial workload with public DBOS runtime/client
  stream APIs plus read-only listener-state inspection.
- Entry points:
  - `DBOS.write_stream`, `DBOS.write_stream_async`, `DBOS.close_stream`, and
    `DBOS.close_stream_async`
  - `DBOS.read_stream(..., offset=...)` and
    `DBOS.read_stream_async(..., offset=..., polling_interval_sec=...)`
  - `DBOSClient.read_stream(..., offset=...)` and
    `DBOSClient.read_stream_async(..., offset=...)`
  - `DBOS.start_workflow`, `DBOS.start_workflow_async`, handle result/status
  - read-only `dbos._sys_db.streams_map` snapshots for listener cleanup
- Fault model: reader reconnects from last-seen offsets, blocked readers waiting
  for future writes, LISTEN/NOTIFY disabled or unavailable fallback, client
  readers without an in-process notification listener, async readers with long
  polling intervals, workflow termination without explicit stream close, and
  relaunch after a prefix has been consumed.

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 3181 | runtime-live-reconnect-offsets | sync and async runtime readers consume a prefix, reconnect at last-seen offset while writer continues, with long async polling interval | observed suffix is gapless from the requested offset and low-latency delivery proves notification wakeup rather than full-interval polling |
| case-002 | 3182 | client-and-fallback-resume | DBOSClient reader resumes from offset while in-process listener is unavailable or `use_listen_notify=False` | client/fallback readers still deliver every modeled suffix within fallback bounds and unregister listener payloads |
| case-003 | 3183 | blocked-reader-termination-relaunch | reader catches up to a live unclosed stream, workflow terminates without close, then a fresh reader resumes after relaunch | blocked reader exits within termination bound, no stale `streams_map` entry remains, and post-relaunch offset read returns only the modeled suffix |

#### Invariants

- Must hold for reconnect/resume:
  - For each reader, observed values equal `modeled_sequence[offset:]` from the
    reader's requested offset, with no prefix replay, duplicates, gaps, or
    post-close values.
  - A reader that disconnects after a prefix and reconnects from last-seen
    offset sees the same suffix as a fresh reader starting at that offset.
  - Runtime sync, runtime async, client sync, and client async observations
    agree where each path is included in the case.
- Must hold for liveness and cleanup:
  - In-process LISTEN/NOTIFY cases use a polling interval long enough that
    sub-interval delivery is evidence of notification wakeup.
  - Client and disabled-listener fallback cases finish within the recorded
    fallback bound, not only at the outer workload timeout.
  - Reader `streams_map` payloads are unregistered after normal close,
    reconnect, exception, termination-without-close, and relaunch paths.
  - A reader blocked after catching up to an unclosed stream exits once the
    writer workflow reaches terminal state.
- Must hold for status/replay:
  - Writer handle result and workflow terminal status agree with the modeled
    stream close or termination path.
  - Relaunch/reconnect reads do not resurrect stale listeners or mutate prior
    terminal status.
  - All artifacts include the seed, derived offsets, timing windows, backend,
    product ref, and exact reader path used.

#### Setup And Classification

- Build profile: `default`.
- Backend: Postgres is primary for true LISTEN/NOTIFY evidence. SQLite or
  `use_listen_notify=False` runs are fallback/polling evidence only and must be
  labeled as such.
- Target ref handling: current pinned target contains PRs `#691` and `#693`; if
  an executor refreshes `./target`, record the new DBOS ref in the artifact.
- Expected runtime: bounded seconds per case; avoid fixed sleeps except where
  explicitly modeling fallback/polling intervals.

#### What This Must Not Repeat

- Do not repeat completed-stream offset reads from rung 002 or the bounded seed
  sweep.
- Do not assert millisecond latency on fallback/client paths that do not have an
  in-process notification listener.
- Do not treat a reader that only starts after workflow completion as proving
  live listener behavior.

#### Stale Conditions

Mark this rung stale if DBOS changes stream reader offset semantics,
LISTEN/NOTIFY trigger/channel implementation, `streams_map` listener cleanup,
workflow termination behavior for unclosed streams, client stream polling
contract, or the public sync/async `read_stream` signatures.

### Rung: rung-006-client-get-event-prompt-polling

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-006-client-get-event-prompt-polling
frontier: message-event-cancellation
status: ready
order: 6
level: client-api-liveness
workload_file: .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py
seeds: [7130, 7131, 7132, 7133]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/pull/713
  - target/dbos/_client.py
  - target/dbos/_sys_db.py
  - target/tests/test_client.py
  - .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/user-behavior-testing/overview.md
gate_results:
  surface_evidence: ready_from_pr_713_client_get_event_polling_fix_and_existing_client_waiter_gap
  duplicate_check: existing_runtime_get_event_fallback_and_client_stream_cases_do_not_cover_client_get_event_no_listener_polling
  product_test_gap: pr_713_tests_one_sync_prompt_delivery_path_without_async_terminal_or_waiter_cleanup_matrix
  oracle_critic: ready_with_client_latency_event_value_terminal_none_and_workflow_events_map_cleanup_invariants
```

#### Source Contract

- Frontier ID: `message-event-cancellation`.
- Rung ID: `rung-006-client-get-event-prompt-polling`.
- Protected product promise: DBOSClient `get_event` and `get_event_async`
  deliver event values promptly even though the client has no notification
  listener, return stable `None` for modeled terminal misses, and clean waiter
  state after success, timeout, and cancellation.
- Replay command:
  `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-006-client-get-event-prompt-polling --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `7130`, `7131`, `7132`, and `7133`; every run must
  persist workflow IDs, event keys, client path, timeout, event-set delay,
  polling interval, observed latency, final event table value, waiter payload,
  workflow terminal status, and cleanup observations.
- Invariant oracle: client result, latency bound, durable event value,
  workflow terminal state, and `workflow_events_map` cleanup must agree with
  the modeled event schedule.

#### Goal

Exercise PR `#713`'s client polling fix beyond the narrow product test. Runtime
`get_event` fallback is already harvested in rung 002, and client stream
fallback/resume is green in rung 005. This rung targets the distinct client
event path where no listener thread will ever signal the wait, so progress
depends on periodic database re-checks.

#### Workload Shape

- Type: product-runtime/client API liveness workload.
- Build profile: default real Postgres through `.workers/run-with-postgres.sh`.
- Expected path:
  `.workers/workloads/message-event-cancellation/message_event_cancellation_workload.py`.
- Entry points:
  - `DBOSClient.get_event`
  - `DBOSClient.get_event_async`
  - workflow `DBOS.set_event` / `DBOS.set_event_async`
  - read-only `workflow_events` and `workflow_events_map` observations

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 7130 | sync-client-delayed-event | start a workflow that sets an event after a modeled delay while sync `DBOSClient.get_event` waits with a long timeout | client returns the modeled value within the prompt polling bound, not near the full timeout; event row and handle result agree |
| case-002 | 7131 | async-client-delayed-event | same as case 001 through `DBOSClient.get_event_async` with concurrent async workflow execution | async client polls without blocking the event loop, returns promptly, and leaves no waiter entry |
| case-003 | 7132 | client-terminal-missing-event | workflow reaches terminal success without setting the requested key while client waits | client returns `None` within the terminal/miss bound instead of waiting for the full timeout, and terminal status is unchanged |
| case-004 | 7133 | client-event-update-race | workflow sets an initial value, later updates the same key, while client calls before and after the update window | first client read observes the initial modeled value promptly; later read observes the updated modeled value; no stale waiter or old value leaks across reads |

#### Invariants

- Must hold:
  - Sync and async client waits finish within the modeled prompt bound derived
    from `_event_recheck_interval()` plus scheduling slack.
  - Client waits do not depend on an in-process listener thread or on the
    outer timeout expiring.
  - Returned values match durable `workflow_events` rows and modeled event
    update order.
  - Missing-key terminal cases return `None` within the modeled bound and do
    not mutate workflow terminal status.
  - `workflow_events_map` has no modeled payload after success, terminal miss,
    timeout, or async cancellation cleanup.
- Must never happen:
  - A client `get_event` call waits near the full timeout after the value has
    been durably set.
  - A prompt delivery pass is classified without recording set time, return
    time, polling interval, and durable event row.
  - A runtime `DBOS.get_event` fallback case is used as a substitute for the
    client no-listener path.

#### Expected Signatures

- Success: all client waits return the modeled value or `None` within the
  latency bound, durable event rows agree with the result, terminal state is
  stable, and waiter maps are clean.
- Finding: delayed delivery until the full timeout, wrong/stale event value,
  async client blocking the event loop, missing-key wait hanging past terminal
  state, leaked waiter payload, or terminal row mutation.
- Setup block: executor cannot safely isolate client waiters or observe
  `workflow_events_map` without product source edits.
- Low signal: workload only repeats runtime fallback cases, uses short outer
  timeouts that cannot distinguish prompt polling from timeout expiry, or
  checks only completion without latency and durable event evidence.

## Oracle Contract

The oracle is a client-event ledger with explicit timestamps: wait registered,
event set, workflow terminal, client return, and cleanup observed. A result is
valid only when the public client result, durable event table, terminal status,
and waiter cleanup match the modeled schedule and latency bound.

## Stale Conditions

Mark stale if DBOS changes DBOSClient event waiting, `_event_recheck_interval`,
workflow event overwrite semantics, notification listener ownership, or
`workflow_events_map` cleanup behavior.

### Rung: rung-000-message-event-smoke

Evidence source: `evidence-key:frontiers/message-event-cancellation/rungs/rung-000-message-event-smoke.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-000-message-event-smoke
frontier: message-event-cancellation
status: ready
order: 0
level: baseline
workload_file: read-only:/Users/viswa/code/workers/dbos-transact-py/tests/test_dbos.py
seeds:
  - 0
updated_at: 2026-06-20T07:32:15Z
```

#### Rung 000: Message Event Smoke

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072706564103000Z.prompt.md`.
- Frontier ID: `message-event-cancellation`.
- Rung ID: `rung-000-message-event-smoke`.
- Protected product promise: DBOS messages, events, notifications, and streams remain durable and idempotent across timeout, replay, fork, listener fallback, and client-driven workflows.
- Replay command: use the product pytest command in `Execution Map`; no workload code is created for this baseline.
- Seed policy: fixed seed `0`; no generated variance.
- Invariant oracle: product communication tests must pass under a recorded Postgres setup, but this baseline does not prove the adversarial stale-waiter oracle.

##### Goal

- Run an existing product-native pytest command read-only.
- Prove DBOS dependency bootstrap, Postgres configuration, migrations, DBOS launch/destroy, and the relevant communication fixture paths execute before new workload code is created.
- Preserve: the setup boundary for messages, events, listener fallback, and streams without adding a new adversarial workload.

##### Workload File

- Expected path: `read-only:/Users/viswa/code/workers/dbos-transact-py/tests/test_dbos.py`.
- Create or reuse: reuse product tests read-only; do not create workload code for this baseline.
- Why one file is enough for this rung: this is a setup/product-harness proof. The command may include a small `tests/test_streaming.py` target for stream smoke, but no new workload file is required.
- When to create a new file instead: never for this baseline. If product-native tests cannot run, record a setup block or solve setup under the runner's allowed harness scope.

##### Workload Shape

- Type: product-native pytest setup proof.
- Entry points:
  - `DBOS.send`, `DBOS.send_bulk`, `DBOS.recv`
  - `DBOS.set_event`, `DBOS.get_event`
  - `DBOS.read_stream` through a small existing stream test when included
- Sequence:
  - Launch the product test environment against Postgres.
  - Run a narrow set of existing communication tests.
  - Record command, product commit, DB mode, dependency setup, and pass/fail output.
- Variance: none beyond product fixture configuration.

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
| --- | --- | --- | --- | --- |
| case-001 | 0 | baseline-existing-product-tests | none | product harness starts and reaches message, event, fallback, and optional stream product paths |


##### Invariants

- Must hold:
  - The command reaches at least one message idempotency path and one event/fallback path under Postgres.
  - If stream smoke is included, stream values are read in the product test's exact expected order.
  - The runner records whether Postgres or SQLite was used; a SQLite-only pass is setup evidence only, not frontier success.
- Eventually must hold:
  - The selected pytest command exits zero or the runner records a concrete setup blocker.
- Must never happen:
  - The runner implements new adversarial workload code for this rung.
  - The baseline is treated as proving the selected adversarial cancellation/timeout oracle.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_dbos.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_streaming.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/conftest.py`
- Suggested command family:
  - `cd /Users/viswa/code/workers/dbos-transact-py && pdm run pytest tests/test_dbos.py::test_send_idempotency_key tests/test_dbos.py::test_send_bulk_idempotency_key tests/test_dbos.py::test_set_get_events tests/test_dbos.py::test_notification_fallback_polling tests/test_streaming.py::test_stream_interleaved_operations`
  - If `pdm` is unavailable in the runner image, use the harness-owned Python/runtime wrapper and record the exact command.
- Setup assumptions:
  - Use Postgres for a meaningful baseline pass.
  - Product source is read-only.
  - The runner owns or safely isolates any product test databases it creates or drops.
- Per-case evidence to record:
  - command, exit code, product commit, Python version, DB mode, redacted DB URLs, selected test names, and a short pass/fail summary.
- Replay notes:
  - Record exact command and environment variables; no generated seed replay is required.

##### Expected Signatures

- Success: the narrow product command exits zero under Postgres and reaches the named communication paths.
- Finding: not applicable; this rung is a setup proof, not an adversarial finding rung.
- Setup block: dependency installation, DB connection, migrations, missing wrappers, or product fixture setup prevents execution.
- Low signal: command only runs SQLite, only imports DBOS, or skips all communication tests.
- Goal drift: runner writes a new workload or broadens into full product pytest for this baseline.

##### Stop Conditions

- Stop when: the baseline command passes with recorded setup evidence or a setup blocker is documented.
- Escalate when: the only way to run the baseline requires product repository edits, existing workload code, or unsafe shared Postgres mutation.

### Rung: rung-001-duplicate-timeout-cancel

Evidence source: `evidence-key:frontiers/message-event-cancellation/rungs/rung-001-duplicate-timeout-cancel.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-duplicate-timeout-cancel
frontier: message-event-cancellation
status: selected
order: 1
level: adversarial
workload_file: .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py
seeds:
  - 3101
  - 3103
  - 3107
updated_at: 2026-06-20T07:32:15Z
```

#### Rung 001: Duplicate Timeout Cancel

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072706564103000Z.prompt.md`.
- Frontier ID: `message-event-cancellation`.
- Rung ID: `rung-001-duplicate-timeout-cancel`.
- Protected product promise: DBOS messages/events remain durable and idempotent when duplicate sends, timeout waits, and async cancellation interleave.
- Replay command: `python .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-001 --case <case-id>`.
- Seed policy: exact seeds `3101`, `3103`, and `3107`; persist derived cancellation offsets, topic/key names, timeout values, and duplicate order.
- Invariant oracle: stale waiters are absent after cancellation, duplicate keys deliver at most once, bulk rejection is atomic, and workflow timeout replay remains stable.

##### Goal

- Build and run: one harness-local Python workload that drives DBOS async wait cancellation, duplicate message sends, bulk send rejection, and durable timeout replay against real Postgres.
- Preserve: stale waiter cleanup, idempotent delivery, no partial bulk delivery, and durable timeout results across replay.

##### Workload File

- Expected path: `.workers/workloads/message-event-cancellation/message_event_cancellation_workload.py`.
- Create or reuse: create new; do not copy existing workload implementations.
- Why one file is enough for this rung: all three cases use the same actor set, DBOS communication APIs, Postgres setup, ledger model, cancellation gates, and invariant checks. Only wait kind, duplicate path, and timeout schedule vary.
- When to create a new file instead: only if Workload Runner cannot keep async cancellation gates and synchronous timeout replay clear in one parameterized harness. Do not split cases by seed or topic.

##### Workload Shape

- Type: module/integration stateful workload.
- Entry points:
  - `DBOS.recv_async`, `DBOS.get_event_async`, `DBOS.recv`, `DBOS.get_event`
  - `DBOS.send`, `DBOS.send_bulk`, `DBOS.set_event`
  - `DBOS.start_workflow`, `SetWorkflowID`, workflow handle results
  - Read-only snapshots of `dbos._sys_db.notifications_map` and `workflow_events_map` only as diagnostic evidence for stale waiter cleanup
- Sequence:
  - Launch DBOS against isolated Postgres with unique `wio_message_` database names.
  - Build an independent ledger keyed by case, workflow ID, topic/key, idempotency key, expected timeout branch, and expected deliveries.
  - For cancellation cases, gate the setup check after waiter registration, cancel the async wait task, release setup, and require immediate cleanup evidence before reusing the same topic/key.
  - Drive duplicate or bulk send attempts from public APIs and classify each generated operation before inspecting DBOS results.
  - For timeout replay, let a workflow wait time out, rerun the same workflow ID or fork from the recorded timeout step, then write the late message/event and prove the recorded timeout result remains stable.
  - Check invariants after the cancellation phase, after send/set operations, and at terminal workflow state.
- Variance: seed controls workflow IDs, topic/key names, idempotency keys, cancellation offset, duplicate order, timeout duration, and late-send offset while preserving the same task.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | timing/order | cancelled `recv_async` removes its topic waiter before the topic is reused | cancel after `notifications_map` registration but before setup completes, then send the same idempotency key twice | cancellation reaches target window; later recv on same workflow/topic is not rejected by stale registration | map entry absent; exactly one duplicate-key message is delivered; second recv times out |
| case-002 | timing/order | cancelled `get_event_async` removes its event waiter before a setter writes the key | cancel after `workflow_events_map` registration, then set the event and perform a later get on the same workflow/key | cancellation reaches target window; later get_event sees the modeled value | map entry absent; final event value is read once and agrees with `get_all_events` |
| case-003 | retry/idempotency | recorded timeout results are stable across replay and bulk duplicate rejection has no partial delivery | run one `recv` or `get_event` timeout inside a workflow, replay it, then attempt late duplicate/bulk sends | replay returns the same timeout branch; rejected bulk duplicate does not deliver anything extra | timeout result remains `None`; delivered message/event ledger equals the model |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3101 | cancel-recv-registered-then-duplicate-send | no dependency fault; cancellation gate around recv setup | one target workflow, one topic, one idempotency key sent twice | stale notification waiter cleanup plus at-most-once message delivery |
| case-002 | 3103 | cancel-event-registered-then-set-event | no dependency fault; cancellation gate around get_event setup | one target workflow, one event key, setter workflow writes final value | stale event waiter cleanup plus eventual event read |
| case-003 | 3107 | timeout-before-late-send-and-bulk-reject | no dependency fault; late send after timeout replay | one timeout workflow plus one bulk batch with duplicate idempotency key | durable timeout replay and no partial bulk side effect |


##### Invariants

- Must hold:
  - `notifications_map.get(f"{workflow_id}::{topic}") is None` immediately after the cancelled recv case completes cleanup.
  - `workflow_events_map.get(f"{workflow_id}::{key}") is None` immediately after the cancelled get_event case completes cleanup.
  - For each modeled `(destination, topic, idempotency_key)`, delivered message count is at most one.
  - Duplicate `send` or `send_bulk` operations with the same idempotency key do not produce a second workflow body effect or second consumed message.
  - A duplicate idempotency key inside one bulk batch is rejected before transaction effects; no modeled destination receives a partial message.
  - Workflow-internal timeout results replay as the same `None` branch for the same workflow ID or configured fork replay point.
  - Workflow handle results, event values, and optional read-only status rows match the ledger.
- Eventually must hold:
  - After cancellation cleanup and a healthy Postgres window of 5 seconds, the later wait on the same topic/key either receives the modeled value or times out exactly as modeled.
- Must never happen:
  - The case passes without proving the target cancellation registration window was reached.
  - A timeout case passes only because the late send/event was never attempted.
  - A duplicate rejection is counted as success without proving no delivery side effect occurred.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_async.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_dbos.py`
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_dbos.py`
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_sys_db.py`
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_client.py`
- Suggested command family:
  - `python .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-001 --case case-001`
  - `python .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-001 --all-cases --sequential`
- Setup assumptions:
  - Use real Postgres, not SQLite, for a useful green result.
  - The workload may monkeypatch harness-local runtime methods such as `recv_check` or `get_event_check` to create gates, but must restore them and must not modify product source.
  - The workload owns or isolates its databases and records cleanup.
  - Rung 000 has passed or the runner has equivalent current setup proof.
- Per-case evidence to record:
  - seed, derived case JSON, workflow IDs, topic/key, idempotency keys, cancellation gate reached timestamp, map cleanup checks, send/bulk operation decisions, handle results, timeout/replay results, final event values, product commit, and redacted DB connection details.
- Replay notes:
  - Persist exact seed plus derived schedule (`cancel_offset_ms`, `timeout_seconds`, `late_send_offset_ms`, duplicate order, and topic/key names). Seed alone is not enough if calibration adjusts offsets.

##### Expected Signatures

- Success: all three cases reach their named target windows, stale waiter maps are empty after cancellation, duplicate sends deliver once, bulk rejection has no partial side effect, timeout replay is stable, and handle/event results match the ledger.
- Finding: stale waiter entry after cancellation, conflict or hang on later same-topic/key wait, duplicate message delivery, partial bulk delivery after rejection, timeout replay changing after late send/event, or terminal result disagreement.
- Setup block: DBOS imports/dependencies fail, Postgres isolation cannot be established, migrations cannot run, async cancellation gate cannot be reached in 4 calibration attempts for a matrix row, or product runtime cannot expose enough evidence without source changes.
- Low signal: workload only reruns existing product pytests, never hits cancellation setup windows, checks only command completion, or uses SQLite for final classification.
- Goal drift: runner adds fork, stream, Kafka, DB restart, load, or seed sweep behavior before this three-case stale-waiter/timeout oracle is proven.

##### Stop Conditions

- Stop when: all 3 matrix cases pass with artifacts, one strong invariant violation is captured, or a setup/window blocker is documented.
- Escalate when: reaching the target windows requires product source edits, existing workload code, a different oracle, or a new adversarial axis.

### Rung: rung-002-listener-fallback-fork-stream

Evidence source: `evidence-key:frontiers/message-event-cancellation/rungs/rung-002-listener-fallback-fork-stream.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-002-listener-fallback-fork-stream
frontier: message-event-cancellation
status: ready
order: 2
level: adversarial
workload_file: .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py
seeds:
  - 3111
  - 3113
  - 3117
  - 3119
  - 3121
updated_at: 2026-06-20T07:32:15Z
```

#### Rung 002: Listener Fallback Fork Stream

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072706564103000Z.prompt.md`.
- Frontier ID: `message-event-cancellation`.
- Rung ID: `rung-002-listener-fallback-fork-stream`.
- Protected product promise: DBOS communication state remains durable and idempotent across listener fallback, fork inclusion, event reads, and stream writes.
- Replay command: `python .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-002 --case <case-id>`.
- Seed policy: exact seeds `3111`, `3113`, `3117`, `3119`, and `3121`; persist derived fallback intervals, fork tree, event-step model, and stream sequences.
- Invariant oracle: fallback delivery, fork fanout, fork event state, and stream order/close semantics must match the independent ledger.

##### Goal

- Build and run: the same message-event workload file extended to listener fallback, fork inclusion, fork event reads, and stream offset conservation.
- Preserve: fallback polling delivery, fork-scoped communication state, send-to-forks idempotency, and gapless per-key stream order.

##### Workload File

- Expected path: `.workers/workloads/message-event-cancellation/message_event_cancellation_workload.py`.
- Create or reuse: reuse and extend the rung 001 file if it exists. If rung 001 was blocked before a file was created, create the same file with support for rung 002 cases.
- Why one file is enough for this rung: the actor set, DBOS app setup, ledger model, Postgres setup, and evidence format are the same as rung 001. The new cases add communication surfaces but keep the same product promise and oracle family.
- When to create a new file instead: only if stream offset generation requires a materially different subprocess/runtime harness that would make the message/event ledger ambiguous.

##### Workload Shape

- Type: module/integration stateful workload.
- Entry points:
  - `DBOS.recv`, `DBOS.get_event`, `DBOS.set_event`
  - `DBOS.send(..., send_to_forks=True)`, `DBOS.send_bulk(..., send_to_forks=True)`
  - `DBOS.fork_workflow`, workflow handle results
  - `DBOS.write_stream`, `DBOS.read_stream`, `DBOS.close_stream`
  - Listener cleanup/fallback controls on `dbos._sys_db`
- Sequence:
  - Launch DBOS against isolated Postgres with short fallback polling intervals recorded in the case artifact.
  - For fallback cases, stop the notification listener before or after a wait registers, send/set a value, and require delivery through fallback polling.
  - For fork cases, create a root workflow with known event/message checkpoints, fork at modeled steps, then drive `send_to_forks` and event reads.
  - For stream cases, run one workflow that interleaves writes across keys and optionally uses async/concurrent write tasks; close all streams and read them back through public APIs.
  - Check ledger invariants after each case phase and at terminal workflow state.
- Variance: seed controls fallback interval, send/set offset, fork step, descendant count, stream key mix, writer count, and interleaving delays.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | dependency response | recv fallback polling preserves delivery when LISTEN/NOTIFY is dead | stop listener, then send to a waiting workflow topic | result arrives within fallback bound, not only after command timeout | delivered message equals ledger and no stale active wait remains |
| case-002 | dependency response | get_event fallback polling preserves event delivery when listener is dead | stop listener, set event while getter waits | getter returns modeled value inside fallback bound | event value and `get_all_events` agree with ledger |
| case-003 | version/migration | `send_to_forks` fans out to existing descendants without duplicating root or forks | create root plus forks, send duplicate idempotency key with `send_to_forks=True` | root and each descendant receive exactly one modeled message | delivery set equals modeled root+descendants and no duplicate per idempotency key |
| case-004 | version/migration | forked event state reflects fork point and later convergence | set/update event around fork points, read from each fork and root | fork event values match modeled step boundary, later completed values converge when expected | observed event map equals fork-point model |
| case-005 | scale/concurrency pressure | stream offsets remain gapless under interleaved writes and close | interleave writes across multiple keys and writer tasks before close | each stream read terminates and yields exact modeled sequence | no offset gaps, duplicates, wrong order, or post-close values |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3111 | fallback-recv-listener-stopped-before-send | listener stopped; fallback interval 100ms | one receiver workflow, one topic, one sender workflow | recv fallback delivery and cleanup |
| case-002 | 3113 | fallback-get-event-listener-stopped-before-set | listener stopped; fallback interval 100ms | one setter workflow, one getter workflow, one event key | get_event fallback delivery and final event state |
| case-003 | 3117 | fork-fanout-duplicate-key | no dependency fault; fork before fanout send | root plus 2 descendants, one idempotency key sent twice | send-to-forks fanout and idempotency per destination |
| case-004 | 3119 | fork-event-step-boundaries | no dependency fault; fork at steps 1, 2, 3 | one event key updated by workflow and step | fork event state at fork point and convergence |
| case-005 | 3121 | interleaved-stream-offsets | no dependency fault; optional async write interleaving | 3 stream keys, 2 writers, 5 values per hot key | gapless ordered stream reads and close semantics |


##### Invariants

- Must hold:
  - Listener fallback cases must record listener stopped before the relevant send/set operation.
  - Fallback recv and get_event results arrive within the modeled fallback bound and match the ledger.
  - `send_to_forks` delivery set equals the root plus descendants that existed at send time, with one delivery per `(destination, topic, idempotency_key)`.
  - Fork event reads match the model for each fork step; final root event value agrees with `get_all_events`.
  - Each stream key's read values exactly equal the generated sequence and terminate only after modeled close.
  - Handle results, event values, stream values, and optional read-only status snapshots agree with the independent ledger.
- Eventually must hold:
  - After listener fallback or stream close, all waits/readers finish inside bounded time or the case records a liveness failure.
- Must never happen:
  - A fallback case passes because the listener was still running.
  - A fork fanout case treats missing descendants as success.
  - A stream case checks only length while ignoring order, duplicates, and close.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_dbos.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_workflow_management.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_streaming.py`
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_sys_db.py`
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_workflow_commands.py`
- Suggested command family:
  - `python .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-002 --case case-001`
  - `python .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-002 --all-cases --sequential`
- Setup assumptions:
  - Rung 001 passed or its setup artifacts are reusable.
  - Use Postgres and short, explicit fallback polling intervals.
  - Listener stop/cleanup is confined to this workload's DBOS instance.
- Per-case evidence to record:
  - seed, derived schedule, listener state, fallback interval, workflow/fork IDs, modeled delivery set, idempotency keys, event snapshots, stream generated sequences, read sequences, handle results, product commit, and redacted DB details.
- Replay notes:
  - Persist exact fork tree, event-step model, and stream generated sequences because these are more durable than seed alone.

##### Expected Signatures

- Success: all five cases reach target windows, fallback delivery works while listener is stopped, fork/fanout/event state matches the model, streams are gapless and ordered, and terminal handle results agree.
- Finding: fallback timeout, missing/duplicate fork delivery, wrong fork event value, stream offset gap/duplicate/order inversion, post-close value, or terminal result disagreement.
- Setup block: listener cannot be stopped safely, stream/fork APIs cannot run under the prepared Postgres setup, or a target window cannot be reached within 4 calibration attempts.
- Low signal: listener remains live, fork descendants are not actually created, stream checks only count, or the workload wraps existing tests without the cross-case ledger.
- Goal drift: runner adds DB restart, Kafka, or broad seed sweep before these five cases are proven.

##### Stop Conditions

- Stop when: all 5 matrix cases pass with artifacts, one strong invariant violation is captured, or a setup/window blocker is documented.
- Escalate when: the runner needs product source edits, existing workload code, or a different oracle to reach fallback/fork/stream windows.

### Rung: rung-003-recovery-replay-cancellation

Evidence source: `evidence-key:frontiers/message-event-cancellation/rungs/rung-003-recovery-replay-cancellation.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-003-recovery-replay-cancellation
frontier: message-event-cancellation
status: deferred
order: 3
level: failure
workload_file: .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py
seeds:
  - 3131
  - 3133
  - 3137
  - 3139
  - 3141
  - 3143
  - 3147
  - 3149
updated_at: 2026-06-20T07:32:15Z
```

#### Rung 003: Recovery Replay Cancellation

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072706564103000Z.prompt.md`.
- Frontier ID: `message-event-cancellation`.
- Rung ID: `rung-003-recovery-replay-cancellation`.
- Protected product promise: DBOS communication results survive executor relaunch/replay without stale waiters, duplicate effects, or changed timeout branches.
- Replay command: `python .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-003 --case <case-id>`.
- Seed policy: exact seeds `3131`, `3133`, `3137`, `3139`, `3141`, `3143`, `3147`, and `3149`; persist derived relaunch boundary and communication ledger.
- Invariant oracle: post-relaunch handles/status rows, wait cleanup, deliveries, events, and streams must agree with the pre-relaunch independent model.

##### Goal

- Build and run: the same workload file extended to executor relaunch/replay while waits, events, fork fanout, and stream writes are in flight.
- Preserve: the same message/event/stream ledger under partial application interruption, repeated cancellation windows, and healthy recovery.

##### Workload File

- Expected path: `.workers/workloads/message-event-cancellation/message_event_cancellation_workload.py`.
- Create or reuse: reuse and extend the file from rungs 001 and 002.
- Why one file is enough for this rung: the product promise and oracle remain the same; this rung adds process/replay and repeated fault dimensions to the same state model.
- When to create a new file instead: only if executor relaunch requires a separate subprocess driver that cannot share a clear case model with the existing workload. Keep artifacts and case schemas compatible.

##### Workload Shape

- Type: module/integration safety-liveness simulation.
- Entry points:
  - Rung 001 and 002 communication APIs
  - `DBOS.destroy`, relaunch with the same Postgres databases, workflow handle/result retrieval
  - `DBOS._recover_pending_workflows` or public recovery/client paths when the runner can keep the boundary clear
  - Read-only workflow status and operation-result inspection for modeled workflow IDs
- Sequence:
  - Launch a DBOS app against isolated Postgres and start modeled workflows with wait, event, fork, and stream phases.
  - Drive a selected cancellation/fallback/stream target window.
  - Interrupt the executor at the case's point, relaunch against the same durable state, and recover or retrieve workflow results.
  - Restore healthy execution, complete any harness gates, and assert ledger/SQL/handle agreement.
  - Repeat cancellation or fallback windows only within the case's bounded schedule.
- Variance: seed controls interruption point, replay path, cancellation count, fallback interval, stream write count, and fork tree size while preserving the same model.

##### Attack Plan

| Case | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| case-001 | partial failure/recovery | a cancelled recv waiter is not revived as stale state after relaunch | cancel recv at setup window, destroy/relaunch, then reuse same topic | later wait succeeds or times out as modeled, with no conflict | no stale registration; terminal result matches ledger |
| case-002 | partial failure/recovery | a cancelled get_event waiter is not revived as stale state after relaunch | cancel get_event at setup window, destroy/relaunch, then set/read event | event read succeeds once and final event state matches model | no stale registration; event ledger matches |
| case-003 | retry/idempotency | timeout replay survives executor relaunch and late send | timeout in workflow, relaunch, recover/retrieve, then send late message | timeout result remains stable after relaunch | durable timeout branch equals model |
| case-004 | dependency response | listener fallback plus relaunch does not lose a message | stop listener, send while waiter is pending, relaunch before result retrieval | recovered result contains modeled message | fallback delivery and terminal status agree |
| case-005 | version/migration | fork fanout survives relaunch between fork creation and send | create root/forks, relaunch, send to forks with idempotency key | root/descendant delivery set is exact | fanout ledger equals observed deliveries |
| case-006 | scale/concurrency pressure | stream writes are replayed without duplicate offsets after relaunch | write some stream values, relaunch before close, finish stream | final stream sequence is exact and closed | gapless order with no duplicate replayed values |
| case-007 | timing/order | repeated cancellation does not accumulate waiter-map counts | perform two cancel/reuse cycles on distinct topics/keys in one DBOS app | each cycle cleans up before reuse | no stale entries and later waits behave as modeled |
| case-008 | retry/idempotency | bulk duplicate rejection remains atomic across relaunch | prepare receiver, relaunch, attempt rejected duplicate-key bulk batch, then valid send | rejected batch has no partial delivery, valid send delivers once | bulk atomicity and later liveness both hold |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3131 | cancel-recv-then-relaunch-before-reuse | executor relaunch, no DB restart | one workflow, one topic, one duplicate key | stale notification cleanup across relaunch |
| case-002 | 3133 | cancel-event-then-relaunch-before-set | executor relaunch, no DB restart | one target workflow, one event key | stale event cleanup across relaunch |
| case-003 | 3137 | timeout-replay-after-relaunch | executor relaunch, no DB restart | one timeout workflow, late sender | durable timeout replay |
| case-004 | 3139 | fallback-waiter-relaunch | listener stopped plus executor relaunch | one waiter, one sender | fallback liveness after relaunch |
| case-005 | 3141 | fork-tree-relaunch-before-fanout | executor relaunch, no DB restart | root plus 3 descendants | fork fanout after recovered durable state |
| case-006 | 3143 | stream-relaunch-before-close | executor relaunch, no DB restart | 2 stream keys, 6 values per hot key | stream replay without duplicate offsets |
| case-007 | 3147 | repeated-cancel-reuse | no dependency fault; two cancellation cycles | two topics and two event keys | cleanup count does not accumulate |
| case-008 | 3149 | relaunch-then-bulk-reject-valid-send | executor relaunch, no DB restart | one receiver, one rejected bulk batch, one valid batch | bulk atomicity plus post-failure liveness |


##### Invariants

- Must hold:
  - Every modeled workflow ID is terminal, cancelled by the model, or explicitly classified as a bounded liveness failure after recovery.
  - No stale notification or event waiter registration blocks reuse after relaunch.
  - Timeout branches, delivered messages, event values, fork delivery sets, and stream sequences match the pre-relaunch ledger.
  - Completed stream writes and message deliveries are not duplicated after replay.
  - Rejected bulk batches have no partial delivery before or after relaunch.
  - Handle results and read-only workflow status rows agree for modeled workflows.
- Eventually must hold:
  - After relaunch and a healthy Postgres window of 15 seconds, all modeled live workflows make terminal progress or a failure is recorded.
- Must never happen:
  - A case passes because relaunch did not actually happen.
  - A case treats missing recovery/result retrieval as success without SQL or handle evidence.
  - The workload changes the product promise from communication durability to generic recovery.

##### Execution Map

- Suggested files to inspect:
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_dbos.py`
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_recovery.py`
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_sys_db.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_streaming.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_failures.py`
- Suggested command family:
  - `python .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-003 --case case-001`
  - `python .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-003 --all-cases --sequential`
- Setup assumptions:
  - Rungs 001 and 002 have passed or produced reusable setup artifacts.
  - The runner can relaunch DBOS against the same isolated Postgres state without product source edits.
  - Do not restart Postgres in this rung; that is a different frontier axis unless later strategy changes it.
- Per-case evidence to record:
  - seed, derived schedule, relaunch boundary, workflow IDs, pre/post status rows, waiter cleanup checks, delivery/event/stream ledger, handle results, product commit, and redacted DB details.
- Replay notes:
  - Persist derived relaunch script arguments and any subprocess boundary metadata because process timing is not fully captured by seed.

##### Expected Signatures

- Success: all eight cases reach their relaunch target, communication invariants survive recovery, no stale waiters or duplicate effects appear, and live workflows terminally complete within bounds.
- Finding: stale waiter after relaunch, changed timeout result, duplicate message/event/stream effect, missing fork fanout, stream offset gap, partial bulk delivery, stranded `PENDING` row for a modeled live workflow, or handle/status disagreement.
- Setup block: relaunch cannot be performed under allowed scope, DB isolation cannot be proven, subprocess boundary loses required logs, or target windows cannot be reached in 4 calibration attempts.
- Low signal: relaunch is simulated only by rerunning functions in the same DBOS instance, or the case ignores durable state and only checks command completion.
- Goal drift: runner adds DB restart, Kafka, load, or a general recovery suite instead of preserving this communication ledger.

##### Stop Conditions

- Stop when: all 8 matrix cases pass with artifacts, one strong invariant violation is captured, or a setup/window blocker is documented.
- Escalate when: recovery/relaunch requires product source edits, unsafe database mutation, existing workload code, or a different oracle.

### Rung: rung-004-bounded-seed-sweep

Evidence source: `evidence-key:frontiers/message-event-cancellation/rungs/rung-004-bounded-seed-sweep.md`

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-004-bounded-seed-sweep
frontier: message-event-cancellation
status: deferred
order: 4
level: sweep
workload_file: .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py
seeds:
  - 3151
  - 3152
  - 3153
  - 3154
  - 3155
  - 3156
  - 3157
  - 3158
  - 3159
  - 3160
  - 3161
  - 3162
  - 3163
  - 3164
  - 3165
  - 3166
  - 3167
  - 3168
  - 3169
  - 3170
  - 3171
  - 3172
  - 3173
  - 3174
updated_at: 2026-06-20T07:32:15Z
```

#### Rung 004: Bounded Seed Sweep

##### Source Contract

- Evidence key: `evidence-key:events/frontier_designer-20260620T072706564103000Z.prompt.md`.
- Frontier ID: `message-event-cancellation`.
- Rung ID: `rung-004-bounded-seed-sweep`.
- Protected product promise: the same DBOS message/event/stream durability and idempotency promises hold across a bounded replayable matrix.
- Replay command: `python .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-004 --case <case-id>`.
- Seed policy: exact seeds `3151..3174`; every case must persist full derived command sequence and timing offsets.
- Invariant oracle: all earlier rung invariants remain active; no case may pass by skipping its declared target window.

##### Goal

- Build and run: a bounded sequential sweep over the already-proven message-event workload model.
- Preserve: the same message/event/stream promise and oracle while broadening cancellation offsets, fallback polling intervals, fork shape, and stream interleavings.

##### Workload File

- Expected path: `.workers/workloads/message-event-cancellation/message_event_cancellation_workload.py`.
- Create or reuse: reuse the file from earlier rungs. This rung must not be the first workload implementation.
- Why one file is enough for this rung: this is a breadth expansion over the same actor, product goal, adversarial axes, and invariant oracle already proven by smaller rungs.
- When to create a new file instead: only if earlier findings are minimized into a separate deterministic regression workload; otherwise keep the sweep parameterized in the same file.

##### Workload Shape

- Type: bounded stateful sweep.
- Entry points:
  - All APIs exercised by rungs 001 through 003, excluding Kafka and Postgres restart.
- Sequence:
  - Execute 24 sequential matrix cases, each generated from a fixed seed and declared case family.
  - Record derived case JSON before running the DBOS actions.
  - Check invariants after every meaningful transition and at terminal state.
  - Stop on the first strong invariant violation unless the runner is explicitly collecting multiple independent signatures for the same already-captured failure.
- Variance: fixed seed ladder with bounded branch choices for cancellation offset, fallback interval, duplicate order, fork count, stream key mix, and optional relaunch boundary.

##### Attack Plan

| Case Family | Axis | Assumption Attacked | Perturbation | Expected Observation | Oracle |
| --- | --- | --- | --- | --- | --- |
| cancel-reuse | timing/order | stale waiter cleanup is independent of cancellation offset | sweep recv/get_event cancellation offsets before reuse | every target window either reached or classified bounded-blocked | map cleanup and later wait result match ledger |
| duplicate-timeout | duplicate/replay | idempotency and timeout replay compose across duplicate orders | sweep duplicate order and late-send offsets after timeout | timeout branch stable and one delivery per key | timeout/delivery ledger matches |
| fallback | dependency response | fallback polling remains live across polling intervals and send/set offsets | sweep fallback intervals and listener stop points | waits finish within modeled fallback bound | fallback result equals ledger |
| fork-stream | version/migration and scale/concurrency pressure | fork inclusion and stream offsets hold under broader generated shapes | sweep fork count, fork step, stream key mix, writer count | fanout set and stream sequences are exact | fork and stream ledgers match observed state |
| replay | partial failure/recovery | relaunch preserves communication state across selected windows | sweep a small subset of proven relaunch boundaries | terminal state and communication ledger survive relaunch | handle/status/ledger agreement |

##### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Data Shape | Expected Focus |
| --- | --- | --- | --- | --- | --- |
| case-001 | 3151 | cancel-reuse-recv-offset-0ms | no dependency fault | one workflow, one topic | recv cleanup at immediate cancellation |
| case-002 | 3152 | cancel-reuse-recv-offset-25ms | no dependency fault | one workflow, one topic | recv cleanup after short setup delay |
| case-003 | 3153 | cancel-reuse-recv-offset-100ms | no dependency fault | one workflow, one topic | recv cleanup after longer setup delay |
| case-004 | 3154 | cancel-reuse-event-offset-0ms | no dependency fault | one workflow, one event key | event cleanup at immediate cancellation |
| case-005 | 3155 | cancel-reuse-event-offset-25ms | no dependency fault | one workflow, one event key | event cleanup after short setup delay |
| case-006 | 3156 | cancel-reuse-event-offset-100ms | no dependency fault | one workflow, one event key | event cleanup after longer setup delay |
| case-007 | 3157 | duplicate-send-before-timeout | no dependency fault | one receiver, one duplicate key | duplicate send order before timeout |
| case-008 | 3158 | duplicate-send-after-timeout | no dependency fault | one timeout workflow, late duplicate key | timeout stability after duplicate send |
| case-009 | 3159 | bulk-duplicate-key-reject | no dependency fault | one rejected bulk batch, one later valid batch | bulk atomicity plus liveness |
| case-010 | 3160 | fallback-recv-interval-50ms | listener stopped | one receiver, one topic | fast fallback recv |
| case-011 | 3161 | fallback-recv-interval-250ms | listener stopped | one receiver, one topic | slower fallback recv bound |
| case-012 | 3162 | fallback-event-interval-50ms | listener stopped | one getter, one setter | fast fallback get_event |
| case-013 | 3163 | fallback-event-interval-250ms | listener stopped | one getter, one setter | slower fallback get_event bound |
| case-014 | 3164 | fork-fanout-two-descendants | no dependency fault | root plus 2 descendants | fanout set size 3 |
| case-015 | 3165 | fork-fanout-four-descendants | no dependency fault | root plus 4 descendants | fanout set size 5 |
| case-016 | 3166 | fork-event-early-step | no dependency fault | event fork at early step | fork event boundary |
| case-017 | 3167 | fork-event-late-step | no dependency fault | event fork at late step | fork event boundary |
| case-018 | 3168 | stream-two-keys-two-writers | no dependency fault | 2 keys, 2 writers | interleaved stream offsets |
| case-019 | 3169 | stream-three-keys-three-writers | no dependency fault | 3 keys, 3 writers | broader stream offsets |
| case-020 | 3170 | stream-hot-key-five-writes | no dependency fault | hot key with 5 values | same-key ordering |
| case-021 | 3171 | replay-cancel-recv | executor relaunch, no DB restart | one cancelled recv then relaunch | cleanup after relaunch |
| case-022 | 3172 | replay-timeout-late-send | executor relaunch, no DB restart | timeout workflow and late sender | timeout stability after relaunch |
| case-023 | 3173 | replay-stream-before-close | executor relaunch, no DB restart | stream writes before close | stream replay no duplicates |
| case-024 | 3174 | mixed-small-session | listener stopped plus duplicate send, no relaunch | one receiver, one event, one stream key | cross-surface ledger conservation |


##### Invariants

- Must hold:
  - Every case maps to one declared case family and one adversarial axis before execution begins.
  - Every generated operation is classified in the ledger before DBOS results are inspected.
  - Cancellation cleanup, idempotency, timeout replay, fallback delivery, fork inclusion, stream order, and terminal result invariants from earlier rungs hold for their case families.
  - Derived case JSON, seed, and schedule are persisted for every case.
  - No matrix case can pass by skipping its target window; skipped windows must be classified as `blocked: target-window-not-reached`.
- Eventually must hold:
  - For live cases with no intentional terminal cancellation, modeled workflows reach terminal state inside 15 seconds after healthy conditions are restored.
- Must never happen:
  - The sweep broadens into unbounded load, Kafka, Postgres restart, or new product promises.
  - The runner treats more seeds as useful if earlier rungs have not proven the oracle.

##### Execution Map

- Suggested files to inspect:
  - The workload file and run summaries from rungs 001 through 003.
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_sys_db.py`
  - `/Users/viswa/code/workers/dbos-transact-py/dbos/_dbos.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_async.py`
  - `/Users/viswa/code/workers/dbos-transact-py/tests/test_streaming.py`
- Suggested command family:
  - `python .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-004 --all-cases --sequential`
  - For retry after a setup-only failure: `python .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-004 --case case-<nnn>`
- Setup assumptions:
  - Rungs 001 and 002 passed, and rung 003 either passed or its replay cases are explicitly excluded from the sweep by frontier revision.
  - Use Postgres with isolated databases.
  - Sequential execution is required for v0 to keep waiter-map and listener state diagnosable.
- Per-case evidence to record:
  - seed, derived case JSON, case family, adversarial axis, target-window evidence, operation ledger, workflow/fork IDs, event/stream values, handle/status results, product commit, and redacted DB details.
- Replay notes:
  - Persist one JSON artifact per case containing the full generated command sequence and timing offsets. Seed alone is not sufficient for failure minimization.

##### Expected Signatures

- Success: all 24 cases either pass their family invariants with target-window evidence or are explicitly blocked for setup/window reachability without weakening the oracle.
- Finding: any invariant violation from earlier rungs, especially a stale waiter after cancellation, duplicate delivery, timeout instability, fallback liveness failure, fork mismatch, stream offset bug, or relaunch disagreement.
- Setup block: product dependencies, Postgres isolation, listener control, subprocess relaunch, or target-window calibration blocks multiple case families.
- Low signal: sweep runs before smaller rungs prove the harness, lacks derived replay artifacts, or reports only command completion.
- Goal drift: runner changes the frontier into broad load, Kafka, Postgres restart, or unrelated DBOS recovery testing.

##### Stop Conditions

- Stop when: all 24 matrix cases pass with artifacts, one strong finding is captured and replay JSON is saved, or a setup/window blocker is documented.
- Escalate when: more seeds are needed before the named target windows are reachable, replay cases require source edits, or a finding needs minimization into a new rung.
