# [candidate] DBOSClient terminal missing event waits near full timeout before returning None

Status: `not-ready`

Disposition: candidate draft and possible contract question; clarify intended
`DBOSClient.get_event` terminal-miss semantics and reduce to a local repro
before filing.

## Summary

The `E-025` workload found that `DBOSClient.get_event` returns event values
promptly for no-listener sync and async delayed-event cases, and preserves
updated event values. However, when the target workflow completes successfully
without setting the requested key, the client waits close to the full timeout
before returning `None`.

The product question is whether a client wait should conclude promptly once the
target workflow is terminal and the event key is absent, or whether waiting
until timeout is intentional API behavior.

## Environment

- DBOS source: `3df88c4bcc3a2b73d91b765b459f4c7beb6a690c`
- Evidence: `.workers/runs/E-025.md`
- Work item: `.workers/work-items/e-025.md`
- Workload: `.workers/workloads/message-event-cancellation/message_event_cancellation_workload.py`
- Related upstream PR: `dbos-inc/dbos-transact-py#713`, merged

## Reproduction Story

Current local harness shape:

```bash
.workers/run-with-postgres.sh .workers/python-runtime.sh \
  .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py \
  --rung rung-006-client-get-event-prompt-polling \
  --case case-003 \
  --seed 7132
```

The upstream-ready version should be a standalone DBOS script or test that:

1. starts a workflow that reaches `SUCCESS` without calling `set_event` for a
   selected key;
2. calls `DBOSClient.get_event(workflow_id, missing_key, timeout_seconds=12)`
   from a client with no notification listener;
3. records when the workflow becomes terminal;
4. records when `get_event` returns `None`;
5. asserts the result is either promptly returned after terminal state, or
   documents that full-timeout waiting is the intended contract.

## Expected Behavior

If terminal-miss promptness is part of the client contract, the client should
return stable `None` shortly after it observes the workflow is terminal and the
requested event key is absent.

If the intended contract is timeout-only, DBOS docs should state that
`DBOSClient.get_event` may wait until timeout even when the target workflow has
already completed without setting the key.

## Actual Behavior

The focused WIO run reported:

- invariant failure: `client_terminal_miss_within_prompt_bound`;
- client had no listener: `listener_running=false`,
  `use_listen_notify=false`;
- actual recheck interval was `0.1s`;
- target workflow reached durable `SUCCESS` without the requested key;
- final event table was empty;
- client returned `None` after `12.810s` against a `12.0s` timeout;
- the client returned roughly eight seconds after the workflow was already
  terminal.

## Impact

If prompt terminal misses are intended, client callers can spend nearly their
full timeout waiting for an event that DBOS already knows will never be set by
the completed workflow. That affects liveness and user-facing latency for
client-side event waits.

## Evidence

- Failing case run: `01KVYSJEJGE1QMEP81B285D3SM`
- Passing delayed-event sync run: `01KVYSJFWR0NBGE3B4GP2D1BW0`
- Passing delayed-event async run: `01KVYSJFGY5S75C4DT5Z1ZQVS5`
- Passing event-update run: `01KVYSJF2AYVWWXJSA1E8319H6`
- Run record: `.workers/runs/E-025.md`
- Work item: `.workers/work-items/e-025.md`

## Controls And Non-Claims

- Sync delayed event delivery passed under the cloud prompt bound.
- Async delayed event delivery passed and did not block the event loop.
- Event update reads preserved the current value.
- Initial all-cases WIO server errors and earlier calibration runs were
  discarded from final evidence.
- This draft does not claim timeout-only behavior is a bug until the DBOS
  client contract is clarified.

## Upstream Duplicate/Fix Check

Checked on 2026-06-25:

- PR `#713` is merged and is related to timing/polling behavior.
- No existing upstream issue was found for terminal missing event waits near
  full timeout before returning `None`.
