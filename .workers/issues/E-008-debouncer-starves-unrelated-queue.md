# [debouncer] Active debounce key can still delay unrelated queued workflows

After the async debouncer change, a hot debounce key can still delay unrelated
DB-backed queued workflows. In the repro, the unrelated workflow bodies are
trivial, but their handles complete only after roughly 8-10 seconds while a
debouncer row is active.

The debounced workflow itself is expected to wait for the debounce window. The
observed result is that unrelated queue work waits too.

## Environment observed

- DBOS source: checkout at `3df88c4bcc3a2b73d91b765b459f4c7beb6a690c`
- Runtime observed: DBOS `2.24.0-12-g3df88c4`, CPython `3.14.6`
- Backend: Postgres-backed DBOS queue

## Minimal repro

No standalone local repro is available yet. Do not file this as an upstream
issue until the failure can be reproduced in a standalone script or a normal
DBOS checkout.

The repro creates an active hot-key debouncer row, then enqueues three trivial
workflows on an unrelated DB-backed queue and waits for their handles.

Observed result:

```text
INVARIANT unrelated_workflows_complete_inside_pressure_window ... FAIL
```

The unrelated workflow bodies themselves take around `0.00013s` to `0.00016s`,
but the handles complete after roughly `8s` to `10s`, outside the `2.5s`
pressure-window bound.

<details>
<summary>Local verification note</summary>

Local verification did not reproduce this result on my macOS host. The focused
case and the full rung both passed against the source checkout above.

Local result:

```text
INVARIANT unrelated_workflows_complete_inside_pressure_window ... PASS
elapsed_sec = 1.01
```

The same all-cases local run also passed. Do not file this one with a
local-reproduction claim without a smaller standalone repro or an environment
that preserves the slower queued-handle timing.

Plain x86_64 Linux EC2 also did not reproduce the failure. Focused `case-002`
passed against the same DBOS target checkout with both the default Python 3.12
environment and a Python 3.14.0b2 environment, including an attempted
single-CPU/CPU-contention run. The EC2 all-cases rung replay also passed. In
these runs the unrelated queued handles completed in roughly `1.01s` to
`1.02s`, not the `8s` to `10s` observed in WIO cloud.

</details>

## Expected behavior

An active debounce window for one key should not consume scheduling capacity
needed for unrelated queued workflows to start and complete promptly.

## Actual behavior

While the hot-key debouncer row is active, unrelated trivial queued workflows
complete through handles only after several seconds.

## Relevant implementation path

Issue `#724` and PR `#739` addressed the original synchronous debouncer
thread-consumption problem. This repro is a follow-up liveness case: even after
that change, the interaction between the debouncer internal workflow, queue
polling, and executor/event-loop scheduling can still delay unrelated DB-backed
queue work under hot debounce pressure.

Isolation property: debouncer delay for one key is isolated from unrelated queue
work.
