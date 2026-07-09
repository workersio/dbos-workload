# E-008 Cloud Run Summary

## Short Version

E-008 is an environment-sensitive DBOS liveness finding. In WIO cloud, an
active hot-key async debouncer row stayed pending while unrelated trivial
workflows on a separate DB-backed queue only completed through their handles
after roughly the hot debounce window. The intended bound was `2.5s`; the
focused cloud confirmation observed `10.54s`.

This is not currently a standalone local repro. macOS and plain EC2 runs
completed the same focused case in about `1.01s`.

## Cloud Evidence

### Focused Confirmation

- Run ID: `01KVVJTZVY7JV3JJ231C77DT2E`
- Batch ID: `nd7d76xj9cf83hwp65xntydfrd898e5c`
- Project: `DBOS Workload Fresh`
- Branch: `main`
- Workload commit: `9b90b68086da039d3ef8362edde11eca1677c179`
- Target DBOS ref: `3df88c4bcc3a2b73d91b765b459f4c7beb6a690c`
- Workload file:
  `.workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py`
- Rung: `rung-006-async-debouncer-worker-starvation`
- Case: `case-002`
- State: `failed`
- Exit code: `1`
- `hasInvariantViolation`: `true`
- Failed invariant: `unrelated_workflows_complete_inside_pressure_window`

Command:

```bash
.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-006-async-debouncer-worker-starvation --case case-002
```

### Full Matrix

- Run ID: `01KVVJPJ6YKK2Z2ZN3FCCPNQS2`
- Batch ID: `nd74vkcz2kadh50hves1dkrmpx898e24`
- Workload commit: `9b90b68086da039d3ef8362edde11eca1677c179`
- State: `failed`
- Exit code: `1`
- Same failing invariant in `case-002`

Command:

```bash
.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-006-async-debouncer-worker-starvation --all-cases --sequential
```

## What Happened

1. The workload submitted hot-key debounced workflows.
2. It verified that a hot-key `_dbos_debouncer_workflow` row was active on
   `_dbos_internal_queue` with status `PENDING`.
3. Immediately before waiting on unrelated work, it verified that the hot-key
   debouncer row was still active.
4. It submitted three trivial workflows to unrelated queue
   `wio_debounce_unrelated_queue_3471`.
5. The trivial workflow bodies were fast, each taking about
   `0.00013s` to `0.00016s`.
6. The handles completed after `10.537618736s` in the focused run, exceeding
   the `2.5s` liveness bound.
7. The full matrix reproduced the same signature with `8.191957509s` handle
   completion.

## Failed Invariant

```text
unrelated_workflows_complete_inside_pressure_window
```

Expected:

```text
Unrelated trivial queued workflows complete inside 2.5s while hot debouncer rows are active.
```

Observed in focused run:

```text
elapsed_sec = 10.537618736
bound_sec = 2.5
unrelated_kind = queued
errors = []
workflow_count = 3
```

## Interpretation

The product-facing symptom is that a hot-key debouncer pressure window can
coexist with unrelated DB-backed queue work whose bodies are trivial, but whose
handles only become observable after several seconds. That violates the
intended isolation property: debouncer delay for one key should not starve
unrelated queue work.

The failure reproduced in both the full matrix and a focused cloud case-only
replay, so it is not just sequential case history.

## Caveat

Plain local and EC2 executions have not reproduced the failure so far:

- macOS focused `case-002`: passed, unrelated handles completed in about `1.01s`.
- macOS all-cases rung replay: passed.
- Plain x86_64 Linux EC2 focused `case-002`: passed, about `1.01s`.
- Plain x86_64 Linux EC2 with Python `3.14.0b2`: passed.
- Plain x86_64 Linux EC2 with CPU contention and one CPU: passed, about `1.02s`.
- Plain x86_64 Linux EC2 all-cases replay with Python `3.14.0b2`: passed.

Treat this as WIO-cloud-confirmed, environment-sensitive evidence. Do not file
it as a standalone local reproduction until there is a smaller DBOS script or a
matched deterministic-worker repro.

## Local References

- Canonical run record: `.workers/runs/E-008.md`
- Work item: `.workers/work-items/e-008.md`
- Issue draft: `.workers/issues/E-008-debouncer-starves-unrelated-queue.md`
- Workload: `.workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py`
