# Run evidence — scheduled-queue-controls-compose

Exploration: `scheduled-queue-controls-compose`
Promise: `scheduled-work-fires-predictably` (area: scheduling)
Rung: `rung-005-scheduled-queue-controls-compose`

## Verdict: GREEN (survived — weak evidence)

Scheduled workflows admitted through queues with concurrency + rate controls;
both the timing contract and the queue bounds held. 13/13 invariants PASS,
`hasInvariantViolation=False`, exit 0.

## Run

| batch (exploration id) | run id | image | state | invariants |
|---|---|---|---|---|
| nd774pjh6dcns7azhr9btsm8k98a7mp3 | 01KX3YE46SXVJ9VH1CG96QCG7S | d255d25 | succeeded | 13/13 PASS |

Command: `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/scheduler-debouncer-timing/scheduler_debouncer_timing_workload.py --rung rung-005-scheduled-queue-controls-compose --all-cases --sequential` (depth 1, no faults).

## Interpretation

A green baseline at depth 1 means the modeled compose cases passed, not that
the surface is exhausted. Deeper attack (fault-timing on the queue admission
window, higher depth interleavings) remains producible work for this promise.
