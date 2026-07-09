---
key: workflow-invoke-outcome-pipeline
title: Workflow invoke outcome pipeline
description: "Every workflow invocation flows through one restructured outcome pipeline — concurrent same-id invokes, completed-result replay, and recorded-error re-raise stay exactly-once and consistent across the sync and async paths."
order: 120
---

# Area: workflow-invoke-outcome-pipeline

## Current State

New area from the diff-directed scan of #763 (`a43fead`, "Improve Behavior
Consistency"). No prior harvest. Highest-blast-radius change of the three new
commits: every workflow invocation flows through the restructured
`_core.workflow_wrapper`.

Evidence:
- `dbos/_core.py workflow_wrapper` — restructured from `.wrap(init_wf)` to
  `.wrap(get_wf_invoke).intercept(check_and_init)`. `check_and_init` populates a
  shared-closure `init_status` only on the `should_execute` branch and returns
  `NoResult()` otherwise; `get_wf_invoke` reads `init_status["status"]` and must
  not run when the recorded outcome is returned.
- `dbos/_outcome.py Pending._intercept` / `_wrap` — async path runs
  `check_and_init` and `get_wf_invoke` on separate `asyncio.to_thread` threads.

## Product Promise

Every workflow invocation runs its body exactly once or returns the recorded
result/error without re-running — including concurrent same-id invocations and
re-invocation of an already-completed async workflow (result replay and error
re-raise).

## What Not To Repeat

- E-024 (decorator-composition-fidelity) is inner-hook duplication under
  decorator wrapping — do not re-derive it. This area targets the raw invoke
  pipeline short-circuit itself.
- Scout traced the SYNC path as behavior-preserving; do not spend budget
  re-asserting single-threaded sync invocation. The async cross-thread and
  concurrent-same-id matrix is the live falsification surface.

## Deeper / Broader Search

| Direction | Why |
|---|---|
| recorded child-workflow-id short-circuit | `check_and_init` handles a recorded child id path; a distinct replay branch. |
| error re-raise fidelity under portable serialization | a recorded error deserialized to `PortableWorkflowError` on replay — does the original type survive the new pipeline? |
| intercept/wrap ordering under step vs workflow | the combinator reorder could interact with step recording. |

## Rung Ladder (see work-item e-030)

- rung-001-concurrent-and-replay-invoke: concurrent same-id + completed-async
  result replay + completed-async error re-raise; exactly-once body oracle.

## Stale Conditions

Mark stale if DBOS changes `workflow_wrapper`, the Outcome `intercept`/`wrap`
combinators, `check_and_init`/`get_wf_invoke`, or target ref advances past #763
with an invoke-pipeline rewrite.
