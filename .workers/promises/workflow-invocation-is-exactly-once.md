---
key: workflow-invocation-is-exactly-once
area: workflow-invoke-outcome-pipeline
title: Workflow invocation runs the body once or returns the recorded outcome
claim: >-
  Every workflow invocation either runs its body exactly once or returns the
  already-recorded result/error without re-running — including concurrent
  invocations of the same workflow id and re-invocation of an already-completed
  async workflow. #763 restructured the invoke pipeline to
  .wrap(get_wf_invoke).intercept(check_and_init); the recorded-result short
  circuit and the recorded-error re-raise must survive that restructure and the
  async cross-thread (asyncio.to_thread) split.
status: active
provenance: https://docs.dbos.dev/explanations/how-workflows-work (durable single-execution / recorded-result replay; #763 "Improve Behavior Consistency" reworked _core.workflow_wrapper)
explorations:
  - key: concurrent-and-replay-invoke
    title: Concurrent same-id and completed-async re-invoke stay exactly-once
    description: >-
      (a) Two concurrent invocations of one workflow id: the body runs once,
      both callers get the identical recorded result. (b) Re-invoking an
      already-completed async workflow returns the recorded result, and one that
      recorded an ERROR re-raises the ORIGINAL exception type — never a pipeline
      leak (KeyError / NoResult / missing init_status). Side effect fires exactly
      once across all invocations.
    status: ready
    result: null
    reason: null
    workload: workloads/workflow-invoke-outcome-pipeline/workflow_invoke_outcome_pipeline_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/workflow-invoke-outcome-pipeline/workflow_invoke_outcome_pipeline_workload.py --rung rung-001-concurrent-and-replay-invoke --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: null
---

# Workflow invocation runs the body once or returns the recorded outcome

Evidence lineage: `areas/workflow-invoke-outcome-pipeline.md`, work item `e-030`.
New corridor from the diff-directed scan of #763 (`a43fead`, "Improve Behavior
Consistency"), which restructured `_core.workflow_wrapper` from `.wrap(init_wf)`
to `.wrap(get_wf_invoke).intercept(check_and_init)` — the highest-blast-radius
change in the three new commits (every workflow invocation flows through it).
`check_and_init` populates a shared-closure `init_status` only on the
`should_execute` branch; for async (`Pending._intercept`/`_wrap`,
`dbos/_outcome.py`) `check_and_init` and `get_wf_invoke` run on separate
`asyncio.to_thread` threads. Distinct from E-024 (decorator inner-hook
duplication) — this targets the raw invoke pipeline's short-circuit, not
decorator wrapping. Lower prior confidence (scout traced the sync path as
behavior-preserving); the async/concurrent matrix is the falsification surface.
