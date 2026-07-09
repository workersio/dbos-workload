---
key: decorated-functions-replay-durably
area: workflows
title: Decorated functions replay durably
claim: >-
  DBOS-decorated functions stay discoverable, durable, and replayable when
  application code wraps them with ordinary Python decorators — replay
  serves the stored result without re-running application-side wrapper
  logic or duplicating its side effects.
status: active
provenance: https://docs.dbos.dev/python/tutorials/workflow-tutorial (decorator registration and replay semantics)
explorations:
  - key: decorator-entrypoint-matrix
    title: Wrapped entrypoints replay without side effects
    description: >-
      Workflows composed with __wrapped__-preserving application decorators
      across sync/async and inner/outer orderings; completed replay must
      serve the stored result. RED confirmed at target 9922c1d (E-024):
      invoking an already-completed DBOS-outer async workflow re-executes
      the inner application hook (invariant
      dbos_outer_completed_replay_does_not_rerun_inner_hook FAIL); the sync
      control does not. Upstream filing pending human decision.
    status: done
    result: finding
    reason: null
    workload: workloads/decorator-composition-fidelity/decorator_composition_fidelity_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/decorator-composition-fidelity/decorator_composition_fidelity_workload.py --rung rung-001-custom-decorator-entrypoint-matrix --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: "run 01KX3Y9XE23VM5SYAEWD3PB33M — INVARIANT dbos_outer_completed_replay_does_not_rerun_inner_hook FAIL"
    freshness: new-current
    reported: null
    published: nd76xkcjz2a333g1xq4jb4nsrd8a7vjt
---

# Decorated functions replay durably

Evidence lineage: `areas/decorator-composition-fidelity.md` rung 001 and
issue draft E-024 (completed-replay-reruns-inner-hook, observed on the
pinned target 3df88c4b). Durable status and hook ledgers stayed stable;
only the wrapper side effects duplicated. Held as an active candidate —
no official published until the finding is filed or refuted.
