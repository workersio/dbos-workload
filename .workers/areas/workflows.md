---
key: workflows
title: Workflows
description: Durable workflows checkpoint deterministically, survive decorator composition, keep lifecycle operations safe, and expose their full state to queries.
order: 40
---

# Workflows

What this area covers: the core durable-execution contract of DBOS Transact
Python — a `@DBOS.workflow()` function checkpoints each step, replays from
recorded outputs instead of re-running work, and remains inspectable and
manageable (cancel, resume, fork, delete, query) at every point in its life.
Async workflows, decorator-wrapped entrypoints, and operator lifecycle
actions must all preserve that contract.

Boundaries:
- In scope: async checkpoint determinism, decorator composition, lifecycle
  state transitions (cancel/resume/fork/timeout/recovery), attribute and
  temporal queries over workflow state.
- Out of scope until a promise names them: queue admission (see Queues),
  crash recovery ownership (see Recovery).

Evidence lineage: legacy hunt corpora in `areas/async-checkpoint-determinism.md`
(rungs 001–003), `areas/decorator-composition-fidelity.md` (rung 001, candidate
E-024), `areas/lifecycle-fork-state.md` (rungs 001–005, findings #735 and
global-timeout both closed as intended), and `areas/workflow-attributes-query.md`
(rungs 000–007).
