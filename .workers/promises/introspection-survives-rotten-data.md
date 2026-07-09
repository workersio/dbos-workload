---
key: introspection-survives-rotten-data
area: platform
title: Introspection survives rotten data
claim: >-
  Control-plane APIs stay available and actionable when durable records
  contain stale or undeserializable application data — one rotten schedule
  context never denies listing, filtering, pausing, or deleting unrelated
  schedules.
status: active
provenance: https://docs.dbos.dev/python/reference/schedules (schedule introspection APIs); safe-deserialize hardening landed in PR #694
explorations:
  - key: rotten-schedule-introspection
    title: One bad row never blinds the operator
    description: >-
      A schedule context made undeserializable in place; every list, get,
      filter, pause, resume, delete, trigger, and conductor-formatting path
      must keep working for unrelated schedules and degrade gracefully for
      the rotten one. Regression guard on PR #694.
    status: done
    result: null
    reason: null
    workload: workloads/control-plane-state-introspection/control_plane_state_introspection_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/control-plane-state-introspection/control_plane_state_introspection_workload.py --rung rung-001-rotten-schedule-context-introspection --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd7acz0ahsbk0tfevctqfxzqf58a6wn8
---

# Introspection survives rotten data

Evidence lineage: `areas/control-plane-state-introspection.md` rung 001,
proven green on the pinned target 3df88c4b (50/50 invariants across
runtime, client, and conductor paths).
