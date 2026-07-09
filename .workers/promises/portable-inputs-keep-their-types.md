---
key: portable-inputs-keep-their-types
area: serialization
title: Portable inputs keep their types
claim: >-
  Workflows using the portable JSON serializer receive arguments matching
  their Python type hints across scheduled triggers, backfills, queued and
  recovered rows, and direct row insertion — datetimes normalize
  deterministically and invalid values fail with modeled errors.
status: active
provenance: https://docs.dbos.dev/python/reference/serialization (portable JSON serializer type coercion and validation)
explorations:
  - key: scheduled-datetime-roundtrip
    title: Scheduled datetimes round-trip typed
    description: >-
      Scheduled triggers, class and instance methods, and directly inserted
      portable rows carrying datetime and date arguments; every entry path
      must deliver values compatible with the declared type hints or fail
      with a modeled terminal error.
    status: done
    result: null
    reason: null
    workload: workloads/portable-input-type-fidelity/portable_input_type_fidelity_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/portable-input-type-fidelity/portable_input_type_fidelity_workload.py --rung rung-001-scheduled-datetime-portable-roundtrip --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd72vya11v22btqbn4rptw73598a7rxq
---

# Portable inputs keep their types

Evidence lineage: `areas/portable-input-type-fidelity.md` rung 001, proven
green on the pinned target 3df88c4b (42/42 invariants across scheduled,
queued, recovered, and direct-insert entry paths).
