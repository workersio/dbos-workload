---
key: errors-keep-their-meaning
area: serialization
title: Errors keep their meaning
claim: >-
  A failed workflow's durable record preserves the application error's type
  and message through storage, retry, and recovery — retrieval returns the
  modeled error, never a serializer artifact in its place.
status: active
provenance: https://docs.dbos.dev/python/tutorials/workflow-tutorial (durable error records preserve application error identity); portable error metadata via the portable JSON serializer
explorations:
  - key: serializer-smoke
    title: Error identity survives durable storage
    description: >-
      Baseline: application exceptions raised in workflows must come back
      from durable status and retrieval paths with their original type and
      message under the default serializer.
    status: done
    result: null
    reason: null
    workload: workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-000-serializer-smoke --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd790yhgz784mpxhz27pvmrcts8a7vmz
  - key: portable-error-metadata
    title: Portable errors keep structured metadata
    description: >-
      Errors stored with portable JSON metadata must return their modeled
      name, message, code, and data through public retrieval. RED confirmed
      at target 9922c1d (E-003, rung-004 case-001): structured metadata is
      dropped on raw workflow errors — invariant
      structured_raw_workflow_error_preserves_metadata FAIL. Not yet filed
      upstream; filing pending human decision.
    status: done
    result: finding
    reason: null
    workload: workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-004-portable-structured-error-metadata --case case-001
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: "run 01KX3YEZAWWGWH4Q7BX7DD7K6Q case-001 — INVARIANT structured_raw_workflow_error_preserves_metadata FAIL"
    freshness: new-current
    reported: null
    published: nd70z1k9rh0kwnk3jyv6bm4pm98a786y
---

# Errors keep their meaning

Evidence lineage: `areas/serialization-error-fidelity.md` rungs 000–005.
Rungs 000–002 green; rung-003 case-003 (portable config masks a modeled
ValueError with a serializer TypeError) and rung-004 case-001 (structured
raw-workflow error metadata dropped) are open candidates observed at
0c41e6df, not yet filed upstream. Retry-class stored-error liveness
improved by PR #704 in the pinned target.
