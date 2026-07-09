---
key: workflow-state-is-queryable
area: workflows
title: Workflow state is fully queryable
claim: >-
  Every workflow's attributes, status, and timing are visible and filterable
  through the public query APIs — consistently across creation, update,
  replay, fork, terminal transitions, schedules, and export/import.
status: active
provenance: https://docs.dbos.dev/python/reference/contexts (workflow attributes and list/query APIs); temporal introspection added across PRs #674/#681/#682/#685
explorations:
  - key: attribute-query-smoke
    title: Attributes round-trip through queries
    description: >-
      Baseline: user attributes set at creation and update are returned by
      list and get APIs with exact values, composing with status, name, and
      queue filters.
    status: done
    result: null
    reason: null
    workload: workloads/workflow-attributes-query/workflow_attributes_query_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-000-attribute-smoke --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd72vd6sd8kbfe0eqm8h2ytqm58a61eh
  - key: temporal-introspection-windows
    title: Timing windows survive every transition
    description: >-
      Completion and dequeue timestamps must stay consistent across direct,
      queued, and delayed workflows, cancel/resume transitions, relaunch,
      export/import, aggregation buckets, and latency outputs.
    status: done
    result: null
    reason: null
    workload: workloads/workflow-attributes-query/workflow_attributes_query_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/workflow-attributes-query/workflow_attributes_query_workload.py --rung rung-007-temporal-introspection-windows --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd71jj1qf26b3rd1b0h3he2rj98a63x3
---

# Workflow state is fully queryable

Evidence lineage: `areas/workflow-attributes-query.md` rungs 000–007, all
green; rungs 005–007 proven on the pinned target 3df88c4b (schedule
identity, legacy scheduler app-version, temporal introspection windows).
