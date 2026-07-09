---
key: schemas-stay-isolated
area: platform
title: Schemas stay isolated
claim: >-
  Multiple DBOS clients and runtimes using different Postgres schemas in
  one process stay isolated — every read and write targets the schema
  declared for that object, not whichever schema was initialized last.
status: active
provenance: https://docs.dbos.dev/python/reference/configuration (per-instance schema configuration); cross-schema leak fix proposed upstream in PR #728
explorations:
  - key: two-schema-client-isolation
    title: Client A must not read schema B
    description: >-
      Two clients on different schemas in one process: after client B
      initializes, client A's list calls return schema B's workflows even
      though the physical rows sit in the correct schemas — class-level
      SQLAlchemy schema metadata is mutated globally by set_schema. Fix
      proposed upstream in PR #728; the pinned target predates it.
    status: done
    result: null
    reason: null
    workload: workloads/schema-isolation-multi-client/schema_isolation_multi_client_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/schema-isolation-multi-client/schema_isolation_multi_client_workload.py --rung rung-001-two-schema-client-datasource-isolation --case case-001
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: "dbos-inc/dbos-transact-py PR #728"
    published: nd7c0zxxxrtxkkze19gqzss3dn8a60py
---

# Schemas stay isolated

Evidence lineage: `areas/schema-isolation-multi-client.md` rung 001,
reproduced twice on the pinned target 3df88c4b with physical SQL
confirming rows landed in the correct schemas while the API read the
wrong one. Upstream fix proposed in PR #728 (per-engine
schema_translate_map); this row stays red until a target bump lands it.
