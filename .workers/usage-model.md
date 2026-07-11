---
target: dbos
runner: .workers/run-with-postgres.sh .workers/python-runtime.sh
actor-model: process-parallel
personas: {}
flows: {}
events: {}
modules: []
---

# Usage model — DBOS Transact (Python)

Unfilled skeleton. The first producer episode fills this from evidence
(README/quickstarts, examples, docs task pages, the vendor's own tests, the
issue tracker) via the usage-scout fan-out, then takes the strategy-critic
model audit before anything is emitted.

`runner:` boots Postgres and the vendored venv python — the same SUT harness
the previous corpus proved (`build.sh` vendors dbos-transact-py + builds the
venv at prepare time).
