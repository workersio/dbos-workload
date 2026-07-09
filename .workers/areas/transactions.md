---
key: transactions
title: Transactions
description: Steps and transactions execute once and only once from the application's view — replay never re-runs committed work, and rollback never strands partial state.
order: 20
---

# Transactions

What this area covers: DBOS's once-and-only-once (OAOO) execution contract
for `@DBOS.transaction()` / datasource operations and their interaction with
workflow replay and recovery. The docs promise that a committed transaction's
side effects happen exactly once even when the surrounding workflow is
re-executed, that retriable database errors (serialization, deadlock) are
retried transparently, and that transactional messaging (`send` inside a
transaction) is atomic with the commit.

Boundaries:
- In scope: replay-once semantics, rollback at the enqueue boundary,
  retry/cleanup on induced failures, transactional send visibility.
- Open candidate (not yet a promise row): SQLite-backed datasource records a
  terminal ERROR before its retry attempt (E-023) — pending upstream triage.

Evidence lineage: legacy corpus in `areas/datasource-transaction-oaoo.md`
(rungs 000–006), curated history in `work-items/` (E-007, E-023).
