# DBOS fault library (`.workers/fault/`)

Network fault models for `wio simulate create --faults <name> --depth N`.
Each `(seed, fault)` pair is one workload. Schema is **netem v1**
(`packages/environment/src/netem.rs` in formal — `NetemV1`, strict
`deny_unknown_fields`): per-interface `match` rules with
`delay/loss/corrupt/duplicate/reorder/rate/slot` clauses, rendered to
in-guest `tc netem` qdiscs. When `seed` is omitted the **run seed** is folded
into the qdisc seeds, so sweeps stay deterministic per seed.

**Semantics:** rules are installed at guest boot and stay active for the
whole run — these are *ambient flakiness profiles*, not time-windowed
outages. On TCP, packet loss surfaces as stalls, timeouts, and connection
resets — the retry/recovery surface. Keep loss moderate (≤ ~30%) or the
workload's own Postgres/Kafka setup phase can't complete and the run dies as
setup noise instead of exercising a promise.

## Models (`net/` — bare names resolve here)

| model | target | profile | aims at |
| --- | --- | --- | --- |
| `db-flaky` | Postgres :5432 | 10% correlated loss + 20±80ms jitter | OCC retry (#664 class), reconnect paths, exactly-once across transient DB errors |
| `db-slow` | Postgres :5432 | 600±400ms latency | timeout/retry paths, dedup + debounce under slow system DB |
| `db-burst-loss` | Postgres :5432 | 30% loss, correlation 0.9 (bursty micro-outages) | repeated-reconnect state machines, double-recovery (E-002 class) |
| `kafka-flaky` | broker :9092 | 10% loss + 50±150ms jitter | consumer rebalance/redelivery, offset loss (#718 class), exactly-once consumer promise |

All matches are `bidirectional: true` (the renderer mirrors src/dst) and
port-scoped, so only the target service's traffic is shaped.

## Plane split — what does NOT go here

netem shapes *packets*. These classes are injected at the **harness level**
inside the workload body instead:

- **crash-at-step** → crash-clock library (process self-kill at seeded step)
- **clock skew** → harness-level time manipulation
- **application-level duplicate delivery** → client-side replay (netem
  `duplicate` only duplicates packets; TCP dedups them — it does not create
  app-visible duplicates)

## Binding notes (for reuse on other repos)

These files are instances of generic classes parameterized by repo topology:
the `dport` values (here 5432/9092) and the intensity (match the repo's
timeout budget). To port to another product, re-bind ports + intensities;
the class structure is unchanged.
