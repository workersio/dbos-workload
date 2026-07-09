# DBOS fault library (`.workers/fault/`)

Network fault models for `wio simulate create --faults <name> --depth N`.
Each `(seed, fault)` pair is one workload; window times and ranged values are
sampled **deterministically per seed** (same seed + config → identical plan).
Schema version `2026-06-24` — see formal `packages/hypervisor/src/workloads/
network_faults.rs` for the canonical schema.

## Models (`net/` — bare names resolve here)

| model | target | window | aims at |
| --- | --- | --- | --- |
| `db-blackhole` | Postgres :5432 | one 2–8s outage at 5–20s | recovery, reconnect, OCC retry (#664 class), exactly-once across DB loss |
| `db-degrade` | Postgres :5432 | 0.5–2.5s added latency for 5–15s | timeout/retry paths, dedup under slow system DB |
| `db-flap` | Postgres :5432 | two short outages (~5s and ~14s) | repeated-reconnect state machines, double-recovery (E-002 class) |
| `kafka-blackhole` | broker :9092 | one 2–8s outage at 5–20s | consumer rebalance/redelivery, offset loss (#718 class), exactly-once consumer promise |

Services are defined **by port only** (loopback addresses may not repeat
across services, and everything here shares 127.0.0.1). Window band 5–20s
assumes runs of ≥30s — keep workload bodies alive past 30s or the fault
window can miss the interesting phase.

## Plane split — what does NOT go here

The runtime's fault plane is **network-only** (latency / bandwidth / partition
/ blackhole / degrade). These classes are injected at the **harness level**
inside the workload body instead:

- **crash-at-step** → crash-clock library (process self-kill at seeded step)
- **clock skew** → harness-level time manipulation
- **duplicate delivery** → client-level replay; `kafka-blackhole` *induces*
  redelivery via rebalance but explicit dup-injection lives in the workload

## Binding notes (for reuse on other repos)

These files are instances of generic classes parameterized by repo topology:
the service ports (here 5432/9092) and the window band (match the repo's run
length). To port to another product, re-bind ports + windows; the class
structure is unchanged.
