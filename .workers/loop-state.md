# Loop State — fleet-dbos workload harness

Dispatcher state for the `wio:workload-harness` skill loop. The custom
`.workers/loops/*.md` + `work-items/` scaffold predates this file; this is the
skill-native dispatcher ledger. Spec tree + this file = the loop.

## Target / scan frontier

| Field | Value |
|---|---|
| Project (cloud) | `DBOS Workload` / `kn71mb4pcxmees43sy547v76z98a7fv0` / `workersio/dbos-workload` |
| Fork model | fork-native: repo root IS the DBOS source (`dbos/`), vendored by `.workers/build.sh` |
| Prepared image | `buildStatus: succeeded` at HEAD `3506659` (verified 2026-07-09) |
| target-head-sha (DBOS source frontier) | `a43fead` (#763 Improve Behavior Consistency) |
| last-scanned-sha | `a43fead` (advanced this session; was `9922c1d`) |

## Counters

| Field | Value |
|---|---|
| loops run this session | 1 |
| workloads run this session | 0 (executor in flight) |
| loop cap (rail) | 100 |
| workload cap (rail) | 250 |
| no-new-information streak | 0 |
| staleness K | 5 |

## In-flight

- executor: `e-028` / `rung-001-gc-orphan-oaoo` — GC two-phase orphan → stale
  OAOO replay. Workload build + local sanity in progress; cloud draft run next.

## Re-entry

- re-entry: none pending.

## Triggers

- Row-4 diff-directed trigger CLEARED this session: scanned `9922c1d..a43fead`
  (3 new DBOS commits: #752 debounce-with-delay, #751 incremental GC, #763
  behavior-consistency). Corridors folded into `backlog.md`.

## Session log

### Session 2026-07-09 (interactive, Opus)
- Dispatcher row 4 fired: new target commits since last scan (`9922c1d`).
- Diff-directed scan of #752/#751/#763 + candidate-scout fan-out + first-principles
  read of the GC (`_sys_db`/`_app_db` two-phase) and debouncer (`debounce_delayed_workflow`)
  changes.
- Promoted top corridor **e-028 gc-orphan-oaoo** (GC two-phase orphan → OAOO
  stale replay on reused workflow id; data-correctness, weight 3–4) to executor-ready.
- Backlog seeded with ranked residual corridors (debounce concurrent-coalescing,
  outcome-pipeline async replay, post-deadline debounce input mutation, whitespace
  idempotency-key 500).
- Verified executor gate live: wio authed, project prepared at HEAD, all
  `wio simulate` flags present (`--exploration`/`--workload-file`/`--seed`).
- Advanced last-scanned-sha to `a43fead`.
