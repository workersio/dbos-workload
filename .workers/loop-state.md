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
| loops run this session | 6 |
| workloads run this session | 6 (e-028 x2 incl app-db variant; e-029 x2; e-030) |
| loop cap (rail) | 100 |
| workload cap (rail) | 250 |
| no-new-information streak | 0 |
| staleness K | 5 |

## Worker note

- Managed worker auto-idled to `offline` mid-session (blocked prepare/run);
  restarted with `wio worker start` (billed 16-slot container; auto-idles again).

## In-flight

- none. Diff-directed batch corridors resolved on cloud:
  - `e-028` → **RED finding candidate** (`01KX460BYM2JHVTJKT2XBQE4WN` white-box;
    **strengthened** by app-db-batch public-API variant `01KX4BZJHVB2V4MKPA9FDY08JE`).
  - `e-029` → **GREEN regression rung** (`01KX47FF2KHTYPY50VCVFSP6BX`).
  - `e-030` → **GREEN regression rung** (`01KX4BVNZ14SCYC29KYRH5BY19`).

## Findings this session

- **e-028** GC two-phase orphan → stale OAOO replay (#751): RED, cloud-confirmed
  via BOTH the crash-between-phases (rung-001) and the public-API app-db-batch
  partial-failure (rung-002) triggers. `finding_candidate`; upstream filing held
  for Viswa. Ready to draft a public-API dossier on his go.

## Diff-directed batch — COMPLETE (trigger cleared)

The `9922c1d..a43fead` diff-directed episode is fully covered:
- **#751** incremental GC → **RED** finding (e-028, two triggers: crash-between-phases
  + public-API app-db-batch partial failure).
- **#752** debounce-with-delay → **GREEN** regression (e-029 concurrent coalescing).
  L4 (post-deadline input mutation) **RETIRED as observational** — source grounding
  shows `debounce_timeout` caps only delay time, no input-freeze guarantee (#718 trap).
- **#763** behavior-consistency → **GREEN** regression (e-030 invoke pipeline).

last-scanned-sha `a43fead` == target-head-sha → no new-commit trigger.

## Dispatcher status → row 1 boundary (diff frontier)

No in-flight/ready work, no pending re-entry, diff-directed trigger cleared,
backlog top-active below threshold (only parked entries: whitespace-key 3,
gc-leak 2). Coverage of the **triggered** frontier is exhausted. Resuming the loop
means either: (a) a **standing-pool scout refresh** (row 6 producer, `overview.md`
areas — mostly harvested green, lower value), or (b) **await new target commits**
(the fault-library workstream keeps pushing to origin/main; a code-touching commit
re-fires row 4). Recommend (b) unless directed to grind the standing pool.

## Open decision (Viswa)

- **e-028** filing: RED finding, now with a public-API-triggered variant that
  removes the white-box objection. Draft-a-dossier is ready on his go; not filed.

## Findings this session

- **e-028** GC two-phase orphan → stale OAOO replay (#751): **RED**, cloud-confirmed
  (`01KX460BYM2JHVTJKT2XBQE4WN`). `finding_candidate`; upstream filing held for
  human triage (Viswa).

## Re-entry

- re-entry: `partial-gc-orphan-reuse` → **switch** (RED-harvested; filing held).
- re-entry: `concurrent-bounce-coalescing` → **deepen/recal** — green in substance,
  oracle recalibrated for cloud timing; re-run then decay the debounce corridor.
- Next ready executor corridors after e-029 green: backlog **L4** (post-deadline
  debounce input mutation, reuses debounce harness) and **L3** (outcome-pipeline
  async replay, #763). **L1b** (GC app-db-batch / GC-vs-recovery) deepens e-028.

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
