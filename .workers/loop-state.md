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

## SESSION RESUME (2026-07-09, post-publish) — read this first

Done this session: (1) **e-028 FILED #769** upstream + bookkeeping. (2) **e-031
NEW RED finding** — write_stream-from-step not exactly-once — full oracle-plane
workload + specs committed; local pg confirmed (control GREEN, step-retry-sync +
async RED, selftest RED). (3) **publish.py** fixed (dead project id) + 3 area
frontmatter files fixed; **41 officials published** to live project, `published:`
ids recorded/committed; cloud runs (incl. e-031 x3) **executing** on worker —
verdicts via conductor watch. (4) **S2 demoted** 10→4 (atomic effect+record blocks
double-apply; reason in backlog). HEAD `72f903d`, tree clean, synced.

**NEXT (aim discipline):** S3 DEMOTED (grounded, backlog). **S4
`recv-async-cancel-storm` (8) is the genuine next attack — NOT a demote.**
Grounding done: `_run_event_setup_async` (`_sys_db.py:3119-3170`) — the function's
OWN docstring admits the hazard: a leftover `notifications_map`/recv entry after
cancellation → next recv `DBOSWorkflowConflictIDError` → "parks the caller in
await_workflow_result forever." The code is the fix; its residual is the
**double-cancel** path: on a 2nd CancelledError during cleanup-wait it defers
unregister to `add_done_callback(unregister)` and re-raises — a window where a
concurrent recv on the same (workflow, topic) can trip the stale entry before the
callback fires. Attack: `interleave` (2-3 async actors) driving recv_async →
cancel → cancel-again (force deferred path) racing a 2nd recv into the window;
oracle = liveness watchdog (caller must not park) + no spurious ConflictID +
terminal sweep. Build v0.6.0 oracle-plane workload. After S4, backlog top-active
is below threshold (S5 parked at 5) → row-1 coverage-exhausted candidate. **Filing:** e-028 done (#769); **e-031 FILED #770**
(2026-07-09, Viswa-approved; record `.workers/issues/E-031-...-770.md`). Local pg for dev-box runs: postgres on
:5459 (`unix_socket_directories=/tmp/wiopg`); export
`DBOS_POSTGRES_ADMIN_URL=postgresql+psycopg://postgres:dbos@127.0.0.1:5459/postgres`
and run the workload via `.workers/python-runtime.sh` directly (run-with-postgres.sh
needs root chown). **Never edit tracked files while publish.py runs — it needs a
clean+pushed tree per create.**

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

## OPERATING DIRECTIVES (v0.6.0 — 2026-07-09, survive compaction)

- **Universal oracle plane — every NEW workload MUST carry:** (1) liveness
  watchdog, (2) terminal-state sweep, (3) `durawatch` manifest where effects are
  acked, (4) fault timing via `crashclock` declared spaces, (5) async-parity
  drivers where the API has async forms. Import from `.workers/lib/`
  (crashclock, durawatch, genlib, interleave; see `.workers/lib/README.md`).
  Conventions: declared spaces not magic constants; determinism; **anti-vacuity
  floors — a case that never armed its fault / acked too few effects is VOID not
  green**; each oracle has a selftest that can go RED (`ORACLE_SELFTEST`).
  Dependency services via `.workers/recipes/` (exit 44 = setup-block, never a
  product finding).
- **Aim discipline:** do NOT stop while any above-threshold backlog row is
  un-attacked — attack it, or demote it with a recorded reason.
- **Decisions in force:** (a) grind the STANDING POOL now (do not wait for new
  commits); (b) publish done explorations via `.workers/publish.py`; (c) e-028
  filing — **DONE**, filed upstream as #769 (Viswa gave GO).

## ACTIVE TASKS (post-compaction resume here)

- **e-028 FILING — DONE. FILED as #769.**
  https://github.com/dbos-inc/dbos-transact-py/issues/769 — filed 2026-07-09 via
  `gh` as **viswa-abe**. ONE issue, BOTH variants (sys-db-side partial GC +
  app-db partial failure). Ordinary-user framing, ZERO product vocab, standalone
  repro in collapsed `<details>`. Repro **verified on released `dbos==2.26.0`**
  (both variants + control) before posting. Note: released `garbage_collect` has
  NO `batch_size` (single non-batched app-db delete) — issue does not reference
  batching (that's main-only, #751). Record: `.workers/issues/E-028-gc-orphan-oaoo-filed-769.md`.
  Promise `reported: 769`. Dossier draft: `.workers/dossiers/e-028-gc-orphan-oaoo.md`.
- **publish.py BLOCKED** — crashes intermittently at `image_commit()` when
  `wio projects get` returns `preparation.currentImage: null` (transient during
  any in-flight prepare). Needs a None-guard + retry. Publication of the 23 done
  explorations is PENDING. (Being patched.)
- **standing-pool scout** running (agent `a014f5a57b64e1206`) → seeds `backlog.md`,
  then attack top-first with v0.6.0 oracle-plane workloads.

## S1 stream-step-oaoo — CONFIRMED RED (probe, local) 2026-07-09

**Finding e-031 candidate.** `DBOS.write_stream` from a **step** context routes to
`write_stream_from_step` (`_sys_db.py:4229`) which has **no**
`_check_operation_execution_txn` guard (its workflow sibling
`write_stream_from_workflow` DOES, `:4265`); `streams` PK is
`(workflow_uuid, key, offset)`, excludes `function_id`. A `@DBOS.step(max_attempts=3)`
that calls `DBOS.write_stream` then fails re-invokes the body under the SAME
`function_id` each retry (`_core.py` retry loop), re-inserting a duplicate at a new
offset. **Probe (local pg :5459): step-context → `['V','V','V']` count=3;
workflow-context → `['V']` count=1.** Same identical API (`DBOS.write_stream`), no
doc distinction → differential OAOO-consistency violation, sibling to e-028.
Full oracle-plane workload BUILT + verified local: `.workers/workloads/stream-step-oaoo/stream_step_oaoo_workload.py`.
case-001 control copies=1 GREEN; case-002 step-retry-sync K=2 copies=2 RED;
case-003 step-retry-async K=4 copies=4 RED (async parity via write_stream_async);
ORACLE_SELFTEST forces control RED (oracle live). Specs written: area
`stream-durability-oaoo`, promise `streams-record-each-write-once` (3 explorations),
work-item `e-031`, run `E-031`. Backlog S1 → DONE. **Cloud replay PENDING**
(publish/prepare churn); **upstream FILED #770** (2026-07-09, Viswa-approved).
Local pg for dev-box runs: `pg_ctl -D <scratch>/pg16 -o "-p 5459 -c unix_socket_directories=/tmp/wiopg"` (harness run-with-postgres.sh needs root chown; bypass it).

## Standing-pool grind — plan (resume here post-compaction)

Diff-directed batch done. Now row-6 producer on the standing pool: refresh the
scout fan-out over `work-items/overview.md` areas, promote above-threshold
corridors into `backlog.md`, attack top-first. NEW workloads carry the v0.6.0
oracle plane above. Candidate high-value angles not yet attacked with the new
plane: recovery×GC join (durawatch across recover_pending), queue dequeue-crash
fault-timing (crashclock op-index kill), notification/event durability
(durawatch delay ladder), datasource OAOO under dependency restart
(crashclock restart_dependency). Ground each before asserting.

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
