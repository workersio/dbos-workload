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

## DOSSIER BATCH (2026-07-09, post-filing-freeze) — read this first

Viswa froze upstream filing (he batch-sends himself). Deliverable: maintainer-ready
dossiers in `.workers/dossiers/` + `READY-TO-SEND.md` index. **Verification is
EMPIRICAL against latest RELEASED pypi dbos (2.26.0), NOT the fork** — the fork
tracks unreleased main and is AHEAD of the release (has #744, #763 that 2.26.0
lacks). Released venv: `<scratchpad>/venv/bin/python` (real 2.26.0 in site-packages;
run repros as scripts from a NEUTRAL dir so `import dbos` = release, not `./dbos`).
Fork test: `PYTHONPATH=<repo>`. Local pg still on :5459.

Triage of the 6 named held candidates + 1 new scout find:
- **e-032 (NEW, scout)** — `DBOS.send` from a step not exactly-once (e-031 analog on
  notifications). **LIVE on release AND main** → highest value. Confirmed both
  (step-send=3 copies, wf-send=1). Dossier+repro+work-item+run done.
- **e-024** — completed async workflow re-runs body. LIVE on release, FIXED on main. Packaged.
- **e-023** — sqlite datasource pre-check not retried. LIVE on release, FIXED on main (#763). Packaged.
- **e-025** — client get_event full-timeout after terminal miss. LIVE both, but CONTRACT
  QUESTION (#718 risk) — packaged as a QUESTION, not a defect.
- **e-002, e-015** — FIXED in released 2.26.0 (#744 shipped). NOT sendable.
- **e-008** — env-only repro + mechanism removed upstream. NOT sendable.
All in `READY-TO-SEND.md`. Filing stays with Viswa. Scout also flagged
resume-vs-finalize (#718 risk, low conf) + sync-child-getresult (exploratory) — not pursued.

## SESSION RESUME (2026-07-09, post-publish) — read this first

Done this session: (1) **e-028 FILED #769** upstream + bookkeeping. (2) **e-031
NEW RED finding** — write_stream-from-step not exactly-once — full oracle-plane
workload + specs committed; local pg confirmed (control GREEN, step-retry-sync +
async RED, selftest RED). (3) **publish.py** fixed (dead project id) + 3 area
frontmatter files fixed; **41 officials published** to live project, `published:`
ids recorded/committed; cloud runs (incl. e-031 x3) **executing** on worker —
verdicts via conductor watch. (4) **S2 demoted** 10→4 (atomic effect+record blocks
double-apply; reason in backlog). HEAD `72f903d`, tree clean, synced.

**STANDING POOL COVERAGE-EXHAUSTED (row 1) — 2026-07-09.** S1 RED→filed (#770);
S2/S3/S4 attacked-and-DEMOTED with grounded reasons (backlog); S5 parked at 5 <
threshold 6. No in-flight/ready/re-entry work; diff frontier `a43fead` ==
last-scanned (no new dbos/ source commits — the concurrent workstream only touches
`.workers/`). Backlog top-active below threshold → **row-1 goal state.** Resume
triggers: (a) new dbos/ source commits (rebase the fork onto newer upstream) →
row-4 diff-directed; (b) a fresh scout refresh if grinding deeper is directed.
S4 probe evidence: `workloads/recv-cancel-storm/_probe.py` (double-cancel window
transient + gated by per-process single-execution → non-finding). **Filing:**
e-028 done (#769); **e-031 FILED #770**
(2026-07-09, Viswa-approved; record `.workers/issues/E-031-...-770.md`). Local pg for dev-box runs: postgres on
:5459 (`unix_socket_directories=/tmp/wiopg`); export
`DBOS_POSTGRES_ADMIN_URL=postgresql+psycopg://postgres:dbos@127.0.0.1:5459/postgres`
and run the workload via `.workers/python-runtime.sh` directly (run-with-postgres.sh
needs root chown). **Never edit tracked files while publish.py runs — it needs a
clean+pushed tree per create.**

## SESSION RESUME (2026-07-10, /goal continuous) — read this first

**e-032 oracle-plane re-entry RESOLVED.** Built the full v0.6.0 oracle-plane
workload `.workers/workloads/send-step-oaoo/send_step_oaoo_workload.py` (mirrors
e-031 stream-step-oaoo). **CLOUD-CONFIRMED RED** on the fork image: exploration
`nd7egj5bq47xxtj3q25tkhn2vh8a9js9`, project `kn71mb4p…`, branch main `9efd60b`,
depth 3. case-001 control GREEN (copies=1); case-002 step-sync + case-003
step-async RED (copies=3, received=3, async parity via `DBOS.send_async`);
durawatch mutation; crashclock K=3; terminal SUCCESS; selftest RED (local).
Run record `../runs/E-032.md`; work-item rung `done_red`. Filing HELD (Viswa
batch-sends). Two gotchas recorded: (a) root `.gitignore:184 !.workers/lib/**`
un-ignores lib pycache → added `.workers/.gitignore lib/__pycache__/`; (b) cloud
command MUST wrap `.workers/run-with-postgres.sh` or the guest has no pg (:5432
refused → SETUP-BLOCK). HEAD `9efd60b`, pushed, image prepared+matched.

After e-032: the re-entry queue's remaining items (partial-gc-orphan-reuse =
RED-harvested/held; concurrent-bounce-coalescing = green/decayed) are not fresh
executor work. Diff frontier `a43fead` == last-scanned (no new dbos/ source
commits).

**PRODUCER SCOUT this cycle — OAOO sibling-gap family EXHAUSTED, L1b RESOLVED →
row-1 coverage-exhausted.** Ran a source-grounded scout of the sibling-gap
corridor that produced e-031/e-032: the only durable-write primitives splitting on
`is_workflow()` with distinct `_from_step`/`_from_workflow` persistence are
`write_stream` (RED e-031), `send` (RED e-032), and `set_event` — and `set_event`
is **NOT a finding** (both writes are `on_conflict_do_update` UPSERTs keyed on
stable columns incl. `function_id`; a same-`function_id` step retry overwrites the
same row → idempotent by construction, guard absence compensated). Other
`is_workflow()` sites (`_core.py` 1891/1936/2000) are generic step-invocation
dispatch, not durable writes. Family closed (backlog §OAOO sibling-gap).
Also ground-demoted the last above-threshold backlog row **L1b**: its app-db-batch
part = DONE (e-028 rung-002, cloud `01KX4BZJ…`); its GC-vs-recovery race =
non-finding (`garbage_collect` `gc_filter` excludes PENDING/ENQUEUED/DELAYED → GC
only deletes TERMINAL, recovery only re-enqueues PENDING/ENQUEUED → disjoint status
sets). L2→e-029 GREEN, L3→e-030 GREEN (reconciled in backlog).

**→ Dispatcher row-1 boundary: no above-threshold un-attacked corridor, no
ready/in-flight/re-entry work, no diff trigger (a43fead==last-scanned). Coverage of
the current frontier is exhausted.** Resume triggers: (a) new dbos/ source commits
(rebase fork onto newer upstream) → row-4 diff-directed; (b) a directed deeper
standing-pool grind (S5 notify-loss availability corridor is parked at 5 < 6, the
next-highest residual). Filing of held candidates (e-032 etc.) stays with Viswa.

## SESSION RESUME (2026-07-10, Wave 2 re-arm) — read this first

**FLEET-STOP for the `a43fead` frontier was VALID and is now SUPERSEDED by a
Viswa-approved Wave 2 directive: re-arm with CHANGED AXES, not the same groove.**
Four axes: (1) fault-matrix sweep of `.workers/fault/net/` across top harvested
promise areas — NEW workloads fault-engaged by default; (2) UNPARK S5
notify-loss-db-reconnect (availability) — threshold waived this wave; (3)
depth/volume: re-run strongest OAOO-family workloads at depth ≥20 with seed sweeps
(e-031/e-032 ran depth 1/3); (4) genlib input-generation campaign on
serialization/input surfaces. Ranked corridor pool **W2-1..W2-5** written to
`backlog.md` §Wave 2. Dispatcher re-fired → executor mode, top-first from W2-1.

Executor gate re-verified this wave: `wio 0.4.0` cloud; worker
`worker-62f0f9ec…` ONLINE 16 slots (us-east-1); `simulate create` supports
`--faults`/`--depth`/`--seed` (total = depth×faults; bare fault names + repo-relative
paths resolve in `.workers/fault/`). Faults shape guest Postgres :5432 — matches the
cloud guest pg, so fault-engaged OAOO runs are viable. Project `kn71mb4p…`.
**Gotcha (still in force):** cloud command MUST wrap `.workers/run-with-postgres.sh`
or the guest has no pg; keep tree clean+pushed while prepare/publish runs.

## WAVE 2 COMPLETE (2026-07-10) — changed-axes sweep, read this first

All five Wave-2 corridors executed. **Two new findings + one strengthening +
two green robustness confirmations.** Filing of findings HELD for Viswa.

| corridor | axis | result |
|---|---|---|
| **W2-1** oaoo-under-dbfault-depthsweep | 1+3 | **STRENGTHEN** — e-031/e-032 reproduce identically under db-flaky + db-burst-loss (76 runs); the GUARDED workflow-context control path holds exactly-once under fault (0/76 breaks) → new under-fault regression-guard. No amplification (copies==K). `runs/W2-1-…` |
| **W2-4 → e-033** genlib-serialization | 4 | **NEW RED (cloud-confirmed)** — portable JSON serializer emits non-RFC-8259 `NaN`/`Infinity` for float edges; the stored `workflow_status.inputs` carries `NaN` yet the workflow SUCCEEDs (silent). Cloud exp `nd78s44b…`. + determinism: `set`-of-strings serializes non-deterministically across processes (local-confirmed; cloud subprocess too slow → VOID). `work-items/e-033.md` |
| **W2-3 → e-034** notify-reconnect-latency (S5 unparked) | 1+2 | **RED characterization (cloud-confirmed)** — deterministic (trigger-disable) 60s recv stall on a missed NOTIFY: control 6.3s vs missed 63.6s (cloud exp `nd7464zd…`), no loss. Availability weight 2, documented-fallback → characterization, not finding_candidate. `work-items/e-034.md` |
| **W2-2** recovery-db-faults | 1 | **GREEN** — rung-001 12/12 SUCCEEDED under db-flaky; #744 recovery fix robust under real packet loss. |
| **W2-5** debounce (db-slow) | 1 | **GREEN** — coalescing held (4 green, 3 setup-noise). Queue arm not run (low-value follow-on; S3 queue already grounded-demoted; db-slow trips setup on the crash-orchestrating workloads). |

Operational learnings recorded (survive compaction): (a) a RED (exit 1) under an
active fault is classified `state:failed/failureCategory:fault_model` — same as a
fault crash; verdicts MUST be parsed from stdout INVARIANT/VERDICT lines
(`scratchpad/analyze_oaoo.py`), not run state. (b) `wio workloads logs` truncates
to the tail → rely on the aggregated final `VERDICT:` line. (c) The managed worker
intermittently wedges (online, 0 ongoing, pending backlog); `wio worker stop`+`start`
clears it, but orphans queued explorations → cancel + relaunch. (d) Guest
subprocesses that import the full `dbos` stack take >45s (cold musl imports) — avoid
per-value subprocess fan-out in cloud workloads. (e) db-slow (600ms) / db-burst-loss
(30%) intermittently trip pg connect-timeout at setup → exit-44 SETUP-BLOCK (VOID,
not a finding); db-flaky (10%) is the clean reconnect signal.

**Dispatcher post-Wave-2: changed-axes frontier covered.** No above-threshold
un-attacked Wave-2 corridor remains; new findings (e-033, e-034) held for Viswa.
Resume triggers unchanged: new dbos/ source commits, or a further directed wave.

## Counters

| Field | Value |
|---|---|
| loops run this session | 1 (e-032 executor: oracle-plane build + cloud replay) |
| workloads run this session | 1 (e-032 send-step-oaoo, cloud x3 seeds) |
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

- re-entry: `e-032 step-send-duplication` → **deepen** — build the full
  oracle-plane workload for e-032 (operator-queued 2026-07-10; the
  instruction was typed into the prior interactive session but never ran).
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
