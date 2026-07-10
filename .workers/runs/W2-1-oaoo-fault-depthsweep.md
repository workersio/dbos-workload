# Run W2-1 — OAOO family under DB-fault, depth sweep (Wave 2, axes 1+3)

Corridor: `oaoo-under-dbfault-depthsweep` (backlog §Wave 2, score 12).
Question: re-run the two strongest OAOO-family workloads (e-032 send-step,
e-031 stream-step) **fault-engaged** at depth 20 and test two hypotheses the
depth-1/3 baselines could not:
- **(a)** does the GUARDED workflow-context CONTROL path stay copies==1 under
  transient DB loss/reconnect, or does a fault-induced recovery re-execution
  break the exactly-once guard on the *protected* path? (would be a NEW,
  higher-value red — a break on a green path.)
- **(b)** does the step-path duplication widen beyond the retry multiplicity K
  under reconnect churn (copies > K)? (fault-amplified duplication.)

Depth/seed rationale: fault-engaged ⇒ depth 20 per the seed-sensitivity rule —
the netem realization is folded into each run's seed, so 20 seeds = 20 distinct
loss/jitter/burst timings (genuine coverage). NB the graded K is pinned per case
(seed 92xx/93xx enters `RETRY_SPACE.point`), so depth varies the *fault*
realization, not K — exactly the axis-1 question we want.

| Field | Value |
|---|---|
| Project | `kn71mb4pcxmees43sy547v76z98a7fv0` (DBOS Workload) |
| Branch / commit | main / `a2252a5` (image buildStatus succeeded, commitSha matched) |
| Faults | `db-flaky` (10% loss + 60±40ms) , `db-burst-loss` (30% loss corr 0.9) |
| Depth | 20 per fault ⇒ 40 runs per workload |

## send-step-oaoo (e-032) — exploration `nd70qjy6zyh8aagmas268v6nd98a95gd`

Command: `.workers/run-with-postgres.sh .workers/python-runtime.sh
.workers/workloads/send-step-oaoo/send_step_oaoo_workload.py --rung
rung-001-send-step-oaoo --all-cases --sequential` ; `--faults
db-flaky,db-burst-loss --depth 20 --timeout 600 --mem 2048`.

**Result: 40/40 terminal, 0 setup-blocks** (db-burst-loss's 30% loss did NOT
break the pg setup phase — the fault library is well-tuned to the guest).
- Every run RED on the **step** cases: `case-002` (step-sync) and `case-003`
  (step-async) both `copies=3, k=3` on all 40 seeds, both faults. e-032
  reproduced identically under network fault.
- **HYP (a): 0 control-path breaks.** case-001 (control, guarded
  workflow-context) passed its exactly-once + terminal + durawatch invariants on
  every one of the 40 runs (absent from every aggregated fail-list). The guard
  held under transient DB loss, jitter, AND 30% bursty micro-outages.
- **HYP (b): 0 amplification.** copies never exceeded k (stayed exactly 3).
  The duplication is precisely the retry multiplicity, not fault-widened.

Interpretation: **strengthening + robustness result, no new red.** The e-032
step-path OAOO defect is robust under network faults; the guarded workflow-context
path is now a confirmed regression-guard that holds exactly-once under db-flaky and
db-burst-loss. Note: WIO classifies a RED (exit 1) under an active fault as
`state:failed / failureCategory:fault_model` — indistinguishable from a
fault-induced crash without reading stdout; verdicts here were extracted by
parsing per-run INVARIANT/VERDICT lines (`scratchpad/analyze_oaoo.py`).

## stream-step-oaoo (e-031) — exploration `nd70jb2awgcktcf05pdwkkxen98a8vzd`

Command: same shape on
`.workers/workloads/stream-step-oaoo/stream_step_oaoo_workload.py --rung
rung-001-stream-step-oaoo`. Faults + depth identical.

**Result: 36/40 terminal, 0 setup-blocks, all 36 RED** (4 stragglers wedged in
`pending` with 0 ongoing + idle slots after the batch drained — cancelled; 36 is
conclusive). Identical clean pattern to send-step:
- step cases RED at `copies == k` on every seed: case-002 (step-sync) copies=2=k,
  case-003 (step-async) copies=4=k. e-031 reproduced under both faults.
- **HYP (a): 0 control-path breaks** — case-001 guarded write held exactly-once
  under db-flaky AND db-burst-loss (18 each).
- **HYP (b): 0 amplification** — copies never exceeded k.

## W2-1 verdict

**Strengthening + robustness, no new red.** Both OAOO findings (e-031 write_stream,
e-032 send) reproduce identically under db-flaky (10% loss+jitter) and db-burst-loss
(30% bursty micro-outages) across 76 fault realizations; the step-path duplication
is exactly the retry multiplicity K (not fault-widened); and — the novel Wave-2
result — the **guarded workflow-context CONTROL path holds exactly-once under
transient DB fault** (0/76 breaks), so it is now a confirmed under-fault
regression-guard for the exactly-once primitive. Hypotheses (a) and (b) both
refuted. No filing change (e-031 filed #770, e-032 held for Viswa).

### Operational learning (recorded for the loop)
A workload that exits non-zero (RED, exit 1) **while a fault model is active** is
classified by WIO as `state: failed / failureCategory: fault_model` — the SAME
classification a genuine fault-induced crash gets. State/category alone cannot
distinguish a real RED finding from fault noise in a fault-engaged sweep; verdicts
MUST be extracted by parsing per-run `INVARIANT`/`VERDICT` stdout
(`scratchpad/analyze_oaoo.py`). Also: `wio workloads logs` truncates to the tail,
so multi-case runs need the aggregated final `VERDICT:` line (which lists every
failed invariant across all cases) to confirm a case that scrolled off passed.
