# READY-TO-SEND — maintainer dossier index

Batch of DBOS Transact (Python) durability/correctness findings, packaged for
Viswa to send upstream in one pass. **Nothing here is auto-filed.** Each "send"
row is a complete self-contained package: an ordinary-user issue body (ZERO
product vocabulary; repro in a collapsed `<details>`) + a standalone deterministic
repro script, with a one-line differential.

**Verification is empirical against the latest RELEASED PyPI `dbos` (2.26.0)** —
NOT the in-repo fork, which tracks unreleased upstream `main` and is ahead of the
release. This distinction matters: several older candidates read as "fixed" in the
fork source but are still LIVE in the version users actually `pip install`.

Severity weights: data-loss 4 · correctness 3 · availability 2 · wrong-error 1 · cosmetic 0.

## SEND — reproduces on released dbos 2.26.0

| id | title | sev | dossier | repro | verified-on | note |
|----|-------|:---:|---------|-------|-------------|------|
| **e-032** | **`DBOS.send` from inside a step is not exactly-once — delivers the message once per step retry** | **3** | `dossiers/e-032.md` | `dossiers/e-032-repro.py` | **dbos 2.26.0 + main ✅** | **LIVE on release AND main — novel, unfixed. Highest value.** New (scout). |
| e-024 | Invoking a completed **async** workflow re-executes its function body (sync path does not) | 3 | `dossiers/e-024.md` | `dossiers/e-024-repro.py` | dbos 2.26.0 ✅ | live on release; **fixed on main** — heads-up value |
| e-023 | SQLite datasource: transient `database is locked` during the OAOO pre-check fails the workflow permanently instead of retrying | 2 | `dossiers/e-023.md` | `dossiers/e-023-repro.py` | dbos 2.26.0 ✅ | live on release; **fixed on main** (#763-era) — heads-up value |
| e-025 | DBOSClient.get_event waits the full timeout after the target workflow is already terminal without the key | 2? | `dossiers/e-025.md` | `dossiers/e-025-repro.py` | dbos 2.26.0 + main ✅ | **SEND AS A QUESTION, not a defect** — may be intended timeout semantics (#718 risk). Body is question-framed. |

## NOT sendable — fixed in released 2.26.0 or not standalone-reproducible

| id | title | why excluded |
|----|-------|--------------|
| e-002 | Stale queued-recovery executes a queued workflow off-queue | **Fixed in released 2.26.0** (`_recover_workflow` always returns a polling handle; `clear_queue_assignment -> None`). Repro would show green. |
| e-015 | SQL-enqueued role-denied workflow stuck PENDING forever | **Fixed in released 2.26.0** (`_check_required_roles_or_finalize_error` present). Also already filed upstream (#743). |
| e-008 | Active debounce window delays unrelated queued work | Only ever reproduced in-cloud (4 standalone negative controls); the blocking-debouncer mechanism was **removed upstream** (delayed-enqueue redesign). Not standalone-reproducible. |

## Already filed (this session, with prior approval)

| id | title | sev | dossier | repro | verified-on | upstream |
|----|-------|:---:|---------|-------|-------------|----------|
| e-028 | Interrupted `garbage_collect` orphans `transaction_outputs` → reused workflow id replays a dead step result | 3–4 | `dossiers/e-028-gc-orphan-oaoo.md` | scratch `e028_repro.py` | dbos 2.26.0 | #769 |
| e-031 | `write_stream` from a step is not exactly-once (duplicates on every step retry) | 3 | `dossiers/e-031-stream-step-oaoo.md` | `dossiers/e-031-repro.py` | dbos 2.26.0 | #770 |

## Via report skill

Packaged through the `report` skill (four-stage brief/gates/packet/preflight,
full packet dir under `.workers/reports/<id>/`). Verdict column is the skill's.

| id | title | sev | conf | verdict | packet dir | verified-on | hold reason |
|----|-------|:---:|:---:|---------|------------|-------------|-------------|
| e-033 | Portable serialization writes `NaN`/`Infinity` into `workflow_status.inputs`, so the stored input is not valid JSON | 3 | high | **HOLD** | `reports/e-033/` | dbos 2.26.0 + main ✅ (3/3) | rate limit: 3 open, maintainer-unanswered issues by viswa-abe (#768/#769/#770). Send when open-unanswered < 2. |

## Method notes

- Each repro prints a clear differential (control value vs bug value) and exits 1
  with a "REPRODUCES" line when the defect fires; exit 0 = does not reproduce.
- Repros need only `pip install dbos` (+ `sqlalchemy`, `psycopg[binary]` for the
  Postgres ones). SQLite-based repros need no external services.
- "Fixed on main" rows are honest heads-ups: the latest release has the bug, but
  the dev branch already resolves it — Viswa decides whether a heads-up + a
  regression-test suggestion is worth sending.
- Highest-value findings are those live on BOTH release AND main (e-028, e-031) —
  found against the current source frontier; those were the two already filed.
