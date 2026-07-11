# Preflight — e-033

## Verdict: **HOLD**

All hard gates pass; the packet is send-worthy. It is **held** on the rate-limit
gate: we already have 3 open, maintainer-unanswered issues on the target.
Responsiveness on open threads outranks new filings. Queue it; send after some of
#768/#769/#770 are answered or closed and the open-unanswered count drops below 2.

## Rate limit — **FAIL (→ HOLD)**

`gh issue list --repo dbos-inc/dbos-transact-py --author viswa-abe --state open`
→ **3 open**, and each has **0 comments** (checked per-issue `comments` array =
all maintainer-unanswered):

- #770 (write_stream from a step not exactly-once) — 0 comments
- #769 (interrupted garbage_collect orphans transaction_outputs) — 0 comments
- #768 (queued whole-second-quantized timeout deadlines on SQLite) — 0 comments

3 ≥ 2 threshold → HOLD. (These are the recently filed e-031/e-028/e-034-class
issues.)

## Duplicate re-check at send time — clear as of this run

No open/closed issue on NaN/Infinity portability or set determinism (G4). Re-run
the G4 searches immediately before sending, since issues move.

## Account check

Correct gh account for this target: **`viswa-abe`** (Workers product;
`workersio/*` and upstream dbos filings). Not `viswa-q`. A human sends; nothing
is auto-filed.

## Vocabulary lint — **PASS**

`grep -niE '(wio|workers|harness|corpus|invariant|exploration|oracle|workload|
episode|dispatcher)|e-033|W2-4'` over `packet.md` + `repro.py` → **no hits**.
Sendable files are clean.

## No cross-references — **PASS**

`grep -nE '#[0-9]{2,}|dbos-inc/...#'` over `packet.md` + `repro.py` → **no
hits**. No upstream issue/PR numbers embedded (GitHub would mirror them onto
upstream timelines). Note for the human: if any fork commit accompanies this,
keep issue/PR numbers out of the commit message too.

## Security route — **N-A**

Not a disclosure bug (G7 N-A). Public issue is the correct channel once the rate
limit clears.

## Send checklist (for the human, when unblocked)

1. Confirm open-unanswered issue count on the repo is < 2.
2. Re-run the G4 duplicate searches.
3. Paste `packet.md` as the issue body; attach/inline `repro.py`.
4. File under `viswa-abe`.
5. Do not reference other issue/PR numbers in the body or any fork commit.

## Calibration log

(Append maintainer response after sending.)
