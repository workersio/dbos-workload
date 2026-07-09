# DBOS Findings To Issues Profile

This profile tells the reusable findings-to-issues workflow how to treat DBOS
Transact Python findings in this workload harness.

## Target

| Field | Value |
|---|---|
| Product | DBOS Transact Python |
| Upstream repo | `dbos-inc/dbos-transact-py` |
| Local target checkout | `./target` |
| Current evidence ref | `3df88c4bcc3a2b73d91b765b459f4c7beb6a690c` |

## Status Rules

Use one lifecycle status per finding:

| Status | Meaning |
|---|---|
| `not-ready` | Track the finding, but do not file yet. It needs local/normal DBOS reproduction, contract framing, duplicate/fix accounting, or stronger controls. |
| `ready` | A local issue draft is reviewable and has a normal DBOS or local harness reproduction story, impact, evidence, and controls. |
| `filed` | The issue is already filed upstream or covered by an upstream issue that remains active. |
| `closed` | The upstream issue/PR is closed, fixed, declined, superseded, or retained only as regression/history. |
| `discarded` | Do not turn this into an issue. The behavior was a harness/setup/platform issue, false oracle, unsupported usage, duplicate, or non-actionable. |

Keep the nuanced judgment in `Disposition` or `Notes`, not in the status.

## Filing Bar

Do not mark a DBOS finding `ready` until it can be explained as DBOS product
behavior with a local or normal-DBOS reproduction story. WIO cloud evidence is
supporting evidence, not the only reason to believe the bug exists, unless the
artifact is explicitly marked as a candidate.

Prefer upstream issue bodies that include:

- a standalone script or small patch to an existing DBOS test;
- a local harness command only when it is the clearest current repro;
- explicit positive and negative controls;
- exact target ref, DBOS version when known, backend, and runtime;
- what the draft does not claim.

## DBOS-Specific Heuristics

- Separate DBOS product bugs from harness errors, cloud setup, Alpine packaging,
  read-only `/workspace`, macOS/Linux mismatch, timing calibration, and false
  oracles.
- Preserve passing adjacent cases because they sharpen the issue. For example,
  if Postgres controls pass and SQLite fails, say that.
- For lifecycle and retention semantics, file as a contract question only when
  the surprising behavior has cleanup, reliability, data-loss, operator, or
  user-visible impact.
- Do not re-file behavior covered by an upstream issue or merged PR.
- If maintainers close an issue as intended behavior, keep the local row
  `closed` with the rationale and do not reopen unless new evidence changes the
  product impact.
- If current upstream contains a fix but the pinned evidence ref does not, mark
  the finding `closed` or `filed` according to the upstream state, and keep the
  workload as regression/history.

## Required Draft Sections

Each local issue draft or candidate note should include:

- status and disposition;
- title;
- summary;
- environment;
- reproduction story;
- expected behavior;
- actual behavior;
- impact;
- evidence;
- controls and non-claims;
- upstream duplicate/fix check.
