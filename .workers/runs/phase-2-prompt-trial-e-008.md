# Phase 2 Prompt Trial: E-008

## Summary

| Field | Value |
|---|---|
| Work item | E-008 |
| Frontier | scheduler-debouncer-timing |
| Trial rung | rung-007-standalone-repro-minimization |
| Trial date | 2026-06-24 |
| Status | blocked_workload |

## Producer Step

Producer read `.workers/map.md` as a factual index and selected
`work-items/e-008.md` because the existing finding is explicitly
environment-sensitive:

- WIO cloud reproduced unrelated queue liveness delay under hot-key debouncer
  pressure.
- Plain macOS and EC2 runs did not reproduce it.
- The work item already says a smaller standalone repro or matched deterministic
  worker repro is needed before filing with a local-reproduction claim.

Producer proposed a follow-up rung:

```text
rung-007-standalone-repro-minimization
```

Intended product promise:

```text
active debouncer windows do not starve unrelated queued workflows
```

Intended purpose:

```text
reduce the WIO-only liveness failure to a standalone or matched-environment
repro before filing with a local-reproduction claim
```

## Executor Review

Executor read only the factual map row, `work-items/e-008.md`, linked run
`runs/E-008.md`, and linked issue draft. Executor rejected the trial rung before
creating or editing workload code.

Rejection reason:

- no concrete fault trigger beyond "standalone or matched-environment repro";
- no replay command or case selector for a new workload invocation;
- no environment-control plan that distinguishes DBOS liveness from WIO
  deterministic-worker scheduling;
- no updated oracle threshold for standalone execution, where existing local and
  EC2 attempts complete in about `1.01s`;
- no stale/freshness gate for target changes after PR `#739`.

Verdict:

```text
blocked_workload
```

The executor did not claim a transient `.workers/.claims/*` lock because the
work item did not meet the executor-ready gate. This is the intended prompt
behavior: missing fault/oracle/freshness/replay detail caused a useful blocked
result rather than silent strategy invention.

## Evidence Boundary

No workload code changed.

No WIO cloud or local target workload execution was run for this trial.

No `map.md` queue/status/owner fields were added. The map remains a factual
index. Producer/triage incorporated this blocked result into
`work-items/e-008.md`.

## Follow-Up Needed

Before this rung can become executor-ready, producer must specify:

- exact replay command and case selector;
- target ref and freshness classification;
- setup profile and environment controls;
- fault trigger or pressure mechanism;
- oracle threshold and timing evidence needed to separate product liveness from
  deterministic-worker/platform scheduling behavior;
- stale conditions for DBOS debouncer implementation changes.
