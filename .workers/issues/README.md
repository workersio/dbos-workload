# DBOS Issue Ledger

These are local drafts, candidate notes, and filing records for DBOS Transact
Python findings. Creating or updating these files does not create GitHub
issues.

Use [profile.md](profile.md) for DBOS-specific filing rules.

## Status Buckets

| Status | Meaning |
|---|---|
| `not-ready` | Track locally, but do not file yet. Needs local/normal DBOS repro, contract framing, duplicate/fix accounting, or stronger controls. |
| `ready` | Local draft is reviewable and has enough normal/local DBOS reproduction story, impact, evidence, and controls. |
| `filed` | Filed upstream or covered by an active upstream issue. |
| `closed` | Upstream closed/fixed/declined/superseded, or retained only as regression/history. |
| `discarded` | Do not file; harness/setup/platform issue, false oracle, unsupported usage, duplicate, or non-actionable. |

## Ready

| Finding | Draft | Disposition |
|---|---|---|
| `E-024` invoking a completed async workflow re-executes its function body (sync does not) | [E-024-decorator-replay-reruns-inner-hook-candidate.md](E-024-decorator-replay-reruns-inner-hook-candidate.md) | Reproduced locally with a standalone SQLite script ([repros/e024_decorator_replay_hook.py](repros/e024_decorator_replay_hook.py)); sync control passes. Root cause: `_outcome.py` `Pending._wrap` awaits the body before the completed-status short-circuit. Ready to file. |
| `E-023` SQLite datasource OAOO pre-check read is not covered by the lock retry loop | [E-023-sqlite-datasource-locked-retry-candidate.md](E-023-sqlite-datasource-locked-retry-candidate.md) | Reproduced locally with a standalone SQLite script ([repros/e023_sqlite_locked_precheck.py](repros/e023_sqlite_locked_precheck.py)); unlocked control passes. Root cause: `_datasource.py` `_check_execution` runs before the retry loop. Ready to file. |

## Not Ready

| Finding | Local artifact | Disposition |
|---|---|---|
| `E-003` portable structured error metadata lost in durable workflow status | Work item: [e-003.md](../work-items/e-003.md) | Decide whether this is distinct from issue `#730` / PR `#731`; if distinct, make a focused local draft. |
| `E-008` active debounce key delays unrelated queued workflows | [E-008-debouncer-starves-unrelated-queue.md](E-008-debouncer-starves-unrelated-queue.md) | Needs standalone or matched-environment repro; did not reproduce on macOS or x86 Linux EC2 — likely cloud-timing artifact. Do not file with only WIO evidence. |
| `E-025` client terminal missing event waits near full timeout before returning `None` | [E-025-client-terminal-missing-event-timeout-candidate.md](E-025-client-terminal-missing-event-timeout-candidate.md) | Candidate draft and possible contract question; clarify intended terminal-miss semantics. |
| Historical same-key debouncer duplicate completions | Local note pending | Convert legacy WIO script to a normal DBOS repro before filing. |
| Historical duplicate `get_event` deliveries under same-key burst | Local note pending | Convert legacy WIO script to a normal DBOS repro before filing. |
| `global_timeout` cancels future delayed workflow | Root draft: `/Users/viswa/code/workers/dbos-global-timeout-delayed-workflow-github-issue-draft-20260622.md` | Contract question only; local repro needs repair before filing. |
| `E-022` up-to-date warm starts wait behind held migration advisory lock | Work item: [e-022.md](../work-items/e-022.md) | Candidate tied to PR `#677`; convert cloud candidate into local/normal DBOS repro. |

## Filed

| Finding | Local artifact | Upstream | Disposition |
|---|---|---|---|
| `E-002` stale queued recovery can execute queue-owned work | [E-002-stale-queued-recoverer-executes.md](E-002-stale-queued-recoverer-executes.md) | [#742](https://github.com/dbos-inc/dbos-transact-py/issues/742) | Filed upstream; PR [#744](https://github.com/dbos-inc/dbos-transact-py/pull/744) is open as of 2026-06-25. |
| `E-015` SQL-enqueued required-role denial stays `PENDING` | [E-015-sql-auth-denied-workflow-stuck-pending.md](E-015-sql-auth-denied-workflow-stuck-pending.md) | [#743](https://github.com/dbos-inc/dbos-transact-py/issues/743) | Filed upstream; PR [#744](https://github.com/dbos-inc/dbos-transact-py/pull/744) is open as of 2026-06-25. |

## Closed

| Finding | Local artifact | Upstream | Disposition |
|---|---|---|---|
| Kafka offset loss after relaunch | Work item/area history | [#733](https://github.com/dbos-inc/dbos-transact-py/issues/733), PR [#738](https://github.com/dbos-inc/dbos-transact-py/pull/738) | Fixed upstream; keep as regression/history. |
| CLI missing Docker secret falls back to SQLite while app migration writes Postgres | [HIST-cli-missing-docker-secret-closed-734.md](HIST-cli-missing-docker-secret-closed-734.md) | [#734](https://github.com/dbos-inc/dbos-transact-py/issues/734) | Filed and closed by maintainer decision; do not duplicate. |
| Replacement children used by fork result but not owned by child graph/delete | [HIST-replacement-children-cleanup-semantics-closed-735.md](HIST-replacement-children-cleanup-semantics-closed-735.md) | [#735](https://github.com/dbos-inc/dbos-transact-py/issues/735) | Closed as intended behavior/docs clarification. |
| Portable JSON default serializer masks exceptions | Work item: [e-003.md](../work-items/e-003.md) | [#730](https://github.com/dbos-inc/dbos-transact-py/issues/730), PR [#731](https://github.com/dbos-inc/dbos-transact-py/pull/731) | Already covered/fixed; only file `E-003` if distinct from this issue. |
| Multi-schema client isolation | Work item: [e-005.md](../work-items/e-005.md) | PR [#728](https://github.com/dbos-inc/dbos-transact-py/pull/728) | Fixed upstream; do not duplicate. |
| Async partitioned queue exceeds worker concurrency | Work item: [e-006.md](../work-items/e-006.md) | PR [#727](https://github.com/dbos-inc/dbos-transact-py/pull/727) | Fixed upstream; do not duplicate. |
| `E-018` concurrent public `apply_schedules()` callers raise schedule-name conflicts | [e-018.md](../work-items/e-018.md) | PR [#741](https://github.com/dbos-inc/dbos-transact-py/pull/741) | PR merged on 2026-06-24; treat as fixed/regression history unless current upstream reproduces. |
| `#716` Postgres restart during recovery | Local history | PR [#717](https://github.com/dbos-inc/dbos-transact-py/pull/717) | Fixed upstream; regression green. |

## Discarded

| Finding | Disposition |
|---|---|
| Recovery DB restart gate timeout | False oracle: assumed recovery handle collection was a barrier. Do not file. |
| `#702` debounce deploy skew | Known unsupported deploy-skew behavior. Do not file new issue. |
| `#664` `asyncio.to_thread` transaction context | Unsupported usage; do not file unless maintainers ask to reopen. |
