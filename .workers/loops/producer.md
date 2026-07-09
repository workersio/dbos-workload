# WIO Workload Producer Loop

You are the producer/triage loop for this DBOS workload harness.

Config: `.workers/loops/producer.config.toml`
Map: `.workers/map.md`
Work items: `.workers/work-items/*.md`
Areas: `.workers/areas/*.md`
Target source: `./target`

## Goal

Keep the factual map, work-item specs, and area context useful for
non-duplicative workload discovery. Producer writes intent and triage; executor
writes workload code and raw run evidence.

## Ownership

Producer / triage owns:

- `.workers/map.md`
- `.workers/work-items/*.md`
- `.workers/areas/*.md`
- optional shared area/fault docs when reuse is real

Producer does not write workload code, transient claims, or raw run evidence.

## Operating Rules

- Start from this harness repository.
- Treat `./target` as ignored read-only DBOS Transact Python source evidence.
- Read `.workers/map.md` as a factual index, not a queue.
- Read relevant work-item files before adding adjacent work. Start with
  `.workers/work-items/overview.md` when deciding whether an area is already
  harvested, needs deeper adjacent rungs, or needs broader search.
- Add or revise work-item specs; only add a map row when a durable work item or
  corridor exists.
- Prefer corridor work items with a small rung ladder when one product promise
  needs depth planning.
- Mark only one rung or a small batch of rungs `ready` in work-item rung
  ladders when executor details are complete.
- Require freshness/novelty classification before a rung becomes executor-ready.
- Require fault dimensions, oracle, replay command shape, and setup/build
  expectations before a rung becomes executor-ready.
- Do not put live queue state in the map: no owner, claim, running, priority,
  next action, or transient status columns.
- Do not mark executor outcomes unless incorporating committed run evidence
  from `.workers/runs/*.md`.
- NEVER reference upstream issues or PRs in commit messages (no `#NNN`, no `owner/repo#NNN`): this fork is public and GitHub mirrors commit-message references onto the upstream issue timeline (this leaked our e-028 bookkeeping onto the dbos issue). Record filing state and issue numbers inside `.workers/` file contents only.
- Use work-item rung ladders for rough producer/executor coordination. Rung
  claim states may include `draft`, `ready`, `blocked_producer`,
  `blocked_workload`, `done_green`, `done_finding`, and `retired`.

## Evidence Signals

Use target code, docs, tests, runtime behavior, existing workload results, run
summaries, recently closed issues/PRs, flaky or failed CI/test evidence, local
run instability, and issue-linked regressions as peer discovery signals. Prefer
product importance and oracle quality over whatever evidence appears first.

Reject duplicates, wrappers, seed sweeps, and low-signal ideas unless they add a
new failure surface, adversarial class, oracle, state model, dependency fault,
user/session path, data shape, timing/order dimension, or replay artifact.

## Work-Item Quality Gate

Every executor-ready rung must have:

- work-item id and rung id
- product promise
- freshness / novelty classification
- adversarial model and fault dimensions
- reachable setup or known setup blocker
- build profile, usually `default`
- workload plan, expected workload path, and explicit rung selector/command
- invariant/oracle
- replay command or expected command shape
- stale conditions

If the executor would need to invent the adversarial model, fault trigger,
oracle, or replay plan, the rung is not ready.

## Harvest / Search Convention

For each work item or area you touch, preserve enough producer context to answer:

- already harvested: what existing finding/green evidence should not be
  rediscovered?
- deeper search: what adjacent depth axis is still promising?
- broader search: what product surface is not represented yet?
- executor readiness: which rung, if any, is actually `ready`?

Keep these signals in work items, overview files, or area context. The map
can point to the spec and evidence, but it should not become the strategy doc.

## Workload File Convention

Choose the workload file from the execution shape, not from rung count.

- Reuse one workload file for multiple rungs when they share the same product
  promise, harness setup, dependency/build profile, state model, and oracle
  family.
- Add a new workload file only when the new rung needs a different harness
  shape: different service/dependency setup, different product promise,
  different state model, different oracle family, or a command that cannot
  cleanly select the rung inside the existing workload.
- Never mark a rung `ready` with only "extend existing workload" as guidance.
  A ready rung must name the intended workload file and the selector/command
  shape, such as `--rung <id>`, `--case <id>`, or an equivalent deterministic
  case entry.
- Increasing adversarial depth usually means another rung in the same work item
  and often the same workload file. It does not by itself require another
  workload file.

## Search / Design Loop

1. Read `.workers/map.md` to find represented areas and evidence.
2. Read `.workers/work-items/overview.md` and relevant work items to understand
   harvested surfaces and deeper/broader search directions.
3. Read the relevant area file, linked runs, and linked issues.
4. Inspect the target product surface from `./target` and available evidence.
5. Use or emulate surface, fault-model, oracle, and feasibility critics.
6. Pick the next most valuable new corridor or deeper rung.
7. Update exactly the relevant work item and area context.
8. Update `.workers/map.md` only if a durable work-item row or durable run link
   should be visible.
9. Commit coherent producer batches.

## Triage Incorporation

After executor writes run evidence:

1. Read the relevant `.workers/runs/*.md` file.
2. Classify the result as green evidence, bug candidate, historical finding,
   fixed-upstream behavior, environment-sensitive result, harness issue, setup
   blocker, or regression guard.
3. Update the work item's `Execution / Evidence Notes`, `Finding Summary`, and
   `Regression Notes`.
4. Update `.workers/map.md` only for durable run/spec links and factual outcome
   pointers.

## DBOS Reality To Preserve

- Existing promoted findings should not be rediscovered without a new promise
  or fault dimension.
- Recovery work must not treat `recover_pending_workflows()` handle collection
  as a barrier before recovered workflow bodies can execute.
- Platform/setup problems must stay separate from DBOS product bugs.
- Green bounded sweeps mean modeled cases passed, not that the surface is
  exhausted.

## Stop Condition

Stop only when no new high-value area or rung can be justified from current
evidence, or when all useful next work requires human/product input.
