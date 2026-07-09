# WIO Workload Executor Loop

You are an executor loop for this DBOS workload harness.

Config: `.workers/loops/executor.config.toml`
Map: `.workers/map.md`
Work items: `.workers/work-items/*.md`
Claims: `.workers/.claims/`
Target source: `./target`

## Goal

Claim one executor-ready rung from a work item, implement or update the workload
needed for that rung, execute it in WIO cloud from a clean committed state, and
write raw run evidence. Do not rewrite producer-owned intent.

## Execution Boundary

- Execute real workload runs in Formal/WIO cloud using a prepared Git commit.
- Do not treat a local workload run as executor evidence unless it explicitly
  reproduces, minimizes, or debugs an already recorded cloud result.
- Local pre-cloud work is limited to reading evidence, claiming a rung, editing
  workload/build files, and non-target sanity checks such as syntax review.
- Cloud prepare uses `.workers/build.sh`, not ignored `./target`.
- Current WIO cloud project: `DBOS Workload Fresh`
  (`workersio/dbos-workload-fresh`, project ID
  `kn7a3jjm0frn1qgwpms30amdas88ztwy`).

## Claim Rules

- Start from this harness repository.
- Treat `./target` as ignored read-only DBOS Transact Python source evidence.
- Read `.workers/map.md` as a factual index, then read the selected
  `.workers/work-items/*.md`.
- Own exactly one `ready` rung inside one work item at a time.
- Do not select work items or rungs that already have durable run evidence or
  `done_*`/finding status unless the user explicitly asks for a rerun,
  minimization, or regression-guard execution. Use
  `.workers/work-items/overview.md` "Current claimable rungs" as the default
  executor queue when it exists.
- Claim by creating `.workers/.claims/<work-item-id>--<rung-id>.lock`.
- Claims are transient and must not be committed.
- Do not create tracked "picked this up" or "running" state.
- NEVER reference upstream issues or PRs in commit messages (no `#NNN`, no `owner/repo#NNN`): this fork is public and GitHub mirrors commit-message references onto the upstream issue timeline (this leaked our e-028 bookkeeping onto the dbos issue). Record filing state and issue numbers inside `.workers/` file contents only.

## Write Scope

Executor may write:

- `.workers/.claims/`
- `.workers/workloads/**`
- `.workers/runs/*.md`
- focused `.workers/build.sh` or `.workers/builds/*.sh` fixes when setup blocks
  the selected rung
- a mechanical rung status/run-link update in the selected work item when the
  run evidence or blocked result has been written
- a mechanical durable run-link update to `.workers/map.md` only when the
  workflow explicitly permits it

Executor must not casually rewrite the work-item spec, area promise,
adversarial axis, or curated finding/regression summary. Mechanical executor
status is allowed only to help producer/triage see rough outcome. If the
selected rung lacks fault details, oracle, freshness, setup, or replay plan,
write a blocked run note or blocked handoff and stop instead of inventing the
missing strategy.

## Clean Execution Rule

Do not run Formal/cloud workloads from dirty tracked state.

Before a real cloud workload execution:

1. Commit all workload/build changes needed for the selected rung.
2. Push the branch that WIO will prepare. This repo normally uses
   `fresh/main` for the `DBOS Workload Fresh` project.
3. Verify `git status --short` prints nothing except ignored local claim/cache
   files.
4. Record the commit SHA and branch that were run.

Canonical cloud command shape:

```bash
PROJECT_ID=kn7a3jjm0frn1qgwpms30amdas88ztwy
RUN_COMMIT="$(git rev-parse HEAD)"
git push fresh HEAD:main
wio switch cloud
wio projects prepare "${PROJECT_ID}"
wio simulate create "${PROJECT_ID}" \
  --branch main \
  --command '<cloud replay command from the work-item rung>' \
  --workload-path '<primary .workers/workloads/... file>' \
  --depth 1 \
  --timeout 600 \
  --mem 2048 \
  --format json
```

## Execution Loop

1. Read `.workers/map.md` to identify areas and specs.
2. Select one work-item rung whose claim state is `ready` and has no active
   claim, preferring the "Current claimable rungs" list when present.
3. Read the work item, selected rung, linked area file, and linked
   prior evidence.
4. Restate the product promise, fault dimensions, build profile, oracle, and
   replay plan in notes.
5. Use or emulate `wio-strategy-critic` before implementation.
6. Build the smallest workload that exercises that rung.
7. Commit workload/build changes required for execution.
8. Push and prepare the committed branch for WIO cloud execution.
9. Run the workload through WIO cloud using the selected command.
10. Preserve WIO batch/run IDs, raw command, branch, commit, target ref,
    seed/case, logs, artifacts, and invariant result.
11. Write `.workers/runs/<work-item-or-corridor-id>.md`.
12. Use or emulate `wio-test-reviewer` on the diff and run evidence.
13. Add only a mechanical rung status/run-link update to the selected work item
    when useful; leave curated interpretation to producer/triage.
14. Remove the transient claim and repeat.

## Workload Quality Gate

- One workload file may implement multiple rungs when the work item names that
  file and the rungs share setup, state model, and oracle family.
- Do not create a new workload file just because the selected rung is more
  adversarial than prior rungs. Add a selector/case inside the named workload
  file when the harness shape is the same.
- Create a new workload file only when the selected rung requires a different
  dependency/build profile, product promise, state model, oracle family, or
  replay command shape from the existing workload file.
- The selected rung must be runnable by an explicit deterministic selector or
  command. If the work item does not specify the workload file and selector,
  mark the rung `blocked_producer` instead of inventing the structure.
- Do not create one file per seed.
- Do not silently change the product promise, area, or rung.
- Do not weaken the oracle to make the workload pass.
- Setup failures are not product bugs.
- Findings require replayable invariant evidence.
- A wrapper, runner, or seed sweep is not a new workload unless it adds a new
  oracle, adversarial model, state path, dependency fault, or replay artifact.

## Stop Condition

Stop when no ready rung is claimable, or when every ready rung is blocked by
missing setup/product context that needs producer or human input.
