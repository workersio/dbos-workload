# DBOS Workload Harness

This repository is for DBOS workload discovery and execution using the current
WIO workload harness shape.

Milestone events (upstream issue filed, finding cloud-confirmed, coverage
milestone) are tracked outside this repo too: update Viswa's wiki —
`~/work/wiki/pages/workers/plans/fleet-dbos.md` (narrative) and ledger rows in
`~/work/wiki/pages/workers/interception-ledger.md` — commit AND push the wiki
(pushing is publishing), and refresh `~/work/ROADMAP.md`.

Target under test:

- DBOS Transact Python: `./target`
- Generated workloads in this repo should exercise DBOS Transact Python
  behavior from that source tree and its public/runtime behavior.
- Treat this repo as the workload harness repo, not the product source repo.

Local target checkout:

- `./target` is intentionally gitignored. It gives agents a local DBOS Transact
  Python source tree to inspect without making this harness a multi-repo commit.
- If `./target` is missing, create it from the local clone when available:

  ```bash
  git clone /Users/viswa/code/workers/dbos-transact-py target
  git -C target checkout 0c41e6dfb46440184d19a52cdecc64a8c5f40d60
  ```

- If the local clone is unavailable, clone through the Workers GitHub alias:

  ```bash
  git clone git@github-autobot:dbos-inc/dbos-transact-py.git target
  git -C target checkout 0c41e6dfb46440184d19a52cdecc64a8c5f40d60
  ```

- Refresh `./target` only when the intended DBOS Transact Python evidence ref
  changes, then update `.workers/map.md` and both loop config files to record
  the new ref.

Rules:

- Do not read, copy, diff, or port workload implementations from archived DBOS
  workload repositories.
- Do not use `/Users/viswa/code/workload-archives/dbos-workload-legacy` as an
  implementation reference during generation.
- Archived DBOS workloads may be used only for human audit after a generated
  workload exists.
- New workload files under `.workers/workloads/` must be generated from DBOS
  Transact Python public behavior, local package behavior, docs, tests, runtime
  evidence from `./target`, and the canonical `.workers/map.md` /
  `.workers/frontiers/*.md` state in this repository.
- `./target` is an ignored local checkout of DBOS Transact Python for source
  exploration only. Treat it as read-only evidence. Do not commit it, do not
  write harness state there, and do not make product changes there unless the
  user explicitly switches the task to DBOS Transact Python itself.
- Every generated workload must cite its frontier id, rung id, protected
  product promise, replay command, seed policy, and invariant oracle.
- When doing DBOS workload discovery or generation, actively look for genuinely
  new frontiers as well as deeper rungs in existing frontiers. Prefer a new
  frontier when target code, docs, tests, runtime evidence, public behavior,
  existing workload results, target churn, issues, PRs, or flaky/failing tests
  expose an important product surface not represented in `.workers/frontiers/`.
- Do not add compatibility wrappers around old workloads. A generated workload
  must introduce a concrete failure surface, adversarial class, oracle,
  state model, dependency fault, timing/order dimension, or replay artifact.
- The previous role/event tree is not active state. Relevant finding and rung
  reality has been consolidated into `.workers/map.md` and
  `.workers/frontiers/*.md`.
- Start Codex from this harness repo. Producer and executor goals are in
  `.workers/loops/producer.md` and `.workers/loops/executor.md`.
- Do not start harness work from `/Users/viswa/code/workers` or from
  `./target`; this repo's `AGENTS.md` is the control surface for DBOS workload
  generation.
- Work directly on this repo's current branch for now. Commit coherent batches
  before running cloud workloads.
- Do not run Formal/cloud workloads from dirty tracked state. Commit
  workload/build changes first, verify a clean `git status`, then run the
  workload against that commit.
- Executor runs are cloud-first. Use WIO project `DBOS Workload Fresh`
  (`workersio/dbos-workload-fresh`, project ID
  `kn7a3jjm0frn1qgwpms30amdas88ztwy`) and push the committed harness branch to
  `fresh/main` before cloud prepare/run. Local workload execution is only for
  reproducing, minimizing, or debugging an already recorded cloud result, or for
  non-target sanity checks.
- Claims are local coordination only. Files under `.workers/.claims/` must never
  be committed.
