# Area: decorator-composition-fidelity

## Current State

Current status: new from PR `#706`; one executor-ready rung models custom
decorator composition across DBOS workflow entry points. No run evidence yet.

Evidence:

- PR `#706`: "Test for Custom Decorators" added a product test for one async
  workflow wrapped by a custom `functools.wraps` decorator.
- `target/dbos/_registrations.py`: `get_func_info` and class registration walk
  the `__wrapped__` chain to find DBOS metadata.
- `target/dbos/_core.py`: workflow, transaction, and step decorators set DBOS
  names and function info on wrapped callables.
- `target/tests/test_dbos.py::test_workflow_wrapped_by_custom_decorator` covers
  direct async invocation, queue enqueue, client enqueue, and recovery for one
  decorator order.

## Product Promise

DBOS-decorated functions remain discoverable, durable, replayable, and
operator-visible when application code composes them with ordinary Python
decorators that preserve `__wrapped__`.

## Why This Matters

Applications often add tracing, auth, metrics, validation, or domain metadata
decorators around DBOS workflows and steps. If DBOS loses the intended function
metadata or registers the wrong wrapper, public enqueue/client/recovery paths
can fail only after deployment, or replay can silently run hooks and side
effects the wrong number of times.

## What Not To Repeat

- Do not only rerun PR `#706`'s single async happy path.
- Do not use a custom decorator that omits `functools.wraps` for the positive
  cases; that is a caller-contract violation unless modeled as a rejected
  control.
- Do not assert only that the wrapped function returns. The oracle must inspect
  durable workflow names, step names, hook ledgers, replay/recovery behavior,
  and public/client entry points.

## Loop-1 Search Directions

| Direction | Why It Is Deeper |
|---|---|
| Decorator order | DBOS metadata can live on either the outer callable or a wrapped inner callable. |
| Failure and replay | Hook execution counts are different for first execution, recovery, and replayed stored results. |
| Class methods | `@DBOS.dbos_class` walks wrapped instance/static/class methods and can lose class metadata. |
| Public entry points | Direct calls, queue enqueue, client enqueue, recovery, and list/status APIs use different name lookup paths. |

## Rung Index

```yaml
schema_version: 1
columns: [id, path, status, order, level, workload_file, matrix, summary]
rungs:
  - [
      "rung-001-custom-decorator-entrypoint-matrix",
      "inline:rung-001-custom-decorator-entrypoint-matrix",
      "ready",
      "1",
      "api-composition",
      ".workers/workloads/decorator-composition-fidelity/decorator_composition_fidelity_workload.py",
      "4 cases",
      "custom decorator composition must preserve DBOS metadata, hook counts, durable rows, entrypoint lookup, and replay/recovery semantics",
    ]
```

## Rung Details

### Rung: rung-001-custom-decorator-entrypoint-matrix

Rung metadata:

```yaml
schema_version: 1
kind: frontier_rung
id: rung-001-custom-decorator-entrypoint-matrix
frontier: decorator-composition-fidelity
status: ready
order: 1
level: api-composition
workload_file: .workers/workloads/decorator-composition-fidelity/decorator_composition_fidelity_workload.py
seeds: [7060, 7061, 7062, 7063]
updated_at: 2026-06-24T00:00:00Z
producer_evidence:
  - https://github.com/dbos-inc/dbos-transact-py/pull/706
  - target/dbos/_registrations.py
  - target/dbos/_core.py
  - target/dbos/_dbos.py
  - target/tests/test_dbos.py
references_used:
  - /Users/viswa/.agents/skills/wio/references/workload-modeling/overview.md
  - /Users/viswa/.agents/skills/wio/references/test-oracles-and-assertions/overview.md
  - /Users/viswa/.agents/skills/wio/references/user-behavior-testing/overview.md
gate_results:
  surface_evidence: ready_from_pr_706_and_registration_wrapped_chain_lookup
  duplicate_check: no_existing_workload_models_custom_decorator_composition_or_dbos_metadata_preservation
  product_test_gap: pr_706_covers_one_async_happy_path_one_decorator_order_only
  oracle_critic: ready_with_hook_ledger_durable_name_step_status_entrypoint_replay_and_recovery_invariants
```

#### Source Contract

- Frontier ID: `decorator-composition-fidelity`.
- Rung ID: `rung-001-custom-decorator-entrypoint-matrix`.
- Protected product promise: DBOS workflows, steps, transactions, and class
  methods remain registered under the intended DBOS names and execute with
  correct durable/replay semantics when wrapped by well-formed Python
  decorators using `functools.wraps`.
- Replay command:
  `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/decorator-composition-fidelity/decorator_composition_fidelity_workload.py --rung rung-001-custom-decorator-entrypoint-matrix --case <case-id> --seed <seed>`.
- Seed policy: exact seeds `7060`, `7061`, `7062`, and `7063`; every run must
  persist generated workflow IDs, DBOS registered names, wrapper order, hook
  ledger events, entry point used, expected step/function names, durable status
  rows, and replay/recovery observations.
- Invariant oracle: hook ledger, public results/errors, workflow status rows,
  operation output rows, list/status API names, and replay/recovery call counts
  must agree with the modeled decorator composition.

#### Goal

Exercise the user-facing decorator composition contract beyond PR `#706`'s
single happy path. The workload must prove that DBOS metadata survives
well-formed wrappers at every public entry point that uses function-name lookup
or durable replay.

#### Workload Shape

- Type: Python API/session workload with real DBOS runtime and Postgres.
- Build profile: default real Postgres through `.workers/run-with-postgres.sh`.
- Expected path:
  `.workers/workloads/decorator-composition-fidelity/decorator_composition_fidelity_workload.py`.
- Entry points:
  - `@DBOS.workflow`, `@DBOS.step`, `@DBOS.transaction`, and `@DBOS.dbos_class`
  - ordinary custom decorators using `functools.wraps`
  - direct calls, `DBOS.start_workflow`, queue enqueue, `DBOSClient.enqueue`,
    recovery, retrieval, and list/status APIs

#### Parameter Matrix

| Case | Seed | Schedule | Fault Model | Expected Focus |
|---|---:|---|---|---|
| case-001 | 7060 | dbos-outer-async-entrypoints | custom decorator inside `@DBOS.workflow`, matching PR `#706` but with durable row/name oracles and replay checks | direct, queue, client, and recovery paths run hooks exactly when execution happens; status rows use the DBOS workflow name; replay of a completed workflow does not rerun hooks |
| case-002 | 7061 | custom-outer-function-lookup | custom decorator outside `@DBOS.workflow` and `@DBOS.step`, both using `functools.wraps` | `DBOS.start_workflow`, queue enqueue, and client enqueue resolve the intended DBOS name through `__wrapped__`; step names remain stable and hook counts are not doubled |
| case-003 | 7062 | sync-failure-replay-boundary | sync wrapped workflow calls wrapped step and transaction, then fails through a modeled application error | first execution records the modeled error and hook error count; second invocation with same workflow ID replays stored failure without rerunning app hooks or duplicating transaction side effects |
| case-004 | 7063 | class-method-metadata | `@DBOS.dbos_class` wraps instance, static, and class method workflows/steps with custom decorators | class name, method workflow names, function type metadata, status rows, and handle results agree across direct and enqueued entry points |

#### Invariants

- Must hold:
  - Every positive custom decorator uses `functools.wraps` and therefore
    preserves a `__wrapped__` chain for DBOS metadata discovery.
  - DBOS public entry points resolve the intended DBOS workflow name, not an
    incidental wrapper `__qualname__`.
  - Durable `workflow_status.name`, step/function names, and list/status API
    observations match the modeled DBOS names.
  - Hook ledgers run exactly once for each real execution and not for completed
    replay paths that should use stored results.
  - Recovery re-executes only modeled pending work and preserves the same DBOS
    names and hook sequence.
  - Failure paths preserve the original application error and do not overwrite
    it with wrapper/metadata lookup failures.
- Must never happen:
  - Client or queue enqueue fails because the outer custom wrapper is not found
    in `workflow_info_map`.
  - Durable rows use wrapper names that make later retrieval or recovery miss
    the registered workflow.
  - Replay doubles custom hook side effects or transaction side effects.
  - The workload treats missing `functools.wraps` support as a DBOS bug in a
    positive case.

#### Expected Signatures

- Success: all cases reach the modeled entry points, durable metadata matches
  the expected DBOS names, hook ledgers match execution/replay/recovery state,
  and public results/errors agree with the model.
- Finding: workflow-not-found from a well-formed wrapper, wrong durable name,
  missing class metadata, duplicate hook/side-effect on replay, recovery using
  an unwrapped function path, or application error masked by wrapper metadata.
- Setup block: the executor cannot isolate hook ledgers or class registrations
  between cases without changing DBOS product code.
- Low signal: the workload only checks a returned value, repeats PR `#706`
  without durable row/replay/class-method oracles, or uses an invalid custom
  decorator as the main positive case.

## Oracle Contract

Use an independent hook ledger plus DBOS durable row observations. The ledger
records wrapper name, phase (`before`, `after`, `error`), workflow ID, entry
point, and execution attempt. Durable observations must include workflow status
name/status, operation output function names, class metadata where relevant,
and public handle/list results. The oracle fails if the ledger and durable DBOS
state disagree.

## Stale Conditions

Mark stale if DBOS changes decorator metadata storage, removes `__wrapped__`
chain traversal, changes workflow/class registration semantics, or adds a
first-class custom decorator API that supersedes the ordinary `functools.wraps`
contract.
