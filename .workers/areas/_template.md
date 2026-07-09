# Area: <frontier-id>

## Product Promise

What user/operator/API behavior must remain true?

## Why This Matters

Why would a bug here matter to a real user, operator, support workflow, release,
or production incident?

## Evidence

- Code:
- Tests:
- Docs:
- Existing workloads/runs:
- Recent churn:

## Adversarial Model

What assumption are we attacking? Include dependency faults, ordering/timing,
state recovery, duplicate/replayed actions, boundary data, permission edges, or
other realistic failure mechanisms.

## Rungs

| Rung | Status | Goal | Build profile | Oracle | Notes |
|---|---|---|---|---|---|

## Oracle Contract

What invariant must fail if the product promise is broken? Do not weaken this
oracle to make a workload pass.

## Stale Conditions

When should this frontier or a rung be marked `stale` instead of executed?
