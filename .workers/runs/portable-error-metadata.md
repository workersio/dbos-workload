# Run evidence — portable-error-metadata

Exploration: `portable-error-metadata`
Promise: `errors-keep-their-meaning` (area: serialization / serialization-error-fidelity)
Rung: `rung-004-portable-structured-error-metadata` case-001

## Verdict: FINDING (RED confirmed) — E-003

Invariant `structured_raw_workflow_error_preserves_metadata` **FAIL**. An error
stored with portable JSON metadata must return its modeled name, message, code,
and data through public retrieval; on a raw workflow error the structured
metadata is dropped, so the reconstructed error loses fields a caller relies on.
hasInvariantViolation=True, 5 invariants (4 PASS + 1 FAIL).

## Run

| batch | run id | image | state | invariants |
|---|---|---|---|---|
| nd73h6n8wwep5em0jm2vq85ejd8a6f7w | 01KX3YEZAWWGWH4Q7BX7DD7K6Q | d255d25 | failed | hasInvariantViolation=True; FAIL structured_raw_workflow_error_preserves_metadata |

Command: `.workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/serialization-error-fidelity/serialization_error_fidelity_workload.py --rung rung-004-portable-structured-error-metadata --case case-001` (depth 1, no faults).

## Interpretation

Real error-fidelity finding on the pinned target (the serialization workload
already emits the parseable INVARIANT format, so the red surfaced without a
harness fix). Sibling rung-003 (serializer TypeError masking the app error under
portable config) is the other candidate on this promise; not run this episode.
Upstream filing is a human decision (`reported: null`). Published to the internal
status page as a red.
