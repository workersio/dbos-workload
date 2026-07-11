# Brief — e-033: portable serializer stores invalid JSON for float edges

## What breaks, in one paragraph

DBOS's "portable JSON" serializer is the format you pick when you want a
workflow's inputs to be readable across languages / SDKs / plain database
readers (as opposed to the default Python-pickle format). When a workflow using
it is called with `float('nan')`, `float('inf')`, or `float('-inf')`, the value
is written into the durable `dbos.workflow_status.inputs` column as the tokens
`NaN` / `Infinity` / `-Infinity`. Those tokens are **not** valid JSON (RFC 8259
has no such literals), so any conforming JSON reader — a workflow resumed in
another language, a `DBOSClient`, or a `JSON.parse` — rejects the stored input.
The workflow nonetheless finishes with status `SUCCESS` and no error is raised:
the durable input is silently non-portable exactly for the format whose entire
purpose is portability.

## Their promise

The portable-workflows docs page states the contract directly
(https://docs.dbos.dev/explanations/portable-workflows):

- "The `portable_json` format is straightforward use of JSON that all SDKs can
  read and write."
- data that "can even be read and written from the database without any DBOS
  code at all."
- "any DBOS application in any language can read or write it."

`NaN` / `Infinity` in the stored column violate all three — this is the
maintainer's own promise, not one we inferred. Defect framing (not a question).

## Severity and blast radius

Correctness, weight **3**. Realistic story: a Python service persists workflow
inputs with portable serialization specifically so a TypeScript/Go/Java worker
(or an external consumer reading Postgres directly, which the docs invite) can
pick them up. Any input containing a NaN/Infinity float — common in analytics,
ML feature payloads, sensor data, or a division that produced `inf` — is written
as invalid JSON. The cross-language reader fails to parse it; within pure Python
it stays hidden because DBOS's own `json.loads` happens to accept `NaN` on the
way back. So it's a latent durable-data defect that only surfaces at the
cross-language boundary the feature exists to serve.

## Verify it yourself

Fresh env, ~3 min (needs a reachable Postgres):

```
pip install "dbos==2.26.0" "psycopg[binary]" "sqlalchemy"
PG_URL="postgresql://postgres:dbos@localhost:5459/postgres" \
  python .workers/reports/e-033/repro.py
```

Expected red output:

```
float('nan'):
  status: SUCCESS
  stored inputs: {"namedArgs":{},"positionalArgs":[NaN]}
  valid JSON: False
REPRODUCES: workflow SUCCEEDED but its durable inputs column is not valid JSON
```

(Exit code 1 = reproduced; the control `1.5` stores valid JSON and passes.)

## Confidence

**High** for the portability finding.

- Cloud-confirmed and local-confirmed (run W2-4); reproduces 3/3 on a clean venv
  against released `dbos==2.26.0`.
- The serializer source region is byte-identical between released 2.26.0 and
  current upstream `main`, so it is red on both (not already fixed on main).
- Concrete cross-language failure shown: Node `JSON.parse` rejects the stored
  token.
- Residual risks considered and excluded: not a Python round-trip bug (DBOS's
  own reader tolerates NaN — that's why it's silent, and it's noted honestly in
  the packet); not environment-specific (pure serializer behavior of
  `json.dumps`); not flaky (deterministic per value).
- The **determinism** and **frozenset** observations (see appendix) are weaker
  and are deliberately NOT in the sendable packet.
