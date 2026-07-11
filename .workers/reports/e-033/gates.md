# Gates — e-033 portable serializer portability/determinism

Verified against released `dbos==2.26.0` in an isolated uv venv (Python 3.12.3),
Postgres 16 on localhost:5459. Scratch:
`.../scratchpad/report-dbos/` (nothing written into the fleet repo except this
report dir).

## G1. Released-version reproduction (hard) — **PASS**

- Installed `dbos==2.26.0` (confirmed latest on PyPI via the JSON API:
  `info.version == "2.26.0"`). Ran `repro.py` end-to-end: workflow with
  `float('nan')` under PORTABLE serialization → stored
  `dbos.workflow_status.inputs` = `{"namedArgs":{},"positionalArgs":[NaN]}`,
  status `SUCCESS`. **Red on release.**
- **Main:** diffed the released `dbos/_serialization.py` `_portableify` +
  `serialize` region against the fork clone at HEAD `ac93a7a` (tracks upstream
  `main`): **IDENTICAL**. `serialize` = `json.dumps(portable,
  separators=(",",":"), ensure_ascii=False)` with no `allow_nan=False` in both.
  **Red on main too** → strongest send (live on release AND main), not
  already-fixed.

## G2. Determinism (hard) — **PASS**

- Ran `repro.py` 3 consecutive times from clean state: **3/3 red**, exit 1 each,
  stored column `{"namedArgs":{},"positionalArgs":[NaN]}` identical every run.
  The failure is deterministic per value (no timing/seed dependence in the
  portability path).

## G3. Minimality and standalone-ness (hard) — **PASS**

- `repro.py` is ~95 lines, imports only `dbos`, `sqlalchemy`, `json`, `os`,
  `uuid` — no imports from our tree, no scaffolding. Runs with
  `pip install "dbos==2.26.0" "psycopg[binary]" "sqlalchemy"` + one `python`
  invocation against any reachable Postgres (`PG_URL` env, sensible default).
  Creates/drops its own throwaway databases so it is idempotent.
- Self-evident output: prints the stored column and `valid JSON: False`, a
  control row (`1.5` → valid JSON: True) proving the check discriminates, and a
  `REPRODUCES` line; exit code 1 on repro.

## G4. Duplicate sweep (hard) — **PASS (no duplicate)**

Read-only `gh` searches on `dbos-inc/dbos-transact-py`, state=all:

- phrasings: `portable json NaN`, `Infinity serialization`, `portable serializer
  float`, `NaN`, `Infinity`, `RFC 8259`, `allow_nan`, `set serialization`,
  `non-deterministic serialization`, `portable_json` — **no issue about
  NaN/Infinity float portability or set determinism**.
- Nearest misses (both different, both CLOSED):
  - **#697** "Confusing `datetime` handling in Portable serialization" — about
    datetime type coercion on the read side; fixed by PR #700. Different seam.
  - **#730** "Portable-JSON default serializer fails serializing exceptions" —
    about the exception path when serializer is the global default; fixed by PR
    #731. Different seam.
- Recent commits touching `dbos/_serialization.py` (`git log`): #700 (datetime),
  #731 (exception serialization), #744 (recovery/roles), #694, #674 — **none
  touch the float/NaN or set-ordering paths**. No just-merged fix.

## G5. Promise provenance (soft — decides framing) — **PASS (their promise)**

Upstream docs page https://docs.dbos.dev/explanations/portable-workflows:
- "The `portable_json` format is straightforward use of JSON that all SDKs can
  read and write."
- "can even be read and written from the database without any DBOS code at all."
- "any DBOS application in any language can read or write it."

Plus source docstrings: module comment "Portable error payload to store in DB /
exchange across languages", serializer `name() == "portable_json"`. The
portability promise is explicit and upstream → **defect framing** for the
NaN/Infinity finding, not a question.

(Note: the *determinism-of-sets* sub-finding does NOT have an equivalent
explicit promise — the docs' "deterministic" language is about datetime
normalization. That axis is held out of the packet; see appendix.)

## G6. Root-cause pointer (soft) — **PASS (high confidence)**

`dbos/_serialization.py` → `DBOSPortableJSONSerializer.serialize` =
`json.dumps(_portableify(value), separators=(",",":"), ensure_ascii=False)`. No
`allow_nan=False`, so `json.dumps` emits `NaN`/`Infinity`/`-Infinity` for float
edges. Included in the packet, hedged as "appears to come from". Verified: the
symmetric `deserialize` = plain `json.loads`, which *accepts* NaN — explaining
why pure-Python round-trips hide the defect.

## G7. Security classification (hard router) — **N-A (not security)**

No plaintext-where-encrypted, key handling, auth/permission bypass, or
cross-tenant visibility. Durable-data correctness only. Not a SECURITY route.

## Gate summary

G1 PASS · G2 PASS · G3 PASS · G4 PASS · G5 PASS · G6 PASS · G7 N-A.
All hard gates pass → packet is send-worthy. Verdict is decided by PREFLIGHT
(rate limit).
