# Appendix — e-033 (private; never sent)

## Severity / confidence

- Severity: **correctness, weight 3**. Durable input silently non-portable for
  the float edge cases, in the format whose purpose is portability.
- Confidence: **high** (portability finding). Cloud-confirmed (exploration
  `nd78s44bvcgdrna4hqk6nt3edd8a98nt`) + local, 3/3 on clean released 2.26.0,
  red on release AND main.

## Backing records

- Work-item: `.workers/work-items/e-033.md`
- Run record: `.workers/runs/W2-4-serialization-genfuzz.md`
- Workload: `.workers/workloads/serialization-genfuzz/serialization_genfuzz_workload.py`
- Repro (this packet): `.workers/reports/e-033/repro.py`

## What is in the packet vs held back

The sendable packet covers ONLY the **portability** finding (NaN/Infinity stored
as non-JSON, workflow SUCCEEDs) — the axis with an explicit upstream promise and
cloud confirmation. Two related observations are deliberately held out because
they are weaker / more likely to draw a closed-as-intended:

1. **Non-deterministic set ordering.** `_portableify(set)` = `[_portableify(v)
   for v in value]`, i.e. Python set-iteration order. For string elements that
   order is `PYTHONHASHSEED`-dependent, so the same `set` input serializes to
   different bytes across processes (verified on 2.26.0: seed 0 →
   `["k01","k03","k00",...]`, seed 1337 → `["k05","k02","k01",...]`). This
   breaks any input-hash/dedup that assumes stable bytes. But: the docs'
   "deterministic" language is about *datetime* normalization, not set ordering;
   sets have no canonical order in any language and aren't really part of the
   JSON value space (the serializer coerces set→list). A maintainer could
   reasonably say "don't put sets in portable inputs." Lower-confidence framing →
   held. If ever filed, frame as a question, not a defect.
2. **`frozenset` asymmetry.** `set` serializes (to a list) but `frozenset`
   raises a raw `TypeError: Object of type frozenset is not portable JSON
   serializable` (frozenset is not a `set` subclass, so `_portableify` has no
   branch). Minor / arguably-correct (unsupported type). Not worth a filing on
   its own.

## Anticipated maintainer questions (prepared answers)

- **"Python reads it back fine, so what's the problem?"** Correct — DBOS's own
  `deserialize` is plain `json.loads`, and CPython's `json.loads` accepts `NaN`
  by default, so a pure-Python round-trip survives. That is exactly why the
  defect is *silent*. The promise is cross-language / DBOS-code-free reads; a
  conforming reader (JS `JSON.parse`, Go `encoding/json`, strict
  `json.loads(..., parse_constant=...)`) rejects the column. Shown in the packet
  with Node.
- **"Isn't this just how `json.dumps` works?"** Yes — `json.dumps` emits
  non-standard tokens unless `allow_nan=False`. The report's point is that the
  *portable* serializer inherits that default while advertising standard,
  all-SDK-readable JSON, and persists the result durably without a modeled
  error. One-line fix surface: pass `allow_nan=False` (turns it into a modeled
  `ValueError` at serialize time) or encode NaN/Infinity portably.
- **"Which versions?"** Tested released 2.26.0 and current `main` (identical
  serializer source). Not tested: older releases (irrelevant; latest is red).
- **"Does it happen for normal floats?"** No — `1.5` etc. store valid JSON
  (control row in the repro). Only NaN / ±Infinity.

## Planned reply if closed-as-intended

Stay calm, no re-litigation. If the maintainer says NaN/Infinity are
out-of-scope for portable inputs: accept it, and ask whether the serializer
should then **reject** them with a modeled error at serialize time (so the
non-portable value never reaches the durable column silently) rather than
storing invalid JSON and reporting SUCCESS — that reframes the same evidence as
a fail-loud request, which is the durable-correctness core of the finding.

## Follow-up plan

When a fix lands (likely `allow_nan=False` or explicit NaN handling), re-run
`repro.py` against the fixed release and post a one-line confirmation on the
issue. Closing the loop builds reporter trust for the batch.

## Calibration log

(Append maintainer outcome here after sending: fixed / closed-as-intended /
question / no-response, with quoted reasoning.)
