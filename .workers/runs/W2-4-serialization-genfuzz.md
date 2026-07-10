# Run W2-4 — serialization-genfuzz (Wave 2, axis 4: genlib input campaign)

Corridor: `genlib-serialization-input-fidelity` (backlog §Wave 2, score 8).
Work-item: `e-033`. Promise: `portable-inputs-keep-their-types`.

New workload `.workers/workloads/serialization-genfuzz/serialization_genfuzz_workload.py`:
a seeded type-axis catalog (scalar / bignum / float-edge / str / unicode /
container / set_order / decimal / datetime / unsupported) driven through the REAL
product serializer `dbos._serialization.DBOSPortableJSONSerializer`, three
promise-anchored oracles + the v0.6.0 universal plane.

## Local sanity (pg :5459) — RED on all three oracles

| case | oracle | result |
|---|---|---|
| case-001 | portability: stored form must be strict RFC-8259 JSON | **RED** — `float('nan'/'inf'/'-inf')` → `NaN`/`Infinity`/`-Infinity`; 20 clean values PASS |
| case-002 | determinism: bytes identical across 4 `PYTHONHASHSEED`s | **RED** — `set_strs` & `dict_with_set` → `distinct=4`; `set_ints` stable; 18 others PASS |
| case-003 | e2e: real workflow input persisted to `dbos.workflow_status.inputs` | **RED** — stored input for nan = `{"namedArgs":{},"positionalArgs":[NaN]}` (invalid JSON on disk), workflow terminal **SUCCESS** (silent) |

Oracle non-vacuity: each case PASSES the majority of the catalog and FAILS only the
genuinely-problematic axis values — the oracles discriminate. Extra find:
`frozenset` raises raw `TypeError` in `_portableify` while `set` is handled
(asymmetric type support).

Depth/seed rationale: the finding is deterministic per case (case-002 varies
`PYTHONHASHSEED` internally via subprocesses, so one run fully demonstrates
non-determinism; no VM-seed branch). Baseline `--depth 1` is the clean proof;
`db-flaky` adds the Wave-2 fault axis (robustness, not the primary signal).

## Cloud confirm — PORTABILITY CONFIRMED (determinism local-only)

Command as above; `--depth 1`, project `kn71mb4p…`, branch main.

- **Portability (case-001) + e2e round-trip (case-003): CLOUD-CONFIRMED RED.**
  Exploration `nd78s44bvcgdrna4hqk6nt3edd8a98nt` (run `01KX628JKY…`, image
  `0186a22`): 6 invariants FAIL —
  `portable_strict_json_float_{nan,inf,ninf}` (serializer) +
  `roundtrip_strict_json_float_{nan,inf,ninf}` (the STORED `workflow_status.inputs`
  literally contains `NaN`/`Infinity`/`-Infinity`, workflow terminal SUCCESS).
  The earlier fault run (`nd76mwss…`, db-flaky) reproduced case-001 identically.
- **Determinism (case-002): LOCAL-CONFIRMED, cloud VOID.** The oracle spawns a
  fresh interpreter per `PYTHONHASHSEED` to expose set-iteration order; importing
  the full `dbos` stack in a guest subprocess takes >45s (cold musl imports) and
  times out, so the case VOIDs in cloud (batched to 4 spawns + launched via
  `python-runtime.sh` — still too slow; not a product signal). The determinism
  RED is rock-solid locally (`distinct=4` across 4 hash seeds; `set_ints` stable).
  Not iterating further: the primary portability finding is cloud-confirmed and the
  determinism finding is independently reproducible offline
  (`python3 …serialization_genfuzz_workload.py --case case-002`).

**e-033 verdict: portability RED cloud-confirmed; determinism RED local-confirmed.**
Filing HELD for Viswa.
