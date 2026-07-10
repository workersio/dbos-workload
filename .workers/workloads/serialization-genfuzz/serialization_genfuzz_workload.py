#!/usr/bin/env python3
"""WIO workload — DBOS portable JSON input serializer: portability + determinism fidelity.

Frontier: serialization-genfuzz
Rung: rung-001-portable-input-serialization-fidelity

Protected product promise (`portable-inputs-keep-their-types`):
  "Workflows using the portable JSON serializer receive arguments matching their
  Python type hints ... datetimes normalize DETERMINISTICALLY and invalid values
  fail with modeled errors." The serializer name is `portable_json`; the stored
  form is advertised as *portable* JSON.

Mechanism under test (source-grounded, dbos/_serialization.py):
  DBOSPortableJSONSerializer.serialize(value) = json.dumps(_portableify(value),
  separators=(",",":"), ensure_ascii=False). Two properties the "portable JSON"
  + "deterministic" promise implies but the implementation does not guarantee:
    * PORTABILITY — json.dumps emits the NON-standard tokens NaN / Infinity /
      -Infinity for float('nan'/'inf'/'-inf') (dbos never passes allow_nan=False).
      RFC-8259 JSON has no such tokens; a strict / cross-language (JS JSON.parse)
      reader REJECTS the stored input. The "portable" claim fails for float edges.
    * DETERMINISM — _portableify(set/frozenset) = [ _portableify(v) for v in value ],
      i.e. it serializes in Python SET-ITERATION order, which depends on
      PYTHONHASHSEED and so varies BETWEEN PROCESSES. The same logical input
      serializes to different bytes in two workers → non-deterministic stored
      input, against the promise's "deterministically" language and any
      input-hash / dedup that assumes stable bytes.

  A third, softer axis (modeled-error fidelity): bytes and non-string-keyed maps
  raise a raw TypeError from _portableify ("... is not portable JSON
  serializable" / "map with non-string keys"). Observed, not hard-graded here.

Oracles (v0.6.0 plane):
  case-001 portability-strict-json (seed 8401) — every generated value's portable
    serialization must parse under a STRICT RFC-8259 reader (NaN/Infinity
    rejected). RED when a float-edge value yields a non-standard token.
  case-002 serialization-determinism (seed 8402) — every generated value is
    re-serialized in independent subprocesses under DISTINCT PYTHONHASHSEEDs; the
    bytes must be identical across seeds. RED when set/frozenset ordering diverges.
  case-003 end-to-end-workflow-roundtrip (seed 8403) — a real @DBOS.workflow
    configured with the portable serializer echoes each (portable-serializable)
    value; the STORED sysdb `inputs` bytes are read back and must (a) strict-parse
    and (b) equal the deterministic expected serialization. Anchors P+D at the
    actual product storage layer (needs Postgres). RED mirrors case-001/002 on
    the real path.

  Universal plane: liveness watchdog; terminal-state sweep (case-003 workflows
  must reach SUCCESS); crashclock declares the generator catalog as the swept
  space; anti-vacuity floors (a case that generated no value of its target class
  is VOID, not green); ORACLE_SELFTEST plants a known violation into each oracle
  so a green run is proven non-vacuous.

Replay:
  .workers/run-with-postgres.sh .workers/python-runtime.sh \
    .workers/workloads/serialization-genfuzz/serialization_genfuzz_workload.py \
    --rung rung-001-portable-input-serialization-fidelity --all-cases --sequential
"""
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[3]
VENDOR_ROOT = REPO_ROOT / ".workers" / "vendor" / "dbos-transact-py"
LOCAL_TARGET = REPO_ROOT / "target"
VENV_ROOT = REPO_ROOT / ".workers" / "vendor" / "dbos-venv"
LIB_ROOT = REPO_ROOT / ".workers" / "lib"

site_packages = sorted(VENV_ROOT.glob("lib/python*/site-packages"))
if site_packages:
    sys.path.insert(0, str(site_packages[-1]))
for _t in [VENDOR_ROOT, LOCAL_TARGET]:
    if _t.exists():
        sys.path.insert(0, str(_t))
        break
sys.path.insert(0, str(LIB_ROOT))

try:
    import crashclock
    import sqlalchemy as sa
    from sqlalchemy.engine import make_url

    from dbos import DBOS, DBOSConfig, SetWorkflowID
    from dbos._serialization import (
        DBOSPortableJSONSerializer,
        WorkflowSerializationFormat,
    )
except Exception as exc:  # pragma: no cover - setup evidence path.
    print(f"SETUP-BLOCK imports=false error={type(exc).__name__}: {exc}", flush=True)
    raise SystemExit(42)


FRONTIER_ID = "serialization-genfuzz"
RUNG_ID = "rung-001-portable-input-serialization-fidelity"
APP_ID = "wio-serialization-genfuzz"

CASE_MATRIX: dict[str, tuple[int, str]] = {
    "case-001": (8401, "portability-strict-json"),
    "case-002": (8402, "serialization-determinism"),
    "case-003": (8403, "end-to-end-workflow-roundtrip"),
}

# Declared swept space: the type-axis catalog index the generator walks. Auditors
# see the search space rather than a magic value list.
CATALOG_SPACE = crashclock.op_index("catalog_index", lo=0, hi=63)

LIVENESS_BUDGET_S = 200.0
HASHSEEDS = ("0", "1", "7", "1337")  # distinct PYTHONHASHSEED values for det. oracle


class SetupBlock(Exception):
    pass


# --------------------------------------------------------------------------- #
# Emission
# --------------------------------------------------------------------------- #
_INV: list[tuple[str, str, bool]] = []
_VOID_REASON: Optional[str] = None


def event(name: str, **fields: Any) -> None:
    parts = [f"WIO-EVENT {name}"]
    parts += [f"{k}={json.dumps(v, sort_keys=True, default=str)}" for k, v in fields.items()]
    print(" ".join(parts), flush=True)


def invariant(id_: str, name: str, ok: bool, **fields: Any) -> None:
    summary = json.dumps(fields, sort_keys=True, default=str) if fields else "ok"
    print(f"INVARIANT {id_} {name} {'PASS' if ok else 'FAIL'} {summary}", flush=True)
    _INV.append((id_, name, ok))


def mark_void(reason: str) -> None:
    global _VOID_REASON
    _VOID_REASON = reason


def final_verdict() -> int:
    if any(not ok for _, _, ok in _INV):
        fails = [i for i, _, ok in _INV if not ok]
        print(f"VERDICT: RED — {len(fails)} invariant(s) failed: {','.join(fails)}", flush=True)
        return 1
    if _VOID_REASON is not None:
        print(f"VERDICT: VOID — {_VOID_REASON}", flush=True)
        return 3
    print("VERDICT: GREEN — portable serialization stayed portable + deterministic", flush=True)
    return 0


# --------------------------------------------------------------------------- #
# Liveness watchdog
# --------------------------------------------------------------------------- #
class Liveness:
    def __init__(self, budget_s: float, label: str):
        self.budget_s, self.label = budget_s, label
        self._done = threading.Event()

    def _watch(self) -> None:
        if not self._done.wait(self.budget_s):
            print(f"INVARIANT liveness_{self.label} makes_progress FAIL "
                  + json.dumps({"budget_s": self.budget_s, "note": "watchdog fired"}), flush=True)
            print("VERDICT: RED — liveness watchdog fired", flush=True)
            os._exit(1)

    def __enter__(self) -> "Liveness":
        self._t = threading.Thread(target=self._watch, daemon=True)
        self._t.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._done.set()


# --------------------------------------------------------------------------- #
# Seeded type-axis catalog. Generation is deterministic (seed only affects the
# few randomized fillers); the class coverage is fixed so one run sweeps every
# axis. Each entry: (id, value, axis-class). `selftest` mutates specific entries.
# --------------------------------------------------------------------------- #
@dataclass
class GenValue:
    vid: str
    value: Any
    axis: str
    portable_serializable: bool  # False => _portableify raises (bytes / non-str keys)


def build_catalog(seed: int, selftest: bool) -> list[GenValue]:
    import random
    rng = random.Random(seed)
    cat: list[GenValue] = []

    def add(vid: str, value: Any, axis: str, ps: bool = True) -> None:
        cat.append(GenValue(vid, value, axis, ps))

    # --- scalars (portable, standard) ---
    add("none", None, "scalar")
    add("bool_t", True, "scalar")
    add("int_small", rng.randint(-1000, 1000), "scalar")
    add("int_big", 2 ** 70 + rng.randint(0, 1000), "bignum")     # > 2^53: JS-lossy
    add("float_norm", rng.random() * 1e6, "float")
    add("str_ascii", "hello-" + str(rng.randint(0, 9999)), "str")
    add("str_unicode", "héllo-☃-\U0001F600-key", "str")
    add("str_empty", "", "str")

    # --- float edges: the PORTABILITY axis (NaN/Infinity are non-standard JSON) ---
    add("float_nan", float("nan"), "float_edge")
    add("float_inf", float("inf"), "float_edge")
    add("float_ninf", float("-inf"), "float_edge")

    # --- containers ---
    add("list_mixed", [1, "a", None, 2.5], "container")
    add("tuple_mixed", (1, "a", None), "container")
    add("dict_str_keys", {"a": 1, "b": [2, 3]}, "container")
    add("nested", {"x": [{"y": (1, 2)}, {"z": None}]}, "container")

    # --- the DETERMINISM axis: sets of STRINGS serialize in hash-seed-dependent
    #     order (str hashing IS randomized by PYTHONHASHSEED; int hashing is not,
    #     so set(range(...)) would NOT diverge and is a weaker canary kept for
    #     contrast). _portableify handles `set` but NOT `frozenset` (frozenset is
    #     not a set subclass) — so frozenset is an unsupported-type entry. ---
    add("set_ints", set(range(24)), "set_order")                       # likely stable
    add("set_strs", set(f"k{i:02d}" for i in range(24)), "set_order")  # real canary
    add("dict_with_set", {"tags": set(f"t{i:02d}" for i in range(24)), "n": 1}, "set_order")
    add("frozenset_strs", frozenset(f"f{i:02d}" for i in range(8)), "unsupported", ps=False)

    # --- Decimal / datetime (portable via coercion string form) ---
    add("decimal_plain", Decimal("3.14159"), "decimal")
    add("decimal_exp", Decimal("1E+3"), "decimal")
    add("dt_aware", datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc), "datetime")

    # --- unsupported => _portableify raises TypeError (modeled-error axis) ---
    add("bytes_val", b"\x00\x01\x02raw", "unsupported", ps=False)
    add("nonstr_key_map", {1: "a", 2: "b"}, "unsupported", ps=False)

    if selftest:
        # Plant KNOWN violations so each oracle must go RED (proof of liveness):
        #  * a "portable" value carrying a raw NaN under a benign id the P-oracle
        #    would otherwise call clean — ensures P can fire even if the impl were
        #    fixed for the obvious float ids.
        add("selftest_hidden_nan", {"deep": [1, float("nan")]}, "float_edge")
        #  * an extra large STRING set to widen the determinism divergence surface.
        add("selftest_big_set", set(f"s{i:03d}" for i in range(64)), "set_order")
    return cat


# --------------------------------------------------------------------------- #
# The product serializer (exact instance the product configures) + a strict reader
# --------------------------------------------------------------------------- #
_SER = DBOSPortableJSONSerializer()


def _reject_constant(tok: str) -> Any:
    raise ValueError(f"non-standard JSON token: {tok}")


def strict_parses(text: str) -> tuple[bool, Optional[str]]:
    """True iff `text` is RFC-8259 JSON (NaN/Infinity rejected)."""
    try:
        json.loads(text, parse_constant=_reject_constant)
        return True, None
    except ValueError as exc:
        return False, str(exc)


# --------------------------------------------------------------------------- #
# Subprocess serialize mode (for the determinism oracle under varied hash seeds)
# --------------------------------------------------------------------------- #
def _determinism_vids(seed: int, selftest: bool) -> list[str]:
    """The vids whose serialization can diverge across processes: the set-order axis
    (plus any set-order selftest plants). Batched so the determinism oracle spawns
    ONE subprocess per hash seed, not one per value (heavy dbos import each)."""
    return [gv.vid for gv in build_catalog(seed, selftest)
            if gv.portable_serializable and gv.axis == "set_order"]


def _emit_serialization(seed: int, selftest: bool) -> int:
    """Regenerate the catalog (deterministic) and print, as one JSON object,
    {vid: portable_serialization} for every determinism-axis value. Run under a
    chosen PYTHONHASHSEED so set iteration order is exposed."""
    cat = {gv.vid: gv for gv in build_catalog(seed, selftest)}
    out: dict[str, str] = {}
    for vid in _determinism_vids(seed, selftest):
        out[vid] = _SER.serialize(cat[vid].value)
    print(json.dumps(out), flush=True)
    return 0


PYRUN = REPO_ROOT / ".workers" / "python-runtime.sh"


def _child_cmd(seed: int, selftest: bool) -> list[str]:
    """Spawn the emit subprocess through the SAME launcher the main workload uses
    (`python-runtime.sh`), so it picks the correct interpreter + musl PYTHONPATH.
    A raw /usr/bin/python3 subprocess in the guest hangs importing the musl-built
    dbos deps under glibc; the launcher avoids that. Falls back to the current
    interpreter locally when the launcher is absent."""
    tail = [str(Path(__file__).resolve()), "--emit-serialization", "--seed", str(seed)]
    tail += ["--selftest-catalog"] if selftest else []
    if PYRUN.exists():
        return [str(PYRUN), *tail]
    return [sys.executable, *tail]


def serialize_batch_under_hashseed(seed: int, selftest: bool, hashseed: str) -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = hashseed
    out = subprocess.run(
        _child_cmd(seed, selftest),
        capture_output=True, text=True, env=env, timeout=45,
    )
    if out.returncode != 0 or not out.stdout.strip():
        raise RuntimeError(f"emit failed rc={out.returncode} err={out.stderr[-200:]}")
    return json.loads(out.stdout.strip().splitlines()[-1])


# --------------------------------------------------------------------------- #
# Oracle cases
# --------------------------------------------------------------------------- #
def run_case_001_portability(seed: int, selftest: bool) -> None:
    """Every generated value's portable serialization must be strict RFC-8259 JSON."""
    cat = build_catalog(seed, selftest)
    checked = 0
    edge_seen = 0
    for gv in cat:
        if not gv.portable_serializable:
            continue
        try:
            text = _SER.serialize(gv.value)
        except Exception as exc:
            invariant(f"portable_serialize_{gv.vid}", "serialize_succeeds", False,
                      vid=gv.vid, axis=gv.axis, error=f"{type(exc).__name__}: {exc}")
            continue
        checked += 1
        if gv.axis == "float_edge":
            edge_seen += 1
        ok, why = strict_parses(text)
        invariant(f"portable_strict_json_{gv.vid}", "stored_form_is_standard_json", ok,
                  vid=gv.vid, axis=gv.axis, serialized=text[:120], reason=why)
    event("portability_summary", seed=seed, checked=checked, float_edges=edge_seen)
    if checked == 0:
        mark_void("case-001 serialized no value")
    if edge_seen == 0:
        mark_void("case-001 exercised no float-edge value (portability axis vacuous)")


def run_case_002_determinism(seed: int, selftest: bool) -> None:
    """The portable serialization of each determinism-axis value must be
    byte-identical across processes with distinct PYTHONHASHSEEDs. Batched: ONE
    subprocess per hash seed (each a fresh interpreter → fresh hash seed)."""
    vids = _determinism_vids(seed, selftest)
    if not vids:
        mark_void("case-002 exercised no set value (determinism axis vacuous)")
        return
    # batch[hashseed] = {vid: serialized}
    batches: list[dict[str, str]] = []
    try:
        for hs in HASHSEEDS:
            batches.append(serialize_batch_under_hashseed(seed, selftest, hs))
    except Exception as exc:
        # subprocess/interpreter infra failure — VOID (not a product RED, so no
        # failing invariant is emitted; the case simply cannot be graded here).
        mark_void(f"case-002 determinism subprocess infra failure: {type(exc).__name__}: {exc}")
        event("determinism_infra", ran=False, error=f"{type(exc).__name__}: {exc}")
        return
    for vid in vids:
        outs = [b.get(vid, "<missing>") for b in batches]
        stable = len(set(outs)) == 1
        invariant(f"determinism_{vid}", "cross_process_stable", stable,
                  vid=vid, distinct=len(set(outs)), samples=[o[:80] for o in outs[:2]])
    event("determinism_summary", seed=seed, set_axis_values=len(vids), hashseeds=len(HASHSEEDS))


# ---- case-003: real workflow round-trip (needs Postgres) ---- #
@DBOS.workflow(name="genfuzzEcho", serialization_type=WorkflowSerializationFormat.PORTABLE)
def echo_wf(value: Any) -> Any:
    return value


def admin_url() -> sa.URL:
    raw = os.environ.get("DBOS_POSTGRES_ADMIN_URL",
                         "postgresql+psycopg://postgres:dbos@localhost:5432/postgres")
    url = make_url(raw)
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+psycopg")
    return url


def quote_ident(v: str) -> str:
    return '"' + v.replace('"', '""') + '"'


def prepare_databases(prefix: str) -> tuple[str, str]:
    base = admin_url()
    app_db, sys_db = f"{prefix}_app", f"{prefix}_sys"
    admin = base.set(database=base.database or "postgres").render_as_string(hide_password=False)
    try:
        engine = sa.create_engine(admin, connect_args={"connect_timeout": 5})
        with engine.connect() as raw:
            c = raw.execution_options(isolation_level="AUTOCOMMIT")
            c.execute(sa.text("SET statement_timeout = '8000ms'"))
            for db in (app_db, sys_db):
                c.execute(sa.text(f"DROP DATABASE IF EXISTS {quote_ident(db)} WITH (FORCE)"))
                c.execute(sa.text(f"CREATE DATABASE {quote_ident(db)}"))
        engine.dispose()
    except Exception as exc:
        raise SetupBlock(f"postgres setup failed: {type(exc).__name__}: {exc}") from exc
    return (
        base.set(drivername="postgresql", database=app_db).render_as_string(hide_password=False),
        base.set(drivername="postgresql+psycopg", database=sys_db).render_as_string(hide_password=False),
    )


def drop_databases(prefix: str) -> None:
    base = admin_url()
    admin = base.set(database=base.database or "postgres").render_as_string(hide_password=False)
    engine = sa.create_engine(admin, connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as raw:
            c = raw.execution_options(isolation_level="AUTOCOMMIT")
            c.execute(sa.text("SET statement_timeout = '5000ms'"))
            for suffix in ("app", "sys"):
                c.execute(sa.text(
                    f"DROP DATABASE IF EXISTS {quote_ident(prefix + '_' + suffix)} WITH (FORCE)"))
    except Exception:
        pass
    finally:
        engine.dispose()


def read_stored_inputs(sys_url: str, wid: str) -> Optional[str]:
    engine = sa.create_engine(sys_url, connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as c:
            return c.execute(sa.text(
                "SELECT inputs FROM dbos.workflow_status WHERE workflow_uuid = :w"
            ), {"w": wid}).scalar()
    finally:
        engine.dispose()


def run_case_003_roundtrip(seed: int, selftest: bool, prefix: str) -> list[str]:
    cat = build_catalog(seed, selftest)
    app_url, sys_url = prepare_databases(prefix)
    config: DBOSConfig = {
        "name": APP_ID,
        "application_database_url": app_url,
        "system_database_url": sys_url,
        "application_version": f"{APP_ID}-c3",
        "executor_id": "wio-genfuzz-c3",
        "serializer": DBOSPortableJSONSerializer(),
        "enable_otlp": False,
    }
    wids: list[str] = []
    DBOS.destroy(destroy_registry=False)
    DBOS(config=config)
    DBOS.launch()
    try:
        checked = 0
        for gv in cat:
            if not gv.portable_serializable:
                continue
            wid = f"{FRONTIER_ID}-c3-{gv.vid}-{uuid.uuid4().hex[:8]}"
            try:
                with SetWorkflowID(wid):
                    echo_wf(gv.value)
            except Exception as exc:
                invariant(f"roundtrip_submit_{gv.vid}", "workflow_submit_succeeds", False,
                          vid=gv.vid, error=f"{type(exc).__name__}: {exc}")
                continue
            wids.append(wid)
            stored = read_stored_inputs(sys_url, wid)
            if stored is None:
                invariant(f"roundtrip_stored_{gv.vid}", "input_persisted", False, vid=gv.vid)
                continue
            checked += 1
            ok, why = strict_parses(stored)
            invariant(f"roundtrip_strict_json_{gv.vid}", "stored_input_is_standard_json", ok,
                      vid=gv.vid, axis=gv.axis, stored=stored[:120], reason=why)
        event("roundtrip_summary", seed=seed, checked=checked, workflows=len(wids))
        if checked == 0:
            mark_void("case-003 persisted no workflow input")
        return wids
    finally:
        # terminal sweep before teardown
        for wid in wids:
            try:
                status = DBOS.retrieve_workflow(wid).get_status().status
                invariant("terminal_state_case-003", "workflow_reaches_terminal",
                          status == "SUCCESS", wid=wid, status=status)
            except Exception as exc:
                invariant("terminal_state_case-003", "workflow_reaches_terminal", False,
                          wid=wid, error=f"{type(exc).__name__}: {exc}")
        DBOS.destroy(destroy_registry=False)
        drop_databases(prefix)


# --------------------------------------------------------------------------- #
# Case runner
# --------------------------------------------------------------------------- #
def run_case(case_id: str, seed_override: Optional[int]) -> None:
    if case_id not in CASE_MATRIX:
        raise SetupBlock(f"unsupported case {case_id}")
    seed, scenario = CASE_MATRIX[case_id]
    if seed_override is not None and seed_override != seed:
        raise SetupBlock(f"{case_id} requires seed {seed}, got {seed_override}")
    selftest = crashclock.selftest_active()
    crashclock.clock_armed(case_id, CATALOG_SPACE.point(seed))
    event("case_begin", case=case_id, scenario=scenario, seed=seed, selftest=selftest)

    with Liveness(LIVENESS_BUDGET_S, case_id):
        if scenario == "portability-strict-json":
            run_case_001_portability(seed, selftest)
        elif scenario == "serialization-determinism":
            run_case_002_determinism(seed, selftest)
        elif scenario == "end-to-end-workflow-roundtrip":
            prefix = f"wio_genfuzz_{seed}_c3"
            run_case_003_roundtrip(seed, selftest, prefix)
        else:  # pragma: no cover
            raise SetupBlock(f"unknown scenario {scenario}")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--rung", default=RUNG_ID)
    p.add_argument("--case", default=None)
    p.add_argument("--all-cases", action="store_true")
    p.add_argument("--sequential", action="store_true")
    p.add_argument("--seed", type=int, default=None)
    # subprocess serialize mode (determinism oracle)
    p.add_argument("--emit-serialization", action="store_true")
    p.add_argument("--selftest-catalog", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if args.emit_serialization:
        return _emit_serialization(args.seed, args.selftest_catalog)

    if args.rung not in (RUNG_ID, "rung-001"):
        print(f"SETUP-BLOCK unsupported rung {args.rung}", flush=True)
        return 42

    if args.all_cases:
        # Run the strong end-to-end round-trip (case-003) BEFORE the
        # subprocess-spawning determinism case (case-002), so a subprocess-infra
        # hiccup can never mask the primary evidence.
        cases = ["case-001", "case-003", "case-002"]
    elif args.case:
        cases = [args.case]
    else:
        cases = ["case-001"]

    try:
        for cid in cases:
            run_case(cid, args.seed if len(cases) == 1 else None)
    except SetupBlock as exc:
        print(f"SETUP-BLOCK {exc}", flush=True)
        return 44

    return final_verdict()


if __name__ == "__main__":
    raise SystemExit(main())
