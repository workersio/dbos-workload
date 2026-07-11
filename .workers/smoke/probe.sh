#!/bin/sh
# Guest plumbing probe for the v2 scenario spine — the full verdict matrix on
# the stub target, direct invocations (no nested subprocess). Each section is
# marker-delimited so a partial transcript still localizes a hang.
cd "$(dirname "$0")/../.." || exit 44
echo "PROBE python=$(command -v python3)"
python3 -c "print('PY_OK')"
echo "== scenario_gen selftest =="
python3 .workers/lib/test_scenario_gen.py 2>&1 | tail -1
echo "== healthy L0 (expect GREEN rc=0) =="
python3 .workers/lib/run_scenario.py .workers/smoke/scenarios/t.md --seed 1
echo "HEALTHY_RC=$?"
echo "== redproof (expect ORACLE_SELFTEST PASS rc=0) =="
python3 .workers/lib/run_scenario.py .workers/smoke/scenarios/t.md --seed 1 --redproof
echo "REDPROOF_RC=$?"
echo "== planted red (expect RED rc=1) =="
WIO_STUB_MODE=red python3 .workers/lib/run_scenario.py .workers/smoke/scenarios/t.md --seed 1
echo "RED_RC=$?"
echo "== vacuous (expect VOID rc=3) =="
WIO_STUB_MODE=void python3 .workers/lib/run_scenario.py .workers/smoke/scenarios/t.md --seed 1
echo "VOID_RC=$?"
echo "== multi-actor interleaved (expect GREEN rc=0) =="
python3 .workers/lib/run_scenario.py .workers/smoke/scenarios/m.md --seed 3
echo "MULTI_RC=$?"
echo "== watchdog (expect liveness FAIL rc=1) =="
WIO_STUB_MODE=hang WIO_WATCHDOG_S=5 python3 .workers/lib/run_scenario.py .workers/smoke/scenarios/t.md --seed 1
echo "HANG_RC=$?"
echo "PROBE_DONE"
