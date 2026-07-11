#!/usr/bin/env python3
"""Flow drivers for the DBOS Transact (Python) usage model.

Executor-owned. Each flow declared in ``usage-model.md`` gets exactly one
driver class here (check.py G2 enforces the bijection). The Python spine
owns seeds, clocks, ledger, and INVARIANT lines — see
``lib/CONTRACT.md`` for the Flow / make_sut / EVENTS contract.

Empty at scaffold time: the first executor episode writes the drivers for
the model's two hottest flows.
"""

FLOWS: dict = {}


def make_sut(meta, seed):
    raise NotImplementedError("no flows implemented yet — first executor episode fills this")


EVENTS: dict = {}
