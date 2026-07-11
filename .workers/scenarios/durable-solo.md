---
key: durable-solo
rung: L0
cast: {workflow-runner: 1}
flows: [durable-workflow]
depth: 15
status: ready
result: null
replay: null
redproof: {run: 01KX9GH6PHB59KVRBZMYDFGRD7, seed: 88385493}
invariants: [step-exactly-once, resumes-after-crash, workflow-terminal]
story: >-
  A single job runs its three steps once each and finishes, and its result is
  still readable from the database afterward.
---
L0 floor for durable-workflow: one workflow-runner, no event. Establishes that
a healthy run is GREEN so the crash scenarios' reds are attributable. The oracle
acks the durable result + each step-run=1, then re-observes both from the system
DB. Red-proof plants a lost/mutated observation on one of these acked entries.
