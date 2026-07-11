---
key: durable-crash-recovery
rung: L3
cast: {workflow-runner: 2}
flows: [durable-workflow]
event: {key: crash-restart, at: crashclock}
depth: 50
status: planned
result: null
replay: null
redproof: null
invariants: [step-exactly-once, resumes-after-crash, workflow-terminal]
story: >-
  The server crashes in the middle of two jobs; when it restarts, both finish
  correctly and no step runs a second time.
---
L3 recovery probe (the core promise). A crash-restart lands at the crashpoint
barrier between submit and re-observe; recovery must drive both workflows to
SUCCESS with each completed step still run exactly once. Its no-event sibling is
durable-solo/durable-contention at lower rungs, so a red here is attributable to
recovery. Promote to ready after the L1 floor is done.
