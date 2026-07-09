---
key: notifications-deliver-exactly-once
area: messaging
title: Notifications deliver exactly once
claim: >-
  Messages, events, and streams deliver exactly the expected observable
  notifications — duplicates are absorbed, timeouts and cancellation leave
  no stale waiters, and stream consumers resume at correct offsets across
  recovery.
status: active
provenance: https://docs.dbos.dev/python/tutorials/workflow-communication (send/recv, events, and streams deliver exactly-once observable notifications)
explorations:
  - key: duplicate-timeout-cancel
    title: Duplicates, timeouts, and cancels stay clean
    description: >-
      Duplicate sends, receive timeouts, and cancellation during waits;
      receivers must see each message once, timeouts must fire exactly
      as modeled, and no waiter may survive its workflow.
    status: done
    result: null
    reason: null
    workload: workloads/message-event-cancellation/message_event_cancellation_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-001-duplicate-timeout-cancel --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd7avfwz7h7kht2gm2jtpbd6md8a6p0s
  - key: live-stream-resume-offsets
    title: Streams resume at the right offset
    description: >-
      Live stream listeners disconnected and resumed must continue from
      correct offsets with no duplicated or skipped entries, across
      writer-side recovery and reader-side relaunch.
    status: done
    result: null
    reason: null
    workload: workloads/message-event-cancellation/message_event_cancellation_workload.py
    command: .workers/run-with-postgres.sh .workers/python-runtime.sh .workers/workloads/message-event-cancellation/message_event_cancellation_workload.py --rung rung-005-live-stream-resume-listener-offsets --all-cases --sequential
    faults: []
    depth: 1
    timeout: 600
    mem: 2048
    replay: null
    freshness: new-current
    reported: null
    published: nd71g1y1w14mvb3aybz1yprhth8a79sy
---

# Notifications deliver exactly once

Evidence lineage: `areas/message-event-cancellation.md` rungs 001–005, all
green; rung-005 proven on the pinned target 3df88c4b (48/48 invariants).
