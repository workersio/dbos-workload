---
key: scheduling
title: Scheduling
description: Scheduled and debounced work fires predictably with the latest intended input, and schedule management stays atomic under concurrent callers.
order: 60
---

# Scheduling

What this area covers: DBOS's timed-execution surface — `@DBOS.scheduled`
cron workflows, dynamic schedules, and debouncers. The docs promise scheduled
work starts predictably, debounced work preserves the latest intended input
within max-wait bounds, and applying or updating schedules is atomic and
idempotent even when several callers race.

Boundaries:
- In scope: scheduler/debouncer timing and worker pressure, schedule registry
  concurrency and live updates, composition with queue controls.
- Out of scope until a promise names them: overlap policy for long-running
  scheduled workflows (upstream contract still open in issue #718).

Evidence lineage: legacy hunt corpora in `areas/scheduler-debouncer-timing.md`
(rungs 000–006) and `areas/schedule-registry-concurrency.md` (rung 001;
concurrent-apply race fixed upstream by PR #741, not yet in the pinned
target).
