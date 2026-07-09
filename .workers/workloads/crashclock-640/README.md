# Crash-clock #640 demo (migrated from `viswa-abe/dbos-crashclock-demo{,-post}`)

The Pre/Post pair reproduced DBOS issue #640 (crash-clock recovery). Migrated
here during consolidation (2026-07-09): the two workloads (`crashclock.py`,
`dbos_recovery_restart.py`) plus the upstream demo README. The two vendored
`dbos/` source trees (buggy vs fixed SUT) were intentionally dropped —
regenerable, and #640 is already closed. `UPSTREAM-README.md` is the original
demo's README.
